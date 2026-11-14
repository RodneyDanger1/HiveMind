//! Engine-level read-only tool-loop tests (W12 Wave 2).
//!
//! These drive the real `RunEngine` with a scripted `ProviderExecutor` (test
//! double only; no paid call, no network). They pin the wiring contract:
//! `TOOL_CALL` is published before execution, only read-only tools execute
//! through the configured workspace jail, non-read-only calls are refused by
//! name without side effects, and every call is persisted as a `tool_turns` row
//! while the per-run counters enforce the W12 budgets. A plain reply keeps the
//! pre-1.2 behaviour exactly.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use hiverouter_contract::{AgentState, Refusal, RunId, TaskId, ToolOutcome, Usage};
use hiverouter_core::bus::{EventBus, SequencedEvent};
use hiverouter_core::db::Database;
use hiverouter_core::entities::{Run, TaskRecord};
use hiverouter_core::execution::{
    DeltaSink, ProviderExecutor, RunEngine, StreamedReply, ToolTurnTranscript,
};
use hiverouter_core::graph::GraphAuthority;
use hiverouter_core::planner::{Plan, PlanNode};
use hiverouter_core::prompts::TemplateRegistry;
use hiverouter_core::tool_loop::{MAX_CALLS_PER_RUN, MAX_TOOL_TURNS};
use hiverouter_core::store::RunStore;
use hiverouter_provider::tool_assembler::AssembledToolCall;
use hiverouter_tools::WorkspaceJail;
use tokio::sync::broadcast::error::RecvError;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const NOTE_TEXT: &str = "hello tool loop";

fn leaf_plan() -> Plan {
    Plan::new(vec![PlanNode {
        objective: None,
        node_id: TaskId::new(),
        role: "super".to_owned(),
        deps: Vec::new(),
        depth: 1,
        parent: None,
    }])
}

/// Scripted provider: returns one leaf plan, a fixed set of assembled tool
/// calls for the leaf turn, and queued follow-up replies for the tool loop.
/// Test-only double; production paths never contain it.
struct ScriptedExecutor {
    plan: Plan,
    tool_calls: Vec<AssembledToolCall>,
    follow_ups: Mutex<VecDeque<StreamedReply>>,
    stream_continuation: bool,
    fail_continuation: bool,
    transcript_lengths: Mutex<Vec<usize>>,
    transcript_result_counts: Mutex<Vec<usize>>,
}

impl ScriptedExecutor {
    fn new(tool_calls: Vec<AssembledToolCall>) -> Arc<Self> {
        Self::build(tool_calls, Vec::new(), false, false)
    }

    fn with_follow_ups(
        tool_calls: Vec<AssembledToolCall>,
        follow_ups: Vec<StreamedReply>,
    ) -> Arc<Self> {
        Self::build(tool_calls, follow_ups, false, false)
    }

    fn with_streaming_follow_up(
        tool_calls: Vec<AssembledToolCall>,
        follow_ups: Vec<StreamedReply>,
    ) -> Arc<Self> {
        Self::build(tool_calls, follow_ups, true, false)
    }

    fn with_failing_continuation(tool_calls: Vec<AssembledToolCall>) -> Arc<Self> {
        Self::build(tool_calls, Vec::new(), false, true)
    }

    fn build(
        tool_calls: Vec<AssembledToolCall>,
        follow_ups: Vec<StreamedReply>,
        stream_continuation: bool,
        fail_continuation: bool,
    ) -> Arc<Self> {
        Arc::new(Self {
            plan: leaf_plan(),
            tool_calls,
            follow_ups: Mutex::new(follow_ups.into()),
            stream_continuation,
            fail_continuation,
            transcript_lengths: Mutex::new(Vec::new()),
            transcript_result_counts: Mutex::new(Vec::new()),
        })
    }

    /// Observed `(transcript lengths, cumulative transcript result counts)` per
    /// continuation call, in call order.
    fn transcript_observation(&self) -> (Vec<usize>, Vec<usize>) {
        (
            self.transcript_lengths
                .lock()
                .expect("transcript lengths")
                .clone(),
            self.transcript_result_counts
                .lock()
                .expect("transcript result counts")
                .clone(),
        )
    }
}

#[async_trait]
impl ProviderExecutor for ScriptedExecutor {
    async fn plan(
        &self,
        _run_id: RunId,
        _prompt: &str,
        _cancel: &CancellationToken,
    ) -> Result<Plan, Refusal> {
        Ok(self.plan.clone())
    }

    async fn execute(
        &self,
        _run_id: RunId,
        _task: &TaskRecord,
        _prompt: &str,
        _cancel: &CancellationToken,
        _on_delta: &DeltaSink,
    ) -> Result<StreamedReply, Refusal> {
        Ok(StreamedReply {
            deltas: Vec::new(),
            usage: Usage::default(),
            tool_calls: self.tool_calls.clone(),
            reasoning: String::new(),
        })
    }

    async fn continue_with_tools(
        &self,
        _run_id: RunId,
        _task: &TaskRecord,
        _prompt: &str,
        _cancel: &CancellationToken,
        on_delta: &DeltaSink,
        transcript: &[ToolTurnTranscript],
    ) -> Result<StreamedReply, Refusal> {
        let cumulative_results = transcript
            .iter()
            .map(|turn| turn.results.len())
            .sum::<usize>();
        self.transcript_lengths
            .lock()
            .expect("record transcript length")
            .push(transcript.len());
        self.transcript_result_counts
            .lock()
            .expect("record transcript results")
            .push(cumulative_results);
        if self.fail_continuation {
            return Err(Refusal::ProviderInvalidResponse);
        }
        let next = self
            .follow_ups
            .lock()
            .expect("follow-up queue lock")
            .pop_front();
        let reply = next.unwrap_or_else(final_answer);
        if self.stream_continuation {
            for delta in &reply.deltas {
                on_delta(delta);
            }
        }
        Ok(reply)
    }
}

fn final_answer() -> StreamedReply {
    StreamedReply {
        deltas: vec!["final answer".to_owned()],
        usage: Usage::default(),
        tool_calls: Vec::new(),
        reasoning: String::new(),
    }
}

fn tool_only_reply(tool_calls: Vec<AssembledToolCall>) -> StreamedReply {
    StreamedReply {
        deltas: Vec::new(),
        usage: Usage::default(),
        tool_calls,
        reasoning: String::new(),
    }
}

struct Harness {
    engine: Arc<RunEngine>,
    bus: Arc<EventBus>,
    db: Arc<Database>,
    _data: tempfile::TempDir,
}

fn harness(executor: Arc<dyn ProviderExecutor>, workspace: Option<&std::path::Path>) -> Harness {
    let data = tempfile::tempdir().expect("data tempdir");
    let db = Arc::new(Database::open(&data.path().join("hiverouter.db")).expect("open db"));
    db.migrate().expect("migrate");
    let bus = Arc::new(EventBus::new());
    let mut engine = RunEngine::new(
        db.clone(),
        bus.clone(),
        Arc::new(GraphAuthority::new()),
        executor,
        Arc::new(TemplateRegistry::default()),
    );
    if let Some(root) = workspace {
        engine = engine.with_tool_workspace(WorkspaceJail::open(root).expect("workspace jail"));
    }
    Harness {
        engine: Arc::new(engine),
        bus,
        db,
        _data: data,
    }
}

#[derive(Default)]
struct Collected {
    tool_calls: Vec<serde_json::Value>,
    tool_results: Vec<serde_json::Value>,
    agent_messages: Vec<serde_json::Value>,
    reply_deltas: Vec<String>,
    sequence: Vec<String>,
    final_status: serde_json::Value,
}

async fn next_event(
    receiver: &mut tokio::sync::broadcast::Receiver<SequencedEvent>,
) -> SequencedEvent {
    loop {
        match tokio::time::timeout(Duration::from_secs(10), receiver.recv()).await {
            Ok(Ok(event)) => return event,
            Ok(Err(RecvError::Lagged(_))) => continue,
            Ok(Err(RecvError::Closed)) => panic!("engine event bus closed"),
            Err(_) => panic!("timed out waiting for an engine event"),
        }
    }
}

async fn drive(harness: &Harness, prompt: &str) -> (Run, Collected) {
    let mut receiver = harness.bus.subscribe();
    let run = harness
        .engine
        .submit_prompt(prompt, "project-1", Uuid::now_v7())
        .expect("submit prompt");
    let mut collected = Collected::default();
    loop {
        let event = next_event(&mut receiver).await;
        match event.envelope.name.as_str() {
            "TOOL_CALL" => {
                collected.sequence.push("TOOL_CALL".to_owned());
                collected.tool_calls.push(event.envelope.payload.clone());
            }
            "TOOL_RESULT" => {
                collected.sequence.push("TOOL_RESULT".to_owned());
                collected.tool_results.push(event.envelope.payload.clone());
            }
            "AGENT_MESSAGE" => {
                collected.sequence.push("AGENT_MESSAGE".to_owned());
                collected.agent_messages.push(event.envelope.payload.clone());
                if event.envelope.payload["kind"] == "ask_parent" {
                    let message_id = harness
                        .engine
                        .store
                        .agent_messages_for_run(run.id)
                        .ok()
                        .and_then(|messages| {
                            messages
                                .into_iter()
                                .find(|message| message.status == "open")
                                .and_then(|message| message.id.parse::<Uuid>().ok())
                        });
                    if let Some(message_id) = message_id {
                        // Simulated operator answer for the blocking ask_parent path.
                        harness
                            .engine
                            .store
                            .answer_agent_message(run.id, message_id, "operator", "parent answer")
                            .expect("answer");
                        harness.engine.wake_a2a_waiter(message_id).await;
                    }
                }
            }
            "REPLY_DELTA" => {
                if let Some(text) = event.envelope.payload["text_delta"].as_str() {
                    collected.reply_deltas.push(text.to_owned());
                }
            }
            "RUN_STATUS" => {
                if event.envelope.payload["status"] == "running" {
                    continue; // D2: the run now starts before leaf execution
                }
                collected.final_status = event.envelope.payload.clone();
                break;
            }
            _ => {}
        }
    }
    (run, collected)
}

fn read_call(id: &str, path: &str, index: u32) -> AssembledToolCall {
    AssembledToolCall {
        index,
        id: id.to_owned(),
        name: "file_read".to_owned(),
        arguments: serde_json::json!({ "path": path }),
    }
}

#[tokio::test]
async fn ask_parent_writes_a_durable_message() {
    let harness = harness(
        ScriptedExecutor::new(vec![AssembledToolCall {
            index: 0,
            id: "call_msg".to_owned(),
            name: "ask_parent".to_owned(),
            arguments: serde_json::json!({ "text": "need the parent decision" }),
        }]),
        None,
    );
    let (run, collected) = drive(&harness, "message the parent").await;
    assert_eq!(collected.final_status["status"], "completed");
    assert_eq!(collected.tool_results.len(), 1);
    assert_eq!(collected.tool_results[0]["outcome"], "ok");
    assert_eq!(
        harness
            .engine
            .store
            .agent_message_count(run.id)
            .expect("message count"),
        1
    );
    assert_eq!(collected.agent_messages.len(), 1);
    assert_eq!(collected.agent_messages[0]["kind"], "ask_parent");
    assert_eq!(collected.agent_messages[0]["text"], "need the parent decision");
    let messages = harness
        .engine
        .store
        .agent_messages_for_run(run.id)
        .expect("messages");
    assert_eq!(messages[0].status, "answered");
    assert_eq!(messages[0].answer_text.as_deref(), Some("parent answer"));
}

#[tokio::test]
async fn read_only_tool_call_executes_publishes_and_persists() {
    let workspace = tempfile::tempdir().expect("workspace");
    std::fs::write(workspace.path().join("note.txt"), NOTE_TEXT.as_bytes()).expect("write note");
    let harness = harness(
        ScriptedExecutor::new(vec![read_call("call_1", "note.txt", 0)]),
        Some(workspace.path()),
    );

    let (run, collected) = drive(&harness, "read the note").await;
    assert_eq!(collected.final_status["status"], "completed");
    assert_eq!(collected.tool_calls.len(), 1);
    assert_eq!(collected.tool_calls[0]["tool"], "file_read");
    assert_eq!(collected.tool_calls[0]["tool_call_id"], "call_1");
    assert_eq!(collected.tool_calls[0]["turn"].as_u64(), Some(1));
    assert_eq!(
        collected.tool_calls[0]["args_sha256"]
            .as_str()
            .expect("args hash")
            .len(),
        64
    );
    assert_eq!(collected.tool_results.len(), 1);
    assert_eq!(collected.tool_results[0]["outcome"], "ok");
    assert_eq!(
        collected.tool_results[0]["bytes"].as_u64(),
        Some(NOTE_TEXT.len() as u64)
    );
    assert_eq!(
        collected.tool_results[0]["sha256"]
            .as_str()
            .expect("result hash")
            .len(),
        64
    );
    let call_position = collected
        .sequence
        .iter()
        .position(|name| name == "TOOL_CALL")
        .expect("TOOL_CALL event");
    let result_position = collected
        .sequence
        .iter()
        .position(|name| name == "TOOL_RESULT")
        .expect("TOOL_RESULT event");
    assert!(call_position < result_position, "TOOL_CALL must precede TOOL_RESULT");

    let turns = harness.db.tool_turns_for_run(run.id).expect("tool turns");
    assert_eq!(turns.len(), 1);
    assert_eq!(turns[0].tool, "file_read");
    assert_eq!(turns[0].tool_call_id, "call_1");
    assert_eq!(turns[0].outcome, ToolOutcome::Ok);
    assert_eq!(harness.db.run_tool_counters(run.id).expect("counters"), (1, 1));
}

#[tokio::test]
async fn plain_reply_keeps_the_pre_tool_loop_behaviour() {
    let harness = harness(ScriptedExecutor::new(Vec::new()), None);
    let (run, collected) = drive(&harness, "just answer").await;
    assert_eq!(collected.final_status["status"], "completed");
    assert!(collected.tool_calls.is_empty(), "plain replies emit no TOOL_CALL");
    assert!(collected.tool_results.is_empty(), "plain replies emit no TOOL_RESULT");
    assert!(harness
        .db
        .tool_turns_for_run(run.id)
        .expect("tool turns")
        .is_empty());
    assert_eq!(harness.db.run_tool_counters(run.id).expect("counters"), (0, 0));
}

#[tokio::test]
async fn non_read_tool_is_refused_by_name_and_never_executes() {
    let workspace = tempfile::tempdir().expect("workspace");
    let write_call = AssembledToolCall {
        index: 0,
        id: "call_write".to_owned(),
        name: "file_write".to_owned(),
        arguments: serde_json::json!({
            "path": "forbidden.txt",
            "content": "must-not-be-written"
        }),
    };
    let harness = harness(
        ScriptedExecutor::new(vec![write_call]),
        Some(workspace.path()),
    );

    let (run, collected) = drive(&harness, "write a file").await;
    assert_eq!(collected.final_status["status"], "completed");
    assert_eq!(collected.tool_calls.len(), 1, "the attempt is announced");
    assert_eq!(collected.tool_results.len(), 1);
    assert_eq!(collected.tool_results[0]["outcome"], "refused");
    assert_eq!(collected.tool_results[0]["code"], "E_TOOL_NOT_ALLOWED");
    assert!(
        !workspace.path().join("forbidden.txt").exists(),
        "a non-read-only tool must not execute"
    );
    let turns = harness.db.tool_turns_for_run(run.id).expect("tool turns");
    assert_eq!(turns.len(), 1);
    assert_eq!(turns[0].outcome, ToolOutcome::Refused);
    assert_eq!(turns[0].code.as_deref(), Some("E_TOOL_NOT_ALLOWED"));
}

#[tokio::test]
async fn read_call_without_a_jail_is_refused_unavailable() {
    let harness = harness(
        ScriptedExecutor::new(vec![read_call("call_no_jail", "note.txt", 0)]),
        None,
    );
    let (run, collected) = drive(&harness, "read without workspace").await;
    assert_eq!(collected.final_status["status"], "completed");
    assert_eq!(collected.tool_results.len(), 1);
    assert_eq!(collected.tool_results[0]["outcome"], "refused");
    assert_eq!(collected.tool_results[0]["code"], "E_FILE_TREE_UNAVAILABLE");
    let turns = harness.db.tool_turns_for_run(run.id).expect("tool turns");
    assert_eq!(turns.len(), 1);
    assert_eq!(turns[0].outcome, ToolOutcome::Refused);
    assert_eq!(
        harness.db.run_tool_counters(run.id).expect("counters"),
        (1, 1),
        "the refused attempt is counted"
    );
}

#[tokio::test]
async fn per_run_call_budget_refuses_calls_beyond_the_cap() {
    let workspace = tempfile::tempdir().expect("workspace");
    std::fs::write(workspace.path().join("note.txt"), NOTE_TEXT.as_bytes()).expect("write note");
    let calls: Vec<AssembledToolCall> = (0..=MAX_CALLS_PER_RUN)
        .map(|index| read_call(&format!("call_{index}"), "note.txt", index))
        .collect();
    let harness = harness(ScriptedExecutor::new(calls), Some(workspace.path()));

    let (run, collected) = drive(&harness, "read many times").await;
    assert_eq!(collected.final_status["status"], "completed");
    assert_eq!(
        collected.tool_calls.len(),
        MAX_CALLS_PER_RUN as usize,
        "only within-budget calls are announced"
    );
    assert_eq!(collected.tool_results.len(), (MAX_CALLS_PER_RUN + 1) as usize);
    let executed = collected
        .tool_results
        .iter()
        .filter(|result| result["outcome"] == "ok")
        .count();
    let refused = collected
        .tool_results
        .iter()
        .filter(|result| result["code"] == "E_CALL_LIMIT")
        .count();
    assert_eq!(
        executed,
        MAX_CALLS_PER_RUN as usize,
        "the cap allows exactly MAX_CALLS_PER_RUN reads"
    );
    assert_eq!(refused, 1, "the over-budget call is refused by name");
    assert_eq!(
        harness.db.run_tool_counters(run.id).expect("counters"),
        (1, MAX_CALLS_PER_RUN + 1),
        "every attempted call is counted for the next turn"
    );
    assert_eq!(
        harness.db.tool_turns_for_run(run.id).expect("turns").len(),
        (MAX_CALLS_PER_RUN + 1) as usize
    );
}

#[tokio::test]
async fn follow_up_turn_finishes_after_read_only_result() {
    let workspace = tempfile::tempdir().expect("workspace");
    std::fs::write(workspace.path().join("note.txt"), NOTE_TEXT.as_bytes()).expect("write note");
    let harness = harness(
        ScriptedExecutor::with_follow_ups(
            vec![read_call("call_1", "note.txt", 0)],
            vec![final_answer()],
        ),
        Some(workspace.path()),
    );

    let (run, collected) = drive(&harness, "read then answer").await;
    assert_eq!(collected.final_status["status"], "completed");
    assert_eq!(collected.tool_calls.len(), 1);
    assert_eq!(collected.tool_results.len(), 1);
    assert_eq!(
        collected.reply_deltas,
        vec!["final answer".to_owned()],
        "only the follow-up turn produces the visible final reply"
    );
    assert_eq!(harness.db.run_tool_counters(run.id).expect("counters"), (1, 1));
    let turns = harness.db.tool_turns_for_run(run.id).expect("turns");
    assert_eq!(turns.len(), 1);
    assert_eq!(turns[0].turn, 1);

    let store = RunStore::new(harness.db.clone());
    let messages = store.messages(run.id).expect("messages");
    assert_eq!(messages.len(), 2, "user + exactly one assistant message");
    assert_eq!(messages[1].text, "final answer");
}

#[tokio::test]
async fn second_tool_turn_executes_then_finishes() {
    let workspace = tempfile::tempdir().expect("workspace");
    std::fs::write(workspace.path().join("note.txt"), NOTE_TEXT.as_bytes()).expect("write note");
    let executor = ScriptedExecutor::with_follow_ups(
        vec![read_call("call_1", "note.txt", 0)],
        vec![
            tool_only_reply(vec![read_call("call_2", "note.txt", 0)]),
            final_answer(),
        ],
    );
    let harness = harness(executor.clone(), Some(workspace.path()));

    let (run, collected) = drive(&harness, "read twice").await;
    assert_eq!(collected.final_status["status"], "completed");
    assert_eq!(collected.tool_calls.len(), 2);
    assert_eq!(collected.tool_results.len(), 2);
    assert_eq!(collected.reply_deltas, vec!["final answer".to_owned()]);
    assert_eq!(harness.db.run_tool_counters(run.id).expect("counters"), (2, 2));
    let turns = harness.db.tool_turns_for_run(run.id).expect("turns");
    assert_eq!(turns.len(), 2);
    assert_eq!(turns[0].turn, 1);
    assert_eq!(turns[1].turn, 2);

    // The continuation transcript is cumulative: turn 2 must still see turn 1.
    let (lengths, result_counts) = executor.transcript_observation();
    assert_eq!(lengths, vec![1, 2], "cumulative transcript per continuation");
    assert_eq!(
        result_counts,
        vec![1, 2],
        "second continuation carries both bounded results"
    );

    let store = RunStore::new(harness.db.clone());
    let messages = store.messages(run.id).expect("messages");
    assert_eq!(messages.len(), 2);
    assert_eq!(messages[1].text, "final answer");
}

#[tokio::test]
async fn persisted_turn_cap_fails_honestly_instead_of_fake_completion() {
    let workspace = tempfile::tempdir().expect("workspace");
    std::fs::write(workspace.path().join("note.txt"), NOTE_TEXT.as_bytes()).expect("write note");
    let mut follow_ups = Vec::new();
    for index in 0..MAX_TOOL_TURNS {
        follow_ups.push(tool_only_reply(vec![read_call(
            &format!("call_{}", index + 1),
            "note.txt",
            0,
        )]));
    }
    let harness = harness(
        ScriptedExecutor::with_follow_ups(
            vec![read_call("call_0", "note.txt", 0)],
            follow_ups,
        ),
        Some(workspace.path()),
    );

    let (run, collected) = drive(&harness, "keep calling tools").await;
    assert_eq!(collected.final_status["status"], "failed");
    assert_eq!(
        collected.final_status["error_code"],
        "E_CALL_LIMIT",
        "the turn cap is a named call refusal, never a fake completion or money refusal"
    );
    assert_eq!(
        harness.db.run_tool_counters(run.id).expect("counters"),
        (MAX_TOOL_TURNS, MAX_TOOL_TURNS),
        "all completed tool turns"
    );
    assert_eq!(
        harness.db.tool_turns_for_run(run.id).expect("turns").len(),
        (MAX_TOOL_TURNS + 1) as usize,
        "completed turns + the cap-refused call row"
    );
    assert!(
        collected.reply_deltas.is_empty(),
        "no final reply text was invented"
    );
    let store = RunStore::new(harness.db.clone());
    assert_eq!(
        store.messages(run.id).expect("messages").len(),
        1,
        "no assistant message is persisted for a failed tool loop"
    );
}

#[tokio::test]
async fn streaming_follow_up_does_not_duplicate_deltas() {
    let workspace = tempfile::tempdir().expect("workspace");
    std::fs::write(workspace.path().join("note.txt"), NOTE_TEXT.as_bytes()).expect("write note");
    let harness = harness(
        ScriptedExecutor::with_streaming_follow_up(
            vec![read_call("call_1", "note.txt", 0)],
            vec![final_answer()],
        ),
        Some(workspace.path()),
    );

    let (run, collected) = drive(&harness, "stream the final answer").await;
    assert_eq!(collected.final_status["status"], "completed");
    assert_eq!(
        collected.reply_deltas,
        vec!["final answer".to_owned()],
        "the sink published the fragment once and the returned delta was not replayed"
    );
    let store = RunStore::new(harness.db.clone());
    let messages = store.messages(run.id).expect("messages");
    assert_eq!(messages.len(), 2);
    assert_eq!(messages[1].text, "final answer");
}

#[tokio::test]
async fn exhausted_call_budget_fails_with_call_limit() {
    let workspace = tempfile::tempdir().expect("workspace");
    std::fs::write(workspace.path().join("note.txt"), NOTE_TEXT.as_bytes()).expect("write note");
    let initial_calls: Vec<AssembledToolCall> = (0..MAX_CALLS_PER_RUN)
        .map(|index| read_call(&format!("call_{index}"), "note.txt", index))
        .collect();
    let harness = harness(
        ScriptedExecutor::with_follow_ups(
            initial_calls,
            vec![tool_only_reply(vec![read_call("call_extra", "note.txt", 0)])],
        ),
        Some(workspace.path()),
    );

    let (run, collected) = drive(&harness, "exhaust the call budget").await;
    assert_eq!(collected.final_status["status"], "failed");
    assert_eq!(
        collected.final_status["error_code"],
        "E_CALL_LIMIT",
        "an all-budget-refused turn is a call limit, not a malformed provider reply"
    );
    assert_eq!(
        harness.db.run_tool_counters(run.id).expect("counters"),
        (2, MAX_CALLS_PER_RUN + 1),
        "all allowed calls in turn 1 + the refused extra attempt in turn 2"
    );
    assert_eq!(
        harness.db.tool_turns_for_run(run.id).expect("turns").len(),
        (MAX_CALLS_PER_RUN + 1) as usize
    );
    let store = RunStore::new(harness.db.clone());
    assert_eq!(
        store.messages(run.id).expect("messages").len(),
        1,
        "no assistant message is persisted for a failed tool loop"
    );
}

#[tokio::test]
async fn continuation_refusal_fails_the_task_by_name() {
    let workspace = tempfile::tempdir().expect("workspace");
    std::fs::write(workspace.path().join("note.txt"), NOTE_TEXT.as_bytes()).expect("write note");
    let harness = harness(
        ScriptedExecutor::with_failing_continuation(vec![read_call("call_1", "note.txt", 0)]),
        Some(workspace.path()),
    );

    let (run, collected) = drive(&harness, "read then fail").await;
    assert_eq!(collected.final_status["status"], "failed");
    assert_eq!(
        collected.final_status["error_code"],
        "E_PROVIDER_INVALID_RESPONSE",
        "the continuation refusal is reported by name"
    );
    assert_eq!(harness.db.run_tool_counters(run.id).expect("counters"), (1, 1));
    assert_eq!(
        harness.db.tool_turns_for_run(run.id).expect("turns").len(),
        1
    );

    // The task must be terminal (Failed), not left Running while the run fails.
    let store = RunStore::new(harness.db.clone());
    let tasks = store.tasks_for_run(run.id).expect("tasks");
    assert!(
        tasks.iter().any(|task| task.state == AgentState::Failed),
        "the executing task must be marked Failed: {tasks:?}"
    );
    assert_eq!(
        store.messages(run.id).expect("messages").len(),
        1,
        "no assistant message is persisted for a failed tool loop"
    );
}

