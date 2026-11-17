//! Live-defect regression: a doomed tool call must be refused **before** it
//! creates an approval card.
//!
//! Live evidence (run `01a1eb5a-3b83-7583-8328-95c5a2128263`, live DB
//! `C:\Users\Riley\AppData\Local\HiveMind\hiverouter.db`):
//!
//! - approval 1: `shell_exec { program: "sha256sum", args: ["live-approval-2.md"] }`
//!   `decision = allow_once`, `executed_at = NULL`, tool turn `refused
//!   E_TOOL_NOT_ALLOWED`;
//! - approval 2: `shell_exec { program: "sh", args: ["-l", "-c", "..."] }`
//!   `decision = allow_once`, `executed_at = NULL`, tool turn `refused
//!   E_TOOL_NOT_ALLOWED`.
//!
//! The operator approved two cards and neither tool ever executed: the
//! approval was committed in `RunEngine::suspend_for_approval` before the
//! shell-policy / registry / jail preflight ran. `sha256sum` is not on the
//! shell allowlist and `sh` is a command-interpreter host, so both are refused
//! by `preflight_side_effect` — after consent was already spent.
//!
//! The invariant pinned here is falsifiable and exact: **a tool call that the
//! executor preflight refuses produces zero approval rows and a named refusal
//! (`E_TOOL_NOT_ALLOWED`) in the tool turn.** The test approves any card it
//! sees, so on the pre-fix tree it fails on the zero-approval assertions (the
//! cards are real); on the fixed tree no card is ever created.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use hiverouter_contract::command::ApprovalResolve;
use hiverouter_contract::event::ApprovalRequest as ApprovalRequestEvent;
use hiverouter_contract::{PermissionPolicy, Refusal, RunId, TaskId, ToolOutcome, Usage};
use hiverouter_core::bus::{EventBus, SequencedEvent};
use hiverouter_core::db::Database;
use hiverouter_core::entities::{Run, TaskRecord};
use hiverouter_core::execution::{
    DeltaSink, EscalationAnswer, ProviderExecutor, RunEngine, StreamedReply, ToolTurnTranscript,
};
use hiverouter_core::graph::GraphAuthority;
use hiverouter_core::planner::{Plan, PlanNode};
use hiverouter_core::prompts::TemplateRegistry;
use hiverouter_core::tools_bridge::ToolsBridge;
use hiverouter_provider::tool_assembler::AssembledToolCall;
use hiverouter_tools::shell::ShellPolicy;
use hiverouter_tools::{AgentTier, ToolRegistry, WorkspaceJail};
use tokio::sync::broadcast::error::RecvError;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

/* ------------------------------------------------------------------ */
/* Scripted provider (test double only: no network, no key)            */
/* ------------------------------------------------------------------ */

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

struct ScriptedExecutor {
    plan: Plan,
    replies: Mutex<VecDeque<StreamedReply>>,
}

impl ScriptedExecutor {
    fn new(replies: Vec<StreamedReply>) -> Arc<Self> {
        Arc::new(Self {
            plan: leaf_plan(),
            replies: Mutex::new(replies.into()),
        })
    }

    fn pop(&self) -> StreamedReply {
        self.replies
            .lock()
            .expect("scripted replies lock")
            .pop_front()
            .unwrap_or_else(final_answer)
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
        on_delta: &DeltaSink,
    ) -> Result<StreamedReply, Refusal> {
        let reply = self.pop();
        for delta in &reply.deltas {
            on_delta(delta);
        }
        Ok(reply)
    }

    async fn continue_with_tools(
        &self,
        _run_id: RunId,
        _task: &TaskRecord,
        _prompt: &str,
        _cancel: &CancellationToken,
        on_delta: &DeltaSink,
        _transcript: &[ToolTurnTranscript],
    ) -> Result<StreamedReply, Refusal> {
        let reply = self.pop();
        for delta in &reply.deltas {
            on_delta(delta);
        }
        Ok(reply)
    }

    async fn answer_escalation(
        &self,
        _run_id: RunId,
        _parent: &TaskRecord,
        _run_prompt: &str,
        _question: &str,
        _parent_context: Option<&hiverouter_core::context::ScopedContext>,
        _cancel: &CancellationToken,
    ) -> Result<EscalationAnswer, Refusal> {
        Err(Refusal::ProviderInvalidResponse)
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

fn tool_only_reply(calls: Vec<AssembledToolCall>) -> StreamedReply {
    StreamedReply {
        deltas: Vec::new(),
        usage: Usage::default(),
        tool_calls: calls,
        reasoning: String::new(),
    }
}

fn shell_call(
    tool_call_id: &str,
    program: &str,
    args: &[&str],
    index: u32,
) -> AssembledToolCall {
    AssembledToolCall {
        index,
        id: tool_call_id.to_owned(),
        name: "shell_exec".to_owned(),
        arguments: serde_json::json!({
            "program": program,
            "args": args,
        }),
    }
}

fn write_call(tool_call_id: &str, path: &str, content: &str, index: u32) -> AssembledToolCall {
    AssembledToolCall {
        index,
        id: tool_call_id.to_owned(),
        name: "file_write".to_owned(),
        arguments: serde_json::json!({ "path": path, "content": content }),
    }
}

fn run_tests_call(tool_call_id: &str, index: u32) -> AssembledToolCall {
    AssembledToolCall {
        index,
        id: tool_call_id.to_owned(),
        name: "run_tests".to_owned(),
        arguments: serde_json::json!({ "runner": "cargo", "args": [] }),
    }
}

/* ------------------------------------------------------------------ */
/* Harness                                                             */
/* ------------------------------------------------------------------ */

struct Harness {
    engine: Arc<RunEngine>,
    bridge: Arc<ToolsBridge>,
    bus: Arc<EventBus>,
    db: Arc<Database>,
    workspace: tempfile::TempDir,
    _data: tempfile::TempDir,
}

fn harness(executor: Arc<dyn ProviderExecutor>) -> Harness {
    let data = tempfile::tempdir().expect("data tempdir");
    let workspace = tempfile::tempdir().expect("workspace tempdir");
    let db = Arc::new(Database::open(&data.path().join("hiverouter.db")).expect("open db"));
    db.migrate().expect("migrate core tables");
    ToolsBridge::migrate(&db).expect("migrate tools tables");

    let bridge = Arc::new(
        ToolsBridge::new(workspace.path(), db.clone())
            .with_data_dir(data.path())
            .with_approval_ttl(120),
    );
    let bus = Arc::new(EventBus::new());
    let engine = RunEngine::new(
        db.clone(),
        bus.clone(),
        Arc::new(GraphAuthority::new()),
        executor,
        Arc::new(TemplateRegistry::default()),
    )
    .with_tool_workspace(WorkspaceJail::open(workspace.path()).expect("workspace jail"))
    // Production policy: only git.exe / cargo.exe with their pinned argument
    // shapes. `sha256sum` is not on it and `sh` is an interpreter host.
    .with_shell_policy(ShellPolicy::conservative_default())
    .with_tools(bridge.clone());

    Harness {
        engine: Arc::new(engine),
        bridge,
        bus,
        db,
        workspace,
        _data: data,
    }
}

impl Harness {
    fn workspace_path(&self, relative: &str) -> std::path::PathBuf {
        self.workspace.path().join(relative)
    }

    fn tool_turns(&self, run: RunId) -> Vec<hiverouter_core::db::ToolTurnRecord> {
        self.db.tool_turns_for_run(run).expect("tool turns")
    }

    /// Every approval row for a run, including `policy:*` auto-grants (the
    /// bridge counter excludes those, so the raw table is the stricter pin).
    fn approval_rows(&self, run: RunId) -> u64 {
        self.db
            .with_connection(|connection| {
                let count: i64 = connection.query_row(
                    "SELECT COUNT(*) FROM approvals WHERE run_id = ?1",
                    rusqlite::params![run.to_string()],
                    |row| row.get(0),
                )?;
                Ok(count)
            })
            .map(|count| u64::try_from(count).expect("non-negative approval count"))
            .expect("approval row count")
    }

    fn resolve(&self, request: &ApprovalRequestEvent, decision: &str) -> bool {
        let nonce = request
            .nonce
            .clone()
            .expect("APPROVAL_REQUEST must carry a nonce");
        let payload = ApprovalResolve {
            approval_id: request.approval_id,
            nonce,
            decision: decision.to_owned(),
            reason: None,
        };
        let resolution = self
            .bridge
            .resolve_approval(&payload)
            .expect("committed decision");
        self.engine.notify_approval_resolved(&resolution)
    }

    /// Submits a prompt and settles the run, answering every approval card it
    /// sees with `on_approval` (`approve` / `deny`). Answering cards is what
    /// makes the pre-fix tree observable instead of hanging on the TTL; on the
    /// fixed tree the doomed-command tests never see one.
    async fn drive(
        &self,
        prompt: &str,
        policy: PermissionPolicy,
        on_approval: &str,
    ) -> (Run, Collected) {
        let mut receiver = self.bus.subscribe();
        let run = self
            .engine
            .submit_prompt_scoped(
                prompt,
                "project-approval-preflight",
                None,
                Some(policy),
                Uuid::now_v7(),
            )
            .expect("submit prompt");
        let mut collected = Collected::default();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        while collected.final_status.is_none() {
            let event = match tokio::time::timeout_at(deadline, receiver.recv()).await {
                Ok(Ok(event)) => event,
                Ok(Err(RecvError::Lagged(_))) => continue,
                Ok(Err(RecvError::Closed)) => panic!("engine event bus closed"),
                Err(_) => panic!(
                    "run did not settle within 30s; approval cards seen: {}",
                    collected.approval_requests.len()
                ),
            };
            collected.push(&event);
            if event.envelope.name == "APPROVAL_REQUEST" {
                let request: ApprovalRequestEvent =
                    serde_json::from_value(event.envelope.payload.clone())
                        .expect("approval payload");
                collected.approval_requests.push(request.clone());
                assert!(
                    self.resolve(&request, on_approval),
                    "approval decision {on_approval:?} must wake the engine exactly once"
                );
            }
        }
        (run, collected)
    }
}

#[derive(Default)]
struct Collected {
    outcomes: Vec<String>,
    codes: Vec<String>,
    statuses: Vec<String>,
    final_status: Option<String>,
    terminal_error_code: Option<String>,
    approval_requests: Vec<ApprovalRequestEvent>,
}

impl Collected {
    fn push(&mut self, event: &SequencedEvent) {
        match event.envelope.name.as_str() {
            "TOOL_RESULT" => {
                self.outcomes.push(
                    event.envelope.payload["outcome"]
                        .as_str()
                        .unwrap_or_default()
                        .to_owned(),
                );
                self.codes.push(
                    event.envelope.payload["code"]
                        .as_str()
                        .unwrap_or_default()
                        .to_owned(),
                );
            }
            "RUN_STATUS" => {
                let status = event.envelope.payload["status"]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned();
                self.statuses.push(status.clone());
                if matches!(
                    status.as_str(),
                    "completed" | "failed" | "cancelled" | "killed" | "interrupted"
                ) {
                    self.terminal_error_code = event.envelope.payload["error_code"]
                        .as_str()
                        .map(str::to_owned);
                    self.final_status = Some(status);
                }
            }
            _ => {}
        }
    }
}

/* ------------------------------------------------------------------ */
/* 1. The live defect: zero approval rows, named refusal before consent*/
/* ------------------------------------------------------------------ */

#[tokio::test]
async fn disallowed_shell_commands_never_create_an_approval_card() {
    // Exactly the two live commands from run 01a1eb5a: an unlisted program and
    // an interpreter host. Scripted across two tool turns, as they were live.
    let harness = harness(ScriptedExecutor::new(vec![
        tool_only_reply(vec![shell_call(
            "call-live-sha256sum",
            "sha256sum",
            &["live-approval-2.md"],
            0,
        )]),
        tool_only_reply(vec![shell_call(
            "call-live-sh",
            "sh",
            &["-l", "-c", "wc -c < live-approval-2.md && od -An -tx1 live-approval-2.md"],
            0,
        )]),
        final_answer(),
    ]));

    // Live run policy was `trusted` (file_write auto-granted); shell_exec still
    // asks because it has no workspace `path`, so it hits the same seam.
    let (run, collected) = harness
        .drive(
            "replay the live approval defect",
            PermissionPolicy::Trusted,
            "approve",
        )
        .await;

    assert!(
        collected.approval_requests.is_empty(),
        "a shell command the executor preflight will refuse must not create an \
         approval card (live defect 01a1eb5a-3b83-7583-8328-95c5a2128263); \
         cards seen: {:?}",
        collected
            .approval_requests
            .iter()
            .map(|request| (&request.tool, request.args_preview.clone()))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        harness.bridge.approval_count_for_run(run.id).expect("count"),
        0,
        "pre-refused calls must not increment the approval wait count"
    );
    assert_eq!(
        harness.approval_rows(run.id),
        0,
        "no approval row (operator or policy) may be committed for a pre-refused call"
    );
    assert!(
        harness
            .bridge
            .pending_approvals(run.id)
            .expect("pending")
            .is_empty(),
        "no pending approval may survive a pre-refused call"
    );

    assert_eq!(
        collected.final_status.as_deref(),
        Some("completed"),
        "the run continues after a pre-refusal (a refusal is a tool result, not a run failure)"
    );
    assert_eq!(collected.terminal_error_code, None);
    assert_eq!(
        collected.codes,
        vec!["E_TOOL_NOT_ALLOWED".to_owned(), "E_TOOL_NOT_ALLOWED".to_owned()],
        "both calls are refused by name before consent; outcomes={:?}",
        collected.outcomes
    );

    let turns = harness.tool_turns(run.id);
    assert_eq!(turns.len(), 2, "one durable tool turn per refused call");
    for turn in &turns {
        assert_eq!(turn.tool, "shell_exec");
        assert_eq!(turn.outcome, ToolOutcome::Refused);
        assert_eq!(turn.code.as_deref(), Some("E_TOOL_NOT_ALLOWED"));
        // A pre-refusal has no result body: absent stays absent, never 0.
        assert_eq!(turn.bytes, None);
        assert_eq!(turn.sha256, None);
    }
    assert_eq!(turns[0].turn, 1);
    assert_eq!(turns[1].turn, 2);
    assert!(
        !harness.workspace_path("live-approval-2.md").exists(),
        "nothing was written by a pre-refused shell call"
    );
}

/// A schema-valid but currently-denied shell call is also pre-refused (the
/// preflight is the executor's, not a bespoke shell check): this pins the
/// second live program class, an interpreter host.
#[test]
fn preflight_names_the_refusal_for_the_two_live_commands() {
    let harness = harness(ScriptedExecutor::new(Vec::new()));
    let registry = ToolRegistry::standard();
    let policy = ShellPolicy::conservative_default();
    let tier = AgentTier::WorkerAgent;

    let sha = harness
        .bridge
        .preflight_model_tool_approval(
            &registry,
            &policy,
            tier,
            "shell_exec",
            &serde_json::json!({ "program": "sha256sum", "args": ["live-approval-2.md"] }),
        )
        .expect_err("unlisted program must be refused before approval");
    assert_eq!(sha.code(), "E_TOOL_NOT_ALLOWED");

    let sh = harness
        .bridge
        .preflight_model_tool_approval(
            &registry,
            &policy,
            tier,
            "shell_exec",
            &serde_json::json!({ "program": "sh", "args": ["-c", "true"] }),
        )
        .expect_err("interpreter host must be refused before approval");
    assert_eq!(sh.code(), "E_TOOL_NOT_ALLOWED");
}

/* ------------------------------------------------------------------ */
/* 2. Allowed behavior is unchanged                                    */
/* ------------------------------------------------------------------ */

#[tokio::test]
async fn allowed_file_write_still_asks_and_executes_once() {
    let harness = harness(ScriptedExecutor::new(vec![
        tool_only_reply(vec![write_call(
            "call-allowed-write",
            "approval-preflight-ok.txt",
            "PREFLIGHT-OK",
            0,
        )]),
        final_answer(),
    ]));

    let (run, collected) = harness
        .drive("write the allowed file", PermissionPolicy::Classic, "approve")
        .await;

    assert_eq!(
        collected.approval_requests.len(),
        1,
        "an allowed file_write still requires exactly one operator approval"
    );
    assert_eq!(collected.approval_requests[0].tool, "file_write");
    assert_eq!(
        harness.bridge.approval_count_for_run(run.id).expect("count"),
        1
    );
    assert_eq!(collected.outcomes, vec!["ok".to_owned()]);
    assert_eq!(
        std::fs::read_to_string(harness.workspace_path("approval-preflight-ok.txt"))
            .expect("written file"),
        "PREFLIGHT-OK"
    );
    let turns = harness.tool_turns(run.id);
    assert_eq!(turns.len(), 1);
    assert_eq!(turns[0].outcome, ToolOutcome::Ok);
    assert_eq!(turns[0].code, None);
}

#[tokio::test]
async fn allowed_shell_command_still_asks_and_executes_once() {
    let harness = harness(ScriptedExecutor::new(vec![
        tool_only_reply(vec![shell_call(
            "call-allowed-shell",
            "git.exe",
            &["--version"],
            0,
        )]),
        final_answer(),
    ]));

    let (run, collected) = harness
        .drive("run the allowed shell", PermissionPolicy::Classic, "approve")
        .await;

    assert_eq!(
        collected.approval_requests.len(),
        1,
        "an allowlisted shell command still requires exactly one operator approval"
    );
    assert_eq!(collected.approval_requests[0].tool, "shell_exec");
    assert_eq!(
        harness.bridge.approval_count_for_run(run.id).expect("count"),
        1
    );
    assert_eq!(collected.outcomes, vec!["ok".to_owned()]);
    assert_eq!(collected.codes, vec![String::new()]);
}

#[tokio::test]
async fn allowed_run_tests_still_asks_before_the_suite_runs() {
    let harness = harness(ScriptedExecutor::new(vec![
        tool_only_reply(vec![run_tests_call("call-allowed-tests", 0)]),
        final_answer(),
    ]));

    // Deny the card so the assertion is about the *pre-approval* gate, not about
    // actually compiling a suite inside an empty temp workspace.
    let (run, collected) = harness
        .drive("run the allowed suite", PermissionPolicy::Classic, "deny")
        .await;

    assert_eq!(
        collected.approval_requests.len(),
        1,
        "a valid run_tests call still reaches consent instead of being pre-refused"
    );
    assert_eq!(collected.approval_requests[0].tool, "run_tests");
    assert_eq!(
        harness.bridge.approval_count_for_run(run.id).expect("count"),
        1
    );
    assert_eq!(
        collected.outcomes,
        vec!["denied".to_owned()],
        "the operator denial is reported truthfully"
    );
    assert_eq!(collected.codes, vec![String::new()]);
    let turns = harness.tool_turns(run.id);
    assert_eq!(turns.len(), 1);
    assert_eq!(turns[0].outcome, ToolOutcome::Denied);
}
