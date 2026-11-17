//! Usage/claim truth pins (TRUTHSEEKER seat).
//!
//! Scope: reconcile the persisted `run_usage` row, provider-call accounting,
//! `GET /api/usage/runs/{id}`, `GET /api/runs/{id}/detail`, and null-vs-zero /
//! unpriced / capped-run / scripted-run provenance.
//!
//! Every test name states the invariant. `red_` marks a pin that was RED when
//! this audit wrote it: it asserts the *honest* contract and each failure names
//! the source file that owns the fix. After the routed fixes most of these pins
//! are GREEN; re-run the suite for the current state (at the time of writing one
//! remains RED: `spent_usd()` still reports 0.00 for unknown spend).
//! Non-`red_` tests are pins for behaviour that was already honest.
//!
//! Invariants under test:
//!   I1  An absent provider counter stays absent end-to-end; it is never a 0.
//!   I2  A total over a call whose counter was omitted is INCOMPLETE, never a
//!       number (usage.rs V2 / accounting.rs V1-V6).
//!   I3  An unsettled/unknown spend is never exposed as a numeric 0.
//!   I4  The usage route states whether the row is final/complete and how many
//!       provider calls produced it, so tokens can be reconciled with calls.
//!   I5  An external (points) endpoint reports measured tokens with NO USD
//!       settlement (`UNPRICED`, `settled_usd: null`).
//!   I6  A run refused before any provider send reports `usage: null`, never a
//!       measured-looking 0 row.
//!   I7  A run that is refused at a cap keeps the usage of its completed calls.
//!   I8  A scripted/unconfigured executor cannot persist measured-looking usage.
//!   I9  Every served provider call (including tool-call turns) contributes its
//!       measured usage to the run total exactly once.
//!
//! Live-store evidence (read-only, 2026-11-17) is reported in this seat's
//! message to the parent; it is deliberately not copied into a docs file (this
//! seat owns no documentation path). The tests below are deterministic and
//! never touch the live DB or the network.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use hiverouter_contract::{AgentState, Refusal, Usage};
use hiverouter_core::bus::{EventBus, SequencedEvent};
use hiverouter_core::config::Config;
use hiverouter_core::db::Database;
use hiverouter_core::entities::TaskRecord;
use hiverouter_core::execution::{
    DeltaSink, ProviderExecutor, RunEngine, StreamedReply, UnconfiguredExecutor,
};
use hiverouter_core::graph::GraphAuthority;
use hiverouter_core::http::{self, AppState};
use hiverouter_core::planner::{Plan, PlanNode};
use hiverouter_core::prompts::TemplateRegistry;
use hiverouter_core::run_checks;
use hiverouter_core::store::RunStore;
use hiverouter_provider::accounting::RunAccounting;
use hiverouter_provider::budget::{BudgetLimits, RunBudget};
use hiverouter_provider::pricing::PricingTable;
use hiverouter_provider::tool_assembler::AssembledToolCall;
use hiverouter_tools::WorkspaceJail;
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Shared fixtures
// ---------------------------------------------------------------------------

fn plan_with(nodes: usize, depth: u8) -> Plan {
    let nodes = (0..nodes)
        .map(|_| PlanNode {
            objective: None,
            node_id: hiverouter_contract::TaskId::new(),
            // A HyperAgent may only spawn branch leads; a deep leaf plan would
            // be refused by the spawn caps before any provider call.
            role: "super".to_owned(),
            deps: Vec::new(),
            depth,
            parent: None,
        })
        .collect();
    Plan::new(nodes)
}

/// A fully measured provider report (the shape a DeepSeek completion returns).
fn measured(prompt: u64, completion: u64, reasoning: Option<u64>) -> Usage {
    Usage {
        prompt_tokens: Some(prompt),
        completion_tokens: Some(completion),
        total_tokens: Some(prompt.saturating_add(completion)),
        prompt_cache_hit_tokens: Some(prompt / 2),
        prompt_cache_miss_tokens: Some(prompt - prompt / 2),
        reasoning_tokens: reasoning,
    }
}

struct ScriptedUsageExecutor {
    plan: Plan,
    planner_usage: Usage,
    leaf_usage: Usage,
    pending: Mutex<Usage>,
}

impl ScriptedUsageExecutor {
    fn new(plan: Plan, planner_usage: Usage, leaf_usage: Usage) -> Arc<Self> {
        Arc::new(Self {
            plan,
            planner_usage,
            leaf_usage,
            pending: Mutex::new(Usage::default()),
        })
    }
}

#[async_trait]
impl ProviderExecutor for ScriptedUsageExecutor {
    async fn plan(
        &self,
        _run_id: hiverouter_contract::RunId,
        _prompt: &str,
        _cancel: &CancellationToken,
    ) -> Result<Plan, Refusal> {
        *self.pending.lock().expect("pending usage lock") = self.planner_usage;
        Ok(self.plan.clone())
    }

    async fn execute(
        &self,
        _run_id: hiverouter_contract::RunId,
        _task: &TaskRecord,
        _prompt: &str,
        _cancel: &CancellationToken,
        on_delta: &DeltaSink,
    ) -> Result<StreamedReply, Refusal> {
        on_delta("done");
        Ok(StreamedReply {
            deltas: vec!["done".to_owned()],
            usage: self.leaf_usage,
            tool_calls: Vec::new(),
            reasoning: String::new(),
        })
    }

    fn take_pending_usage(&self) -> Usage {
        std::mem::take(&mut *self.pending.lock().expect("pending usage lock"))
    }
}

/// A leaf that issues one read tool per turn for two turns and then answers.
/// Every reply carries distinct measured usage, so the persisted run total can
/// be compared with the exact number of provider calls actually served.
struct ToolLoopUsageExecutor {
    plan: Plan,
    planner_usage: Usage,
    pending: Mutex<Usage>,
    served: std::sync::atomic::AtomicUsize,
    continuation: Mutex<u8>,
}

impl ToolLoopUsageExecutor {
    fn new(plan: Plan, planner_usage: Usage) -> Arc<Self> {
        Arc::new(Self {
            plan,
            planner_usage,
            pending: Mutex::new(Usage::default()),
            served: std::sync::atomic::AtomicUsize::new(0),
            continuation: Mutex::new(0),
        })
    }

    fn calls_served(&self) -> usize {
        self.served.load(std::sync::atomic::Ordering::SeqCst)
    }
}

fn read_call(id: &str, path: &str) -> AssembledToolCall {
    AssembledToolCall {
        index: 0,
        id: id.to_owned(),
        name: "file_read".to_owned(),
        arguments: serde_json::json!({ "path": path }),
    }
}

#[async_trait]
impl ProviderExecutor for ToolLoopUsageExecutor {
    async fn plan(
        &self,
        _run_id: hiverouter_contract::RunId,
        _prompt: &str,
        _cancel: &CancellationToken,
    ) -> Result<Plan, Refusal> {
        self.served
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        *self.pending.lock().expect("pending usage lock") = self.planner_usage;
        Ok(self.plan.clone())
    }

    async fn execute(
        &self,
        _run_id: hiverouter_contract::RunId,
        _task: &TaskRecord,
        _prompt: &str,
        _cancel: &CancellationToken,
        on_delta: &DeltaSink,
    ) -> Result<StreamedReply, Refusal> {
        self.served
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        on_delta("reading");
        // Call #2 of 4: measured(10, 1, Some(1)), one read tool call.
        Ok(StreamedReply {
            deltas: vec!["reading".to_owned()],
            usage: measured(10, 1, Some(1)),
            tool_calls: vec![read_call("call_read_1", "note.txt")],
            reasoning: String::new(),
        })
    }

    async fn continue_with_tools(
        &self,
        _run_id: hiverouter_contract::RunId,
        _task: &TaskRecord,
        _prompt: &str,
        _cancel: &CancellationToken,
        on_delta: &DeltaSink,
        _transcript: &[hiverouter_core::execution::ToolTurnTranscript],
    ) -> Result<StreamedReply, Refusal> {
        self.served
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let mut turn = self.continuation.lock().expect("continuation lock");
        *turn += 1;
        if *turn == 1 {
            // Call #3 of 4: measured(20, 2, Some(2)), one more read.
            on_delta("reading again");
            Ok(StreamedReply {
                deltas: vec!["reading again".to_owned()],
                usage: measured(20, 2, Some(2)),
                tool_calls: vec![read_call("call_read_2", "note.txt")],
                reasoning: String::new(),
            })
        } else {
            // Call #4 of 4: the final answer, measured(30, 3, Some(3)).
            on_delta("final answer");
            Ok(StreamedReply {
                deltas: vec!["final answer".to_owned()],
                usage: measured(30, 3, Some(3)),
                tool_calls: Vec::new(),
                reasoning: String::new(),
            })
        }
    }

    fn take_pending_usage(&self) -> Usage {
        std::mem::take(&mut *self.pending.lock().expect("pending usage lock"))
    }
}

/// A leaf that ends immediately with the `finish` tool. This is the terminal
/// path where the engine copies the tool-call reply's usage into the terminal
/// reply, so an unconditional per-turn add plus `complete_task`'s add would
/// count the same served call twice.
struct FinishUsageExecutor {
    plan: Plan,
    planner_usage: Usage,
    pending: Mutex<Usage>,
    served: std::sync::atomic::AtomicUsize,
}

impl FinishUsageExecutor {
    fn new(plan: Plan, planner_usage: Usage) -> Arc<Self> {
        Arc::new(Self {
            plan,
            planner_usage,
            pending: Mutex::new(Usage::default()),
            served: std::sync::atomic::AtomicUsize::new(0),
        })
    }

    fn calls_served(&self) -> usize {
        self.served.load(std::sync::atomic::Ordering::SeqCst)
    }
}

#[async_trait]
impl ProviderExecutor for FinishUsageExecutor {
    async fn plan(
        &self,
        _run_id: hiverouter_contract::RunId,
        _prompt: &str,
        _cancel: &CancellationToken,
    ) -> Result<Plan, Refusal> {
        self.served
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        *self.pending.lock().expect("pending usage lock") = self.planner_usage;
        Ok(self.plan.clone())
    }

    async fn execute(
        &self,
        _run_id: hiverouter_contract::RunId,
        _task: &TaskRecord,
        _prompt: &str,
        _cancel: &CancellationToken,
        on_delta: &DeltaSink,
    ) -> Result<StreamedReply, Refusal> {
        self.served
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        on_delta("finished");
        Ok(StreamedReply {
            deltas: vec!["finished".to_owned()],
            usage: measured(10, 1, Some(1)),
            tool_calls: vec![AssembledToolCall {
                index: 0,
                id: "call_finish".to_owned(),
                name: "finish".to_owned(),
                arguments: serde_json::json!({ "verdict": "done" }),
            }],
            reasoning: String::new(),
        })
    }

    fn take_pending_usage(&self) -> Usage {
        std::mem::take(&mut *self.pending.lock().expect("pending usage lock"))
    }
}

struct EngineHarness {
    engine: Arc<RunEngine>,
    bus: Arc<EventBus>,
    store: RunStore,
    _db: Arc<Database>,
    _temp: tempfile::TempDir,
}

fn engine_harness(executor: Arc<dyn ProviderExecutor>) -> EngineHarness {
    engine_harness_inner(executor, None)
}

fn engine_harness_with_workspace(
    executor: Arc<dyn ProviderExecutor>,
    workspace: &std::path::Path,
) -> EngineHarness {
    engine_harness_inner(executor, Some(workspace))
}

fn engine_harness_inner(
    executor: Arc<dyn ProviderExecutor>,
    workspace: Option<&std::path::Path>,
) -> EngineHarness {
    let temp = tempfile::tempdir().expect("temp dir");
    let db = Arc::new(Database::open(&temp.path().join("hiverouter.db")).expect("db"));
    db.migrate().expect("migrate");
    let bus = Arc::new(EventBus::new());
    let mut engine = RunEngine::new(
        db.clone(),
        bus.clone(),
        Arc::new(GraphAuthority::new()),
        executor,
        Arc::new(TemplateRegistry::default()),
    );
    if let Some(workspace) = workspace {
        engine = engine.with_tool_workspace(WorkspaceJail::open(workspace).expect("workspace jail"));
    }
    EngineHarness {
        engine: Arc::new(engine),
        bus,
        store: RunStore::new(db.clone()),
        _db: db,
        _temp: temp,
    }
}

async fn wait_for(
    receiver: &mut tokio::sync::broadcast::Receiver<SequencedEvent>,
    predicate: impl Fn(&SequencedEvent) -> bool,
) -> SequencedEvent {
    for _ in 0..200 {
        match tokio::time::timeout(Duration::from_secs(10), receiver.recv()).await {
            Ok(Ok(event)) => {
                if predicate(&event) {
                    return event;
                }
            }
            Ok(Err(RecvError::Lagged(_))) => continue,
            Ok(Err(RecvError::Closed)) => panic!("event bus closed"),
            Err(_) => panic!("timed out waiting for the expected event"),
        }
    }
    panic!("expected event did not arrive within 200 frames");
}

async fn terminal_usage(
    harness: &EngineHarness,
    run_id: hiverouter_contract::RunId,
) -> Option<Usage> {
    for _ in 0..100 {
        if let Some(usage) = harness.store.run_usage(run_id).expect("usage read") {
            return Some(usage);
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    harness.store.run_usage(run_id).expect("usage read")
}

struct TestServer {
    addr: SocketAddr,
    state: Arc<AppState>,
    shutdown: Option<oneshot::Sender<()>>,
    handle: tokio::task::JoinHandle<()>,
}

impl TestServer {
    async fn start(temp: &tempfile::TempDir) -> Self {
        let config = Config::resolve(
            "127.0.0.1:0",
            temp.path().to_str().expect("utf8 path"),
            None,
        )
        .expect("test config");
        let database = Database::open(config.db_file()).expect("database");
        database.migrate().expect("migrations");
        let state = Arc::new(AppState::new(
            Arc::new(database),
            run_checks(),
            Arc::new(config),
        ));
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local addr");
        let (shutdown, shutdown_rx) = oneshot::channel();
        let served_state = state.clone();
        let handle = tokio::spawn(async move {
            let _ = http::serve(listener, served_state, async move {
                let _ = shutdown_rx.await;
            })
            .await;
        });
        Self {
            addr,
            state,
            shutdown: Some(shutdown),
            handle,
        }
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        self.handle.abort();
    }
}

async fn http_get(addr: SocketAddr, path: &str) -> (u16, String) {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let request = format!(
        "GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\nAccept: application/json\r\n\r\n"
    );
    stream
        .write_all(request.as_bytes())
        .await
        .expect("write request");
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await.expect("read response");
    let text = String::from_utf8_lossy(&raw).into_owned();
    let status = text
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse::<u16>().ok())
        .expect("http status line");
    let body = text
        .split("\r\n\r\n")
        .nth(1)
        .unwrap_or_default()
        .to_owned();
    (status, body)
}

// ---------------------------------------------------------------------------
// I2 — pin (RED at audit): a total over an omitted counter is not a number
// ---------------------------------------------------------------------------

#[test]
fn red_partial_usage_sum_never_settles_a_measured_total() {
    // Call A omitted completion/total (a provider response with a partial
    // usage object); call B reported completion but omitted prompt.
    let call_a = Usage {
        prompt_tokens: Some(100),
        ..Usage::default()
    };
    let call_b = Usage {
        completion_tokens: Some(50),
        total_tokens: Some(150),
        ..Usage::default()
    };
    let sum = call_a.checked_add(&call_b);

    // V2 in hiverouter-contract/src/usage.rs: "A run with None counters is
    // INCOMPLETE and its cost must not be settled." A sum that silently drops
    // the omitted side turns an INCOMPLETE run into a measured-looking total.
    assert_eq!(
        sum.completion_tokens, None,
        "DEFECT (hiverouter-contract/src/usage.rs::checked_add): completion_tokens \
         was omitted by call A but the aggregate reports {:?}; the run total must \
         stay unknown when any contributing call omitted the counter",
        sum.completion_tokens
    );
    assert_eq!(
        sum.total_tokens, None,
        "DEFECT (hiverouter-contract/src/usage.rs::checked_add): total_tokens \
         was omitted by call A but the aggregate reports {:?}",
        sum.total_tokens
    );
}

#[tokio::test]
async fn red_run_total_hides_an_omitted_per_call_counter() {
    // Planner reports no reasoning_tokens (optional per documented schema);
    // the leaf reply reports reasoning=4. The persisted run row must therefore
    // NOT claim a measured reasoning total of 4.
    let planner = measured(10, 5, None);
    let leaf = measured(20, 7, Some(4));
    let harness = engine_harness(ScriptedUsageExecutor::new(
        plan_with(1, 1),
        planner,
        leaf,
    ));
    let mut receiver = harness.bus.subscribe();
    let run = harness
        .engine
        .submit_prompt("usage truth", "p", Uuid::now_v7())
        .expect("submit");
    let terminal = wait_for(&mut receiver, |event| {
        event.envelope.name.as_str() == "RUN_STATUS"
            && event.envelope.payload["status"] != "running"
    })
    .await;
    assert_eq!(terminal.envelope.payload["status"], "completed");

    let usage = terminal_usage(&harness, run.id)
        .await
        .expect("completed run with measured replies must persist a usage row");
    assert_eq!(
        usage.reasoning_tokens, None,
        "DEFECT (execution.rs add_run_usage -> usage.rs checked_add): the planner \
         omitted reasoning_tokens, so the run aggregate must stay unknown; it \
         currently reports {:?} (only the leaf calls' measured values)",
        usage.reasoning_tokens
    );
}

#[tokio::test]
async fn plain_reply_run_sums_every_served_call_exactly() {
    // Control case for the tool-turn defect: a planner call + one plain leaf
    // reply are both plain (no tool calls), so both must contribute.
    let planner = measured(10, 5, Some(1));
    let leaf = measured(20, 7, Some(2));
    let harness = engine_harness(ScriptedUsageExecutor::new(
        plan_with(1, 1),
        planner,
        leaf,
    ));
    let mut receiver = harness.bus.subscribe();
    let run = harness
        .engine
        .submit_prompt("plain reply", "p", Uuid::now_v7())
        .expect("submit");
    let terminal = wait_for(&mut receiver, |event| {
        event.envelope.name.as_str() == "RUN_STATUS"
            && event.envelope.payload["status"] != "running"
    })
    .await;
    assert_eq!(terminal.envelope.payload["status"], "completed");
    let usage = terminal_usage(&harness, run.id)
        .await
        .expect("completed run persists usage");
    assert_eq!(usage.prompt_tokens, Some(30), "{usage:?}");
    assert_eq!(usage.completion_tokens, Some(12), "{usage:?}");
    assert_eq!(usage.total_tokens, Some(42), "{usage:?}");
    assert_eq!(usage.reasoning_tokens, Some(3), "{usage:?}");
}

// ---------------------------------------------------------------------------
// I9 - pin (RED at audit): every served provider call contributes its measured usage
// ---------------------------------------------------------------------------

#[tokio::test]
async fn red_tool_turn_replies_are_missing_from_the_run_usage_total() {
    // Four provider calls are served: planner + first leaf reply (tool call) +
    // continuation 1 (tool call) + continuation 2 (final answer). Measured
    // usage per call is 100/10, 10/1, 20/2, 30/3 and each reports reasoning.
    // The persisted run total must therefore be 160 prompt / 16 completion /
    // 176 total / 11 reasoning.
    let workspace = tempfile::tempdir().expect("workspace");
    std::fs::write(workspace.path().join("note.txt"), b"hello tool loop").expect("write note");
    let executor = ToolLoopUsageExecutor::new(plan_with(1, 1), measured(100, 10, Some(5)));
    let harness = engine_harness_with_workspace(executor.clone(), workspace.path());
    let mut receiver = harness.bus.subscribe();
    let run = harness
        .engine
        .submit_prompt("read then answer", "p", Uuid::now_v7())
        .expect("submit");
    let terminal = wait_for(&mut receiver, |event| {
        event.envelope.name.as_str() == "RUN_STATUS"
            && event.envelope.payload["status"] != "running"
    })
    .await;
    assert_eq!(terminal.envelope.payload["status"], "completed");
    assert_eq!(
        executor.calls_served(),
        4,
        "the scripted provider served planner + three replies"
    );

    let usage = terminal_usage(&harness, run.id)
        .await
        .expect("a completed run must persist usage");
    assert_eq!(
        usage.prompt_tokens,
        Some(160),
        "DEFECT (execution.rs execute_task): the first reply and continuation-1 \
         were billable provider calls (10 + 20 prompt tokens) but only the \
         final reply and planner were added to run_usage; persisted {usage:?}"
    );
    assert_eq!(
        usage.completion_tokens,
        Some(16),
        "DEFECT (execution.rs execute_task): tool-call turn completions (1 + 2) \
         are missing from the persisted total; persisted {usage:?}"
    );
    assert_eq!(
        usage.total_tokens,
        Some(176),
        "DEFECT (execution.rs execute_task): persisted total does not sum the \
         four served calls; persisted {usage:?}"
    );
    assert_eq!(
        usage.reasoning_tokens,
        Some(11),
        "DEFECT (execution.rs execute_task): reasoning of intermediate tool \
         turns is missing; persisted {usage:?}"
    );
}

#[tokio::test]
async fn red_finish_reply_is_counted_exactly_once() {
    // Two served calls: planner measured(100, 10, Some(5)) and one leaf reply
    // measured(10, 1, Some(1)) that terminates via the `finish` tool. The true
    // run total is 110 prompt / 11 completion / 121 total / 6 reasoning.
    let executor = FinishUsageExecutor::new(plan_with(1, 1), measured(100, 10, Some(5)));
    // `finish` is a read-class tool; the current execute_tool_plan still requires
    // a workspace jail for every read-class tool, so provide one.
    let workspace = tempfile::tempdir().expect("workspace");
    let harness = engine_harness_with_workspace(executor.clone(), workspace.path());
    let mut receiver = harness.bus.subscribe();
    let run = harness
        .engine
        .submit_prompt("finish now", "p", Uuid::now_v7())
        .expect("submit");
    let terminal = wait_for(&mut receiver, |event| {
        event.envelope.name.as_str() == "RUN_STATUS"
            && event.envelope.payload["status"] != "running"
    })
    .await;
    assert_eq!(
        terminal.envelope.payload["status"], "completed",
        "finish path must complete; terminal payload: {}",
        terminal.envelope.payload
    );
    assert_eq!(executor.calls_served(), 2, "planner + one finish reply");

    let usage = terminal_usage(&harness, run.id)
        .await
        .expect("completed run persists usage");
    assert_eq!(
        usage.prompt_tokens,
        Some(110),
        "DEFECT (execution.rs): the finish reply is added once as a tool-call \
         turn and again by complete_task (which copies its usage into the \
         terminal reply); each served call must count exactly once: {usage:?}"
    );
    assert_eq!(usage.completion_tokens, Some(11), "{usage:?}");
    assert_eq!(usage.total_tokens, Some(121), "{usage:?}");
    assert_eq!(usage.reasoning_tokens, Some(6), "{usage:?}");
}

#[tokio::test]
async fn red_finish_reply_done_event_keeps_measured_usage() {
    // The DB total is now counted once, but zeroing the terminal copy's usage
    // also zeroes the REPLY_DONE payload for a finish/report-terminated task.
    // A consumer of the event stream must still see the measured usage of the
    // served call, not "not reported".
    let executor = FinishUsageExecutor::new(plan_with(1, 1), measured(100, 10, Some(5)));
    let workspace = tempfile::tempdir().expect("workspace");
    let harness = engine_harness_with_workspace(executor.clone(), workspace.path());
    let mut receiver = harness.bus.subscribe();
    let run = harness
        .engine
        .submit_prompt("finish now", "p", Uuid::now_v7())
        .expect("submit");
    let mut reply_done: Vec<Value> = Vec::new();
    loop {
        let event = wait_for(&mut receiver, |_| true).await;
        if event.envelope.name.as_str() == "REPLY_DONE" {
            reply_done.push(event.envelope.payload.clone());
        }
        if event.envelope.name.as_str() == "RUN_STATUS"
            && event.envelope.payload["status"] != "running"
        {
            break;
        }
    }
    let usage = terminal_usage(&harness, run.id)
        .await
        .expect("completed run persists usage");
    assert_eq!(usage.prompt_tokens, Some(110), "{usage:?}");
    assert!(
        reply_done.iter().any(|payload| {
            payload["usage"]["prompt_tokens"].as_u64() == Some(10)
                && payload["usage"]["completion_tokens"].as_u64() == Some(1)
        }),
        "DEFECT (execution.rs complete_task): the REPLY_DONE event for a \
         finish/report-terminated task carries no measured usage even though the \
         call was served and counted in run_usage. Events: {reply_done:?}"
    );
}

// ---------------------------------------------------------------------------
// I3 — RED: unknown spend is not a numeric zero
// ---------------------------------------------------------------------------

#[test]
fn red_unsettled_spend_api_reports_zero() {
    let mut budget = RunBudget::new(BudgetLimits::default());
    // A stream failed in transport after it may have been billed. The report is
    // honest (null), but the numeric accessor used by budget math/UI callers
    // returns 0.00 for the same state.
    budget.record_transport_failure(Refusal::ProviderIncomplete);
    assert_eq!(
        budget.report().settled_usd,
        None,
        "report stays null for unknown spend"
    );
    assert_eq!(
        budget.settled_usd(),
        None,
        "an INCOMPLETE run with unknown spend must expose None, not a numeric zero"
    );
    assert!(
        budget.spent_usd().is_zero(),
        "the legacy budget-math floor may be zero, but display must use settled_usd()"
    );
}

// ---------------------------------------------------------------------------
// I4 — pin (RED at audit): the usage route cannot be reconciled with provider calls
// ---------------------------------------------------------------------------

#[tokio::test]
async fn red_usage_route_has_no_completeness_provenance_or_call_count() {
    let temp = tempfile::tempdir().expect("temp dir");
    let server = TestServer::start(&temp).await;
    let run = server
        .state
        .engine
        .store
        .create_run("p", "usage", AgentState::Running)
        .expect("run");
    // A cumulative checkpoint written mid-run (every reply checkpoints). It is
    // NOT final and its counters may be partial; the route returns it with the
    // same shape as a terminal settled row.
    server
        .state
        .engine
        .store
        .record_run_usage(run.id, &measured(21, 34, None))
        .expect("checkpoint");
    let (status, body) = http_get(server.addr, &format!("/api/usage/runs/{}", run.id)).await;
    assert_eq!(status, 200, "usage route: {body}");
    let value: Value = serde_json::from_str(&body).expect("usage JSON");
    let usage = value["usage"].as_object().expect("usage object");

    assert!(
        usage.contains_key("status"),
        "DEFECT (http.rs run_usage): the route returns counters only, with no \
         complete/partial/unknown status, so a mid-run checkpoint renders as a \
         measured final total. Payload: {value}"
    );
    assert!(
        usage.contains_key("provenance"),
        "DEFECT (http.rs run_usage): no provenance label (measured/derived/\
         unknown) accompanies the counters. Payload: {value}"
    );
    assert!(
        usage.contains_key("calls"),
        "DEFECT (http.rs run_usage): no provider-call count accompanies the \
         counters, so run_usage cannot be reconciled with provider accounting \
         (a legacy 2-call row is indistinguishable from a 50-call row). \
         Payload: {value}"
    );
}

#[tokio::test]
async fn red_running_checkpoint_must_not_be_labelled_complete() {
    // Every reply checkpoints the cumulative row mid-run. A row whose six
    // counters are all present is "complete so far", not a final settled total:
    // the route must expose finality (run terminal vs in progress) so the UI
    // does not render a live subtotal as a measured run total.
    let temp = tempfile::tempdir().expect("temp dir");
    let server = TestServer::start(&temp).await;
    let running = server
        .state
        .engine
        .store
        .create_run("p", "running checkpoint", AgentState::Running)
        .expect("run");
    server
        .state
        .engine
        .store
        .record_run_usage(running.id, &measured(21, 34, Some(1)))
        .expect("checkpoint");
    let (status, body) = http_get(server.addr, &format!("/api/usage/runs/{}", running.id)).await;
    assert_eq!(status, 200, "usage route: {body}");
    let value: Value = serde_json::from_str(&body).expect("usage JSON");
    let usage = value["usage"].as_object().expect("usage object");
    let final_flag = usage.get("final").and_then(Value::as_bool);
    let status_label = usage.get("status").and_then(Value::as_str);
    assert!(
        final_flag == Some(false)
            || matches!(status_label, Some("partial" | "checkpoint" | "running")),
        "DEFECT (http.rs run_usage): a non-terminal run's checkpoint is labelled \
         status={status_label:?} final={final_flag:?}; it must be marked \
         non-final (e.g. final:false) so a live subtotal is not read as a \
         measured run total. Payload: {value}"
    );

    // Control: the same counters on a terminal run are final/complete.
    let done = server
        .state
        .engine
        .store
        .create_run("p", "terminal usage", AgentState::Completed)
        .expect("run");
    server
        .state
        .engine
        .store
        .record_run_usage(done.id, &measured(21, 34, Some(1)))
        .expect("usage");
    let (status, body) = http_get(server.addr, &format!("/api/usage/runs/{}", done.id)).await;
    assert_eq!(status, 200, "usage route: {body}");
    let value: Value = serde_json::from_str(&body).expect("usage JSON");
    assert_eq!(
        value["usage"]["status"], "complete",
        "a terminal measured row must report complete: {value}"
    );
}

// ---------------------------------------------------------------------------
// I1/I6 — GREEN: absent usage stays null through DB, route, and detail
// ---------------------------------------------------------------------------

#[tokio::test]
async fn null_usage_stays_null_not_zero_across_db_and_routes() {
    let temp = tempfile::tempdir().expect("temp dir");
    let server = TestServer::start(&temp).await;
    let run = server
        .state
        .engine
        .store
        .create_run("p", "no usage", AgentState::Failed)
        .expect("run");
    // The persisted row keeps the three counters it knows and NULL for the
    // counter the provider omitted; a re-read must not coerce it to 0.
    server
        .state
        .engine
        .store
        .record_run_usage(
            run.id,
            &Usage {
                prompt_tokens: Some(9),
                completion_tokens: None,
                total_tokens: None,
                ..Usage::default()
            },
        )
        .expect("usage");
    let stored = server
        .state
        .engine
        .store
        .run_usage(run.id)
        .expect("read")
        .expect("row");
    assert_eq!(stored.completion_tokens, None, "NULL survives the DB round-trip");

    let (status, body) = http_get(server.addr, &format!("/api/usage/runs/{}", run.id)).await;
    assert_eq!(status, 200);
    let value: Value = serde_json::from_str(&body).expect("usage JSON");
    assert_eq!(value["usage"]["prompt_tokens"], 9);
    assert_eq!(value["usage"]["completion_tokens"], Value::Null);
    assert_eq!(value["usage"]["total_tokens"], Value::Null);
    assert_ne!(value["usage"]["completion_tokens"], 0, "unknown is not 0");

    // Detail route: same nulls, no invented derivation.
    let (status, body) = http_get(server.addr, &format!("/api/runs/{}/detail", run.id)).await;
    assert_eq!(status, 200);
    let detail: Value = serde_json::from_str(&body).expect("detail JSON");
    assert_eq!(detail["usage"]["prompt_tokens"], 9);
    assert_eq!(detail["usage"]["completion_tokens"], Value::Null);

    // A run with no usage row at all is `null`, not an all-zero object.
    let empty = server
        .state
        .engine
        .store
        .create_run("p", "refused before send", AgentState::Failed)
        .expect("run");
    let (status, body) = http_get(server.addr, &format!("/api/usage/runs/{}", empty.id)).await;
    assert_eq!(status, 200);
    let value: Value = serde_json::from_str(&body).expect("usage JSON");
    assert_eq!(value["usage"], Value::Null);
}

// ---------------------------------------------------------------------------
// I7 — GREEN: a capped run keeps the usage of its completed calls
// ---------------------------------------------------------------------------

#[tokio::test]
async fn capped_run_keeps_measured_usage_of_completed_calls() {
    let planner = measured(10, 5, Some(1));
    let leaf = measured(1, 2, Some(0));
    let mut harness = engine_harness(ScriptedUsageExecutor::new(
        plan_with(2, 1),
        planner,
        leaf,
    ));
    // Planner + first leaf are allowed; the second leaf's claim is refused.
    Arc::get_mut(&mut harness.engine)
        .expect("single owner before submit")
        .caps
        .max_calls = 2;
    let mut receiver = harness.bus.subscribe();
    let run = harness
        .engine
        .submit_prompt("cap probe", "p", Uuid::now_v7())
        .expect("submit");
    let terminal = wait_for(&mut receiver, |event| {
        event.envelope.name.as_str() == "RUN_STATUS"
            && event.envelope.payload["status"] != "running"
    })
    .await;
    assert_eq!(terminal.envelope.payload["status"], "failed");
    assert_eq!(
        terminal.envelope.payload["error_code"],
        Refusal::CallLimit.code()
    );
    let usage = terminal_usage(&harness, run.id)
        .await
        .expect("a capped run must keep the usage of calls that already settled");
    // At least the planner's 10/5 (and whatever leaf calls completed) must be
    // present; the refused call contributes nothing and never zeroes the row.
    assert!(
        usage.prompt_tokens.expect("prompt recorded") >= 10,
        "capped run lost settled planner usage: {usage:?}"
    );
    assert!(
        usage.total_tokens.expect("total recorded") >= 15,
        "capped run lost settled planner tokens: {usage:?}"
    );
}

// ---------------------------------------------------------------------------
// I5 — GREEN: unpriced external provider settles no USD, never 0
// ---------------------------------------------------------------------------

#[test]
fn unpriced_endpoint_reports_measured_tokens_and_no_usd() {
    let mut accounting = RunAccounting::external(PricingTable::documented());
    // A points endpoint may omit the DeepSeek cache split; that is honest for
    // an external endpoint and must not mark the usage incomplete.
    let external = Usage {
        prompt_tokens: Some(120),
        completion_tokens: Some(30),
        total_tokens: Some(150),
        ..Usage::default()
    };
    let call = accounting
        .record(hiverouter_provider::Model::Flash, Some(&external), 0)
        .expect("external usage is measured");
    assert_eq!(call.usage_provenance.label(), "measured");
    assert_eq!(
        call.amount_provenance.label(),
        "unpriced",
        "no USD amount is derived for a points endpoint"
    );
    assert!(call.amount_usd.is_zero(), "no fabricated nonzero USD amount");

    let report = accounting.report();
    assert_eq!(report.status, "UNPRICED");
    assert_eq!(report.settled_usd, None, "unpriced cost is null, never 0.00");
    assert_eq!(report.prompt_tokens, Some(120));
    assert_eq!(report.amount_provenance, "unpriced");
    assert!(accounting.settled_usd().is_err(), "no settled figure to return");
}

#[test]
fn incomplete_usage_exports_no_settled_cost_and_no_zero() {
    let mut accounting = RunAccounting::new(PricingTable::documented());
    accounting
        .record(hiverouter_provider::Model::Flash, None, 0)
        .expect_err("absent usage is refused");
    let report = accounting.report();
    assert_eq!(report.status, "INCOMPLETE");
    assert_eq!(report.settled_usd, None);
    assert_eq!(report.prompt_tokens, None);
    assert_eq!(report.completion_tokens, None);
    assert_eq!(report.incomplete_reason, Some("E_USAGE_INCOMPLETE"));
}

#[test]
fn zero_call_unpriced_run_claims_no_usd() {
    // No calls, external mode: still UNPRICED/null. (A priced zero-call report
    // does return "0" today; that is a known design choice, not pinned here.)
    let report = RunAccounting::external(PricingTable::documented()).report();
    assert_eq!(report.status, "UNPRICED");
    assert_eq!(report.settled_usd, None);
    assert_eq!(report.calls, 0);
}

// ---------------------------------------------------------------------------
// I8 — GREEN: scripted/unconfigured executors cannot persist usage
// ---------------------------------------------------------------------------

#[tokio::test]
async fn unconfigured_provider_persists_no_scripted_usage() {
    let harness = engine_harness(Arc::new(UnconfiguredExecutor));
    let before = harness.store.list_runs(50).expect("run list");
    let outcome = harness
        .engine
        .submit_prompt("no key", "p", Uuid::now_v7());
    let error = outcome.expect_err("a keyless provider must refuse at admission");
    assert_eq!(
        error,
        Refusal::KeyMissing,
        "refusal is named, not a fabricated run"
    );
    let after = harness.store.list_runs(50).expect("run list");
    assert_eq!(
        before.len(),
        after.len(),
        "a keyless admission must not create a run (and so cannot persist usage)"
    );
}

// ---------------------------------------------------------------------------
// Census pins: the doc/code caps agree (capability-auditor census)
// ---------------------------------------------------------------------------

#[test]
fn call_cap_census_matches_provider_and_core() {
    assert_eq!(
        hiverouter_core::caps::MAX_CALLS,
        u64::from(hiverouter_provider::CALL_CAP),
        "core caps.rs MAX_CALLS and provider CALL_CAP must not drift"
    );
    assert_eq!(
        hiverouter_core::caps::MAX_AGENTS,
        u64::from(hiverouter_provider::AGENT_LIMIT)
    );
    assert_eq!(
        hiverouter_core::caps::MAX_SPAWN_DEPTH,
        hiverouter_provider::MAX_SPAWN_DEPTH
    );
    assert_eq!(
        hiverouter_core::caps::MAX_FANOUT as u64,
        u64::from(hiverouter_provider::FANOUT_LIMIT)
    );
}
