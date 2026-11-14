//! Independent governance integration checks for root/lower-level detached
//! principals and same-run rewires.
//!
//! This file is intentionally self-contained: it uses the same public engine
//! seams as `run_lifecycle.rs` but does not modify that suite. No provider key,
//! network, or production edit is involved.

mod support;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use hiverouter_contract::command::RewireTask;
use hiverouter_contract::{schema, AgentState, Command, Refusal, RunId, TaskId, Usage};
use hiverouter_core::bus::{EventBus, SequencedEvent};
use hiverouter_core::daemon::DaemonStore;
use hiverouter_core::db::Database;
use hiverouter_core::entities::TaskRecord;
use hiverouter_core::execution::{
    DeltaSink, ProviderExecutor, RunEngine, StreamedReply, ToolTurnTranscript,
};
use hiverouter_core::graph::GraphAuthority;
use hiverouter_core::planner::{Plan, PlanNode};
use hiverouter_core::prompts::TemplateRegistry;
use hiverouter_core::remote::RemoteStore;
use hiverouter_core::store::RunStore;
use hiverouter_provider::tool_assembler::AssembledToolCall;
use serde_json::json;
use support::{
    error_code, frame_name, recv_until, TestServer, WsClient, TEST_ORIGIN,
};
use tokio::sync::broadcast::error::RecvError;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

fn super_plan() -> Plan {
    Plan::new(vec![PlanNode {
        objective: None,
        node_id: TaskId::new(),
        role: "super".to_owned(),
        deps: Vec::new(),
        depth: 1,
        parent: None,
    }])
}

struct TestEngine {
    engine: Arc<RunEngine>,
    bus: Arc<EventBus>,
    store: RunStore,
    db: Arc<Database>,
    remote: Arc<RemoteStore>,
    _daemons: Arc<DaemonStore>,
    _temp: tempfile::TempDir,
}

fn test_engine(executor: Arc<dyn ProviderExecutor>) -> TestEngine {
    let temp = tempfile::tempdir().expect("temp dir");
    let db = Arc::new(Database::open(&temp.path().join("hiverouter.db")).expect("db"));
    db.migrate().expect("migrate");
    let bus = Arc::new(EventBus::new());
    let remote = Arc::new(RemoteStore::new(db.clone()));
    let daemons = Arc::new(DaemonStore::new(db.clone()));
    let engine = Arc::new(
        RunEngine::new(
            db.clone(),
            bus.clone(),
            Arc::new(GraphAuthority::new()),
            executor,
            Arc::new(TemplateRegistry::default()),
        )
        .with_remote_store(remote.clone())
        .with_daemon_store(daemons.clone())
        .with_root_orchestrator(),
    );
    TestEngine {
        engine,
        bus,
        store: RunStore::new(db.clone()),
        db,
        remote,
        _daemons: daemons,
        _temp: temp,
    }
}

async fn wait_terminal(
    receiver: &mut tokio::sync::broadcast::Receiver<SequencedEvent>,
) -> SequencedEvent {
    for _ in 0..300 {
        match tokio::time::timeout(Duration::from_secs(10), receiver.recv()).await {
            Ok(Ok(event)) => {
                if event.envelope.name == "RUN_STATUS" {
                    let status = event.envelope.payload["status"].as_str().unwrap_or_default();
                    if matches!(status, "completed" | "failed" | "cancelled" | "interrupted") {
                        return event;
                    }
                }
            }
            Ok(Err(RecvError::Lagged(_))) => continue,
            Ok(Err(RecvError::Closed)) => panic!("event bus closed"),
            Err(_) => panic!("timed out waiting for terminal RUN_STATUS"),
        }
    }
    panic!("terminal RUN_STATUS did not arrive");
}

/* ---------------------- lower-level request_detached ---------------------- */

struct DetachedExecutor {
    calls: AtomicUsize,
    duplicate: bool,
}

impl DetachedExecutor {
    fn single() -> Arc<Self> {
        Arc::new(Self {
            calls: AtomicUsize::new(0),
            duplicate: false,
        })
    }

    fn duplicate() -> Arc<Self> {
        Arc::new(Self {
            calls: AtomicUsize::new(0),
            duplicate: true,
        })
    }
}

#[async_trait]
impl ProviderExecutor for DetachedExecutor {
    async fn plan(
        &self,
        _run_id: RunId,
        _prompt: &str,
        _cancel: &CancellationToken,
    ) -> Result<Plan, Refusal> {
        Ok(super_plan())
    }

    async fn execute(
        &self,
        _run_id: RunId,
        _task: &TaskRecord,
        _prompt: &str,
        _cancel: &CancellationToken,
        _on_delta: &DeltaSink,
    ) -> Result<StreamedReply, Refusal> {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            let first = AssembledToolCall {
                index: 0,
                id: "call-detached".to_owned(),
                name: "request_detached".to_owned(),
                arguments: json!({
                    "kind": "remote",
                    "display_name": "Branch light",
                    "reason": "branch needs a detached indicator light"
                }),
            };
            let calls = if self.duplicate {
                let mut second = first.clone();
                second.id = "call-detached-2".to_owned();
                vec![first, second]
            } else {
                vec![first]
            };
            return Ok(StreamedReply {
                deltas: Vec::new(),
                usage: Usage::default(),
                tool_calls: calls,
                reasoning: String::new(),
            });
        }
        Ok(StreamedReply {
            deltas: vec!["detached request complete".to_owned()],
            usage: Usage::default(),
            tool_calls: Vec::new(),
            reasoning: String::new(),
        })
    }

    async fn continue_with_tools(
        &self,
        _run_id: RunId,
        _task: &TaskRecord,
        _prompt: &str,
        _cancel: &CancellationToken,
        _on_delta: &DeltaSink,
        _transcript: &[ToolTurnTranscript],
    ) -> Result<StreamedReply, Refusal> {
        Ok(StreamedReply {
            deltas: vec!["detached continuation".to_owned()],
            usage: Usage::default(),
            tool_calls: Vec::new(),
            reasoning: String::new(),
        })
    }

    async fn finalize_root(
        &self,
        _run_id: RunId,
        _task: &TaskRecord,
        _prompt: &str,
        _cancel: &CancellationToken,
        _on_delta: &DeltaSink,
    ) -> Result<StreamedReply, Refusal> {
        Ok(StreamedReply {
            deltas: vec!["root answer".to_owned()],
            usage: Usage::default(),
            tool_calls: Vec::new(),
            reasoning: String::new(),
        })
    }
}

#[tokio::test]
async fn lower_level_request_detached_creates_attributed_remote_and_audit() {
    let harness = test_engine(DetachedExecutor::single());
    let mut receiver = harness.bus.subscribe();
    let run = harness
        .engine
        .submit_prompt("ask for a detached light", "p", Uuid::now_v7())
        .expect("submit");
    let terminal = wait_terminal(&mut receiver).await;
    assert_eq!(terminal.envelope.payload["status"], "completed");

    let remotes = harness.remote.list_remotes().expect("remotes");
    assert_eq!(remotes.len(), 1, "one admitted remote");
    assert_eq!(remotes[0].owner_id, run.id.to_string());
    assert!(
        remotes[0].created_by.starts_with("agent:"),
        "created_by must be agent:<task>: {}",
        remotes[0].created_by
    );
    assert_eq!(
        harness
            .remote
            .list_devices(&remotes[0].remote_id)
            .expect("devices")
            .len(),
        1,
        "the lower-level admission provisions one device"
    );
    assert_eq!(
        harness
            .remote
            .list_grants(&remotes[0].remote_id)
            .expect("grants")
            .len(),
        1,
        "the lower-level admission provisions one narrow grant"
    );
    let audit = harness
        .remote
        .list_audit(Some(&remotes[0].remote_id), 50)
        .expect("audit");
    for action in ["remote_created", "device_created", "grant_created"] {
        assert!(
            audit.iter().any(|row| {
                row.action == action
                    && row.actor_kind == "system"
                    && row.actor_id == remotes[0].created_by
            }),
            "missing {action} system audit row: {audit:?}"
        );
    }
    assert!(
        audit.iter().any(|row| {
            row.action == "hyper_admission"
                && row.actor_kind == "system"
                && row.actor_id == remotes[0].created_by
                && row.detail_json.contains("\"decision\":\"allow\"")
        }),
        "admission allow row missing: {audit:?}"
    );
}

#[tokio::test]
async fn duplicate_request_detached_is_refused_and_does_not_duplicate_rows() {
    let harness = test_engine(DetachedExecutor::duplicate());
    let mut receiver = harness.bus.subscribe();
    let run = harness
        .engine
        .submit_prompt("ask twice for the same detached light", "p", Uuid::now_v7())
        .expect("submit");
    wait_terminal(&mut receiver).await;

    assert_eq!(
        harness.remote.list_remotes().expect("remotes").len(),
        1,
        "the duplicate must not create a second remote"
    );
    let turns = harness.db.tool_turns_for_run(run.id).expect("tool turns");
    assert!(
        turns.iter().any(|turn| {
            turn.tool == "request_detached"
                && turn.code.as_deref() == Some(Refusal::DetachedConflict.code())
        }),
        "duplicate must be persisted as E_DETACHED_CONFLICT: {turns:?}"
    );
}

/* ------------------------- root checkpoint refusal ------------------------ */

struct RootSuperExecutor {
    orchestrations: AtomicUsize,
}

impl RootSuperExecutor {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            orchestrations: AtomicUsize::new(0),
        })
    }
}

#[async_trait]
impl ProviderExecutor for RootSuperExecutor {
    async fn plan(
        &self,
        _run_id: RunId,
        _prompt: &str,
        _cancel: &CancellationToken,
    ) -> Result<Plan, Refusal> {
        Ok(super_plan())
    }

    async fn execute(
        &self,
        _run_id: RunId,
        task: &TaskRecord,
        _prompt: &str,
        _cancel: &CancellationToken,
        on_delta: &DeltaSink,
    ) -> Result<StreamedReply, Refusal> {
        let text = format!("branch {}", task.id);
        on_delta(&text);
        Ok(StreamedReply {
            deltas: vec![text],
            usage: Usage::default(),
            tool_calls: Vec::new(),
            reasoning: String::new(),
        })
    }

    async fn orchestrate_root(
        &self,
        _run_id: RunId,
        _task: &TaskRecord,
        _prompt: &str,
        _cancel: &CancellationToken,
        _on_delta: &DeltaSink,
    ) -> Result<StreamedReply, Refusal> {
        let name = if self.orchestrations.fetch_add(1, Ordering::SeqCst) == 0 {
            "spawn_superagent"
        } else {
            "finish"
        };
        Ok(StreamedReply {
            deltas: Vec::new(),
            usage: Usage::default(),
            tool_calls: vec![AssembledToolCall {
                index: 0,
                id: format!("call-{name}"),
                name: name.to_owned(),
                arguments: json!({}),
            }],
            reasoning: String::new(),
        })
    }

    async fn finalize_root(
        &self,
        _run_id: RunId,
        _task: &TaskRecord,
        _prompt: &str,
        _cancel: &CancellationToken,
        _on_delta: &DeltaSink,
    ) -> Result<StreamedReply, Refusal> {
        Ok(StreamedReply {
            deltas: vec!["root answer".to_owned()],
            usage: Usage::default(),
            tool_calls: Vec::new(),
            reasoning: String::new(),
        })
    }
}

#[tokio::test]
async fn root_spawn_superagent_refusal_is_durable_in_the_run_bank() {
    let harness = test_engine(RootSuperExecutor::new());
    let mut receiver = harness.bus.subscribe();
    let run = harness
        .engine
        .submit_prompt("add a branch", "p", Uuid::now_v7())
        .expect("submit");
    let terminal = wait_terminal(&mut receiver).await;
    assert_eq!(terminal.envelope.payload["status"], "completed");

    let tasks = harness.store.tasks_for_run(run.id).expect("tasks");
    assert_eq!(
        tasks.len(),
        2,
        "root + the planned branch; the root checkpoint must not add a branch"
    );
    let root = tasks
        .iter()
        .find(|task| task.parent_task_id.is_none())
        .expect("root");
    assert_eq!(root.role, "planner");
    assert_eq!(
        tasks.iter().filter(|task| task.role == "super").count(),
        1,
        "only the planned branch lead exists"
    );
    let gate_rows = harness
        .store
        .memory_hyper_summary(run.id, 8)
        .expect("root gate memory");
    assert!(
        gate_rows.iter().any(|entry| {
            entry.key.starts_with("root_gate:")
                && entry.value.contains("\"action\":\"spawn_superagent\"")
                && entry.value.contains("\"decision\":\"refused\"")
        }),
        "root gate refusal must be durable: {gate_rows:?}"
    );
}

/* -------------------- same-run rewire (foreign parent) ------------------- */

async fn connect(server: &TestServer) -> WsClient {
    let mut client = WsClient::connect(server.addr, TEST_ORIGIN, None)
        .await
        .expect("websocket connect");
    client.hello(None).await.expect("send hello");
    client
}

async fn send_command(client: &mut WsClient, command: &Command) -> uuid::Uuid {
    let envelope = schema::encode_command(command).expect("encode command");
    let correlation = envelope.correlation_id;
    client
        .send_text(&envelope.to_json().expect("serialize command"))
        .await
        .expect("send command");
    correlation
}

#[tokio::test]
async fn rewire_to_a_foreign_run_parent_is_refused_before_mutation() {
    let temp = tempfile::tempdir().expect("temp dir");
    let server = TestServer::start(&temp).await;
    let store = RunStore::new(server.state.db.clone());
    let run_a = store
        .create_run("p", "run a", AgentState::Running)
        .expect("run a");
    let run_b = store
        .create_run("p", "run b", AgentState::Running)
        .expect("run b");

    let root_a = TaskId::new();
    let root_b = TaskId::new();
    let task = TaskId::new();
    server.state.graph.add_task(root_a, None).expect("root a");
    server.state.graph.add_task(root_b, None).expect("root b");
    server
        .state
        .graph
        .add_task(task, Some(root_a))
        .expect("task");
    for (id, run_id, parent, role, depth) in [
        (root_a, run_a.id, None, "planner", 0u8),
        (root_b, run_b.id, None, "planner", 0u8),
        (task, run_a.id, Some(root_a), "super", 1u8),
    ] {
        store
            .insert_task(&TaskRecord {
                id,
                run_id,
                parent_task_id: parent,
                role: role.to_owned(),
                depth,
                spawn_seq: depth as u64 + 1,
                state: AgentState::Running,
                rev: 0,
            })
            .expect("task row");
    }

    let mut client = connect(&server).await;
    let correlation = send_command(
        &mut client,
        &Command::RewireTask(RewireTask {
            task_id: task,
            new_parent_id: root_b,
            expected_rev: 0,
        }),
    )
    .await;
    let error = recv_until(&mut client, |value| {
        frame_name(value) == "ERROR" && value["correlation_id"] == correlation.to_string()
    })
    .await;
    assert_eq!(
        error_code(&error),
        Refusal::TaskNotFound.code(),
        "a foreign-run parent must be refused before any graph/context write: {error:?}"
    );
    assert_eq!(
        server.state.graph.parent_of(task).expect("parent"),
        Some(root_a),
        "the graph must remain unchanged"
    );
    assert_eq!(
        store.get_task(task).expect("task row").parent_task_id,
        Some(root_a),
        "the task row must remain unchanged"
    );
}
