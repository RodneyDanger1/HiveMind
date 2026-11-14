//! S0 scale probe: measures the current engine's plan-dispatch wall time at
//! small fan-out with an instant provider double. It does not claim scheduler
//! concurrency; the engine is serial today, so this is an honest baseline.
//!
//! Run with: `cargo test -p hiverouter-core --test scale_s0 -- --nocapture`

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use hiverouter_contract::{Refusal, Usage};
use hiverouter_core::bus::EventBus;
use hiverouter_core::db::Database;
use hiverouter_core::entities::TaskRecord;
use hiverouter_core::execution::{
    DeltaSink, ProviderExecutor, RunEngine, StreamedReply,
};
use hiverouter_core::graph::GraphAuthority;
use hiverouter_core::planner::{Plan, PlanNode};
use hiverouter_core::prompts::TemplateRegistry;
use hiverouter_core::store::RunStore;
use hiverouter_contract::TaskId;
use tokio_util::sync::CancellationToken;

struct ScaleExecutor {
    plan: Plan,
    calls: AtomicUsize,
}

#[async_trait]
impl ProviderExecutor for ScaleExecutor {
    async fn plan(
        &self,
        _run_id: hiverouter_contract::RunId,
        _prompt: &str,
        _cancel: &CancellationToken,
    ) -> Result<Plan, Refusal> {
        Ok(self.plan.clone())
    }

    async fn execute(
        &self,
        _run_id: hiverouter_contract::RunId,
        task: &TaskRecord,
        _prompt: &str,
        _cancel: &CancellationToken,
        on_delta: &DeltaSink,
    ) -> Result<StreamedReply, Refusal> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let text = format!("result {}", task.id);
        on_delta(&text);
        Ok(StreamedReply {
            deltas: vec![text],
            usage: Usage {
                prompt_tokens: Some(1),
                completion_tokens: Some(1),
                total_tokens: Some(2),
                ..Usage::default()
            },
            tool_calls: Vec::new(),
            reasoning: String::new(),
        })
    }
}

async fn probe(nodes: usize) -> (u64, usize, usize) {
    let temp = tempfile::tempdir().expect("temp dir");
    let db = Arc::new(Database::open(&temp.path().join("scale.db")).expect("db"));
    db.migrate().expect("migrate");
    let plan = Plan::new(
        (0..nodes)
            .map(|_| PlanNode {
                objective: None,
                node_id: TaskId::new(),
                role: "super".to_owned(),
                deps: Vec::new(),
                depth: 1,
                parent: None,
            })
            .collect(),
    );
    let executor = Arc::new(ScaleExecutor {
        plan,
        calls: AtomicUsize::new(0),
    });
    let bus = Arc::new(EventBus::new());
    let engine = Arc::new(RunEngine::new(
        db.clone(),
        bus.clone(),
        Arc::new(GraphAuthority::new()),
        executor.clone(),
        Arc::new(TemplateRegistry::default()),
    ));
    let store = RunStore::new(db);
    let started = Instant::now();
    let run = engine
        .submit_prompt("scale probe", "scale", uuid::Uuid::now_v7())
        .expect("submit");
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let current = store.get_run(run.id).expect("run");
        if current.status.is_terminal() {
            let elapsed = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
            let tasks = store.tasks_for_run(run.id).expect("tasks").len();
            return (elapsed, tasks, executor.calls.load(Ordering::SeqCst));
        }
        assert!(Instant::now() < deadline, "scale probe timed out");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

#[tokio::test]
async fn s0_probe_measures_small_fanout() {
    for nodes in [2usize, 8usize] {
        let (wall_ms, tasks, calls) = probe(nodes).await;
        println!("S0 nodes={nodes} tasks={tasks} calls={calls} wall_ms={wall_ms}");
        assert_eq!(tasks, nodes + 1, "root plus planned nodes");
        assert_eq!(calls, nodes + 1, "one serial provider call per task");
        assert!(wall_ms < 30_000, "S0 must finish inside the probe budget");
    }
}
