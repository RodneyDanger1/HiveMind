//! S1 serial baseline: 8 SuperAgent branches with 4 Agents each (41 tasks, the plan cap)
//! including the HyperAgent root) through the existing single-run serial
//! engine with an instant provider double.
//!
//! This is deliberately a **baseline**, not a concurrency claim: the engine is
//! serial today. The output records the measured wall time and the exact task
//! and provider-call counts so a later admission scheduler can be compared
//! against the same fixture.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use hiverouter_contract::{Refusal, RunId, TaskId, Usage};
use hiverouter_core::bus::EventBus;
use hiverouter_core::db::Database;
use hiverouter_core::entities::TaskRecord;
use hiverouter_core::execution::{DeltaSink, ProviderExecutor, RunEngine, StreamedReply};
use hiverouter_core::graph::GraphAuthority;
use hiverouter_core::planner::{Plan, PlanNode};
use hiverouter_core::prompts::TemplateRegistry;
use hiverouter_core::store::RunStore;
use tokio_util::sync::CancellationToken;

struct ScaleExecutor {
    plan: Plan,
    calls: AtomicUsize,
    active: AtomicUsize,
    max_active: AtomicUsize,
    delay_ms: u64,
}

#[async_trait]
impl ProviderExecutor for ScaleExecutor {
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
        task: &TaskRecord,
        _prompt: &str,
        _cancel: &CancellationToken,
        on_delta: &DeltaSink,
    ) -> Result<StreamedReply, Refusal> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_active.fetch_max(active, Ordering::SeqCst);
        if self.delay_ms > 0 {
            tokio::time::sleep(Duration::from_millis(self.delay_ms)).await;
        }
        self.active.fetch_sub(1, Ordering::SeqCst);
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

fn plan() -> Plan {
    let mut nodes = Vec::new();
    for _ in 0..8 {
        let branch = PlanNode {
            objective: None,
            node_id: TaskId::new(),
            role: "super".to_owned(),
            deps: Vec::new(),
            depth: 1,
            parent: None,
        };
        for _ in 0..4 {
            nodes.push(PlanNode {
                objective: None,
                node_id: TaskId::new(),
                role: "worker".to_owned(),
                deps: Vec::new(),
                depth: 2,
                parent: Some(branch.node_id),
            });
        }
        nodes.push(branch);
    }
    Plan::new(nodes)
}

#[tokio::test]
async fn s1_parallel_baseline_measures_41_tasks_at_bound_32() {
    let temp = tempfile::tempdir().expect("temp dir");
    let db = Arc::new(Database::open(&temp.path().join("scale-s1p.db")).expect("db"));
    db.migrate().expect("migrate");
    let executor = Arc::new(ScaleExecutor {
        plan: plan(),
        calls: AtomicUsize::new(0),
        active: AtomicUsize::new(0),
        max_active: AtomicUsize::new(0),
        delay_ms: 10,
    });
    let engine = Arc::new(
        RunEngine::new(
            db.clone(),
            Arc::new(EventBus::new()),
            Arc::new(GraphAuthority::new()),
            executor.clone(),
            Arc::new(TemplateRegistry::default()),
        )
        .with_parallel_dispatch(32),
    );
    let store = RunStore::new(db);
    let started = Instant::now();
    let run = engine
        .submit_prompt("S1 parallel baseline", "scale", uuid::Uuid::now_v7())
        .expect("submit");
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let current = store.get_run(run.id).expect("run");
        if current.status.is_terminal() {
            let elapsed = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
            let tasks = store.tasks_for_run(run.id).expect("tasks").len();
            let calls = executor.calls.load(Ordering::SeqCst);
            let max_active = executor.max_active.load(Ordering::SeqCst);
            println!("S1 parallel tasks={tasks} provider_calls={calls} max_active={max_active} wall_ms={elapsed}");
            assert_eq!(current.status.as_str(), "completed");
            assert_eq!(tasks, 41);
            assert_eq!(calls, tasks);
            assert!(max_active > 1, "bounded parallel dispatch must overlap provider calls");
            assert!(elapsed < 60_000);
            return;
        }
        assert!(Instant::now() < deadline, "S1 parallel baseline timed out");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

#[tokio::test]
async fn s1_serial_baseline_measures_41_tasks() {
    let temp = tempfile::tempdir().expect("temp dir");
    let db = Arc::new(Database::open(&temp.path().join("scale-s1.db")).expect("db"));
    db.migrate().expect("migrate");
    let executor = Arc::new(ScaleExecutor {
        plan: plan(),
        calls: AtomicUsize::new(0),
        active: AtomicUsize::new(0),
        max_active: AtomicUsize::new(0),
        delay_ms: 10,
    });
    let engine = Arc::new(RunEngine::new(
        db.clone(),
        Arc::new(EventBus::new()),
        Arc::new(GraphAuthority::new()),
        executor.clone(),
        Arc::new(TemplateRegistry::default()),
    ));
    let store = RunStore::new(db);
    let started = Instant::now();
    let run = engine
        .submit_prompt("S1 baseline", "scale", uuid::Uuid::now_v7())
        .expect("submit");
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let current = store.get_run(run.id).expect("run");
        if current.status.is_terminal() {
            let elapsed = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
            let tasks = store.tasks_for_run(run.id).expect("tasks").len();
            let calls = executor.calls.load(Ordering::SeqCst);
            println!("S1 serial tasks={tasks} provider_calls={calls} wall_ms={elapsed}");
            assert_eq!(current.status.as_str(), "completed");
            assert_eq!(tasks, 41, "root + 40 planned nodes (the MAX_PLAN_NODES cap)");
            assert_eq!(calls, tasks, "serial engine makes one provider call per task");
            assert!(elapsed < 60_000, "S1 baseline must finish inside the probe budget");
            return;
        }
        assert!(Instant::now() < deadline, "S1 baseline timed out");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}
