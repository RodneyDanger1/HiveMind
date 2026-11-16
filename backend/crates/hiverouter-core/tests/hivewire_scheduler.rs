//! HiveWire daemon scheduler integration tests against a real temp SQLite
//! database: lease, expiry, exactly-once requeue, bounded queue and terminal
//! attempt ceiling.

mod db {
    pub use hiverouter_core::db::*;
}

#[path = "../src/hivewire.rs"]
mod hivewire;

use std::sync::Arc;
use std::time::Duration;

use hiverouter_core::db::Database;
use hivewire::{
    DaemonScheduler, Hello, HiveWireConfig, HiveWireStore, MessageBody, MessageState, TaskPayload,
    WireEnvelope,
};
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

const TOKEN: &str = "hivewire-scheduler-token-0123456789";
const REMOTE: &str = "remote-a";

fn database() -> (TempDir, Arc<Database>) {
    let temp = tempfile::tempdir().expect("tempdir");
    let database = Database::open(&temp.path().join("hivewire.db")).expect("open");
    database.migrate().expect("migrate");
    (temp, Arc::new(database))
}

fn store_with_capacity(database: Arc<Database>, capacity: usize) -> HiveWireStore {
    let config = HiveWireConfig::new(REMOTE, format!("inproc://hive-a/{REMOTE}"), TOKEN)
        .with_queue_capacity(capacity);
    HiveWireStore::open(database, config).expect("store")
}

fn enqueue(store: &HiveWireStore, session_id: &str, sequence: u64, task: &str) -> u64 {
    let frame = WireEnvelope::post_handshake(
        session_id,
        sequence,
        format!("msg-{task}"),
        MessageBody::Task(TaskPayload {
            task_id: task.to_owned(),
            kind: "probe".to_owned(),
            payload: serde_json::json!({"task": task}),
        }),
    );
    match store.accept(&frame).expect("accept task") {
        hivewire::AcceptOutcome::Queued { sequence } => sequence,
        other => panic!("expected a queued task, got {other:?}"),
    }
}

fn handshake(store: &HiveWireStore) -> hivewire::HandshakeAck {
    store
        .handshake(
            &WireEnvelope::hello(
                "hello-1",
                Hello {
                    remote_id: REMOTE.to_owned(),
                    nonce: "nonce-1".to_owned(),
                    last_seen_sequence: 0,
                },
            ),
            TOKEN,
        )
        .expect("handshake")
}

#[test]
fn lease_expiry_requeues_a_task_exactly_once() {
    let (_temp, database) = database();
    let store = store_with_capacity(database.clone(), 8);
    let ack = handshake(&store);
    enqueue(&store, &ack.session_id, ack.next_sequence, "task-1");

    let scheduler = DaemonScheduler::new(database, 40, 3, 8).expect("scheduler");
    let first = scheduler.tick("daemon-1").expect("first tick");
    assert!(first.requeued == 0);
    assert_eq!(first.leased.len(), 1);
    assert_eq!(first.leased[0].attempts, 1);

    // A second immediate tick must not requeue or re-lease a live lease.
    let second = scheduler.tick("daemon-1").expect("second tick");
    assert_eq!(second.requeued, 0);
    assert!(second.leased.is_empty());
    assert_eq!(second.queue_depth, 1);

    std::thread::sleep(Duration::from_millis(70));

    let third = scheduler.tick("daemon-1").expect("third tick");
    assert_eq!(
        third.requeued, 1,
        "the stale lease is returned to the queue once"
    );
    assert_eq!(third.leased.len(), 1, "and re-leased in the same tick");
    assert_eq!(third.leased[0].message_id, "msg-task-1");
    assert_eq!(third.leased[0].attempts, 2);
    assert_eq!(third.queue_depth, 1);

    // Nothing is left stale, so another tick requeues nothing: the task cannot
    // be duplicated by a second expiry pass.
    let fourth = scheduler.tick("daemon-1").expect("fourth tick");
    assert_eq!(fourth.requeued, 0);
    assert!(fourth.leased.is_empty());

    let pending = store.pending_tasks(10).expect("pending");
    assert_eq!(pending.len(), 1, "exactly one queue row for the task");
    assert_eq!(pending[0].attempts, 2);
    assert_eq!(pending[0].state, MessageState::Leased);
    assert_eq!(scheduler.queue_depth().expect("depth"), 1);
}

#[test]
fn a_lease_is_completed_only_by_its_owner() {
    let (_temp, database) = database();
    let store = store_with_capacity(database.clone(), 4);
    let ack = handshake(&store);
    enqueue(&store, &ack.session_id, ack.next_sequence, "task-1");

    let scheduler = DaemonScheduler::new(database, 5_000, 3, 4).expect("scheduler");
    let tick = scheduler.tick("daemon-1").expect("tick");
    assert_eq!(tick.leased.len(), 1);

    let lost = scheduler
        .complete("daemon-2", REMOTE, "msg-task-1")
        .expect_err("wrong owner");
    assert_eq!(lost.code(), "E_HIVEWIRE_LEASE_LOST");
    assert_eq!(
        store.message_state("msg-task-1").expect("state"),
        Some(MessageState::Leased)
    );

    scheduler
        .complete("daemon-1", REMOTE, "msg-task-1")
        .expect("owner completes");
    assert_eq!(
        store.message_state("msg-task-1").expect("state"),
        Some(MessageState::Completed)
    );
    assert_eq!(scheduler.queue_depth().expect("depth"), 0);
    assert_eq!(
        scheduler
            .complete("daemon-1", REMOTE, "msg-task-1")
            .expect_err("already completed")
            .code(),
        "E_HIVEWIRE_LEASE_LOST"
    );
}

#[test]
fn attempt_ceiling_fails_the_task_terminally() {
    let (_temp, database) = database();
    let store = store_with_capacity(database.clone(), 4);
    let ack = handshake(&store);
    enqueue(&store, &ack.session_id, ack.next_sequence, "task-1");

    let scheduler = DaemonScheduler::new(database, 20, 1, 4).expect("scheduler");
    let first = scheduler.tick("daemon-1").expect("first tick");
    assert_eq!(first.leased.len(), 1);
    std::thread::sleep(Duration::from_millis(40));

    let second = scheduler.tick("daemon-1").expect("second tick");
    assert_eq!(second.exhausted, 1);
    assert_eq!(second.requeued, 0);
    assert!(second.leased.is_empty());
    assert_eq!(second.queue_depth, 0);
    assert_eq!(
        store.message_state("msg-task-1").expect("state"),
        Some(MessageState::Failed)
    );
    let third = scheduler.tick("daemon-1").expect("third tick");
    assert_eq!(third.exhausted, 0, "a failed task is not expired twice");
}

#[test]
fn bounded_queue_refuses_overflow_by_name() {
    let (_temp, database) = database();
    let store = store_with_capacity(database, 1);
    let ack = handshake(&store);
    enqueue(&store, &ack.session_id, ack.next_sequence, "task-1");

    let overflow = WireEnvelope::post_handshake(
        &ack.session_id,
        2,
        "msg-task-2",
        MessageBody::Task(TaskPayload {
            task_id: "task-2".to_owned(),
            kind: "probe".to_owned(),
            payload: serde_json::json!({"task": "task-2"}),
        }),
    );
    let error = store.accept(&overflow).expect_err("full queue");
    assert_eq!(error.code(), "E_HIVEWIRE_QUEUE_FULL");
    assert!(error.to_string().contains("full"));
    assert_eq!(store.queue_depth().expect("depth"), 1);
    assert_eq!(store.pending_tasks(10).expect("pending").len(), 1);
}

#[test]
fn scheduler_bounds_are_validated() {
    let (_temp, database) = database();
    assert_eq!(
        DaemonScheduler::new(database.clone(), 0, 3, 1)
            .expect_err("zero lease")
            .code(),
        "E_HIVEWIRE_CONFIG"
    );
    assert_eq!(
        DaemonScheduler::new(database.clone(), 100, 0, 1)
            .expect_err("zero attempts")
            .code(),
        "E_HIVEWIRE_CONFIG"
    );
    let scheduler = DaemonScheduler::new(database, 100, 3, 1).expect("valid scheduler");
    assert_eq!(
        scheduler.tick("   ").expect_err("empty owner").code(),
        "E_HIVEWIRE_CONFIG"
    );
}

#[test]
fn run_loop_leases_until_cancelled_and_reports_work() {
    let (_temp, database) = database();
    let store = store_with_capacity(database.clone(), 8);
    let ack = handshake(&store);
    enqueue(&store, &ack.session_id, ack.next_sequence, "task-loop-1");

    let scheduler = DaemonScheduler::new(database, 5_000, 3, 8)
        .expect("scheduler")
        .with_generation("gen-loop");
    let cancel = CancellationToken::new();
    let thread_cancel = cancel.clone();
    let (report_tx, report_rx) = std::sync::mpsc::channel();
    let handle = std::thread::spawn(move || {
        let report = scheduler.run_loop(thread_cancel, Duration::from_millis(10), 8);
        let _ = report_tx.send(report);
    });

    // Wait until the production loop has actually leased the task.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let pending = store.pending_tasks(10).expect("pending");
        if pending
            .iter()
            .any(|task| task.state == MessageState::Leased)
        {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the run_loop did not lease a ready task within 5 s"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    // Give the loop one live tick past its recovery tick, then cancel.
    std::thread::sleep(Duration::from_millis(50));
    cancel.cancel();

    let report = report_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("the loop must stop after cancellation")
        .expect("the loop report");
    handle.join().expect("loop thread joins");
    assert!(report.cancelled, "the loop must report its cancellation");
    assert!(report.ticks >= 1, "at least one live tick ran");
    assert!(report.tasks_leased >= 1, "the loop leased real work");
    assert_eq!(report.recovery_ticks, 1, "one crash-recovery tick at start");
    assert_eq!(report.errors, 0, "a healthy loop reports no errors");
}

#[tokio::test]
async fn run_loop_async_cancels_promptly() {
    let (_temp, database) = database();
    let store = store_with_capacity(database.clone(), 4);
    let ack = handshake(&store);
    enqueue(&store, &ack.session_id, ack.next_sequence, "task-async-1");

    let scheduler = Arc::new(
        DaemonScheduler::new(database, 5_000, 3, 4)
            .expect("scheduler")
            .with_generation("gen-async"),
    );
    let cancel = CancellationToken::new();
    let task = tokio::spawn(Arc::clone(&scheduler).run_loop_async(
        cancel.clone(),
        Duration::from_millis(10),
        4,
    ));
    tokio::time::sleep(Duration::from_millis(100)).await;
    cancel.cancel();
    let report = tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .expect("the async loop must stop after cancellation")
        .expect("join")
        .expect("report");
    assert!(report.cancelled);
    assert!(report.tasks_leased >= 1);
}

#[test]
fn run_loop_rejects_zero_interval_batch_and_owner() {
    let (_temp, database) = database();
    let scheduler = DaemonScheduler::new(database, 100, 3, 1).expect("scheduler");
    let cancel = CancellationToken::new();
    assert_eq!(
        scheduler
            .run_loop(cancel.clone(), Duration::ZERO, 1)
            .expect_err("zero interval")
            .code(),
        "E_HIVEWIRE_CONFIG"
    );
    assert_eq!(
        scheduler
            .run_loop(cancel.clone(), Duration::from_millis(1), 0)
            .expect_err("zero batch")
            .code(),
        "E_HIVEWIRE_CONFIG"
    );
    assert_eq!(
        scheduler
            .run_loop_as("   ", cancel, Duration::from_millis(1), 1)
            .expect_err("empty owner")
            .code(),
        "E_HIVEWIRE_CONFIG"
    );
}
