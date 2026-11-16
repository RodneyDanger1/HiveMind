//! Lease-ownership tests for HiveWire completions.
//!
//! A `result`/`error` settles exactly the lease it names. A late completion (the
//! lease expired, was requeued, or was re-leased) must fail with
//! `E_HIVEWIRE_LEASE_LOST` and persist no effect; a byte-identical resend of an
//! already accepted completion must be a duplicate with a single effect.

mod db {
    pub use hiverouter_core::db::*;
}

#[path = "../src/hivewire.rs"]
mod hivewire;

use std::sync::Arc;
use std::time::Duration;

use hiverouter_core::db::Database;
use hivewire::{
    AcceptOutcome, DaemonScheduler, Hello, HiveWireConfig, HiveWireStore, LeaseIdentity,
    MessageBody, MessageState, TaskError, TaskPayload, TaskResult, WireEnvelope,
};
use serde_json::json;
use tempfile::TempDir;

const TOKEN: &str = "hivewire-lease-token-0123456789";
const REMOTE: &str = "remote-a";
const DAEMON: &str = "daemon-1";

fn database() -> (TempDir, Arc<Database>) {
    let temp = tempfile::tempdir().expect("tempdir");
    let database = Database::open(&temp.path().join("hivewire.db")).expect("open");
    database.migrate().expect("migrate");
    (temp, Arc::new(database))
}

fn store(database: Arc<Database>) -> HiveWireStore {
    HiveWireStore::open(
        database,
        HiveWireConfig::new(REMOTE, format!("inproc://hive-a/{REMOTE}"), TOKEN),
    )
    .expect("store")
}

fn handshake(store: &HiveWireStore) -> (String, u64) {
    let ack = store
        .handshake(
            &WireEnvelope::hello(
                "hello-lease-1",
                Hello {
                    remote_id: REMOTE.to_owned(),
                    nonce: "nonce-lease-1".to_owned(),
                    last_seen_sequence: 0,
                },
            ),
            TOKEN,
        )
        .expect("handshake");
    (ack.session_id, ack.next_sequence)
}

fn enqueue(store: &HiveWireStore, session: &str, sequence: u64, task: &str) -> String {
    let message_id = format!("msg-{task}");
    let frame = WireEnvelope::post_handshake(
        session,
        sequence,
        &message_id,
        MessageBody::Task(TaskPayload {
            task_id: task.to_owned(),
            kind: "probe".to_owned(),
            payload: json!({"task": task}),
        }),
    );
    assert_eq!(
        store.accept(&frame).expect("enqueue"),
        AcceptOutcome::Queued { sequence }
    );
    message_id
}

fn lease_identity(leased: &hivewire::LeasedTask) -> LeaseIdentity {
    leased.lease_identity()
}

fn result_frame(
    session: &str,
    sequence: u64,
    message_id: &str,
    task_id: &str,
    lease: LeaseIdentity,
) -> WireEnvelope {
    WireEnvelope::post_handshake(
        session,
        sequence,
        message_id,
        MessageBody::Result(TaskResult {
            task_id: task_id.to_owned(),
            lease,
            ok: true,
            output: json!({"done": true}),
        }),
    )
}

#[test]
fn late_result_after_lease_expiry_is_lease_lost() {
    let (_temp, database) = database();
    let store = store(database.clone());
    let (session, next) = handshake(&store);
    let task_message = enqueue(&store, &session, next, "task-late-1");

    let scheduler = DaemonScheduler::new(database.clone(), 20, 3, 4)
        .expect("scheduler")
        .with_generation("gen-late");
    let first = scheduler.tick(DAEMON).expect("first tick");
    assert_eq!(first.leased.len(), 1);
    let lease = lease_identity(&first.leased[0]);

    // The daemon finishes after its lease expired.
    std::thread::sleep(Duration::from_millis(50));
    let late = result_frame(
        &session,
        next + 1,
        "msg-late-result-1",
        "task-late-1",
        lease.clone(),
    );
    let error = store
        .accept(&late)
        .expect_err("a late result must be refused");
    assert_eq!(error.code(), "E_HIVEWIRE_LEASE_LOST");
    assert_eq!(
        store.message_state(&task_message).expect("state"),
        Some(MessageState::Leased),
        "the late result must not settle the row"
    );

    // Expiry requeues it; the same late result still cannot settle it.
    let expiry = scheduler.expire_leases().expect("expire");
    assert_eq!(expiry.requeued, 1);
    assert_eq!(
        store.accept(&late).expect_err("still late").code(),
        "E_HIVEWIRE_LEASE_LOST"
    );
    assert_eq!(
        store.message_state(&task_message).expect("state"),
        Some(MessageState::Queued)
    );

    // A fresh lease of the same task succeeds with the *new* identity.
    let second = scheduler.tick(DAEMON).expect("second tick");
    assert_eq!(second.leased.len(), 1);
    assert_eq!(second.leased[0].attempts, 2);
    let fresh = result_frame(
        &session,
        next + 1,
        "msg-late-result-1",
        "task-late-1",
        lease_identity(&second.leased[0]),
    );
    assert_eq!(
        store.accept(&fresh).expect("fresh lease settles"),
        AcceptOutcome::Recorded { sequence: next + 1 }
    );
    assert_eq!(
        store.message_state(&task_message).expect("state"),
        Some(MessageState::Completed)
    );
}

#[test]
fn completion_requires_the_current_attempt_and_owner() {
    let (_temp, database) = database();
    let store = store(database.clone());
    let (session, next) = handshake(&store);
    enqueue(&store, &session, next, "task-identity-1");
    let scheduler = DaemonScheduler::new(database, 5_000, 3, 4)
        .expect("scheduler")
        .with_generation("gen-identity");
    let tick = scheduler.tick(DAEMON).expect("tick");
    let lease = lease_identity(&tick.leased[0]);

    let mut wrong_attempt = lease.clone();
    wrong_attempt.attempt += 1;
    assert_eq!(
        store
            .accept(&result_frame(
                &session,
                next + 1,
                "msg-wrong-attempt",
                "task-identity-1",
                wrong_attempt
            ))
            .expect_err("wrong attempt")
            .code(),
        "E_HIVEWIRE_LEASE_LOST"
    );

    let mut wrong_owner = lease.clone();
    wrong_owner.daemon_id = "daemon-2".to_owned();
    assert_eq!(
        store
            .accept(&result_frame(
                &session,
                next + 1,
                "msg-wrong-owner",
                "task-identity-1",
                wrong_owner
            ))
            .expect_err("wrong owner")
            .code(),
        "E_HIVEWIRE_LEASE_LOST"
    );

    let mut wrong_message = lease.clone();
    wrong_message.message_id = "msg-other".to_owned();
    assert_eq!(
        store
            .accept(&result_frame(
                &session,
                next + 1,
                "msg-wrong-message",
                "task-identity-1",
                wrong_message
            ))
            .expect_err("wrong leased message")
            .code(),
        "E_HIVEWIRE_BAD_FRAME"
    );

    assert_eq!(
        store.message_state("msg-task-identity-1").expect("state"),
        Some(MessageState::Leased),
        "no refused completion may settle the row"
    );
    assert_eq!(
        store
            .accept(&result_frame(
                &session,
                next + 1,
                "msg-correct",
                "task-identity-1",
                lease
            ))
            .expect("correct identity"),
        AcceptOutcome::Recorded { sequence: next + 1 }
    );
}

#[test]
fn duplicate_result_delivery_has_a_single_effect() {
    let (_temp, database) = database();
    let store = store(database.clone());
    let (session, next) = handshake(&store);
    enqueue(&store, &session, next, "task-dup-1");
    let scheduler = DaemonScheduler::new(database.clone(), 5_000, 3, 4)
        .expect("scheduler")
        .with_generation("gen-dup");
    let tick = scheduler.tick(DAEMON).expect("tick");
    let lease = lease_identity(&tick.leased[0]);

    let result = result_frame(
        &session,
        next + 1,
        "msg-dup-result",
        "task-dup-1",
        lease.clone(),
    );
    assert_eq!(
        store.accept(&result).expect("first delivery"),
        AcceptOutcome::Recorded { sequence: next + 1 }
    );
    let completed_at: Option<i64> = database
        .with_connection(|connection| {
            connection.query_row(
                "SELECT completed_at_ms FROM hivewire_messages WHERE message_id = 'msg-task-dup-1'",
                [],
                |row| row.get(0),
            )
        })
        .expect("completed_at");
    assert!(completed_at.is_some());

    // The identical envelope is a duplicate before any sequence/lease check.
    assert_eq!(
        store.accept(&result).expect("resend"),
        AcceptOutcome::Duplicate
    );
    let completed_at_after: Option<i64> = database
        .with_connection(|connection| {
            connection.query_row(
                "SELECT completed_at_ms FROM hivewire_messages WHERE message_id = 'msg-task-dup-1'",
                [],
                |row| row.get(0),
            )
        })
        .expect("completed_at");
    assert_eq!(
        completed_at, completed_at_after,
        "the effect must not run twice"
    );
    assert_eq!(
        store.message_state("msg-task-dup-1").expect("state"),
        Some(MessageState::Completed)
    );

    // A different message id for the same settled task is late, not a duplicate.
    let second = result_frame(&session, next + 2, "msg-dup-result-2", "task-dup-1", lease);
    assert_eq!(
        store
            .accept(&second)
            .expect_err("late second result")
            .code(),
        "E_HIVEWIRE_LEASE_LOST"
    );
}

#[test]
fn task_error_requires_a_lease_and_settles_once() {
    let (_temp, database) = database();
    let store = store(database.clone());
    let (session, next) = handshake(&store);
    enqueue(&store, &session, next, "task-error-1");
    let scheduler = DaemonScheduler::new(database, 5_000, 3, 4)
        .expect("scheduler")
        .with_generation("gen-error");
    let tick = scheduler.tick(DAEMON).expect("tick");
    let lease = lease_identity(&tick.leased[0]);

    let unleased = WireEnvelope::post_handshake(
        &session,
        next + 1,
        "msg-error-no-lease",
        MessageBody::Error(TaskError {
            task_id: Some("task-error-1".to_owned()),
            lease: None,
            code: "E_REMOTE_FAILED".to_owned(),
            message: "remote failed".to_owned(),
        }),
    );
    assert_eq!(
        store.accept(&unleased).expect_err("missing lease").code(),
        "E_HIVEWIRE_BAD_FRAME"
    );

    let error = WireEnvelope::post_handshake(
        &session,
        next + 1,
        "msg-error-1",
        MessageBody::Error(TaskError {
            task_id: Some("task-error-1".to_owned()),
            lease: Some(lease.clone()),
            code: "E_REMOTE_FAILED".to_owned(),
            message: "remote failed".to_owned(),
        }),
    );
    assert_eq!(
        store.accept(&error).expect("leased error"),
        AcceptOutcome::Recorded { sequence: next + 1 }
    );
    assert_eq!(
        store.message_state("msg-task-error-1").expect("state"),
        Some(MessageState::Failed)
    );
    assert_eq!(
        store.accept(&error).expect("duplicate error"),
        AcceptOutcome::Duplicate
    );

    let late = WireEnvelope::post_handshake(
        &session,
        next + 2,
        "msg-error-2",
        MessageBody::Error(TaskError {
            task_id: Some("task-error-1".to_owned()),
            lease: Some(lease),
            code: "E_REMOTE_FAILED".to_owned(),
            message: "remote failed again".to_owned(),
        }),
    );
    assert_eq!(
        store.accept(&late).expect_err("late error").code(),
        "E_HIVEWIRE_LEASE_LOST"
    );
}

#[test]
fn result_for_a_queued_task_is_refused() {
    let (_temp, database) = database();
    let store = store(database);
    let (session, next) = handshake(&store);
    let message_id = enqueue(&store, &session, next, "task-unleased-1");

    let ghost = LeaseIdentity {
        message_id: message_id.clone(),
        attempt: 1,
        daemon_id: "ghost-daemon".to_owned(),
    };
    let result = result_frame(
        &session,
        next + 1,
        "msg-unleased-result",
        "task-unleased-1",
        ghost,
    );
    assert_eq!(
        store.accept(&result).expect_err("never leased").code(),
        "E_HIVEWIRE_LEASE_LOST"
    );
    assert_eq!(
        store.message_state(&message_id).expect("state"),
        Some(MessageState::Queued)
    );

    let unknown = result_frame(
        &session,
        next + 1,
        "msg-unknown-result",
        "task-never-existed",
        LeaseIdentity {
            message_id: "msg-never-queued".to_owned(),
            attempt: 1,
            daemon_id: "ghost-daemon".to_owned(),
        },
    );
    assert_eq!(
        store.accept(&unknown).expect_err("unknown message").code(),
        "E_HIVEWIRE_BAD_FRAME"
    );
}

#[test]
fn crash_recovery_requeues_previous_generation_and_invalidates_its_result() {
    let (_temp, database) = database();
    let store = store(database.clone());
    let (session, next) = handshake(&store);
    let message_id = enqueue(&store, &session, next, "task-crash-1");

    // A previous process (generation A) leased the task and died.
    let previous = DaemonScheduler::new(database.clone(), 5_000, 3, 4)
        .expect("previous")
        .with_generation("gen-a");
    let before = previous.tick(DAEMON).expect("lease before crash");
    assert_eq!(before.leased.len(), 1);
    let stale = lease_identity(&before.leased[0]);

    // A restarted process (generation B, same owner) recovers the lease without
    // waiting for its deadline and re-leases it.
    let restarted = DaemonScheduler::new(database, 5_000, 3, 4)
        .expect("restarted")
        .with_generation("gen-b");
    let recovery = restarted
        .crash_recovery_tick(DAEMON)
        .expect("recovery tick");
    assert_eq!(recovery.recovered, 1);
    assert_eq!(recovery.leased.len(), 1, "the recovered task is re-leased");
    assert_eq!(recovery.leased[0].attempts, 2);
    assert_eq!(recovery.leased[0].lease_generation, "gen-b");

    // The pre-crash worker's completion is late now.
    let stale_result = result_frame(
        &session,
        next + 1,
        "msg-stale-result",
        "task-crash-1",
        stale,
    );
    assert_eq!(
        store
            .accept(&stale_result)
            .expect_err("stale generation")
            .code(),
        "E_HIVEWIRE_LEASE_LOST"
    );
    assert_eq!(
        store.message_state(&message_id).expect("state"),
        Some(MessageState::Leased)
    );

    let fresh = result_frame(
        &session,
        next + 1,
        "msg-fresh-result",
        "task-crash-1",
        lease_identity(&recovery.leased[0]),
    );
    assert_eq!(
        store.accept(&fresh).expect("fresh result"),
        AcceptOutcome::Recorded { sequence: next + 1 }
    );
    assert_eq!(
        store.message_state(&message_id).expect("state"),
        Some(MessageState::Completed)
    );
}

#[test]
fn a_refused_completion_sequence_can_be_consumed_exactly_once() {
    let (_temp, database) = database();
    let store = store(database);
    let (session, next) = handshake(&store);
    enqueue(&store, &session, next, "task-seq-1");

    // Refusals persist nothing, so the session sequence stays at `next + 1`.
    let refused_sequence = next + 1;
    let refused = result_frame(
        &session,
        refused_sequence,
        "msg-refused-result",
        "task-seq-1",
        LeaseIdentity {
            message_id: "msg-task-seq-1".to_owned(),
            attempt: 1,
            daemon_id: "ghost-daemon".to_owned(),
        },
    );
    assert_eq!(
        store.accept(&refused).expect_err("no lease").code(),
        "E_HIVEWIRE_LEASE_LOST"
    );
    assert_eq!(
        store
            .consume_sequence(&session, refused_sequence)
            .expect("consume"),
        refused_sequence + 1
    );
    assert_eq!(
        store
            .consume_sequence(&session, refused_sequence)
            .expect_err("already consumed")
            .code(),
        "E_HIVEWIRE_SEQUENCE"
    );

    // The next frame uses the consumed sequence and is accepted normally.
    let heartbeat = WireEnvelope::post_handshake(
        &session,
        refused_sequence + 1,
        "msg-after-consume",
        MessageBody::Heartbeat(hivewire::Heartbeat {
            remote_id: REMOTE.to_owned(),
            inflight: 0,
        }),
    );
    assert_eq!(
        store.accept(&heartbeat).expect("heartbeat"),
        AcceptOutcome::Recorded {
            sequence: refused_sequence + 1
        }
    );
}
