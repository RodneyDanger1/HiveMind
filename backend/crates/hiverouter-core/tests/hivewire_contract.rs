//! HiveWire v1 integration tests: handshake auth, envelope contract, dedupe and
//! named refusals against a real temp SQLite database.
//!
//! `src/lib.rs` is outside this change's write scope, so the module is mounted
//! from its source file. The crate-root `db` re-export lets the module's
//! `crate::db` imports resolve exactly as they will inside the library once
//! `pub mod hivewire;` is added to `src/lib.rs`.

mod db {
    pub use hiverouter_core::db::*;
}

#[path = "../src/hivewire.rs"]
mod hivewire;

use std::sync::Arc;

use hiverouter_core::db::Database;
use hivewire::{
    Hello, HiveWireConfig, HiveWireStore, MessageBody, MessageState, TaskPayload, WireEnvelope,
    PROTOCOL_VERSION,
};
use tempfile::TempDir;

const TOKEN: &str = "hivewire-test-launch-token-0123456789";
const REMOTE: &str = "remote-a";

fn database() -> (TempDir, Arc<Database>) {
    let temp = tempfile::tempdir().expect("tempdir");
    let database = Database::open(&temp.path().join("hivewire.db")).expect("open");
    database.migrate().expect("migrate");
    (temp, Arc::new(database))
}

fn config() -> HiveWireConfig {
    HiveWireConfig::new(REMOTE, format!("inproc://hive-a/{REMOTE}"), TOKEN)
}

fn hello(message_id: &str) -> WireEnvelope {
    WireEnvelope::hello(
        message_id,
        Hello {
            remote_id: REMOTE.to_owned(),
            nonce: "nonce-1".to_owned(),
            last_seen_sequence: 0,
        },
    )
}

fn task_frame(session_id: &str, sequence: u64, message_id: &str, task_id: &str) -> WireEnvelope {
    WireEnvelope::post_handshake(
        session_id,
        sequence,
        message_id,
        MessageBody::Task(TaskPayload {
            task_id: task_id.to_owned(),
            kind: "probe".to_owned(),
            payload: serde_json::json!({"ping": true}),
        }),
    )
}

fn session_count(database: &Database) -> i64 {
    database
        .with_connection(|connection| {
            connection.query_row("SELECT COUNT(*) FROM hivewire_sessions", [], |row| row.get(0))
        })
        .expect("count")
}

#[test]
fn handshake_rejects_a_bad_token_by_name_and_creates_no_session() {
    let (_temp, database) = database();
    let store = HiveWireStore::open(database.clone(), config()).expect("store");
    let request = hello("hello-1");

    let error = store
        .handshake(&request, "wrong-launch-token-000000000")
        .expect_err("a bad bearer token must be refused");
    assert_eq!(error.code(), "E_HIVEWIRE_TOKEN_INVALID");
    assert!(!error.how_to_fix().is_empty());
    assert_eq!(session_count(&database), 0, "refused handshake must not create a session");
    assert!(store.queue_depth().expect("depth") == 0);

    let ack = store.handshake(&request, TOKEN).expect("valid token");
    assert_eq!(ack.protocol, PROTOCOL_VERSION);
    assert_eq!(ack.session_id.len(), 36);
    assert_eq!(ack.next_sequence, 1);
    assert!(!ack.replay_required);
    assert_eq!(session_count(&database), 1);
}

#[test]
fn handshake_refuses_a_weak_token_configuration() {
    let (_temp, database) = database();
    let weak = HiveWireConfig::new(REMOTE, "inproc://hive-a/remote-a", "short");
    let error = HiveWireStore::open(database, weak).expect_err("weak token");
    assert_eq!(error.code(), "E_HIVEWIRE_TOKEN_CONFIG");
}

#[test]
fn duplicate_message_is_deduped_and_never_queued_twice() {
    let (_temp, database) = database();
    let store = HiveWireStore::open(database.clone(), config()).expect("store");
    let ack = store.handshake(&hello("hello-1"), TOKEN).expect("handshake");

    let frame = task_frame(&ack.session_id, ack.next_sequence, "msg-1", "task-1");
    assert_eq!(
        store.accept(&frame).expect("first accept"),
        hivewire::AcceptOutcome::Queued { sequence: 1 }
    );
    assert_eq!(
        store.accept(&frame).expect("resend is not a failure"),
        hivewire::AcceptOutcome::Duplicate
    );
    assert_eq!(store.queue_depth().expect("depth"), 1);
    assert_eq!(store.pending_tasks(10).expect("pending").len(), 1);
    assert_eq!(
        store.message_state("msg-1").expect("state"),
        Some(MessageState::Queued)
    );

    // Reusing the sequence with a *new* message id is a sequence gap, not a
    // second queue row.
    let replayed = task_frame(&ack.session_id, 1, "msg-2", "task-2");
    let error = store.accept(&replayed).expect_err("sequence gap");
    assert_eq!(error.code(), "E_HIVEWIRE_SEQUENCE");
    assert_eq!(store.queue_depth().expect("depth"), 1);

    let next = task_frame(&ack.session_id, 2, "msg-2", "task-2");
    assert_eq!(
        store.accept(&next).expect("next sequence"),
        hivewire::AcceptOutcome::Queued { sequence: 2 }
    );
    assert_eq!(store.queue_depth().expect("depth"), 2);
}

#[test]
fn unconfigured_remote_refuses_by_name() {
    let (_temp, database) = database();
    let error = HiveWireStore::open(database, HiveWireConfig::unconfigured("remote-z"))
        .expect_err("unconfigured remote");
    assert_eq!(error.code(), "E_HIVEWIRE_REMOTE_UNCONFIGURED");
    assert!(error.to_string().contains("remote-z"));
    assert!(!error.how_to_fix().is_empty());
}

#[test]
fn a_network_endpoint_is_refused_instead_of_silently_succeeding() {
    let (_temp, database) = database();
    let config = HiveWireConfig::new(REMOTE, "http://127.0.0.1:9999/hivewire", TOKEN);
    let error = HiveWireStore::open(database, config).expect_err("network transport");
    assert_eq!(error.code(), "E_HIVEWIRE_TRANSPORT_UNSUPPORTED");
}

#[test]
fn envelope_is_versioned_and_validated() {
    let (_temp, database) = database();
    let store = HiveWireStore::open(database, config()).expect("store");
    let ack = store.handshake(&hello("hello-1"), TOKEN).expect("handshake");

    let queued = task_frame(&ack.session_id, ack.next_sequence, "msg-1", "task-1");
    assert_eq!(
        store.accept(&queued).expect("task"),
        hivewire::AcceptOutcome::Queued { sequence: 1 }
    );

    let mut frame = task_frame(&ack.session_id, 2, "msg-bad-version", "task-x");
    frame.protocol = "hivewire.v0".to_owned();
    assert_eq!(
        store.accept(&frame).expect_err("old protocol").code(),
        "E_HIVEWIRE_VERSION"
    );

    let mut frame = task_frame(&ack.session_id, 2, "msg-missing-sequence", "task-x");
    frame.sequence = None;
    assert_eq!(
        store.accept(&frame).expect_err("missing sequence").code(),
        "E_HIVEWIRE_BAD_FRAME"
    );

    let hello_frame = hello("hello-2");
    let encoded = serde_json::to_value(&hello_frame).expect("encode");
    assert_eq!(encoded["protocol"], PROTOCOL_VERSION);
    assert_eq!(encoded["body"]["type"], "hello");
    assert_eq!(encoded["body"]["payload"]["remote_id"], REMOTE);
    assert_eq!(
        encoded.get("session_id"),
        None,
        "hello must not carry a session id"
    );

    // A result for the queued task settles it exactly once.
    let result = WireEnvelope::post_handshake(
        &ack.session_id,
        2,
        "msg-result-1",
        MessageBody::Result(hivewire::TaskResult {
            task_id: "task-1".to_owned(),
            ok: true,
            output: serde_json::json!({"done": true}),
        }),
    );
    assert_eq!(
        store.accept(&result).expect("result"),
        hivewire::AcceptOutcome::Recorded { sequence: 2 }
    );
    assert_eq!(
        store.message_state("msg-1").expect("state"),
        Some(MessageState::Completed)
    );
}

#[test]
fn expired_session_is_refused_after_close() {
    let (_temp, database) = database();
    let store = HiveWireStore::open(database.clone(), config()).expect("store");
    let ack = store.handshake(&hello("hello-1"), TOKEN).expect("handshake");
    database
        .with_connection(|connection| {
            connection.execute(
                "UPDATE hivewire_sessions SET closed_at_ms = 1 WHERE session_id = ?1",
                rusqlite::params![ack.session_id],
            )
        })
        .expect("close session");
    let frame = task_frame(&ack.session_id, 1, "msg-1", "task-1");
    assert_eq!(
        store.accept(&frame).expect_err("closed session").code(),
        "E_HIVEWIRE_SESSION_UNKNOWN"
    );
}
