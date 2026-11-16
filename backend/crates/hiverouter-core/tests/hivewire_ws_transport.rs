//! Real loopback WebSocket transport tests for `/api/hivewire/ws`.
//!
//! Each test binds a real `127.0.0.1:0` listener, serves the transport through
//! `axum::serve` with `ConnectInfo`, and drives it with the RFC 6455 client in
//! `hivewire_ws_support` (masked client frames, verified server accept value).

mod db {
    pub use hiverouter_core::db::*;
}

#[path = "../src/hivewire.rs"]
mod hivewire;

mod hivewire_ws_support;

use std::net::SocketAddr;
use std::sync::Arc;

use hiverouter_core::db::Database;
use hivewire::{
    DaemonScheduler, Hello, HiveWireConfig, HiveWireStore, HiveWireTransport, LeaseIdentity,
    MessageBody, MessageState, TaskPayload, TaskResult, WireEnvelope, HIVEWIRE_WS_PATH,
    PROTOCOL_VERSION,
};
use hivewire_ws_support::{body_type, error_code, recv_json, upgrade_status, WsClient};
use serde_json::json;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::sync::oneshot;

const TOKEN: &str = "hivewire-ws-test-launch-token-0123456789";
const REMOTE: &str = "remote-a";
const ALLOWED_ORIGIN: &str = "http://127.0.0.1:9999";

fn database() -> (TempDir, Arc<Database>) {
    let temp = tempfile::tempdir().expect("tempdir");
    let database = Database::open(&temp.path().join("hivewire.db")).expect("open");
    database.migrate().expect("migrate");
    (temp, Arc::new(database))
}

fn store(database: Arc<Database>) -> Arc<HiveWireStore> {
    let config = HiveWireConfig::loopback_ws(REMOTE, 0, TOKEN);
    Arc::new(HiveWireStore::open(database, config).expect("store"))
}

/// A real loopback server plus its transport handle.
struct WsServer {
    addr: SocketAddr,
    transport: Arc<HiveWireTransport>,
    shutdown: Option<oneshot::Sender<()>>,
    handle: tokio::task::JoinHandle<()>,
}

impl WsServer {
    async fn start(transport: Arc<HiveWireTransport>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local addr");
        let app = hivewire::ws::router(transport.clone());
        let (shutdown, shutdown_rx) = oneshot::channel();
        let handle = tokio::spawn(async move {
            let _ = axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .with_graceful_shutdown(async move {
                let _ = shutdown_rx.await;
            })
            .await;
        });
        Self {
            addr,
            transport,
            shutdown: Some(shutdown),
            handle,
        }
    }
}

impl Drop for WsServer {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        self.handle.abort();
    }
}

fn hello(remote: &str, message_id: &str) -> WireEnvelope {
    WireEnvelope::hello(
        message_id,
        Hello {
            remote_id: remote.to_owned(),
            nonce: "ws-nonce-1".to_owned(),
            last_seen_sequence: 0,
        },
    )
}

fn task(session: &str, sequence: u64, message_id: &str, task_id: &str) -> WireEnvelope {
    WireEnvelope::post_handshake(
        session,
        sequence,
        message_id,
        MessageBody::Task(TaskPayload {
            task_id: task_id.to_owned(),
            kind: "probe".to_owned(),
            payload: json!({"ping": true}),
        }),
    )
}

fn heartbeat(session: &str, sequence: u64, message_id: &str) -> WireEnvelope {
    WireEnvelope::post_handshake(
        session,
        sequence,
        message_id,
        MessageBody::Heartbeat(hivewire::Heartbeat {
            remote_id: REMOTE.to_owned(),
            inflight: 0,
        }),
    )
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

async fn send_envelope(client: &mut WsClient, envelope: &WireEnvelope) {
    let text = serde_json::to_string(envelope).expect("encode envelope");
    client.send_text(&text).await.expect("send envelope");
}

async fn handshake(client: &mut WsClient) -> (String, u64) {
    send_envelope(client, &hello(REMOTE, "hello-ws-1")).await;
    let reply = recv_json(client).await;
    assert_eq!(body_type(&reply), "hello_ack", "reply: {reply}");
    assert_eq!(reply["protocol"], PROTOCOL_VERSION);
    assert_eq!(reply["body"]["payload"]["protocol"], PROTOCOL_VERSION);
    assert_eq!(reply["body"]["payload"]["replay_required"], false);
    let session = reply["body"]["payload"]["session_id"]
        .as_str()
        .expect("session id")
        .to_owned();
    let next = reply["body"]["payload"]["next_sequence"]
        .as_u64()
        .expect("next sequence");
    (session, next)
}

#[tokio::test]
async fn loopback_ws_roundtrip_hello_task_lease_and_result() {
    let (_temp, database) = database();
    let store = store(database.clone());
    let transport = Arc::new(HiveWireTransport::new(store.clone()));
    let server = WsServer::start(transport).await;

    let mut client = WsClient::connect(server.addr, HIVEWIRE_WS_PATH, None, Some(TOKEN))
        .await
        .expect("real WebSocket upgrade");

    let (session, next) = handshake(&mut client).await;
    assert_eq!(session.len(), 36, "a real uuid session id");
    assert_eq!(next, 1);

    // One task frame, one acknowledgement, one durable queue row.
    send_envelope(
        &mut client,
        &task(&session, 1, "msg-ws-task-1", "task-ws-1"),
    )
    .await;
    let reply = recv_json(&mut client).await;
    assert_eq!(body_type(&reply), "ack", "reply: {reply}");
    assert_eq!(reply["body"]["payload"]["kind"], "task");
    assert_eq!(reply["body"]["payload"]["sequence"], 1);
    assert_eq!(reply["body"]["payload"]["outcome"], "queued");
    assert_eq!(store.queue_depth().expect("depth"), 1);
    assert_eq!(
        store.message_state("msg-ws-task-1").expect("state"),
        Some(MessageState::Queued)
    );

    // The daemon scheduler leases it; the result must carry that exact lease.
    let scheduler = DaemonScheduler::new(database.clone(), 5_000, 3, 4)
        .expect("scheduler")
        .with_generation("gen-ws-test");
    let tick = scheduler.tick("daemon-ws-test").expect("tick");
    assert_eq!(tick.leased.len(), 1);
    let leased = &tick.leased[0];
    assert_eq!(leased.lease_owner, "daemon-ws-test");
    assert_eq!(leased.attempts, 1);

    send_envelope(
        &mut client,
        &result_frame(
            &session,
            2,
            "msg-ws-result-1",
            "task-ws-1",
            leased.lease_identity(),
        ),
    )
    .await;
    let reply = recv_json(&mut client).await;
    assert_eq!(body_type(&reply), "ack", "reply: {reply}");
    assert_eq!(reply["body"]["payload"]["kind"], "result");
    assert_eq!(reply["body"]["payload"]["outcome"], "recorded");
    assert_eq!(
        store.message_state("msg-ws-task-1").expect("state"),
        Some(MessageState::Completed)
    );
    assert_eq!(scheduler.queue_depth().expect("depth"), 0);

    // Resending the same result is a duplicate acknowledgement with no second
    // effect and consumes no *additional* sequence: the next frame is 3.
    send_envelope(
        &mut client,
        &result_frame(
            &session,
            2,
            "msg-ws-result-1",
            "task-ws-1",
            leased.lease_identity(),
        ),
    )
    .await;
    let duplicate = recv_json(&mut client).await;
    assert_eq!(body_type(&duplicate), "ack", "reply: {duplicate}");
    assert_eq!(duplicate["body"]["payload"]["outcome"], "duplicate");
    assert_eq!(
        store.message_state("msg-ws-task-1").expect("state"),
        Some(MessageState::Completed)
    );

    send_envelope(&mut client, &heartbeat(&session, 3, "msg-ws-heartbeat-1")).await;
    let reply = recv_json(&mut client).await;
    assert_eq!(body_type(&reply), "ack", "reply: {reply}");
    assert_eq!(reply["body"]["payload"]["kind"], "heartbeat");
    assert_eq!(reply["body"]["payload"]["sequence"], 3);

    client.close().await;
}

#[tokio::test]
async fn ws_bad_token_is_refused_before_upgrade() {
    let (_temp, database) = database();
    let store = store(database);
    let transport = Arc::new(HiveWireTransport::new(store));
    let server = WsServer::start(transport).await;

    let response = upgrade_status(
        server.addr,
        HIVEWIRE_WS_PATH,
        None,
        Some("wrong-hivewire-launch-token-000000"),
    )
    .await;
    assert_eq!(response.status, 401, "body: {}", response.body_text());
    assert!(
        response.body_text().contains("E_HIVEWIRE_TOKEN_INVALID"),
        "body: {}",
        response.body_text()
    );
    assert!(
        !response.body_text().contains(TOKEN),
        "never echo the token"
    );

    let missing = upgrade_status(server.addr, HIVEWIRE_WS_PATH, None, None).await;
    assert_eq!(missing.status, 401, "body: {}", missing.body_text());
    assert!(missing.body_text().contains("E_HIVEWIRE_TOKEN_INVALID"));
}

#[tokio::test]
async fn ws_origin_policy_refuses_bad_and_missing_origin() {
    let (_temp, database) = database();
    let transport =
        Arc::new(HiveWireTransport::new(store(database)).with_allowed_origins([ALLOWED_ORIGIN]));
    let server = WsServer::start(transport).await;

    let bad = upgrade_status(
        server.addr,
        HIVEWIRE_WS_PATH,
        Some("http://evil.example"),
        Some(TOKEN),
    )
    .await;
    assert_eq!(bad.status, 403, "body: {}", bad.body_text());
    assert!(
        bad.body_text().contains("E_HIVEWIRE_ORIGIN_REJECTED"),
        "body: {}",
        bad.body_text()
    );

    let missing = upgrade_status(server.addr, HIVEWIRE_WS_PATH, None, Some(TOKEN)).await;
    assert_eq!(missing.status, 403, "body: {}", missing.body_text());
    assert!(missing.body_text().contains("E_HIVEWIRE_ORIGIN_REJECTED"));

    let allowed = upgrade_status(
        server.addr,
        HIVEWIRE_WS_PATH,
        Some(ALLOWED_ORIGIN),
        Some(TOKEN),
    )
    .await;
    assert_eq!(allowed.status, 101, "allowed Origin must upgrade");
    assert!(
        allowed.upgraded(),
        "RFC 6455 accept value: {}",
        allowed.headers
    );
}

#[tokio::test]
async fn ws_hello_for_another_remote_is_refused_by_name() {
    let (_temp, database) = database();
    let transport = Arc::new(HiveWireTransport::new(store(database)));
    let server = WsServer::start(transport).await;
    let mut client = WsClient::connect(server.addr, HIVEWIRE_WS_PATH, None, Some(TOKEN))
        .await
        .expect("upgrade");

    send_envelope(&mut client, &hello("remote-b", "hello-ws-wrong-remote")).await;
    let reply = recv_json(&mut client).await;
    assert_eq!(body_type(&reply), "error", "reply: {reply}");
    assert_eq!(error_code(&reply), "E_HIVEWIRE_REMOTE_MISMATCH");
    assert!(
        client.recv_is_closed().await,
        "a mismatched hello must close the connection"
    );
}

#[tokio::test]
async fn ws_bad_frame_is_named_and_keeps_the_connection_usable() {
    let (_temp, database) = database();
    let store = store(database);
    let transport = Arc::new(HiveWireTransport::new(store));
    let server = WsServer::start(transport).await;
    let mut client = WsClient::connect(server.addr, HIVEWIRE_WS_PATH, None, Some(TOKEN))
        .await
        .expect("upgrade");
    let (session, _) = handshake(&mut client).await;

    client
        .send_text("{not a hivewire envelope")
        .await
        .expect("send bad frame");
    let reply = recv_json(&mut client).await;
    assert_eq!(body_type(&reply), "error", "reply: {reply}");
    assert_eq!(error_code(&reply), "E_HIVEWIRE_BAD_FRAME");

    // The refused frame did not consume sequence 1, and the connection is alive.
    send_envelope(
        &mut client,
        &heartbeat(&session, 1, "msg-ws-after-bad-frame"),
    )
    .await;
    let reply = recv_json(&mut client).await;
    assert_eq!(body_type(&reply), "ack", "reply: {reply}");
    assert_eq!(reply["body"]["payload"]["sequence"], 1);
}

#[tokio::test]
async fn ws_oversized_frame_is_refused_and_closed() {
    let (_temp, database) = database();
    let transport = Arc::new(
        HiveWireTransport::new(store(database)).with_max_frame_bytes(hivewire::ws::MIN_FRAME_BYTES),
    );
    let server = WsServer::start(transport).await;
    let mut client = WsClient::connect(server.addr, HIVEWIRE_WS_PATH, None, Some(TOKEN))
        .await
        .expect("upgrade");
    let _ = handshake(&mut client).await;

    let oversized = "x".repeat(hivewire::ws::MIN_FRAME_BYTES + 64);
    client
        .send_text(&oversized)
        .await
        .expect("send oversized frame");
    let reply = recv_json(&mut client).await;
    assert_eq!(body_type(&reply), "error", "reply: {reply}");
    assert_eq!(error_code(&reply), "E_HIVEWIRE_FRAME_TOO_LARGE");
    assert!(
        client.recv_is_closed().await,
        "an oversized frame must close the connection"
    );
}

#[tokio::test]
async fn ws_binary_frame_is_refused() {
    let (_temp, database) = database();
    let transport = Arc::new(HiveWireTransport::new(store(database)));
    let server = WsServer::start(transport).await;
    let mut client = WsClient::connect(server.addr, HIVEWIRE_WS_PATH, None, Some(TOKEN))
        .await
        .expect("upgrade");
    let _ = handshake(&mut client).await;

    client
        .send_binary(&[0xDE, 0xAD, 0xBE, 0xEF])
        .await
        .expect("send binary");
    let reply = recv_json(&mut client).await;
    assert_eq!(body_type(&reply), "error", "reply: {reply}");
    assert_eq!(error_code(&reply), "E_HIVEWIRE_BAD_FRAME");
    assert!(client.recv_is_closed().await);
}

#[tokio::test]
async fn ws_handshake_timeout_is_named() {
    let (_temp, database) = database();
    let transport = Arc::new(
        HiveWireTransport::new(store(database))
            .with_handshake_timeout(std::time::Duration::from_millis(100)),
    );
    let server = WsServer::start(transport).await;
    let mut client = WsClient::connect(server.addr, HIVEWIRE_WS_PATH, None, Some(TOKEN))
        .await
        .expect("upgrade");

    // No hello is sent.
    let reply = recv_json(&mut client).await;
    assert_eq!(body_type(&reply), "error", "reply: {reply}");
    assert_eq!(error_code(&reply), "E_HIVEWIRE_HANDSHAKE_TIMEOUT");
}

#[tokio::test]
async fn ws_idle_timeout_is_named() {
    let (_temp, database) = database();
    let transport = Arc::new(
        HiveWireTransport::new(store(database))
            .with_idle_timeout(std::time::Duration::from_millis(100)),
    );
    let server = WsServer::start(transport).await;
    let mut client = WsClient::connect(server.addr, HIVEWIRE_WS_PATH, None, Some(TOKEN))
        .await
        .expect("upgrade");
    let _ = handshake(&mut client).await;

    let reply = recv_json(&mut client).await;
    assert_eq!(body_type(&reply), "error", "reply: {reply}");
    assert_eq!(error_code(&reply), "E_HIVEWIRE_IDLE_TIMEOUT");
}

#[tokio::test]
async fn ws_hostile_session_id_cannot_hijack_another_connection() {
    let (_temp, database) = database();
    let transport = Arc::new(HiveWireTransport::new(store(database)));
    let server = WsServer::start(transport).await;

    let mut first = WsClient::connect(server.addr, HIVEWIRE_WS_PATH, None, Some(TOKEN))
        .await
        .expect("first upgrade");
    let (first_session, _) = handshake(&mut first).await;

    let mut second = WsClient::connect(server.addr, HIVEWIRE_WS_PATH, None, Some(TOKEN))
        .await
        .expect("second upgrade");
    let (_second_session, _) = handshake(&mut second).await;

    // The second socket names the first connection's session.
    send_envelope(&mut second, &heartbeat(&first_session, 1, "msg-ws-hijack")).await;
    let reply = recv_json(&mut second).await;
    assert_eq!(body_type(&reply), "error", "reply: {reply}");
    assert_eq!(error_code(&reply), "E_HIVEWIRE_SESSION_UNKNOWN");
}
