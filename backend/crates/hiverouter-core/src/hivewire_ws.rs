//! HiveWire loopback WebSocket transport.
//!
//! One real axum upgrade handler, `GET /api/hivewire/ws`. It is a plaintext
//! loopback transport: there is **no TLS** here and none is claimed. The
//! handler refuses a non-loopback peer, an unlistable `Origin`, and a bearer
//! launch token that does not match the stored SHA-256 digest (constant-time
//! compare) *before* upgrading. After the upgrade:
//!
//! 1. the client must send exactly one [`WireEnvelope`] `hello` within
//!    [`HANDSHAKE_TIMEOUT`] (5 s); its `remote_id` must equal the configured
//!    remote;
//! 2. the server answers with a [`ServerEnvelope`] `hello_ack`;
//! 3. every later frame is one [`WireEnvelope`] and is answered with one
//!    `ack` or one named `error` reply;
//! 4. a connection that sends nothing for [`IDLE_TIMEOUT`] (30 s) is closed;
//! 5. a text frame above the configured ceiling is refused with
//!    `E_HIVEWIRE_FRAME_TOO_LARGE` and the connection is closed with 1009.
//!
//! Sequence rule: an accepted frame consumes its sequence. A frame refused as
//! a *bad frame* or a *sequence gap* does not, so the client resends the
//! correction with the same sequence. A frame refused for a live-lease or
//! capacity reason (`E_HIVEWIRE_LEASE_LOST`, `E_HIVEWIRE_QUEUE_FULL`) **does**
//! consume its sequence (the store advances the session), so a late completion
//! cannot wedge the connection.
//!
//! # Wiring
//!
//! ```ignore
//! // state-agnostic handler: works on any `Router<S>` (before or after
//! // `with_state`), because it extracts no router state.
//! let app = app.route(
//!     hiverouter_core::hivewire::HIVEWIRE_WS_PATH,
//!     axum::routing::get(hiverouter_core::hivewire::ws::handler(transport)),
//! );
//! // or, for a state-free router to merge into `Router<()>`:
//! let app = app.merge(hiverouter_core::hivewire::ws::router(transport));
//! ```
//!
//! Serve with `into_make_service_with_connect_info::<SocketAddr>()`; the
//! loopback-peer refusal depends on `ConnectInfo`.
//!
//! Wiring note for the HTTP owner: merging the router after `with_state` (as
//! the current `build_router` tail allows) also bypasses the app-wide
//! `host_guard`/`mutation_auth` layers; this transport performs its own
//! loopback, Origin and Bearer checks. If the route is instead mounted inside
//! those layers, exclude `/api/hivewire/ws` from the launcher-token `GET
//! /api/*` gate: it authenticates with the HiveWire launch token, not the UI
//! session token.

use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::ws::{CloseFrame, Message, Utf8Bytes, WebSocket, WebSocketUpgrade};
use axum::extract::ConnectInfo;
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use serde_json::json;

use super::remote_store::{HandshakeAck, HiveWireStore};
use super::{
    AckKind, HiveWireError, MessageBody, ServerEnvelope, WireEnvelope, MAX_FRAME_BYTES,
    PROTOCOL_VERSION,
};

/// Route the transport serves.
pub const HIVEWIRE_WS_PATH: &str = "/api/hivewire/ws";

/// Deadline for the client's `hello` frame after the upgrade.
pub const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);

/// A connection that sends no frame for this long is closed.
pub const IDLE_TIMEOUT: Duration = Duration::from_secs(30);

/// Smallest accepted per-message ceiling (the `hello` and reply envelopes must
/// still fit).
pub const MIN_FRAME_BYTES: usize = 1024;

/// Extra bytes allowed on top of the enforced ceiling so the server can see a
/// slightly oversized message and refuse it *by name* before the protocol
/// layer drops the connection.
const FRAME_SIZE_SLACK_BYTES: usize = 1024;

/// WebSocket close codes used by the transport (RFC 6455).
const CLOSE_PROTOCOL_ERROR: u16 = 1002;
const CLOSE_POLICY_VIOLATION: u16 = 1008;
const CLOSE_MESSAGE_TOO_BIG: u16 = 1009;
const CLOSE_INTERNAL_ERROR: u16 = 1011;

/// A configured HiveWire WebSocket transport.
///
/// The transport owns the [`HiveWireStore`] (and therefore the same durable
/// queue the daemon scheduler leases from). It is `Send + Sync`; one instance
/// can serve many connections and be mounted on the shared router.
#[derive(Debug, Clone)]
pub struct HiveWireTransport {
    store: Arc<HiveWireStore>,
    allowed_origins: Vec<String>,
    handshake_timeout: Duration,
    idle_timeout: Duration,
    max_frame_bytes: usize,
}

impl HiveWireTransport {
    /// Builds a transport with the production defaults: no Origin allowlist
    /// (native clients send no `Origin`), 5 s handshake, 30 s idle, 64 KiB
    /// frame ceiling.
    #[must_use]
    pub fn new(store: Arc<HiveWireStore>) -> Self {
        Self {
            store,
            allowed_origins: Vec::new(),
            handshake_timeout: HANDSHAKE_TIMEOUT,
            idle_timeout: IDLE_TIMEOUT,
            max_frame_bytes: MAX_FRAME_BYTES,
        }
    }

    /// Allows the exact (case-insensitive) Origins in `origins`.
    ///
    /// An empty allowlist means "native client": a missing `Origin` is accepted
    /// because no browser is expected to connect. Once at least one Origin is
    /// configured, a missing or non-matching `Origin` is refused.
    #[must_use]
    pub fn with_allowed_origins<I, S>(mut self, origins: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.allowed_origins = origins.into_iter().map(Into::into).collect();
        self
    }

    /// Overrides the `hello` deadline.
    #[must_use]
    pub fn with_handshake_timeout(mut self, timeout: Duration) -> Self {
        self.handshake_timeout = timeout;
        self
    }

    /// Overrides the idle deadline.
    #[must_use]
    pub fn with_idle_timeout(mut self, timeout: Duration) -> Self {
        self.idle_timeout = timeout;
        self
    }

    /// Overrides the per-frame ceiling, clamped to `[MIN_FRAME_BYTES,
    /// MAX_FRAME_BYTES]`.
    #[must_use]
    pub fn with_max_frame_bytes(mut self, bytes: usize) -> Self {
        self.max_frame_bytes = bytes.clamp(MIN_FRAME_BYTES, MAX_FRAME_BYTES);
        self
    }

    /// The durable store this transport serves.
    #[must_use]
    pub fn store(&self) -> &Arc<HiveWireStore> {
        &self.store
    }

    /// Configured per-frame ceiling.
    #[must_use]
    pub fn max_frame_bytes(&self) -> usize {
        self.max_frame_bytes
    }

    /// The hard limit handed to the protocol layer.
    fn protocol_frame_limit(&self) -> usize {
        self.max_frame_bytes.saturating_add(FRAME_SIZE_SLACK_BYTES)
    }

    fn handshake_timeout(&self) -> Duration {
        self.handshake_timeout
    }

    fn idle_timeout(&self) -> Duration {
        self.idle_timeout
    }

    fn origin_allowed(&self, origin: Option<&str>) -> bool {
        if self.allowed_origins.is_empty() {
            return true;
        }
        origin.is_some_and(|origin| {
            self.allowed_origins
                .iter()
                .any(|allowed| allowed.eq_ignore_ascii_case(origin))
        })
    }
}

/// The boxed future returned by [`handler`].
type ResponseFuture = Pin<Box<dyn Future<Output = Response> + Send + 'static>>;

/// Returns a state-agnostic axum handler for [`HIVEWIRE_WS_PATH`].
///
/// The closure captures the transport and extracts no router state, so it can
/// be mounted on any `Router<S>` (before or after `with_state`). Serve with
/// `into_make_service_with_connect_info::<SocketAddr>()`.
pub fn handler(
    transport: Arc<HiveWireTransport>,
) -> impl Clone
       + Fn(WebSocketUpgrade, ConnectInfo<SocketAddr>, HeaderMap) -> ResponseFuture
       + Send
       + Sync
       + 'static {
    move |upgrade, ConnectInfo(peer), headers| {
        let transport = Arc::clone(&transport);
        let future: ResponseFuture =
            Box::pin(async move { handle_upgrade(upgrade, transport, peer, headers).await });
        future
    }
}

/// Builds a complete `Router<()>` with only the HiveWire WebSocket route.
///
/// Merge it into a `Router<()>` (for example after `with_state`) or serve it
/// directly with `into_make_service_with_connect_info::<SocketAddr>()`.
pub fn router(transport: Arc<HiveWireTransport>) -> Router {
    Router::new().route(HIVEWIRE_WS_PATH, get(handler(transport)))
}

/// Pre-upgrade gate, then the WebSocket session loop.
async fn handle_upgrade(
    upgrade: WebSocketUpgrade,
    transport: Arc<HiveWireTransport>,
    peer: SocketAddr,
    headers: HeaderMap,
) -> Response {
    if !peer.ip().is_loopback() {
        return refusal_response(
            &HiveWireError::OriginRejected { origin: None },
            "the HiveWire WebSocket transport is loopback-only",
        );
    }

    let origin = headers
        .get(header::ORIGIN)
        .and_then(|value| value.to_str().ok());
    if !transport.origin_allowed(origin) {
        return refusal_response(
            &HiveWireError::OriginRejected {
                origin: origin.map(str::to_owned),
            },
            "the Origin is not allowlisted for the HiveWire transport",
        );
    }

    let Some(token) = bearer_token(&headers) else {
        return refusal_response(
            &HiveWireError::TokenRejected {
                remote_id: transport.store.remote_id().to_owned(),
            },
            "send Authorization: Bearer <launch token>",
        );
    };
    if let Err(error) = transport.store.authenticate_token(token) {
        return refusal_response(&error, "the bearer launch token was rejected");
    }

    let limit = transport.protocol_frame_limit();
    let token = token.to_owned();
    upgrade
        .max_message_size(limit)
        .max_frame_size(limit)
        .on_upgrade(move |socket| connection_loop(socket, transport, token))
}

/// Builds a pre-upgrade JSON refusal (never a fake 101).
fn refusal_response(error: &HiveWireError, detail: &str) -> Response {
    let status = StatusCode::from_u16(error.http_status()).unwrap_or(StatusCode::BAD_REQUEST);
    let payload = json!({
        "code": error.code(),
        "message": error.to_string(),
        "how_to_fix": error.how_to_fix(),
        "retryable": error.retryable(),
        "detail": detail,
        "transport": "websocket-loopback",
        "tls": false,
        "endpoint": HIVEWIRE_WS_PATH,
    });
    (
        status,
        [(header::CACHE_CONTROL, "no-store")],
        axum::Json(payload),
    )
        .into_response()
}

/// Extracts a `Bearer` launch token from the `Authorization` header.
///
/// The token is never accepted from the query string or a cookie: a URL leaks
/// into logs and history.
fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    let value = headers.get(header::AUTHORIZATION)?.to_str().ok()?;
    let (scheme, token) = value.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("Bearer") {
        return None;
    }
    let token = token.trim();
    if token.is_empty() {
        None
    } else {
        Some(token)
    }
}

async fn connection_loop(mut socket: WebSocket, transport: Arc<HiveWireTransport>, token: String) {
    let handshake = tokio::time::timeout(
        transport.handshake_timeout(),
        handshake_exchange(&mut socket, &transport, &token),
    )
    .await;
    let ack = match handshake {
        Ok(Ok(ack)) => ack,
        Ok(Err(error)) => {
            send_error_and_close(&mut socket, None, None, &error).await;
            return;
        }
        Err(_) => {
            let error = HiveWireError::HandshakeTimeout {
                timeout_ms: duration_millis(transport.handshake_timeout()),
            };
            send_error_and_close(&mut socket, None, None, &error).await;
            return;
        }
    };

    loop {
        let incoming = tokio::time::timeout(transport.idle_timeout(), socket.recv()).await;
        let message = match incoming {
            Err(_) => {
                let error = HiveWireError::IdleTimeout {
                    timeout_ms: duration_millis(transport.idle_timeout()),
                };
                send_error_and_close(&mut socket, Some(&ack.session_id), None, &error).await;
                return;
            }
            Ok(None) => return,
            Ok(Some(Err(error))) => {
                tracing::warn!(
                    remote = %ack.session_id,
                    %error,
                    "HiveWire WebSocket read failed; closing the connection"
                );
                return;
            }
            Ok(Some(Ok(message))) => message,
        };
        match message {
            Message::Text(text) => {
                if handle_client_text(&mut socket, &transport, &ack, text.as_str())
                    .await
                    .is_err()
                {
                    return;
                }
            }
            Message::Binary(_) => {
                let error = HiveWireError::BadFrame {
                    detail: "binary frames are not HiveWire JSON envelopes".to_owned(),
                };
                send_error_and_close(&mut socket, Some(&ack.session_id), None, &error).await;
                return;
            }
            // axum answers ping frames automatically; a ping still counts as
            // activity because it resets the idle deadline above.
            Message::Ping(_) | Message::Pong(_) => {}
            Message::Close(_) => return,
        }
    }
}

/// Reads exactly one `hello` envelope and answers with `hello_ack`.
async fn handshake_exchange(
    socket: &mut WebSocket,
    transport: &HiveWireTransport,
    token: &str,
) -> Result<HandshakeAck, HiveWireError> {
    loop {
        match socket.recv().await {
            Some(Ok(Message::Text(text))) => {
                if text.len() > transport.max_frame_bytes() {
                    return Err(HiveWireError::FrameTooLarge {
                        bytes: text.len(),
                        limit: transport.max_frame_bytes(),
                    });
                }
                let envelope: WireEnvelope =
                    serde_json::from_str(text.as_str()).map_err(|error| {
                        HiveWireError::BadFrame {
                            detail: format!("hello frame is not a HiveWire envelope: {error}"),
                        }
                    })?;
                if !matches!(envelope.body, MessageBody::Hello(_)) {
                    return Err(HiveWireError::BadFrame {
                        detail: "the first frame after the upgrade must be a hello envelope"
                            .to_owned(),
                    });
                }
                let ack = transport.store.handshake(&envelope, token)?;
                let reply = ServerEnvelope::hello_ack(&ack, &envelope.message_id)?;
                send_envelope(socket, &reply)
                    .await
                    .map_err(|()| HiveWireError::Store {
                        detail: "the WebSocket closed before hello_ack was sent".to_owned(),
                    })?;
                return Ok(ack);
            }
            Some(Ok(Message::Binary(_))) => {
                return Err(HiveWireError::BadFrame {
                    detail: "binary frames are not HiveWire JSON envelopes".to_owned(),
                });
            }
            Some(Ok(Message::Ping(_) | Message::Pong(_))) => {}
            Some(Ok(Message::Close(_))) | None => {
                return Err(HiveWireError::BadFrame {
                    detail: "the connection closed before the hello handshake".to_owned(),
                });
            }
            Some(Err(error)) => {
                return Err(HiveWireError::BadFrame {
                    detail: format!("WebSocket read failed during the handshake: {error}"),
                });
            }
        }
    }
}

/// Accepts one post-handshake frame; `Err(())` means the connection must close.
async fn handle_client_text(
    socket: &mut WebSocket,
    transport: &HiveWireTransport,
    ack: &HandshakeAck,
    text: &str,
) -> Result<(), ()> {
    if text.len() > transport.max_frame_bytes() {
        let error = HiveWireError::FrameTooLarge {
            bytes: text.len(),
            limit: transport.max_frame_bytes(),
        };
        send_error_and_close(socket, Some(&ack.session_id), None, &error).await;
        return Err(());
    }

    let envelope: WireEnvelope = match serde_json::from_str(text) {
        Ok(envelope) => envelope,
        Err(error) => {
            let error = HiveWireError::BadFrame {
                detail: format!("frame is not a HiveWire envelope: {error}"),
            };
            return send_error(socket, Some(&ack.session_id), None, None, &error).await;
        }
    };

    // The connection is bound to the session its hello created. Another
    // connection's (valid) session id must not be usable from this socket.
    if envelope.session_id.as_deref() != Some(ack.session_id.as_str()) {
        let error = HiveWireError::SessionUnknown {
            session_id: envelope.session_id.unwrap_or_default(),
        };
        send_error_and_close(socket, Some(&ack.session_id), None, &error).await;
        return Err(());
    }

    let task_id = envelope.body.task_id().map(str::to_owned);
    match transport.store.accept(&envelope) {
        Ok(outcome) => {
            let (sequence, ack_kind) = match outcome {
                super::AcceptOutcome::Queued { sequence } => (sequence, AckKind::Queued),
                super::AcceptOutcome::Recorded { sequence } => (sequence, AckKind::Recorded),
                super::AcceptOutcome::Duplicate => {
                    (envelope.sequence.unwrap_or_default(), AckKind::Duplicate)
                }
            };
            let reply = match ServerEnvelope::ack(
                &ack.session_id,
                &envelope.message_id,
                envelope.body.kind(),
                sequence,
                ack_kind,
            ) {
                Ok(reply) => reply,
                Err(error) => {
                    send_error_and_close(socket, Some(&ack.session_id), None, &error).await;
                    return Err(());
                }
            };
            send_envelope(socket, &reply).await
        }
        Err(error) => {
            if consumes_sequence(&error) {
                if let Some(sequence) = envelope.sequence {
                    if let Err(consume_error) =
                        transport.store.consume_sequence(&ack.session_id, sequence)
                    {
                        send_error_and_close(socket, Some(&ack.session_id), None, &consume_error)
                            .await;
                        return Err(());
                    }
                }
            }
            if send_error(
                socket,
                Some(&ack.session_id),
                Some(&envelope.message_id),
                task_id.as_deref(),
                &error,
            )
            .await
            .is_err()
            {
                return Err(());
            }
            if fatal(&error) {
                let _ = socket
                    .send(Message::Close(Some(CloseFrame {
                        code: close_code(&error),
                        reason: Utf8Bytes::from_static("hivewire refused the frame"),
                    })))
                    .await;
                return Err(());
            }
            Ok(())
        }
    }
}

/// Sends one `ack`/`error` reply envelope.
async fn send_error(
    socket: &mut WebSocket,
    session_id: Option<&str>,
    reply_to: Option<&str>,
    task_id: Option<&str>,
    error: &HiveWireError,
) -> Result<(), ()> {
    let reply = ServerEnvelope::error(session_id, reply_to, task_id, error).map_err(|_| ())?;
    send_envelope(socket, &reply).await
}

/// Sends a refusal reply, then closes the connection with the matching code.
async fn send_error_and_close(
    socket: &mut WebSocket,
    session_id: Option<&str>,
    task_id: Option<&str>,
    error: &HiveWireError,
) {
    let _ = send_error(socket, session_id, None, task_id, error).await;
    let _ = socket
        .send(Message::Close(Some(CloseFrame {
            code: close_code(error),
            reason: Utf8Bytes::from_static("hivewire closed the connection"),
        })))
        .await;
}

async fn send_envelope(socket: &mut WebSocket, envelope: &ServerEnvelope) -> Result<(), ()> {
    let text = envelope.encode().map_err(|_| ())?;
    socket
        .send(Message::Text(text.into()))
        .await
        .map_err(|_| ())
}

/// A live-lease/capacity refusal persists nothing but consumes its sequence so
/// the connection can continue.
fn consumes_sequence(error: &HiveWireError) -> bool {
    matches!(
        error,
        HiveWireError::LeaseLost { .. } | HiveWireError::QueueFull { .. }
    )
}

/// A refusal that leaves the connection unusable.
fn fatal(error: &HiveWireError) -> bool {
    matches!(
        error,
        HiveWireError::SessionUnknown { .. }
            | HiveWireError::ProtocolMismatch { .. }
            | HiveWireError::TokenRejected { .. }
            | HiveWireError::RemoteMismatch { .. }
            | HiveWireError::UnsupportedTransport { .. }
            | HiveWireError::ConfigInvalid { .. }
            | HiveWireError::Store { .. }
    )
}

fn close_code(error: &HiveWireError) -> u16 {
    match error {
        HiveWireError::FrameTooLarge { .. } => CLOSE_MESSAGE_TOO_BIG,
        HiveWireError::Store { .. } => CLOSE_INTERNAL_ERROR,
        HiveWireError::ProtocolMismatch { .. } | HiveWireError::UnsupportedTransport { .. } => {
            CLOSE_PROTOCOL_ERROR
        }
        _ => CLOSE_POLICY_VIOLATION,
    }
}

fn duration_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

/// The exact protocol version this transport speaks (for diagnostics).
#[must_use]
pub fn protocol_version() -> &'static str {
    PROTOCOL_VERSION
}
