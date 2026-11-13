//! HiveWire v1 — local-only authenticated message contract.
//!
//! # What this is
//!
//! A versioned, authenticated, replay-safe message envelope (`hello`, `task`,
//! `result`, `error`, `heartbeat`), a durable bounded queue and a lease-based
//! daemon scheduler for remote work. It is a real protocol state machine: a bad
//! launch token is refused by code, a duplicate message is deduplicated, a full
//! queue refuses the message, and an expired lease requeues the task exactly
//! once. No path in this module reports success without a persisted row.
//!
//! # Exact transport (do not overclaim)
//!
//! This build has **no network transport**. Delivery is a direct in-process
//! call on the same host: the caller holds the [`HiveWireStore`] and passes a
//! [`WireEnvelope`] to [`HiveWireStore::accept`]. The only accepted endpoint
//! scheme is `inproc://<hive>/<remote>`; anything else is refused with
//! [`HiveWireError::UnsupportedTransport`] rather than silently "succeeding".
//! The messages and leases are durable in SQLite, so the daemon scheduler sees
//! the same tickets after a process restart. A TCP/WebSocket listener is *not*
//! implemented here (the HTTP/WS owners are outside this change's scope).
//!
//! # Wiring
//!
//! The crate root is outside this change's write scope, so `src/lib.rs` does not
//! yet declare `pub mod hivewire;`. Every path in these three files uses
//! `crate::`-relative imports only, so the module compiles unchanged once that
//! one line is added. Until then the integration tests mount it with
//! `#[path = "../src/hivewire.rs"] mod hivewire;` (the test crate re-exports
//! `hiverouter_core::db` at its root so `crate::db` resolves in both contexts).
//!
//! # Schema
//!
//! [`remote_store::ensure_schema`] creates the additive `hivewire_*` tables with
//! `CREATE TABLE IF NOT EXISTS`. This is deliberately *not* added to
//! [`crate::db::MIGRATIONS`] because that file is out of scope; the tables are
//! independent of the existing tables and the DDL is idempotent.

use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::Transaction;
use serde::{Deserialize, Serialize};

use crate::db::Database;

#[path = "remote_store.rs"]
pub mod remote_store;
#[path = "daemon_store.rs"]
pub mod daemon_store;

pub use daemon_store::{DaemonScheduler, ExpiryReport, LeasedTask, SchedulerTickReport};
pub use remote_store::{
    ensure_schema, AcceptOutcome, HandshakeAck, HiveWireStore, MessageState, PendingTask,
};

/// Protocol version written into (and checked on) every envelope.
pub const PROTOCOL_VERSION: &str = "hivewire.v1";

/// Hard ceiling for one encoded envelope. Larger frames are refused, never
/// truncated.
pub const MAX_FRAME_BYTES: usize = 64 * 1024;

/// Hard ceiling for one task payload. Enforced in addition to
/// [`MAX_FRAME_BYTES`] so a small kind cannot smuggle a large body.
pub const MAX_TASK_PAYLOAD_BYTES: usize = 32 * 1024;

/// Minimum launch-token length. Shorter tokens are a configuration refusal.
pub const MIN_LAUNCH_TOKEN_LEN: usize = 16;

/// Default bounded-queue capacity (queued + leased tasks).
pub const DEFAULT_QUEUE_CAPACITY: usize = 32;

/// Default lease duration for the daemon scheduler.
pub const DEFAULT_LEASE_MS: i64 = 30_000;

/// Default attempt ceiling before a task is terminally failed.
pub const DEFAULT_MAX_ATTEMPTS: u32 = 3;

/// Milliseconds since the Unix epoch (the clock the durable rows use).
#[must_use]
pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| i64::try_from(duration.as_millis()).unwrap_or(i64::MAX))
}

/// Stable SHA-256 digest of a launch token. The plaintext token is never stored.
#[must_use]
pub fn token_digest(token: &str) -> String {
    hiverouter_tools::sha256_hex(token.as_bytes())
}

/// Constant-time comparison of the stored token digest with a presented token.
#[must_use]
pub fn token_matches(expected_digest_hex: &str, presented: &str) -> bool {
    let presented_digest = token_digest(presented);
    let left = expected_digest_hex.as_bytes();
    let right = presented_digest.as_bytes();
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0u8, |acc, (a, b)| acc | (a ^ b))
        == 0
}

/// A named HiveWire failure. Every error path returns one of these codes; the
/// message is evidence, the code is the stable identity.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum HiveWireError {
    /// No endpoint or launch token is configured for the remote.
    #[error("HiveWire remote `{remote_id}` is not configured: an endpoint and launch token are required")]
    RemoteNotConfigured {
        /// Configured remote name.
        remote_id: String,
    },
    /// The endpoint names a transport this build does not implement.
    #[error("HiveWire endpoint `{endpoint}` is not a local in-process endpoint")]
    UnsupportedTransport {
        /// The rejected endpoint.
        endpoint: String,
    },
    /// The launch token configuration is invalid (too short / whitespace).
    #[error("HiveWire launch token configuration is invalid: {detail}")]
    TokenConfig {
        /// Why the token is unacceptable.
        detail: String,
    },
    /// The presented bearer token does not match the configured digest.
    #[error("the HiveWire launch token was rejected for remote `{remote_id}`")]
    TokenRejected {
        /// Remote the handshake was addressed to.
        remote_id: String,
    },
    /// The hello names a different remote than the configured one.
    #[error("HiveWire hello names remote `{presented}` but the link is configured for `{expected}`")]
    RemoteMismatch {
        /// Configured remote.
        expected: String,
        /// Remote named by the hello.
        presented: String,
    },
    /// The protocol version is not the one this build speaks.
    #[error("unsupported HiveWire protocol `{presented}` (this build speaks {PROTOCOL_VERSION})")]
    ProtocolMismatch {
        /// Version named by the frame.
        presented: String,
    },
    /// The envelope failed structural or payload validation.
    #[error("malformed HiveWire frame: {detail}")]
    BadFrame {
        /// Exact validation failure.
        detail: String,
    },
    /// The frame's sequence is not the next expected sequence.
    #[error("HiveWire sequence {presented} is not the next expected sequence {expected}")]
    SequenceGap {
        /// Server expectation.
        expected: u64,
        /// Value on the frame.
        presented: u64,
    },
    /// The durable queue is at capacity.
    #[error("the HiveWire queue is full ({capacity} tasks queued or leased)")]
    QueueFull {
        /// Configured capacity.
        capacity: usize,
    },
    /// The session id is unknown, closed, or belongs to another remote.
    #[error("HiveWire session `{session_id}` is unknown or closed")]
    SessionUnknown {
        /// Session id on the frame.
        session_id: String,
    },
    /// A completion was posted by a daemon that no longer holds the lease.
    #[error("HiveWire lease for message `{message_id}` is not held by daemon `{owner}`")]
    LeaseLost {
        /// Message whose lease was lost.
        message_id: String,
        /// Daemon that attempted the completion.
        owner: String,
    },
    /// A scheduler/config value is outside its bounds.
    #[error("invalid HiveWire scheduler configuration: {detail}")]
    ConfigInvalid {
        /// Which value is invalid and why.
        detail: String,
    },
    /// The SQLite store failed.
    #[error("HiveWire store failed: {detail}")]
    Store {
        /// Underlying database error text.
        detail: String,
    },
}

impl HiveWireError {
    /// Stable wire code for this failure.
    #[must_use]
    pub fn code(&self) -> &'static str {
        match self {
            Self::RemoteNotConfigured { .. } => "E_HIVEWIRE_REMOTE_UNCONFIGURED",
            Self::UnsupportedTransport { .. } => "E_HIVEWIRE_TRANSPORT_UNSUPPORTED",
            Self::TokenConfig { .. } => "E_HIVEWIRE_TOKEN_CONFIG",
            Self::TokenRejected { .. } => "E_HIVEWIRE_TOKEN_INVALID",
            Self::RemoteMismatch { .. } => "E_HIVEWIRE_REMOTE_MISMATCH",
            Self::ProtocolMismatch { .. } => "E_HIVEWIRE_VERSION",
            Self::BadFrame { .. } => "E_HIVEWIRE_BAD_FRAME",
            Self::SequenceGap { .. } => "E_HIVEWIRE_SEQUENCE",
            Self::QueueFull { .. } => "E_HIVEWIRE_QUEUE_FULL",
            Self::SessionUnknown { .. } => "E_HIVEWIRE_SESSION_UNKNOWN",
            Self::LeaseLost { .. } => "E_HIVEWIRE_LEASE_LOST",
            Self::ConfigInvalid { .. } => "E_HIVEWIRE_CONFIG",
            Self::Store { .. } => "E_HIVEWIRE_STORE",
        }
    }

    /// What the operator can do about the failure.
    #[must_use]
    pub fn how_to_fix(&self) -> &'static str {
        match self {
            Self::RemoteNotConfigured { .. } => {
                "Configure an inproc:// endpoint and a launch token of at least 16 characters for the remote."
            }
            Self::UnsupportedTransport { .. } => {
                "Use the local in-process endpoint (inproc://<hive>/<remote>); no network transport exists in this build."
            }
            Self::TokenConfig { .. } => {
                "Configure a launch token of at least 16 non-whitespace characters."
            }
            Self::TokenRejected { .. } => {
                "Present the launch token configured for this remote; refresh it from the remote's enrolment record."
            }
            Self::RemoteMismatch { .. } => {
                "Address the hello to the remote this link is configured for."
            }
            Self::ProtocolMismatch { .. } => {
                "Upgrade or downgrade the peer so both speak hivewire.v1."
            }
            Self::BadFrame { .. } => {
                "Fix the envelope fields; see the HiveWire v1 contract in this module."
            }
            Self::SequenceGap { .. } => {
                "Handshake again and resume from server_last_sequence; do not skip or replay sequences."
            }
            Self::QueueFull { .. } => {
                "Wait for the daemon scheduler to drain the queue, then resend the task."
            }
            Self::SessionUnknown { .. } => {
                "Run the hello handshake again to obtain a fresh session id."
            }
            Self::LeaseLost { .. } => {
                "Let the lease expire and re-lease the task; do not complete work you no longer own."
            }
            Self::ConfigInvalid { .. } => {
                "Use a positive lease, a positive attempt ceiling and a positive batch size."
            }
            Self::Store { .. } => {
                "Inspect the local SQLite error; the message was refused rather than reported as delivered."
            }
        }
    }
}

/// `hello` payload: identifies the remote and what it last saw.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Hello {
    /// Remote name; must equal the configured remote.
    pub remote_id: String,
    /// Client nonce (unique per connection attempt).
    pub nonce: String,
    /// Highest sequence the client believes it has received.
    pub last_seen_sequence: u64,
}

/// `task` payload: one unit of work for the daemon scheduler.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskPayload {
    /// Caller-chosen task id, stable across resends.
    pub task_id: String,
    /// Task kind (for example `shell` or `probe`); non-empty.
    pub kind: String,
    /// Opaque JSON the remote runner understands.
    pub payload: serde_json::Value,
}

/// `result` payload: the outcome of one leased task.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskResult {
    /// Task that produced this result.
    pub task_id: String,
    /// Whether the task succeeded.
    pub ok: bool,
    /// Opaque JSON result body.
    pub output: serde_json::Value,
}

/// `error` payload: a named remote failure.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskError {
    /// Task that failed, when the failure is task-scoped.
    pub task_id: Option<String>,
    /// Stable refusal code (`E_*`).
    pub code: String,
    /// Operator-readable cause.
    pub message: String,
}

/// `heartbeat` payload: liveness for one remote.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Heartbeat {
    /// Remote reporting liveness.
    pub remote_id: String,
    /// Number of tasks the remote believes are in flight.
    pub inflight: u32,
}

/// The five versioned message bodies.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "payload", rename_all = "snake_case")]
pub enum MessageBody {
    /// Handshake: opens a session.
    Hello(Hello),
    /// Work to enqueue.
    Task(TaskPayload),
    /// Outcome of a task.
    Result(TaskResult),
    /// Named failure.
    Error(TaskError),
    /// Liveness.
    Heartbeat(Heartbeat),
}

impl MessageBody {
    /// Stable kind string (`hello` / `task` / `result` / `error` / `heartbeat`).
    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Hello(_) => "hello",
            Self::Task(_) => "task",
            Self::Result(_) => "result",
            Self::Error(_) => "error",
            Self::Heartbeat(_) => "heartbeat",
        }
    }

    /// Task id carried by this body, when it carries one.
    #[must_use]
    pub fn task_id(&self) -> Option<&str> {
        match self {
            Self::Task(task) => Some(&task.task_id),
            Self::Result(result) => Some(&result.task_id),
            Self::Error(error) => error.task_id.as_deref(),
            Self::Hello(_) | Self::Heartbeat(_) => None,
        }
    }
}

/// The versioned HiveWire envelope. A `hello` carries no session/sequence (the
/// session does not exist yet); every other kind must carry both.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WireEnvelope {
    /// Protocol version; must equal [`PROTOCOL_VERSION`].
    pub protocol: String,
    /// Unique message id per remote; the idempotency key.
    pub message_id: String,
    /// Sender clock, milliseconds since the Unix epoch.
    pub sent_at_ms: i64,
    /// Session id assigned by the handshake (absent on `hello`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    /// Per-session sequence, starting at 1 (absent on `hello`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sequence: Option<u64>,
    /// Message body.
    pub body: MessageBody,
}

fn bad(detail: impl Into<String>) -> HiveWireError {
    HiveWireError::BadFrame {
        detail: detail.into(),
    }
}

impl WireEnvelope {
    /// Builds a `hello` envelope.
    #[must_use]
    pub fn hello(message_id: impl Into<String>, hello: Hello) -> Self {
        Self {
            protocol: PROTOCOL_VERSION.to_owned(),
            message_id: message_id.into(),
            sent_at_ms: now_ms(),
            session_id: None,
            sequence: None,
            body: MessageBody::Hello(hello),
        }
    }

    /// Builds a post-handshake envelope (task/result/error/heartbeat).
    #[must_use]
    pub fn post_handshake(
        session_id: impl Into<String>,
        sequence: u64,
        message_id: impl Into<String>,
        body: MessageBody,
    ) -> Self {
        Self {
            protocol: PROTOCOL_VERSION.to_owned(),
            message_id: message_id.into(),
            sent_at_ms: now_ms(),
            session_id: Some(session_id.into()),
            sequence: Some(sequence),
            body,
        }
    }

    /// Encoded length in bytes, for the frame bound.
    ///
    /// # Errors
    /// [`HiveWireError::BadFrame`] when the envelope cannot be encoded.
    pub fn encoded_len(&self) -> Result<usize, HiveWireError> {
        serde_json::to_vec(self)
            .map(|bytes| bytes.len())
            .map_err(|error| bad(format!("envelope is not encodable: {error}")))
    }

    fn validate_common(&self) -> Result<(), HiveWireError> {
        if self.protocol != PROTOCOL_VERSION {
            return Err(HiveWireError::ProtocolMismatch {
                presented: self.protocol.clone(),
            });
        }
        if self.message_id.trim().is_empty() || self.message_id.len() > 128 {
            return Err(bad("message_id must be 1..=128 characters"));
        }
        if self.sent_at_ms <= 0 {
            return Err(bad("sent_at_ms must be a positive Unix millisecond"));
        }
        let encoded = self.encoded_len()?;
        if encoded > MAX_FRAME_BYTES {
            return Err(bad(format!(
                "frame is {encoded} bytes (limit {MAX_FRAME_BYTES})"
            )));
        }
        Ok(())
    }

    /// Validates a `hello` frame and returns its payload.
    ///
    /// # Errors
    /// Any [`HiveWireError`] from envelope or hello validation.
    pub fn validate_hello(&self) -> Result<&Hello, HiveWireError> {
        self.validate_common()?;
        if self.session_id.is_some() || self.sequence.is_some() {
            return Err(bad("a hello frame must not carry session_id or sequence"));
        }
        let MessageBody::Hello(hello) = &self.body else {
            return Err(bad("a hello frame must carry the hello body"));
        };
        if hello.remote_id.trim().is_empty() || hello.remote_id.len() > 128 {
            return Err(bad("hello.remote_id must be 1..=128 characters"));
        }
        if hello.nonce.trim().is_empty() || hello.nonce.len() > 256 {
            return Err(bad("hello.nonce must be 1..=256 characters"));
        }
        Ok(hello)
    }

    /// Validates a post-handshake frame and returns `(session_id, sequence)`.
    ///
    /// # Errors
    /// Any [`HiveWireError`] from envelope or body validation.
    pub fn validate_post_handshake(&self) -> Result<(&str, u64), HiveWireError> {
        self.validate_common()?;
        if matches!(self.body, MessageBody::Hello(_)) {
            return Err(bad("hello is only valid during the handshake"));
        }
        let session_id = self
            .session_id
            .as_deref()
            .filter(|value| !value.is_empty() && value.len() <= 128)
            .ok_or_else(|| bad("session_id is required after the handshake"))?;
        let sequence = self
            .sequence
            .filter(|value| *value >= 1)
            .ok_or_else(|| bad("sequence must be >= 1 after the handshake"))?;
        if let MessageBody::Task(task) = &self.body {
            if task.task_id.trim().is_empty() || task.task_id.len() > 128 {
                return Err(bad("task.task_id must be 1..=128 characters"));
            }
            if task.kind.trim().is_empty() || task.kind.len() > 64 {
                return Err(bad("task.kind must be 1..=64 characters"));
            }
            let payload_len = serde_json::to_vec(&task.payload)
                .map_err(|error| bad(format!("task payload is not encodable: {error}")))?
                .len();
            if payload_len > MAX_TASK_PAYLOAD_BYTES {
                return Err(bad(format!(
                    "task payload is {payload_len} bytes (limit {MAX_TASK_PAYLOAD_BYTES})"
                )));
            }
        }
        if let MessageBody::Heartbeat(heartbeat) = &self.body {
            if heartbeat.remote_id.trim().is_empty() {
                return Err(bad("heartbeat.remote_id must not be empty"));
            }
        }
        Ok((session_id, sequence))
    }
}

/// Link configuration for one remote. `endpoint` and `launch_token` are both
/// required; an absent value is [`HiveWireError::RemoteNotConfigured`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HiveWireConfig {
    /// Remote name used in the handshake and on every queued task.
    pub remote_id: String,
    /// Endpoint. Only `inproc://<hive>/<remote>` is accepted in this build.
    pub endpoint: Option<String>,
    /// Launch token plaintext. It is hashed before it reaches the database.
    pub launch_token: Option<String>,
    /// Bounded queue capacity (queued + leased tasks).
    pub queue_capacity: usize,
}

impl HiveWireConfig {
    /// Fully configured remote.
    #[must_use]
    pub fn new(
        remote_id: impl Into<String>,
        endpoint: impl Into<String>,
        launch_token: impl Into<String>,
    ) -> Self {
        Self {
            remote_id: remote_id.into(),
            endpoint: Some(endpoint.into()),
            launch_token: Some(launch_token.into()),
            queue_capacity: DEFAULT_QUEUE_CAPACITY,
        }
    }

    /// A remote with no endpoint and no token: every operation must refuse.
    #[must_use]
    pub fn unconfigured(remote_id: impl Into<String>) -> Self {
        Self {
            remote_id: remote_id.into(),
            endpoint: None,
            launch_token: None,
            queue_capacity: DEFAULT_QUEUE_CAPACITY,
        }
    }

    /// Overrides the bounded queue capacity.
    #[must_use]
    pub fn with_queue_capacity(mut self, capacity: usize) -> Self {
        self.queue_capacity = capacity;
        self
    }

    fn validate_remote_id(&self) -> Result<(), HiveWireError> {
        let id = self.remote_id.trim();
        if id.is_empty() || id.len() > 128 {
            return Err(HiveWireError::RemoteNotConfigured {
                remote_id: self.remote_id.clone(),
            });
        }
        Ok(())
    }

    /// Validates the link configuration and the exact transport scheme.
    ///
    /// # Errors
    /// [`HiveWireError::RemoteNotConfigured`] when endpoint/token are absent,
    /// [`HiveWireError::UnsupportedTransport`] for a non-local scheme, and
    /// [`HiveWireError::TokenConfig`] for a weak token.
    pub fn validate(&self) -> Result<(), HiveWireError> {
        self.validate_remote_id()?;
        if self.queue_capacity == 0 {
            return Err(HiveWireError::ConfigInvalid {
                detail: "queue_capacity must be positive".to_owned(),
            });
        }
        let endpoint = self
            .endpoint
            .as_deref()
            .ok_or_else(|| HiveWireError::RemoteNotConfigured {
                remote_id: self.remote_id.clone(),
            })?;
        let Some(rest) = endpoint.strip_prefix("inproc://") else {
            return Err(HiveWireError::UnsupportedTransport {
                endpoint: endpoint.to_owned(),
            });
        };
        if rest.trim_matches('/').is_empty() {
            return Err(HiveWireError::UnsupportedTransport {
                endpoint: endpoint.to_owned(),
            });
        }
        let token = self
            .launch_token
            .as_deref()
            .ok_or_else(|| HiveWireError::RemoteNotConfigured {
                remote_id: self.remote_id.clone(),
            })?;
        if token.chars().count() < MIN_LAUNCH_TOKEN_LEN || token.chars().any(char::is_whitespace) {
            return Err(HiveWireError::TokenConfig {
                detail: format!("the launch token must be at least {MIN_LAUNCH_TOKEN_LEN} non-whitespace characters"),
            });
        }
        Ok(())
    }
}

/// Maps a SQLite error to the store failure.
pub(crate) fn store_error(error: rusqlite::Error) -> HiveWireError {
    HiveWireError::Store {
        detail: error.to_string(),
    }
}

/// Runs `f` inside one exclusive SQLite transaction.
///
/// The closure's named failure wins over a commit failure and the transaction
/// rolls back on every error path.
pub(crate) fn with_tx<T>(
    db: &Database,
    f: impl FnOnce(&Transaction<'_>) -> Result<T, HiveWireError>,
) -> Result<T, HiveWireError> {
    let mut slot: Option<Result<T, HiveWireError>> = None;
    let outer = db.with_connection(|connection| {
        let outcome = match connection.transaction() {
            Ok(tx) => match f(&tx) {
                Ok(value) => tx.commit().map(|()| value).map_err(store_error),
                Err(error) => Err(error),
            },
            Err(error) => Err(store_error(error)),
        };
        slot = Some(outcome);
        Ok(())
    });
    match outer {
        Ok(()) => slot.unwrap_or_else(|| {
            Err(HiveWireError::Store {
                detail: "transaction did not run".to_owned(),
            })
        }),
        Err(error) => Err(HiveWireError::Store {
            detail: error.to_string(),
        }),
    }
}
