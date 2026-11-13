//! HiveWire durable store: sessions, the bounded queue and the idempotency log.
//!
//! All state is SQLite in the workspace database. The link is local-only; see
//! [`super`] for the exact transport statement.

use std::sync::Arc;

use rusqlite::{params, OptionalExtension};
use serde::{Deserialize, Serialize};

use crate::db::Database;

use super::{
    now_ms, store_error, token_digest, token_matches, with_tx, HiveWireConfig, HiveWireError,
    MessageBody, WireEnvelope,
};

/// Creates the additive HiveWire tables. Idempotent; safe on every open.
///
/// This is a runtime DDL rather than a row in [`crate::db::MIGRATIONS`] because
/// `db.rs` is outside this change's write scope. The tables are new and no
/// existing table is altered.
///
/// # Errors
/// [`HiveWireError::Store`] when the DDL fails.
pub fn ensure_schema(db: &Database) -> Result<(), HiveWireError> {
    db.with_connection(|connection| {
        connection.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS hivewire_sessions (
                session_id TEXT PRIMARY KEY,
                remote_id TEXT NOT NULL,
                protocol TEXT NOT NULL,
                token_sha256 TEXT NOT NULL,
                last_seen_sequence INTEGER NOT NULL,
                last_heartbeat_ms INTEGER NOT NULL,
                created_at_ms INTEGER NOT NULL,
                closed_at_ms INTEGER
            );
            CREATE TABLE IF NOT EXISTS hivewire_messages (
                remote_id TEXT NOT NULL,
                message_id TEXT NOT NULL,
                session_id TEXT NOT NULL,
                sequence INTEGER NOT NULL,
                kind TEXT NOT NULL,
                task_id TEXT,
                body_json TEXT NOT NULL,
                state TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                queued_at_ms INTEGER NOT NULL,
                lease_until_ms INTEGER,
                lease_owner TEXT,
                completed_at_ms INTEGER,
                PRIMARY KEY (remote_id, message_id)
            );
            CREATE INDEX IF NOT EXISTS hivewire_messages_ready
                ON hivewire_messages(state, rowid);
            CREATE INDEX IF NOT EXISTS hivewire_messages_task
                ON hivewire_messages(remote_id, task_id);
            CREATE INDEX IF NOT EXISTS hivewire_sessions_remote
                ON hivewire_sessions(remote_id);
            ",
        )?;
        Ok(())
    })
    .map_err(|error| HiveWireError::Store {
        detail: error.to_string(),
    })
}

/// Acknowledgement returned by a successful handshake.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HandshakeAck {
    /// New session id to put on every subsequent envelope.
    pub session_id: String,
    /// Protocol version of the server (always [`super::PROTOCOL_VERSION`]).
    pub protocol: String,
    /// Highest sequence the server has stored for this remote.
    pub server_last_sequence: u64,
    /// Sequence the next post-handshake frame must carry.
    pub next_sequence: u64,
    /// True when the client claims to have seen more than the server has, so it
    /// must replay from the server's history instead of continuing.
    pub replay_required: bool,
}

/// Persisted lifecycle of one message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageState {
    /// Waiting to be leased.
    Queued,
    /// Leased by a daemon until the lease instant.
    Leased,
    /// Finished successfully.
    Completed,
    /// Terminally failed (attempt ceiling or named remote error).
    Failed,
    /// Result/error/heartbeat frame: recorded, not a schedulable task.
    Recorded,
}

impl MessageState {
    /// Database representation.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Leased => "leased",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Recorded => "recorded",
        }
    }

    fn parse(value: &str) -> Result<Self, HiveWireError> {
        match value {
            "queued" => Ok(Self::Queued),
            "leased" => Ok(Self::Leased),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "recorded" => Ok(Self::Recorded),
            other => Err(HiveWireError::Store {
                detail: format!("unknown hivewire message state `{other}`"),
            }),
        }
    }
}

/// One queued task, read back for diagnostics and scheduler wiring.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingTask {
    /// Remote that sent the task.
    pub remote_id: String,
    /// Message id (idempotency key).
    pub message_id: String,
    /// Caller task id.
    pub task_id: Option<String>,
    /// Session it arrived on.
    pub session_id: String,
    /// Per-session sequence.
    pub sequence: u64,
    /// Current state.
    pub state: MessageState,
    /// Number of times the task has been leased.
    pub attempts: u32,
}

/// Outcome of [`HiveWireStore::accept`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AcceptOutcome {
    /// New task persisted in the bounded queue.
    Queued {
        /// Accepted sequence.
        sequence: u64,
    },
    /// New result/error/heartbeat persisted.
    Recorded {
        /// Accepted sequence.
        sequence: u64,
    },
    /// The `(remote_id, message_id)` pair was already stored; nothing was
    /// re-applied and nothing was queued twice.
    Duplicate,
}

/// A configured HiveWire link: one remote, one durable store.
#[derive(Debug)]
pub struct HiveWireStore {
    db: Arc<Database>,
    config: HiveWireConfig,
    token_sha256: String,
}

impl HiveWireStore {
    /// Validates the link, creates the additive tables and returns the store.
    ///
    /// # Errors
    /// A named [`HiveWireError`] for an unconfigured/unsupported link or for a
    /// store failure. No store is returned on a refusal.
    pub fn open(db: Arc<Database>, config: HiveWireConfig) -> Result<Self, HiveWireError> {
        config.validate()?;
        ensure_schema(&db)?;
        let token = config.launch_token.as_deref().unwrap_or_default();
        Ok(Self {
            db,
            token_sha256: token_digest(token),
            config,
        })
    }

    /// Configured remote name.
    #[must_use]
    pub fn remote_id(&self) -> &str {
        &self.config.remote_id
    }

    /// The database handle (used to construct a [`super::DaemonScheduler`] on the
    /// same durable queue).
    #[must_use]
    pub fn database(&self) -> &Arc<Database> {
        &self.db
    }

    /// Authenticates the bearer launch token and opens a session.
    ///
    /// Order: frame validation, bearer token, remote identity, then persistence.
    /// A rejected token never creates a session row.
    ///
    /// # Errors
    /// [`HiveWireError::TokenRejected`] for a bad bearer token,
    /// [`HiveWireError::RemoteMismatch`] for a hello addressed elsewhere, plus
    /// the frame and store errors.
    pub fn handshake(
        &self,
        envelope: &WireEnvelope,
        presented_token: &str,
    ) -> Result<HandshakeAck, HiveWireError> {
        let hello = envelope.validate_hello()?.clone();
        if !token_matches(&self.token_sha256, presented_token) {
            return Err(HiveWireError::TokenRejected {
                remote_id: self.config.remote_id.clone(),
            });
        }
        if hello.remote_id != self.config.remote_id {
            return Err(HiveWireError::RemoteMismatch {
                expected: self.config.remote_id.clone(),
                presented: hello.remote_id,
            });
        }
        let remote_id = self.config.remote_id.clone();
        let session_id = uuid::Uuid::now_v7().to_string();
        let protocol = envelope.protocol.clone();
        let now = now_ms();
        let token_sha256 = self.token_sha256.clone();
        let (server_last_sequence, replay_required) = with_tx(&self.db, |tx| {
            let server_last: i64 = tx
                .query_row(
                    "SELECT COALESCE(MAX(sequence), 0) FROM hivewire_messages WHERE remote_id = ?1",
                    params![remote_id],
                    |row| row.get(0),
                )
                .map_err(store_error)?;
            let server_last_sequence = u64::try_from(server_last).unwrap_or(0);
            tx.execute(
                "INSERT INTO hivewire_sessions
                 (session_id, remote_id, protocol, token_sha256, last_seen_sequence, last_heartbeat_ms, created_at_ms)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?6)",
                params![
                    session_id,
                    remote_id,
                    protocol,
                    token_sha256,
                    i64::try_from(server_last_sequence).unwrap_or(i64::MAX),
                    now
                ],
            )
            .map_err(store_error)?;
            Ok((server_last_sequence, hello.last_seen_sequence > server_last_sequence))
        })?;
        Ok(HandshakeAck {
            session_id,
            protocol: envelope.protocol.clone(),
            server_last_sequence,
            next_sequence: server_last_sequence.saturating_add(1),
            replay_required,
        })
    }

    /// Accepts one post-handshake frame exactly once.
    ///
    /// Idempotency key is `(remote_id, message_id)`, so a resend is
    /// [`AcceptOutcome::Duplicate`] even across a new session. Sequence is then
    /// enforced per session, the bounded queue is checked inside the same
    /// transaction, and the row is persisted before any success is returned.
    ///
    /// # Errors
    /// [`HiveWireError::SessionUnknown`], [`HiveWireError::SequenceGap`],
    /// [`HiveWireError::QueueFull`], plus frame and store errors.
    pub fn accept(&self, envelope: &WireEnvelope) -> Result<AcceptOutcome, HiveWireError> {
        let (session_id, sequence) = envelope.validate_post_handshake()?;
        let session_id = session_id.to_owned();
        let remote_id = self.config.remote_id.clone();
        let queue_capacity = self.config.queue_capacity;
        let body = envelope.body.clone();
        let task_id = body.task_id().map(str::to_owned);
        let kind = body.kind().to_owned();
        let sequence_i64 = i64::try_from(sequence).map_err(|_| super::HiveWireError::BadFrame {
            detail: "sequence does not fit the store".to_owned(),
        })?;
        let now = now_ms();
        let body_json = serde_json::to_string(envelope).map_err(|error| {
            super::HiveWireError::BadFrame {
                detail: format!("envelope is not encodable: {error}"),
            }
        })?;

        with_tx(&self.db, |tx| {
            let session: Option<i64> = tx
                .query_row(
                    "SELECT last_seen_sequence FROM hivewire_sessions
                     WHERE session_id = ?1 AND remote_id = ?2 AND closed_at_ms IS NULL",
                    params![session_id, remote_id],
                    |row| row.get(0),
                )
                .optional()
                .map_err(store_error)?;
            let last_seen = match session {
                Some(last_seen) => u64::try_from(last_seen).unwrap_or(0),
                None => {
                    return Err(HiveWireError::SessionUnknown {
                        session_id: session_id.clone(),
                    });
                }
            };

            let duplicate: bool = tx
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM hivewire_messages WHERE remote_id = ?1 AND message_id = ?2)",
                    params![remote_id, envelope.message_id],
                    |row| row.get(0),
                )
                .map_err(store_error)?;
            if duplicate {
                return Ok(AcceptOutcome::Duplicate);
            }

            let expected = last_seen.saturating_add(1);
            if sequence != expected {
                return Err(HiveWireError::SequenceGap {
                    expected,
                    presented: sequence,
                });
            }

            if matches!(body, MessageBody::Task(_)) {
                let depth: i64 = tx
                    .query_row(
                        "SELECT COUNT(*) FROM hivewire_messages
                         WHERE kind = 'task' AND state IN ('queued', 'leased')",
                        [],
                        |row| row.get(0),
                    )
                    .map_err(store_error)?;
                let depth = usize::try_from(depth).unwrap_or(usize::MAX);
                if depth >= queue_capacity {
                    return Err(HiveWireError::QueueFull {
                        capacity: queue_capacity,
                    });
                }
            }

            let state = if matches!(body, MessageBody::Task(_)) {
                MessageState::Queued.as_str()
            } else {
                MessageState::Recorded.as_str()
            };
            tx.execute(
                "INSERT INTO hivewire_messages
                 (remote_id, message_id, session_id, sequence, kind, task_id, body_json, state, queued_at_ms)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                params![
                    remote_id,
                    envelope.message_id,
                    session_id,
                    sequence_i64,
                    kind,
                    task_id,
                    body_json,
                    state,
                    now
                ],
            )
            .map_err(store_error)?;

            match &body {
                MessageBody::Result(result) => {
                    tx.execute(
                        "UPDATE hivewire_messages
                         SET state = 'completed', completed_at_ms = ?3, lease_until_ms = NULL, lease_owner = NULL
                         WHERE remote_id = ?1 AND task_id = ?2 AND kind = 'task'
                           AND state IN ('queued', 'leased')",
                        params![remote_id, result.task_id, now],
                    )
                    .map_err(store_error)?;
                }
                MessageBody::Error(error) => {
                    if let Some(task) = &error.task_id {
                        tx.execute(
                            "UPDATE hivewire_messages
                             SET state = 'failed', completed_at_ms = ?3, lease_until_ms = NULL, lease_owner = NULL
                             WHERE remote_id = ?1 AND task_id = ?2 AND kind = 'task'
                               AND state IN ('queued', 'leased')",
                            params![remote_id, task, now],
                        )
                        .map_err(store_error)?;
                    }
                }
                MessageBody::Heartbeat(_) => {}
                MessageBody::Hello(_) | MessageBody::Task(_) => {}
            }

            tx.execute(
                "UPDATE hivewire_sessions
                 SET last_seen_sequence = ?2, last_heartbeat_ms = ?3
                 WHERE session_id = ?1",
                params![session_id, sequence_i64, now],
            )
            .map_err(store_error)?;

            Ok(if matches!(body, MessageBody::Task(_)) {
                AcceptOutcome::Queued { sequence }
            } else {
                AcceptOutcome::Recorded { sequence }
            })
        })
    }

    /// Number of tasks currently queued or leased.
    ///
    /// # Errors
    /// [`HiveWireError::Store`] on a database failure.
    pub fn queue_depth(&self) -> Result<usize, HiveWireError> {
        let depth: i64 = self
            .db
            .with_connection(|connection| {
                connection.query_row(
                    "SELECT COUNT(*) FROM hivewire_messages
                     WHERE kind = 'task' AND state IN ('queued', 'leased')",
                    [],
                    |row| row.get(0),
                )
            })
            .map_err(|error| HiveWireError::Store {
                detail: error.to_string(),
            })?;
        Ok(usize::try_from(depth).unwrap_or(usize::MAX))
    }

    /// Current state of one stored message.
    ///
    /// # Errors
    /// [`HiveWireError::Store`] on a database failure or an unknown state value.
    pub fn message_state(&self, message_id: &str) -> Result<Option<MessageState>, HiveWireError> {
        let remote_id = self.config.remote_id.clone();
        let message_id = message_id.to_owned();
        self.db
            .with_connection(|connection| {
                connection
                    .query_row(
                        "SELECT state FROM hivewire_messages WHERE remote_id = ?1 AND message_id = ?2",
                        params![remote_id, message_id],
                        |row| row.get::<_, String>(0),
                    )
                    .optional()
            })
            .map_err(|error| HiveWireError::Store {
                detail: error.to_string(),
            })?
            .map(|state| MessageState::parse(&state))
            .transpose()
    }

    /// Lists queued tasks in insertion order.
    ///
    /// # Errors
    /// [`HiveWireError::Store`] on a database failure.
    pub fn pending_tasks(&self, limit: usize) -> Result<Vec<PendingTask>, HiveWireError> {
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let rows = self
            .db
            .with_connection(|connection| {
                let mut statement = connection.prepare(
                    "SELECT remote_id, message_id, task_id, session_id, sequence, state, attempts
                     FROM hivewire_messages
                     WHERE kind = 'task' AND state IN ('queued', 'leased')
                     ORDER BY rowid ASC LIMIT ?1",
                )?;
                let rows = statement.query_map(params![limit], |row| {
                    Ok((
                        PendingTask {
                            remote_id: row.get(0)?,
                            message_id: row.get(1)?,
                            task_id: row.get(2)?,
                            session_id: row.get(3)?,
                            sequence: u64::try_from(row.get::<_, i64>(4)?).unwrap_or(0),
                            state: MessageState::Queued,
                            attempts: u32::try_from(row.get::<_, i64>(6)?).unwrap_or(0),
                        },
                        row.get::<_, String>(5)?,
                    ))
                })?;
                let mut tasks = Vec::new();
                for row in rows {
                    tasks.push(row?);
                }
                Ok(tasks)
            })
            .map_err(|error| HiveWireError::Store {
                detail: error.to_string(),
            })?;
        rows.into_iter()
            .map(|(mut task, state)| {
                task.state = MessageState::parse(&state)?;
                Ok(task)
            })
            .collect()
    }
}
