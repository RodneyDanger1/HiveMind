//! HiveWire daemon scheduler: leases queued tasks and expires stale leases.
//!
//! The scheduler is the only writer of `state = 'leased'`. Each tick is one
//! SQLite transaction, so a lease is granted once even if two schedulers tick at
//! the same instant (the queue row's `state = 'queued'` predicate is the guard).
//! An expired lease is requeued exactly once: the expiry update moves the row to
//! `queued` and clears the lease columns, so a second expiry pass for the same
//! instant finds nothing to move.

use std::sync::Arc;

use rusqlite::{params, Transaction};

use crate::db::Database;

use super::remote_store::ensure_schema;
use super::{now_ms, store_error, with_tx, HiveWireError};

/// A task that a daemon now owns until `lease_until_ms`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeasedTask {
    /// Remote that queued the task.
    pub remote_id: String,
    /// Message id (idempotency key).
    pub message_id: String,
    /// Caller task id.
    pub task_id: Option<String>,
    /// Per-session sequence.
    pub sequence: u64,
    /// How many times this task has been leased (1 on first lease).
    pub attempts: u32,
    /// Instant after which the lease is stale, in Unix milliseconds.
    pub lease_until_ms: i64,
}

/// Counts from one lease-expiry pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExpiryReport {
    /// Leases returned to the queue (they will be leased again).
    pub requeued: usize,
    /// Leases that hit the attempt ceiling and were terminally failed.
    pub exhausted: usize,
}

/// Result of one live scheduler tick.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchedulerTickReport {
    /// Stale leases returned to the queue.
    pub requeued: usize,
    /// Stale leases failed at the attempt ceiling.
    pub exhausted: usize,
    /// Tasks leased by this tick, in queue order.
    pub leased: Vec<LeasedTask>,
    /// Tasks still queued or leased after the tick.
    pub queue_depth: usize,
}

/// Lease-based scheduler over the durable `hivewire_messages` queue.
#[derive(Debug)]
pub struct DaemonScheduler {
    db: Arc<Database>,
    lease_ms: i64,
    max_attempts: u32,
    batch: usize,
}

impl DaemonScheduler {
    /// Creates a scheduler on the same database as the remote store.
    ///
    /// # Errors
    /// [`HiveWireError::ConfigInvalid`] for a zero/non-positive bound and
    /// [`HiveWireError::Store`] when the tables cannot be created.
    pub fn new(
        db: Arc<Database>,
        lease_ms: i64,
        max_attempts: u32,
        batch: usize,
    ) -> Result<Self, HiveWireError> {
        if lease_ms <= 0 {
            return Err(HiveWireError::ConfigInvalid {
                detail: "lease_ms must be positive".to_owned(),
            });
        }
        if max_attempts == 0 {
            return Err(HiveWireError::ConfigInvalid {
                detail: "max_attempts must be positive".to_owned(),
            });
        }
        if batch == 0 {
            return Err(HiveWireError::ConfigInvalid {
                detail: "batch must be positive".to_owned(),
            });
        }
        ensure_schema(&db)?;
        Ok(Self {
            db,
            lease_ms,
            max_attempts,
            batch,
        })
    }

    /// Lease duration in milliseconds.
    #[must_use]
    pub fn lease_ms(&self) -> i64 {
        self.lease_ms
    }

    /// Attempt ceiling before a task is terminally failed.
    #[must_use]
    pub fn max_attempts(&self) -> u32 {
        self.max_attempts
    }

    /// One live tick: expire stale leases, then lease up to `batch` ready tasks.
    ///
    /// # Errors
    /// [`HiveWireError::ConfigInvalid`] for an empty owner and
    /// [`HiveWireError::Store`] on a database failure.
    pub fn tick(&self, owner: &str) -> Result<SchedulerTickReport, HiveWireError> {
        let owner = owner.trim();
        if owner.is_empty() {
            return Err(HiveWireError::ConfigInvalid {
                detail: "tick owner must not be empty".to_owned(),
            });
        }
        let owner = owner.to_owned();
        let now = now_ms();
        let batch = i64::try_from(self.batch).unwrap_or(i64::MAX);
        let max_attempts = i64::from(self.max_attempts);
        let lease_until_ms = now.saturating_add(self.lease_ms);
        with_tx(&self.db, |tx| {
            let expiry = expire_in_tx(tx, now, max_attempts)?;
            let queued = {
                let mut statement = tx
                    .prepare(
                        "SELECT remote_id, message_id, task_id, sequence
                         FROM hivewire_messages
                         WHERE kind = 'task' AND state = 'queued'
                         ORDER BY rowid ASC LIMIT ?1",
                    )
                    .map_err(store_error)?;
                let rows = statement
                    .query_map(params![batch], |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, Option<String>>(2)?,
                            row.get::<_, i64>(3)?,
                        ))
                    })
                    .map_err(store_error)?;
                let mut queued = Vec::new();
                for row in rows {
                    queued.push(row.map_err(store_error)?);
                }
                queued
            };
            let mut leased = Vec::new();
            for (remote_id, message_id, task_id, sequence) in queued {
                let changed = tx
                    .execute(
                        "UPDATE hivewire_messages
                         SET state = 'leased', lease_until_ms = ?3, lease_owner = ?4,
                             attempts = attempts + 1
                         WHERE remote_id = ?1 AND message_id = ?2 AND state = 'queued'",
                        params![remote_id, message_id, lease_until_ms, owner],
                    )
                    .map_err(store_error)?;
                if changed == 1 {
                    let attempts: i64 = tx
                        .query_row(
                            "SELECT attempts FROM hivewire_messages
                             WHERE remote_id = ?1 AND message_id = ?2",
                            params![remote_id, message_id],
                            |row| row.get(0),
                        )
                        .map_err(store_error)?;
                    leased.push(LeasedTask {
                        remote_id,
                        message_id,
                        task_id,
                        sequence: u64::try_from(sequence).unwrap_or(0),
                        attempts: u32::try_from(attempts).unwrap_or(0),
                        lease_until_ms,
                    });
                }
            }
            let queue_depth = queue_depth_in_tx(tx)?;
            Ok(SchedulerTickReport {
                requeued: expiry.requeued,
                exhausted: expiry.exhausted,
                leased,
                queue_depth,
            })
        })
    }

    /// Expiry-only pass (the first half of [`Self::tick`]); useful for
    /// deterministic recovery tests.
    ///
    /// # Errors
    /// [`HiveWireError::Store`] on a database failure.
    pub fn expire_leases(&self) -> Result<ExpiryReport, HiveWireError> {
        let now = now_ms();
        let max_attempts = i64::from(self.max_attempts);
        with_tx(&self.db, |tx| expire_in_tx(tx, now, max_attempts))
    }

    /// Marks a leased task completed. Only the lease owner may complete it.
    ///
    /// # Errors
    /// [`HiveWireError::LeaseLost`] when the caller no longer holds the lease,
    /// [`HiveWireError::Store`] on a database failure.
    pub fn complete(
        &self,
        owner: &str,
        remote_id: &str,
        message_id: &str,
    ) -> Result<(), HiveWireError> {
        let now = now_ms();
        let changed = self
            .db
            .with_connection(|connection| {
                connection.execute(
                    "UPDATE hivewire_messages
                     SET state = 'completed', completed_at_ms = ?4,
                         lease_until_ms = NULL, lease_owner = NULL
                     WHERE remote_id = ?1 AND message_id = ?2 AND kind = 'task'
                       AND state = 'leased' AND lease_owner = ?3",
                    params![remote_id, message_id, owner, now],
                )
            })
            .map_err(|error| HiveWireError::Store {
                detail: error.to_string(),
            })?;
        if changed == 0 {
            return Err(HiveWireError::LeaseLost {
                message_id: message_id.to_owned(),
                owner: owner.to_owned(),
            });
        }
        Ok(())
    }

    /// Number of tasks queued or leased.
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
}

fn expire_in_tx(
    tx: &Transaction<'_>,
    now: i64,
    max_attempts: i64,
) -> Result<ExpiryReport, HiveWireError> {
    let exhausted = tx
        .execute(
            "UPDATE hivewire_messages
             SET state = 'failed', completed_at_ms = ?1, lease_until_ms = NULL, lease_owner = NULL
             WHERE kind = 'task' AND state = 'leased'
               AND lease_until_ms IS NOT NULL AND lease_until_ms <= ?1
               AND attempts >= ?2",
            params![now, max_attempts],
        )
        .map_err(store_error)?;
    let requeued = tx
        .execute(
            "UPDATE hivewire_messages
             SET state = 'queued', lease_until_ms = NULL, lease_owner = NULL
             WHERE kind = 'task' AND state = 'leased'
               AND lease_until_ms IS NOT NULL AND lease_until_ms <= ?1
               AND attempts < ?2",
            params![now, max_attempts],
        )
        .map_err(store_error)?;
    Ok(ExpiryReport {
        requeued,
        exhausted,
    })
}

fn queue_depth_in_tx(tx: &rusqlite::Connection) -> Result<usize, HiveWireError> {
    let depth: i64 = tx
        .query_row(
            "SELECT COUNT(*) FROM hivewire_messages
             WHERE kind = 'task' AND state IN ('queued', 'leased')",
            [],
            |row| row.get(0),
        )
        .map_err(store_error)?;
    Ok(usize::try_from(depth).unwrap_or(usize::MAX))
}
