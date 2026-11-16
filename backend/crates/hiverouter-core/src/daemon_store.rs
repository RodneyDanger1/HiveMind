//! HiveWire daemon scheduler: leases queued tasks and expires stale leases.
//!
//! The scheduler is the only writer of `state = 'leased'`. Each tick is one
//! SQLite transaction, so a lease is granted once even if two schedulers tick at
//! the same instant (the queue row's `state = 'queued'` predicate is the guard).
//! An expired lease is requeued exactly once: the expiry update moves the row to
//! `queued` and clears the lease columns, so a second expiry pass for the same
//! instant finds nothing to move.
//!
//! Every lease also records the *generation* of the scheduler that granted it
//! (a per-process identity). [`DaemonScheduler::crash_recovery_tick`] uses that
//! to requeue leases this daemon held before a restart, without waiting for
//! their lease deadline and without stealing another live daemon's lease.
//!
//! [`DaemonScheduler::run_loop`] is the production loop: it runs one crash
//! recovery tick, then ticks at a fixed interval until its
//! [`CancellationToken`] is cancelled, counting work and logging (never hiding)
//! store failures. It performs no thread spawning itself, so the binary owner
//! decides where it runs; [`DaemonScheduler::run_loop_async`] runs it on
//! Tokio's blocking pool.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use rusqlite::{params, Transaction};
use tokio_util::sync::CancellationToken;

use crate::db::Database;

use super::remote_store::ensure_schema;
use super::{now_ms, store_error, with_tx, HiveWireError};

/// How often the blocking loop re-checks its cancellation token while sleeping.
const CANCEL_POLL: Duration = Duration::from_millis(25);

/// Upper bound for the error backoff in [`DaemonScheduler::run_loop`].
const MAX_LOOP_BACKOFF: Duration = Duration::from_secs(5);

/// A task that a daemon now owns until `lease_until_ms`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeasedTask {
    /// Remote that queued the task.
    pub remote_id: String,
    /// Message id (idempotency key) of the queued `task` frame.
    pub message_id: String,
    /// Caller task id.
    pub task_id: Option<String>,
    /// Per-session sequence.
    pub sequence: u64,
    /// How many times this task has been leased (1 on first lease).
    pub attempts: u32,
    /// Instant after which the lease is stale, in Unix milliseconds.
    pub lease_until_ms: i64,
    /// Daemon that holds the lease.
    pub lease_owner: String,
    /// Scheduler generation that granted the lease.
    pub lease_generation: String,
}

impl LeasedTask {
    /// The identity a completion must carry for this lease.
    #[must_use]
    pub fn lease_identity(&self) -> super::LeaseIdentity {
        super::LeaseIdentity {
            message_id: self.message_id.clone(),
            attempt: self.attempts,
            daemon_id: self.lease_owner.clone(),
        }
    }
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
    /// Leases recovered from this owner's previous process generation
    /// (crash-recovery tick only).
    pub recovered: usize,
    /// Stale leases returned to the queue.
    pub requeued: usize,
    /// Stale or recovered leases failed at the attempt ceiling.
    pub exhausted: usize,
    /// Tasks leased by this tick, in queue order.
    pub leased: Vec<LeasedTask>,
    /// Tasks still queued or leased after the tick.
    pub queue_depth: usize,
}

/// Aggregate of one [`DaemonScheduler::run_loop`] run.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SchedulerLoopReport {
    /// Successful live ticks (the initial crash-recovery tick is counted
    /// separately in [`Self::recovery_ticks`]).
    pub ticks: u64,
    /// Crash-recovery ticks attempted (0 or 1).
    pub recovery_ticks: u64,
    /// Tasks leased across every tick.
    pub tasks_leased: u64,
    /// Stale leases returned to the queue.
    pub requeued: u64,
    /// Leases recovered from a previous process generation.
    pub recovered: u64,
    /// Leases failed at the attempt ceiling.
    pub exhausted: u64,
    /// Ticks that failed with a named error (each one was logged).
    pub errors: u64,
    /// `code: message` of the most recent tick failure.
    pub last_error: Option<String>,
    /// True when the loop stopped because its token was cancelled.
    pub cancelled: bool,
}

/// Lease-based scheduler over the durable `hivewire_messages` queue.
#[derive(Debug)]
pub struct DaemonScheduler {
    db: Arc<Database>,
    lease_ms: i64,
    max_attempts: u32,
    batch: usize,
    generation: String,
}

impl DaemonScheduler {
    /// Creates a scheduler on the same database as the remote store.
    ///
    /// The generation is a fresh per-process identity, so a new scheduler
    /// instance can recognise leases granted by an earlier process.
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
            generation: default_generation(),
        })
    }

    /// Overrides the generation recorded on every lease.
    ///
    /// Used by tests and by a supervisor that already knows the process
    /// generation. An empty generation is refused at tick time.
    #[must_use]
    pub fn with_generation(mut self, generation: impl Into<String>) -> Self {
        self.generation = generation.into();
        self
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

    /// Generation recorded on leases granted by this scheduler.
    #[must_use]
    pub fn generation(&self) -> &str {
        &self.generation
    }

    /// One live tick: expire stale leases, then lease up to `batch` ready tasks.
    ///
    /// # Errors
    /// [`HiveWireError::ConfigInvalid`] for an empty owner and
    /// [`HiveWireError::Store`] on a database failure.
    pub fn tick(&self, owner: &str) -> Result<SchedulerTickReport, HiveWireError> {
        self.run_tick(owner, self.batch, false)
    }

    /// One live tick with an explicit per-tick batch bound.
    ///
    /// # Errors
    /// [`HiveWireError::ConfigInvalid`] for an empty owner or a zero batch and
    /// [`HiveWireError::Store`] on a database failure.
    pub fn tick_with_batch(
        &self,
        owner: &str,
        batch: usize,
    ) -> Result<SchedulerTickReport, HiveWireError> {
        self.run_tick(owner, batch, false)
    }

    /// Boot tick for a restarted daemon.
    ///
    /// First requeues every lease this `owner` held under a *different*
    /// generation (a previous process), failing those at the attempt ceiling;
    /// then applies the ordinary stale-lease expiry and leases a batch. Leases
    /// owned by other daemons are never touched, even if they are live.
    ///
    /// # Errors
    /// [`HiveWireError::ConfigInvalid`] for an empty owner and
    /// [`HiveWireError::Store`] on a database failure.
    pub fn crash_recovery_tick(&self, owner: &str) -> Result<SchedulerTickReport, HiveWireError> {
        self.run_tick(owner, self.batch, true)
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

    /// Runs ticks until `cancel` is cancelled: one crash-recovery tick, then a
    /// live tick every `interval` for at most `batch` tasks.
    ///
    /// The owner defaults to the stable [`default_daemon_owner`]; use
    /// [`Self::run_loop_as`] to name it explicitly. A tick failure is counted,
    /// logged at error level and retried with bounded exponential backoff; it
    /// never stops the loop silently. Cancellation is observed within
    /// [`CANCEL_POLL`].
    ///
    /// This call blocks the current thread. Run it on a dedicated thread, or
    /// use [`Self::run_loop_async`].
    ///
    /// # Errors
    /// [`HiveWireError::ConfigInvalid`] for a zero interval/batch or an empty
    /// owner. Tick failures are reported in [`SchedulerLoopReport`], not as an
    /// `Err`, because the loop recovers from them.
    pub fn run_loop(
        &self,
        cancel: CancellationToken,
        interval: Duration,
        batch: usize,
    ) -> Result<SchedulerLoopReport, HiveWireError> {
        let owner = default_daemon_owner();
        self.run_loop_as(&owner, cancel, interval, batch)
    }

    /// [`Self::run_loop`] with an explicit lease owner.
    ///
    /// # Errors
    /// As [`Self::run_loop`].
    pub fn run_loop_as(
        &self,
        owner: &str,
        cancel: CancellationToken,
        interval: Duration,
        batch: usize,
    ) -> Result<SchedulerLoopReport, HiveWireError> {
        let owner = validated_owner(owner)?;
        if interval.is_zero() {
            return Err(HiveWireError::ConfigInvalid {
                detail: "interval must be positive".to_owned(),
            });
        }
        if batch == 0 {
            return Err(HiveWireError::ConfigInvalid {
                detail: "batch must be positive".to_owned(),
            });
        }
        self.validated_generation()?;

        let mut report = SchedulerLoopReport::default();
        let mut consecutive_errors: u32 = 0;

        // Crash recovery is attempted once, before the first live tick. A
        // failure is reported and the live loop still runs: ordinary expiry
        // recovers the same leases once their deadline passes.
        report.recovery_ticks = 1;
        match self.run_tick(&owner, batch, true) {
            Ok(tick) => accumulate_tick(&mut report, &tick),
            Err(error) => record_loop_error(&mut report, &error),
        }

        loop {
            if cancel.is_cancelled() {
                report.cancelled = true;
                break;
            }
            match self.run_tick(&owner, batch, false) {
                Ok(tick) => {
                    report.ticks = report.ticks.saturating_add(1);
                    accumulate_tick(&mut report, &tick);
                    consecutive_errors = 0;
                }
                Err(error) => {
                    record_loop_error(&mut report, &error);
                    consecutive_errors = consecutive_errors.saturating_add(1);
                }
            }
            let backoff = if consecutive_errors == 0 {
                interval
            } else {
                let factor = 1_u32 << consecutive_errors.min(3);
                interval.saturating_mul(factor).min(MAX_LOOP_BACKOFF)
            };
            if sleep_until_cancelled(&cancel, backoff.max(CANCEL_POLL)) {
                report.cancelled = true;
                break;
            }
        }
        Ok(report)
    }

    /// Runs [`Self::run_loop`] on Tokio's blocking pool and awaits it.
    ///
    /// # Errors
    /// The loop's configuration errors, plus [`HiveWireError::Store`] when the
    /// blocking task panics or is cancelled by the runtime.
    pub async fn run_loop_async(
        self: Arc<Self>,
        cancel: CancellationToken,
        interval: Duration,
        batch: usize,
    ) -> Result<SchedulerLoopReport, HiveWireError> {
        tokio::task::spawn_blocking(move || self.run_loop(cancel, interval, batch))
            .await
            .map_err(|error| HiveWireError::Store {
                detail: format!("scheduler loop task failed: {error}"),
            })?
    }

    /// Marks a leased task completed. Only the lease owner from the current
    /// generation may complete it.
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
        let generation = self.validated_generation()?;
        let changed = self
            .db
            .with_connection(|connection| {
                connection.execute(
                    "UPDATE hivewire_messages
                     SET state = 'completed', completed_at_ms = ?4,
                         lease_until_ms = NULL, lease_owner = NULL, lease_generation = NULL
                     WHERE remote_id = ?1 AND message_id = ?2 AND kind = 'task'
                       AND state = 'leased' AND lease_owner = ?3
                       AND lease_generation = ?5",
                    params![remote_id, message_id, owner, now, generation],
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

    fn validated_generation(&self) -> Result<String, HiveWireError> {
        let generation = self.generation.trim();
        if generation.is_empty() || generation.len() > 128 {
            return Err(HiveWireError::ConfigInvalid {
                detail: "generation must be 1..=128 characters".to_owned(),
            });
        }
        Ok(generation.to_owned())
    }

    fn run_tick(
        &self,
        owner: &str,
        batch: usize,
        recover: bool,
    ) -> Result<SchedulerTickReport, HiveWireError> {
        let owner = validated_owner(owner)?;
        if batch == 0 {
            return Err(HiveWireError::ConfigInvalid {
                detail: "batch must be positive".to_owned(),
            });
        }
        let generation = self.validated_generation()?;
        let batch = i64::try_from(batch).unwrap_or(i64::MAX);
        let max_attempts = i64::from(self.max_attempts);
        let now = now_ms();
        let lease_until_ms = now.saturating_add(self.lease_ms);
        with_tx(&self.db, |tx| {
            let mut recovered = 0_usize;
            let mut recovered_exhausted = 0_usize;
            if recover {
                let (requeued, exhausted) =
                    recover_generation_in_tx(tx, &owner, &generation, max_attempts)?;
                recovered = requeued;
                recovered_exhausted = exhausted;
            }
            let expiry = expire_in_tx(tx, now, max_attempts)?;
            let leased = lease_batch_in_tx(tx, batch, &owner, &generation, lease_until_ms)?;
            let queue_depth = queue_depth_in_tx(tx)?;
            Ok(SchedulerTickReport {
                recovered,
                requeued: expiry.requeued,
                exhausted: expiry.exhausted.saturating_add(recovered_exhausted),
                leased,
                queue_depth,
            })
        })
    }
}

/// Stable daemon owner used by [`DaemonScheduler::run_loop`].
///
/// It is deliberately **not** derived from the process id: crash recovery
/// recognises this daemon's leases only if the owner string survives a restart.
/// The per-process [`DaemonScheduler::generation`] is what distinguishes the
/// restarted process. Run one scheduler per database; call
/// [`DaemonScheduler::run_loop_as`] with distinct owners for distinct daemons.
#[must_use]
pub fn default_daemon_owner() -> String {
    "hivewire-daemon".to_owned()
}

fn default_generation() -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let sequence = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!(
        "gen-{}-{}-{sequence}",
        std::process::id(),
        now_ms()
    )
}

fn validated_owner(owner: &str) -> Result<String, HiveWireError> {
    let owner = owner.trim();
    if owner.is_empty() || owner.len() > 128 {
        return Err(HiveWireError::ConfigInvalid {
            detail: "tick owner must be 1..=128 characters".to_owned(),
        });
    }
    Ok(owner.to_owned())
}

fn accumulate_tick(report: &mut SchedulerLoopReport, tick: &SchedulerTickReport) {
    report.tasks_leased = report
        .tasks_leased
        .saturating_add(u64::try_from(tick.leased.len()).unwrap_or(u64::MAX));
    report.requeued = report
        .requeued
        .saturating_add(u64::try_from(tick.requeued).unwrap_or(u64::MAX));
    report.recovered = report
        .recovered
        .saturating_add(u64::try_from(tick.recovered).unwrap_or(u64::MAX));
    report.exhausted = report
        .exhausted
        .saturating_add(u64::try_from(tick.exhausted).unwrap_or(u64::MAX));
}

fn record_loop_error(report: &mut SchedulerLoopReport, error: &HiveWireError) {
    report.errors = report.errors.saturating_add(1);
    report.last_error = Some(format!("{}: {error}", error.code()));
    tracing::error!(
        code = error.code(),
        %error,
        "HiveWire daemon scheduler tick failed; the loop retries with backoff"
    );
}

/// Sleeps for `duration`, returning `true` when the token was cancelled first.
fn sleep_until_cancelled(cancel: &CancellationToken, duration: Duration) -> bool {
    let deadline = Instant::now() + duration;
    loop {
        if cancel.is_cancelled() {
            return true;
        }
        let now = Instant::now();
        if now >= deadline {
            return false;
        }
        std::thread::sleep((deadline - now).min(CANCEL_POLL));
    }
}

fn lease_batch_in_tx(
    tx: &Transaction<'_>,
    batch: i64,
    owner: &str,
    generation: &str,
    lease_until_ms: i64,
) -> Result<Vec<LeasedTask>, HiveWireError> {
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
                     lease_generation = ?5, attempts = attempts + 1
                 WHERE remote_id = ?1 AND message_id = ?2 AND state = 'queued'",
                params![remote_id, message_id, lease_until_ms, owner, generation],
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
                lease_owner: owner.to_owned(),
                lease_generation: generation.to_owned(),
            });
        }
    }
    Ok(leased)
}

fn expire_in_tx(
    tx: &Transaction<'_>,
    now: i64,
    max_attempts: i64,
) -> Result<ExpiryReport, HiveWireError> {
    let exhausted = tx
        .execute(
            "UPDATE hivewire_messages
             SET state = 'failed', completed_at_ms = ?1,
                 lease_until_ms = NULL, lease_owner = NULL, lease_generation = NULL
             WHERE kind = 'task' AND state = 'leased'
               AND lease_until_ms IS NOT NULL AND lease_until_ms <= ?1
               AND attempts >= ?2",
            params![now, max_attempts],
        )
        .map_err(store_error)?;
    let requeued = tx
        .execute(
            "UPDATE hivewire_messages
             SET state = 'queued', lease_until_ms = NULL,
                 lease_owner = NULL, lease_generation = NULL
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

/// Requeues (or terminally fails) leases this owner held under an earlier
/// generation, without touching another daemon's lease.
fn recover_generation_in_tx(
    tx: &Transaction<'_>,
    owner: &str,
    generation: &str,
    max_attempts: i64,
) -> Result<(usize, usize), HiveWireError> {
    let failed = tx
        .execute(
            "UPDATE hivewire_messages
             SET state = 'failed', completed_at_ms = ?3,
                 lease_until_ms = NULL, lease_owner = NULL, lease_generation = NULL
             WHERE kind = 'task' AND state = 'leased' AND lease_owner = ?1
               AND (lease_generation IS NULL OR lease_generation <> ?2)
               AND attempts >= ?4",
            params![owner, generation, now_ms(), max_attempts],
        )
        .map_err(store_error)?;
    let requeued = tx
        .execute(
            "UPDATE hivewire_messages
             SET state = 'queued', lease_until_ms = NULL,
                 lease_owner = NULL, lease_generation = NULL
             WHERE kind = 'task' AND state = 'leased' AND lease_owner = ?1
               AND (lease_generation IS NULL OR lease_generation <> ?2)
               AND attempts < ?3",
            params![owner, generation, max_attempts],
        )
        .map_err(store_error)?;
    Ok((requeued, failed))
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
