//! Deterministic, bounded goal loop over a SQLite connection.
//!
//! # What this module owns
//!
//! A goal is a persisted state machine (`active` -> `paused` -> `active` ->
//! `completed`, or `blocked` on a hard stop). Every continuation is a **round**
//! with a unique per-goal marker, and every round start is recorded in a
//! continuation row whose `(goal_id, interval_key)` is unique. That makes
//! delivery idempotent even when a caller or process crashes between a round
//! start and its completion:
//!
//! - [`begin_continuation`] returns `Continue` exactly once per interval;
//!   a second call in the same interval returns `Stop(InFlight)` or
//!   `Stop(IntervalNotElapsed)` and creates no second turn.
//! - [`record_round`] is idempotent for an already-completed round. A replayed
//!   completion returns `duplicate: true` and does not add calls again.
//! - [`resume_goal`] finds an in-flight round after a crash and returns
//!   `ResumeOutcome::InFlight` with the **same** marker; an in-flight round
//!   older than [`IN_FLIGHT_LEASE_SECS`] is explicitly abandoned (audited), so a
//!   later interval can start a fresh round.
//!
//! # Budgets and stop reasons
//!
//! Rounds and provider calls are bounded independently. `begin_continuation`
//! stops with [`StopReason::RoundBudgetExhausted`] or
//! [`StopReason::CallBudgetExhausted`] before creating a round; `record_round`
//! refuses to record calls beyond the remaining budget. A goal becomes
//! `blocked` after [`BLOCKED_AFTER`] consecutive blocked rounds or
//! [`NO_PROGRESS_LIMIT`] consecutive no-progress rounds. The round row persists
//! the no-progress marker (`progress = 0`) and the goal persists the accumulated
//! counters and the final `stop_reason`.
//!
//! # No autonomous merge or approval
//!
//! This module has **no** merge, approval, or apply function. The only write a
//! model can make is [`record_self_edit_proposal`], which persists a
//! `kind = 'self_edit'` proposal with `applied = 0` and
//! `requires_operator_approval = 1`; the schema's `CHECK` constraints make those
//! flags immutable. An operator (or an existing approval path outside this
//! module) must still review and apply it. Goal completion never merges code and
//! never bypasses approval.
//!
//! # Persistence
//!
//! [`GOAL_LOOP_SCHEMA`] / [`GOAL_LOOP_SCHEMA_DOWN`] are the migration SQL and
//! [`goal_loop_migration`] wraps them in the existing [`crate::db::Migration`]
//! shape. All state transitions take `&rusqlite::Connection` and run in a single
//! transaction with their audit row, so a crash cannot persist a transition
//! without its audit event.
//!
//! # Determinism
//!
//! Every mutating function takes an explicit [`Timestamp`] `now`; interval math
//! uses whole Unix seconds. Tests can therefore drive intervals without sleeping.

use hiverouter_contract::{RunId, TaskId, Timestamp};
use rusqlite::{params, Connection, OptionalExtension};
use serde::Serialize;
use uuid::Uuid;

/// `goal_loops` / `goal_rounds` / `goal_continuations` / `goal_proposals` /
/// `goal_audit` migration (forward SQL).
pub const GOAL_LOOP_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS goal_loops (
    goal_id              TEXT PRIMARY KEY NOT NULL,
    run_id               TEXT NOT NULL,
    task_id              TEXT,
    objective            TEXT NOT NULL,
    state                TEXT NOT NULL CHECK (state IN
                             ('active','paused','completed','blocked')),
    round                INTEGER NOT NULL DEFAULT 0 CHECK (round >= 0),
    max_rounds           INTEGER NOT NULL CHECK (max_rounds BETWEEN 1 AND 1000),
    max_calls            INTEGER NOT NULL CHECK (max_calls BETWEEN 1 AND 1000000),
    calls_used           INTEGER NOT NULL DEFAULT 0 CHECK (calls_used >= 0),
    interval_secs        INTEGER NOT NULL CHECK (interval_secs BETWEEN 1 AND 86400),
    blocked_rounds       INTEGER NOT NULL DEFAULT 0 CHECK (blocked_rounds >= 0),
    no_progress_rounds   INTEGER NOT NULL DEFAULT 0 CHECK (no_progress_rounds >= 0),
    last_continuation_at TEXT,
    last_progress_at     TEXT,
    stop_reason          TEXT,
    created_at           TEXT NOT NULL,
    updated_at           TEXT NOT NULL,
    rev                  INTEGER NOT NULL DEFAULT 0 CHECK (rev >= 0)
);
CREATE INDEX IF NOT EXISTS idx_goal_loops_state
    ON goal_loops(state, updated_at);

CREATE TABLE IF NOT EXISTS goal_rounds (
    goal_id      TEXT NOT NULL REFERENCES goal_loops(goal_id) ON DELETE CASCADE,
    round        INTEGER NOT NULL CHECK (round >= 1),
    marker       TEXT NOT NULL,
    status       TEXT NOT NULL CHECK (status IN
                     ('in_flight','completed','abandoned')),
    calls        INTEGER NOT NULL DEFAULT 0 CHECK (calls >= 0),
    progress     INTEGER NOT NULL DEFAULT 1 CHECK (progress IN (0,1)),
    blocked      INTEGER NOT NULL DEFAULT 0 CHECK (blocked IN (0,1)),
    started_at   TEXT NOT NULL,
    completed_at TEXT,
    detail_json  TEXT NOT NULL DEFAULT '{}',
    PRIMARY KEY (goal_id, round),
    UNIQUE (goal_id, marker)
);
CREATE INDEX IF NOT EXISTS idx_goal_rounds_status
    ON goal_rounds(goal_id, status, round);

CREATE TABLE IF NOT EXISTS goal_continuations (
    goal_id      TEXT NOT NULL REFERENCES goal_loops(goal_id) ON DELETE CASCADE,
    round        INTEGER NOT NULL,
    interval_key INTEGER NOT NULL,
    marker       TEXT NOT NULL,
    created_at   TEXT NOT NULL,
    PRIMARY KEY (goal_id, round),
    UNIQUE (goal_id, interval_key),
    UNIQUE (goal_id, marker),
    FOREIGN KEY (goal_id, round) REFERENCES goal_rounds(goal_id, round)
        ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_goal_continuations_interval
    ON goal_continuations(goal_id, interval_key);

CREATE TABLE IF NOT EXISTS goal_proposals (
    proposal_id                 TEXT PRIMARY KEY NOT NULL,
    goal_id                     TEXT NOT NULL REFERENCES goal_loops(goal_id)
                                    ON DELETE CASCADE,
    round                       INTEGER NOT NULL,
    kind                        TEXT NOT NULL CHECK (kind = 'self_edit'),
    body                        TEXT NOT NULL,
    status                      TEXT NOT NULL CHECK (status IN ('proposed','withdrawn')),
    applied                     INTEGER NOT NULL DEFAULT 0 CHECK (applied = 0),
    requires_operator_approval  INTEGER NOT NULL DEFAULT 1
                                    CHECK (requires_operator_approval = 1),
    created_at                  TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_goal_proposals_goal
    ON goal_proposals(goal_id, created_at);

CREATE TABLE IF NOT EXISTS goal_audit (
    audit_id    TEXT PRIMARY KEY NOT NULL,
    goal_id     TEXT NOT NULL,
    action      TEXT NOT NULL,
    outcome     TEXT NOT NULL,
    detail_json TEXT NOT NULL DEFAULT '{}',
    created_at  TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_goal_audit_goal
    ON goal_audit(goal_id, created_at);
"#;

/// Inverse of [`GOAL_LOOP_SCHEMA`].
pub const GOAL_LOOP_SCHEMA_DOWN: &str = r#"
DROP INDEX IF EXISTS idx_goal_audit_goal;
DROP TABLE IF EXISTS goal_audit;
DROP INDEX IF EXISTS idx_goal_proposals_goal;
DROP TABLE IF EXISTS goal_proposals;
DROP INDEX IF EXISTS idx_goal_continuations_interval;
DROP TABLE IF EXISTS goal_continuations;
DROP INDEX IF EXISTS idx_goal_rounds_status;
DROP TABLE IF EXISTS goal_rounds;
DROP INDEX IF EXISTS idx_goal_loops_state;
DROP TABLE IF EXISTS goal_loops;
"#;

/// Builds the ordered migration descriptor for the supplied schema version.
///
/// The owning database module chooses the version; this module owns only the SQL
/// and the migration name (`bounded_goal_loop`).
#[must_use]
pub const fn goal_loop_migration(version: i64) -> crate::db::Migration {
    crate::db::Migration {
        version,
        name: "bounded_goal_loop",
        up: GOAL_LOOP_SCHEMA,
        down: GOAL_LOOP_SCHEMA_DOWN,
    }
}

/// Maximum rounds a goal may request.
pub const MAX_ROUNDS: u32 = 1_000;
/// Maximum provider calls a goal may request.
pub const MAX_CALLS: u32 = 1_000_000;
/// Minimum continuation interval.
pub const MIN_INTERVAL_SECS: u64 = 1;
/// Maximum continuation interval.
pub const MAX_INTERVAL_SECS: u64 = 86_400;
/// Consecutive blocked rounds before the goal becomes `blocked`.
pub const BLOCKED_AFTER: u32 = 3;
/// Consecutive no-progress rounds before the goal becomes `blocked`.
pub const NO_PROGRESS_LIMIT: u32 = 3;
/// Age at which an in-flight round is considered abandoned after a crash.
pub const IN_FLIGHT_LEASE_SECS: u64 = 900;
/// Maximum objective bytes.
pub const MAX_OBJECTIVE_BYTES: usize = 4_096;
/// Maximum round detail bytes.
pub const MAX_DETAIL_BYTES: usize = 4_096;
/// Maximum self-edit proposal bytes.
pub const MAX_PROPOSAL_BYTES: usize = 16_384;
/// Maximum actor id bytes.
pub const MAX_ACTOR_BYTES: usize = 128;
/// The only proposal kind this module accepts.
pub const SELF_EDIT_KIND: &str = "self_edit";

/// Persisted goal state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GoalState {
    /// The loop may be continued.
    Active,
    /// Continuations are refused until resumed.
    Paused,
    /// Terminal: the operator completed the goal.
    Completed,
    /// Terminal: a hard stop (blocked/no-progress/budget policy).
    Blocked,
}

impl GoalState {
    /// Persisted label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Paused => "paused",
            Self::Completed => "completed",
            Self::Blocked => "blocked",
        }
    }

    fn parse(raw: &str) -> rusqlite::Result<Self> {
        match raw {
            "active" => Ok(Self::Active),
            "paused" => Ok(Self::Paused),
            "completed" => Ok(Self::Completed),
            "blocked" => Ok(Self::Blocked),
            _ => Err(rusqlite::Error::InvalidParameterName(format!(
                "unknown goal state {raw:?}"
            ))),
        }
    }
}

/// Persisted round status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RoundStatus {
    /// A continuation was delivered; the round is not finished.
    InFlight,
    /// The round finished and its outcome was recorded.
    Completed,
    /// The round's lease expired after a crash; it did not finish.
    Abandoned,
}

impl RoundStatus {
    /// Persisted label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InFlight => "in_flight",
            Self::Completed => "completed",
            Self::Abandoned => "abandoned",
        }
    }

    fn parse(raw: &str) -> rusqlite::Result<Self> {
        match raw {
            "in_flight" => Ok(Self::InFlight),
            "completed" => Ok(Self::Completed),
            "abandoned" => Ok(Self::Abandoned),
            _ => Err(rusqlite::Error::InvalidParameterName(format!(
                "unknown goal round status {raw:?}"
            ))),
        }
    }
}

/// Why a continuation request did not start a new round.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StopReason {
    /// The goal is paused.
    Paused,
    /// The goal is complete.
    Completed,
    /// The goal is blocked.
    Blocked,
    /// Three consecutive blocked rounds moved the goal to `blocked`.
    BlockedAfterThree,
    /// Three consecutive no-progress rounds moved the goal to `blocked`.
    NoProgressAfterThree,
    /// The round budget is exhausted.
    RoundBudgetExhausted,
    /// The call budget is exhausted.
    CallBudgetExhausted,
    /// The previous continuation started less than one interval ago.
    IntervalNotElapsed,
    /// A round is already in flight; this is the same turn, not a new one.
    InFlight {
        /// In-flight round.
        round: u32,
        /// In-flight round marker.
        marker: String,
    },
}

impl StopReason {
    /// Stable persisted/audit code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Paused => "paused",
            Self::Completed => "completed",
            Self::Blocked => "blocked",
            Self::BlockedAfterThree => "blocked_after_three",
            Self::NoProgressAfterThree => "no_progress_after_three",
            Self::RoundBudgetExhausted => "round_budget_exhausted",
            Self::CallBudgetExhausted => "call_budget_exhausted",
            Self::IntervalNotElapsed => "interval_not_elapsed",
            Self::InFlight { .. } => "in_flight",
        }
    }

    /// Human-readable cause.
    #[must_use]
    pub fn message(&self) -> String {
        match self {
            Self::Paused => "the goal is paused".to_owned(),
            Self::Completed => "the goal is already complete".to_owned(),
            Self::Blocked => "the goal is blocked".to_owned(),
            Self::BlockedAfterThree => {
                "the goal reached three consecutive blocked rounds".to_owned()
            }
            Self::NoProgressAfterThree => {
                "the goal reached three consecutive no-progress rounds".to_owned()
            }
            Self::RoundBudgetExhausted => "the goal round budget is exhausted".to_owned(),
            Self::CallBudgetExhausted => "the goal call budget is exhausted".to_owned(),
            Self::IntervalNotElapsed => {
                "the previous continuation is still inside its interval".to_owned()
            }
            Self::InFlight { round, .. } => {
                format!("round {round} is already in flight; no second turn was created")
            }
        }
    }
}

/// A persisted goal-loop row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GoalLoop {
    /// Stable goal id.
    pub goal_id: String,
    /// Owning run.
    pub run_id: String,
    /// Owning task, when any.
    pub task_id: Option<String>,
    /// Bounded objective text.
    pub objective: String,
    /// Current state.
    pub state: GoalState,
    /// Last started round (`0` before the first continuation).
    pub round: u32,
    /// Round budget.
    pub max_rounds: u32,
    /// Call budget.
    pub max_calls: u32,
    /// Calls recorded across completed rounds.
    pub calls_used: u32,
    /// Minimum seconds between continuation starts.
    pub interval_secs: u64,
    /// Consecutive blocked rounds.
    pub blocked_rounds: u32,
    /// Consecutive no-progress rounds.
    pub no_progress_rounds: u32,
    /// Last continuation start (RFC3339).
    pub last_continuation_at: Option<String>,
    /// Last round that recorded progress (RFC3339).
    pub last_progress_at: Option<String>,
    /// Final/current stop reason code, when any.
    pub stop_reason: Option<String>,
    /// Creation time.
    pub created_at: String,
    /// Last mutation time.
    pub updated_at: String,
    /// Monotonic revision.
    pub rev: u64,
}

/// A persisted round row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GoalRound {
    /// Owning goal.
    pub goal_id: String,
    /// Round number (`1..`).
    pub round: u32,
    /// Unique per-goal round marker.
    pub marker: String,
    /// Current status.
    pub status: RoundStatus,
    /// Calls recorded for this round.
    pub calls: u32,
    /// `true` when progress was recorded.
    pub progress: bool,
    /// `true` when the round was reported blocked.
    pub blocked: bool,
    /// Start time.
    pub started_at: String,
    /// Completion/abandonment time.
    pub completed_at: Option<String>,
    /// Bounded outcome detail.
    pub detail: String,
}

impl GoalRound {
    /// Explicit no-progress marker (`progress == false`).
    #[must_use]
    pub const fn no_progress(&self) -> bool {
        !self.progress
    }
}

/// One persisted continuation start (one delivered turn).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GoalContinuation {
    /// Owning goal.
    pub goal_id: String,
    /// Round started.
    pub round: u32,
    /// Unique interval bucket.
    pub interval_key: i64,
    /// Round marker.
    pub marker: String,
    /// Start time.
    pub created_at: String,
}

/// One goal audit event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GoalAuditEvent {
    /// Audit row id.
    pub audit_id: String,
    /// Owning goal.
    pub goal_id: String,
    /// Action name.
    pub action: String,
    /// Outcome label (`ok`, `stopped`, `blocked`, `proposed`, ...).
    pub outcome: String,
    /// JSON detail.
    pub detail_json: String,
    /// Event time.
    pub created_at: String,
}

/// Inputs for [`create_goal`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewGoal {
    /// Owning run.
    pub run_id: RunId,
    /// Owning task, when any.
    pub task_id: Option<TaskId>,
    /// Bounded objective text.
    pub objective: String,
    /// Round budget (`1..=MAX_ROUNDS`).
    pub max_rounds: u32,
    /// Call budget (`1..=MAX_CALLS`).
    pub max_calls: u32,
    /// Minimum continuation spacing in seconds.
    pub interval_secs: u64,
}

impl NewGoal {
    /// Explicit goal inputs.
    #[must_use]
    pub fn new(
        run_id: RunId,
        objective: impl Into<String>,
        max_rounds: u32,
        max_calls: u32,
        interval_secs: u64,
    ) -> Self {
        Self {
            run_id,
            task_id: None,
            objective: objective.into(),
            max_rounds,
            max_calls,
            interval_secs,
        }
    }

    /// Attaches the owning task.
    #[must_use]
    pub const fn with_task(mut self, task_id: TaskId) -> Self {
        self.task_id = Some(task_id);
        self
    }
}

/// Outcome of a continuation request.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ContinuationDecision {
    /// A new round was created and one turn may be delivered.
    Continue {
        /// Updated goal.
        goal: GoalLoop,
        /// New round.
        round: GoalRound,
        /// Rounds left after this one.
        remaining_rounds: u32,
        /// Calls left in the goal budget.
        remaining_calls: u32,
    },
    /// No new turn was created; the named reason stopped it.
    Stop {
        /// Current goal.
        goal: GoalLoop,
        /// Why no continuation was created.
        reason: StopReason,
    },
}

/// Outcome of [`resume_goal`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ResumeOutcome {
    /// A paused goal was resumed and no round was in flight.
    Resumed,
    /// The goal was already active and no round was in flight.
    AlreadyActive,
    /// A round survived the crash and is still inside its lease.
    InFlight {
        /// Surviving round.
        round: u32,
        /// Its original marker.
        marker: String,
    },
    /// An expired in-flight round was abandoned; the next interval may resume.
    Reclaimed {
        /// Round that was abandoned.
        abandoned_round: u32,
    },
}

/// Result of [`resume_goal`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ResumeReport {
    /// Goal after the resume attempt.
    pub goal: GoalLoop,
    /// What resume found.
    pub outcome: ResumeOutcome,
}

/// One round outcome supplied by the caller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoundOutcome {
    /// Calls actually made in this round.
    pub calls: u32,
    /// Whether the round made progress.
    pub progress: bool,
    /// Whether the round reported itself blocked.
    pub blocked: bool,
    /// Bounded detail text.
    pub detail: String,
}

impl RoundOutcome {
    /// A non-blocked round with the given call count and progress flag.
    #[must_use]
    pub fn new(calls: u32, progress: bool) -> Self {
        Self {
            calls,
            progress,
            blocked: false,
            detail: String::new(),
        }
    }

    /// Marks the round blocked.
    #[must_use]
    pub fn blocked(mut self) -> Self {
        self.blocked = true;
        self
    }

    /// Attaches detail text.
    #[must_use]
    pub fn with_detail(mut self, detail: impl Into<String>) -> Self {
        self.detail = detail.into();
        self
    }
}

/// Result of [`record_round`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RoundReport {
    /// Goal after the write.
    pub goal: GoalLoop,
    /// Round after the write.
    pub round: GoalRound,
    /// `true` when the round was already complete and nothing was counted twice.
    pub duplicate: bool,
}

/// Receipt for a self-edit proposal. It is never an application/merge receipt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProposalReceipt {
    /// Stable proposal id.
    pub proposal_id: String,
    /// Owning goal.
    pub goal_id: String,
    /// Round the proposal refers to.
    pub round: u32,
    /// Always [`SELF_EDIT_KIND`].
    pub kind: String,
    /// Always `false`: this module never applies a proposal.
    pub applied: bool,
    /// Always `true`: an operator approval path must still act.
    pub requires_operator_approval: bool,
    /// Always `false`: no merge was performed.
    pub merge_performed: bool,
    /// Always `false`: no approval was bypassed.
    pub approval_bypassed: bool,
    /// Proposal time.
    pub created_at: String,
}

/// Named failure from the goal loop.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GoalError {
    /// No goal row with this id.
    NotFound {
        /// Missing goal id.
        goal_id: String,
    },
    /// An input field is invalid.
    Invalid {
        /// Field name.
        field: &'static str,
        /// Human-readable cause.
        reason: String,
    },
    /// A state transition is not allowed from the persisted state.
    InvalidTransition {
        /// Current state.
        from: GoalState,
        /// Requested action.
        action: &'static str,
    },
    /// No round row for this goal/number.
    RoundNotFound {
        /// Owning goal.
        goal_id: String,
        /// Requested round.
        round: u32,
    },
    /// The round is not in flight (already completed/abandoned).
    RoundNotInFlight {
        /// Requested round.
        round: u32,
        /// Actual status.
        status: RoundStatus,
    },
    /// The round would exceed the goal call budget.
    CallBudgetExceeded {
        /// Calls already recorded.
        used: u32,
        /// Calls requested by this round.
        requested: u32,
        /// Configured budget.
        max: u32,
    },
    /// A unique per-goal marker/interval constraint was violated.
    DuplicateMarker {
        /// Marker or interval description.
        marker: String,
    },
    /// A persisted timestamp could not be parsed, so interval math is unsafe.
    InvalidTimestamp,
    /// SQLite failure (including a foreign-key or uniqueness violation).
    Store(String),
}

impl GoalError {
    /// Stable refusal code.
    #[must_use]
    pub fn code(&self) -> &'static str {
        match self {
            Self::NotFound { .. } => "E_GOAL_NOT_FOUND",
            Self::Invalid { .. } => "E_GOAL_INVALID",
            Self::InvalidTransition { .. } => "E_GOAL_TRANSITION",
            Self::RoundNotFound { .. } => "E_GOAL_ROUND_NOT_FOUND",
            Self::RoundNotInFlight { .. } => "E_GOAL_ROUND_STATE",
            Self::CallBudgetExceeded { .. } => "E_GOAL_CALL_BUDGET",
            Self::DuplicateMarker { .. } => "E_GOAL_DUPLICATE",
            Self::InvalidTimestamp => "E_GOAL_TIMESTAMP",
            Self::Store(_) => "E_GOAL_STORE",
        }
    }

    /// Human-readable cause.
    #[must_use]
    pub fn message(&self) -> String {
        match self {
            Self::NotFound { goal_id } => format!("goal {goal_id} does not exist"),
            Self::Invalid { field, reason } => format!("goal {field} is invalid: {reason}"),
            Self::InvalidTransition { from, action } => {
                format!("cannot {action} a goal in state {}", from.as_str())
            }
            Self::RoundNotFound { goal_id, round } => {
                format!("goal {goal_id} has no round {round}")
            }
            Self::RoundNotInFlight { round, status } => {
                format!("goal round {round} is {}, not in_flight", status.as_str())
            }
            Self::CallBudgetExceeded {
                used,
                requested,
                max,
            } => format!("goal call budget exceeded: {used} used + {requested} requested > {max}"),
            Self::DuplicateMarker { marker } => {
                format!("goal continuation marker/interval already exists: {marker}")
            }
            Self::InvalidTimestamp => {
                "a goal timestamp is not valid RFC3339; interval math was refused".to_owned()
            }
            Self::Store(detail) => format!("goal store error: {detail}"),
        }
    }
}

impl std::fmt::Display for GoalError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message())
    }
}

impl std::error::Error for GoalError {}

impl From<rusqlite::Error> for GoalError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Store(error.to_string())
    }
}

fn validate_new_goal(goal: &NewGoal) -> Result<(), GoalError> {
    let objective = goal.objective.trim();
    if objective.is_empty() || objective.len() > MAX_OBJECTIVE_BYTES {
        return Err(GoalError::Invalid {
            field: "objective",
            reason: format!("must be 1..={MAX_OBJECTIVE_BYTES} bytes"),
        });
    }
    if objective
        .chars()
        .any(|character| character.is_control() && !matches!(character, '\n' | '\r' | '\t'))
    {
        return Err(GoalError::Invalid {
            field: "objective",
            reason: "must not contain control characters other than newlines/tabs".to_owned(),
        });
    }
    if !(1..=MAX_ROUNDS).contains(&goal.max_rounds) {
        return Err(GoalError::Invalid {
            field: "max_rounds",
            reason: format!("must be 1..={MAX_ROUNDS}"),
        });
    }
    if !(1..=MAX_CALLS).contains(&goal.max_calls) {
        return Err(GoalError::Invalid {
            field: "max_calls",
            reason: format!("must be 1..={MAX_CALLS}"),
        });
    }
    if !(MIN_INTERVAL_SECS..=MAX_INTERVAL_SECS).contains(&goal.interval_secs) {
        return Err(GoalError::Invalid {
            field: "interval_secs",
            reason: format!("must be {MIN_INTERVAL_SECS}..={MAX_INTERVAL_SECS}"),
        });
    }
    Ok(())
}

fn validate_actor(actor: &str) -> Result<(), GoalError> {
    let trimmed = actor.trim();
    if trimmed.is_empty() || trimmed.len() > MAX_ACTOR_BYTES {
        return Err(GoalError::Invalid {
            field: "actor",
            reason: format!("must be 1..={MAX_ACTOR_BYTES} bytes"),
        });
    }
    if trimmed.chars().any(char::is_control) {
        return Err(GoalError::Invalid {
            field: "actor",
            reason: "must not contain control characters".to_owned(),
        });
    }
    Ok(())
}

fn validate_detail(detail: &str) -> Result<(), GoalError> {
    if detail.len() > MAX_DETAIL_BYTES {
        return Err(GoalError::Invalid {
            field: "detail",
            reason: format!("must not exceed {MAX_DETAIL_BYTES} bytes"),
        });
    }
    Ok(())
}

fn validate_proposal(body: &str) -> Result<(), GoalError> {
    let trimmed = body.trim();
    if trimmed.is_empty() || trimmed.len() > MAX_PROPOSAL_BYTES {
        return Err(GoalError::Invalid {
            field: "proposal",
            reason: format!("must be 1..={MAX_PROPOSAL_BYTES} bytes"),
        });
    }
    Ok(())
}

fn row_u32(row: &rusqlite::Row<'_>, index: usize) -> rusqlite::Result<u32> {
    let value: i64 = row.get(index)?;
    u32::try_from(value).map_err(|_| rusqlite::Error::IntegralValueOutOfRange(index, value))
}

fn row_u64(row: &rusqlite::Row<'_>, index: usize) -> rusqlite::Result<u64> {
    let value: i64 = row.get(index)?;
    u64::try_from(value).map_err(|_| rusqlite::Error::IntegralValueOutOfRange(index, value))
}

fn map_goal(row: &rusqlite::Row<'_>) -> rusqlite::Result<GoalLoop> {
    let state: String = row.get(4)?;
    Ok(GoalLoop {
        goal_id: row.get(0)?,
        run_id: row.get(1)?,
        task_id: row.get(2)?,
        objective: row.get(3)?,
        state: GoalState::parse(&state)?,
        round: row_u32(row, 5)?,
        max_rounds: row_u32(row, 6)?,
        max_calls: row_u32(row, 7)?,
        calls_used: row_u32(row, 8)?,
        interval_secs: row_u64(row, 9)?,
        blocked_rounds: row_u32(row, 10)?,
        no_progress_rounds: row_u32(row, 11)?,
        last_continuation_at: row.get(12)?,
        last_progress_at: row.get(13)?,
        stop_reason: row.get(14)?,
        created_at: row.get(15)?,
        updated_at: row.get(16)?,
        rev: row_u64(row, 17)?,
    })
}

fn map_round(row: &rusqlite::Row<'_>) -> rusqlite::Result<GoalRound> {
    let status: String = row.get(3)?;
    Ok(GoalRound {
        goal_id: row.get(0)?,
        round: row_u32(row, 1)?,
        marker: row.get(2)?,
        status: RoundStatus::parse(&status)?,
        calls: row_u32(row, 4)?,
        progress: row.get::<_, i64>(5)? != 0,
        blocked: row.get::<_, i64>(6)? != 0,
        started_at: row.get(7)?,
        completed_at: row.get(8)?,
        detail: row.get(9)?,
    })
}

const GOAL_COLUMNS: &str = "goal_id, run_id, task_id, objective, state, round, max_rounds, \
     max_calls, calls_used, interval_secs, blocked_rounds, no_progress_rounds, \
     last_continuation_at, last_progress_at, stop_reason, created_at, updated_at, rev";

const ROUND_COLUMNS: &str = "goal_id, round, marker, status, calls, progress, blocked, \
     started_at, completed_at, detail_json";

fn read_goal(connection: &Connection, goal_id: &str) -> Result<Option<GoalLoop>, GoalError> {
    let sql = format!("SELECT {GOAL_COLUMNS} FROM goal_loops WHERE goal_id = ?1");
    let goal = connection
        .query_row(&sql, params![goal_id], map_goal)
        .optional()
        .map_err(|error| GoalError::Store(format!("could not read goal {goal_id}: {error}")))?;
    Ok(goal)
}

/// Reads one goal.
///
/// # Errors
/// [`GoalError::NotFound`] when no row has this id; [`GoalError::Store`] for a
/// SQLite failure or an unparseable persisted state.
pub fn get_goal(connection: &Connection, goal_id: &str) -> Result<GoalLoop, GoalError> {
    read_goal(connection, goal_id)?.ok_or_else(|| GoalError::NotFound {
        goal_id: goal_id.to_owned(),
    })
}

fn read_round(
    connection: &Connection,
    goal_id: &str,
    round: u32,
) -> Result<Option<GoalRound>, GoalError> {
    let sql = format!("SELECT {ROUND_COLUMNS} FROM goal_rounds WHERE goal_id = ?1 AND round = ?2");
    let row = connection
        .query_row(&sql, params![goal_id, i64::from(round)], map_round)
        .optional()
        .map_err(|error| {
            GoalError::Store(format!(
                "could not read goal round {goal_id}/{round}: {error}"
            ))
        })?;
    Ok(row)
}

/// Reads one round.
///
/// # Errors
/// [`GoalError::RoundNotFound`] when absent; [`GoalError::Store`] otherwise.
pub fn get_round(
    connection: &Connection,
    goal_id: &str,
    round: u32,
) -> Result<GoalRound, GoalError> {
    read_round(connection, goal_id, round)?.ok_or_else(|| GoalError::RoundNotFound {
        goal_id: goal_id.to_owned(),
        round,
    })
}

fn read_in_flight_round(
    connection: &Connection,
    goal_id: &str,
) -> Result<Option<GoalRound>, GoalError> {
    let sql = format!(
        "SELECT {ROUND_COLUMNS} FROM goal_rounds
         WHERE goal_id = ?1 AND status = 'in_flight'
         ORDER BY round DESC LIMIT 1"
    );
    let row = connection
        .query_row(&sql, params![goal_id], map_round)
        .optional()
        .map_err(|error| {
            GoalError::Store(format!(
                "could not read in-flight round for {goal_id}: {error}"
            ))
        })?;
    Ok(row)
}

/// Lists every round of one goal in ascending round order.
///
/// # Errors
/// [`GoalError::Store`] for a SQLite failure.
pub fn goal_rounds(connection: &Connection, goal_id: &str) -> Result<Vec<GoalRound>, GoalError> {
    let sql =
        format!("SELECT {ROUND_COLUMNS} FROM goal_rounds WHERE goal_id = ?1 ORDER BY round ASC");
    let mut statement = connection
        .prepare(&sql)
        .map_err(|error| GoalError::Store(format!("could not prepare goal round list: {error}")))?;
    let rows = statement
        .query_map(params![goal_id], map_round)
        .map_err(|error| GoalError::Store(format!("could not list goal rounds: {error}")))?;
    let mut rounds = Vec::new();
    for row in rows {
        rounds.push(row.map_err(|error| {
            GoalError::Store(format!("could not decode a goal round: {error}"))
        })?);
    }
    Ok(rounds)
}

/// Lists every continuation start of one goal.
///
/// # Errors
/// [`GoalError::Store`] for a SQLite failure.
pub fn goal_continuations(
    connection: &Connection,
    goal_id: &str,
) -> Result<Vec<GoalContinuation>, GoalError> {
    let mut statement = connection
        .prepare(
            "SELECT goal_id, round, interval_key, marker, created_at
             FROM goal_continuations WHERE goal_id = ?1 ORDER BY round ASC",
        )
        .map_err(|error| {
            GoalError::Store(format!("could not prepare goal continuation list: {error}"))
        })?;
    let rows = statement
        .query_map(params![goal_id], |row| {
            Ok(GoalContinuation {
                goal_id: row.get(0)?,
                round: row_u32(row, 1)?,
                interval_key: row.get(2)?,
                marker: row.get(3)?,
                created_at: row.get(4)?,
            })
        })
        .map_err(|error| GoalError::Store(format!("could not list continuations: {error}")))?;
    let mut continuations = Vec::new();
    for row in rows {
        continuations.push(row.map_err(|error| {
            GoalError::Store(format!("could not decode a continuation: {error}"))
        })?);
    }
    Ok(continuations)
}

/// Lists one goal's audit events in insertion order.
///
/// # Errors
/// [`GoalError::Store`] for a SQLite failure.
pub fn goal_audit(
    connection: &Connection,
    goal_id: &str,
) -> Result<Vec<GoalAuditEvent>, GoalError> {
    let mut statement = connection
        .prepare(
            "SELECT audit_id, goal_id, action, outcome, detail_json, created_at
             FROM goal_audit WHERE goal_id = ?1 ORDER BY rowid ASC",
        )
        .map_err(|error| GoalError::Store(format!("could not prepare goal audit list: {error}")))?;
    let rows = statement
        .query_map(params![goal_id], |row| {
            Ok(GoalAuditEvent {
                audit_id: row.get(0)?,
                goal_id: row.get(1)?,
                action: row.get(2)?,
                outcome: row.get(3)?,
                detail_json: row.get(4)?,
                created_at: row.get(5)?,
            })
        })
        .map_err(|error| GoalError::Store(format!("could not list goal audit: {error}")))?;
    let mut events = Vec::new();
    for row in rows {
        events.push(row.map_err(|error| {
            GoalError::Store(format!("could not decode a goal audit event: {error}"))
        })?);
    }
    Ok(events)
}

fn append_audit(
    connection: &Connection,
    goal_id: &str,
    action: &str,
    outcome: &str,
    detail: &serde_json::Value,
    now: &Timestamp,
) -> Result<(), GoalError> {
    connection
        .execute(
            "INSERT INTO goal_audit
             (audit_id, goal_id, action, outcome, detail_json, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                format!("audit:{}", Uuid::now_v7()),
                goal_id,
                action,
                outcome,
                detail.to_string(),
                now.as_str()
            ],
        )
        .map_err(|error| {
            GoalError::Store(format!("could not append goal audit {action}: {error}"))
        })?;
    Ok(())
}

fn unix_secs(now: &Timestamp) -> Result<u64, GoalError> {
    now.unix_secs().map_err(|_| GoalError::InvalidTimestamp)
}

fn timestamp_secs(raw: &str) -> Result<u64, GoalError> {
    Timestamp::parse(raw)
        .map_err(|_| GoalError::InvalidTimestamp)?
        .unix_secs()
        .map_err(|_| GoalError::InvalidTimestamp)
}

/// Deterministic marker for one `(goal, round)` pair.
///
/// The pair is unique in `goal_rounds`, and `goal_continuations` also enforces
/// `UNIQUE(goal_id, marker)`, so the same round can never be delivered twice
/// under a second marker.
#[must_use]
pub fn round_marker(goal_id: &str, round: u32) -> String {
    format!("{goal_id}:round:{round}")
}

fn interval_key(now: &Timestamp, interval_secs: u64) -> Result<i64, GoalError> {
    let seconds = unix_secs(now)?;
    let bucket = seconds / interval_secs;
    i64::try_from(bucket).map_err(|_| GoalError::InvalidTimestamp)
}

/// Creates an active goal. The insert and its `goal_created` audit event share
/// one transaction.
///
/// # Errors
/// [`GoalError::Invalid`] for an out-of-range budget/interval or invalid
/// objective; [`GoalError::Store`] for a SQLite failure.
pub fn create_goal(
    connection: &Connection,
    goal: &NewGoal,
    now: Timestamp,
) -> Result<GoalLoop, GoalError> {
    validate_new_goal(goal)?;
    let goal_id = format!("goal:{}", Uuid::now_v7());
    let transaction = connection.unchecked_transaction()?;
    transaction.execute(
        "INSERT INTO goal_loops
         (goal_id, run_id, task_id, objective, state, round, max_rounds, max_calls,
          calls_used, interval_secs, blocked_rounds, no_progress_rounds,
          last_continuation_at, last_progress_at, stop_reason, created_at,
          updated_at, rev)
         VALUES (?1, ?2, ?3, ?4, 'active', 0, ?5, ?6, 0, ?7, 0, 0,
                 NULL, NULL, NULL, ?8, ?8, 0)",
        params![
            goal_id,
            goal.run_id.to_string(),
            goal.task_id.map(|task_id| task_id.to_string()),
            goal.objective.trim(),
            i64::from(goal.max_rounds),
            i64::from(goal.max_calls),
            i64::try_from(goal.interval_secs).unwrap_or(i64::MAX),
            now.as_str()
        ],
    )?;
    append_audit(
        &transaction,
        &goal_id,
        "goal_created",
        "ok",
        &serde_json::json!({
            "run_id": goal.run_id.to_string(),
            "task_id": goal.task_id.map(|task_id| task_id.to_string()),
            "max_rounds": goal.max_rounds,
            "max_calls": goal.max_calls,
            "interval_secs": goal.interval_secs,
        }),
        &now,
    )?;
    transaction.commit()?;
    get_goal(connection, &goal_id)
}

/// Pauses an active goal. Pausing an already-paused goal is idempotent.
///
/// # Errors
/// [`GoalError::InvalidTransition`] from `completed`/`blocked`;
/// [`GoalError::Invalid`] for a blank actor; [`GoalError::Store`] otherwise.
pub fn pause_goal(
    connection: &Connection,
    goal_id: &str,
    now: Timestamp,
    actor: &str,
) -> Result<GoalLoop, GoalError> {
    validate_actor(actor)?;
    let transaction = connection.unchecked_transaction()?;
    let goal = read_goal(&transaction, goal_id)?.ok_or_else(|| GoalError::NotFound {
        goal_id: goal_id.to_owned(),
    })?;
    if goal.state == GoalState::Paused {
        transaction.commit()?;
        return Ok(goal);
    }
    if !matches!(goal.state, GoalState::Active) {
        return Err(GoalError::InvalidTransition {
            from: goal.state,
            action: "pause",
        });
    }
    transaction.execute(
        "UPDATE goal_loops SET state = 'paused', updated_at = ?2, rev = rev + 1
         WHERE goal_id = ?1 AND state = 'active'",
        params![goal_id, now.as_str()],
    )?;
    append_audit(
        &transaction,
        goal_id,
        "goal_paused",
        "ok",
        &serde_json::json!({"actor": actor.trim(), "round": goal.round}),
        &now,
    )?;
    transaction.commit()?;
    get_goal(connection, goal_id)
}

fn handle_in_flight(
    connection: &Connection,
    goal_id: &str,
    now: &Timestamp,
) -> Result<ResumeOutcome, GoalError> {
    let Some(round) = read_in_flight_round(connection, goal_id)? else {
        return Ok(ResumeOutcome::AlreadyActive);
    };
    let started = timestamp_secs(&round.started_at)?;
    let now_secs = unix_secs(now)?;
    if now_secs < started.saturating_add(IN_FLIGHT_LEASE_SECS) {
        return Ok(ResumeOutcome::InFlight {
            round: round.round,
            marker: round.marker,
        });
    }
    connection.execute(
        "UPDATE goal_rounds SET status = 'abandoned', completed_at = ?3
         WHERE goal_id = ?1 AND round = ?2 AND status = 'in_flight'",
        params![goal_id, i64::from(round.round), now.as_str()],
    )?;
    append_audit(
        connection,
        goal_id,
        "round_abandoned",
        "reclaimed",
        &serde_json::json!({
            "round": round.round,
            "marker": round.marker,
            "reason": "in_flight_lease_expired",
            "lease_secs": IN_FLIGHT_LEASE_SECS,
        }),
        now,
    )?;
    Ok(ResumeOutcome::Reclaimed {
        abandoned_round: round.round,
    })
}

/// Resumes a paused goal, or re-attaches to a round that survived a crash.
///
/// A surviving in-flight round keeps its original marker and is never
/// duplicated. If its lease has expired, it is marked `abandoned` and audited;
/// the goal stays active so a later interval can start a fresh round.
///
/// # Errors
/// [`GoalError::InvalidTransition`] from `completed`/`blocked`;
/// [`GoalError::Invalid`] for a blank actor; [`GoalError::Store`] otherwise.
pub fn resume_goal(
    connection: &Connection,
    goal_id: &str,
    now: Timestamp,
    actor: &str,
) -> Result<ResumeReport, GoalError> {
    validate_actor(actor)?;
    let transaction = connection.unchecked_transaction()?;
    let goal = read_goal(&transaction, goal_id)?.ok_or_else(|| GoalError::NotFound {
        goal_id: goal_id.to_owned(),
    })?;
    if matches!(goal.state, GoalState::Completed | GoalState::Blocked) {
        return Err(GoalError::InvalidTransition {
            from: goal.state,
            action: "resume",
        });
    }
    let resumed = if goal.state == GoalState::Paused {
        transaction.execute(
            "UPDATE goal_loops SET state = 'active', updated_at = ?2, rev = rev + 1
             WHERE goal_id = ?1 AND state = 'paused'",
            params![goal_id, now.as_str()],
        )?;
        append_audit(
            &transaction,
            goal_id,
            "goal_resumed",
            "ok",
            &serde_json::json!({"actor": actor.trim(), "round": goal.round}),
            &now,
        )?;
        true
    } else {
        false
    };
    let outcome = handle_in_flight(&transaction, goal_id, &now)?;
    let outcome = match (resumed, outcome) {
        (true, ResumeOutcome::AlreadyActive) => ResumeOutcome::Resumed,
        (_, outcome) => outcome,
    };
    transaction.commit()?;
    Ok(ResumeReport {
        goal: get_goal(connection, goal_id)?,
        outcome,
    })
}

/// Completes an active or paused goal. Completing an already-complete goal is
/// idempotent.
///
/// # Errors
/// [`GoalError::InvalidTransition`] for `blocked`;
/// [`GoalError::Invalid`] for a blank actor/reason; [`GoalError::Store`]
/// otherwise.
pub fn complete_goal(
    connection: &Connection,
    goal_id: &str,
    now: Timestamp,
    actor: &str,
    reason: &str,
) -> Result<GoalLoop, GoalError> {
    validate_actor(actor)?;
    let reason = reason.trim();
    if reason.is_empty() || reason.len() > 512 {
        return Err(GoalError::Invalid {
            field: "reason",
            reason: "must be 1..=512 bytes".to_owned(),
        });
    }
    let transaction = connection.unchecked_transaction()?;
    let goal = read_goal(&transaction, goal_id)?.ok_or_else(|| GoalError::NotFound {
        goal_id: goal_id.to_owned(),
    })?;
    if goal.state == GoalState::Completed {
        transaction.commit()?;
        return Ok(goal);
    }
    if goal.state == GoalState::Blocked {
        return Err(GoalError::InvalidTransition {
            from: goal.state,
            action: "complete",
        });
    }
    if let Some(round) = read_in_flight_round(&transaction, goal_id)? {
        transaction.execute(
            "UPDATE goal_rounds SET status = 'abandoned', completed_at = ?3
             WHERE goal_id = ?1 AND round = ?2 AND status = 'in_flight'",
            params![goal_id, i64::from(round.round), now.as_str()],
        )?;
        append_audit(
            &transaction,
            goal_id,
            "round_abandoned",
            "goal_completed",
            &serde_json::json!({
                "round": round.round,
                "marker": round.marker,
                "reason": "goal_completed",
            }),
            &now,
        )?;
    }
    transaction.execute(
        "UPDATE goal_loops SET state = 'completed', stop_reason = ?2,
             updated_at = ?3, rev = rev + 1
         WHERE goal_id = ?1 AND state IN ('active','paused')",
        params![goal_id, StopReason::Completed.code(), now.as_str()],
    )?;
    append_audit(
        &transaction,
        goal_id,
        "goal_completed",
        "ok",
        &serde_json::json!({"actor": actor.trim(), "reason": reason}),
        &now,
    )?;
    transaction.commit()?;
    get_goal(connection, goal_id)
}

fn persist_stop_reason(
    connection: &Connection,
    goal: &GoalLoop,
    reason: &StopReason,
    now: &Timestamp,
) -> Result<(), GoalError> {
    let code = reason.code();
    if goal.stop_reason.as_deref() == Some(code) {
        return Ok(());
    }
    connection.execute(
        "UPDATE goal_loops SET stop_reason = ?2, updated_at = ?3, rev = rev + 1
         WHERE goal_id = ?1",
        params![goal.goal_id, code, now.as_str()],
    )?;
    append_audit(
        connection,
        &goal.goal_id,
        "continuation_stopped",
        "stopped",
        &serde_json::json!({"reason": code}),
        now,
    )?;
    Ok(())
}

/// Requests one continuation turn.
///
/// At most one round starts per interval. A second request in the same interval
/// returns [`StopReason::InFlight`] (round not finished) or
/// [`StopReason::IntervalNotElapsed`] (round finished) and creates no new row.
///
/// # Errors
/// [`GoalError::NotFound`]; [`GoalError::Store`] for a SQLite/uniqueness
/// failure; [`GoalError::InvalidTimestamp`] for an unparseable persisted time.
pub fn begin_continuation(
    connection: &Connection,
    goal_id: &str,
    now: Timestamp,
) -> Result<ContinuationDecision, GoalError> {
    let transaction = connection.unchecked_transaction()?;
    let goal = read_goal(&transaction, goal_id)?.ok_or_else(|| GoalError::NotFound {
        goal_id: goal_id.to_owned(),
    })?;
    match goal.state {
        GoalState::Paused => {
            transaction.commit()?;
            return Ok(ContinuationDecision::Stop {
                goal,
                reason: StopReason::Paused,
            });
        }
        GoalState::Completed => {
            transaction.commit()?;
            return Ok(ContinuationDecision::Stop {
                goal,
                reason: StopReason::Completed,
            });
        }
        GoalState::Blocked => {
            transaction.commit()?;
            return Ok(ContinuationDecision::Stop {
                goal,
                reason: StopReason::Blocked,
            });
        }
        GoalState::Active => {}
    }

    if let Some(round) = read_in_flight_round(&transaction, goal_id)? {
        let started = timestamp_secs(&round.started_at)?;
        let now_secs = unix_secs(&now)?;
        if now_secs < started.saturating_add(IN_FLIGHT_LEASE_SECS) {
            transaction.commit()?;
            return Ok(ContinuationDecision::Stop {
                goal,
                reason: StopReason::InFlight {
                    round: round.round,
                    marker: round.marker,
                },
            });
        }
        transaction.execute(
            "UPDATE goal_rounds SET status = 'abandoned', completed_at = ?3
             WHERE goal_id = ?1 AND round = ?2 AND status = 'in_flight'",
            params![goal_id, i64::from(round.round), now.as_str()],
        )?;
        append_audit(
            &transaction,
            goal_id,
            "round_abandoned",
            "expired",
            &serde_json::json!({
                "round": round.round,
                "marker": round.marker,
                "reason": "in_flight_lease_expired",
            }),
            &now,
        )?;
    }

    let reason = if goal.round >= goal.max_rounds {
        Some(StopReason::RoundBudgetExhausted)
    } else if goal.calls_used >= goal.max_calls {
        Some(StopReason::CallBudgetExhausted)
    } else {
        None
    };
    if let Some(reason) = reason {
        persist_stop_reason(&transaction, &goal, &reason, &now)?;
        transaction.commit()?;
        return Ok(ContinuationDecision::Stop {
            goal: get_goal(connection, goal_id)?,
            reason,
        });
    }

    if let Some(last) = goal.last_continuation_at.as_deref() {
        let last_secs = timestamp_secs(last)?;
        let now_secs = unix_secs(&now)?;
        if now_secs < last_secs.saturating_add(goal.interval_secs) {
            transaction.commit()?;
            return Ok(ContinuationDecision::Stop {
                goal,
                reason: StopReason::IntervalNotElapsed,
            });
        }
    }
    let key = interval_key(&now, goal.interval_secs)?;
    let occupied: Option<i64> = transaction
        .query_row(
            "SELECT 1 FROM goal_continuations WHERE goal_id = ?1 AND interval_key = ?2",
            params![goal_id, key],
            |row| row.get(0),
        )
        .optional()?;
    if occupied.is_some() {
        transaction.commit()?;
        return Ok(ContinuationDecision::Stop {
            goal,
            reason: StopReason::IntervalNotElapsed,
        });
    }

    let round_number = goal
        .round
        .checked_add(1)
        .ok_or_else(|| GoalError::Invalid {
            field: "round",
            reason: "round counter overflow".to_owned(),
        })?;
    let marker = round_marker(goal_id, round_number);
    transaction.execute(
        "INSERT INTO goal_rounds
         (goal_id, round, marker, status, calls, progress, blocked, started_at,
          completed_at, detail_json)
         VALUES (?1, ?2, ?3, 'in_flight', 0, 1, 0, ?4, NULL, '{}')",
        params![goal_id, i64::from(round_number), marker, now.as_str()],
    )?;
    transaction
        .execute(
            "INSERT INTO goal_continuations
             (goal_id, round, interval_key, marker, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![goal_id, i64::from(round_number), key, marker, now.as_str()],
        )
        .map_err(|error| {
            if error.sqlite_error_code() == Some(rusqlite::ErrorCode::ConstraintViolation) {
                GoalError::DuplicateMarker {
                    marker: round_marker(goal_id, round_number),
                }
            } else {
                GoalError::Store(format!("could not record continuation: {error}"))
            }
        })?;
    transaction.execute(
        "UPDATE goal_loops SET round = ?2, last_continuation_at = ?3,
             stop_reason = NULL, updated_at = ?3, rev = rev + 1
         WHERE goal_id = ?1",
        params![goal_id, i64::from(round_number), now.as_str()],
    )?;
    append_audit(
        &transaction,
        goal_id,
        "continuation_started",
        "ok",
        &serde_json::json!({
            "round": round_number,
            "marker": marker,
            "interval_key": key,
            "remaining_rounds": goal.max_rounds.saturating_sub(round_number),
            "remaining_calls": goal.max_calls.saturating_sub(goal.calls_used),
        }),
        &now,
    )?;
    transaction.commit()?;

    let goal = get_goal(connection, goal_id)?;
    let round = get_round(connection, goal_id, round_number)?;
    Ok(ContinuationDecision::Continue {
        remaining_rounds: goal.max_rounds.saturating_sub(goal.round),
        remaining_calls: goal.max_calls.saturating_sub(goal.calls_used),
        goal,
        round,
    })
}

/// Records one round's outcome.
///
/// Replaying a completed round is idempotent: the same rows are returned with
/// `duplicate: true`, and calls are not counted twice. A blocked or no-progress
/// streak of [`BLOCKED_AFTER`] / [`NO_PROGRESS_LIMIT`] moves the goal to
/// `blocked` and persists the stop reason in the same transaction.
///
/// # Errors
/// [`GoalError::RoundNotFound`], [`GoalError::RoundNotInFlight`],
/// [`GoalError::CallBudgetExceeded`] (a round may not overspend the goal
/// budget), [`GoalError::Invalid`] for oversized detail, or [`GoalError::Store`].
pub fn record_round(
    connection: &Connection,
    goal_id: &str,
    round_number: u32,
    outcome: &RoundOutcome,
    now: Timestamp,
) -> Result<RoundReport, GoalError> {
    validate_detail(&outcome.detail)?;
    let transaction = connection.unchecked_transaction()?;
    let goal = read_goal(&transaction, goal_id)?.ok_or_else(|| GoalError::NotFound {
        goal_id: goal_id.to_owned(),
    })?;
    if goal.state == GoalState::Completed {
        return Err(GoalError::InvalidTransition {
            from: goal.state,
            action: "record a round",
        });
    }
    let round = read_round(&transaction, goal_id, round_number)?.ok_or_else(|| {
        GoalError::RoundNotFound {
            goal_id: goal_id.to_owned(),
            round: round_number,
        }
    })?;
    if round.status == RoundStatus::Completed {
        transaction.commit()?;
        return Ok(RoundReport {
            goal,
            round,
            duplicate: true,
        });
    }
    if round.status != RoundStatus::InFlight {
        return Err(GoalError::RoundNotInFlight {
            round: round_number,
            status: round.status,
        });
    }
    let remaining = goal.max_calls.saturating_sub(goal.calls_used);
    if outcome.calls > remaining {
        return Err(GoalError::CallBudgetExceeded {
            used: goal.calls_used,
            requested: outcome.calls,
            max: goal.max_calls,
        });
    }

    let detail_json = serde_json::json!({
        "detail": outcome.detail.trim(),
        "calls": outcome.calls,
        "progress": outcome.progress,
        "blocked": outcome.blocked,
    })
    .to_string();
    transaction.execute(
        "UPDATE goal_rounds SET status = 'completed', calls = ?3, progress = ?4,
             blocked = ?5, completed_at = ?6, detail_json = ?7
         WHERE goal_id = ?1 AND round = ?2 AND status = 'in_flight'",
        params![
            goal_id,
            i64::from(round_number),
            i64::from(outcome.calls),
            i64::from(i32::from(outcome.progress)),
            i64::from(i32::from(outcome.blocked)),
            now.as_str(),
            detail_json
        ],
    )?;

    let blocked_rounds = if outcome.blocked {
        goal.blocked_rounds.saturating_add(1)
    } else {
        0
    };
    let no_progress_rounds = if outcome.progress {
        0
    } else {
        goal.no_progress_rounds.saturating_add(1)
    };
    let calls_used = goal.calls_used.saturating_add(outcome.calls);
    let last_progress_at = if outcome.progress {
        Some(now.as_str().to_owned())
    } else {
        goal.last_progress_at.clone()
    };
    let (state, stop_reason) = if blocked_rounds >= BLOCKED_AFTER {
        (
            GoalState::Blocked,
            Some(StopReason::BlockedAfterThree.code().to_owned()),
        )
    } else if no_progress_rounds >= NO_PROGRESS_LIMIT {
        (
            GoalState::Blocked,
            Some(StopReason::NoProgressAfterThree.code().to_owned()),
        )
    } else {
        (goal.state, goal.stop_reason.clone())
    };
    transaction.execute(
        "UPDATE goal_loops SET calls_used = ?2, blocked_rounds = ?3,
             no_progress_rounds = ?4, last_progress_at = ?5, state = ?6,
             stop_reason = ?7, updated_at = ?8, rev = rev + 1
         WHERE goal_id = ?1",
        params![
            goal_id,
            i64::from(calls_used),
            i64::from(blocked_rounds),
            i64::from(no_progress_rounds),
            last_progress_at,
            state.as_str(),
            stop_reason,
            now.as_str()
        ],
    )?;
    append_audit(
        &transaction,
        goal_id,
        "round_completed",
        "ok",
        &serde_json::json!({
            "round": round_number,
            "calls": outcome.calls,
            "progress": outcome.progress,
            "blocked": outcome.blocked,
            "blocked_rounds": blocked_rounds,
            "no_progress_rounds": no_progress_rounds,
            "calls_used": calls_used,
            "state": state.as_str(),
        }),
        &now,
    )?;
    if state == GoalState::Blocked && goal.state != GoalState::Blocked {
        append_audit(
            &transaction,
            goal_id,
            "goal_blocked",
            "blocked",
            &serde_json::json!({
                "reason": stop_reason,
                "blocked_rounds": blocked_rounds,
                "no_progress_rounds": no_progress_rounds,
            }),
            &now,
        )?;
    }
    transaction.commit()?;
    Ok(RoundReport {
        goal: get_goal(connection, goal_id)?,
        round: get_round(connection, goal_id, round_number)?,
        duplicate: false,
    })
}

/// Persists a self-edit proposal. This is the **only** proposal write path and
/// it never applies, merges, or approves anything.
///
/// # Errors
/// [`GoalError::NotFound`], [`GoalError::RoundNotFound`],
/// [`GoalError::InvalidTransition`] for a completed goal,
/// [`GoalError::Invalid`] for a blank/oversized body, or [`GoalError::Store`].
pub fn record_self_edit_proposal(
    connection: &Connection,
    goal_id: &str,
    round: u32,
    body: &str,
    now: Timestamp,
) -> Result<ProposalReceipt, GoalError> {
    validate_proposal(body)?;
    let transaction = connection.unchecked_transaction()?;
    let goal = read_goal(&transaction, goal_id)?.ok_or_else(|| GoalError::NotFound {
        goal_id: goal_id.to_owned(),
    })?;
    if goal.state == GoalState::Completed {
        return Err(GoalError::InvalidTransition {
            from: goal.state,
            action: "propose a self-edit",
        });
    }
    read_round(&transaction, goal_id, round)?.ok_or_else(|| GoalError::RoundNotFound {
        goal_id: goal_id.to_owned(),
        round,
    })?;
    let proposal_id = format!("proposal:{}", Uuid::now_v7());
    transaction.execute(
        "INSERT INTO goal_proposals
         (proposal_id, goal_id, round, kind, body, status, applied,
          requires_operator_approval, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, 'proposed', 0, 1, ?6)",
        params![
            proposal_id,
            goal_id,
            i64::from(round),
            SELF_EDIT_KIND,
            body.trim(),
            now.as_str()
        ],
    )?;
    append_audit(
        &transaction,
        goal_id,
        "self_edit_proposed",
        "proposed",
        &serde_json::json!({
            "proposal_id": proposal_id,
            "round": round,
            "kind": SELF_EDIT_KIND,
            "applied": false,
            "requires_operator_approval": true,
            "merge_performed": false,
            "approval_bypassed": false,
        }),
        &now,
    )?;
    transaction.commit()?;
    Ok(ProposalReceipt {
        proposal_id,
        goal_id: goal_id.to_owned(),
        round,
        kind: SELF_EDIT_KIND.to_owned(),
        applied: false,
        requires_operator_approval: true,
        merge_performed: false,
        approval_bypassed: false,
        created_at: now.as_str().to_owned(),
    })
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    fn connection() -> Connection {
        let connection = Connection::open_in_memory().expect("in-memory sqlite");
        connection
            .execute_batch("PRAGMA foreign_keys = ON;")
            .expect("foreign keys");
        connection
            .execute_batch(GOAL_LOOP_SCHEMA)
            .expect("goal schema");
        connection
    }

    fn goal(connection: &Connection) -> GoalLoop {
        create_goal(
            connection,
            &NewGoal::new(RunId::new(), "ship the bounded loop", 8, 8, 60),
            Timestamp::from_unix_secs(1_000),
        )
        .expect("goal")
    }

    #[test]
    fn marker_is_unique_per_goal_and_round() {
        assert_eq!(round_marker("goal:a", 1), "goal:a:round:1");
        assert_ne!(round_marker("goal:a", 1), round_marker("goal:b", 1));
        assert_ne!(round_marker("goal:a", 1), round_marker("goal:a", 2));
    }

    #[test]
    fn duplicate_continuation_creates_one_round() {
        let connection = connection();
        let goal = goal(&connection);
        let first =
            begin_continuation(&connection, &goal.goal_id, Timestamp::from_unix_secs(1_000))
                .expect("first");
        let ContinuationDecision::Continue { round, .. } = &first else {
            panic!("first continuation must start: {first:?}");
        };
        assert_eq!(round.round, 1);
        let second =
            begin_continuation(&connection, &goal.goal_id, Timestamp::from_unix_secs(1_001))
                .expect("second");
        assert!(
            matches!(
                second,
                ContinuationDecision::Stop {
                    reason: StopReason::InFlight { round: 1, .. },
                    ..
                }
            ),
            "second call must reuse the in-flight turn: {second:?}"
        );
        assert_eq!(
            goal_rounds(&connection, &goal.goal_id)
                .expect("rounds")
                .len(),
            1
        );
        assert_eq!(
            goal_continuations(&connection, &goal.goal_id)
                .expect("continuations")
                .len(),
            1
        );
    }

    #[test]
    fn completed_round_replay_does_not_double_count_calls() {
        let connection = connection();
        let goal = goal(&connection);
        begin_continuation(&connection, &goal.goal_id, Timestamp::from_unix_secs(1_000))
            .expect("begin");
        let outcome = RoundOutcome::new(1, true);
        let first = record_round(
            &connection,
            &goal.goal_id,
            1,
            &outcome,
            Timestamp::from_unix_secs(1_010),
        )
        .expect("record");
        assert!(!first.duplicate);
        assert_eq!(first.goal.calls_used, 1);
        let replay = record_round(
            &connection,
            &goal.goal_id,
            1,
            &outcome,
            Timestamp::from_unix_secs(1_020),
        )
        .expect("replay");
        assert!(replay.duplicate);
        assert_eq!(replay.goal.calls_used, 1);
    }

    #[test]
    fn budget_stops_before_creating_a_round() {
        let connection = connection();
        let goal = create_goal(
            &connection,
            &NewGoal::new(RunId::new(), "budget", 1, 1, 10),
            Timestamp::from_unix_secs(1_000),
        )
        .expect("goal");
        begin_continuation(&connection, &goal.goal_id, Timestamp::from_unix_secs(1_000))
            .expect("begin");
        record_round(
            &connection,
            &goal.goal_id,
            1,
            &RoundOutcome::new(1, true),
            Timestamp::from_unix_secs(1_001),
        )
        .expect("record");
        let decision =
            begin_continuation(&connection, &goal.goal_id, Timestamp::from_unix_secs(1_100))
                .expect("budget");
        assert!(
            matches!(
                decision,
                ContinuationDecision::Stop {
                    reason: StopReason::RoundBudgetExhausted,
                    ..
                }
            ),
            "{decision:?}"
        );
    }

    #[test]
    fn pause_blocks_continuation_and_resume_restores_it() {
        let connection = connection();
        let goal = goal(&connection);
        pause_goal(
            &connection,
            &goal.goal_id,
            Timestamp::from_unix_secs(1_001),
            "operator:test",
        )
        .expect("pause");
        let paused =
            begin_continuation(&connection, &goal.goal_id, Timestamp::from_unix_secs(1_002))
                .expect("paused");
        assert!(matches!(
            paused,
            ContinuationDecision::Stop {
                reason: StopReason::Paused,
                ..
            }
        ));
        let report = resume_goal(
            &connection,
            &goal.goal_id,
            Timestamp::from_unix_secs(1_003),
            "operator:test",
        )
        .expect("resume");
        assert_eq!(report.outcome, ResumeOutcome::Resumed);
        let resumed =
            begin_continuation(&connection, &goal.goal_id, Timestamp::from_unix_secs(1_004))
                .expect("resumed");
        assert!(matches!(resumed, ContinuationDecision::Continue { .. }));
    }

    #[test]
    fn three_blocked_rounds_block_the_goal() {
        let connection = connection();
        let goal = create_goal(
            &connection,
            &NewGoal::new(RunId::new(), "blocked", 8, 8, 10),
            Timestamp::from_unix_secs(1_000),
        )
        .expect("goal");
        for index in 0..BLOCKED_AFTER {
            let start = 1_000 + u64::from(index) * 10;
            begin_continuation(&connection, &goal.goal_id, Timestamp::from_unix_secs(start))
                .expect("begin");
            record_round(
                &connection,
                &goal.goal_id,
                index + 1,
                &RoundOutcome::new(0, true).blocked(),
                Timestamp::from_unix_secs(start + 1),
            )
            .expect("record");
        }
        let blocked = get_goal(&connection, &goal.goal_id).expect("goal");
        assert_eq!(blocked.state, GoalState::Blocked);
        assert_eq!(blocked.stop_reason.as_deref(), Some("blocked_after_three"));
        let decision =
            begin_continuation(&connection, &goal.goal_id, Timestamp::from_unix_secs(1_100))
                .expect("blocked");
        assert!(matches!(
            decision,
            ContinuationDecision::Stop {
                reason: StopReason::Blocked,
                ..
            }
        ));
    }

    #[test]
    fn three_no_progress_rounds_leave_the_marker_and_block() {
        let connection = connection();
        let goal = create_goal(
            &connection,
            &NewGoal::new(RunId::new(), "no progress", 8, 8, 10),
            Timestamp::from_unix_secs(1_000),
        )
        .expect("goal");
        for index in 0..NO_PROGRESS_LIMIT {
            let start = 1_000 + u64::from(index) * 10;
            begin_continuation(&connection, &goal.goal_id, Timestamp::from_unix_secs(start))
                .expect("begin");
            record_round(
                &connection,
                &goal.goal_id,
                index + 1,
                &RoundOutcome::new(0, false),
                Timestamp::from_unix_secs(start + 1),
            )
            .expect("record");
        }
        let rounds = goal_rounds(&connection, &goal.goal_id).expect("rounds");
        assert!(rounds.iter().all(GoalRound::no_progress));
        let blocked = get_goal(&connection, &goal.goal_id).expect("goal");
        assert_eq!(blocked.state, GoalState::Blocked);
        assert_eq!(
            blocked.stop_reason.as_deref(),
            Some("no_progress_after_three")
        );
    }

    #[test]
    fn self_edit_proposal_is_never_applied() {
        let connection = connection();
        let goal = goal(&connection);
        begin_continuation(&connection, &goal.goal_id, Timestamp::from_unix_secs(1_000))
            .expect("begin");
        record_round(
            &connection,
            &goal.goal_id,
            1,
            &RoundOutcome::new(0, true),
            Timestamp::from_unix_secs(1_001),
        )
        .expect("record");
        let receipt = record_self_edit_proposal(
            &connection,
            &goal.goal_id,
            1,
            "remove the unused branch",
            Timestamp::from_unix_secs(1_002),
        )
        .expect("proposal");
        assert_eq!(receipt.kind, SELF_EDIT_KIND);
        assert!(!receipt.applied);
        assert!(receipt.requires_operator_approval);
        assert!(!receipt.merge_performed);
        assert!(!receipt.approval_bypassed);
        let applied: i64 = connection
            .query_row(
                "SELECT applied FROM goal_proposals WHERE proposal_id = ?1",
                params![receipt.proposal_id],
                |row| row.get(0),
            )
            .expect("row");
        assert_eq!(applied, 0);
    }
}
