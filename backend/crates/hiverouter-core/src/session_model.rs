//! Workspace session / continuation model (read model over the persisted
//! session, run, task, message and todo tables).
//!
//! A session groups runs. This module reads that grouping from the database
//! that already stores it — `sessions`, `runs.session_id`, `todos`,
//! `chat_messages` and `task_results` (schema v25 and v27+). It introduces no
//! table, no column and no cached summary: every value below is derived from
//! those rows at call time, so the model cannot drift from the store.
//!
//! # What this module owns
//!
//! * [`attach_run`] — binds an existing run to a session, with project and
//!   ownership validation (the only write; it updates the existing
//!   `runs.session_id` column).
//! * [`session_run_model`] — the ordered run ledger of a session, the most
//!   recently completed run, and a bounded summary of pending work.
//! * [`continuation_context`] — a byte-bounded continuation context built
//!   **only** from persisted `chat_messages` and `task_results`, with explicit
//!   per-item provenance and explicit truncation/omission accounting.
//! * [`assess_continuation`] — whether a run's persisted state permits a
//!   continuation run, and why.
//! * [`latest_resumable_run`] — the newest failed/interrupted run of a
//!   session, for callers that do not name a run explicitly.
//!
//! # Ordering
//!
//! Runs are ordered by `(created_at ASC, rowid ASC)`, i.e. insertion order with
//! a deterministic tie-break for timestamps that share a second. Messages and
//! task results are ordered first by their run's position, then by
//! `(created_at ASC, source ASC, record_id ASC)`; at the same timestamp a chat
//! message precedes a task result, and record ids break any remaining tie.
//! The continuation context is emitted in that ascending order; selection for
//! the byte budget happens from the newest item backwards.
//!
//! # Boundedness
//!
//! `session_run_model` caps the run ledger at [`MAX_SESSION_RUNS`] and the
//! pending summary at [`MAX_PENDING_ACTIVE_RUNS`] / [`MAX_PENDING_TODOS`] /
//! [`MAX_PENDING_TODO_TEXT_BYTES`], flagging every trim. `continuation_context`
//! caps the stored-text payload at the caller's `budget_bytes` (validated to
//! `1..=MAX_CONTINUATION_BUDGET_BYTES`), flags truncated items, and reports
//! every omitted item.
//!
//! # Read/write profile
//!
//! Every function except [`attach_run`] only reads. `attach_run` performs one
//! guarded `UPDATE runs SET session_id = ...`; it never changes schema.
//!
//! # Integration note
//!
//! This file is not yet declared in `lib.rs` (that edit is outside this task's
//! file scope). The integration test compiles it directly through `#[path]`,
//! and `docs/SESSION-MODEL.md` records the one-line registration plus the
//! HTTP/CLI wiring still required.

use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use hiverouter_contract::AgentState;

/// Default byte budget for a continuation context.
pub const DEFAULT_CONTINUATION_BUDGET_BYTES: usize = 8 * 1024;
/// Hard ceiling for a continuation context budget. Callers cannot ask for a
/// bigger context than this.
pub const MAX_CONTINUATION_BUDGET_BYTES: usize = 256 * 1024;
/// Maximum number of runs listed in a session ledger (newest kept).
pub const MAX_SESSION_RUNS: usize = 500;
/// Maximum number of active runs listed in the pending summary.
pub const MAX_PENDING_ACTIVE_RUNS: usize = 20;
/// Maximum number of open todos listed in the pending summary.
pub const MAX_PENDING_TODOS: usize = 10;
/// Maximum bytes kept per open-todo text in the pending summary.
pub const MAX_PENDING_TODO_TEXT_BYTES: usize = 200;
/// Maximum rows fetched per source (messages, task results) before the byte
/// budget is applied. Keeping this bounded stops a pathological session from
/// being fully materialised in memory.
pub const MAX_CONTEXT_SOURCE_ROWS: usize = 2_000;

/// Result alias for this module.
pub type SessionResult<T> = Result<T, SessionModelError>;

/// Errors raised while reading the session model.
#[derive(Debug, Error)]
pub enum SessionModelError {
    /// The named session does not exist.
    #[error("E_SESSION_NOT_FOUND: session {0} does not exist")]
    SessionNotFound(String),
    /// The session exists but is archived and cannot accept new runs.
    #[error("E_SESSION_ARCHIVED: session {0} is archived")]
    SessionArchived(String),
    /// The named run does not exist.
    #[error("E_RUN_NOT_FOUND: run {0} does not exist")]
    RunNotFound(String),
    /// The run exists but is bound to a different session.
    #[error("E_RUN_NOT_IN_SESSION: run {0} is not attached to session {1}")]
    RunNotInSession(String, String),
    /// The run is already bound to another session.
    #[error("E_RUN_IN_OTHER_SESSION: run {0} already belongs to session {1}")]
    RunInOtherSession(String, String),
    /// Run and session belong to different projects.
    #[error(
        "E_SESSION_PROJECT_MISMATCH: run project {run_project} does not match session project {session_project}"
    )]
    ProjectMismatch {
        /// Project recorded on the run.
        run_project: String,
        /// Project recorded on the session.
        session_project: String,
    },
    /// The requested continuation budget is outside the allowed range.
    #[error(
        "E_CONTEXT_BUDGET_INVALID: budget {0} is outside 1..={MAX_CONTINUATION_BUDGET_BYTES} bytes"
    )]
    InvalidBudget(usize),
    /// A stored run status is not one this binary knows.
    #[error("E_RUN_STATE_UNKNOWN: run {run_id} has persisted status {status:?}")]
    UnknownRunState {
        /// Run whose row is corrupt/unknown.
        run_id: String,
        /// The raw stored string.
        status: String,
    },
    /// The guarded attach update lost a race with another writer.
    #[error("E_ATTACH_CONTENDED: run {0} changed while it was being attached")]
    AttachContended(String),
    /// SQLite reported an error.
    #[error("SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
}

/// Outcome of [`attach_run`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttachOutcome {
    /// The run was unbound and is now bound to the session.
    Attached,
    /// The run was already bound to the session; no write happened.
    AlreadyAttached,
}

/// One run in a session's ordered ledger.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionRun {
    /// Run id.
    pub run_id: String,
    /// 1-based position in the session's full run order.
    pub ordinal: u64,
    /// Persisted lifecycle state.
    pub status: AgentState,
    /// Original prompt.
    pub prompt: String,
    /// Named error code for failed runs.
    pub error_code: Option<String>,
    /// Run creation timestamp.
    pub created_at: String,
}

/// The most recently completed run of a session.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompletedRunRef {
    /// Run id.
    pub run_id: String,
    /// Completion timestamp (`runs.updated_at` at the time the row completed).
    pub completed_at: String,
}

/// One live run in the pending summary.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActiveRunRef {
    /// Run id.
    pub run_id: String,
    /// Persisted lifecycle state.
    pub status: AgentState,
    /// Run creation timestamp.
    pub created_at: String,
}

/// One open todo in the pending summary (text bounded).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingTodo {
    /// Todo id.
    pub todo_id: String,
    /// Monotonic order within the session (as stored).
    pub seq: i64,
    /// Todo text, capped at [`MAX_PENDING_TODO_TEXT_BYTES`].
    pub text: String,
    /// True when `text` was cut to the cap.
    pub truncated: bool,
    /// Creation timestamp.
    pub created_at: String,
}

/// Bounded summary of work still outstanding in a session.
///
/// Derived from persisted rows only: active runs, open todos and unsettled
/// tasks of the session's runs.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingWork {
    /// Active runs, oldest first, capped at [`MAX_PENDING_ACTIVE_RUNS`].
    pub active_runs: Vec<ActiveRunRef>,
    /// Total active runs (not capped).
    pub active_run_count: usize,
    /// True when `active_runs` was capped.
    pub active_runs_truncated: bool,
    /// Open todos, by `seq`, capped at [`MAX_PENDING_TODOS`].
    pub open_todos: Vec<PendingTodo>,
    /// Total open todos (not capped).
    pub open_todo_count: usize,
    /// True when `open_todos` was capped.
    pub open_todos_truncated: bool,
    /// Count of tasks in non-terminal states across the session's runs.
    pub unfinished_task_count: usize,
}

impl PendingWork {
    /// True when nothing is outstanding.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.active_run_count == 0 && self.open_todo_count == 0 && self.unfinished_task_count == 0
    }
}

/// The full read model for one session.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionRunModel {
    /// Session id.
    pub session_id: String,
    /// Project/workspace binding.
    pub project_id: String,
    /// True when the session is archived.
    pub archived: bool,
    /// Run ids in ascending session order (capped at [`MAX_SESSION_RUNS`]).
    pub run_ids: Vec<String>,
    /// Detail rows parallel to `run_ids`.
    pub runs: Vec<SessionRun>,
    /// Total number of runs in the session (not capped).
    pub run_count: usize,
    /// True when `run_ids`/`runs` were capped (oldest runs omitted).
    pub runs_truncated: bool,
    /// Most recently completed run, if any.
    pub last_completed_run: Option<CompletedRunRef>,
    /// Bounded pending-work summary.
    pub pending_work: PendingWork,
}

/// Where one continuation-context item came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextSource {
    /// A persisted `chat_messages` row.
    ChatMessage,
    /// A persisted `task_results` row.
    TaskResult,
}

impl ContextSource {
    /// Stable wire string.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            ContextSource::ChatMessage => "chat_message",
            ContextSource::TaskResult => "task_result",
        }
    }
}

/// One item of continuation context with explicit provenance.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextItem {
    /// Source table.
    pub source: ContextSource,
    /// Run the row belongs to.
    pub run_id: String,
    /// Source row identity: message id or task id.
    pub record_id: String,
    /// Chat role for messages (`user`, `assistant`, `system`, `tool`);
    /// `None` for task results.
    pub role: Option<String>,
    /// Source row timestamp.
    pub created_at: String,
    /// The persisted text, or its prefix when `truncated` is true.
    pub text: String,
    /// Byte length of the stored text before any truncation.
    pub original_bytes: usize,
    /// True when `text` is a prefix of the stored text.
    pub truncated: bool,
}

impl ContextItem {
    /// True when this item's text was cut.
    #[must_use]
    pub fn is_truncated(&self) -> bool {
        self.truncated
    }
}

/// Byte-bounded continuation context for a session.
///
/// `items` are in ascending session order. `used_bytes` is the sum of the
/// included (possibly truncated) text lengths and never exceeds
/// `budget_bytes`. Truncation and omission are both explicit; an empty context
/// is reported as empty, never filled with invented text.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContinuationContext {
    /// Session id.
    pub session_id: String,
    /// Caller-supplied text budget.
    pub budget_bytes: usize,
    /// Sum of included text byte lengths.
    pub used_bytes: usize,
    /// Sum of the original byte lengths of the included items.
    pub original_bytes: usize,
    /// Included items in ascending order.
    pub items: Vec<ContextItem>,
    /// Total items persisted for the session (messages + task results).
    pub total_items: usize,
    /// Items not represented in `items` (budget or fetch cap).
    pub omitted_items: usize,
    /// True when any item was truncated or any item was omitted.
    pub truncated: bool,
    /// Distinct run ids that contributed an item, ascending.
    pub source_run_ids: Vec<String>,
}

impl ContinuationContext {
    /// True when no persisted content was included.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
}

/// How a caller may continue a session from a given run.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContinueDecision {
    /// Create a continuation run seeded with the run's persisted history.
    ContinueSession,
    /// The run is finished or explicitly stopped; a new run may start in the
    /// session but there is no unfinished state to seed from.
    StartNewRun,
    /// Continuation is refused until the blocking condition is resolved.
    Refuse,
}

/// Why [`assess_continuation`] reached its decision.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContinueReason {
    /// Failed run: its persisted history can seed a continuation run.
    FailedRunContinuable,
    /// Interrupted run: recovered after a crash; its history can seed a
    /// continuation run.
    InterruptedRunContinuable,
    /// A run is still executing (or queued); continuing would duplicate work.
    RunStillActive,
    /// The run is paused on an approval decision; resolve that first.
    ApprovalPending,
    /// The run completed; continuation starts a new run.
    RunCompleted,
    /// The run was cancelled by the user; continuation starts a new run.
    RunCancelled,
    /// The run was killed; continuation starts a new run.
    RunKilled,
    /// The persisted state is not a runnable state.
    RunStateNotRunnable,
    /// Archived sessions accept no new runs.
    SessionArchived,
}

impl ContinueReason {
    /// Stable wire code for this reason.
    #[must_use]
    pub fn code(self) -> &'static str {
        match self {
            ContinueReason::FailedRunContinuable => "failed_run_continuable",
            ContinueReason::InterruptedRunContinuable => "interrupted_run_continuable",
            ContinueReason::RunStillActive => "E_RUN_ACTIVE",
            ContinueReason::ApprovalPending => "E_APPROVAL_PENDING",
            ContinueReason::RunCompleted => "run_completed",
            ContinueReason::RunCancelled => "run_cancelled",
            ContinueReason::RunKilled => "run_killed",
            ContinueReason::RunStateNotRunnable => "E_RUN_STATE",
            ContinueReason::SessionArchived => "E_SESSION_ARCHIVED",
        }
    }

    /// Human-readable explanation for the decision.
    #[must_use]
    pub fn message(self) -> &'static str {
        match self {
            ContinueReason::FailedRunContinuable => {
                "the run failed; its persisted messages and task results can seed a continuation run"
            }
            ContinueReason::InterruptedRunContinuable => {
                "the run was interrupted by a restart; its persisted history can seed a continuation run"
            }
            ContinueReason::RunStillActive => {
                "the run is still active; wait for it to finish or cancel it before continuing"
            }
            ContinueReason::ApprovalPending => {
                "the run is paused waiting for an approval decision; resolve the approval first"
            }
            ContinueReason::RunCompleted => {
                "the run completed; continuing the session starts a new run"
            }
            ContinueReason::RunCancelled => {
                "the run was cancelled by the user; continuing the session starts a new run"
            }
            ContinueReason::RunKilled => {
                "the run was killed; continuing the session starts a new run"
            }
            ContinueReason::RunStateNotRunnable => {
                "the persisted run state is not a runnable state"
            }
            ContinueReason::SessionArchived => {
                "the session is archived; unarchive it before continuing"
            }
        }
    }
}

/// Result of [`assess_continuation`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContinueAssessment {
    /// Session id.
    pub session_id: String,
    /// Run the assessment is about.
    pub run_id: String,
    /// Persisted run state.
    pub status: AgentState,
    /// What the caller may do.
    pub decision: ContinueDecision,
    /// Why.
    pub reason: ContinueReason,
    /// Human-readable detail.
    pub detail: String,
}

impl ContinueAssessment {
    /// True when a continuation run may be created.
    #[must_use]
    pub fn allows_continuation(&self) -> bool {
        !matches!(self.decision, ContinueDecision::Refuse)
    }

    /// Stable wire code for the reason.
    #[must_use]
    pub fn reason_code(&self) -> &'static str {
        self.reason.code()
    }
}

/// Attaches an existing run to a session.
///
/// The run must exist, belong to the same project as the session, and either
/// be unbound or already bound to this session. Archived sessions refuse.
/// Attaching twice is idempotent ([`AttachOutcome::AlreadyAttached`]).
///
/// # Errors
/// [`SessionModelError`] for missing/archived session or run, project
/// mismatch, a run bound to another session, or SQLite failure.
pub fn attach_run(
    connection: &Connection,
    session_id: &str,
    run_id: &str,
) -> SessionResult<AttachOutcome> {
    let session = load_session(connection, session_id)?;
    if session.archived {
        return Err(SessionModelError::SessionArchived(session_id.to_owned()));
    }
    let run: (String, Option<String>) = connection
        .query_row(
            "SELECT project_id, session_id FROM runs WHERE id = ?1",
            params![run_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?
        .ok_or_else(|| SessionModelError::RunNotFound(run_id.to_owned()))?;
    let (run_project, current_session) = run;
    if run_project != session.project_id {
        return Err(SessionModelError::ProjectMismatch {
            run_project,
            session_project: session.project_id,
        });
    }
    if let Some(current_session) = current_session {
        if current_session == session_id {
            return Ok(AttachOutcome::AlreadyAttached);
        }
        return Err(SessionModelError::RunInOtherSession(
            run_id.to_owned(),
            current_session,
        ));
    }
    // Guarded update: only an unbound run is attached. If another writer won
    // the race, re-read and report the concrete outcome instead of pretending
    // the attach succeeded.
    for attempt in 0..2 {
        let changed = connection.execute(
            "UPDATE runs SET session_id = ?1 WHERE id = ?2 AND session_id IS NULL",
            params![session_id, run_id],
        )?;
        if changed == 1 {
            return Ok(AttachOutcome::Attached);
        }
        let now: Option<Option<String>> = connection
            .query_row(
                "SELECT session_id FROM runs WHERE id = ?1",
                params![run_id],
                |row| row.get(0),
            )
            .optional()?;
        match now {
            Some(Some(bound)) if bound == session_id => return Ok(AttachOutcome::AlreadyAttached),
            Some(Some(bound)) => {
                return Err(SessionModelError::RunInOtherSession(
                    run_id.to_owned(),
                    bound,
                ))
            }
            Some(None) if attempt == 0 => continue,
            _ => break,
        }
    }
    Err(SessionModelError::AttachContended(run_id.to_owned()))
}

/// Reads the session run ledger, the last completed run and the pending-work
/// summary.
///
/// # Errors
/// [`SessionModelError`] when the session is missing, a run carries an unknown
/// state, or SQLite fails.
pub fn session_run_model(
    connection: &Connection,
    session_id: &str,
) -> SessionResult<SessionRunModel> {
    let session = load_session(connection, session_id)?;
    let total_runs_i64: i64 = connection.query_row(
        "SELECT COUNT(*) FROM runs WHERE session_id = ?1",
        params![session_id],
        |row| row.get(0),
    )?;
    let run_count = usize_from_i64(total_runs_i64);
    let total_runs = total_runs_i64.max(0);
    let offset = total_runs.saturating_sub(i64_from_usize(MAX_SESSION_RUNS));
    let limit = i64_from_usize(MAX_SESSION_RUNS);

    let mut runs = Vec::new();
    {
        let mut statement = connection.prepare(
            "SELECT id, prompt, status, error_code, created_at
             FROM runs
             WHERE session_id = ?1
             ORDER BY created_at ASC, rowid ASC
             LIMIT ?2 OFFSET ?3",
        )?;
        let mut rows = statement.query(params![session_id, limit, offset])?;
        while let Some(row) = rows.next()? {
            let run_id: String = row.get(0)?;
            let prompt: String = row.get(1)?;
            let raw_status: String = row.get(2)?;
            let error_code: Option<String> = row.get(3)?;
            let created_at: String = row.get(4)?;
            let status = parse_agent_state(&raw_status).ok_or_else(|| {
                SessionModelError::UnknownRunState {
                    run_id: run_id.clone(),
                    status: raw_status,
                }
            })?;
            let ordinal = u64::try_from(offset)
                .unwrap_or(0)
                .saturating_add(u64::try_from(runs.len()).unwrap_or(0))
                .saturating_add(1);
            runs.push(SessionRun {
                run_id,
                ordinal,
                status,
                prompt,
                error_code,
                created_at,
            });
        }
    }
    let run_ids = runs
        .iter()
        .map(|run| run.run_id.clone())
        .collect::<Vec<_>>();
    let last_completed_run = connection
        .query_row(
            "SELECT id, updated_at
             FROM runs
             WHERE session_id = ?1 AND status = 'completed'
             ORDER BY updated_at DESC, rowid DESC
             LIMIT 1",
            params![session_id],
            |row| {
                Ok(CompletedRunRef {
                    run_id: row.get(0)?,
                    completed_at: row.get(1)?,
                })
            },
        )
        .optional()?;
    let pending_work = load_pending_work(connection, session_id)?;

    Ok(SessionRunModel {
        session_id: session.session_id,
        project_id: session.project_id,
        archived: session.archived,
        run_ids,
        runs,
        run_count,
        runs_truncated: run_count > MAX_SESSION_RUNS,
        last_completed_run,
        pending_work,
    })
}

/// Builds a byte-bounded continuation context from persisted chat messages and
/// task results belonging to the session's runs.
///
/// Nothing is synthesised: every item is a stored row's text (or its prefix
/// when truncated). Selection is newest-first until the budget is exhausted;
/// the returned items are ascending by session order.
///
/// # Errors
/// [`SessionModelError::InvalidBudget`] when `budget_bytes` is zero or exceeds
/// [`MAX_CONTINUATION_BUDGET_BYTES`], [`SessionModelError::SessionNotFound`]
/// when the session is missing, or SQLite failure.
pub fn continuation_context(
    connection: &Connection,
    session_id: &str,
    budget_bytes: usize,
) -> SessionResult<ContinuationContext> {
    if budget_bytes == 0 || budget_bytes > MAX_CONTINUATION_BUDGET_BYTES {
        return Err(SessionModelError::InvalidBudget(budget_bytes));
    }
    load_session(connection, session_id)?;

    let total_items: i64 = connection.query_row(
        "SELECT
            (SELECT COUNT(*) FROM chat_messages m
              JOIN runs r ON r.id = m.run_id
             WHERE r.session_id = ?1)
          + (SELECT COUNT(*) FROM task_results tr
              JOIN runs r ON r.id = tr.run_id
             WHERE r.session_id = ?1)",
        params![session_id],
        |row| row.get(0),
    )?;
    let total_items = usize_from_i64(total_items);

    let source_limit = i64_from_usize(MAX_CONTEXT_SOURCE_ROWS);
    let mut collected = Vec::new();
    {
        let mut statement = connection.prepare(
            "SELECT m.id, m.run_id, m.role, m.text, m.created_at,
                    r.created_at AS run_created_at, r.rowid AS run_rowid
             FROM chat_messages m
             JOIN runs r ON r.id = m.run_id
             WHERE r.session_id = ?1
             ORDER BY r.created_at DESC, r.rowid DESC, m.created_at DESC, m.rowid DESC
             LIMIT ?2",
        )?;
        let mut rows = statement.query(params![session_id, source_limit])?;
        while let Some(row) = rows.next()? {
            collected.push(CollectedItem {
                source: ContextSource::ChatMessage,
                record_id: row.get(0)?,
                run_id: row.get(1)?,
                role: Some(row.get(2)?),
                text: row.get(3)?,
                created_at: row.get(4)?,
                run_created_at: row.get(5)?,
                run_rowid: row.get(6)?,
            });
        }
    }
    {
        let mut statement = connection.prepare(
            "SELECT tr.task_id, tr.run_id, tr.text, tr.created_at,
                    r.created_at AS run_created_at, r.rowid AS run_rowid
             FROM task_results tr
             JOIN runs r ON r.id = tr.run_id
             WHERE r.session_id = ?1
             ORDER BY r.created_at DESC, r.rowid DESC, tr.created_at DESC, tr.rowid DESC
             LIMIT ?2",
        )?;
        let mut rows = statement.query(params![session_id, source_limit])?;
        while let Some(row) = rows.next()? {
            collected.push(CollectedItem {
                source: ContextSource::TaskResult,
                record_id: row.get(0)?,
                run_id: row.get(1)?,
                role: None,
                text: row.get(2)?,
                created_at: row.get(3)?,
                run_created_at: row.get(4)?,
                run_rowid: row.get(5)?,
            });
        }
    }
    collected.sort_by(|left, right| right.sort_key().cmp(&left.sort_key()));

    let fetched = collected.len();
    let mut remaining = budget_bytes;
    let mut selected = Vec::new();
    for item in collected {
        if remaining == 0 {
            break;
        }
        let original_bytes = item.text.len();
        let (text, truncated) = if original_bytes <= remaining {
            (item.text, false)
        } else {
            (truncate_to_bytes(&item.text, remaining).to_owned(), true)
        };
        remaining = remaining.saturating_sub(text.len());
        selected.push(ContextItem {
            source: item.source,
            run_id: item.run_id,
            record_id: item.record_id,
            role: item.role,
            created_at: item.created_at,
            text,
            original_bytes,
            truncated,
        });
    }
    selected.reverse();

    let used_bytes = selected.iter().map(|item| item.text.len()).sum::<usize>();
    let original_bytes = selected
        .iter()
        .map(|item| item.original_bytes)
        .sum::<usize>();
    let any_truncated = selected.iter().any(ContextItem::is_truncated);
    let omitted_items =
        total_items.saturating_sub(fetched) + fetched.saturating_sub(selected.len());
    let mut source_run_ids: Vec<String> = Vec::new();
    for item in &selected {
        if !source_run_ids.iter().any(|id| id == &item.run_id) {
            source_run_ids.push(item.run_id.clone());
        }
    }
    Ok(ContinuationContext {
        session_id: session_id.to_owned(),
        budget_bytes,
        used_bytes,
        original_bytes,
        items: selected,
        total_items,
        omitted_items,
        truncated: any_truncated || omitted_items > 0,
        source_run_ids,
    })
}

/// Decides whether a run may be continued, and how.
///
/// Failed and interrupted runs are continuable: the engine has no mid-run
/// resume, so a continuation run is created and seeded from this run's
/// persisted history. Completed/cancelled/killed runs start a new run.
/// Active runs and runs paused on an approval are refused. Archived sessions
/// are refused.
///
/// # Errors
/// [`SessionModelError`] when the session/run is missing, the run belongs to
/// another session, the stored state is unknown, or SQLite fails.
pub fn assess_continuation(
    connection: &Connection,
    session_id: &str,
    run_id: &str,
) -> SessionResult<ContinueAssessment> {
    let session = load_session(connection, session_id)?;
    let row: Option<Option<String>> = connection
        .query_row(
            "SELECT session_id FROM runs WHERE id = ?1",
            params![run_id],
            |row| row.get(0),
        )
        .optional()?;
    let bound_session = row.ok_or_else(|| SessionModelError::RunNotFound(run_id.to_owned()))?;
    match bound_session {
        Some(bound) if bound == session_id => {}
        Some(other) => return Err(SessionModelError::RunNotInSession(run_id.to_owned(), other)),
        None => {
            return Err(SessionModelError::RunNotInSession(
                run_id.to_owned(),
                session_id.to_owned(),
            ))
        }
    }
    let raw_status: String = connection.query_row(
        "SELECT status FROM runs WHERE id = ?1",
        params![run_id],
        |row| row.get(0),
    )?;
    let status =
        parse_agent_state(&raw_status).ok_or_else(|| SessionModelError::UnknownRunState {
            run_id: run_id.to_owned(),
            status: raw_status,
        })?;
    let (decision, reason) = if session.archived {
        (ContinueDecision::Refuse, ContinueReason::SessionArchived)
    } else {
        match status {
            AgentState::Failed => (
                ContinueDecision::ContinueSession,
                ContinueReason::FailedRunContinuable,
            ),
            AgentState::Interrupted => (
                ContinueDecision::ContinueSession,
                ContinueReason::InterruptedRunContinuable,
            ),
            AgentState::AwaitingApproval => {
                (ContinueDecision::Refuse, ContinueReason::ApprovalPending)
            }
            AgentState::Queued
            | AgentState::Planning
            | AgentState::Thinking
            | AgentState::Running
            | AgentState::Streaming => (ContinueDecision::Refuse, ContinueReason::RunStillActive),
            AgentState::Completed => (ContinueDecision::StartNewRun, ContinueReason::RunCompleted),
            AgentState::Cancelled => (ContinueDecision::StartNewRun, ContinueReason::RunCancelled),
            AgentState::Killed => (ContinueDecision::StartNewRun, ContinueReason::RunKilled),
            AgentState::Healthy
            | AgentState::Empty
            | AgentState::Loading
            | AgentState::Reconnecting
            | AgentState::Replay => (
                ContinueDecision::Refuse,
                ContinueReason::RunStateNotRunnable,
            ),
        }
    };
    Ok(ContinueAssessment {
        session_id: session_id.to_owned(),
        run_id: run_id.to_owned(),
        status,
        decision,
        reason,
        detail: reason.message().to_owned(),
    })
}

/// Returns the newest failed/interrupted run of a session, if any.
///
/// This only selects a candidate; call [`assess_continuation`] for the
/// authoritative decision (archival, identity and state checks).
///
/// # Errors
/// [`SessionModelError`] when the session is missing or SQLite fails.
pub fn latest_resumable_run(
    connection: &Connection,
    session_id: &str,
) -> SessionResult<Option<String>> {
    load_session(connection, session_id)?;
    Ok(connection
        .query_row(
            "SELECT id FROM runs
             WHERE session_id = ?1 AND status IN ('failed','interrupted')
             ORDER BY updated_at DESC, rowid DESC
             LIMIT 1",
            params![session_id],
            |row| row.get(0),
        )
        .optional()?)
}

struct LoadedSession {
    session_id: String,
    project_id: String,
    archived: bool,
}

fn load_session(connection: &Connection, session_id: &str) -> SessionResult<LoadedSession> {
    connection
        .query_row(
            "SELECT project_id, archived_at FROM sessions WHERE id = ?1",
            params![session_id],
            |row| {
                let project_id: String = row.get(0)?;
                let archived_at: Option<String> = row.get(1)?;
                Ok(LoadedSession {
                    session_id: session_id.to_owned(),
                    project_id,
                    archived: archived_at.is_some(),
                })
            },
        )
        .optional()?
        .ok_or_else(|| SessionModelError::SessionNotFound(session_id.to_owned()))
}

fn load_pending_work(connection: &Connection, session_id: &str) -> SessionResult<PendingWork> {
    let active_run_count = usize_from_i64(connection.query_row(
        "SELECT COUNT(*) FROM runs
         WHERE session_id = ?1
           AND status IN ('queued','planning','thinking','running','streaming','awaiting_approval')",
        params![session_id],
        |row| row.get(0),
    )?);
    let mut active_runs = Vec::new();
    {
        let mut statement = connection.prepare(
            "SELECT id, status, created_at
             FROM runs
             WHERE session_id = ?1
               AND status IN ('queued','planning','thinking','running','streaming','awaiting_approval')
             ORDER BY created_at ASC, rowid ASC
             LIMIT ?2",
        )?;
        let mut rows =
            statement.query(params![session_id, i64_from_usize(MAX_PENDING_ACTIVE_RUNS)])?;
        while let Some(row) = rows.next()? {
            let run_id: String = row.get(0)?;
            let raw_status: String = row.get(1)?;
            let created_at: String = row.get(2)?;
            let status = parse_agent_state(&raw_status).ok_or_else(|| {
                SessionModelError::UnknownRunState {
                    run_id: run_id.clone(),
                    status: raw_status,
                }
            })?;
            active_runs.push(ActiveRunRef {
                run_id,
                status,
                created_at,
            });
        }
    }
    let open_todo_count = usize_from_i64(connection.query_row(
        "SELECT COUNT(*) FROM todos WHERE session_id = ?1 AND state = 'open'",
        params![session_id],
        |row| row.get(0),
    )?);
    let mut open_todos = Vec::new();
    {
        let mut statement = connection.prepare(
            "SELECT id, seq, text, created_at
             FROM todos
             WHERE session_id = ?1 AND state = 'open'
             ORDER BY seq ASC, rowid ASC
             LIMIT ?2",
        )?;
        let mut rows = statement.query(params![session_id, i64_from_usize(MAX_PENDING_TODOS)])?;
        while let Some(row) = rows.next()? {
            let todo_id: String = row.get(0)?;
            let seq: i64 = row.get(1)?;
            let stored_text: String = row.get(2)?;
            let created_at: String = row.get(3)?;
            let original_bytes = stored_text.len();
            let text = truncate_to_bytes(&stored_text, MAX_PENDING_TODO_TEXT_BYTES).to_owned();
            open_todos.push(PendingTodo {
                todo_id,
                seq,
                truncated: text.len() < original_bytes,
                text,
                created_at,
            });
        }
    }
    let unfinished_task_count = usize_from_i64(connection.query_row(
        "SELECT COUNT(*) FROM tasks t
         JOIN runs r ON r.id = t.run_id
         WHERE r.session_id = ?1
           AND t.state NOT IN ('completed','failed','cancelled','killed','interrupted')",
        params![session_id],
        |row| row.get(0),
    )?);
    Ok(PendingWork {
        active_runs_truncated: active_run_count > active_runs.len(),
        active_runs,
        active_run_count,
        open_todos_truncated: open_todo_count > open_todos.len(),
        open_todos,
        open_todo_count,
        unfinished_task_count,
    })
}

struct CollectedItem {
    source: ContextSource,
    run_id: String,
    record_id: String,
    role: Option<String>,
    text: String,
    created_at: String,
    run_created_at: String,
    run_rowid: i64,
}

impl CollectedItem {
    fn sort_key(&self) -> (&str, i64, &str, u8, &str) {
        (
            &self.run_created_at,
            self.run_rowid,
            &self.created_at,
            match self.source {
                ContextSource::ChatMessage => 0,
                ContextSource::TaskResult => 1,
            },
            &self.record_id,
        )
    }
}

fn parse_agent_state(value: &str) -> Option<AgentState> {
    Some(match value {
        "healthy" => AgentState::Healthy,
        "running" => AgentState::Running,
        "queued" => AgentState::Queued,
        "failed" => AgentState::Failed,
        "completed" => AgentState::Completed,
        "cancelled" => AgentState::Cancelled,
        "killed" => AgentState::Killed,
        "planning" => AgentState::Planning,
        "thinking" => AgentState::Thinking,
        "awaiting_approval" => AgentState::AwaitingApproval,
        "streaming" => AgentState::Streaming,
        "reconnecting" => AgentState::Reconnecting,
        "replay" => AgentState::Replay,
        "empty" => AgentState::Empty,
        "loading" => AgentState::Loading,
        "interrupted" => AgentState::Interrupted,
        _ => return None,
    })
}

fn truncate_to_bytes(text: &str, limit: usize) -> &str {
    if text.len() <= limit {
        return text;
    }
    let mut end = limit;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

fn usize_from_i64(value: i64) -> usize {
    usize::try_from(value).unwrap_or(usize::MAX)
}

fn i64_from_usize(value: usize) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}
