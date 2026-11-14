//! SQLite store for runs, tasks, chat messages and command receipts
//! (blueprint Phase 4, WP 4.6; schema created by migration v2).

use std::sync::{Arc, OnceLock};

use hiverouter_contract::{AgentState, Refusal, RunId, TaskId, Timestamp, Usage};
use rusqlite::{params, OptionalExtension};
use uuid::Uuid;

use crate::db::{Database, DbError};
use crate::entities::{ChatMessage, ChatRole, PermissionPolicy, Run, Session, TaskRecord, Todo};
use crate::run_state::{is_terminal_task_state, TASK_TERMINAL_STATES};

fn map(error: DbError) -> Refusal {
    error.refusal()
}

/// Result of verifying one run's journal hash chain.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JournalVerification {
    /// True only when every chained row links and rehashes correctly.
    pub valid: bool,
    /// Chained rows checked.
    pub checked: usize,
    /// Legacy rows with no hash.
    pub unchained: usize,
}

fn journal_event_hash(
    previous: &str,
    run_id: &str,
    bank_id: &str,
    entry_id: &str,
    action: &str,
    actor_kind: &str,
    actor_id: &str,
    detail_json: &str,
    created_at: &str,
) -> String {
    let canonical = format!(
        "{previous}|{run_id}|{bank_id}|{entry_id}|{action}|{actor_kind}|{actor_id}|{detail_json}|{created_at}"
    );
    hiverouter_tools::sha256::sha256_hex(canonical.as_bytes())
}

/// Appends one hash-chained journal row for a run.
fn append_memory_journal(
    connection: &rusqlite::Connection,
    run_id: &str,
    bank_id: &str,
    entry_id: &str,
    action: &str,
    actor_kind: &str,
    actor_id: Option<&str>,
    detail_json: &str,
    created_at: &str,
) -> rusqlite::Result<()> {
    let previous: Option<String> = connection
        .query_row(
            "SELECT event_hash FROM memory_journal
             WHERE run_id = ?1 AND event_hash IS NOT NULL
             ORDER BY seq DESC LIMIT 1",
            params![run_id],
            |row| row.get(0),
        )
        .optional()?;
    let prev = previous.as_deref().unwrap_or("");
    let hash = journal_event_hash(
        prev,
        run_id,
        bank_id,
        entry_id,
        action,
        actor_kind,
        actor_id.unwrap_or(""),
        detail_json,
        created_at,
    );
    connection.execute(
        "INSERT INTO memory_journal
         (run_id, bank_id, entry_id, action, actor_kind, actor_id,
          detail_json, created_at, prev_hash, event_hash)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
        params![
            run_id,
            bank_id,
            entry_id,
            action,
            actor_kind,
            actor_id,
            detail_json,
            created_at,
            previous,
            hash
        ],
    )?;
    connection.execute(
        "UPDATE runs SET memory_journal_head = ?1 WHERE id = ?2",
        params![hash, run_id],
    )?;
    Ok(())
}

/// One durable memory row, including its hive/branch scope.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentMemoryRecord {
    /// Owning run.
    pub run_id: String,
    /// Hive/branch scope (parent task id or `root`).
    pub scope_id: String,
    /// Agent/role key.
    pub agent_key: String,
    /// Memory key.
    pub key: String,
    /// Memory value.
    pub value: String,
    /// Task that wrote the row.
    pub task_id: String,
    /// RFC3339 creation time.
    pub created_at: String,
}

/// Receipt for one run-scoped memory clear.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemoryClearReceipt {
    /// Legacy flat rows removed.
    pub flat_rows: u64,
    /// Bank entries removed.
    pub bank_entries: u64,
    /// Banks closed.
    pub banks_closed: u64,
}

/// One open A2A question listed for the operator.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingAgentMessage {
    /// Owning run.
    pub run_id: String,
    /// Message id.
    pub id: String,
    /// Sender task.
    pub from_task_id: String,
    /// Recipient task.
    pub to_task_id: String,
    /// ask_parent/send_to_sibling.
    pub kind: String,
    /// Bounded text.
    pub text: String,
    /// Deadline, when set.
    pub deadline_at: Option<String>,
    /// Recorded answer attempts.
    pub attempts: i64,
    /// Last attempt time.
    pub last_attempt_at: Option<String>,
    /// Creation time.
    pub created_at: String,
}

/// One entry returned to an agent's bank read-set.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemoryReadEntry {
    /// Entry id.
    pub entry_id: String,
    /// Bank source.
    pub bank_id: String,
    /// Stable key.
    pub key: String,
    /// Stored value.
    pub value: String,
}

/// One persisted memory entry row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemoryEntryRecord {
    /// Entry id.
    pub id: String,
    /// Bank owner.
    pub bank_id: String,
    /// Stable key.
    pub key: String,
    /// observation/summary/fact/decision/constraint/preference/artifact_ref/note.
    pub kind: String,
    /// Stored text.
    pub value: String,
    /// Provenance class.
    pub source_kind: String,
    /// Trust label, absent for migration notes.
    pub trust: Option<String>,
    /// active/superseded/retracted.
    pub status: String,
    /// Creation time.
    pub created_at: String,
}

/// One persisted memory bank row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemoryBankRecord {
    /// Bank id.
    pub id: String,
    /// global/project/run/hive/branch/agent_role.
    pub kind: String,
    /// Opaque scope id.
    pub scope_id: String,
    /// Owning run, when any.
    pub run_id: Option<String>,
    /// Owning task, when any.
    pub owner_task_id: Option<String>,
    /// Parent bank provenance.
    pub parent_bank_id: Option<String>,
    /// Display label.
    pub display_name: Option<String>,
    /// Monotonic revision.
    pub rev: i64,
    /// Creation time.
    pub created_at: String,
    /// Close time, when cleared.
    pub closed_at: Option<String>,
}

/// Persisted ownership projection for one task.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskOwnership {
    /// run/branch/none/...
    pub mode: String,
    /// Opaque ownership scope.
    pub scope: Option<String>,
    /// Owning task.
    pub owner_task_id: Option<String>,
}

/// Persisted task identity derived at admission by the v13 task trigger.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskIdentity {
    /// `hyper`, `super`, `agent`, or `subagent`.
    pub class: String,
    /// Rank 1..4.
    pub rank: u8,
    /// Persona/profile id.
    pub profile_id: String,
}

/// The `memory_entries.kind` vocabulary accepted by the v14 schema check.
pub const MEMORY_ENTRY_KINDS: [&str; 8] = [
    "observation",
    "summary",
    "fact",
    "decision",
    "constraint",
    "preference",
    "artifact_ref",
    "note",
];

/// Trust labels the task read filter (`memory_read_set` / `hyper_summary_rows`)
/// accepts. `quarantined` is deliberately absent: such a row is hidden from
/// model reads, so a model-facing write must never create one.
pub const READABLE_MEMORY_TRUST: [&str; 3] = ["unverified", "verified", "authoritative"];

/// Bank a writer in `class` owns. `None` means the class is unknown and the
/// caller must refuse rather than guess a bank.
fn caller_bank_for_class(class: &str, run_id: RunId, task_id: TaskId) -> Option<String> {
    match class {
        "hyper" => Some(format!("bank:run:{run_id}")),
        "super" => Some(format!("bank:hive:{task_id}")),
        "agent" | "subagent" => Some(format!("bank:branch:{task_id}")),
        _ => None,
    }
}

/// One durable agent-to-agent message row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentMessageRecord {
    /// Message id.
    pub id: String,
    /// Sender task id.
    pub from_task_id: String,
    /// Recipient task id.
    pub to_task_id: String,
    /// Message kind (`ask_parent` / `send_to_sibling`).
    pub kind: String,
    /// Bounded text.
    pub text: String,
    /// Message lifecycle status (`open`, `answered`, `expired`).
    pub status: String,
    /// Optional deadline (RFC3339).
    pub deadline_at: Option<String>,
    /// Optional answer time (RFC3339).
    pub answered_at: Option<String>,
    /// Optional operator/parent answer text.
    pub answer_text: Option<String>,
    /// Actor that answered (`operator`, task id, etc.).
    pub answer_actor: Option<String>,
    /// Optional message this row replies to.
    pub in_reply_to: Option<String>,
    /// Number of recorded answer attempts.
    pub attempts: i64,
    /// Last attempt time (RFC3339), if any.
    pub last_attempt_at: Option<String>,
    /// RFC3339 creation time.
    pub created_at: String,
}

/// SQL list of terminal task states, derived from the single state-machine set.
///
/// The guard must never drift from [`TASK_TERMINAL_STATES`]: a state added to the
/// lifecycle machine is therefore also un-resurrectable in SQLite. Values are
/// this crate's own `AgentState::as_str()` constants (never caller input).
fn terminal_task_state_sql() -> &'static str {
    static LIST: OnceLock<String> = OnceLock::new();
    LIST.get_or_init(|| {
        TASK_TERMINAL_STATES
            .iter()
            .map(|state| format!("'{}'", state.as_str()))
            .collect::<Vec<_>>()
            .join(", ")
    })
}

/// Outcome of one guarded task-state write (inside the connection lock).
enum TaskWrite {
    /// The row was non-terminal and is now `state` at this revision.
    Applied(u64),
    /// No task row has this id.
    Missing,
    /// The row is terminal; `current` is the state that was already persisted.
    Terminal {
        /// Terminal state already stored.
        current: AgentState,
        /// Revision already stored (unchanged by the refused write).
        rev: u64,
    },
}


/// Typed access to the run/task/message tables.
#[derive(Debug, Clone)]
pub struct RunStore {
    db: Arc<Database>,
}

impl RunStore {
    /// Creates a store over the shared database.
    #[must_use]
    pub fn new(db: Arc<Database>) -> Self {
        Self { db }
    }

    /// Shared database handle for same-crate read models that need a SQLite
    /// connection (session continuation context, diagnostics).
    #[must_use]
    pub(crate) fn database(&self) -> &Arc<Database> {
        &self.db
    }

    /// Inserts a new run.
    ///
    /// # Errors
    /// `E_INTERNAL_ERROR` / `E_DB_BUSY` on database errors.
    pub fn create_run(
        &self,
        project_id: &str,
        prompt: &str,
        status: AgentState,
    ) -> Result<Run, Refusal> {
        self.create_run_scoped(project_id, prompt, status, None, PermissionPolicy::Classic)
    }

    /// Creates a run bound to an optional session and explicit policy.
    pub fn create_run_scoped(
        &self,
        project_id: &str,
        prompt: &str,
        status: AgentState,
        session_id: Option<&str>,
        permission_policy: PermissionPolicy,
    ) -> Result<Run, Refusal> {
        if let Some(session_id) = session_id {
            let session = self.session(session_id)?;
            if session.project_id != project_id {
                return Err(Refusal::SessionNotFound);
            }
        }
        let now = Timestamp::now();
        let run = Run {
            id: RunId::new(),
            project_id: project_id.to_owned(),
            prompt: prompt.to_owned(),
            status,
            error_code: None,
            created_at: now.clone(),
            updated_at: now,
            session_id: session_id.map(ToOwned::to_owned),
            permission_policy,
        };
        self.db
            .with_connection(|connection| {
                connection.execute(
                    "INSERT OR IGNORE INTO projects(id, name, created_at) VALUES (?1, ?1, ?2)",
                    params![run.project_id, run.created_at.as_str()],
                )?;
                connection.execute(
                    "INSERT INTO runs(id, project_id, prompt, status, error_code, created_at, updated_at, session_id, permission_policy)
                     VALUES (?1, ?2, ?3, ?4, NULL, ?5, ?6, ?7, ?8)",
                    params![
                        run.id.to_string(),
                        run.project_id,
                        run.prompt,
                        run.status.as_str(),
                        run.created_at.as_str(),
                        run.updated_at.as_str(),
                        run.session_id,
                        run.permission_policy.as_str()
                    ],
                )?;
                Ok(())
            })
            .map_err(map)?;
        Ok(run)
    }

    /// Creates a named session. The default policy is `classic` (current behavior).
    pub fn create_session(
        &self,
        project_id: &str,
        name: &str,
        workspace_root: &str,
        permission_policy: PermissionPolicy,
    ) -> Result<Session, Refusal> {
        let now = Timestamp::now();
        let session = Session {
            id: Uuid::now_v7().to_string(),
            project_id: project_id.to_owned(),
            name: name.to_owned(),
            workspace_root: workspace_root.to_owned(),
            permission_policy,
            created_at: now.clone(),
            updated_at: now,
            archived_at: None,
        };
        self.db
            .with_connection(|connection| {
                connection.execute(
                    "INSERT INTO sessions(id, project_id, name, workspace_root, permission_policy, created_at, updated_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                    params![
                        session.id,
                        session.project_id,
                        session.name,
                        session.workspace_root,
                        session.permission_policy.as_str(),
                        session.created_at.as_str(),
                        session.updated_at.as_str()
                    ],
                )?;
                connection.execute(
                    "INSERT INTO session_events(id, session_id, kind, detail_json, created_at)
                     VALUES (?1, ?2, 'session_created', ?3, ?4)",
                    params![
                        Uuid::now_v7().to_string(),
                        session.id,
                        serde_json::json!({
                            "project_id": session.project_id,
                            "policy": session.permission_policy.as_str()
                        })
                        .to_string(),
                        Timestamp::now().as_str()
                    ],
                )?;
                Ok(())
            })
            .map_err(map)?;
        Ok(session)
    }

    /// Lists sessions newest first.
    pub fn sessions(&self) -> Result<Vec<Session>, Refusal> {
        self.db
            .with_connection(|connection| {
                let mut statement = connection.prepare(
                    "SELECT id, project_id, name, workspace_root, permission_policy, created_at, updated_at, archived_at
                     FROM sessions WHERE archived_at IS NULL ORDER BY created_at DESC, id DESC",
                )?;
                let rows = statement.query_map([], |row| {
                    Ok(Session {
                        id: row.get(0)?,
                        project_id: row.get(1)?,
                        name: row.get(2)?,
                        workspace_root: row.get(3)?,
                        permission_policy: policy_at(row, 4)?,
                        created_at: timestamp_at(row, 5, "created_at")?,
                        updated_at: timestamp_at(row, 6, "updated_at")?,
                        archived_at: optional_timestamp_at(row, 7, "archived_at")?,
                    })
                })?;
                let mut sessions = Vec::new();
                for row in rows {
                    sessions.push(row?);
                }
                Ok(sessions)
            })
            .map_err(map)
    }

    /// Reads one session by id.
    pub fn session(&self, id: &str) -> Result<Session, Refusal> {
        let session = self
            .db
            .with_connection(|connection| {
                connection
                    .query_row(
                        "SELECT id, project_id, name, workspace_root, permission_policy, created_at, updated_at, archived_at
                         FROM sessions WHERE id = ?1",
                        params![id],
                        |row| {
                            Ok(Session {
                                id: row.get(0)?,
                                project_id: row.get(1)?,
                                name: row.get(2)?,
                                workspace_root: row.get(3)?,
                                permission_policy: policy_at(row, 4)?,
                                created_at: timestamp_at(row, 5, "created_at")?,
                                updated_at: timestamp_at(row, 6, "updated_at")?,
                                archived_at: optional_timestamp_at(row, 7, "archived_at")?,
                            })
                        },
                    )
                    .optional()
            })
            .map_err(map)?;
        session.ok_or(Refusal::SessionNotFound)
    }

    /// Lists sessions, optionally including archived rows. Newest first.
    pub fn sessions_with_archived(
        &self,
        include_archived: bool,
    ) -> Result<Vec<Session>, Refusal> {
        self.db
            .with_connection(|connection| {
                let mut statement = connection.prepare(
                    "SELECT id, project_id, name, workspace_root, permission_policy, created_at, updated_at, archived_at
                     FROM sessions WHERE (?1 = 1 OR archived_at IS NULL)
                     ORDER BY created_at DESC, id DESC",
                )?;
                let rows = statement.query_map(params![i64::from(include_archived)], |row| {
                    Ok(Session {
                        id: row.get(0)?,
                        project_id: row.get(1)?,
                        name: row.get(2)?,
                        workspace_root: row.get(3)?,
                        permission_policy: policy_at(row, 4)?,
                        created_at: timestamp_at(row, 5, "created_at")?,
                        updated_at: timestamp_at(row, 6, "updated_at")?,
                        archived_at: optional_timestamp_at(row, 7, "archived_at")?,
                    })
                })?;
                let mut sessions = Vec::new();
                for row in rows {
                    sessions.push(row?);
                }
                Ok(sessions)
            })
            .map_err(map)
    }

    /// Archives or unarchives one session and journals the change atomically.
    pub fn set_session_archived(
        &self,
        session_id: &str,
        archived: bool,
    ) -> Result<Session, Refusal> {
        // Resolve the row first so a missing id is the named SessionNotFound
        // refusal (404) rather than a generic InternalError from rowcount 0.
        self.session(session_id)?;
        let now = Timestamp::now();
        self.db
            .with_connection(|connection| {
                let transaction = connection.unchecked_transaction()?;
                let changed = transaction.execute(
                    "UPDATE sessions SET archived_at = ?2, updated_at = ?3 WHERE id = ?1",
                    params![
                        session_id,
                        if archived { Some(now.as_str()) } else { None },
                        now.as_str()
                    ],
                )?;
                if changed == 0 {
                    return Err(rusqlite::Error::QueryReturnedNoRows);
                }
                transaction.execute(
                    "INSERT INTO session_events(id, session_id, kind, detail_json, created_at)
                     VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![
                        Uuid::now_v7().to_string(),
                        session_id,
                        if archived { "session_archived" } else { "session_unarchived" },
                        serde_json::json!({ "archived": archived }).to_string(),
                        now.as_str()
                    ],
                )?;
                transaction.commit()?;
                Ok(())
            })
            .map_err(map)?;
        self.session(session_id)
    }

    /// Appends a session event (best-effort audit only; never changes run state).
    pub fn append_session_event(
        &self,
        session_id: &str,
        kind: &str,
        detail_json: &serde_json::Value,
    ) -> Result<(), Refusal> {
        self.session(session_id)?;
        self.db
            .with_connection(|connection| {
                connection.execute(
                    "INSERT INTO session_events(id, session_id, kind, detail_json, created_at)
                     VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![
                        Uuid::now_v7().to_string(),
                        session_id,
                        kind,
                        detail_json.to_string(),
                        Timestamp::now().as_str()
                    ],
                )?;
                Ok(())
            })
            .map_err(map)
    }

    /// Sets a run's permission policy. Only an explicit operator/API choice should
    /// call this; the default remains `classic`.
    pub fn set_run_policy(&self, run_id: RunId, policy: PermissionPolicy) -> Result<(), Refusal> {
        let changed = self
            .db
            .with_connection(|connection| {
                connection.execute(
                    "UPDATE runs SET permission_policy = ?1, updated_at = ?2 WHERE id = ?3",
                    params![policy.as_str(), Timestamp::now().as_str(), run_id.to_string()],
                )
            })
            .map_err(map)?;
        if changed == 0 {
            return Err(Refusal::RunNotFound);
        }
        Ok(())
    }

    /// Sets the default policy for new runs created in a session.
    pub fn set_session_policy(
        &self,
        session_id: &str,
        policy: PermissionPolicy,
    ) -> Result<(), Refusal> {
        if self.session(session_id)?.archived_at.is_some() {
            return Err(Refusal::DecisionRequired);
        }
        let changed = self
            .db
            .with_connection(|connection| {
                let now = Timestamp::now();
                let changed = connection.execute(
                    "UPDATE sessions SET permission_policy = ?1, updated_at = ?2 WHERE id = ?3",
                    params![policy.as_str(), now.as_str(), session_id],
                )?;
                if changed == 1 {
                    connection.execute(
                        "INSERT INTO session_events(id, session_id, kind, detail_json, created_at)
                         VALUES (?1, ?2, 'policy_changed', ?3, ?4)",
                        params![
                            Uuid::now_v7().to_string(),
                            session_id,
                            serde_json::json!({ "policy": policy.as_str() }).to_string(),
                            now.as_str()
                        ],
                    )?;
                }
                Ok(changed)
            })
            .map_err(map)?;
        if changed == 0 {
            return Err(Refusal::SessionNotFound);
        }
        Ok(())
    }

    /// Adds a todo to a session.
    pub fn add_todo(&self, session_id: &str, text: &str) -> Result<Todo, Refusal> {
        if self.session(session_id)?.archived_at.is_some() {
            return Err(Refusal::DecisionRequired);
        }
        let todo = Todo {
            id: Uuid::now_v7().to_string(),
            session_id: session_id.to_owned(),
            seq: 0,
            text: text.to_owned(),
            state: "open".to_owned(),
            created_at: Timestamp::now(),
        };
        self.db
            .with_connection(|connection| {
                let transaction = connection.unchecked_transaction()?;
                transaction.execute(
                    "INSERT INTO todos(id, session_id, seq, text, state, created_at)
                     VALUES (?1, ?2, (SELECT COALESCE(MAX(seq), 0) + 1 FROM todos WHERE session_id = ?2), ?3, 'open', ?4)",
                    params![todo.id, todo.session_id, todo.text, todo.created_at.as_str()],
                )?;
                transaction.execute(
                    "INSERT INTO session_events(id, session_id, kind, detail_json, created_at)
                     VALUES (?1, ?2, 'todo_added', ?3, ?4)",
                    params![
                        Uuid::now_v7().to_string(),
                        todo.session_id,
                        serde_json::json!({ "todo_id": todo.id, "text": todo.text }).to_string(),
                        todo.created_at.as_str()
                    ],
                )?;
                transaction.commit()?;
                Ok(())
            })
            .map_err(map)?;
        let mut todos = self.todos(&todo.session_id)?;
        Ok(todos.pop().unwrap_or(todo))
    }

    /// Moves one todo to open/done/cancelled and journals the change.
    pub fn set_todo_state(
        &self,
        session_id: &str,
        todo_id: &str,
        state: &str,
    ) -> Result<Todo, Refusal> {
        if !matches!(state, "open" | "done" | "cancelled") {
            return Err(Refusal::SchemaInvalid);
        }
        if self.session(session_id)?.archived_at.is_some() {
            return Err(Refusal::DecisionRequired);
        }
        let now = Timestamp::now();
        let changed = self
            .db
            .with_connection(|connection| {
                let transaction = connection.unchecked_transaction()?;
                let changed = transaction.execute(
                    "UPDATE todos SET state = ?3 WHERE id = ?1 AND session_id = ?2",
                    params![todo_id, session_id, state],
                )?;
                if changed == 0 {
                    return Err(rusqlite::Error::QueryReturnedNoRows);
                }
                transaction.execute(
                    "INSERT INTO session_events(id, session_id, kind, detail_json, created_at)
                     VALUES (?1, ?2, 'todo_state_changed', ?3, ?4)",
                    params![
                        Uuid::now_v7().to_string(),
                        session_id,
                        serde_json::json!({ "todo_id": todo_id, "state": state }).to_string(),
                        now.as_str()
                    ],
                )?;
                transaction.commit()?;
                Ok(())
            })
            .map_err(map);
        if changed.is_err() {
            return Err(Refusal::DecisionRequired);
        }
        self.todo(session_id, todo_id)?
            .ok_or(Refusal::DecisionRequired)
    }

    /// Reads one todo, or `None` when it does not belong to the session.
    pub fn todo(&self, session_id: &str, todo_id: &str) -> Result<Option<Todo>, Refusal> {
        self.db
            .with_connection(|connection| {
                connection
                    .query_row(
                        "SELECT id, session_id, seq, text, state, created_at FROM todos
                         WHERE id = ?1 AND session_id = ?2",
                        params![todo_id, session_id],
                        |row| {
                            Ok(Todo {
                                id: row.get(0)?,
                                session_id: row.get(1)?,
                                seq: row.get::<_, i64>(2)? as u64,
                                text: row.get(3)?,
                                state: row.get(4)?,
                                created_at: timestamp_at(row, 5, "created_at")?,
                            })
                        },
                    )
                    .optional()
            })
            .map_err(map)
    }

    /// Lists session todos in order.
    pub fn todos(&self, session_id: &str) -> Result<Vec<Todo>, Refusal> {
        self.db
            .with_connection(|connection| {
                let mut statement = connection.prepare(
                    "SELECT id, session_id, seq, text, state, created_at FROM todos
                     WHERE session_id = ?1 ORDER BY seq ASC, id ASC",
                )?;
                let rows = statement.query_map(params![session_id], |row| {
                    Ok(Todo {
                        id: row.get(0)?,
                        session_id: row.get(1)?,
                        seq: row.get::<_, i64>(2)? as u64,
                        text: row.get(3)?,
                        state: row.get(4)?,
                        created_at: timestamp_at(row, 5, "created_at")?,
                    })
                })?;
                let mut todos = Vec::new();
                for row in rows {
                    todos.push(row?);
                }
                Ok(todos)
            })
            .map_err(map)
    }

    /// Lists durable projects in creation order.
    ///
    /// # Errors
    /// Database refusal.
    pub fn projects(&self) -> Result<Vec<(String, String, String)>, Refusal> {
        self.db
            .with_connection(|connection| {
                let mut statement =
                    connection.prepare("SELECT id, name, created_at FROM projects ORDER BY created_at ASC, id ASC")?;
                let rows = statement.query_map([], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                })?;
                let mut projects = Vec::new();
                for row in rows {
                    projects.push(row?);
                }
                Ok(projects)
            })
            .map_err(map)
    }

    /// Reads a run by id.
    ///
    /// # Errors
    /// `E_RUN_NOT_FOUND` when the row does not exist.
    pub fn get_run(&self, run_id: RunId) -> Result<Run, Refusal> {
        let maybe = self
            .db
            .with_connection(|connection| {
                connection
                    .query_row(
                        "SELECT id, project_id, prompt, status, error_code, created_at, updated_at, session_id, permission_policy
                         FROM runs WHERE id = ?1",
                        params![run_id.to_string()],
                        row_to_run,
                    )
                    .optional()
            })
            .map_err(map)?;
        maybe.ok_or(Refusal::RunNotFound)
    }

    /// Whether a non-terminal run exists.
    ///
    /// # Errors
    /// Database refusal.
    pub fn latest_active_run(&self) -> Result<Option<Run>, Refusal> {
        self.db
            .with_connection(|connection| {
                connection
                    .query_row(
                        "SELECT id, project_id, prompt, status, error_code, created_at, updated_at, session_id, permission_policy
                         FROM runs WHERE status IN ('queued','planning','thinking','running','streaming','awaiting_approval')
                         ORDER BY created_at DESC LIMIT 1",
                        [],
                        row_to_run,
                    )
                    .optional()
            })
            .map_err(map)
    }

    /// Lists runs bound to one session, newest first.
    pub fn runs_for_session(&self, session_id: &str) -> Result<Vec<Run>, Refusal> {
        self.db
            .with_connection(|connection| {
                let mut statement = connection.prepare(
                    "SELECT id, project_id, prompt, status, error_code, created_at, updated_at, session_id, permission_policy
                     FROM runs WHERE session_id = ?1 ORDER BY created_at DESC, id DESC",
                )?;
                let rows = statement.query_map(params![session_id], row_to_run)?;
                let mut runs = Vec::new();
                for row in rows {
                    runs.push(row?);
                }
                Ok(runs)
            })
            .map_err(map)
    }

    /// Lists the newest runs.
    ///
    /// # Errors
    /// Database refusal.
    pub fn list_runs(&self, limit: u32) -> Result<Vec<Run>, Refusal> {
        self.db
            .with_connection(|connection| {
                let mut statement = connection.prepare(
                    "SELECT id, project_id, prompt, status, error_code, created_at, updated_at, session_id, permission_policy
                     FROM runs ORDER BY created_at DESC LIMIT ?1",
                )?;
                let rows = statement.query_map(params![i64::from(limit)], row_to_run)?;
                let mut runs = Vec::new();
                for row in rows {
                    runs.push(row?);
                }
                Ok(runs)
            })
            .map_err(map)
    }

    /// Updates a run's status and error code.
    ///
    /// # Errors
    /// `E_RUN_NOT_FOUND` when the run does not exist.
    pub fn set_run_status(
        &self,
        run_id: RunId,
        status: AgentState,
        error_code: Option<&str>,
    ) -> Result<(), Refusal> {
        let updated = self
            .db
            .with_connection(|connection| {
                connection.execute(
                    "UPDATE runs SET status = ?2, error_code = ?3, updated_at = ?4
                     WHERE id = ?1
                       AND status NOT IN ('completed','failed','cancelled','killed','interrupted')",
                    params![
                        run_id.to_string(),
                        status.as_str(),
                        error_code,
                        Timestamp::now().as_str()
                    ],
                )
            })
            .map_err(map)?;
        if updated == 0 {
            let current = self.get_run(run_id)?;
            if current.status.is_terminal() {
                return if current.status == status {
                    Ok(())
                } else {
                    Err(Refusal::RunTerminal)
                };
            }
            return Err(Refusal::RunNotFound);
        }
        Ok(())
    }

    /// Inserts a task row.
    ///
    /// # Errors
    /// Database refusal.
    pub fn insert_task(&self, task: &TaskRecord) -> Result<(), Refusal> {
        self.db
            .with_connection(|connection| {
                connection.execute(
                    "INSERT INTO tasks(id, run_id, parent_task_id, role, depth, spawn_seq, state, rev)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                    params![
                        task.id.to_string(),
                        task.run_id.to_string(),
                        task.parent_task_id.map(|parent| parent.to_string()),
                        task.role,
                        i64::from(task.depth),
                        i64::try_from(task.spawn_seq).unwrap_or(i64::MAX),
                        task.state.as_str(),
                        i64::try_from(task.rev).unwrap_or(i64::MAX),
                    ],
                )?;
                ensure_bank_for_inserted_task(connection, task)?;
                Ok(())
            })
            .map_err(map)
    }

    /// Stores the bounded objective/instruction for one task.
    ///
    /// The instruction is task-local context, never a grant or policy override.
    ///
    /// # Errors
    /// Database refusal; `E_TASK_NOT_FOUND` when the row does not exist.
    pub fn set_task_instruction(&self, task_id: TaskId, instruction: &str) -> Result<(), Refusal> {
        let updated = self
            .db
            .with_connection(|connection| {
                connection.execute(
                    "UPDATE tasks SET instruction = ?2 WHERE id = ?1",
                    params![task_id.to_string(), instruction],
                )
            })
            .map_err(map)?;
        if updated == 0 {
            return Err(Refusal::TaskNotFound);
        }
        Ok(())
    }

    /// Reads the task-local instruction, if one was stored.
    ///
    /// # Errors
    /// Database refusal.
    pub fn task_instruction(&self, task_id: TaskId) -> Result<Option<String>, Refusal> {
        self.db
            .with_connection(|connection| {
                connection
                    .query_row(
                        "SELECT instruction FROM tasks WHERE id = ?1",
                        params![task_id.to_string()],
                        |row| row.get::<_, Option<String>>(0),
                    )
                    .optional()
                    .map(|value| value.flatten())
            })
            .map_err(map)
    }

    /// Reads a task by id.
    ///
    /// # Errors
    /// `E_TASK_NOT_FOUND` when the row does not exist.
    pub fn get_task(&self, task_id: TaskId) -> Result<TaskRecord, Refusal> {
        let maybe = self
            .db
            .with_connection(|connection| {
                connection
                    .query_row(
                        "SELECT id, run_id, parent_task_id, role, depth, spawn_seq, state, rev
                         FROM tasks WHERE id = ?1",
                        params![task_id.to_string()],
                        row_to_task,
                    )
                    .optional()
            })
            .map_err(map)?;
        maybe.ok_or(Refusal::TaskNotFound)
    }

    /// Updates a task's state and bumps its revision.
    ///
    /// Terminal states are sticky: a task that is already `completed`, `failed`,
    /// `cancelled`, `killed` or `interrupted` can never be moved to a *different*
    /// state (a late completion may not resurrect a killed task). Replaying the
    /// same terminal state is idempotent and returns the stored revision without
    /// bumping it.
    ///
    /// # Errors
    /// `E_TASK_NOT_FOUND` when the row does not exist; `E_TASK_TERMINAL` when the
    /// row is terminal and a different state was requested. The read/compare and
    /// the write happen inside one connection lock, so the decision cannot race a
    /// concurrent writer.
    pub fn update_task_state(&self, task_id: TaskId, state: AgentState) -> Result<u64, Refusal> {
        let id = task_id.to_string();
        let sql = format!(
            "UPDATE tasks SET state = ?2, rev = rev + 1 WHERE id = ?1 AND state NOT IN ({})",
            terminal_task_state_sql()
        );
        let outcome = self
            .db
            .with_connection(|connection| {
                let updated = connection.execute(&sql, params![id, state.as_str()])?;
                if updated > 0 {
                    let rev: i64 = connection.query_row(
                        "SELECT rev FROM tasks WHERE id = ?1",
                        params![id],
                        |row| row.get(0),
                    )?;
                    return Ok(TaskWrite::Applied(u64::try_from(rev).unwrap_or(0)));
                }
                let existing = connection
                    .query_row(
                        "SELECT state, rev FROM tasks WHERE id = ?1",
                        params![id],
                        |row| {
                            let current: String = row.get(0)?;
                            let rev: i64 = row.get(1)?;
                            Ok((current, rev))
                        },
                    )
                    .optional()?;
                Ok(match existing {
                    None => TaskWrite::Missing,
                    Some((current, rev)) => TaskWrite::Terminal {
                        current: parse_state(&current),
                        rev: u64::try_from(rev).unwrap_or(0),
                    },
                })
            })
            .map_err(map)?;
        match outcome {
            TaskWrite::Applied(rev) => Ok(rev),
            TaskWrite::Missing => Err(Refusal::TaskNotFound),
            TaskWrite::Terminal { current, rev } => {
                if current == state && is_terminal_task_state(current) {
                    // Idempotent replay: the task is already in this terminal
                    // state. No resurrection and no revision churn.
                    Ok(rev)
                } else {
                    tracing::warn!(
                        task = %task_id,
                        from = current.as_str(),
                        to = state.as_str(),
                        "refused to move a terminal task to a different state"
                    );
                    Err(Refusal::TaskTerminal)
                }
            }
        }
    }

    /// Moves a persisted task row to a new parent and pins its row revision to
    /// the graph node revision produced by the same rewire.
    ///
    /// The graph is the live authority, but roster/fan-out/A2A readers use the
    /// task rows. Without this write those readers keep the pre-rewire parent
    /// forever, including across a restart.
    ///
    /// # Errors
    /// `E_TASK_NOT_FOUND` when the row does not exist; database refusal otherwise.
    pub fn update_task_parent(
        &self,
        task_id: TaskId,
        parent_task_id: Option<TaskId>,
        rev: u64,
    ) -> Result<(), Refusal> {
        let id = task_id.to_string();
        let parent = parent_task_id.map(|parent| parent.to_string());
        let revision = i64::try_from(rev).map_err(|_| Refusal::InternalError)?;
        let updated = self
            .db
            .with_connection(|connection| {
                connection.execute(
                    "UPDATE tasks SET parent_task_id = ?2, rev = ?3 WHERE id = ?1",
                    params![id, parent, revision],
                )
            })
            .map_err(map)?;
        if updated == 0 {
            return Err(Refusal::TaskNotFound);
        }
        Ok(())
    }

    /// Lists a run's tasks in spawn order.
    ///
    /// # Errors
    /// Database refusal.
    pub fn tasks_for_run(&self, run_id: RunId) -> Result<Vec<TaskRecord>, Refusal> {
        self.db
            .with_connection(|connection| {
                let mut statement = connection.prepare(
                    "SELECT id, run_id, parent_task_id, role, depth, spawn_seq, state, rev
                     FROM tasks WHERE run_id = ?1 ORDER BY spawn_seq ASC",
                )?;
                let rows = statement.query_map(params![run_id.to_string()], row_to_task)?;
                let mut tasks = Vec::new();
                for row in rows {
                    tasks.push(row?);
                }
                Ok(tasks)
            })
            .map_err(map)
    }

    /// Number of tasks in a run.
    ///
    /// # Errors
    /// Database refusal.
    pub fn task_count(&self, run_id: RunId) -> Result<u64, Refusal> {
        let count: i64 = self
            .db
            .with_connection(|connection| {
                connection.query_row(
                    "SELECT COUNT(*) FROM tasks WHERE run_id = ?1",
                    params![run_id.to_string()],
                    |row| row.get(0),
                )
            })
            .map_err(map)?;
        Ok(u64::try_from(count).unwrap_or(0))
    }

    /// Appends a chat message.
    ///
    /// # Errors
    /// Database refusal.
    pub fn append_message(
        &self,
        run_id: RunId,
        role: ChatRole,
        text: &str,
    ) -> Result<ChatMessage, Refusal> {
        let message = ChatMessage {
            id: Uuid::now_v7(),
            run_id,
            role,
            text: text.to_owned(),
            created_at: Timestamp::now(),
        };
        self.db
            .with_connection(|connection| {
                connection.execute(
                    "INSERT INTO chat_messages(id, run_id, role, text, created_at)
                     VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![
                        message.id.to_string(),
                        message.run_id.to_string(),
                        message.role.as_str(),
                        message.text,
                        message.created_at.as_str()
                    ],
                )?;
                Ok(())
            })
            .map_err(map)?;
        Ok(message)
    }

    /// Lists a run's chat messages in insertion order.
    ///
    /// # Errors
    /// Database refusal.
    pub fn messages(&self, run_id: RunId) -> Result<Vec<ChatMessage>, Refusal> {
        self.db
            .with_connection(|connection| {
                let mut statement = connection.prepare(
                    "SELECT id, run_id, role, text, created_at FROM chat_messages
                     WHERE run_id = ?1 ORDER BY created_at ASC, rowid ASC",
                )?;
                let rows = statement.query_map(params![run_id.to_string()], |row| {
                    let role: String = row.get(2)?;
                    Ok(ChatMessage {
                        id: {
                            let raw: String = row.get(0)?;
                            raw.parse().unwrap_or_else(|_| Uuid::nil())
                        },
                        run_id: {
                            let raw: String = row.get(1)?;
                            raw.parse().unwrap_or_else(|_| RunId::from_uuid(Uuid::nil()))
                        },
                        role: ChatRole::from_str(&role).unwrap_or(ChatRole::System),
                        text: row.get(3)?,
                        created_at: {
                            let raw: String = row.get(4)?;
                            Timestamp::parse(&raw).unwrap_or_else(|_| Timestamp::from_unix_secs(0))
                        },
                    })
                })?;
                let mut messages = Vec::new();
                for row in rows {
                    messages.push(row?);
                }
                Ok(messages)
            })
            .map_err(map)
    }

    /// Records the bounded final text of a completed task.
    ///
    /// One row per task; a retry replaces the same task's row rather than
    /// duplicating it. Parent aggregators read this table, never the run-wide
    /// chat transcript, so a child result cannot be attributed to the wrong task.
    ///
    /// # Errors
    /// Database refusal.
    pub fn record_task_result(
        &self,
        run_id: RunId,
        task_id: TaskId,
        text: &str,
    ) -> Result<(), Refusal> {
        self.db
            .with_connection(|connection| {
                connection.execute(
                    "INSERT INTO task_results(task_id, run_id, text, created_at)
                     VALUES (?1, ?2, ?3, ?4)
                     ON CONFLICT(task_id) DO UPDATE SET text = excluded.text,
                                                        created_at = excluded.created_at",
                    params![
                        task_id.to_string(),
                        run_id.to_string(),
                        text,
                        Timestamp::now().as_str()
                    ],
                )?;
                Ok(())
            })
            .map_err(map)?;
        Ok(())
    }

    /// Reads a task's recorded final text.
    ///
    /// # Errors
    /// Database refusal.
    pub fn task_result(&self, task_id: TaskId) -> Result<Option<String>, Refusal> {
        self.db
            .with_connection(|connection| {
                connection
                    .query_row(
                        "SELECT text FROM task_results WHERE task_id = ?1",
                        params![task_id.to_string()],
                        |row| row.get(0),
                    )
                    .optional()
            })
            .map_err(map)
    }

    /// Records one durable agent-to-agent message under an atomic run budget.
    ///
    /// The insert and the count check are one SQL statement, so concurrent
    /// senders cannot both pass a check and exceed the budget.
    ///
    /// # Errors
    /// `E_CALL_LIMIT` when the run already holds `max_messages`; database refusal otherwise.
    pub fn record_agent_message_limited(
        &self,
        run_id: RunId,
        from_task_id: TaskId,
        to_task_id: TaskId,
        kind: &str,
        text: &str,
        max_messages: u64,
    ) -> Result<Uuid, Refusal> {
        let id = Uuid::now_v7();
        let changed = self
            .db
            .with_connection(|connection| {
                connection.execute(
                    "INSERT INTO agent_messages(id, run_id, from_task_id, to_task_id, kind, text, created_at)
                     SELECT ?1, ?2, ?3, ?4, ?5, ?6, ?7
                     WHERE (SELECT COUNT(*) FROM agent_messages WHERE run_id = ?2) < ?8",
                    params![
                        id.to_string(),
                        run_id.to_string(),
                        from_task_id.to_string(),
                        to_task_id.to_string(),
                        kind,
                        text,
                        Timestamp::now().as_str(),
                        i64::try_from(max_messages).unwrap_or(i64::MAX)
                    ],
                )
            })
            .map_err(map)?;
        if changed == 0 {
            return Err(Refusal::CallLimit);
        }
        Ok(id)
    }

    /// Finds an already-open identical question so a model cannot burn the
    /// parent answer budget on the same text twice.
    ///
    /// # Errors
    /// Database refusal.
    pub fn find_open_agent_message(
        &self,
        run_id: RunId,
        from_task_id: TaskId,
        to_task_id: TaskId,
        kind: &str,
        text: &str,
    ) -> Result<Option<Uuid>, Refusal> {
        let raw: Option<String> = self
            .db
            .with_connection(|connection| {
                connection
                    .query_row(
                        "SELECT id FROM agent_messages
                         WHERE run_id = ?1 AND from_task_id = ?2 AND to_task_id = ?3
                           AND kind = ?4 AND text = ?5 AND status = 'open'
                         ORDER BY created_at DESC, rowid DESC LIMIT 1",
                        params![
                            run_id.to_string(),
                            from_task_id.to_string(),
                            to_task_id.to_string(),
                            kind,
                            text
                        ],
                        |row| row.get(0),
                    )
                    .optional()
            })
            .map_err(map)?;
        Ok(raw.and_then(|value| value.parse::<Uuid>().ok()))
    }

    /// Records one answer attempt and its audit row atomically.
    ///
    /// # Errors
    /// `E_TASK_NOT_FOUND` when the message is missing; `E_TASK_TERMINAL` when it
    /// is no longer open; database refusal otherwise.
    pub fn record_agent_message_attempt(
        &self,
        run_id: RunId,
        message_id: Uuid,
        actor: &str,
        action: &str,
        detail_json: &str,
        max_attempts: i64,
    ) -> Result<(), Refusal> {
        let actor = actor.trim();
        let action = action.trim();
        if actor.is_empty() || action.is_empty() || max_attempts < 1 {
            return Err(Refusal::SchemaInvalid);
        }
        let changed = self
            .db
            .with_connection(|connection| {
                let tx = connection.unchecked_transaction()?;
                let changed = tx.execute(
                    "UPDATE agent_messages
                     SET attempts = attempts + 1, last_attempt_at = ?3
                     WHERE run_id = ?1 AND id = ?2 AND status = 'open' AND attempts < ?4",
                    params![
                        run_id.to_string(),
                        message_id.to_string(),
                        Timestamp::now().as_str(),
                        max_attempts
                    ],
                )?;
                if changed == 1 {
                    tx.execute(
                        "INSERT INTO a2a_audit(audit_id, run_id, message_id, actor, action, detail_json, created_at)
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                        params![
                            Uuid::now_v7().to_string(),
                            run_id.to_string(),
                            message_id.to_string(),
                            actor,
                            action,
                            detail_json,
                            Timestamp::now().as_str()
                        ],
                    )?;
                    tx.commit()?;
                }
                Ok(changed)
            })
            .map_err(map)?;
        if changed == 1 {
            return Ok(());
        }
        let status = self.agent_message_status(run_id, message_id)?;
        match status {
            None => Err(Refusal::TaskNotFound),
            Some(status) if status == "open" => Err(Refusal::CallLimit),
            Some(_) => Err(Refusal::TaskTerminal),
        }
    }

    /// Answers one open A2A message with a compare-and-set transition.
    ///
    /// # Errors
    /// `E_TASK_NOT_FOUND` when the message is missing; `E_TASK_TERMINAL` when it
    /// is no longer open; database refusal otherwise.
    pub fn answer_agent_message(
        &self,
        run_id: RunId,
        message_id: Uuid,
        actor: &str,
        answer: &str,
    ) -> Result<(), Refusal> {
        let changed = self
            .db
            .with_connection(|connection| {
                let tx = connection.unchecked_transaction()?;
                let changed = tx.execute(
                    "UPDATE agent_messages
                     SET status = 'answered', answered_at = ?3, answer_text = ?4,
                         answer_actor = ?5
                     WHERE run_id = ?1 AND id = ?2 AND status = 'open'",
                    params![
                        run_id.to_string(),
                        message_id.to_string(),
                        Timestamp::now().as_str(),
                        answer,
                        actor
                    ],
                )?;
                if changed == 1 {
                    tx.execute(
                        "INSERT INTO a2a_audit(audit_id, run_id, message_id, actor, action, detail_json, created_at)
                         VALUES (?1, ?2, ?3, ?4, 'answered', ?5, ?6)",
                        params![
                            Uuid::now_v7().to_string(),
                            run_id.to_string(),
                            message_id.to_string(),
                            actor,
                            serde_json::json!({"answer_chars": answer.chars().count()}).to_string(),
                            Timestamp::now().as_str()
                        ],
                    )?;
                    tx.commit()?;
                }
                Ok(changed)
            })
            .map_err(map)?;
        if changed == 1 {
            return Ok(());
        }
        let exists: i64 = self
            .db
            .with_connection(|connection| {
                connection.query_row(
                    "SELECT COUNT(*) FROM agent_messages WHERE run_id = ?1 AND id = ?2",
                    params![run_id.to_string(), message_id.to_string()],
                    |row| row.get(0),
                )
            })
            .map_err(map)?;
        if exists == 0 {
            Err(Refusal::TaskNotFound)
        } else {
            Err(Refusal::TaskTerminal)
        }
    }

    /// Sets the deadline for one open message.
    ///
    /// # Errors
    /// Database refusal.
    pub fn set_agent_message_deadline(
        &self,
        run_id: RunId,
        message_id: Uuid,
        deadline: &str,
    ) -> Result<(), Refusal> {
        self.db
            .with_connection(|connection| {
                connection.execute(
                    "UPDATE agent_messages SET deadline_at = ?3
                     WHERE run_id = ?1 AND id = ?2 AND status = 'open'",
                    params![run_id.to_string(), message_id.to_string(), deadline],
                )
            })
            .map_err(map)?;
        Ok(())
    }

    /// Reads one message's current status, if it exists.
    ///
    /// # Errors
    /// Database refusal.
    pub fn agent_message_status(
        &self,
        run_id: RunId,
        message_id: Uuid,
    ) -> Result<Option<String>, Refusal> {
        self.db
            .with_connection(|connection| {
                connection
                    .query_row(
                        "SELECT status FROM agent_messages WHERE run_id = ?1 AND id = ?2",
                        params![run_id.to_string(), message_id.to_string()],
                        |row| row.get(0),
                    )
                    .optional()
            })
            .map_err(map)
    }

    /// Marks an open message expired.
    ///
    /// # Errors
    /// Database refusal.
    pub fn expire_agent_message(&self, run_id: RunId, message_id: Uuid) -> Result<(), Refusal> {
        self.db
            .with_connection(|connection| {
                connection.execute(
                    "UPDATE agent_messages SET status = 'expired'
                     WHERE run_id = ?1 AND id = ?2 AND status = 'open'",
                    params![run_id.to_string(), message_id.to_string()],
                )
            })
            .map_err(map)?;
        Ok(())
    }

    /// Records one durable agent-to-agent message.
    ///
    /// # Errors
    /// Database refusal.
    pub fn record_agent_message(
        &self,
        run_id: RunId,
        from_task_id: TaskId,
        to_task_id: TaskId,
        kind: &str,
        text: &str,
    ) -> Result<Uuid, Refusal> {
        let id = Uuid::now_v7();
        self.db
            .with_connection(|connection| {
                connection.execute(
                    "INSERT INTO agent_messages(id, run_id, from_task_id, to_task_id, kind, text, created_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                    params![
                        id.to_string(),
                        run_id.to_string(),
                        from_task_id.to_string(),
                        to_task_id.to_string(),
                        kind,
                        text,
                        Timestamp::now().as_str()
                    ],
                )?;
                Ok(())
            })
            .map_err(map)?;
        Ok(id)
    }

    /// Number of durable messages recorded in a run.
    ///
    /// # Errors
    /// Database refusal.
    pub fn agent_message_count(&self, run_id: RunId) -> Result<u64, Refusal> {
        let count: i64 = self
            .db
            .with_connection(|connection| {
                connection.query_row(
                    "SELECT COUNT(*) FROM agent_messages WHERE run_id = ?1",
                    params![run_id.to_string()],
                    |row| row.get(0),
                )
            })
            .map_err(map)?;
        Ok(u64::try_from(count).unwrap_or(0))
    }

    /// Reads a task's persisted ownership projection.
    ///
    /// # Errors
    /// Database refusal.
    pub fn task_ownership(&self, task_id: TaskId) -> Result<Option<TaskOwnership>, Refusal> {
        self.db
            .with_connection(|connection| {
                let row: Option<(Option<String>, Option<String>, Option<String>)> = connection
                    .query_row(
                        "SELECT ownership_mode, ownership_scope, owner_task_id
                         FROM tasks WHERE id = ?1",
                        params![task_id.to_string()],
                        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                    )
                    .optional()?;
                Ok(row.and_then(|(mode, scope, owner)| {
                    Some(TaskOwnership {
                        mode: mode?,
                        scope,
                        owner_task_id: owner,
                    })
                }))
            })
            .map_err(map)
    }

    /// Reads a task's persisted class/rank/profile, when the v13 trigger
    /// resolved it. An unknown role stays `None`; it is never guessed.
    ///
    /// # Errors
    /// Database refusal.
    pub fn task_identity(&self, task_id: TaskId) -> Result<Option<TaskIdentity>, Refusal> {
        self.db
            .with_connection(|connection| {
                let row: Option<(Option<String>, Option<i64>, Option<String>)> = connection
                    .query_row(
                        "SELECT class, rank, profile_id FROM tasks WHERE id = ?1",
                        params![task_id.to_string()],
                        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                    )
                    .optional()?;
                Ok(row.and_then(|(class, rank, profile)| {
                    Some(TaskIdentity {
                        class: class?,
                        rank: u8::try_from(rank?).ok()?,
                        profile_id: profile?,
                    })
                }))
            })
            .map_err(map)
    }

    /// Lists a run's durable agent messages in insertion order.
    ///
    /// # Errors
    /// Database refusal.
    pub fn agent_messages_for_run(
        &self,
        run_id: RunId,
    ) -> Result<Vec<AgentMessageRecord>, Refusal> {
        self.db
            .with_connection(|connection| {
                let mut statement = connection.prepare(
                    "SELECT id, from_task_id, to_task_id, kind, text, created_at,
                            status, deadline_at, answered_at, answer_text, answer_actor, in_reply_to,
                            attempts, last_attempt_at
                     FROM agent_messages WHERE run_id = ?1 ORDER BY created_at ASC, rowid ASC",
                )?;
                let rows = statement.query_map(params![run_id.to_string()], |row| {
                    Ok(AgentMessageRecord {
                        id: row.get(0)?,
                        from_task_id: row.get(1)?,
                        to_task_id: row.get(2)?,
                        kind: row.get(3)?,
                        text: row.get(4)?,
                        created_at: row.get(5)?,
                        status: row.get(6)?,
                        deadline_at: row.get(7)?,
                        answered_at: row.get(8)?,
                        answer_text: row.get(9)?,
                        answer_actor: row.get(10)?,
                        in_reply_to: row.get(11)?,
                        attempts: row.get(12)?,
                        last_attempt_at: row.get(13)?,
                    })
                })?;
                let mut messages = Vec::new();
                for row in rows {
                    messages.push(row?);
                }
                Ok(messages)
            })
            .map_err(map)
    }

    /// Lists durable messages addressed to one task inside one run, newest
    /// first and bounded by `limit`. This is the recipient's A2A inbox.
    ///
    /// # Errors
    /// Database refusal.
    pub fn agent_messages_for_task(
        &self,
        run_id: RunId,
        task_id: TaskId,
        limit: usize,
    ) -> Result<Vec<AgentMessageRecord>, Refusal> {
        self.db
            .with_connection(|connection| {
                let mut statement = connection.prepare(
                    "SELECT id, from_task_id, to_task_id, kind, text, created_at,
                            status, deadline_at, answered_at, answer_text, answer_actor, in_reply_to,
                            attempts, last_attempt_at
                     FROM agent_messages
                     WHERE run_id = ?1 AND to_task_id = ?2
                     ORDER BY created_at DESC, rowid DESC LIMIT ?3",
                )?;
                let rows = statement.query_map(
                    params![
                        run_id.to_string(),
                        task_id.to_string(),
                        i64::try_from(limit).unwrap_or(i64::MAX)
                    ],
                    |row| {
                        Ok(AgentMessageRecord {
                            id: row.get(0)?,
                            from_task_id: row.get(1)?,
                            to_task_id: row.get(2)?,
                            kind: row.get(3)?,
                            text: row.get(4)?,
                            created_at: row.get(5)?,
                            status: row.get(6)?,
                            deadline_at: row.get(7)?,
                            answered_at: row.get(8)?,
                            answer_text: row.get(9)?,
                            answer_actor: row.get(10)?,
                            in_reply_to: row.get(11)?,
                            attempts: row.get(12)?,
                            last_attempt_at: row.get(13)?,
                        })
                    },
                )?;
                let mut messages = Vec::new();
                for row in rows {
                    messages.push(row?);
                }
                Ok(messages)
            })
            .map_err(map)
    }


    /// Upserts one durable role-scoped memory entry.
    ///
    /// # Errors
    /// Database refusal.
    pub fn record_agent_memory(
        &self,
        agent_key: &str,
        key: &str,
        value: &str,
        run_id: RunId,
        task_id: TaskId,
    ) -> Result<(), Refusal> {
        self.record_agent_memory_scoped(agent_key, key, value, run_id, task_id, "run")
    }

    /// Writes one memory entry inside a run **and branch scope**. `scope_id` is
    /// the owning hive/branch key (the parent task id, or `root`).
    ///
    /// # Errors
    /// Database refusal.
    pub fn record_agent_memory_scoped(
        &self,
        agent_key: &str,
        key: &str,
        value: &str,
        run_id: RunId,
        task_id: TaskId,
        scope_id: &str,
    ) -> Result<(), Refusal> {
        // Fail closed: a task whose persisted identity is missing must never
        // fall back to the run bank, which every task in the run can read. Its
        // own branch is the narrowest readable bank that cannot leak across
        // tasks.
        let class = self
            .task_identity(task_id)?
            .map(|identity| identity.class);
        let bank_id = match class.as_deref() {
            Some("hyper") => format!("bank:run:{run_id}"),
            Some("super") => format!("bank:hive:{task_id}"),
            Some("agent" | "subagent") => format!("bank:branch:{task_id}"),
            None => format!("bank:branch:{task_id}"),
            Some(_) if scope_id != "run" && scope_id != "legacy" => {
                format!("bank:branch:{task_id}")
            }
            Some(_) => format!("bank:run:{run_id}"),
        };
        self.db
            .with_connection(|connection| {
                connection.execute(
                    "INSERT INTO agent_memory(run_id, scope_id, agent_key, key, value, task_id, created_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                     ON CONFLICT(run_id, scope_id, agent_key, key) DO UPDATE SET
                        value = excluded.value,
                        task_id = excluded.task_id,
                        created_at = excluded.created_at",
                    params![
                        run_id.to_string(),
                        scope_id,
                        agent_key,
                        key,
                        value,
                        task_id.to_string(),
                        Timestamp::now().as_str()
                    ],
                )?;
                // Dual-write the bank view. The flat table remains the legacy
                // read path until the P4 read-set migration lands.
                connection.execute(
                    "INSERT OR IGNORE INTO memory_banks
                     (id, kind, scope_id, run_id, parent_bank_id, display_name,
                      rev, created_at, closed_at)
                     VALUES (?1, 'run', ?2, ?2, 'bank:global:default', ?3, 0, ?4, NULL)",
                    params![
                        format!("bank:run:{run_id}"),
                        run_id.to_string(),
                        format!("Run {run_id}"),
                        Timestamp::now().as_str()
                    ],
                )?;
                if bank_id.starts_with("bank:run:") {
                    connection.execute(
                        "INSERT OR IGNORE INTO memory_banks
                         (id, kind, scope_id, run_id, parent_bank_id, display_name,
                          rev, created_at, closed_at)
                         VALUES (?1, 'run', ?2, ?2, 'bank:global:default', ?3, 0, ?4, NULL)",
                        params![
                            bank_id,
                            run_id.to_string(),
                            format!("Run {run_id}"),
                            Timestamp::now().as_str()
                        ],
                    )?;
                } else if bank_id.starts_with("bank:hive:") {
                    connection.execute(
                        "INSERT OR IGNORE INTO memory_banks
                         (id, kind, scope_id, run_id, parent_bank_id, display_name,
                          rev, created_at, closed_at)
                         VALUES (?1, 'run', ?2, ?3, 'bank:global:default', ?4, 0, ?5, NULL)",
                        params![
                            format!("bank:run:{run_id}"),
                            run_id.to_string(),
                            run_id.to_string(),
                            format!("Run {run_id}"),
                            Timestamp::now().as_str()
                        ],
                    )?;
                    connection.execute(
                        "INSERT OR IGNORE INTO memory_banks
                         (id, kind, scope_id, run_id, owner_task_id, parent_bank_id,
                          display_name, rev, created_at, closed_at)
                         VALUES (?1, 'hive', ?2, ?3, ?2, ?4, ?5, 0, ?6, NULL)",
                        params![
                            bank_id,
                            task_id.to_string(),
                            run_id.to_string(),
                            format!("bank:run:{run_id}"),
                            format!("Hive {task_id}"),
                            Timestamp::now().as_str()
                        ],
                    )?;
                } else {
                    connection.execute(
                        "INSERT OR IGNORE INTO memory_banks
                         (id, kind, scope_id, run_id, parent_bank_id, display_name,
                          rev, created_at, closed_at)
                         VALUES (?1, 'run', ?2, ?3, 'bank:global:default', ?4, 0, ?5, NULL)",
                        params![
                            format!("bank:run:{run_id}"),
                            run_id.to_string(),
                            run_id.to_string(),
                            format!("Run {run_id}"),
                            Timestamp::now().as_str()
                        ],
                    )?;
                    connection.execute(
                        "INSERT OR IGNORE INTO memory_banks
                         (id, kind, scope_id, run_id, owner_task_id, parent_bank_id,
                          display_name, rev, created_at, closed_at)
                         VALUES (?1, 'branch', ?2, ?3, ?2, ?4, ?5, 0, ?6, NULL)",
                        params![
                            bank_id,
                            task_id.to_string(),
                            run_id.to_string(),
                            format!("bank:run:{run_id}"),
                            format!("Task {task_id}"),
                            Timestamp::now().as_str()
                        ],
                    )?;
                }
                connection.execute(
                    "DELETE FROM memory_entries WHERE bank_id = ?1 AND entry_key = ?2 AND status = 'active'",
                    params![bank_id, key],
                )?;
                let entry_id = format!("entry:{}", Uuid::now_v7());
                connection.execute(
                    "INSERT INTO memory_entries
                     (id, bank_id, entry_key, kind, value, source_kind, source_task_id,
                      source_run_id, trust, status, created_at, updated_at)
                     VALUES (?1, ?2, ?3, 'observation', ?4, 'agent_result', ?5, ?6,
                             'unverified', 'active', ?7, ?7)",
                    params![
                        entry_id,
                        bank_id,
                        key,
                        value,
                        task_id.to_string(),
                        run_id.to_string(),
                        Timestamp::now().as_str()
                    ],
                )?;
                append_memory_journal(
                    connection,
                    &run_id.to_string(),
                    &bank_id,
                    &entry_id,
                    "entry_written",
                    "agent",
                    Some(agent_key),
                    "{}",
                    Timestamp::now().as_str(),
                )?;
                Ok(())
            })
            .map_err(map)?;
        Ok(())
    }

    /// Validated, caller-bank memory write for a model-facing tool surface.
    ///
    /// Unlike the engine's [`Self::record_agent_memory_scoped`], this API is
    /// strict by construction:
    /// - the caller's persisted class must exist (unknown identity is refused,
    ///   never defaulted to a wider bank);
    /// - the write lands only in the caller's own bank (`hyper` -> run bank,
    ///   `super` -> its hive bank, `agent`/`subagent` -> its branch bank);
    /// - `kind` and `trust` are validated against the exact vocabularies the
    ///   read filter uses, so a row can never be written silently unreadable.
    ///
    /// Returns the new entry id.
    ///
    /// # Errors
    /// `E_TASK_NOT_FOUND` when the caller has no persisted identity,
    /// `E_SCHEMA_INVALID` for an unknown kind/trust or empty/oversized key,
    /// database refusal otherwise.
    pub fn record_memory_entry(
        &self,
        run_id: RunId,
        task_id: TaskId,
        agent_key: &str,
        key: &str,
        value: &str,
        kind: &str,
        trust: &str,
    ) -> Result<String, Refusal> {
        if key.trim().is_empty() || key.len() > 256 {
            return Err(Refusal::SchemaInvalid);
        }
        if !MEMORY_ENTRY_KINDS.contains(&kind) {
            return Err(Refusal::SchemaInvalid);
        }
        if !READABLE_MEMORY_TRUST.contains(&trust) {
            return Err(Refusal::SchemaInvalid);
        }
        let identity = self.task_identity(task_id)?.ok_or(Refusal::TaskNotFound)?;
        let bank_id =
            caller_bank_for_class(&identity.class, run_id, task_id).ok_or(Refusal::SchemaInvalid)?;
        let now = Timestamp::now();
        let entry_id = format!("entry:{}", Uuid::now_v7());
        let (bank_kind, owner_task_id, parent_bank_id, display_name): (
            &str,
            Option<String>,
            String,
            String,
        ) = match identity.class.as_str() {
            "hyper" => (
                "run",
                None,
                "bank:global:default".to_owned(),
                format!("Run {run_id}"),
            ),
            "super" => (
                "hive",
                Some(task_id.to_string()),
                format!("bank:run:{run_id}"),
                format!("Hive {task_id}"),
            ),
            _ => (
                "branch",
                Some(task_id.to_string()),
                format!("bank:run:{run_id}"),
                format!("Task {task_id}"),
            ),
        };
        self.db
            .with_connection(|connection| {
                let transaction = connection.transaction()?;
                transaction.execute(
                    "INSERT OR IGNORE INTO memory_banks
                     (id, kind, scope_id, run_id, owner_task_id, parent_bank_id,
                      display_name, rev, created_at, closed_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 0, ?8, NULL)",
                    params![
                        bank_id,
                        bank_kind,
                        task_id.to_string(),
                        run_id.to_string(),
                        owner_task_id,
                        parent_bank_id,
                        display_name,
                        now.as_str()
                    ],
                )?;
                transaction.execute(
                    "DELETE FROM memory_entries
                     WHERE bank_id = ?1 AND entry_key = ?2 AND status = 'active'",
                    params![bank_id, key],
                )?;
                transaction.execute(
                    "INSERT INTO memory_entries
                     (id, bank_id, entry_key, kind, value, source_kind, source_task_id,
                      source_run_id, trust, status, created_at, updated_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, 'agent_result', ?6, ?7, ?8, 'active', ?9, ?9)",
                    params![
                        entry_id,
                        bank_id,
                        key,
                        kind,
                        value,
                        task_id.to_string(),
                        run_id.to_string(),
                        trust,
                        now.as_str()
                    ],
                )?;
                append_memory_journal(
                    &transaction,
                    &run_id.to_string(),
                    &bank_id,
                    &entry_id,
                    "entry_written",
                    "agent",
                    Some(agent_key),
                    "{}",
                    now.as_str(),
                )?;
                transaction.commit()?;
                Ok(())
            })
            .map_err(map)?;
        Ok(entry_id)
    }

    /// Lists memory banks for one run.
    ///
    /// # Errors
    /// Database refusal.
    pub fn memory_banks_for_run(&self, run_id: RunId) -> Result<Vec<MemoryBankRecord>, Refusal> {
        self.db
            .with_connection(|connection| {
                let mut statement = connection.prepare(
                    "SELECT id, kind, scope_id, run_id, owner_task_id, parent_bank_id,
                            display_name, rev, created_at, closed_at
                     FROM memory_banks WHERE run_id = ?1
                     ORDER BY CASE kind WHEN 'run' THEN 0 WHEN 'hive' THEN 1
                                        WHEN 'branch' THEN 2 ELSE 3 END, id",
                )?;
                let rows = statement.query_map(params![run_id.to_string()], |row| {
                    Ok(MemoryBankRecord {
                        id: row.get(0)?,
                        kind: row.get(1)?,
                        scope_id: row.get(2)?,
                        run_id: row.get(3)?,
                        owner_task_id: row.get(4)?,
                        parent_bank_id: row.get(5)?,
                        display_name: row.get(6)?,
                        rev: row.get(7)?,
                        created_at: row.get(8)?,
                        closed_at: row.get(9)?,
                    })
                })?;
                let mut banks = Vec::new();
                for row in rows {
                    banks.push(row?);
                }
                Ok(banks)
            })
            .map_err(map)
    }
    /// newest first. This is the recall path used by a running task: facts from
    /// another run or another hive must not be injected.
    ///
    /// # Errors
    /// Database refusal.
    pub fn agent_memory_for_run(
        &self,
        run_id: RunId,
        agent_key: &str,
        limit: usize,
    ) -> Result<Vec<(String, String)>, Refusal> {
        self.db
            .with_connection(|connection| {
                let mut statement = connection.prepare(
                    "SELECT key, value FROM agent_memory WHERE run_id = ?1 AND agent_key = ?2
                     ORDER BY created_at DESC, rowid DESC LIMIT ?3",
                )?;
                let rows = statement.query_map(
                    params![
                        run_id.to_string(),
                        agent_key,
                        i64::try_from(limit).unwrap_or(i64::MAX)
                    ],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )?;
                let mut memories = Vec::new();
                for row in rows {
                    memories.push(row?);
                }
                Ok(memories)
            })
            .map_err(map)
    }

    /// Returns the latest durable memories for one run/scope across every role,
    /// newest first. This is the branch/run bank recall path: sibling isolation
    /// comes from selecting the correct scope, not from a role-name filter.
    ///
    /// # Errors
    /// Database refusal.
    pub fn agent_memory_for_scope_any_role(
        &self,
        run_id: RunId,
        scope_id: &str,
        limit: usize,
    ) -> Result<Vec<(String, String)>, Refusal> {
        self.db
            .with_connection(|connection| {
                let mut statement = connection.prepare(
                    "SELECT key, value FROM agent_memory
                     WHERE run_id = ?1 AND scope_id = ?2
                     ORDER BY created_at DESC, rowid DESC LIMIT ?3",
                )?;
                let rows = statement.query_map(
                    params![
                        run_id.to_string(),
                        scope_id,
                        i64::try_from(limit).unwrap_or(i64::MAX)
                    ],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )?;
                let mut memories = Vec::new();
                for row in rows {
                    memories.push(row?);
                }
                Ok(memories)
            })
            .map_err(map)
    }

    /// Writes the engine-owned run digest as a summary entry in the run bank.
    /// The digest is data, not a model-authored fact.
    ///
    /// # Errors
    /// Database refusal.
    pub fn write_run_digest(&self, run_id: RunId, digest: &str) -> Result<(), Refusal> {
        self.db
            .with_connection(|connection| {
                let bank_id = format!("bank:run:{run_id}");
                connection.execute(
                    "INSERT OR IGNORE INTO memory_banks
                     (id, kind, scope_id, run_id, parent_bank_id, display_name,
                      rev, created_at, closed_at)
                     VALUES (?1, 'run', ?2, ?2, 'bank:global:default', ?3, 0, ?4, NULL)",
                    params![bank_id, run_id.to_string(), format!("Run {run_id}"), Timestamp::now().as_str()],
                )?;
                connection.execute(
                    "DELETE FROM memory_entries
                     WHERE bank_id = ?1 AND entry_key = 'run-digest' AND status = 'active'",
                    params![bank_id],
                )?;
                connection.execute(
                    "INSERT INTO memory_entries
                     (id, bank_id, entry_key, kind, value, source_kind, source_run_id,
                      trust, status, created_at, updated_at)
                     VALUES (?1, ?2, 'run-digest', 'summary', ?3, 'engine_summary',
                             ?4, 'unverified', 'active', ?5, ?5)",
                    params![
                        format!("entry:{}", Uuid::now_v7()),
                        bank_id,
                        digest,
                        run_id.to_string(),
                        Timestamp::now().as_str()
                    ],
                )?;
                Ok(())
            })
            .map_err(map)
    }

    /// Clears one terminal run's legacy rows and bank entries, closing its banks.
    /// The caller must enforce terminal state; this method does not delete a
    /// role-global or another run's memory.
    ///
    /// # Errors
    /// Database refusal.
    pub fn clear_run_memory(
        &self,
        run_id: RunId,
        actor: &str,
        reason: &str,
    ) -> Result<MemoryClearReceipt, Refusal> {
        self.db
            .with_connection(|connection| {
                let transaction = connection.transaction()?;
                let flat_rows = transaction.execute(
                    "DELETE FROM agent_memory WHERE run_id = ?1",
                    params![run_id.to_string()],
                )?;
                let bank_entries = transaction.execute(
                    "DELETE FROM memory_entries
                     WHERE bank_id IN (SELECT id FROM memory_banks WHERE run_id = ?1)",
                    params![run_id.to_string()],
                )?;
                let banks_closed = transaction.execute(
                    "UPDATE memory_banks SET closed_at = ?2, rev = rev + 1
                     WHERE run_id = ?1 AND closed_at IS NULL",
                    params![run_id.to_string(), Timestamp::now().as_str()],
                )?;
                // The wipe itself is the audit event; keep it in the same
                // transaction as the deletes so a clear cannot be unlogged.
                append_memory_journal(
                    &transaction,
                    &run_id.to_string(),
                    "",
                    "",
                    "clear_run",
                    "operator",
                    Some(actor),
                    &serde_json::json!({
                        "reason": reason,
                        "flat_rows": flat_rows,
                        "bank_entries": bank_entries,
                        "banks_closed": banks_closed,
                    })
                    .to_string(),
                    Timestamp::now().as_str(),
                )?;
                transaction.commit()?;
                Ok(MemoryClearReceipt {
                    flat_rows: u64::try_from(flat_rows).unwrap_or(0),
                    bank_entries: u64::try_from(bank_entries).unwrap_or(0),
                    banks_closed: u64::try_from(banks_closed).unwrap_or(0),
                })
            })
            .map_err(map)
    }

    /// Lists open A2A questions across runs, newest first and bounded.
    ///
    /// # Errors
    /// Database refusal.
    pub fn pending_agent_messages(
        &self,
        limit: usize,
    ) -> Result<Vec<PendingAgentMessage>, Refusal> {
        self.db
            .with_connection(|connection| {
                let mut statement = connection.prepare(
                    "SELECT run_id, id, from_task_id, to_task_id, kind, text,
                            deadline_at, attempts, last_attempt_at, created_at
                     FROM agent_messages
                     WHERE status = 'open'
                     ORDER BY created_at DESC, rowid DESC LIMIT ?1",
                )?;
                let rows = statement.query_map(
                    params![i64::try_from(limit).unwrap_or(i64::MAX)],
                    |row| {
                        Ok(PendingAgentMessage {
                            run_id: row.get(0)?,
                            id: row.get(1)?,
                            from_task_id: row.get(2)?,
                            to_task_id: row.get(3)?,
                            kind: row.get(4)?,
                            text: row.get(5)?,
                            deadline_at: row.get(6)?,
                            attempts: row.get(7)?,
                            last_attempt_at: row.get(8)?,
                            created_at: row.get(9)?,
                        })
                    },
                )?;
                let mut messages = Vec::new();
                for row in rows {
                    messages.push(row?);
                }
                Ok(messages)
            })
            .map_err(map)
    }

    /// Expires every open message of a crashed run; the in-process waiter no
    /// longer exists, so the rows must not stay answerable-open forever.
    ///
    /// # Errors
    /// Database refusal.
    pub fn expire_open_agent_messages(&self, run_id: RunId) -> Result<(), Refusal> {
        self.db
            .with_connection(|connection| {
                connection.execute(
                    "UPDATE agent_messages SET status = 'expired'
                     WHERE run_id = ?1 AND status = 'open'",
                    params![run_id.to_string()],
                )?;
                Ok(())
            })
            .map_err(map)?;
        Ok(())
    }

    /// Operator-only trust transition for one bank entry; model text can never
    /// call this path. Only `verified`/`authoritative` targets are accepted.
    ///
    /// # Errors
    /// Database refusal; returns `false` when the entry is not in that run/bank.
    pub fn promote_memory_entry(
        &self,
        run_id: RunId,
        bank_id: &str,
        entry_id: &str,
        trust: &str,
        actor: &str,
    ) -> Result<bool, Refusal> {
        if !matches!(trust, "verified" | "authoritative") {
            return Err(Refusal::SchemaInvalid);
        }
        self.db
            .with_connection(|connection| {
                let transaction = connection.transaction()?;
                let changed = transaction.execute(
                    "UPDATE memory_entries SET trust = ?4, updated_at = ?5
                     WHERE id = ?1 AND bank_id = ?2 AND status = 'active'
                       AND EXISTS (SELECT 1 FROM memory_banks b
                                   WHERE b.id = ?2 AND b.run_id = ?3)",
                    params![
                        entry_id,
                        bank_id,
                        run_id.to_string(),
                        trust,
                        Timestamp::now().as_str()
                    ],
                )?;
                if changed == 1 {
                    append_memory_journal(
                        &transaction,
                        &run_id.to_string(),
                        bank_id,
                        entry_id,
                        "trust_changed",
                        "operator",
                        Some(actor),
                        &serde_json::json!({ "trust": trust }).to_string(),
                        Timestamp::now().as_str(),
                    )?;
                }
                transaction.commit()?;
                Ok(changed == 1)
            })
            .map_err(map)
    }

    /// Lists recent memory-journal actions for one run.
    ///
    /// # Errors
    /// Database refusal.
    pub fn memory_journal_for_run(
        &self,
        run_id: RunId,
        limit: usize,
    ) -> Result<Vec<(String, String, String, String)>, Refusal> {
        self.db
            .with_connection(|connection| {
                let mut statement = connection.prepare(
                    "SELECT action, bank_id, entry_id, created_at
                     FROM memory_journal WHERE run_id = ?1
                     ORDER BY seq DESC LIMIT ?2",
                )?;
                let rows = statement.query_map(
                    params![run_id.to_string(), i64::try_from(limit).unwrap_or(i64::MAX)],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )?;
                let mut journal = Vec::new();
                for row in rows {
                    journal.push(row?);
                }
                Ok(journal)
            })
            .map_err(map)
    }

    /// Persists the exact bank/entry set that shaped one task prompt.
    ///
    /// # Errors
    /// Database refusal.
    pub fn record_memory_context_snapshot(
        &self,
        task_id: TaskId,
        run_id: RunId,
        banks: &[String],
        entry_ids: &[String],
        rendered_chars: usize,
        truncated: bool,
        prompt_sha256: &str,
    ) -> Result<(), Refusal> {
        self.db
            .with_connection(|connection| {
                connection.execute(
                    "INSERT INTO memory_context_snapshots
                     (id, task_id, run_id, banks_json, entry_ids_json,
                      rendered_chars, truncated, prompt_sha256, created_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                    params![
                        format!("snapshot:{}", Uuid::now_v7()),
                        task_id.to_string(),
                        run_id.to_string(),
                        serde_json::to_string(banks).map_err(|_| rusqlite::Error::InvalidQuery)?,
                        serde_json::to_string(entry_ids).map_err(|_| rusqlite::Error::InvalidQuery)?,
                        i64::try_from(rendered_chars).unwrap_or(i64::MAX),
                        i64::from(truncated),
                        prompt_sha256,
                        Timestamp::now().as_str()
                    ],
                )?;
                Ok(())
            })
            .map_err(map)?;
        Ok(())
    }

    /// Reads the latest context snapshot for a task.
    ///
    /// # Errors
    /// Database refusal.
    pub fn memory_context_snapshot(
        &self,
        task_id: TaskId,
    ) -> Result<Option<(String, String, usize, bool, String, String)>, Refusal> {
        self.db
            .with_connection(|connection| {
                connection
                    .query_row(
                        "SELECT banks_json, entry_ids_json, rendered_chars, truncated,
                                prompt_sha256, created_at
                         FROM memory_context_snapshots
                         WHERE task_id = ?1
                         ORDER BY created_at DESC, rowid DESC LIMIT 1",
                        params![task_id.to_string()],
                        |row| {
                            Ok((
                                row.get(0)?,
                                row.get(1)?,
                                usize::try_from(row.get::<_, i64>(2)?).unwrap_or(usize::MAX),
                                row.get::<_, i64>(3)? != 0,
                                row.get(4)?,
                                row.get(5)?,
                            ))
                        },
                    )
                    .optional()
            })
            .map_err(map)
    }

    /// Reads the journal hash chain for a run, oldest first.
    ///
    /// # Errors
    /// Database refusal.
    pub fn memory_journal_chain(
        &self,
        run_id: RunId,
    ) -> Result<Vec<(Option<String>, String)>, Refusal> {
        self.db
            .with_connection(|connection| {
                let mut statement = connection.prepare(
                    "SELECT prev_hash, event_hash FROM memory_journal
                     WHERE run_id = ?1 AND event_hash IS NOT NULL
                     ORDER BY seq ASC",
                )?;
                let rows = statement.query_map(params![run_id.to_string()], |row| {
                    Ok((row.get(0)?, row.get(1)?))
                })?;
                let mut chain = Vec::new();
                for row in rows {
                    chain.push(row?);
                }
                Ok(chain)
            })
            .map_err(map)
    }

    /// Verifies the run's chained journal rows.
    ///
    /// # Errors
    /// Database refusal.
    pub fn verify_memory_journal(
        &self,
        run_id: RunId,
    ) -> Result<JournalVerification, Refusal> {
        self.db
            .with_connection(|connection| {
                let mut statement = connection.prepare(
                    "SELECT run_id, bank_id, entry_id, action, actor_kind, actor_id,
                            detail_json, created_at, prev_hash, event_hash
                     FROM memory_journal WHERE run_id = ?1 ORDER BY seq ASC",
                )?;
                let rows = statement.query_map(params![run_id.to_string()], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<String>>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, Option<String>>(5)?,
                        row.get::<_, String>(6)?,
                        row.get::<_, String>(7)?,
                        row.get::<_, Option<String>>(8)?,
                        row.get::<_, Option<String>>(9)?,
                    ))
                })?;
                let mut previous: Option<String> = None;
                let mut checked = 0usize;
                let mut unchained = 0usize;
                let mut valid = true;
                for row in rows {
                    let (
                        run,
                        bank,
                        entry,
                        action,
                        actor_kind,
                        actor_id,
                        detail,
                        created,
                        prev_hash,
                        event_hash,
                    ) = row?;
                    let Some(event_hash) = event_hash else {
                        unchained += 1;
                        continue;
                    };
                    let prev_hash = prev_hash.unwrap_or_default();
                    let expected = journal_event_hash(
                        previous.as_deref().unwrap_or(""),
                        &run,
                        bank.as_deref().unwrap_or(""),
                        entry.as_deref().unwrap_or(""),
                        &action,
                        &actor_kind,
                        actor_id.as_deref().unwrap_or(""),
                        &detail,
                        &created,
                    );
                    if prev_hash != previous.as_deref().unwrap_or("")
                        || event_hash != expected
                    {
                        valid = false;
                    }
                    previous = Some(event_hash);
                    checked += 1;
                }
                let head: Option<String> = connection
                    .query_row(
                        "SELECT memory_journal_head FROM runs WHERE id = ?1",
                        params![run_id.to_string()],
                        |row| row.get(0),
                    )
                    .optional()?
                    .flatten();
                if head != previous {
                    valid = false;
                }
                Ok(JournalVerification {
                    valid,
                    checked,
                    unchained,
                })
            })
            .map_err(map)
    }

    /// Prunes inactive (superseded/mark inactived/expired) bank entries for one run.
    /// Active entries are never touched. A dry run counts without deleting and
    /// does not journal. Returns the number of rows that would/were removed.
    ///
    /// # Errors
    /// Database refusal.
    pub fn prune_inactive_memory(&self, run_id: RunId, dry_run: bool) -> Result<u64, Refusal> {
        self.db
            .with_connection(|connection| {
                let count: i64 = connection.query_row(
                    "SELECT COUNT(*) FROM memory_entries
                     WHERE bank_id IN (SELECT id FROM memory_banks WHERE run_id = ?1)
                       AND status IN ('superseded','retracted')",
                    params![run_id.to_string()],
                    |row| row.get(0),
                )?;
                if dry_run || count == 0 {
                    return Ok(u64::try_from(count).unwrap_or(0));
                }
                let transaction = connection.transaction()?;
                let removed = transaction.execute(
                    "DELETE FROM memory_entries
                     WHERE bank_id IN (SELECT id FROM memory_banks WHERE run_id = ?1)
                       AND status IN ('superseded','retracted')",
                    params![run_id.to_string()],
                )?;
                append_memory_journal(
                    &transaction,
                    &run_id.to_string(),
                    "",
                    "",
                    "entries_pruned",
                    "operator",
                    Some("operator"),
                    &serde_json::json!({ "removed": removed }).to_string(),
                    Timestamp::now().as_str(),
                )?;
                transaction.commit()?;
                Ok(u64::try_from(removed).unwrap_or(0))
            })
            .map_err(map)
    }

    /// Reads the bounded bank set for one task: own branch, parent branch, and
    /// the run bank. Sibling/foreign-run banks are not addressable.
    ///
    /// # Errors
    /// Database refusal.
    pub fn memory_read_set(
        &self,
        run_id: RunId,
        task_id: TaskId,
        parent_task_id: Option<TaskId>,
        limit: usize,
    ) -> Result<Vec<MemoryReadEntry>, Refusal> {
        let parent_bank = parent_task_id
            .map(|parent| format!("bank:branch:{parent}"))
            .unwrap_or_else(|| "bank:branch:none".to_owned());
        let task_bank = format!("bank:branch:{task_id}");
        let run_bank = format!("bank:run:{run_id}");
        self.db
            .with_connection(|connection| {
                let hive_bank = nearest_hive_bank(connection, Some(task_id))?
                    .unwrap_or_else(|| "bank:hive:none".to_owned());
                let mut statement = connection.prepare(
                    "SELECT id, bank_id, entry_key, value FROM memory_entries
                     WHERE status = 'active'
                       AND (trust IN ('unverified','verified','authoritative')
                            OR (trust IS NULL AND kind = 'note'))
                       AND bank_id IN (?1, ?2, ?3, ?4)
                     ORDER BY created_at DESC, id DESC LIMIT ?5",
                )?;
                let rows = statement.query_map(
                    params![
                        run_bank,
                        parent_bank,
                        task_bank,
                        hive_bank,
                        i64::try_from(limit).unwrap_or(i64::MAX)
                    ],
                    |row| {
                        Ok(MemoryReadEntry {
                            entry_id: row.get(0)?,
                            bank_id: row.get(1)?,
                            key: row.get(2)?,
                            value: row.get(3)?,
                        })
                    },
                )?;
                let mut entries = Vec::new();
                for row in rows {
                    entries.push(row?);
                }
                Ok(entries)
            })
            .map_err(map)
    }

    /// Returns the bounded run/hive summary set used by the HyperAgent control
    /// turn. Values are intentionally returned for summaries; callers must
    /// truncate them and never expose sibling branch raw entries.
    ///
    /// # Errors
    /// Database refusal.
    pub fn memory_hyper_summary(
        &self,
        run_id: RunId,
        limit: usize,
    ) -> Result<Vec<MemoryReadEntry>, Refusal> {
        self.db
            .with_connection(|connection| hyper_summary_rows(connection, run_id, limit))
            .map_err(map)
    }

    /// Lists active entries in one bank, newest first and bounded.
    ///
    /// # Errors
    /// Database refusal.
    pub fn memory_entries_for_bank(
        &self,
        bank_id: &str,
        limit: usize,
    ) -> Result<Vec<MemoryEntryRecord>, Refusal> {
        self.db
            .with_connection(|connection| {
                let mut statement = connection.prepare(
                    "SELECT id, bank_id, entry_key, kind, value, source_kind,
                            trust, status, created_at
                     FROM memory_entries
                     WHERE bank_id = ?1 AND status = 'active'
                     ORDER BY created_at DESC, id DESC LIMIT ?2",
                )?;
                let rows = statement.query_map(
                    params![bank_id, i64::try_from(limit).unwrap_or(i64::MAX)],
                    |row| {
                        Ok(MemoryEntryRecord {
                            id: row.get(0)?,
                            bank_id: row.get(1)?,
                            key: row.get(2)?,
                            kind: row.get(3)?,
                            value: row.get(4)?,
                            source_kind: row.get(5)?,
                            trust: row.get(6)?,
                            status: row.get(7)?,
                            created_at: row.get(8)?,
                        })
                    },
                )?;
                let mut entries = Vec::new();
                for row in rows {
                    entries.push(row?);
                }
                Ok(entries)
            })
            .map_err(map)
    }

    /// Lists active entries in one bank **scoped to one run**.
    ///
    /// This is the isolated read API: the bank must belong to `run_id`, so a
    /// caller cannot read a same-shaped bank from a foreign run. Prefer this
    /// over [`Self::memory_entries_for_bank`], which is the unscoped operator
    /// surface and trusts its caller to supply an in-run bank id.
    ///
    /// # Errors
    /// Database refusal.
    pub fn memory_entries_for_bank_scoped(
        &self,
        run_id: RunId,
        bank_id: &str,
        limit: usize,
    ) -> Result<Vec<MemoryEntryRecord>, Refusal> {
        self.db
            .with_connection(|connection| {
                let mut statement = connection.prepare(
                    "SELECT e.id, e.bank_id, e.entry_key, e.kind, e.value, e.source_kind,
                            e.trust, e.status, e.created_at
                     FROM memory_entries e
                     JOIN memory_banks b ON b.id = e.bank_id
                     WHERE e.bank_id = ?1 AND b.run_id = ?2 AND e.status = 'active'
                     ORDER BY e.created_at DESC, e.id DESC LIMIT ?3",
                )?;
                let rows = statement.query_map(
                    params![
                        bank_id,
                        run_id.to_string(),
                        i64::try_from(limit).unwrap_or(i64::MAX)
                    ],
                    |row| {
                        Ok(MemoryEntryRecord {
                            id: row.get(0)?,
                            bank_id: row.get(1)?,
                            key: row.get(2)?,
                            kind: row.get(3)?,
                            value: row.get(4)?,
                            source_kind: row.get(5)?,
                            trust: row.get(6)?,
                            status: row.get(7)?,
                            created_at: row.get(8)?,
                        })
                    },
                )?;
                let mut entries = Vec::new();
                for row in rows {
                    entries.push(row?);
                }
                Ok(entries)
            })
            .map_err(map)
    }

    /// Returns the latest durable memories for a role inside one run and one
    /// hive/branch scope, newest first.
    ///
    /// # Errors
    /// Database refusal.
    pub fn agent_memory_for_scope(
        &self,
        run_id: RunId,
        scope_id: &str,
        agent_key: &str,
        limit: usize,
    ) -> Result<Vec<(String, String)>, Refusal> {
        self.db
            .with_connection(|connection| {
                let mut statement = connection.prepare(
                    "SELECT key, value FROM agent_memory
                     WHERE run_id = ?1 AND scope_id = ?2 AND agent_key = ?3
                     ORDER BY created_at DESC, rowid DESC LIMIT ?4",
                )?;
                let rows = statement.query_map(
                    params![
                        run_id.to_string(),
                        scope_id,
                        agent_key,
                        i64::try_from(limit).unwrap_or(i64::MAX)
                    ],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )?;
                let mut memories = Vec::new();
                for row in rows {
                    memories.push(row?);
                }
                Ok(memories)
            })
            .map_err(map)
    }

    /// Lists every durable memory row for one run with its branch scope.
    ///
    /// This is the read-only operator surface; it never feeds a running task.
    ///
    /// # Errors
    /// Database refusal.
    pub fn agent_memory_entries_for_run(
        &self,
        run_id: RunId,
        limit: usize,
    ) -> Result<Vec<AgentMemoryRecord>, Refusal> {
        self.agent_memory_entries_scoped(run_id, None, limit)
    }

    /// Lists run memory rows, optionally limited to one hive/branch scope.
    ///
    /// # Errors
    /// Database refusal.
    pub fn agent_memory_entries_for_scope(
        &self,
        run_id: RunId,
        scope_id: &str,
        limit: usize,
    ) -> Result<Vec<AgentMemoryRecord>, Refusal> {
        self.agent_memory_entries_scoped(run_id, Some(scope_id), limit)
    }

    fn agent_memory_entries_scoped(
        &self,
        run_id: RunId,
        scope_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<AgentMemoryRecord>, Refusal> {
        self.db
            .with_connection(|connection| {
                let limit = i64::try_from(limit).unwrap_or(i64::MAX);
                let mut statement = connection.prepare(
                    "SELECT run_id, scope_id, agent_key, key, value, task_id, created_at
                     FROM agent_memory
                     WHERE run_id = ?1 AND (?2 IS NULL OR scope_id = ?2)
                     ORDER BY created_at DESC, rowid DESC LIMIT ?3",
                )?;
                let rows = statement.query_map(
                    params![run_id.to_string(), scope_id, limit],
                    |row| {
                        Ok(AgentMemoryRecord {
                            run_id: row.get(0)?,
                            scope_id: row.get(1)?,
                            agent_key: row.get(2)?,
                            key: row.get(3)?,
                            value: row.get(4)?,
                            task_id: row.get(5)?,
                            created_at: row.get(6)?,
                        })
                    },
                )?;
                let mut memories = Vec::new();
                for row in rows {
                    memories.push(row?);
                }
                Ok(memories)
            })
            .map_err(map)
    }

    /// Returns the latest durable memories for a role, newest first.
    ///
    /// # Errors
    /// Database refusal.
    pub fn agent_memory(
        &self,
        agent_key: &str,
        limit: usize,
    ) -> Result<Vec<(String, String)>, Refusal> {
        self.db
            .with_connection(|connection| {
                let mut statement = connection.prepare(
                    "SELECT key, value FROM agent_memory WHERE agent_key = ?1
                     ORDER BY created_at DESC, rowid DESC LIMIT ?2",
                )?;
                let rows = statement.query_map(
                    params![agent_key, i64::try_from(limit).unwrap_or(i64::MAX)],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )?;
                let mut memories = Vec::new();
                for row in rows {
                    memories.push(row?);
                }
                Ok(memories)
            })
            .map_err(map)
    }


    /// Persists one run's measured token totals (NULL stays NULL).
    ///
    /// # Errors
    /// Database refusal.
    pub fn record_run_usage(&self, run_id: RunId, usage: &Usage) -> Result<(), Refusal> {
        let as_sql = |value: Option<u64>| value.map(|value| i64::try_from(value).unwrap_or(i64::MAX));
        self.db
            .with_connection(|connection| {
                connection.execute(
                    "INSERT INTO run_usage(run_id, prompt_tokens, completion_tokens, total_tokens,
                        prompt_cache_hit_tokens, prompt_cache_miss_tokens, reasoning_tokens, created_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                     ON CONFLICT(run_id) DO UPDATE SET
                        prompt_tokens = excluded.prompt_tokens,
                        completion_tokens = excluded.completion_tokens,
                        total_tokens = excluded.total_tokens,
                        prompt_cache_hit_tokens = excluded.prompt_cache_hit_tokens,
                        prompt_cache_miss_tokens = excluded.prompt_cache_miss_tokens,
                        reasoning_tokens = excluded.reasoning_tokens,
                        created_at = excluded.created_at",
                    params![
                        run_id.to_string(),
                        as_sql(usage.prompt_tokens),
                        as_sql(usage.completion_tokens),
                        as_sql(usage.total_tokens),
                        as_sql(usage.prompt_cache_hit_tokens),
                        as_sql(usage.prompt_cache_miss_tokens),
                        as_sql(usage.reasoning_tokens),
                        Timestamp::now().as_str()
                    ],
                )?;
                Ok(())
            })
            .map_err(map)?;
        Ok(())
    }

    /// Reads a run's persisted usage, if any.
    ///
    /// # Errors
    /// Database refusal.
    pub fn run_usage(&self, run_id: RunId) -> Result<Option<Usage>, Refusal> {
        self.db
            .with_connection(|connection| {
                connection
                    .query_row(
                        "SELECT prompt_tokens, completion_tokens, total_tokens,
                                prompt_cache_hit_tokens, prompt_cache_miss_tokens, reasoning_tokens
                         FROM run_usage WHERE run_id = ?1",
                        params![run_id.to_string()],
                        |row| {
                            let read = |index: usize| -> rusqlite::Result<Option<u64>> {
                                Ok(row
                                    .get::<_, Option<i64>>(index)?
                                    .and_then(|value| u64::try_from(value).ok()))
                            };
                            Ok(Usage {
                                prompt_tokens: read(0)?,
                                completion_tokens: read(1)?,
                                total_tokens: read(2)?,
                                prompt_cache_hit_tokens: read(3)?,
                                prompt_cache_miss_tokens: read(4)?,
                                reasoning_tokens: read(5)?,
                            })
                        },
                    )
                    .optional()
            })
            .map_err(map)
    }

    /// Lists persisted imported profiles as `(id, profile JSON, source)`.
    ///
    /// # Errors
    /// Database or stored-JSON refusal.
    pub fn agent_profiles(
        &self,
    ) -> Result<Vec<(String, serde_json::Value, String)>, Refusal> {
        let rows = self
            .db
            .with_connection(|connection| {
                let mut statement =
                    connection.prepare("SELECT id, profile_json, source FROM agent_profiles")?;
                let rows = statement.query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                })?;
                let mut profiles = Vec::new();
                for row in rows {
                    profiles.push(row?);
                }
                Ok(profiles)
            })
            .map_err(map)?;
        let mut decoded = Vec::new();
        for (id, json, source) in rows {
            let value = serde_json::from_str(&json).map_err(|_| Refusal::SchemaInvalid)?;
            decoded.push((id, value, source));
        }
        Ok(decoded)
    }

    /// Upserts one imported profile as validated JSON.
    ///
    /// # Errors
    /// Database refusal.
    pub fn upsert_agent_profile(
        &self,
        id: &str,
        profile: &serde_json::Value,
        source: &str,
    ) -> Result<(), Refusal> {
        let json = serde_json::to_string(profile).map_err(|_| Refusal::SchemaInvalid)?;
        self.db
            .with_connection(|connection| {
                connection.execute(
                    "INSERT INTO agent_profiles(id, profile_json, source, imported_at)
                     VALUES (?1, ?2, ?3, ?4)
                     ON CONFLICT(id) DO UPDATE SET
                        profile_json = excluded.profile_json,
                        source = excluded.source,
                        imported_at = excluded.imported_at",
                    params![id, json, source, Timestamp::now().as_str()],
                )?;
                Ok(())
            })
            .map_err(map)?;
        Ok(())
    }

    /// Deletes one imported profile; returns whether a row was removed.
    ///
    /// # Errors
    /// Database refusal.
    pub fn delete_agent_profile(&self, id: &str) -> Result<bool, Refusal> {
        let removed = self
            .db
            .with_connection(|connection| {
                connection.execute("DELETE FROM agent_profiles WHERE id = ?1", params![id])
            })
            .map_err(map)?;
        Ok(removed > 0)
    }

    /// Clears one role's durable memories.
    ///
    /// # Errors
    /// Database refusal.
    pub fn clear_agent_memory(&self, agent_key: &str) -> Result<u64, Refusal> {
        let removed = self
            .db
            .with_connection(|connection| {
                connection.execute(
                    "DELETE FROM agent_memory WHERE agent_key = ?1",
                    params![agent_key],
                )
            })
            .map_err(map)?;
        Ok(u64::try_from(removed).unwrap_or(0))
    }

    pub fn record_receipt(&self, correlation_id: Uuid, run_id: RunId) -> Result<(), Refusal> {
        self.db
            .with_connection(|connection| {
                connection.execute(
                    "INSERT OR REPLACE INTO command_receipts(correlation_id, run_id, created_at)
                     VALUES (?1, ?2, ?3)",
                    params![correlation_id.to_string(), run_id.to_string(), Timestamp::now().as_str()],
                )?;
                Ok(())
            })
            .map_err(map)?;
        Ok(())
    }

    /// Looks up the run a mutating command already created (24 h window).
    ///
    /// # Errors
    /// Database refusal.
    pub fn receipt_run(&self, correlation_id: Uuid) -> Result<Option<RunId>, Refusal> {
        let maybe: Option<(String, String)> = self
            .db
            .with_connection(|connection| {
                connection
                    .query_row(
                        "SELECT run_id, created_at FROM command_receipts WHERE correlation_id = ?1",
                        params![correlation_id.to_string()],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .optional()
            })
            .map_err(map)?;
        let Some((run_id, created_at)) = maybe else {
            return Ok(None);
        };
        let Ok(created) = Timestamp::parse(&created_at) else {
            return Ok(None);
        };
        let Ok(created_secs) = created.unix_secs() else {
            return Ok(None);
        };
        let now = Timestamp::now().unix_secs().unwrap_or(created_secs);
        if now.saturating_sub(created_secs) > 24 * 3_600 {
            return Ok(None);
        }
        Ok(run_id.parse().ok())
    }
}

/// Creates the run/hive/branch bank row for a task inside its insert
/// transaction. The v13 identity trigger has already filled `class`; a missing
/// identity is left alone rather than guessed.
fn ensure_bank_for_inserted_task(
    connection: &rusqlite::Connection,
    task: &TaskRecord,
) -> rusqlite::Result<()> {
    let class: Option<String> = connection
        .query_row(
            "SELECT class FROM tasks WHERE id = ?1",
            params![task.id.to_string()],
            |row| row.get(0),
        )
        .optional()?
        .flatten();
    let Some(class) = class else {
        return Ok(());
    };
    let run_bank = format!("bank:run:{}", task.run_id);
    let now = Timestamp::now().as_str().to_owned();
    connection.execute(
        "INSERT OR IGNORE INTO memory_banks
         (id, kind, scope_id, run_id, parent_bank_id, display_name, rev, created_at, closed_at)
         VALUES (?1, 'run', ?2, ?2, 'bank:global:default', ?3, 0, ?4, NULL)",
        params![run_bank, task.run_id.to_string(), format!("Run {}", task.run_id), now],
    )?;
    match class.as_str() {
        "super" => {
            connection.execute(
                "INSERT OR IGNORE INTO memory_banks
                 (id, kind, scope_id, run_id, owner_task_id, parent_bank_id, display_name,
                  rev, created_at, closed_at)
                 VALUES (?1, 'hive', ?2, ?3, ?2, ?4, ?5, 0, ?6, NULL)",
                params![
                    format!("bank:hive:{}", task.id),
                    task.id.to_string(),
                    task.run_id.to_string(),
                    run_bank,
                    format!("Hive {}", task.id),
                    now
                ],
            )?;
        }
        "agent" | "subagent" => {
            let parent_bank = nearest_hive_bank(connection, task.parent_task_id)?
                .unwrap_or_else(|| run_bank.clone());
            connection.execute(
                "INSERT OR IGNORE INTO memory_banks
                 (id, kind, scope_id, run_id, owner_task_id, parent_bank_id, display_name,
                  rev, created_at, closed_at)
                 VALUES (?1, 'branch', ?2, ?3, ?2, ?4, ?5, 0, ?6, NULL)",
                params![
                    format!("bank:branch:{}", task.id),
                    task.id.to_string(),
                    task.run_id.to_string(),
                    parent_bank,
                    format!("Task {}", task.id),
                    now
                ],
            )?;
        }
        _ => {}
    }
    Ok(())
}

/// Finds the nearest ancestor SuperAgent hive bank for an Agent/Subagent.
fn nearest_hive_bank(
    connection: &rusqlite::Connection,
    mut cursor: Option<TaskId>,
) -> rusqlite::Result<Option<String>> {
    for _ in 0..=8u8 {
        let Some(task_id) = cursor else {
            return Ok(None);
        };
        let row: Option<(Option<String>, Option<String>)> = connection
            .query_row(
                "SELECT class, parent_task_id FROM tasks WHERE id = ?1",
                params![task_id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let Some((class, parent)) = row else {
            return Ok(None);
        };
        if class.as_deref() == Some("super") {
            return Ok(Some(format!("bank:hive:{task_id}")));
        }
        if class.as_deref() == Some("hyper") {
            return Ok(None);
        }
        cursor = parent.and_then(|parent| parent.parse::<TaskId>().ok());
    }
    Ok(None)
}

/// The engine's bounded HyperAgent summary projection: run/hive entries only,
/// newest first, never a raw sibling bank.
fn hyper_summary_rows(
    connection: &rusqlite::Connection,
    run_id: RunId,
    limit: usize,
) -> rusqlite::Result<Vec<MemoryReadEntry>> {
    let mut statement = connection.prepare(
        "SELECT e.id, e.bank_id, e.entry_key, e.value
         FROM memory_entries e
         JOIN memory_banks b ON b.id = e.bank_id
         WHERE b.run_id = ?1 AND b.kind IN ('run','hive')
           AND e.status = 'active'
           AND (e.trust IN ('unverified','verified','authoritative')
                OR (e.trust IS NULL AND e.kind = 'note'))
         ORDER BY e.created_at DESC, e.id DESC LIMIT ?2",
    )?;
    let rows = statement.query_map(
        params![run_id.to_string(), i64::try_from(limit).unwrap_or(i64::MAX)],
        |row| {
            Ok(MemoryReadEntry {
                entry_id: row.get(0)?,
                bank_id: row.get(1)?,
                key: row.get(2)?,
                value: row.get(3)?,
            })
        },
    )?;
    let mut entries = Vec::new();
    for row in rows {
        entries.push(row?);
    }
    Ok(entries)
}

fn timestamp_at(
    row: &rusqlite::Row<'_>,
    index: usize,
    field: &str,
) -> rusqlite::Result<Timestamp> {
    let raw: String = row.get(index)?;
    Timestamp::parse(&raw).map_err(|_| {
        rusqlite::Error::InvalidColumnType(index, field.to_owned(), rusqlite::types::Type::Text)
    })
}

fn optional_timestamp_at(
    row: &rusqlite::Row<'_>,
    index: usize,
    field: &str,
) -> rusqlite::Result<Option<Timestamp>> {
    let raw: Option<String> = row.get(index)?;
    raw.map(|value| {
        Timestamp::parse(&value).map_err(|_| {
            rusqlite::Error::InvalidColumnType(
                index,
                field.to_owned(),
                rusqlite::types::Type::Text,
            )
        })
    })
    .transpose()
}

fn policy_at(row: &rusqlite::Row<'_>, index: usize) -> rusqlite::Result<PermissionPolicy> {
    let raw: String = row.get(index)?;
    PermissionPolicy::from_str_strict(&raw).ok_or_else(|| {
        rusqlite::Error::InvalidColumnType(
            index,
            "permission_policy".to_owned(),
            rusqlite::types::Type::Text,
        )
    })
}

fn row_to_run(row: &rusqlite::Row<'_>) -> rusqlite::Result<Run> {
    let id: String = row.get(0)?;
    let status: String = row.get(3)?;
    let error_code: Option<String> = row.get(4)?;
    Ok(Run {
        id: id.parse().unwrap_or_else(|_| RunId::from_uuid(Uuid::nil())),
        project_id: row.get(1)?,
        prompt: row.get(2)?,
        status: parse_state(&status),
        error_code,
        created_at: timestamp_at(row, 5, "created_at")?,
        updated_at: timestamp_at(row, 6, "updated_at")?,
        session_id: row.get(7)?,
        permission_policy: policy_at(row, 8)?,
    })
}

fn row_to_task(row: &rusqlite::Row<'_>) -> rusqlite::Result<TaskRecord> {
    let id: String = row.get(0)?;
    let run_id: String = row.get(1)?;
    let parent: Option<String> = row.get(2)?;
    let state: String = row.get(6)?;
    let depth: i64 = row.get(4)?;
    let spawn_seq: i64 = row.get(5)?;
    let rev: i64 = row.get(7)?;
    Ok(TaskRecord {
        id: id.parse().unwrap_or_else(|_| TaskId::from_uuid(Uuid::nil())),
        run_id: run_id
            .parse()
            .unwrap_or_else(|_| RunId::from_uuid(Uuid::nil())),
        parent_task_id: parent.and_then(|raw| raw.parse().ok()),
        role: row.get(3)?,
        depth: u8::try_from(depth).unwrap_or(u8::MAX),
        spawn_seq: u64::try_from(spawn_seq).unwrap_or(0),
        state: parse_state(&state),
        rev: u64::try_from(rev).unwrap_or(0),
    })
}

fn parse_state(value: &str) -> AgentState {
    match value {
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
        _ => AgentState::Failed,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> RunStore {
        let db = Database::open_in_memory().expect("in-memory database");
        db.migrate().expect("migrate");
        RunStore::new(Arc::new(db))
    }

    fn seed_task(store: &RunStore, state: AgentState) -> TaskRecord {
        let run = store
            .create_run("project-1", "terminal guard", AgentState::Running)
            .expect("run");
        let task = TaskRecord {
            id: TaskId::new(),
            run_id: run.id,
            parent_task_id: None,
            role: "worker".to_owned(),
            depth: 1,
            spawn_seq: 1,
            state,
            rev: 0,
        };
        store.insert_task(&task).expect("task row");
        task
    }

    #[test]
    fn terminal_task_cannot_be_resurrected() {
        let store = store();
        let task = seed_task(&store, AgentState::Queued);

        // Non-terminal writes stay legal and bump the revision.
        let running = store
            .update_task_state(task.id, AgentState::Running)
            .expect("start");
        assert!(running > 0, "a real transition bumps the revision");
        let completed = store
            .update_task_state(task.id, AgentState::Completed)
            .expect("complete");
        assert!(completed > running);

        // Every resurrection attempt is refused by name and changes nothing.
        for attempted in [
            AgentState::Running,
            AgentState::Killed,
            AgentState::Failed,
            AgentState::Interrupted,
        ] {
            assert_eq!(
                store.update_task_state(task.id, attempted),
                Err(Refusal::TaskTerminal),
                "{attempted} must not resurrect a completed task"
            );
            let stored = store.get_task(task.id).expect("task");
            assert_eq!(stored.state, AgentState::Completed);
            assert_eq!(stored.rev, completed, "a refused write must not bump rev");
        }

        // Replaying the same terminal state is idempotent, not an error.
        assert_eq!(
            store.update_task_state(task.id, AgentState::Completed),
            Ok(completed)
        );
        assert_eq!(store.get_task(task.id).expect("task").rev, completed);
    }

    #[test]
    fn task_parent_row_follows_a_rewire() {
        let store = store();
        let task = seed_task(&store, AgentState::Running);
        let new_parent = TaskId::new();
        store
            .update_task_parent(task.id, Some(new_parent), 9)
            .expect("parent write");
        let stored = store.get_task(task.id).expect("task");
        assert_eq!(stored.parent_task_id, Some(new_parent));
        assert_eq!(stored.rev, 9, "row rev is pinned to the graph rev");
        assert_eq!(
            store.update_task_parent(TaskId::new(), None, 1),
            Err(Refusal::TaskNotFound)
        );
    }

    #[test]
    fn killed_task_stays_killed_after_a_late_completion() {
        let store = store();
        let task = seed_task(&store, AgentState::Running);
        store
            .update_task_state(task.id, AgentState::Killed)
            .expect("kill");
        assert_eq!(
            store.update_task_state(task.id, AgentState::Completed),
            Err(Refusal::TaskTerminal),
            "a late completion may not overwrite an acknowledged kill"
        );
        assert_eq!(
            store.get_task(task.id).expect("task").state,
            AgentState::Killed
        );
    }

    #[test]
    fn terminal_sql_list_matches_the_state_machine() {
        let list = terminal_task_state_sql();
        for terminal in TASK_TERMINAL_STATES {
            assert!(
                list.contains(&format!("'{}'", terminal.as_str())),
                "terminal set {list} is missing {}",
                terminal.as_str()
            );
        }
        for active in [
            AgentState::Queued,
            AgentState::Running,
            AgentState::AwaitingApproval,
        ] {
            assert!(
                !list.contains(&format!("'{}'", active.as_str())),
                "active state {active} must stay settlable"
            );
        }
    }

    #[test]
    fn unknown_task_update_is_refused_by_name() {
        let store = store();
        assert_eq!(
            store.update_task_state(TaskId::new(), AgentState::Running),
            Err(Refusal::TaskNotFound)
        );
    }

    #[test]
    fn agent_messages_are_durable_and_counted_per_run() {
        let store = store();
        let run = store
            .create_run("p", "message", AgentState::Running)
            .expect("run");
        let from = TaskId::new();
        let to = TaskId::new();
        let message_id = store
            .record_agent_message(run.id, from, to, "ask_parent", "hello")
            .expect("message");
        assert_ne!(message_id, Uuid::nil());
        assert_eq!(store.agent_message_count(run.id).expect("count"), 1);
        assert_eq!(store.agent_message_count(RunId::new()).expect("other"), 0);
    }

    #[test]
    fn open_duplicate_question_is_findable_until_answered() {
        let store = store();
        let run = store
            .create_run("p", "duplicate", AgentState::Running)
            .expect("run");
        let from = TaskId::new();
        let to = TaskId::new();
        let id = store
            .record_agent_message(run.id, from, to, "ask_parent", "same?")
            .expect("message");
        assert_eq!(
            store
                .find_open_agent_message(run.id, from, to, "ask_parent", "same?")
                .expect("find"),
            Some(id)
        );
        assert_eq!(
            store
                .find_open_agent_message(run.id, from, to, "ask_parent", "different?")
                .expect("find"),
            None
        );
        store
            .answer_agent_message(run.id, id, "operator", "answer")
            .expect("answer");
        assert_eq!(
            store
                .find_open_agent_message(run.id, from, to, "ask_parent", "same?")
                .expect("find"),
            None
        );
    }

    #[test]
    fn a2a_attempts_and_audit_are_recorded_once_per_transition() {
        let store = store();
        let run = store
            .create_run("p", "attempts", AgentState::Running)
            .expect("run");
        let from = TaskId::new();
        let to = TaskId::new();
        let id = store
            .record_agent_message(run.id, from, to, "ask_parent", "question")
            .expect("message");
        store
            .record_agent_message_attempt(run.id, id, "parent:test", "parent_answer", "{}", 2)
            .expect("first attempt");
        store
            .record_agent_message_attempt(run.id, id, "parent:test", "parent_answer", "{}", 2)
            .expect("second attempt");
        let message = store
            .agent_messages_for_run(run.id)
            .expect("messages")
            .into_iter()
            .find(|message| message.id == id.to_string())
            .expect("message row");
        assert_eq!(message.attempts, 2);
        assert!(message.last_attempt_at.is_some());
        let count = |store: &RunStore| -> i64 {
            store
                .db
                .with_connection(|connection| {
                    connection.query_row(
                        "SELECT COUNT(*) FROM a2a_audit WHERE run_id = ?1",
                        params![run.id.to_string()],
                        |row| row.get(0),
                    )
                })
                .expect("audit count")
        };
        assert_eq!(count(&store), 2);
        assert_eq!(
            store.record_agent_message_attempt(run.id, id, "parent:test", "parent_answer", "{}", 2),
            Err(Refusal::CallLimit),
            "the persisted attempt cap must refuse a third attempt"
        );
        store
            .answer_agent_message(run.id, id, "operator", "answer")
            .expect("answer");
        assert_eq!(count(&store), 3);
        assert_eq!(
            store.record_agent_message_attempt(run.id, id, "parent:test", "parent_answer", "{}", 2),
            Err(Refusal::TaskTerminal)
        );
        assert_eq!(
            store.answer_agent_message(run.id, id, "operator", "again"),
            Err(Refusal::TaskTerminal)
        );
    }

    #[test]
    fn agent_message_budget_is_atomic_and_named() {
        let store = store();
        let run = store
            .create_run("p", "message budget", AgentState::Running)
            .expect("run");
        let from = TaskId::new();
        let to = TaskId::new();
        store
            .record_agent_message_limited(run.id, from, to, "ask_parent", "one", 1)
            .expect("first message");
        assert_eq!(
            store.record_agent_message_limited(run.id, from, to, "ask_parent", "two", 1),
            Err(Refusal::CallLimit)
        );
        assert_eq!(store.agent_message_count(run.id).expect("count"), 1);
    }

    #[test]
    fn memory_trust_promotion_is_journaled() {
        let store = store();
        let run = store
            .create_run("p", "trust", AgentState::Running)
            .expect("run");
        let task = TaskId::new();
        store
            .record_agent_memory_scoped("worker", "claim", "value", run.id, task, &task.to_string())
            .expect("memory");
        let bank_id = format!("bank:branch:{task}");
        let entries = store.memory_entries_for_bank(&bank_id, 10).expect("entries");
        let entry_id = &entries[0].id;
        assert!(store
            .promote_memory_entry(run.id, &bank_id, entry_id, "verified", "operator")
            .expect("promote"));
        let entries = store.memory_entries_for_bank(&bank_id, 10).expect("entries");
        assert_eq!(entries[0].trust.as_deref(), Some("verified"));
        let journal = store.memory_journal_for_run(run.id, 10).expect("journal");
        assert!(journal.iter().any(|(action, _, _, _)| action == "trust_changed"));
        assert!(!store
            .promote_memory_entry(run.id, &bank_id, "missing", "verified", "operator")
            .expect("missing"));
    }

    #[test]
    fn pending_a2a_questions_expire_on_crash() {
        let store = store();
        let run = store
            .create_run("p", "pending", AgentState::Running)
            .expect("run");
        let from = TaskId::new();
        let to = TaskId::new();
        let id = store
            .record_agent_message(run.id, from, to, "ask_parent", "question")
            .expect("message");
        let pending = store.pending_agent_messages(10).expect("pending");
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].id, id.to_string());
        store.expire_open_agent_messages(run.id).expect("expire");
        assert!(store.pending_agent_messages(10).expect("pending").is_empty());
        assert_eq!(
            store.answer_agent_message(run.id, id, "operator", "late"),
            Err(Refusal::TaskTerminal)
        );
    }

    #[test]
    fn a2a_answer_is_cas_and_duplicate_is_refused() {
        let store = store();
        let run = store
            .create_run("p", "answer", AgentState::Running)
            .expect("run");
        let from = TaskId::new();
        let to = TaskId::new();
        let message_id = store
            .record_agent_message(run.id, from, to, "ask_parent", "question")
            .expect("message");
        store
            .answer_agent_message(run.id, message_id, "operator", "answer")
            .expect("first answer");
        assert_eq!(
            store.answer_agent_message(run.id, message_id, "operator", "again"),
            Err(Refusal::TaskTerminal)
        );
        let messages = store.agent_messages_for_run(run.id).expect("messages");
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].status, "answered");
        assert_eq!(messages[0].answer_text.as_deref(), Some("answer"));
        assert_eq!(messages[0].answer_actor.as_deref(), Some("operator"));
        assert_eq!(
            store.answer_agent_message(run.id, Uuid::now_v7(), "operator", "missing"),
            Err(Refusal::TaskNotFound)
        );
    }

    #[test]
    fn journal_verification_detects_truncation() {
        let store = store();
        let run = store
            .create_run("p", "truncate", AgentState::Running)
            .expect("run");
        let task = TaskId::new();
        store
            .record_agent_memory_scoped("worker", "one", "a", run.id, task, &task.to_string())
            .expect("first");
        store
            .record_agent_memory_scoped("worker", "two", "b", run.id, task, &task.to_string())
            .expect("second");
        store
            .db
            .with_connection(|connection| {
                connection.execute(
                    "DELETE FROM memory_journal WHERE seq = (
                        SELECT MAX(seq) FROM memory_journal WHERE run_id = ?1
                     )",
                    params![run.id.to_string()],
                )?;
                Ok(())
            })
            .expect("truncate");
        let verification = store.verify_memory_journal(run.id).expect("verify");
        assert!(!verification.valid, "a truncated chain must not verify");
    }

    #[test]
    fn run_accepts_only_one_durable_owner() {
        let store = store();
        let run = store
            .create_run("p", "one owner", AgentState::Running)
            .expect("run");
        let first = TaskRecord {
            id: TaskId::new(),
            run_id: run.id,
            parent_task_id: None,
            role: "planner".to_owned(),
            depth: 0,
            spawn_seq: 0,
            state: AgentState::Queued,
            rev: 0,
        };
        let second = TaskRecord {
            id: TaskId::new(),
            run_id: run.id,
            parent_task_id: None,
            role: "planner".to_owned(),
            depth: 0,
            spawn_seq: 1,
            state: AgentState::Queued,
            rev: 0,
        };
        store.insert_task(&first).expect("first owner");
        assert!(store.insert_task(&second).is_err(), "second owner must be refused");
    }

    #[test]
    fn task_ownership_is_persisted_by_class() {
        let store = store();
        let run = store
            .create_run("p", "ownership", AgentState::Running)
            .expect("run");
        let root = TaskRecord {
            id: TaskId::new(),
            run_id: run.id,
            parent_task_id: None,
            role: "planner".to_owned(),
            depth: 0,
            spawn_seq: 0,
            state: AgentState::Queued,
            rev: 0,
        };
        let super_agent = TaskRecord {
            id: TaskId::new(),
            run_id: run.id,
            parent_task_id: Some(root.id),
            role: "superagent".to_owned(),
            depth: 1,
            spawn_seq: 1,
            state: AgentState::Queued,
            rev: 0,
        };
        let agent = TaskRecord {
            id: TaskId::new(),
            run_id: run.id,
            parent_task_id: Some(super_agent.id),
            role: "agent".to_owned(),
            depth: 2,
            spawn_seq: 2,
            state: AgentState::Queued,
            rev: 0,
        };
        for task in [&root, &super_agent, &agent] {
            store.insert_task(task).expect("insert");
        }
        let root_ownership = store.task_ownership(root.id).expect("root").expect("row");
        assert_eq!(root_ownership.mode, "run");
        let expected_scope = format!("run:{}", run.id);
        assert_eq!(root_ownership.scope.as_deref(), Some(expected_scope.as_str()));
        let super_ownership = store
            .task_ownership(super_agent.id)
            .expect("super")
            .expect("row");
        assert_eq!(super_ownership.mode, "branch");
        let expected_owner = super_agent.id.to_string();
        assert_eq!(
            super_ownership.owner_task_id.as_deref(),
            Some(expected_owner.as_str())
        );
        assert_eq!(
            store.task_ownership(agent.id).expect("agent").expect("row").mode,
            "none"
        );
    }

    #[test]
    fn task_identity_is_persisted_by_the_v13_trigger() {
        let store = store();
        let run = store
            .create_run("p", "identity", AgentState::Running)
            .expect("run");
        let subagent = TaskRecord {
            id: TaskId::new(),
            run_id: run.id,
            parent_task_id: None,
            role: "sub-agent".to_owned(),
            depth: 2,
            spawn_seq: 1,
            state: AgentState::Queued,
            rev: 0,
        };
        let unknown = TaskRecord {
            id: TaskId::new(),
            run_id: run.id,
            parent_task_id: None,
            role: "researcher".to_owned(),
            depth: 1,
            spawn_seq: 2,
            state: AgentState::Queued,
            rev: 0,
        };
        store.insert_task(&subagent).expect("subagent");
        store.insert_task(&unknown).expect("unknown");
        let identity = store
            .task_identity(subagent.id)
            .expect("identity")
            .expect("resolved");
        assert_eq!(identity.class, "subagent");
        assert_eq!(identity.rank, 4);
        assert_eq!(identity.profile_id, "subagent");
        assert!(store.task_identity(unknown.id).expect("unknown").is_none());
    }

    #[test]
    fn memory_journal_is_hash_chained() {
        let store = store();
        let run = store
            .create_run("p", "chain", AgentState::Running)
            .expect("run");
        let task = TaskId::new();
        store
            .record_agent_memory_scoped("worker", "one", "a", run.id, task, &task.to_string())
            .expect("first");
        store
            .record_agent_memory_scoped("worker", "two", "b", run.id, task, &task.to_string())
            .expect("second");
        let chain = store.memory_journal_chain(run.id).expect("chain");
        assert_eq!(chain.len(), 2);
        assert!(chain[0].0.is_none());
        assert_eq!(chain[1].0.as_deref(), Some(chain[0].1.as_str()));
        assert_ne!(chain[0].1, chain[1].1);
        let verification = store.verify_memory_journal(run.id).expect("verify");
        assert!(verification.valid);
        assert_eq!(verification.checked, 2);
        assert_eq!(verification.unchained, 0);
    }

    #[test]
    fn prune_inactive_memory_never_touches_active_entries() {
        let store = store();
        let run = store
            .create_run("p", "prune", AgentState::Running)
            .expect("run");
        let task = TaskId::new();
        store
            .record_agent_memory_scoped("worker", "old", "a", run.id, task, &task.to_string())
            .expect("old");
        store
            .db
            .with_connection(|connection| {
                connection.execute(
                    "UPDATE memory_entries SET status = 'superseded' WHERE entry_key = 'old'",
                    [],
                )?;
                Ok(())
            })
            .expect("mark inactive");
        store
            .record_agent_memory_scoped("worker", "keep", "b", run.id, task, &task.to_string())
            .expect("keep");
        assert_eq!(store.prune_inactive_memory(run.id, true).expect("dry"), 1);
        let bank = format!("bank:branch:{task}");
        assert_eq!(store.memory_entries_for_bank(&bank, 10).expect("entries").len(), 1);
        assert_eq!(store.prune_inactive_memory(run.id, false).expect("prune"), 1);
        let entries = store.memory_entries_for_bank(&bank, 10).expect("entries");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].key, "keep");
    }

    #[test]
    fn journal_verification_detects_a_mutated_hash() {
        let store = store();
        let run = store
            .create_run("p", "tamper", AgentState::Running)
            .expect("run");
        let task = TaskId::new();
        store
            .record_agent_memory_scoped("worker", "one", "a", run.id, task, &task.to_string())
            .expect("first");
        store
            .record_agent_memory_scoped("worker", "two", "b", run.id, task, &task.to_string())
            .expect("second");
        store
            .db
            .with_connection(|connection| {
                connection.execute(
                    "UPDATE memory_journal SET event_hash = 'tampered'
                     WHERE seq = (SELECT MIN(seq) FROM memory_journal WHERE run_id = ?1)",
                    params![run.id.to_string()],
                )?;
                Ok(())
            })
            .expect("tamper");
        let verification = store.verify_memory_journal(run.id).expect("verify");
        assert!(!verification.valid);
    }

    #[test]
    fn memory_context_snapshot_records_bank_set() {
        let store = store();
        let run = store
            .create_run("p", "snapshot", AgentState::Running)
            .expect("run");
        let task = TaskId::new();
        store
            .record_memory_context_snapshot(
                task,
                run.id,
                &["bank:run:1".to_owned()],
                &["entry:1".to_owned()],
                42,
                false,
                "abc",
            )
            .expect("snapshot");
        let snapshot = store
            .memory_context_snapshot(task)
            .expect("read")
            .expect("snapshot");
        assert_eq!(snapshot.2, 42);
        assert!(!snapshot.3);
        assert_eq!(snapshot.4, "abc");
    }

    #[test]
    fn run_digest_is_an_engine_summary_in_the_run_bank() {
        let store = store();
        let run = store
            .create_run("p", "digest", AgentState::Running)
            .expect("run");
        let task = TaskId::new();
        store
            .write_run_digest(run.id, "Run digest: 1 task.")
            .expect("digest");
        let entries = store
            .memory_read_set(run.id, task, None, 16)
            .expect("read set");
        let digest = entries
            .iter()
            .find(|entry| entry.key == "run-digest")
            .expect("digest");
        assert_eq!(digest.value, "Run digest: 1 task.");
    }

    #[test]
    fn memory_read_set_excludes_sibling_branch() {
        let store = store();
        let run = store
            .create_run("p", "read-set", AgentState::Running)
            .expect("run");
        let parent = TaskId::new();
        let child = TaskId::new();
        let sibling = TaskId::new();
        store
            .record_agent_memory_scoped("worker", "parent", "visible", run.id, parent, &parent.to_string())
            .expect("parent memory");
        store
            .record_agent_memory_scoped("worker", "sibling", "hidden", run.id, sibling, &sibling.to_string())
            .expect("sibling memory");
        store
            .record_agent_memory_scoped("engine", "run", "run-visible", run.id, parent, "run")
            .expect("run memory");
        let entries = store
            .memory_read_set(run.id, child, Some(parent), 16)
            .expect("read set");
        assert!(entries.iter().any(|entry| entry.key == "parent"));
        assert!(entries.iter().any(|entry| entry.key == "run"));
        assert!(!entries.iter().any(|entry| entry.key == "sibling"));
    }

    #[test]
    fn agent_memory_dual_writes_branch_bank() {
        let store = store();
        let run = store
            .create_run("p", "banks", AgentState::Running)
            .expect("run");
        let task = TaskId::new();
        store
            .record_agent_memory_scoped("worker", "note", "banked", run.id, task, &task.to_string())
            .expect("memory");
        let banks = store.memory_banks_for_run(run.id).expect("banks");
        assert!(banks.iter().any(|bank| bank.kind == "run"));
        assert!(banks
            .iter()
            .any(|bank| bank.kind == "branch" && bank.owner_task_id.as_deref() == Some(&task.to_string())));
    }

    #[test]
    fn agent_memory_is_durable_and_role_scoped() {
        let store = store();
        let run = store
            .create_run("p", "memory", AgentState::Running)
            .expect("run");
        let task = TaskId::new();
        store
            .record_agent_memory("worker", "task:1", "remembered", run.id, task)
            .expect("memory");
        store
            .record_agent_memory("worker", "task:1", "updated", run.id, task)
            .expect("upsert");
        let memories = store.agent_memory("worker", 8).expect("recall");
        assert_eq!(memories, vec![("task:1".to_owned(), "updated".to_owned())]);
        assert!(store.agent_memory("other", 8).expect("other").is_empty());
    }

    #[test]
    fn agent_memory_is_run_scoped() {
        let store = store();
        let first = store
            .create_run("p", "first", AgentState::Running)
            .expect("first run");
        let second = store
            .create_run("p", "second", AgentState::Running)
            .expect("second run");
        store
            .record_agent_memory("worker", "task:1", "from-first", first.id, TaskId::new())
            .expect("first memory");
        store
            .record_agent_memory("worker", "task:1", "from-second", second.id, TaskId::new())
            .expect("second memory");
        assert_eq!(
            store
                .agent_memory_for_run(first.id, "worker", 8)
                .expect("first recall"),
            vec![("task:1".to_owned(), "from-first".to_owned())]
        );
        assert_eq!(
            store
                .agent_memory_for_run(second.id, "worker", 8)
                .expect("second recall"),
            vec![("task:1".to_owned(), "from-second".to_owned())]
        );
    }

    #[test]
    fn agent_memory_is_branch_scoped() {
        let store = store();
        let run = store
            .create_run("p", "branch", AgentState::Running)
            .expect("run");
        store
            .record_agent_memory_scoped("worker", "task:1", "hive-a", run.id, TaskId::new(), "hive-a")
            .expect("hive a");
        store
            .record_agent_memory_scoped("worker", "task:1", "hive-b", run.id, TaskId::new(), "hive-b")
            .expect("hive b");
        assert_eq!(
            store
                .agent_memory_for_scope(run.id, "hive-a", "worker", 8)
                .expect("hive a recall"),
            vec![("task:1".to_owned(), "hive-a".to_owned())]
        );
        assert_eq!(
            store
                .agent_memory_for_scope(run.id, "hive-b", "worker", 8)
                .expect("hive b recall"),
            vec![("task:1".to_owned(), "hive-b".to_owned())]
        );
    }

    #[test]
    fn create_run_materializes_its_project_once() {
        let store = store();
        store
            .create_run("project-a", "first", AgentState::Running)
            .expect("first run");
        store
            .create_run("project-a", "second", AgentState::Running)
            .expect("second run");
        store
            .create_run("project-b", "third", AgentState::Running)
            .expect("third run");
        let projects = store.projects().expect("projects");
        assert_eq!(projects.len(), 2);
        assert!(projects.iter().any(|(id, _, _)| id == "project-a"));
        assert!(projects.iter().any(|(id, _, _)| id == "project-b"));
    }

    #[test]
    fn run_usage_persists_nulls_not_zero() {
        let store = store();
        let run = store
            .create_run("p", "usage", AgentState::Running)
            .expect("run");
        let usage = Usage {
            prompt_tokens: Some(10),
            completion_tokens: None,
            total_tokens: Some(10),
            ..Usage::default()
        };
        store.record_run_usage(run.id, &usage).expect("usage");
        let reread = store.run_usage(run.id).expect("read").expect("row");
        assert_eq!(reread.prompt_tokens, Some(10));
        assert_eq!(reread.completion_tokens, None);
        assert_eq!(reread.total_tokens, Some(10));
    }
}
