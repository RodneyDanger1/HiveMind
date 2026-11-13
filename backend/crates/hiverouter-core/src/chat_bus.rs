//! Per-tree chat bus — append-only, visibility-scoped messaging between the
//! tasks of one run.
//!
//! HiveMind already has a flat, run-wide agent-to-agent mailbox
//! (`store.record_agent_message` / `agent_messages_for_task`) whose rows carry
//! no tree, visibility, sequence or idempotency key. That mailbox lets a task
//! message any task in the run. This module is the stricter transport the tree
//! hierarchy needs:
//!
//! - one bus per process, one sequence per **tree**;
//! - a tree is identified by `run_id + tree_id`, where `tree_id` is the root
//!   task id of that tree (the run root for the run's main tree);
//! - membership is declared through [`ChatBus::open_tree`] and
//!   [`ChatBus::join_tree`] and is the authority for every read and write — a
//!   task can only post into a tree it belongs to, and only a member can read;
//! - each message records sender, recipient, kind, sequence, idempotency key,
//!   bounded body and visibility;
//! - every accepted mutation is appended to a [`ChatJournal`] first, so a
//!   restart replays exactly the messages that were acknowledged, with the same
//!   sequences. There is no update and no delete API: the journal is
//!   append-only.
//!
//! ## Refusals (named, never silent)
//!
//! | condition | refusal |
//! |---|---|
//! | unknown run tree, unknown task, task not a member of this tree | [`Refusal::TaskNotFound`] |
//! | empty/blank body, body over [`MAX_BODY_CHARS`], blank/oversized idempotency key, key reuse with a different payload, task joined into a second tree of the same run, join whose parent is not a member, conflicting re-open | [`Refusal::SchemaInvalid`] |
//! | observer is neither the tree root nor the run root | [`Refusal::ToolNotAllowed`] |
//! | poisoned bus lock, journal I/O failure, replay invariant failure at write time | [`Refusal::InternalError`] |
//! | a persisted journal that does not replay consistently | [`Refusal::SnapshotCorrupt`] |
//!
//! ## Visibility
//!
//! - [`Visibility::Task`] — sender and recipient only.
//! - [`Visibility::ParentChain`] — sender, recipient, and every ancestor of
//!   either within the tree. This is what makes "a parent reads its
//!   descendants": a descendant's question/answer/result is visible to each of
//!   its ancestors, but never to a sibling branch.
//! - [`Visibility::RootObserver`] — sender, recipient and the tree root.
//!   [`ChatBus::observe_tree`] additionally gives the tree root **and the run
//!   root** the complete audit view, including `Task`-visibility traffic.
//!
//! Reads are scoped by a limit (`1..=`[`MAX_INBOX_LIMIT`]); the newest matching
//! messages are returned in ascending sequence order so a prompt can append
//! them without re-sorting.
//!
//! ## Exact wiring into `execution.rs`
//!
//! The wiring is documented here and **not applied** because this change is
//! scoped to `chat_bus.rs` plus `tests/chat_bus_*` only. The steps below are
//! the complete integration; each is a real call into this module, not a stub.
//!
//! 1. `src/lib.rs` — register the module next to `pub mod checkpoint;`:
//!    ```ignore
//!    pub mod chat_bus;
//!    pub use chat_bus::{
//!        ChatBus, ChatJournal, ChatJournalRecord, ChatKind, ChatMessage, FileJournal,
//!        MemoryJournal, PostOutcome, PostRequest, TreeKey, TreeSummary, Visibility,
//!    };
//!    ```
//!
//! 2. `RunEngine` (`execution.rs:945-1037`) — own one bus per engine. Add the
//!    field after `a2a_delivered` (`execution.rs:1011`):
//!    ```ignore
//!    /// Per-tree chat bus: authoritative sender/recipient/visibility/sequence.
//!    pub chat: Arc<crate::chat_bus::ChatBus>,
//!    ```
//!
//! 3. Both constructors — `RunEngine::new` (`execution.rs:1054-1098`) and
//!    `RunEngine::with_services` (`execution.rs:1346-1393`) — initialize the
//!    field:
//!    ```ignore
//!    chat: Arc::new(crate::chat_bus::ChatBus::in_memory()),
//!    ```
//!    The production boot path (`main.rs`/`daemon.rs`) opens the durable
//!    variant instead so a process restart keeps acknowledged messages:
//!    ```ignore
//!    let chat = Arc::new(crate::chat_bus::ChatBus::open_file(
//!        data_dir.join("chat_bus.jsonl"),
//!    )?);
//!    engine = engine.with_chat_bus(chat);
//!    ```
//!    with the obvious builder
//!    `pub fn with_chat_bus(mut self, chat: Arc<ChatBus>) -> Self { self.chat = chat; self }`
//!    on `RunEngine`.
//!
//! 4. Root tree creation — `RunEngine::dispatch`, after the root task row and
//!    graph node exist (`execution.rs:2331-2346`, immediately after
//!    `self.graph.add_task(root.id, None)?;`):
//!    ```ignore
//!    self.chat.open_tree(run.id, root.id)?;
//!    ```
//!    `open_tree` is idempotent, so a retried dispatch does not append twice.
//!
//! 5. Child membership — `RunEngine::spawn_sub_hive`, after the graph node is
//!    linked (`execution.rs:1991-1999`, immediately after
//!    `self.graph.set_metadata(...)` and before `self.seed_task_context(...)`):
//!    ```ignore
//!    let tree = self.chat.tree_key_for(task.run_id)?;
//!    self.chat
//!        .join_tree(tree.run_id, tree.tree_id, task.id, parent_task_id)?;
//!    ```
//!    The same two lines go in the template-task insertion loop after each
//!    `self.store.insert_task(&record)?` (`execution.rs:2436`).
//!
//! 6. Reading a task's chat — `RunEngine::seed_task_context`, next to the A2A
//!    inbox block (`execution.rs:1701-1717`), so chat messages are model-visible
//!    facts under a distinct `chat:` prefix (never instructions):
//!    ```ignore
//!    if let Ok(tree) = self.chat.tree_key_for(task.run_id) {
//!        if let Ok(messages) = self.chat.inbox(task.run_id, tree.tree_id, task.id, 8) {
//!            for message in messages {
//!                context.facts.insert(
//!                    format!(
//!                        "chat:{}:{}:{}",
//!                        message.from_task_id,
//!                        message.kind.as_str(),
//!                        message.seq
//!                    ),
//!                    message.body.chars().take(500).collect(),
//!                );
//!            }
//!        }
//!    }
//!    ```
//!    `refresh_a2a_inbox` (`execution.rs:4910-4940`) and
//!    `a2a_updates_for_task` (`execution.rs:4878-4908`) append the same facts so
//!    a long-running task sees chat that arrived after its context was seeded.
//!
//! 7. Writing chat — every existing `store.record_agent_message(...)` call site
//!    (`execution.rs:4370-4440` for sibling/parent messages, the evolution vote
//!    path) gains a `chat.post(...)` call keyed by the durable message id so a
//!    retry is single-effect:
//!    ```ignore
//!    self.chat.post(PostRequest {
//!        run_id: task.run_id,
//!        tree_id: tree.tree_id,
//!        from_task_id: task.id,
//!        to_task_id: recipient,
//!        kind: ChatKind::Question,          // or Brief/Answer/Status/Escalation/Result
//!        body: bounded_a2a_text(&text).as_deref().unwrap_or_default(),
//!        idempotency_key: &format!("a2a:{message_id}"),
//!        visibility: Visibility::ParentChain,
//!    })?;
//!    ```
//!    A `Result`/`Status` posted when a task reaches a terminal state uses
//!    `to_task_id: tree.tree_id` and `visibility: Visibility::RootObserver`.
//!
//! 8. Root audit/observer surface — the UI read path (`http.rs` history/status
//!    handlers) calls
//!    `chat.observe_tree(run_id, tree_id, run_root_task_id, limit)`; WS replay
//!    (`ws.rs:712-744`) can publish the same rows as `AGENT_MESSAGE` frames.
//!    Neither surface may read a tree whose observer is not the tree root or the
//!    run root: the bus refuses with `E_TOOL_NOT_ALLOWED`.
//!
//! ## Invariants pinned by `tests/chat_bus_contract.rs`
//!
//! 1. Sequence numbers are per tree, start at 1 and never have gaps or reuse.
//! 2. A post is accepted only when sender and recipient are members of the same
//!    `(run_id, tree_id)`; cross-tree posts and unknown trees are refused.
//! 3. A `ParentChain` message is readable by every ancestor of sender or
//!    recipient, and by nobody in a sibling branch.
//! 4. The tree root and the run root observe every message in the tree through
//!    `observe_tree`; no other task does.
//! 5. An `idempotency_key` appends at most once per tree; a replay returns the
//!    original message and a payload mismatch is refused.
//! 6. A body over [`MAX_BODY_CHARS`] is refused without consuming a sequence.
//! 7. A reopened [`FileJournal`] restores every acknowledged message and
//!    continues the sequence; a corrupt journal is refused by name.

use std::collections::HashMap;
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};

use hiverouter_contract::{Refusal, RunId, TaskId, Timestamp};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Maximum body length accepted by [`ChatBus::post`], counted in Unicode
/// scalar values (not bytes).
///
/// The flat A2A mailbox caps one message at 2,000 characters. The tree bus
/// carries `brief`/`result` summaries as well, so it allows more, but the bound
/// is still hard: an oversized body is refused by name and nothing is appended.
pub const MAX_BODY_CHARS: usize = 4_000;

/// Maximum length of an idempotency key, counted in Unicode scalar values.
pub const MAX_IDEMPOTENCY_KEY_CHARS: usize = 128;

/// Hard upper bound on rows returned by [`ChatBus::inbox`] and
/// [`ChatBus::observe_tree`].
pub const MAX_INBOX_LIMIT: usize = 64;

/// Message kind. The wire form is the `snake_case` string, matching the
/// existing A2A `kind` field convention.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChatKind {
    /// Short orientation note ("I am starting on X").
    Brief,
    /// A question addressed to a parent or peer.
    Question,
    /// The answer to a [`ChatKind::Question`].
    Answer,
    /// Progress/state note.
    Status,
    /// Something the recipient must decide or act on.
    Escalation,
    /// Final outcome of a unit of work.
    Result,
}

impl ChatKind {
    /// Stable wire string.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            ChatKind::Brief => "brief",
            ChatKind::Question => "question",
            ChatKind::Answer => "answer",
            ChatKind::Status => "status",
            ChatKind::Escalation => "escalation",
            ChatKind::Result => "result",
        }
    }

    /// All kinds, for censuses and prompt rosters.
    #[must_use]
    pub fn all() -> &'static [ChatKind] {
        &[
            ChatKind::Brief,
            ChatKind::Question,
            ChatKind::Answer,
            ChatKind::Status,
            ChatKind::Escalation,
            ChatKind::Result,
        ]
    }

    /// Parses the stable wire string. An unknown string is `None` — it is never
    /// guessed into a kind.
    #[must_use]
    pub fn from_str(value: &str) -> Option<Self> {
        match value {
            "brief" => Some(ChatKind::Brief),
            "question" => Some(ChatKind::Question),
            "answer" => Some(ChatKind::Answer),
            "status" => Some(ChatKind::Status),
            "escalation" => Some(ChatKind::Escalation),
            "result" => Some(ChatKind::Result),
            _ => None,
        }
    }
}

/// Who may read a chat message besides its sender and recipient.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Visibility {
    /// Sender and recipient only.
    Task,
    /// Sender, recipient and every ancestor of either inside the tree.
    ParentChain,
    /// Sender, recipient and the tree root.
    RootObserver,
}

/// One persisted chat message. Positions in the tree are fixed by `seq`; the
/// record is immutable once journaled.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChatMessage {
    /// Owning run.
    pub run_id: RunId,
    /// Tree root task id.
    pub tree_id: TaskId,
    /// Per-tree monotonic sequence, starting at 1. Never reused.
    pub seq: u64,
    /// Stable message id (uuid-v7). A retry with the same idempotency key returns
    /// the same id.
    pub message_id: Uuid,
    /// Sender task.
    pub from_task_id: TaskId,
    /// Recipient task.
    pub to_task_id: TaskId,
    /// Message kind.
    pub kind: ChatKind,
    /// Bounded body, stored verbatim.
    pub body: String,
    /// Idempotency key, unique within `(run_id, tree_id)`.
    pub idempotency_key: String,
    /// Read scope.
    pub visibility: Visibility,
    /// RFC3339 creation time.
    pub created_at: Timestamp,
}

/// Identifier of one tree: a run plus its root task.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct TreeKey {
    /// Owning run.
    pub run_id: RunId,
    /// Root task id of the tree.
    pub tree_id: TaskId,
}

/// Everything needed to append one message.
#[derive(Clone, Copy, Debug)]
pub struct PostRequest<'a> {
    /// Owning run.
    pub run_id: RunId,
    /// Tree root task id.
    pub tree_id: TaskId,
    /// Sender task; must be a member of the tree.
    pub from_task_id: TaskId,
    /// Recipient task; must be a member of the same tree.
    pub to_task_id: TaskId,
    /// Message kind.
    pub kind: ChatKind,
    /// Body; empty/blank or longer than [`MAX_BODY_CHARS`] is refused.
    pub body: &'a str,
    /// Idempotency key; blank or longer than
    /// [`MAX_IDEMPOTENCY_KEY_CHARS`] is refused.
    pub idempotency_key: &'a str,
    /// Read scope for the appended message.
    pub visibility: Visibility,
}

/// Result of [`ChatBus::post`]. A duplicate is success, not an error: the
/// caller's retry is acknowledged with the already-committed message and no
/// second row is appended.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PostOutcome {
    /// A new message was appended.
    Appended(ChatMessage),
    /// The idempotency key was already committed with the same payload; the
    /// original message is returned and nothing was appended.
    Duplicate(ChatMessage),
}

impl PostOutcome {
    /// The committed message, whether newly appended or deduplicated.
    #[must_use]
    pub fn message(&self) -> &ChatMessage {
        match self {
            PostOutcome::Appended(message) | PostOutcome::Duplicate(message) => message,
        }
    }

    /// Whether this call appended a new row.
    #[must_use]
    pub fn appended(&self) -> bool {
        matches!(self, PostOutcome::Appended(_))
    }
}

/// Read-only shape of one tree, used by wiring and operator views.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TreeSummary {
    /// Tree identity.
    pub key: TreeKey,
    /// The run's root task at the time the summary was read.
    pub run_root: TaskId,
    /// Number of member tasks.
    pub member_count: usize,
    /// Number of appended messages.
    pub message_count: usize,
    /// Sequence of the newest message (`0` when the tree has none).
    pub last_seq: u64,
}

/// One appended journal record. The tagged JSON line is the durable format; it
/// is only ever appended, never rewritten.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "record", rename_all = "snake_case")]
pub enum ChatJournalRecord {
    /// A tree was opened.
    OpenTree {
        /// Owning run.
        run_id: RunId,
        /// Tree root task id.
        tree_id: TaskId,
    },
    /// A task joined an existing tree.
    JoinTree {
        /// Owning run.
        run_id: RunId,
        /// Tree root task id.
        tree_id: TaskId,
        /// Joining task.
        task_id: TaskId,
        /// Its parent in the same tree.
        parent_task_id: TaskId,
    },
    /// A chat message was committed.
    Post {
        /// The committed message, with its final sequence.
        message: ChatMessage,
    },
}

/// Append-only persistence boundary for the chat bus.
///
/// Production uses [`FileJournal`]; [`MemoryJournal`] is the process-lifetime
/// variant and is also the test double for persistence. Implementations must be
/// append-only: [`ChatJournal::replay`] returns every accepted record in append
/// order, and a record is only acknowledged after [`ChatJournal::append`]
/// returns `Ok`.
pub trait ChatJournal: fmt::Debug + Send + Sync {
    /// Appends one record. Returning `Err` means the record was not committed;
    /// the bus then leaves its in-memory state untouched and does not consume a
    /// sequence.
    ///
    /// # Errors
    /// The implementation's named refusal (`E_INTERNAL_ERROR` for I/O).
    fn append(&self, record: &ChatJournalRecord) -> Result<(), Refusal>;

    /// Returns every committed record in append order.
    ///
    /// # Errors
    /// `E_SNAPSHOT_CORRUPT` when a persisted record cannot be parsed or violates
    /// an ordering invariant; `E_INTERNAL_ERROR` for I/O.
    fn replay(&self) -> Result<Vec<ChatJournalRecord>, Refusal>;
}

/// In-process append-only journal. Records live for the process lifetime and
/// survive exactly as long as the [`ChatBus`] that owns them.
#[derive(Debug, Default)]
pub struct MemoryJournal {
    records: Mutex<Vec<ChatJournalRecord>>,
}

impl MemoryJournal {
    /// Creates an empty journal.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of appended records.
    ///
    /// # Errors
    /// `E_INTERNAL_ERROR` when the journal lock is poisoned.
    pub fn len(&self) -> Result<usize, Refusal> {
        Ok(self
            .records
            .lock()
            .map_err(|_| Refusal::InternalError)?
            .len())
    }

    /// Whether nothing has been appended.
    ///
    /// # Errors
    /// `E_INTERNAL_ERROR` when the journal lock is poisoned.
    pub fn is_empty(&self) -> Result<bool, Refusal> {
        Ok(self.len()? == 0)
    }

    /// Clones the committed records in append order.
    ///
    /// # Errors
    /// `E_INTERNAL_ERROR` when the journal lock is poisoned.
    pub fn records(&self) -> Result<Vec<ChatJournalRecord>, Refusal> {
        Ok(self
            .records
            .lock()
            .map_err(|_| Refusal::InternalError)?
            .clone())
    }
}

impl ChatJournal for MemoryJournal {
    fn append(&self, record: &ChatJournalRecord) -> Result<(), Refusal> {
        self.records
            .lock()
            .map_err(|_| Refusal::InternalError)?
            .push(record.clone());
        Ok(())
    }

    fn replay(&self) -> Result<Vec<ChatJournalRecord>, Refusal> {
        self.records()
    }
}

/// Durable append-only journal: one JSON record per line, flushed and synced
/// before a post is acknowledged.
///
/// A crash can leave a torn final line; that is not silently ignored — replay
/// refuses the whole journal with `E_SNAPSHOT_CORRUPT` so an operator repairs or
/// restores the file instead of the bus pretending a message exists or does not
/// exist. The file is opened for append only; no API truncates or rewrites it.
#[derive(Debug)]
pub struct FileJournal {
    path: PathBuf,
    file: Mutex<File>,
}

impl FileJournal {
    /// Opens (creating if needed) the journal at `path` for appending.
    ///
    /// # Errors
    /// `E_INTERNAL_ERROR` when the parent directory or the file cannot be
    /// created, or the append handle cannot be opened.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, Refusal> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent).map_err(|_| Refusal::InternalError)?;
            }
        }
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|_| Refusal::InternalError)?;
        Ok(Self {
            path,
            file: Mutex::new(file),
        })
    }
}

impl ChatJournal for FileJournal {
    fn append(&self, record: &ChatJournalRecord) -> Result<(), Refusal> {
        let mut line = serde_json::to_string(record).map_err(|_| Refusal::InternalError)?;
        line.push('\n');
        let mut file = self.file.lock().map_err(|_| Refusal::InternalError)?;
        file.write_all(line.as_bytes())
            .map_err(|_| Refusal::InternalError)?;
        file.flush().map_err(|_| Refusal::InternalError)?;
        file.sync_data().map_err(|_| Refusal::InternalError)?;
        Ok(())
    }

    fn replay(&self) -> Result<Vec<ChatJournalRecord>, Refusal> {
        let bytes = match std::fs::read(&self.path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(_) => return Err(Refusal::InternalError),
        };
        let text = String::from_utf8(bytes).map_err(|_| Refusal::SnapshotCorrupt)?;
        let mut records = Vec::new();
        for line in text.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let record: ChatJournalRecord =
                serde_json::from_str(line).map_err(|_| Refusal::SnapshotCorrupt)?;
            records.push(record);
        }
        Ok(records)
    }
}

#[derive(Debug, Default)]
struct BusState {
    run_root: HashMap<RunId, TaskId>,
    trees: HashMap<TreeKey, TreeState>,
}

#[derive(Debug)]
struct TreeState {
    root: TaskId,
    parents: HashMap<TaskId, Option<TaskId>>,
    messages: Vec<ChatMessage>,
    by_key: HashMap<String, usize>,
    next_seq: u64,
}

impl TreeState {
    fn new(root: TaskId) -> Self {
        let mut parents = HashMap::new();
        parents.insert(root, None);
        Self {
            root,
            parents,
            messages: Vec::new(),
            by_key: HashMap::new(),
            next_seq: 1,
        }
    }
}

/// The per-tree chat bus. One instance per server process; every mutation is
/// serialized by one mutex and journaled before it is visible.
#[derive(Debug)]
pub struct ChatBus {
    journal: Arc<dyn ChatJournal>,
    state: Mutex<BusState>,
}

impl ChatBus {
    /// Opens a bus backed by `journal`, replaying every committed record.
    ///
    /// # Errors
    /// Propagates the journal's replay refusal; a journal that does not replay
    /// consistently is refused with `E_SNAPSHOT_CORRUPT` and no partial tree is
    /// installed.
    pub fn open(journal: Arc<dyn ChatJournal>) -> Result<Self, Refusal> {
        let records = journal.replay()?;
        let mut state = BusState::default();
        for record in &records {
            apply_record(&mut state, record)?;
        }
        Ok(Self {
            journal,
            state: Mutex::new(state),
        })
    }

    /// Opens a process-lifetime bus (used by tests and by an engine that has not
    /// wired durable storage yet).
    #[must_use]
    pub fn in_memory() -> Self {
        Self {
            journal: Arc::new(MemoryJournal::new()),
            state: Mutex::new(BusState::default()),
        }
    }

    /// Opens a durable bus over a JSONL journal. A missing file is an empty
    /// journal; an existing file is replayed before any new post is accepted.
    ///
    /// # Errors
    /// `E_INTERNAL_ERROR` for an unopenable path; `E_SNAPSHOT_CORRUPT` for an
    /// unreadable record.
    pub fn open_file(path: impl AsRef<Path>) -> Result<Self, Refusal> {
        Self::open(Arc::new(FileJournal::open(path)?))
    }

    fn lock(&self) -> Result<MutexGuard<'_, BusState>, Refusal> {
        self.state.lock().map_err(|_| Refusal::InternalError)
    }

    /// Opens a tree rooted at `tree_id`. Idempotent: re-opening the same
    /// `(run_id, tree_id)` returns the same key without appending again.
    ///
    /// The first tree opened for a run defines that run's root observer.
    ///
    /// # Errors
    /// `E_INTERNAL_ERROR` when the lock is poisoned or the journal append
    /// fails; `E_SCHEMA_INVALID` when the replayed record contradicts the live
    /// tree (never produced by this API, surfaced instead of ignored).
    pub fn open_tree(&self, run_id: RunId, tree_id: TaskId) -> Result<TreeKey, Refusal> {
        let key = TreeKey { run_id, tree_id };
        let mut state = self.lock()?;
        if state.trees.contains_key(&key) {
            return Ok(key);
        }
        let record = ChatJournalRecord::OpenTree { run_id, tree_id };
        self.journal.append(&record)?;
        apply_record(&mut state, &record)?;
        Ok(key)
    }

    /// Adds `task_id` to an existing tree under `parent_task_id`.
    ///
    /// Idempotent for the same `(tree, task, parent)`; a task may belong to at
    /// most one tree per run.
    ///
    /// # Errors
    /// `E_TASK_NOT_FOUND` when the tree or the parent is unknown;
    /// `E_SCHEMA_INVALID` when the task is already a member with a different
    /// parent or already belongs to another tree of the same run;
    /// `E_INTERNAL_ERROR` when the lock or journal fails.
    pub fn join_tree(
        &self,
        run_id: RunId,
        tree_id: TaskId,
        task_id: TaskId,
        parent_task_id: TaskId,
    ) -> Result<(), Refusal> {
        let key = TreeKey { run_id, tree_id };
        let mut state = self.lock()?;
        {
            let tree = state.trees.get(&key).ok_or(Refusal::TaskNotFound)?;
            if let Some(existing) = tree.parents.get(&task_id) {
                return if *existing == Some(parent_task_id) {
                    Ok(())
                } else {
                    Err(Refusal::SchemaInvalid)
                };
            }
            if !tree.parents.contains_key(&parent_task_id) {
                return Err(Refusal::TaskNotFound);
            }
        }
        if state.trees.iter().any(|(other, tree)| {
            other.run_id == run_id
                && other.tree_id != tree_id
                && tree.parents.contains_key(&task_id)
        }) {
            return Err(Refusal::SchemaInvalid);
        }
        let record = ChatJournalRecord::JoinTree {
            run_id,
            tree_id,
            task_id,
            parent_task_id,
        };
        self.journal.append(&record)?;
        apply_record(&mut state, &record)?;
        Ok(())
    }

    /// Appends one message after validating the tree, membership, body cap and
    /// idempotency key.
    ///
    /// # Errors
    /// `E_TASK_NOT_FOUND` when the tree, sender or recipient is not a member of
    /// this tree (this is the same-tree gate); `E_SCHEMA_INVALID` for an
    /// invalid/oversized body or key, or a key reused with a different payload;
    /// `E_INTERNAL_ERROR` when the lock or journal append fails.
    pub fn post(&self, request: PostRequest<'_>) -> Result<PostOutcome, Refusal> {
        let body_chars = request.body.chars().count();
        if body_chars == 0 || request.body.trim().is_empty() || body_chars > MAX_BODY_CHARS {
            return Err(Refusal::SchemaInvalid);
        }
        if request.idempotency_key.trim().is_empty()
            || request.idempotency_key.chars().count() > MAX_IDEMPOTENCY_KEY_CHARS
        {
            return Err(Refusal::SchemaInvalid);
        }

        let key = TreeKey {
            run_id: request.run_id,
            tree_id: request.tree_id,
        };
        let mut state = self.lock()?;
        {
            let tree = state.trees.get(&key).ok_or(Refusal::TaskNotFound)?;
            if !tree.parents.contains_key(&request.from_task_id)
                || !tree.parents.contains_key(&request.to_task_id)
            {
                return Err(Refusal::TaskNotFound);
            }
            if let Some(index) = tree.by_key.get(request.idempotency_key) {
                let existing = tree.messages.get(*index).ok_or(Refusal::InternalError)?;
                let same_payload = existing.from_task_id == request.from_task_id
                    && existing.to_task_id == request.to_task_id
                    && existing.kind == request.kind
                    && existing.visibility == request.visibility
                    && existing.body == request.body;
                return if same_payload {
                    Ok(PostOutcome::Duplicate(existing.clone()))
                } else {
                    Err(Refusal::SchemaInvalid)
                };
            }
        }

        let next_seq = state
            .trees
            .get(&key)
            .map(|tree| tree.next_seq)
            .ok_or(Refusal::TaskNotFound)?;
        let message = ChatMessage {
            run_id: request.run_id,
            tree_id: request.tree_id,
            seq: next_seq,
            message_id: Uuid::now_v7(),
            from_task_id: request.from_task_id,
            to_task_id: request.to_task_id,
            kind: request.kind,
            body: request.body.to_owned(),
            idempotency_key: request.idempotency_key.to_owned(),
            visibility: request.visibility,
            created_at: Timestamp::now(),
        };
        let record = ChatJournalRecord::Post {
            message: message.clone(),
        };
        self.journal.append(&record)?;
        let tree = state.trees.get_mut(&key).ok_or(Refusal::InternalError)?;
        tree.messages.push(message.clone());
        tree.by_key.insert(
            request.idempotency_key.to_owned(),
            tree.messages.len().saturating_sub(1),
        );
        tree.next_seq = tree.next_seq.saturating_add(1);
        Ok(PostOutcome::Appended(message))
    }

    /// Returns the newest messages visible to `viewer` in this tree, oldest
    /// first.
    ///
    /// `viewer` must be a member of the tree. `Task`-visibility traffic is not
    /// shown to anyone except its sender and recipient; the root's full audit
    /// view is [`ChatBus::observe_tree`].
    ///
    /// # Errors
    /// `E_TASK_NOT_FOUND` for an unknown tree or a viewer that is not a member;
    /// `E_INTERNAL_ERROR` when the lock is poisoned.
    pub fn inbox(
        &self,
        run_id: RunId,
        tree_id: TaskId,
        viewer: TaskId,
        limit: usize,
    ) -> Result<Vec<ChatMessage>, Refusal> {
        let state = self.lock()?;
        let key = TreeKey { run_id, tree_id };
        let tree = state.trees.get(&key).ok_or(Refusal::TaskNotFound)?;
        if !tree.parents.contains_key(&viewer) {
            return Err(Refusal::TaskNotFound);
        }
        let limit = limit.clamp(1, MAX_INBOX_LIMIT);
        let mut visible: Vec<ChatMessage> = tree
            .messages
            .iter()
            .filter(|message| sees(tree, message, viewer))
            .cloned()
            .collect();
        if visible.len() > limit {
            visible.drain(0..visible.len() - limit);
        }
        Ok(visible)
    }

    /// Full audit view of one tree for its root observer: the tree root or the
    /// run root. Unlike [`ChatBus::inbox`], this includes `Task`-visibility
    /// traffic.
    ///
    /// # Errors
    /// `E_TASK_NOT_FOUND` for an unknown tree; `E_TOOL_NOT_ALLOWED` when the
    /// observer is neither the tree root nor the run root; `E_INTERNAL_ERROR`
    /// when the lock is poisoned.
    pub fn observe_tree(
        &self,
        run_id: RunId,
        tree_id: TaskId,
        observer: TaskId,
        limit: usize,
    ) -> Result<Vec<ChatMessage>, Refusal> {
        let state = self.lock()?;
        let key = TreeKey { run_id, tree_id };
        let tree = state.trees.get(&key).ok_or(Refusal::TaskNotFound)?;
        let is_run_root = state
            .run_root
            .get(&run_id)
            .is_some_and(|root| *root == observer);
        if observer != tree.root && !is_run_root {
            return Err(Refusal::ToolNotAllowed);
        }
        let limit = limit.clamp(1, MAX_INBOX_LIMIT);
        let start = tree.messages.len().saturating_sub(limit);
        Ok(tree.messages[start..].to_vec())
    }

    /// Read-only summary of one tree.
    ///
    /// # Errors
    /// `E_TASK_NOT_FOUND` for an unknown tree; `E_INTERNAL_ERROR` when the lock
    /// is poisoned.
    pub fn tree(&self, run_id: RunId, tree_id: TaskId) -> Result<TreeSummary, Refusal> {
        let state = self.lock()?;
        let key = TreeKey { run_id, tree_id };
        let tree = state.trees.get(&key).ok_or(Refusal::TaskNotFound)?;
        Ok(TreeSummary {
            key,
            run_root: state.run_root.get(&run_id).copied().unwrap_or(tree.root),
            member_count: tree.parents.len(),
            message_count: tree.messages.len(),
            last_seq: tree.messages.last().map_or(0, |message| message.seq),
        })
    }

    /// The run's main tree key (the first tree opened for the run).
    ///
    /// # Errors
    /// `E_TASK_NOT_FOUND` when no tree has been opened for the run yet;
    /// `E_INTERNAL_ERROR` when the lock is poisoned.
    pub fn tree_key_for(&self, run_id: RunId) -> Result<TreeKey, Refusal> {
        let state = self.lock()?;
        let tree_id = state.run_root.get(&run_id).ok_or(Refusal::TaskNotFound)?;
        Ok(TreeKey {
            run_id,
            tree_id: *tree_id,
        })
    }

    /// Whether `task_id` is a member of the tree.
    ///
    /// # Errors
    /// `E_INTERNAL_ERROR` when the lock is poisoned.
    pub fn is_member(
        &self,
        run_id: RunId,
        tree_id: TaskId,
        task_id: TaskId,
    ) -> Result<bool, Refusal> {
        let state = self.lock()?;
        Ok(state
            .trees
            .get(&TreeKey { run_id, tree_id })
            .is_some_and(|tree| tree.parents.contains_key(&task_id)))
    }
}

fn sees(tree: &TreeState, message: &ChatMessage, viewer: TaskId) -> bool {
    if message.from_task_id == viewer || message.to_task_id == viewer {
        return true;
    }
    match message.visibility {
        Visibility::Task => false,
        Visibility::ParentChain => {
            is_ancestor(tree, viewer, message.from_task_id)
                || is_ancestor(tree, viewer, message.to_task_id)
        }
        Visibility::RootObserver => viewer == tree.root,
    }
}

fn is_ancestor(tree: &TreeState, ancestor: TaskId, task: TaskId) -> bool {
    let mut current = tree.parents.get(&task).copied().flatten();
    let mut hops = 0_usize;
    while let Some(parent) = current {
        if parent == ancestor {
            return true;
        }
        hops = hops.saturating_add(1);
        if hops > tree.parents.len() {
            return false;
        }
        current = tree.parents.get(&parent).copied().flatten();
    }
    false
}

/// Applies one committed record to in-memory state. The same function is used
/// by replay and by live writes, so a reopened bus produces exactly the state a
/// live bus had.
fn apply_record(state: &mut BusState, record: &ChatJournalRecord) -> Result<(), Refusal> {
    match record {
        ChatJournalRecord::OpenTree { run_id, tree_id } => {
            let key = TreeKey {
                run_id: *run_id,
                tree_id: *tree_id,
            };
            if let Some(existing) = state.trees.get(&key) {
                return if existing.root == *tree_id {
                    Ok(())
                } else {
                    Err(Refusal::SnapshotCorrupt)
                };
            }
            state.run_root.entry(*run_id).or_insert(*tree_id);
            state.trees.insert(key, TreeState::new(*tree_id));
            Ok(())
        }
        ChatJournalRecord::JoinTree {
            run_id,
            tree_id,
            task_id,
            parent_task_id,
        } => {
            let key = TreeKey {
                run_id: *run_id,
                tree_id: *tree_id,
            };
            let tree = state.trees.get_mut(&key).ok_or(Refusal::SnapshotCorrupt)?;
            if tree.parents.contains_key(task_id) {
                return Err(Refusal::SnapshotCorrupt);
            }
            if !tree.parents.contains_key(parent_task_id) {
                return Err(Refusal::SnapshotCorrupt);
            }
            tree.parents.insert(*task_id, Some(*parent_task_id));
            Ok(())
        }
        ChatJournalRecord::Post { message } => {
            let key = TreeKey {
                run_id: message.run_id,
                tree_id: message.tree_id,
            };
            let tree = state.trees.get_mut(&key).ok_or(Refusal::SnapshotCorrupt)?;
            if message.seq != tree.next_seq {
                return Err(Refusal::SnapshotCorrupt);
            }
            if tree.by_key.contains_key(&message.idempotency_key) {
                return Err(Refusal::SnapshotCorrupt);
            }
            tree.by_key
                .insert(message.idempotency_key.clone(), tree.messages.len());
            tree.messages.push(message.clone());
            tree.next_seq = tree.next_seq.saturating_add(1);
            Ok(())
        }
    }
}

#[cfg(test)]
mod internal_tests {
    use super::*;

    #[test]
    fn kind_strings_round_trip_and_never_guess() {
        for kind in ChatKind::all() {
            assert_eq!(ChatKind::from_str(kind.as_str()), Some(*kind));
        }
        assert_eq!(ChatKind::from_str("shout"), None);
        assert_eq!(ChatKind::Question.as_str(), "question");
    }

    #[test]
    fn journal_records_round_trip_as_tagged_json() {
        let record = ChatJournalRecord::OpenTree {
            run_id: RunId::new(),
            tree_id: TaskId::new(),
        };
        let json = serde_json::to_string(&record).expect("serializes");
        assert!(json.contains("\"record\":\"open_tree\""), "{json}");
        let parsed: ChatJournalRecord = serde_json::from_str(&json).expect("deserializes");
        assert_eq!(parsed, record);
    }
}
