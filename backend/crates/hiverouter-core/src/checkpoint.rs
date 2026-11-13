//! Content-addressed file checkpoint journal (CAP-5).
//!
//! A run that mutates workspace files must be able to answer one question
//! truthfully: *what did this file contain before the mutation?* This module owns
//! that journal. It is deliberately **not** a generic undo stack: every entry is
//! bound to `(run_id, seq)`, every pre-image is stored content-addressed by its
//! SHA-256, and a restore is refused unless the file still matches the recorded
//! post-mutation state. A journal that silently overwrites an external edit would
//! destroy operator work, so drift is a refusal, never a rewrite.
//!
//! Mechanical contract:
//!
//! 1. [`CheckpointJournal::journal_before`] snapshots the current content of one
//!    workspace file into `checkpoint_blobs` and records `(run, seq, path,
//!    before)` **before** the caller mutates the file.
//! 2. [`CheckpointJournal::record_after`] records the post-mutation SHA-256 and
//!    size. Until this succeeds the entry is *incomplete* and cannot be restored
//!    (an unknown after-state cannot be drift-checked).
//! 3. [`CheckpointJournal::restore_entry`] restores one entry atomically
//!    (same-directory temp file, fsync, rename) after proving the file still
//!    matches the recorded after-state, and verifies the restored bytes against
//!    the content-addressed SHA-256. [`CheckpointJournal::restore_by_id`] is the
//!    id-addressed operator API: it binds a checkpoint id to the recorded path,
//!    verifies both digests, and returns a [`RestoreReceipt`].
//! 4. [`CheckpointJournal::restore_to`] rewinds every entry of a run at or above
//!    a sequence number. Every affected path is validated **before** the first
//!    write, so a drift refusal cannot leave a half-restored workspace; each
//!    individual file replacement is atomic.
//! 5. Every path goes through the workspace [`WorkspaceJail`]: parent traversal,
//!    absolute/UNC/device/short-name paths and symlink or junction escapes are
//!    refused (never clamped), and a restore target that is itself a symlink is
//!    refused.
//! 6. File content is bounded by [`MAX_CHECKPOINT_BYTES`] (4 MiB). A file larger
//!    than the bound is refused before it is read into the journal.
//!
//! The SQLite schema ships as [`CHECKPOINT_MIGRATION`]. The Lead appends it to
//! [`crate::db::MIGRATIONS`]; [`checkpoint_migration`] exists so the migration can
//! be renumbered without editing this module (Coordinator/CAP conflicts).
//!
//! # Lead wiring
//!
//! The undo tool should resolve `checkpoint_id + path` and call
//! [`CheckpointJournal::restore_by_id`], returning the serialized
//! [`RestoreReceipt`] as its result. Do not call [`CheckpointJournal::restore_entry`]
//! from a tool that accepts a caller-supplied path: only `restore_by_id`
//! re-validates the path binding and returns the receipt. `restore_entry` and
//! `restore_to` remain the internal run-rewind primitives.
//!
//! # Refusal identity
//!
//! The frozen refusal table (`hiverouter_contract::Refusal`) has no dedicated
//! checkpoint code yet. Drift, corruption, an incomplete entry and a missing
//! checkpoint therefore surface as `E_INTERNAL_ERROR` (a refusal, never a fake
//! success); the Lead should add `E_CHECKPOINT_DRIFT`, `E_CHECKPOINT_CORRUPT` and
//! `E_CHECKPOINT_NOT_FOUND` and swap the three arms in
//! [`CheckpointError::refusal`]. Path/file refusals already use their exact
//! codes (`E_PATH_OUTSIDE_JAIL`, `E_JAIL_ESCAPE`, `E_FILE_TOO_LARGE`,
//! `E_TOOL_SCHEMA`, `E_RUN_NOT_FOUND`).

use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use hiverouter_contract::{Refusal, RunId, Timestamp};
use hiverouter_tools::sha256_hex;
use hiverouter_tools::{ToolError, WorkspaceJail};
use rusqlite::OptionalExtension;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::db::{Database, DbError, Migration};

/// Hard bound on a single journalled file (4 MiB).
///
/// The journal stores full pre-images in SQLite, so an unbounded file would let
/// one checkpoint grow the database without limit. Callers may lower this bound
/// with [`CheckpointJournal::with_max_file_bytes`]; they cannot raise it.
pub const MAX_CHECKPOINT_BYTES: u64 = 4 * 1024 * 1024;

/// Hard bound on a checkpoint id accepted by the id-addressed API (128 bytes).
///
/// Ids are uuid-v7 strings (36 bytes); the bound exists so a hostile caller
/// cannot push an unbounded string through the refusal path or the SQL layer.
pub const MAX_CHECKPOINT_ID_BYTES: usize = 128;

/// Name recorded in `schema_migrations` for [`CHECKPOINT_MIGRATION`].
pub const CHECKPOINT_MIGRATION_NAME: &str = "checkpoint_journal";

/// Default migration version (the Lead renumbers it if another CAP took the
/// number).
pub const CHECKPOINT_SCHEMA_VERSION: i64 = 27;

/// Forward SQL for the checkpoint journal (schema v26 by default).
///
/// `checkpoints` rows are immutable once an after-state is recorded except for
/// `restored_at`; `checkpoint_blobs` is content-addressed, so the same pre-image
/// is stored once no matter how many entries reference it.
pub const CHECKPOINT_MIGRATION_UP: &str = r#"
    CREATE TABLE IF NOT EXISTS checkpoint_blobs (
        sha256     TEXT PRIMARY KEY NOT NULL CHECK (length(sha256) = 64),
        size       INTEGER NOT NULL CHECK (size >= 0),
        bytes      BLOB NOT NULL,
        created_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS checkpoints (
        checkpoint_id  TEXT PRIMARY KEY NOT NULL,
        run_id         TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
        seq            INTEGER NOT NULL CHECK (seq >= 1),
        path           TEXT NOT NULL CHECK (length(path) > 0),
        existed_before INTEGER NOT NULL CHECK (existed_before IN (0, 1)),
        before_sha256  TEXT,
        before_size    INTEGER,
        after_sha256   TEXT,
        after_size     INTEGER,
        completed      INTEGER NOT NULL DEFAULT 0 CHECK (completed IN (0, 1)),
        created_at     TEXT NOT NULL,
        completed_at   TEXT,
        restored_at    TEXT,
        UNIQUE (run_id, seq),
        CHECK (
            (existed_before = 0 AND before_sha256 IS NULL AND before_size IS NULL)
            OR
            (existed_before = 1 AND before_sha256 IS NOT NULL AND before_size IS NOT NULL)
        ),
        CHECK (
            (completed = 0 AND after_sha256 IS NULL AND after_size IS NULL
                AND completed_at IS NULL)
            OR
            (completed = 1 AND completed_at IS NOT NULL AND (
                (after_sha256 IS NULL AND after_size IS NULL)
                OR (after_sha256 IS NOT NULL AND after_size IS NOT NULL)
            ))
        ),
        CHECK (after_size IS NULL OR after_size >= 0)
    );
    CREATE INDEX IF NOT EXISTS idx_checkpoints_run_seq
        ON checkpoints(run_id, seq DESC);
"#;

/// Inverse SQL for [`CHECKPOINT_MIGRATION_UP`] (exercised by the Lead's
/// migration-roundtrip test).
pub const CHECKPOINT_MIGRATION_DOWN: &str = r#"
    DROP INDEX IF EXISTS idx_checkpoints_run_seq;
    DROP TABLE IF EXISTS checkpoints;
    DROP TABLE IF EXISTS checkpoint_blobs;
"#;

/// Builds the checkpoint migration at an explicit version.
///
/// `MIGRATIONS` is a `const` slice, so the Lead can append
/// `checkpoint::checkpoint_migration(<next version>)` directly; a plain
/// `#[must_use]` `const fn` is callable in that position.
#[must_use]
pub const fn checkpoint_migration(version: i64) -> Migration {
    Migration {
        version,
        name: CHECKPOINT_MIGRATION_NAME,
        up: CHECKPOINT_MIGRATION_UP,
        down: CHECKPOINT_MIGRATION_DOWN,
    }
}

/// The checkpoint migration at [`CHECKPOINT_SCHEMA_VERSION`].
pub const CHECKPOINT_MIGRATION: Migration = checkpoint_migration(CHECKPOINT_SCHEMA_VERSION);

/// The journalled state of one workspace path.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum FileState {
    /// The path did not exist.
    Absent,
    /// The path existed as a regular file with this content digest and size.
    Present {
        /// Lowercase hex SHA-256 of the file bytes.
        sha256: String,
        /// File size in bytes.
        size: u64,
    },
}

impl FileState {
    /// Content digest when the path existed.
    #[must_use]
    pub fn sha256(&self) -> Option<&str> {
        match self {
            Self::Absent => None,
            Self::Present { sha256, .. } => Some(sha256),
        }
    }

    /// File size when the path existed.
    #[must_use]
    pub fn size(&self) -> Option<u64> {
        match self {
            Self::Absent => None,
            Self::Present { size, .. } => Some(*size),
        }
    }

    /// Human-readable form used in drift refusal details.
    #[must_use]
    pub fn describe(&self) -> String {
        match self {
            Self::Absent => "absent".to_owned(),
            Self::Present { sha256, size } => format!("sha256={sha256} size={size}"),
        }
    }
}

/// One journal entry for a file mutation inside a run.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileCheckpoint {
    /// Stable row id (uuid-v7).
    pub checkpoint_id: String,
    /// Run the mutation belongs to.
    pub run_id: RunId,
    /// Monotonic (per run) journal sequence; `1`-based.
    pub seq: u64,
    /// Normalised workspace-relative path (`/` separators, canonical case).
    pub path: String,
    /// State before the mutation.
    pub before: FileState,
    /// State after the mutation; `None` until [`CheckpointJournal::record_after`].
    pub after: Option<FileState>,
    /// Journal creation timestamp.
    pub created_at: Timestamp,
    /// When the after-state was recorded.
    pub completed_at: Option<Timestamp>,
    /// When this entry was last restored.
    pub restored_at: Option<Timestamp>,
}

/// One path restored by [`CheckpointJournal::restore_entry`] or
/// [`CheckpointJournal::restore_to`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RestoredFile {
    /// Workspace-relative path that was restored.
    pub path: String,
    /// Journal sequences undone for this path, newest first.
    pub undone_seqs: Vec<u64>,
    /// State the path was restored to.
    pub restored: FileState,
}

/// Result of a multi-entry rewind.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RestoreOutcome {
    /// Run that was rewound.
    pub run_id: RunId,
    /// First (inclusive) journal sequence that was undone.
    pub from_seq: u64,
    /// Paths restored, in first-journaled order.
    pub restored: Vec<RestoredFile>,
}

/// Receipt returned by [`CheckpointJournal::restore_by_id`].
///
/// The receipt is produced only after both recorded digests were verified and
/// the restored bytes were read back and matched against the pre-image. It is
/// deliberately a value (not a log line): the caller can persist it as evidence
/// that a specific checkpoint id was undone at a specific instant.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RestoreReceipt {
    /// The checkpoint id that was restored.
    pub checkpoint_id: String,
    /// Run the checkpoint belongs to.
    pub run_id: RunId,
    /// Journal sequence of the restored entry.
    pub seq: u64,
    /// Workspace-relative path that was restored.
    pub path: String,
    /// State observed immediately before the restore; verified equal to the
    /// recorded after-state (the file had not drifted).
    pub observed_after: FileState,
    /// State the path was restored to; verified equal to the recorded
    /// pre-image.
    pub restored_to: FileState,
    /// Bytes written by the restore (`0` when the path was removed because it
    /// did not exist before the mutation).
    pub bytes_written: u64,
    /// When the restore completed.
    pub restored_at: Timestamp,
}

/// Checkpoint journal failure with its stable refusal identity where one exists.
#[derive(Debug, Error)]
pub enum CheckpointError {
    /// A tool-layer refusal propagated from the workspace jail.
    #[error("{refusal}: {detail}")]
    Tool {
        /// Stable refusal identity.
        refusal: Refusal,
        /// Non-secret detail.
        detail: String,
    },
    /// The file exceeds the checkpoint content bound.
    #[error("E_FILE_TOO_LARGE: {path:?} is {size} bytes; the checkpoint cap is {cap} bytes")]
    FileTooLarge {
        /// Workspace-relative path.
        path: String,
        /// Observed size.
        size: u64,
        /// Configured cap.
        cap: u64,
    },
    /// The run row does not exist (the journal is bound to a real run).
    #[error("E_RUN_NOT_FOUND: run `{0}` does not exist")]
    RunNotFound(String),
    /// No checkpoint exists for the `(run, seq)` pair.
    #[error("checkpoint ({run_id}, {seq}) does not exist")]
    CheckpointNotFound {
        /// Run id as stored.
        run_id: String,
        /// Journal sequence.
        seq: u64,
    },
    /// No checkpoint exists for the supplied checkpoint id.
    #[error("checkpoint `{0}` does not exist")]
    CheckpointIdNotFound(String),
    /// The supplied path is not the path the checkpoint was recorded for.
    ///
    /// A checkpoint id is bound to exactly one workspace path; restoring a
    /// different path through it would write the pre-image of one file over
    /// another, so the mismatch is refused before any validation or write.
    #[error(
        "checkpoint {checkpoint_id} was recorded for {recorded:?}, not {requested:?}; \
         restore refused"
    )]
    PathMismatch {
        /// Checkpoint id as stored.
        checkpoint_id: String,
        /// Path recorded in the journal.
        recorded: String,
        /// Path supplied by the caller.
        requested: String,
    },
    /// The `(run, seq)` pair already has a journal entry.
    #[error("checkpoint ({run_id}, {seq}) has already been journaled")]
    DuplicateSeq {
        /// Run id as stored.
        run_id: String,
        /// Journal sequence.
        seq: u64,
    },
    /// `record_after` was never called, so the after-state is unknown.
    #[error(
        "checkpoint ({run_id}, {seq}) has no recorded after-state; \
         call record_after before restoring"
    )]
    Incomplete {
        /// Run id as stored.
        run_id: String,
        /// Journal sequence.
        seq: u64,
    },
    /// The entry cannot be deleted in the requested mode: a void was asked for a
    /// completed mutation, or a no-op discard was asked for an entry whose
    /// after-state differs from its pre-image.
    ///
    /// A completed entry is never deleted: once the mutation ran its pre-image
    /// is the only copy of the operator's bytes, so dropping it would destroy
    /// the ability to restore.
    #[error("checkpoint ({run_id}, {seq}) cannot be removed: {reason}")]
    NotRemovable {
        /// Run id as stored.
        run_id: String,
        /// Journal sequence.
        seq: u64,
        /// Why the row was kept.
        reason: &'static str,
    },
    /// The file no longer matches the recorded after-state (external drift).
    #[error(
        "checkpoint drift at {path:?}: expected {expected}, found {actual}; \
         restore refused (the external edit was not overwritten)"
    )]
    Drift {
        /// Workspace-relative path.
        path: String,
        /// Recorded after-state.
        expected: String,
        /// Observed state.
        actual: String,
    },
    /// Stored journal or blob data failed integrity verification.
    #[error("checkpoint journal is corrupt: {0}")]
    Corrupt(String),
    /// Database failure.
    #[error(transparent)]
    Db(#[from] DbError),
    /// Filesystem failure while journalling or restoring.
    #[error("I/O failure in the checkpoint journal: {0}")]
    Io(#[from] std::io::Error),
}

impl CheckpointError {
    /// Maps the error to the stable wire refusal.
    ///
    /// Drift/corruption/missing entries map to `E_INTERNAL_ERROR` until the
    /// contract adds dedicated checkpoint codes (see the module docs); they are
    /// still refusals, never fabricated success.
    #[must_use]
    pub fn refusal(&self) -> Refusal {
        match self {
            Self::Tool { refusal, .. } => *refusal,
            Self::FileTooLarge { .. } => Refusal::FileTooLarge,
            Self::RunNotFound(_) => Refusal::RunNotFound,
            Self::Db(error) => error.refusal(),
            Self::CheckpointNotFound { .. }
            | Self::CheckpointIdNotFound(_)
            | Self::PathMismatch { .. }
            | Self::DuplicateSeq { .. }
            | Self::Incomplete { .. }
            | Self::NotRemovable { .. }
            | Self::Drift { .. }
            | Self::Corrupt(_)
            | Self::Io(_) => Refusal::InternalError,
        }
    }

    /// Stable wire code for the error.
    #[must_use]
    pub fn code(&self) -> &'static str {
        self.refusal().code()
    }
}

/// Content-addressed file checkpoint journal.
///
/// The journal owns the workspace jail it validates paths against, so callers
/// cannot accidentally skip the escape checks.
#[derive(Debug, Clone)]
pub struct CheckpointJournal {
    database: Arc<Database>,
    jail: Arc<WorkspaceJail>,
    max_file_bytes: u64,
}

impl CheckpointJournal {
    /// Creates a journal over an existing workspace jail.
    #[must_use]
    pub fn new(database: Arc<Database>, jail: WorkspaceJail) -> Self {
        Self {
            database,
            jail: Arc::new(jail),
            max_file_bytes: MAX_CHECKPOINT_BYTES,
        }
    }

    /// Opens (creating if needed) a workspace root and wraps it in a jail.
    ///
    /// # Errors
    /// [`CheckpointError::Tool`] when the root is not absolute or cannot be
    /// opened/canonicalised.
    pub fn open(
        database: Arc<Database>,
        workspace_root: impl AsRef<Path>,
    ) -> Result<Self, CheckpointError> {
        let jail = WorkspaceJail::open(workspace_root).map_err(map_tool_error)?;
        Ok(Self::new(database, jail))
    }

    /// Lowers (never raises) the per-file content bound.
    #[must_use]
    pub fn with_max_file_bytes(mut self, max_file_bytes: u64) -> Self {
        self.max_file_bytes = max_file_bytes.clamp(1, MAX_CHECKPOINT_BYTES);
        self
    }

    /// The workspace jail used for every path.
    #[must_use]
    pub fn jail(&self) -> &WorkspaceJail {
        &self.jail
    }

    /// Allocates the next journal sequence for a run (`1` for the first entry).
    ///
    /// # Errors
    /// [`CheckpointError::Db`] when the query fails.
    pub fn next_seq(&self, run_id: RunId) -> Result<u64, CheckpointError> {
        let highest: Option<i64> = self
            .database
            .with_connection(|connection| {
                connection.query_row(
                    "SELECT MAX(seq) FROM checkpoints WHERE run_id = ?1",
                    rusqlite::params![run_id.to_string()],
                    |row| row.get(0),
                )
            })
            .map_err(CheckpointError::Db)?;
        let next = highest
            .and_then(|value| u64::try_from(value).ok())
            .unwrap_or(0)
            .checked_add(1)
            .ok_or_else(|| {
                CheckpointError::Corrupt(format!(
                    "checkpoint sequence for run {run_id} overflowed u64"
                ))
            })?;
        Ok(next)
    }

    /// Snapshots a file (or its absence) **before** the caller mutates it.
    ///
    /// The pre-image is stored once in `checkpoint_blobs`, keyed by SHA-256, so
    /// repeated mutations of identical content share one blob.
    ///
    /// # Errors
    /// [`CheckpointError::Tool`] for every jail/regular-file refusal,
    /// [`CheckpointError::FileTooLarge`] above the content bound,
    /// [`CheckpointError::RunNotFound`] when the run row does not exist,
    /// [`CheckpointError::DuplicateSeq`] when `(run, seq)` is already journaled,
    /// plus database/I/O failures.
    pub fn journal_before(
        &self,
        run_id: RunId,
        seq: u64,
        requested_path: &str,
    ) -> Result<FileCheckpoint, CheckpointError> {
        if seq == 0 {
            return Err(refused(Refusal::ToolSchema, "checkpoint seq must be >= 1"));
        }
        let seq_i64 = i64::try_from(seq).map_err(|_| {
            refused(
                Refusal::ToolSchema,
                format!("checkpoint seq {seq} exceeds the SQLite INTEGER range"),
            )
        })?;
        let (key, path) = self.normalized_key(requested_path)?;
        self.require_run(run_id)?;
        if self.query_checkpoint(run_id, seq)?.is_some() {
            return Err(CheckpointError::DuplicateSeq {
                run_id: run_id.to_string(),
                seq,
            });
        }
        let (before, blob) = self.snapshot_file(&path, &key)?;
        let created_at = Timestamp::now();
        let checkpoint_id = uuid::Uuid::now_v7().to_string();
        let (before_sha256, before_size) = match &before {
            FileState::Absent => (None, None),
            FileState::Present { sha256, size } => (Some(sha256.clone()), Some(*size)),
        };
        let before_size_i64 = before_size
            .map(|size| {
                i64::try_from(size).map_err(|_| {
                    CheckpointError::Corrupt(format!(
                        "checkpoint pre-image size {size} exceeds the SQLite INTEGER range"
                    ))
                })
            })
            .transpose()?;
        let blobs = match (before.clone(), blob) {
            (FileState::Absent, None) => None,
            (FileState::Present { sha256, size }, Some(bytes)) => Some(BlobWrite {
                sha256,
                size: i64::try_from(size).map_err(|_| {
                    CheckpointError::Corrupt(format!(
                        "checkpoint pre-image size {size} exceeds the SQLite INTEGER range"
                    ))
                })?,
                bytes,
            }),
            _ => {
                return Err(CheckpointError::Corrupt(
                    "file snapshot decoded to an inconsistent (state, content) pair".to_owned(),
                ))
            }
        };
        let run_text = run_id.to_string();
        let created_at_text = created_at.as_str().to_owned();
        let existed_before = if matches!(&before, FileState::Absent) {
            0_i64
        } else {
            1_i64
        };
        let outcome: Result<(), CheckpointError> = self
            .database
            .with_connection(|connection| {
                let transaction = connection.transaction()?;
                if let Some(blob) = &blobs {
                    transaction.execute(
                        "INSERT OR IGNORE INTO checkpoint_blobs(sha256, size, bytes, created_at)
                         VALUES (?1, ?2, ?3, ?4)",
                        rusqlite::params![blob.sha256, blob.size, blob.bytes, created_at_text],
                    )?;
                    let stored: Option<i64> = transaction
                        .query_row(
                            "SELECT size FROM checkpoint_blobs WHERE sha256 = ?1",
                            rusqlite::params![blob.sha256],
                            |row| row.get(0),
                        )
                        .optional()?;
                    match stored {
                        Some(stored) if stored == blob.size => {}
                        Some(stored) => {
                            return Ok(Err(CheckpointError::Corrupt(format!(
                                "blob {} size mismatch: stored {stored}, expected {}",
                                blob.sha256, blob.size
                            ))))
                        }
                        None => {
                            return Ok(Err(CheckpointError::Corrupt(format!(
                                "blob {} disappeared during journal insert",
                                blob.sha256
                            ))))
                        }
                    }
                }
                let inserted = transaction.execute(
                    "INSERT INTO checkpoints(
                         checkpoint_id, run_id, seq, path, existed_before,
                         before_sha256, before_size, created_at
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                    rusqlite::params![
                        checkpoint_id,
                        run_text,
                        seq_i64,
                        key,
                        existed_before,
                        before_sha256,
                        before_size_i64,
                        created_at_text
                    ],
                );
                if let Err(error) = inserted {
                    return Ok(Err(insert_error(error, &run_text, seq)));
                }
                transaction.commit()?;
                Ok(Ok(()))
            })
            .map_err(CheckpointError::Db)?;
        outcome?;
        self.query_checkpoint(run_id, seq)?.ok_or_else(|| {
            CheckpointError::Corrupt(format!(
                "checkpoint ({run_id}, {seq}) disappeared immediately after insert"
            ))
        })
    }

    /// Records the post-mutation state of an existing journal entry.
    ///
    /// The file may now be absent (the mutation deleted it); that is recorded as
    /// [`FileState::Absent`], not as an error. Repeating the call is idempotent
    /// when the file still matches; a second, different after-state is refused as
    /// corruption because the file changed without a journal entry.
    ///
    /// # Errors
    /// [`CheckpointError::CheckpointNotFound`] for an unknown entry,
    /// [`CheckpointError::Corrupt`] for a conflicting second record, plus
    /// jail/database/I/O failures.
    pub fn record_after(&self, run_id: RunId, seq: u64) -> Result<FileCheckpoint, CheckpointError> {
        let checkpoint = self.query_checkpoint(run_id, seq)?.ok_or_else(|| {
            CheckpointError::CheckpointNotFound {
                run_id: run_id.to_string(),
                seq,
            }
        })?;
        let (key, path) = self.resolve_key(&checkpoint.path)?;
        let current = self.read_state(&path, &key)?;
        if let Some(existing) = &checkpoint.after {
            if *existing == current {
                return Ok(checkpoint);
            }
            return Err(CheckpointError::Corrupt(format!(
                "checkpoint ({run_id}, {seq}) already recorded after-state {}; \
                 the file is now {} without a new journal entry",
                existing.describe(),
                current.describe()
            )));
        }
        let (after_sha256, after_size) = match &current {
            FileState::Absent => (None, None),
            FileState::Present { sha256, size } => (Some(sha256.clone()), Some(*size)),
        };
        let after_size_i64 = after_size
            .map(|size| {
                i64::try_from(size).map_err(|_| {
                    CheckpointError::Corrupt(format!(
                        "checkpoint after-image size {size} exceeds the SQLite INTEGER range"
                    ))
                })
            })
            .transpose()?;
        let seq_i64 = i64::try_from(seq).map_err(|_| {
            refused(
                Refusal::ToolSchema,
                format!("checkpoint seq {seq} exceeds the SQLite INTEGER range"),
            )
        })?;
        let completed_at = Timestamp::now();
        let completed_at_text = completed_at.as_str().to_owned();
        let run_text = run_id.to_string();
        let changed = self
            .database
            .with_connection(|connection| {
                connection.execute(
                    "UPDATE checkpoints
                     SET after_sha256 = ?3, after_size = ?4, completed = 1, completed_at = ?5
                     WHERE run_id = ?1 AND seq = ?2 AND completed = 0",
                    rusqlite::params![
                        run_text,
                        seq_i64,
                        after_sha256,
                        after_size_i64,
                        completed_at_text
                    ],
                )
            })
            .map_err(CheckpointError::Db)?;
        let latest = self.query_checkpoint(run_id, seq)?.ok_or_else(|| {
            CheckpointError::CheckpointNotFound {
                run_id: run_id.to_string(),
                seq,
            }
        })?;
        if changed == 1 {
            return Ok(latest);
        }
        // A concurrent writer completed the entry first; accept only an
        // identical after-state.
        if latest.after.as_ref() == Some(&current) {
            Ok(latest)
        } else {
            Err(CheckpointError::Corrupt(format!(
                "checkpoint ({run_id}, {seq}) completed concurrently with a different after-state"
            )))
        }
    }

    /// Deletes an **incomplete** journal entry: a captured pre-image whose
    /// mutation never ran.
    ///
    /// This is the approval hook's cleanup path. The pre-image is captured
    /// before the execution grant is verified; when the grant is refused no
    /// effect happened, so the entry records a mutation that never occurred.
    /// Deleting it keeps `restore_to` usable (an incomplete newest entry would
    /// refuse the whole rewind) and drops the pre-image blob once no other entry
    /// references it.
    ///
    /// The call is idempotent: a missing entry returns `Ok(false)`. A completed
    /// entry is **never** deleted - once the mutation ran its pre-image is the
    /// only copy of the operator's bytes - and is refused as
    /// [`CheckpointError::NotRemovable`].
    ///
    /// # Errors
    /// [`CheckpointError::NotRemovable`] for a completed entry, plus
    /// database/I/O failures.
    pub fn void_entry(&self, run_id: RunId, seq: u64) -> Result<bool, CheckpointError> {
        self.remove_entry(run_id, seq, EntryRemoval::IncompleteOnly)
    }

    /// Records the settled after-state of a mutation attempt, discarding the
    /// entry when the attempt left the file byte-identical.
    ///
    /// Call this once the mutation attempt has settled, success or failure:
    ///
    /// - a write that changed the file (or created/deleted it) returns
    ///   `Some(entry)` with the recorded after-state;
    /// - a byte-identical no-op write, or an `apply_patch` that rejected its
    ///   hunk without writing, returns `None` and deletes the entry, because
    ///   there is nothing to restore. Recording a "mutation" that did not
    ///   change a byte would make the journal lie and would make a later rewind
    ///   rewrite a no-op file it believes it is restoring.
    ///
    /// # Errors
    /// [`CheckpointError::CheckpointNotFound`] for an unknown entry,
    /// [`CheckpointError::Corrupt`] for a conflicting after-state (the file
    /// changed without a journal entry), plus jail/database/I/O failures.
    pub fn record_after_or_discard(
        &self,
        run_id: RunId,
        seq: u64,
    ) -> Result<Option<FileCheckpoint>, CheckpointError> {
        let completed = self.record_after(run_id, seq)?;
        if completed.after.as_ref() == Some(&completed.before) {
            self.remove_entry(run_id, seq, EntryRemoval::NoOp)?;
            return Ok(None);
        }
        Ok(Some(completed))
    }

    /// Reads one journal entry.
    ///
    /// # Errors
    /// Database/decode failures.
    pub fn get(&self, run_id: RunId, seq: u64) -> Result<Option<FileCheckpoint>, CheckpointError> {
        self.query_checkpoint(run_id, seq)
    }

    /// Reads one journal entry by its stable checkpoint id.
    ///
    /// # Errors
    /// [`CheckpointError::Tool`] when the id is empty or over
    /// [`MAX_CHECKPOINT_ID_BYTES`], plus database/decode failures.
    pub fn get_by_id(
        &self,
        checkpoint_id: &str,
    ) -> Result<Option<FileCheckpoint>, CheckpointError> {
        validate_checkpoint_id(checkpoint_id)?;
        self.query_checkpoint_by_id(checkpoint_id)
    }

    /// Restores one checkpoint addressed by **checkpoint id + path** and returns
    /// a verified receipt.
    ///
    /// This is the operator-facing undo primitive. It is intentionally stricter
    /// than [`Self::restore_entry`]:
    ///
    /// 1. the checkpoint id must exist and the supplied path must resolve, under
    ///    the workspace [`WorkspaceJail`], to exactly the path recorded in the
    ///    journal (a mismatch, traversal, absolute path, symlink or junction
    ///    escape is refused before anything is read or written);
    /// 2. the entry must be complete (an unknown after-state cannot be
    ///    drift-checked);
    /// 3. the current file must still match the recorded **after** SHA-256/size
    ///    of the restored **before** SHA-256/size; a stale (externally edited)
    ///    file, a missing/extra file, or a tampered journal row/blob is a
    ///    refusal, never an overwrite;
    /// 4. the restore is atomic (same-directory temp file, fsync, rename) and the
    ///    written bytes are read back and compared with the recorded pre-image
    ///    before the receipt is returned.
    ///
    /// Restoring the same entry twice is refused: after the first restore the
    /// current file matches the pre-image, not the recorded after-state, so the
    /// second call is drift. Re-applying a mutation is a new mutation and must
    /// be journaled; this API never guesses a redo state.
    ///
    /// # Errors
    /// [`CheckpointError::CheckpointIdNotFound`],
    /// [`CheckpointError::PathMismatch`], [`CheckpointError::Drift`],
    /// [`CheckpointError::Incomplete`], [`CheckpointError::Corrupt`] for a
    /// tampered row/blob, and jail/I/O/database failures.
    pub fn restore_by_id(
        &self,
        checkpoint_id: &str,
        requested_path: &str,
    ) -> Result<RestoreReceipt, CheckpointError> {
        validate_checkpoint_id(checkpoint_id)?;
        let checkpoint = self
            .query_checkpoint_by_id(checkpoint_id)?
            .ok_or_else(|| CheckpointError::CheckpointIdNotFound(checkpoint_id.to_owned()))?;
        // Validate the caller-supplied path through the jail first: this must
        // run even when the path "looks" equal to the stored one, because the
        // filesystem may have been rearranged (junction/symlink) since.
        let (resolved_key, _) = self.normalized_key(requested_path)?;
        if resolved_key != checkpoint.path {
            return Err(CheckpointError::PathMismatch {
                checkpoint_id: checkpoint.checkpoint_id,
                recorded: checkpoint.path,
                requested: resolved_key,
            });
        }
        let expected = checkpoint
            .after
            .clone()
            .ok_or_else(|| CheckpointError::Incomplete {
                run_id: checkpoint.run_id.to_string(),
                seq: checkpoint.seq,
            })?;
        let plan = PathPlan {
            path: checkpoint.path.clone(),
            expected: expected.clone(),
            target: checkpoint.before.clone(),
            seqs: vec![checkpoint.seq],
        };
        // `validate_plan` proves the current file still hashes to the recorded
        // after-state and that the pre-image blob still hashes to the recorded
        // before-state. Nothing is written before both proofs pass.
        self.validate_plan(&plan)?;
        self.apply_plan(&plan)?;
        self.mark_restored(checkpoint.run_id, &plan.seqs)?;
        Ok(RestoreReceipt {
            checkpoint_id: checkpoint.checkpoint_id,
            run_id: checkpoint.run_id,
            seq: checkpoint.seq,
            path: plan.path,
            observed_after: expected,
            bytes_written: plan.target.size().unwrap_or(0),
            restored_to: plan.target,
            restored_at: Timestamp::now(),
        })
    }

    /// Reads every journal entry for a run, newest sequence first.
    ///
    /// # Errors
    /// Database/decode failures.
    pub fn entries_for_run(&self, run_id: RunId) -> Result<Vec<FileCheckpoint>, CheckpointError> {
        self.query_entries(run_id, 1)
    }

    /// Atomically restores the pre-mutation state of one journal entry.
    ///
    /// The current file must still match the recorded after-state; otherwise the
    /// restore is refused and the file is left untouched.
    ///
    /// # Errors
    /// [`CheckpointError::Drift`] on external modification,
    /// [`CheckpointError::Incomplete`] before `record_after`,
    /// [`CheckpointError::CheckpointNotFound`] for an unknown entry,
    /// [`CheckpointError::Corrupt`] for a damaged blob, plus jail/I/O failures.
    pub fn restore_entry(&self, run_id: RunId, seq: u64) -> Result<RestoredFile, CheckpointError> {
        let checkpoint = self.query_checkpoint(run_id, seq)?.ok_or_else(|| {
            CheckpointError::CheckpointNotFound {
                run_id: run_id.to_string(),
                seq,
            }
        })?;
        let expected = checkpoint
            .after
            .clone()
            .ok_or_else(|| CheckpointError::Incomplete {
                run_id: run_id.to_string(),
                seq,
            })?;
        let plan = PathPlan {
            path: checkpoint.path.clone(),
            expected,
            target: checkpoint.before.clone(),
            seqs: vec![seq],
        };
        self.validate_plan(&plan)?;
        self.apply_plan(&plan)?;
        self.mark_restored(run_id, &plan.seqs)?;
        Ok(RestoredFile {
            path: plan.path,
            undone_seqs: plan.seqs,
            restored: plan.target,
        })
    }

    /// Rewinds every entry of a run with `seq >= from_seq`.
    ///
    /// Every affected path is drift-checked and every pre-image blob verified
    /// before the first write; a drift refusal therefore changes nothing. Each
    /// file replacement is atomic (temp file + rename). Files that the run created
    /// are removed again.
    ///
    /// # Errors
    /// [`CheckpointError::Drift`] on external modification,
    /// [`CheckpointError::Incomplete`] for a missing after-state,
    /// [`CheckpointError::Corrupt`] for an inconsistent journal chain or blob,
    /// plus jail/I/O failures.
    pub fn restore_to(
        &self,
        run_id: RunId,
        from_seq: u64,
    ) -> Result<RestoreOutcome, CheckpointError> {
        if from_seq == 0 {
            return Err(refused(
                Refusal::ToolSchema,
                "checkpoint rewind sequence must be >= 1",
            ));
        }
        let entries = self.query_entries(run_id, from_seq)?;
        if entries.is_empty() {
            return Ok(RestoreOutcome {
                run_id,
                from_seq,
                restored: Vec::new(),
            });
        }
        let plans = build_plans(&entries, run_id)?;
        // Validate the whole set before the first write: a drift refusal must
        // not leave a half-restored workspace.
        for plan in &plans {
            self.validate_plan(plan)?;
        }
        for plan in &plans {
            self.apply_plan(plan)?;
        }
        for plan in &plans {
            self.mark_restored(run_id, &plan.seqs)?;
        }
        Ok(RestoreOutcome {
            run_id,
            from_seq,
            restored: plans
                .into_iter()
                .map(|plan| RestoredFile {
                    path: plan.path,
                    undone_seqs: plan.seqs,
                    restored: plan.target,
                })
                .collect(),
        })
    }

    /// Deletes content-addressed blobs no checkpoint references.
    ///
    /// Call after pruning runs/entries so the pre-image store does not grow
    /// without bound. Returns the number of blobs removed.
    ///
    /// # Errors
    /// Database failure.
    pub fn prune_orphan_blobs(&self) -> Result<usize, CheckpointError> {
        self.database
            .with_connection(|connection| {
                connection.execute(
                    "DELETE FROM checkpoint_blobs
                     WHERE sha256 NOT IN (
                         SELECT before_sha256 FROM checkpoints
                         WHERE before_sha256 IS NOT NULL
                     )",
                    [],
                )
            })
            .map_err(CheckpointError::Db)
    }

    // --- path handling ------------------------------------------------------

    /// Resolves and canonicalises a requested path, returning the stable
    /// workspace-relative key plus the canonical target path.
    fn normalized_key(&self, requested: &str) -> Result<(String, PathBuf), CheckpointError> {
        let resolved = self.jail.resolve(requested).map_err(map_tool_error)?;
        if resolved == *self.jail.root() {
            return Err(refused(
                Refusal::ToolSchema,
                "a checkpoint must name a file, not the workspace root",
            ));
        }
        // Refuse a symlink target explicitly: `resolve` refuses escapes, but a
        // restore must never write "through" a link even when it points inside.
        match std::fs::symlink_metadata(&resolved) {
            Ok(metadata) => {
                if metadata.file_type().is_symlink() {
                    return Err(refused(
                        Refusal::JailEscape,
                        format!(
                            "checkpoint target {requested:?} is a symlink; symlink targets are refused"
                        ),
                    ));
                }
                if metadata.is_dir() {
                    return Err(refused(
                        Refusal::ToolSchema,
                        format!("checkpoint target {requested:?} is a directory"),
                    ));
                }
                if !metadata.is_file() {
                    return Err(refused(
                        Refusal::ToolSchema,
                        format!("checkpoint target {requested:?} is not a regular file"),
                    ));
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(CheckpointError::Io(error)),
        }
        let canonical = canonicalize_target(&resolved)?;
        if !self.jail.is_inside(&canonical) {
            return Err(refused(
                Refusal::PathOutsideJail,
                format!("checkpoint target {requested:?} resolves outside the workspace root"),
            ));
        }
        let relative = canonical.strip_prefix(self.jail.root()).map_err(|_| {
            refused(
                Refusal::PathOutsideJail,
                format!("checkpoint target {requested:?} is not under the workspace root"),
            )
        })?;
        let mut segments = Vec::new();
        for component in relative.components() {
            match component {
                Component::Normal(segment) => {
                    let text = segment.to_str().ok_or_else(|| {
                        refused(
                            Refusal::ToolSchema,
                            "checkpoint path is not valid UTF-8 and cannot be journaled",
                        )
                    })?;
                    segments.push(text.to_owned());
                }
                _ => {
                    return Err(refused(
                        Refusal::PathOutsideJail,
                        format!("checkpoint path {requested:?} has a non-normal component"),
                    ))
                }
            }
        }
        if segments.is_empty() {
            return Err(refused(
                Refusal::ToolSchema,
                "a checkpoint must name a file, not the workspace root",
            ));
        }
        Ok((segments.join("/"), canonical))
    }

    /// Re-resolves a stored key and refuses when the path now maps elsewhere
    /// (for example a directory was replaced by a junction).
    fn resolve_key(&self, key: &str) -> Result<(String, PathBuf), CheckpointError> {
        let (resolved_key, path) = self.normalized_key(key)?;
        if resolved_key != key {
            return Err(CheckpointError::Corrupt(format!(
                "checkpoint path {key:?} now resolves to {resolved_key:?}"
            )));
        }
        Ok((resolved_key, path))
    }

    // --- file state ---------------------------------------------------------

    /// Reads a bounded file snapshot; `None` content means the file is absent.
    fn snapshot_file(
        &self,
        path: &Path,
        label: &str,
    ) -> Result<(FileState, Option<Vec<u8>>), CheckpointError> {
        match std::fs::symlink_metadata(path) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                Ok((FileState::Absent, None))
            }
            Err(error) => Err(CheckpointError::Io(error)),
            Ok(metadata) => {
                if metadata.file_type().is_symlink() {
                    return Err(refused(
                        Refusal::JailEscape,
                        format!("{label:?} is a symlink; symlink targets are refused"),
                    ));
                }
                if !metadata.is_file() {
                    return Err(refused(
                        Refusal::ToolSchema,
                        format!("{label:?} is not a regular file"),
                    ));
                }
                if metadata.len() > self.max_file_bytes {
                    return Err(CheckpointError::FileTooLarge {
                        path: label.to_owned(),
                        size: metadata.len(),
                        cap: self.max_file_bytes,
                    });
                }
                let bytes = std::fs::read(path)?;
                let size = u64::try_from(bytes.len()).map_err(|_| {
                    CheckpointError::Corrupt(format!(
                        "file {label:?} length does not fit u64 on this platform"
                    ))
                })?;
                if size > self.max_file_bytes {
                    return Err(CheckpointError::FileTooLarge {
                        path: label.to_owned(),
                        size,
                        cap: self.max_file_bytes,
                    });
                }
                Ok((
                    FileState::Present {
                        sha256: sha256_hex(&bytes),
                        size,
                    },
                    Some(bytes),
                ))
            }
        }
    }

    /// Reads the current bounded state without keeping the bytes.
    fn read_state(&self, path: &Path, label: &str) -> Result<FileState, CheckpointError> {
        self.snapshot_file(path, label).map(|(state, _)| state)
    }

    /// Loads and integrity-checks one content-addressed blob.
    fn load_blob(&self, sha256: &str) -> Result<Vec<u8>, CheckpointError> {
        let row: Option<(i64, Vec<u8>)> = self
            .database
            .with_connection(|connection| {
                connection
                    .query_row(
                        "SELECT size, bytes FROM checkpoint_blobs WHERE sha256 = ?1",
                        rusqlite::params![sha256],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .optional()
            })
            .map_err(CheckpointError::Db)?;
        let (stored_size, bytes) = row.ok_or_else(|| {
            CheckpointError::Corrupt(format!(
                "content-addressed blob {sha256} is missing from checkpoint_blobs"
            ))
        })?;
        let size = u64::try_from(bytes.len()).map_err(|_| {
            CheckpointError::Corrupt(format!("blob {sha256} length does not fit u64"))
        })?;
        if i64::try_from(size).ok() != Some(stored_size) {
            return Err(CheckpointError::Corrupt(format!(
                "blob {sha256} stored size {stored_size} does not match its {} bytes",
                bytes.len()
            )));
        }
        if size > MAX_CHECKPOINT_BYTES {
            return Err(CheckpointError::Corrupt(format!(
                "blob {sha256} is {size} bytes, above the hard checkpoint bound"
            )));
        }
        if sha256_hex(&bytes) != sha256 {
            return Err(CheckpointError::Corrupt(format!(
                "blob {sha256} fails its content-addressed SHA-256 check"
            )));
        }
        Ok(bytes)
    }

    // --- restore phases -----------------------------------------------------

    fn validate_plan(&self, plan: &PathPlan) -> Result<(), CheckpointError> {
        let (key, path) = self.resolve_key(&plan.path)?;
        let current = self.read_state(&path, &key)?;
        if current != plan.expected {
            return Err(CheckpointError::Drift {
                path: plan.path.clone(),
                expected: plan.expected.describe(),
                actual: current.describe(),
            });
        }
        if let FileState::Present { sha256, size } = &plan.target {
            let bytes = self.load_blob(sha256)?;
            let actual = u64::try_from(bytes.len()).map_err(|_| {
                CheckpointError::Corrupt(format!("blob {sha256} length does not fit u64"))
            })?;
            if actual != *size {
                return Err(CheckpointError::Corrupt(format!(
                    "blob {sha256} is {actual} bytes but the checkpoint recorded {size}"
                )));
            }
        }
        Ok(())
    }

    fn apply_plan(&self, plan: &PathPlan) -> Result<(), CheckpointError> {
        let (key, path) = self.resolve_key(&plan.path)?;
        // Re-check immediately before the write: a file changed between the
        // validation phase and this point is drift, not something to clobber.
        let current = self.read_state(&path, &key)?;
        if current != plan.expected {
            return Err(CheckpointError::Drift {
                path: plan.path.clone(),
                expected: plan.expected.describe(),
                actual: current.describe(),
            });
        }
        match &plan.target {
            FileState::Present { sha256, size } => {
                let bytes = self.load_blob(sha256)?;
                let actual = u64::try_from(bytes.len()).map_err(|_| {
                    CheckpointError::Corrupt(format!("blob {sha256} length does not fit u64"))
                })?;
                if actual != *size {
                    return Err(CheckpointError::Corrupt(format!(
                        "blob {sha256} is {actual} bytes but the checkpoint recorded {size}"
                    )));
                }
                self.jail
                    .write_atomic(&plan.path, &bytes)
                    .map_err(map_tool_error)?;
                let restored = self.read_state(&path, &key)?;
                if restored != plan.target {
                    return Err(CheckpointError::Corrupt(format!(
                        "restored file {:?} does not match the recorded pre-image ({})",
                        plan.path,
                        plan.target.describe()
                    )));
                }
            }
            FileState::Absent => {
                match std::fs::remove_file(&path) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(CheckpointError::Io(error)),
                }
                if path.exists() {
                    return Err(CheckpointError::Corrupt(format!(
                        "restored absence for {:?} but the path still exists",
                        plan.path
                    )));
                }
            }
        }
        Ok(())
    }

    fn mark_restored(&self, run_id: RunId, seqs: &[u64]) -> Result<(), CheckpointError> {
        let run_text = run_id.to_string();
        let restored_at = Timestamp::now();
        let restored_at_text = restored_at.as_str().to_owned();
        self.database
            .with_connection(|connection| {
                let transaction = connection.transaction()?;
                for seq in seqs {
                    transaction.execute(
                        "UPDATE checkpoints SET restored_at = ?3
                         WHERE run_id = ?1 AND seq = ?2",
                        rusqlite::params![
                            run_text,
                            i64::try_from(*seq).unwrap_or(i64::MAX),
                            restored_at_text
                        ],
                    )?;
                }
                transaction.commit()?;
                Ok(())
            })
            .map_err(CheckpointError::Db)
    }

    // --- persistence --------------------------------------------------------

    /// Deletes one journal entry under an explicit safety mode, and drops its
    /// content-addressed pre-image only when no other entry still references it.
    ///
    /// The row is re-read inside the delete transaction, so a concurrent
    /// `record_after` that completed the entry is refused rather than erased.
    fn remove_entry(
        &self,
        run_id: RunId,
        seq: u64,
        mode: EntryRemoval,
    ) -> Result<bool, CheckpointError> {
        let seq_i64 = i64::try_from(seq).map_err(|_| {
            refused(
                Refusal::ToolSchema,
                format!("checkpoint seq {seq} exceeds the SQLite INTEGER range"),
            )
        })?;
        let run_text = run_id.to_string();
        let outcome: Result<bool, CheckpointError> = self
            .database
            .with_connection(|connection| {
                let transaction = connection.transaction()?;
                let row: Option<RowRemoval> = transaction
                    .query_row(
                        "SELECT completed, before_sha256, before_size, after_sha256, after_size
                         FROM checkpoints WHERE run_id = ?1 AND seq = ?2",
                        rusqlite::params![run_text, seq_i64],
                        |row| {
                            Ok(RowRemoval {
                                completed: row.get(0)?,
                                before_sha256: row.get(1)?,
                                before_size: row.get(2)?,
                                after_sha256: row.get(3)?,
                                after_size: row.get(4)?,
                            })
                        },
                    )
                    .optional()?;
                let Some(row) = row else {
                    transaction.commit()?;
                    return Ok(Ok(false));
                };
                let removable = match mode {
                    EntryRemoval::IncompleteOnly => row.completed == 0,
                    EntryRemoval::NoOp => {
                        row.completed == 1
                            && row.before_sha256 == row.after_sha256
                            && row.before_size == row.after_size
                    }
                };
                if !removable {
                    // Dropping the transaction rolls the read back; nothing is
                    // deleted.
                    return Ok(Err(CheckpointError::NotRemovable {
                        run_id: run_text,
                        seq,
                        reason: mode.not_removable_reason(),
                    }));
                }
                let deleted = transaction.execute(
                    "DELETE FROM checkpoints WHERE run_id = ?1 AND seq = ?2",
                    rusqlite::params![run_text, seq_i64],
                )?;
                if deleted != 1 {
                    return Ok(Err(CheckpointError::Corrupt(format!(
                        "checkpoint ({run_text}, {seq}) disappeared during removal"
                    ))));
                }
                // Targeted orphan cleanup: the pre-image is shared by digest, so
                // only delete it when no remaining entry references it.
                if let Some(sha256) = row.before_sha256 {
                    transaction.execute(
                        "DELETE FROM checkpoint_blobs
                         WHERE sha256 = ?1
                           AND sha256 NOT IN (
                               SELECT before_sha256 FROM checkpoints
                               WHERE before_sha256 IS NOT NULL
                           )",
                        rusqlite::params![sha256],
                    )?;
                }
                transaction.commit()?;
                Ok(Ok(true))
            })
            .map_err(CheckpointError::Db)?;
        outcome
    }

    fn require_run(&self, run_id: RunId) -> Result<(), CheckpointError> {
        let count: i64 = self
            .database
            .with_connection(|connection| {
                connection.query_row(
                    "SELECT COUNT(*) FROM runs WHERE id = ?1",
                    rusqlite::params![run_id.to_string()],
                    |row| row.get(0),
                )
            })
            .map_err(CheckpointError::Db)?;
        if count == 0 {
            return Err(CheckpointError::RunNotFound(run_id.to_string()));
        }
        Ok(())
    }

    fn query_checkpoint(
        &self,
        run_id: RunId,
        seq: u64,
    ) -> Result<Option<FileCheckpoint>, CheckpointError> {
        let seq_i64 = i64::try_from(seq).map_err(|_| {
            refused(
                Refusal::ToolSchema,
                format!("checkpoint seq {seq} exceeds the SQLite INTEGER range"),
            )
        })?;
        let row = self
            .database
            .with_connection(|connection| {
                connection
                    .query_row(
                        "SELECT checkpoint_id, run_id, seq, path, existed_before,
                                before_sha256, before_size, after_sha256, after_size,
                                completed, created_at, completed_at, restored_at
                         FROM checkpoints WHERE run_id = ?1 AND seq = ?2",
                        rusqlite::params![run_id.to_string(), seq_i64],
                        row_to_data,
                    )
                    .optional()
            })
            .map_err(CheckpointError::Db)?;
        row.map(decode_row).transpose()
    }

    fn query_checkpoint_by_id(
        &self,
        checkpoint_id: &str,
    ) -> Result<Option<FileCheckpoint>, CheckpointError> {
        let row = self
            .database
            .with_connection(|connection| {
                connection
                    .query_row(
                        "SELECT checkpoint_id, run_id, seq, path, existed_before,
                                before_sha256, before_size, after_sha256, after_size,
                                completed, created_at, completed_at, restored_at
                         FROM checkpoints WHERE checkpoint_id = ?1",
                        rusqlite::params![checkpoint_id],
                        row_to_data,
                    )
                    .optional()
            })
            .map_err(CheckpointError::Db)?;
        row.map(decode_row).transpose()
    }

    fn query_entries(
        &self,
        run_id: RunId,
        from_seq: u64,
    ) -> Result<Vec<FileCheckpoint>, CheckpointError> {
        let from_i64 = i64::try_from(from_seq).map_err(|_| {
            refused(
                Refusal::ToolSchema,
                format!("checkpoint seq {from_seq} exceeds the SQLite INTEGER range"),
            )
        })?;
        let rows = self
            .database
            .with_connection(|connection| {
                let mut statement = connection.prepare(
                    "SELECT checkpoint_id, run_id, seq, path, existed_before,
                            before_sha256, before_size, after_sha256, after_size,
                            completed, created_at, completed_at, restored_at
                     FROM checkpoints
                     WHERE run_id = ?1 AND seq >= ?2
                     ORDER BY seq DESC",
                )?;
                let mapped = statement
                    .query_map(rusqlite::params![run_id.to_string(), from_i64], row_to_data)?;
                let mut rows = Vec::new();
                for row in mapped {
                    rows.push(row?);
                }
                Ok(rows)
            })
            .map_err(CheckpointError::Db)?;
        rows.into_iter().map(decode_row).collect()
    }
}

/// One path to restore: the state the current file must be in, and the state to
/// put it back to.
struct PathPlan {
    path: String,
    expected: FileState,
    target: FileState,
    seqs: Vec<u64>,
}

/// How [`CheckpointJournal::remove_entry`] is allowed to delete a row.
#[derive(Clone, Copy)]
enum EntryRemoval {
    /// Only an entry whose after-state is still unknown (no mutation ran).
    IncompleteOnly,
    /// Only a completed entry whose after-state equals its pre-image.
    NoOp,
}

impl EntryRemoval {
    /// Human-readable reason used when the row is refused deletion.
    fn not_removable_reason(self) -> &'static str {
        match self {
            Self::IncompleteOnly => "it already records a mutation (completed)",
            Self::NoOp => "its after-state differs from the pre-image",
        }
    }
}

/// The columns [`CheckpointJournal::remove_entry`] needs to decide safely.
struct RowRemoval {
    completed: i64,
    before_sha256: Option<String>,
    before_size: Option<i64>,
    after_sha256: Option<String>,
    after_size: Option<i64>,
}

/// Blob to insert for a journal entry.
struct BlobWrite {
    sha256: String,
    size: i64,
    bytes: Vec<u8>,
}

/// Raw `checkpoints` row before validation.
struct RowData {
    checkpoint_id: String,
    run_id: String,
    seq: i64,
    path: String,
    existed_before: i64,
    before_sha256: Option<String>,
    before_size: Option<i64>,
    after_sha256: Option<String>,
    after_size: Option<i64>,
    completed: i64,
    created_at: String,
    completed_at: Option<String>,
    restored_at: Option<String>,
}

fn row_to_data(row: &rusqlite::Row<'_>) -> rusqlite::Result<RowData> {
    Ok(RowData {
        checkpoint_id: row.get(0)?,
        run_id: row.get(1)?,
        seq: row.get(2)?,
        path: row.get(3)?,
        existed_before: row.get(4)?,
        before_sha256: row.get(5)?,
        before_size: row.get(6)?,
        after_sha256: row.get(7)?,
        after_size: row.get(8)?,
        completed: row.get(9)?,
        created_at: row.get(10)?,
        completed_at: row.get(11)?,
        restored_at: row.get(12)?,
    })
}

fn decode_row(row: RowData) -> Result<FileCheckpoint, CheckpointError> {
    let run_id = row.run_id.parse::<RunId>().map_err(|_| {
        CheckpointError::Corrupt(format!(
            "checkpoint {} has an invalid run id {:?}",
            row.checkpoint_id, row.run_id
        ))
    })?;
    let seq = u64::try_from(row.seq).map_err(|_| {
        CheckpointError::Corrupt(format!(
            "checkpoint {} has an invalid seq {}",
            row.checkpoint_id, row.seq
        ))
    })?;
    if seq == 0 {
        return Err(CheckpointError::Corrupt(format!(
            "checkpoint {} has seq 0; journal sequences start at 1",
            row.checkpoint_id
        )));
    }
    let existed_before = match row.existed_before {
        0 => false,
        1 => true,
        other => {
            return Err(CheckpointError::Corrupt(format!(
                "checkpoint {} has existed_before {other}",
                row.checkpoint_id
            )))
        }
    };
    let before_sha256 = validate_stored_sha(row.before_sha256, &row.checkpoint_id)?;
    let before_size = validate_stored_size(row.before_size, &row.checkpoint_id)?;
    let before = if existed_before {
        FileState::Present {
            sha256: before_sha256.ok_or_else(|| {
                CheckpointError::Corrupt(format!(
                    "checkpoint {} says the file existed but has no pre-image digest",
                    row.checkpoint_id
                ))
            })?,
            size: before_size.ok_or_else(|| {
                CheckpointError::Corrupt(format!(
                    "checkpoint {} says the file existed but has no pre-image size",
                    row.checkpoint_id
                ))
            })?,
        }
    } else {
        if before_sha256.is_some() || before_size.is_some() {
            return Err(CheckpointError::Corrupt(format!(
                "checkpoint {} says the file was absent but stores pre-image metadata",
                row.checkpoint_id
            )));
        }
        FileState::Absent
    };
    let completed = match row.completed {
        0 => false,
        1 => true,
        other => {
            return Err(CheckpointError::Corrupt(format!(
                "checkpoint {} has completed {other}",
                row.checkpoint_id
            )))
        }
    };
    let after_sha256 = validate_stored_sha(row.after_sha256, &row.checkpoint_id)?;
    let after_size = validate_stored_size(row.after_size, &row.checkpoint_id)?;
    let after = if completed {
        match (after_sha256, after_size) {
            (Some(sha256), Some(size)) => Some(FileState::Present { sha256, size }),
            (None, None) => Some(FileState::Absent),
            _ => {
                return Err(CheckpointError::Corrupt(format!(
                    "checkpoint {} has a partial after-state",
                    row.checkpoint_id
                )))
            }
        }
    } else {
        if after_sha256.is_some() || after_size.is_some() || row.completed_at.is_some() {
            return Err(CheckpointError::Corrupt(format!(
                "checkpoint {} is incomplete but carries after-state metadata",
                row.checkpoint_id
            )));
        }
        None
    };
    let created_at = Timestamp::parse(&row.created_at).map_err(|_| {
        CheckpointError::Corrupt(format!(
            "checkpoint {} has an invalid created_at {:?}",
            row.checkpoint_id, row.created_at
        ))
    })?;
    let completed_at =
        parse_optional_timestamp(row.completed_at, &row.checkpoint_id, "completed_at")?;
    let restored_at = parse_optional_timestamp(row.restored_at, &row.checkpoint_id, "restored_at")?;
    Ok(FileCheckpoint {
        checkpoint_id: row.checkpoint_id,
        run_id,
        seq,
        path: row.path,
        before,
        after,
        created_at,
        completed_at,
        restored_at,
    })
}

fn validate_stored_sha(
    value: Option<String>,
    checkpoint_id: &str,
) -> Result<Option<String>, CheckpointError> {
    match value {
        Some(sha256) if is_sha256_hex(&sha256) => Ok(Some(sha256)),
        Some(sha256) => Err(CheckpointError::Corrupt(format!(
            "checkpoint {checkpoint_id} has a malformed SHA-256 {sha256:?}"
        ))),
        None => Ok(None),
    }
}

fn validate_stored_size(
    value: Option<i64>,
    checkpoint_id: &str,
) -> Result<Option<u64>, CheckpointError> {
    match value {
        Some(size) => {
            let size = u64::try_from(size).map_err(|_| {
                CheckpointError::Corrupt(format!(
                    "checkpoint {checkpoint_id} has a negative size {size}"
                ))
            })?;
            Ok(Some(size))
        }
        None => Ok(None),
    }
}

fn parse_optional_timestamp(
    value: Option<String>,
    checkpoint_id: &str,
    field: &str,
) -> Result<Option<Timestamp>, CheckpointError> {
    match value {
        Some(raw) => Timestamp::parse(&raw).map(Some).map_err(|_| {
            CheckpointError::Corrupt(format!(
                "checkpoint {checkpoint_id} has an invalid {field} {raw:?}"
            ))
        }),
        None => Ok(None),
    }
}

/// Groups entries by path and builds the validated rewind plans.
fn build_plans(
    entries: &[FileCheckpoint],
    run_id: RunId,
) -> Result<Vec<PathPlan>, CheckpointError> {
    // `entries` is newest-first; keep first-seen path order stable.
    let mut order: Vec<String> = Vec::new();
    let mut groups: std::collections::HashMap<String, Vec<&FileCheckpoint>> =
        std::collections::HashMap::new();
    for entry in entries {
        if !groups.contains_key(&entry.path) {
            order.push(entry.path.clone());
        }
        groups
            .entry(entry.path.clone())
            .or_default()
            .push(entry);
    }
    let mut plans = Vec::new();
    for path in order {
        let group = groups.remove(&path).ok_or_else(|| {
            CheckpointError::Corrupt("path group disappeared while building plans".to_owned())
        })?;
        let newest = group
            .first()
            .ok_or_else(|| CheckpointError::Corrupt("empty checkpoint path group".to_owned()))?;
        let expected = newest
            .after
            .clone()
            .ok_or_else(|| CheckpointError::Incomplete {
                run_id: run_id.to_string(),
                seq: newest.seq,
            })?;
        // Newer entry's before-state must equal the next older entry's
        // after-state; otherwise the journal itself is inconsistent and writing
        // the "oldest before" would silently discard an unjournaled state.
        for pair in group.windows(2) {
            let newer = pair[0];
            let older = pair[1];
            let older_after = older
                .after
                .clone()
                .ok_or_else(|| CheckpointError::Incomplete {
                    run_id: run_id.to_string(),
                    seq: older.seq,
                })?;
            if newer.before != older_after {
                return Err(CheckpointError::Corrupt(format!(
                    "journal chain for {path:?} is inconsistent between seq {} \
                     (before {}) and seq {} (after {})",
                    newer.seq,
                    newer.before.describe(),
                    older.seq,
                    older_after.describe()
                )));
            }
        }
        let oldest = group
            .last()
            .ok_or_else(|| CheckpointError::Corrupt("empty checkpoint path group".to_owned()))?;
        let mut seqs: Vec<u64> = group.iter().map(|entry| entry.seq).collect();
        seqs.sort_unstable_by(|left, right| right.cmp(left));
        plans.push(PathPlan {
            path,
            expected,
            target: oldest.before.clone(),
            seqs,
        });
    }
    Ok(plans)
}

/// Canonicalises a target, resolving the nearest existing ancestor when the
/// final components do not exist yet.
fn canonicalize_target(resolved: &Path) -> Result<PathBuf, CheckpointError> {
    let mut missing: Vec<std::ffi::OsString> = Vec::new();
    let mut probe = resolved;
    loop {
        match std::fs::canonicalize(probe) {
            Ok(mut canonical) => {
                for name in missing.iter().rev() {
                    canonical.push(name);
                }
                return Ok(canonical);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let name = probe.file_name().ok_or_else(|| {
                    CheckpointError::Io(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        format!(
                            "checkpoint target {} has no existing ancestor inside the workspace",
                            resolved.display()
                        ),
                    ))
                })?;
                missing.push(name.to_os_string());
                probe = probe.parent().ok_or_else(|| {
                    CheckpointError::Io(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        format!(
                            "checkpoint target {} has no existing ancestor inside the workspace",
                            resolved.display()
                        ),
                    ))
                })?;
            }
            Err(error) => return Err(CheckpointError::Io(error)),
        }
    }
}

fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

/// Validates a caller-supplied checkpoint id at the API boundary.
fn validate_checkpoint_id(checkpoint_id: &str) -> Result<(), CheckpointError> {
    if checkpoint_id.trim().is_empty() {
        return Err(refused(Refusal::ToolSchema, "checkpoint id is empty"));
    }
    if checkpoint_id.len() > MAX_CHECKPOINT_ID_BYTES {
        return Err(refused(
            Refusal::ToolSchema,
            format!("checkpoint id exceeds {MAX_CHECKPOINT_ID_BYTES} bytes"),
        ));
    }
    if checkpoint_id.chars().any(char::is_control) {
        return Err(refused(
            Refusal::ToolSchema,
            "checkpoint id contains control characters",
        ));
    }
    Ok(())
}

fn map_tool_error(error: ToolError) -> CheckpointError {
    CheckpointError::Tool {
        refusal: error.refusal(),
        detail: error.detail().to_owned(),
    }
}

fn refused(refusal: Refusal, detail: impl Into<String>) -> CheckpointError {
    CheckpointError::Tool {
        refusal,
        detail: detail.into(),
    }
}

fn insert_error(error: rusqlite::Error, run_id: &str, seq: u64) -> CheckpointError {
    if let rusqlite::Error::SqliteFailure(_, Some(message)) = &error {
        if message.contains("UNIQUE constraint failed: checkpoints.run_id, checkpoints.seq") {
            return CheckpointError::DuplicateSeq {
                run_id: run_id.to_owned(),
                seq,
            };
        }
        if message.contains("FOREIGN KEY constraint failed") {
            return CheckpointError::RunNotFound(run_id.to_owned());
        }
    }
    CheckpointError::Db(DbError::Sqlite(error))
}
