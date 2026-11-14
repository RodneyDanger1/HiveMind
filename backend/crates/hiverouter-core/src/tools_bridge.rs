//! Core -> tools bridge (Phase 6 wiring for the patched frozen commands).
//!
//! `hiverouter-core` depends on `hiverouter-tools` (never the reverse). This
//! module is the single wiring point for the tools-owned capabilities used by
//! the WS command dispatch and the `/diagnose` route:
//!
//! - `SETTINGS_STATUS` -> [`hiverouter_tools::commands::settings_status_command`]
//! - `FILE_TREE_LIST` / `FILE_READ` -> the workspace jail ([`WorkspaceJail`]),
//!   with the configured data directory and approval-ledger directory denied
//!   (the same containment the engine's read-only jail applies)
//! - `APPROVAL_RESOLVE` -> the single approval gate + nonce/replay state machine,
//!   and exactly one append-only ledger row per committed decision
//!   ([`ToolsBridge::resolve_approval_ledgered`]); refusals never ledger.
//! - model-originated side effects -> one committed approval row and the frozen
//!   `APPROVAL_REQUEST` payload ([`ToolsBridge::request_model_tool_approval`]);
//!   the producer never executes the tool.
//! - `/diagnose` -> [`hiverouter_tools::health::diagnostics`]
//!
//! When a capability is genuinely unavailable (no usable workspace root), the
//! bridge returns the truthful named refusal `E_FILE_TREE_UNAVAILABLE` instead
//! of a generic internal error.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use hiverouter_contract::command;
use hiverouter_contract::{event, PermissionPolicy, Refusal, RunId, TaskId, Timestamp};
use hiverouter_tools::approval::{ApprovalRequest, ApprovalResolution};
use hiverouter_tools::git::{Git, GitDiffRequest, GitLogRequest};
use hiverouter_tools::jail::WorkspaceJail;
use hiverouter_tools::ledger::{append_approval_decision, RunLedger, LEDGER_FILE_NAME};
use hiverouter_tools::tool::{GitDiffArgs, GitLogArgs, GitStatusArgs};
use hiverouter_tools::ToolError;

use crate::checkpoint::{CheckpointError, CheckpointJournal, FileCheckpoint};
use crate::db::Database;

/// How many times the bridge re-allocates a checkpoint sequence when a
/// concurrent call for the same run took it first (bounded retry, never a loop).
const CHECKPOINT_SEQ_ATTEMPTS: u8 = 3;

/// Maximum workspace root entries listed by `harness_diagnose` (names only).
const MAX_DIAGNOSE_TOP_LEVEL: usize = 32;

/// One row per agent tier: whether `harness_diagnose` is on its allowlist.
///
/// Diagnostics availability is a capability fact, so it is reported from the
/// same frozen registry the executor uses rather than restated by hand.
fn diagnostics_availability() -> Vec<serde_json::Value> {
    use hiverouter_tools::tool::{AgentTier, ToolId, ToolRegistry};
    let registry = ToolRegistry::standard();
    AgentTier::all()
        .into_iter()
        .map(|tier| {
            serde_json::json!({
                "tier": tier.label(),
                "granted": registry.is_allowed(tier, ToolId::HarnessDiagnose),
            })
        })
        .collect()
}

/// Wired tools capabilities for the local server.
#[derive(Debug)]
pub struct ToolsBridge {
    jail: Option<WorkspaceJail>,
    jail_error: Option<String>,
    gate: hiverouter_tools::approval::ApprovalGate,
    workspace_root: PathBuf,
    data_dir: Option<PathBuf>,
    db: Arc<Database>,
    /// The one open append-only ledger per directory: a single instance keeps
    /// the per-file monotonic `seq` counter and its write lock meaningful across
    /// commands (two fresh `RunLedger`s would each restart at seq 0).
    ledger: Mutex<Option<(PathBuf, RunLedger)>>,
    /// Config-owned side-effect kill switch (never a raw env read at execute time).
    side_effects_enabled: bool,
    /// Optional CAP-5 journal: when present, a `file_write` / `apply_patch`
    /// before-image is captured before the grant CAS and settled after the
    /// mutation attempt. `None` leaves the pre-CAP-5 path byte-for-byte intact.
    checkpoint_journal: Option<CheckpointJournal>,
}

/// Appends the SQLite sidecar suffix to the whole path (`foo.sqlite` ->
/// `foo.sqlite-wal`) instead of replacing the extension.
fn sqlite_sidecar(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(suffix);
    PathBuf::from(value)
}

impl ToolsBridge {
    /// Creates a bridge that denies the conventional `<workspace_root>/data`
    /// subtree **and** the directory containing the SQLite database.
    ///
    /// This constructor exists for test fixtures and compatibility callers.
    /// Deriving the denied directory from `db.path()` is a best-effort fallback:
    /// production wiring must use [`Self::new_with_data_dir`] with the configured
    /// `data_dir`, because the configured data directory also owns snapshots and
    /// the approval ledger and may differ from the database's parent when
    /// `--db` is customised.
    ///
    /// A workspace root that cannot be opened leaves the jail unavailable (file
    /// commands then refuse `E_FILE_TREE_UNAVAILABLE`).
    #[must_use]
    pub fn new(workspace_root: &Path, db: Arc<Database>) -> Self {
        let mut denied = vec![workspace_root.join("data")];
        if let Some(parent) = db.path().parent() {
            if !parent.as_os_str().is_empty() {
                denied.push(parent.to_path_buf());
            }
        }
        Self::new_with_denied_paths(workspace_root, &denied, db)
    }

    fn new_with_denied_paths(
        workspace_root: &Path,
        denied_paths: &[PathBuf],
        db: Arc<Database>,
    ) -> Self {
        let (jail, jail_error) = Self::open_jail_with_denies(workspace_root, denied_paths);
        Self {
            jail,
            jail_error,
            gate: hiverouter_tools::approval::ApprovalGate::new(),
            workspace_root: workspace_root.to_path_buf(),
            data_dir: None,
            db,
            ledger: Mutex::new(None),
            side_effects_enabled: true,
            checkpoint_journal: None,
        }
    }

    /// Creates the bridge with the configured data directory denied.
    ///
    /// The jail refuses every path under `data_dir` and under
    /// `data_dir/ledger` (the approval ledger). This is the same containment the
    /// engine's read-only workspace jail applies, extended to the bridge's
    /// `FILE_READ`/`FILE_TREE_LIST` surface and — critically — to approved
    /// side effects, so an approved `file_write` cannot corrupt the SQLite
    /// database, snapshots, or the append-only approval ledger.
    ///
    /// A workspace root that cannot be opened leaves the jail unavailable (file
    /// commands then refuse `E_FILE_TREE_UNAVAILABLE`).
    #[must_use]
    pub fn new_with_data_dir(workspace_root: &Path, data_dir: &Path, db: Arc<Database>) -> Self {
        let denied = Self::data_denials(workspace_root, data_dir, db.path());
        let mut bridge = Self::new_with_denied_paths(workspace_root, &denied, db);
        bridge.data_dir = Some(data_dir.to_path_buf());
        bridge
    }

    /// When the workspace lives under `data_dir` (the default desktop layout),
    /// denying the whole data directory would deny every workspace file. In
    /// that case deny only the sensitive data surfaces; otherwise keep the
    /// stronger whole-directory denial for an external workspace.
    pub(crate) fn data_denials(
        workspace_root: &Path,
        data_dir: &Path,
        db_path: &Path,
    ) -> Vec<PathBuf> {
        let workspace_inside_data = workspace_root.starts_with(data_dir);
        let mut denied = Vec::new();
        if !workspace_inside_data {
            denied.push(data_dir.to_path_buf());
        }
        denied.push(Self::approval_ledger_dir(data_dir));
        for name in [
            "audit",
            "ledger",
            "snapshots",
            "window",
            "provider.key",
            "runtime.json",
            "launch.token",
            "server.stdout.log",
            "server.stderr.log",
            "launcher.log",
        ] {
            denied.push(data_dir.join(name));
        }
        // A custom `--db` may live outside `data_dir`; deny the exact database
        // file and its SQLite sidecars without denying a parent that may also
        // contain the workspace.
        for candidate in [
            db_path.to_path_buf(),
            sqlite_sidecar(db_path, "-wal"),
            sqlite_sidecar(db_path, "-shm"),
        ] {
            denied.push(candidate);
        }
        denied
    }

    /// Replaces the denied data directory on an existing bridge.
    ///
    /// Useful for fixtures that build the bridge first and only then know the
    /// data root; production wiring should prefer [`Self::new_with_data_dir`].
    #[must_use]
    pub fn with_data_dir(mut self, data_dir: &Path) -> Self {
        let db_path = self.db.path();
        let denied = Self::data_denials(&self.workspace_root, data_dir, db_path);
        let (jail, jail_error) = Self::open_jail_with_denies(&self.workspace_root, &denied);
        self.jail = jail;
        self.jail_error = jail_error;
        self.data_dir = Some(data_dir.to_path_buf());
        self
    }

    fn open_jail_with_denies(
        workspace_root: &Path,
        denied_paths: &[PathBuf],
    ) -> (Option<WorkspaceJail>, Option<String>) {
        if !workspace_root.is_dir() {
            return (
                None,
                Some(format!(
                    "workspace root {} is not an existing directory",
                    workspace_root.display()
                )),
            );
        }
        match WorkspaceJail::open(workspace_root) {
            Ok(mut jail) => {
                for path in denied_paths {
                    jail = jail.deny_path(path);
                }
                (Some(jail), None)
            }
            Err(error) => (
                None,
                Some(format!("{}: {}", error.code(), error.detail())),
            ),
        }
    }

    /// Creates the tools-owned approval table (idempotent). Called from the core
    /// migration/boot path so the tools layer never opens its own connection.
    ///
    /// # Errors
    /// `E_INTERNAL_ERROR` when the DDL fails.
    pub fn migrate(db: &Database) -> Result<(), Refusal> {
        db.with_connection_refusal(|conn| {
            hiverouter_tools::approval::migrate(conn).map_err(|error| error.refusal())
        })
    }

    /// Boot-time orphan reaper: no in-process waiter survives a restart, so every
    /// still-pending approval is consumed (`decision = expired`) regardless of its
    /// remaining TTL. A late UI decision is then refused by name (timeout/replay)
    /// and can never execute a side effect.
    ///
    /// # Errors
    /// `E_INTERNAL_ERROR` when the approval table cannot be read/updated.
    pub fn expire_pending_approvals(db: &Database, now_unix_secs: u64) -> Result<u64, Refusal> {
        db.with_connection_refusal(|conn| {
            hiverouter_tools::approval::expire_orphaned_pending(conn, now_unix_secs)
                .map_err(|error| error.refusal())
        })
    }

    /// Builder for tests: a bridge whose approval TTL differs from the default.
    #[must_use]
    pub fn with_approval_ttl(mut self, ttl_secs: u64) -> Self {
        self.gate = hiverouter_tools::approval::ApprovalGate::with_ttl(ttl_secs);
        self
    }

    /// Applies the config-owned side-effect gate.
    #[must_use]
    pub fn with_side_effects_enabled(mut self, enabled: bool) -> Self {
        self.side_effects_enabled = enabled;
        self
    }

    /// Installs the CAP-5 checkpoint journal used to snapshot the before-image
    /// of a file-mutating granted side effect (`file_write`, `apply_patch`).
    ///
    /// The journal is optional and additive: without it the execution path is
    /// unchanged. It must be opened over the **same** workspace root as this
    /// bridge - a mismatch is refused with `E_INTERNAL_ERROR` before the grant
    /// is verified rather than silently snapshotting a different file.
    #[must_use]
    pub fn with_checkpoint_journal(mut self, journal: CheckpointJournal) -> Self {
        self.checkpoint_journal = Some(journal);
        self
    }

    /// The installed checkpoint journal, when CAP-5 journalling is enabled.
    #[must_use]
    pub fn checkpoint_journal(&self) -> Option<&CheckpointJournal> {
        self.checkpoint_journal.as_ref()
    }

    /// Whether the global side-effect kill switch is enabled.
    #[must_use]
    pub fn side_effects_enabled(&self) -> bool {
        self.side_effects_enabled
    }

    /// Marks one pending approval expired (live-wait timeout).
    ///
    /// # Errors
    /// `E_INTERNAL_ERROR` when the approval row cannot be updated.
    pub fn expire_approval(&self, approval_id: &str, now_unix_secs: u64) -> Result<bool, Refusal> {
        self.db.with_connection_refusal(|conn| {
            hiverouter_tools::approval::expire(conn, approval_id, now_unix_secs)
                .map_err(|error| error.refusal())
        })
    }

    /// Marks one pending approval cancelled (run/task cancellation).
    ///
    /// # Errors
    /// `E_INTERNAL_ERROR` when the approval row cannot be updated.
    pub fn cancel_approval(
        &self,
        approval_id: &str,
        now_unix_secs: u64,
    ) -> Result<bool, Refusal> {
        self.db.with_connection_refusal(|conn| {
            hiverouter_tools::approval::cancel_pending(conn, approval_id, now_unix_secs)
                .map_err(|error| error.refusal())
        })
    }

    /// Stored decision label for an approval (`None` when pending/unknown).
    ///
    /// # Errors
    /// `E_INTERNAL_ERROR` when the approval row cannot be read.
    pub fn approval_decision(&self, approval_id: &str) -> Result<Option<String>, Refusal> {
        self.db.with_connection_refusal(|conn| {
            hiverouter_tools::approval::decision_label(conn, approval_id)
                .map_err(|error| error.refusal())
        })
    }

    /// Number of approval rows ever created for a run.
    ///
    /// # Errors
    /// `E_INTERNAL_ERROR` when the count cannot be read.
    pub fn approval_count_for_run(&self, run_id: RunId) -> Result<u64, Refusal> {
        // Policy auto-grants are audited decisions, not human waits. Counting
        // them against MAX_APPROVAL_WAITS_PER_RUN would block a trusted run
        // after eight edits even though no operator prompt ever occurred.
        self.db.with_connection_refusal(|conn| {
            let count: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM approvals
                     WHERE run_id = ?1 AND COALESCE(reason, '') NOT LIKE 'policy:%'",
                    rusqlite::params![run_id.to_string()],
                    |row| row.get(0),
                )
                .map_err(|error| ToolError::from(error).refusal())?;
            u64::try_from(count).map_err(|_| Refusal::InternalError)
        })
    }

    /// Issues and immediately resolves one single-use policy grant. The grant is
    /// bound to the exact run/tool_call/tool/args and is ledgers as
    /// `reason = policy:<name>` before the execution path consumes it. This is
    /// not an approval bypass: `execute_approved_bound` still performs preflight,
    /// bound-grant CAS, jail and at-most-once checks.
    pub fn grant_policy_approval(
        &self,
        run_id: RunId,
        task_id: TaskId,
        tool_call_id: &str,
        tool: &str,
        args: &serde_json::Value,
        policy: PermissionPolicy,
    ) -> Result<String, Refusal> {
        let request = self.request_model_tool_approval(
            run_id,
            task_id,
            tool_call_id,
            tool,
            args,
        )?;
        let nonce = request.nonce.ok_or(Refusal::ApprovalRequired)?;
        let payload = command::ApprovalResolve {
            approval_id: request.approval_id,
            nonce,
            decision: "approve".to_owned(),
            reason: Some(format!("policy:{}", policy.as_str())),
        };
        let ledger_dir = self
            .data_dir
            .as_ref()
            .map(|data_dir| Self::approval_ledger_dir(data_dir))
            .ok_or(Refusal::InternalError)?;
        self.resolve_approval_ledgered(&payload, &ledger_dir)?;
        Ok(request.approval_id.to_string())
    }

    /// Verifies and consumes the execution grant for one side-effect tool call.
    ///
    /// This is the only execution authority: the caller must have completed all
    /// read-only pre-checks first so a doomed call never burns consent.
    ///
    /// This unbound lookup matches `(run, tool, args_sha256)` and is retained
    /// for existing callers. New execution paths must use
    /// [`Self::verify_tool_grant_bound`] so the grant is bound to the exact
    /// `approval_id` and provider `tool_call_id` as well.
    ///
    /// # Errors
    /// `E_APPROVAL_REQUIRED`, `E_APPROVAL_TIMEOUT`, `E_APPROVAL_REPLAY`.
    pub fn verify_tool_grant(
        &self,
        run_id: RunId,
        tool: hiverouter_tools::ToolId,
        args: &serde_json::Value,
        now_unix_secs: u64,
    ) -> Result<(), Refusal> {
        self.db.with_connection_refusal(|conn| {
            self.gate
                .verify_grant(conn, run_id, tool, args, now_unix_secs)
                .map_err(|error| error.refusal())
        })
    }

    /// Verifies and consumes the grant bound to one exact approval request and
    /// provider tool call.
    ///
    /// # Errors
    /// `E_APPROVAL_NOT_FOUND`, `E_APPROVAL_REQUIRED`, `E_APPROVAL_TIMEOUT`,
    /// `E_APPROVAL_REPLAY`.
    pub fn verify_tool_grant_bound(
        &self,
        approval_id: &str,
        tool_call_id: &str,
        run_id: RunId,
        tool: hiverouter_tools::ToolId,
        args: &serde_json::Value,
        now_unix_secs: u64,
    ) -> Result<(), Refusal> {
        self.db.with_connection_refusal(|conn| {
            self.gate
                .verify_grant_bound(
                    conn,
                    approval_id,
                    tool_call_id,
                    run_id,
                    tool,
                    args,
                    now_unix_secs,
                )
                .map_err(|error| error.refusal())
        })
    }

    /// Workspace root the jail was opened against.
    #[must_use]
    pub fn workspace_root(&self) -> &Path {
        &self.workspace_root
    }

    /// Verifies the single-use grant, then commits exactly one preflighted side
    /// effect (C1a split protocol).
    ///
    /// Ordering is load-bearing: the read-only preflight runs **before** the
    /// grant CAS, so a doomed call (escape, oversize, stale hash) never burns
    /// consent; after the CAS exactly one effect is attempted and the grant
    /// stays consumed even if that effect fails (at-most-once).
    ///
    /// When a CAP-5 journal is installed (see
    /// [`Self::with_checkpoint_journal`]) a `file_write` / `apply_patch`
    /// before-image is captured **after** preflight and **before** the grant
    /// CAS, so a capture failure never consumes the approval; a refused grant
    /// voids the capture, so it leaves no restorable checkpoint; a successful
    /// mutation records its after-image.
    ///
    /// # Errors
    /// Named refusals from preflight, `E_APPROVAL_REQUIRED` / `E_APPROVAL_REPLAY`
    /// from the grant CAS, or an execution error after the grant was consumed.
    pub async fn execute_approved(
        &self,
        shell_policy: &hiverouter_tools::shell::ShellPolicy,
        registry: &hiverouter_tools::tool::ToolRegistry,
        tier: hiverouter_tools::tool::AgentTier,
        run_id: RunId,
        tool: &str,
        args: &serde_json::Value,
        cancellation: Option<&tokio_util::sync::CancellationToken>,
    ) -> Result<hiverouter_tools::executor::ExecutedTool, Refusal> {
        self.execute_approved_with_binding(
            shell_policy,
            registry,
            tier,
            run_id,
            tool,
            args,
            None,
            cancellation,
        )
        .await
    }

    /// Bound variant of [`Self::execute_approved`]: the grant must match the
    /// exact committed `approval_id` and provider `tool_call_id` in addition to
    /// `(run, tool, args_sha256)`.
    ///
    /// # Errors
    /// Named refusals from preflight, `E_APPROVAL_NOT_FOUND` /
    /// `E_APPROVAL_REQUIRED` / `E_APPROVAL_TIMEOUT` / `E_APPROVAL_REPLAY` from
    /// the grant CAS, or an execution error after the grant was consumed.
    pub async fn execute_approved_bound(
        &self,
        shell_policy: &hiverouter_tools::shell::ShellPolicy,
        registry: &hiverouter_tools::tool::ToolRegistry,
        tier: hiverouter_tools::tool::AgentTier,
        run_id: RunId,
        approval_id: &str,
        tool_call_id: &str,
        tool: &str,
        args: &serde_json::Value,
        cancellation: Option<&tokio_util::sync::CancellationToken>,
    ) -> Result<hiverouter_tools::executor::ExecutedTool, Refusal> {
        self.execute_approved_with_binding(
            shell_policy,
            registry,
            tier,
            run_id,
            tool,
            args,
            Some((approval_id, tool_call_id)),
            cancellation,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn execute_approved_with_binding(
        &self,
        shell_policy: &hiverouter_tools::shell::ShellPolicy,
        registry: &hiverouter_tools::tool::ToolRegistry,
        tier: hiverouter_tools::tool::AgentTier,
        run_id: RunId,
        tool: &str,
        args: &serde_json::Value,
        binding: Option<(&str, &str)>,
        cancellation: Option<&tokio_util::sync::CancellationToken>,
    ) -> Result<hiverouter_tools::executor::ExecutedTool, Refusal> {
        if !self.side_effects_enabled {
            return Ok(hiverouter_tools::executor::ExecutedTool::Refused {
                code: Refusal::ToolNotAllowed.code().to_owned(),
                detail: "HIVEROUTER_SIDE_EFFECTS=0 disables granted side effects; \
                         the approval was not consumed"
                    .to_owned(),
            });
        }
        let jail = self.require_jail()?;
        let prepared = hiverouter_tools::side_effects::preflight_side_effect(
            jail,
            registry,
            shell_policy,
            tier,
            tool,
            args,
            cancellation,
        )
        .map_err(Self::map_tool_error)?;
        let tool_id = hiverouter_tools::ToolId::from_id(tool).ok_or(Refusal::ToolNotFound)?;
        let now_unix_secs = Timestamp::now().unix_secs().unwrap_or(0);
        // CAP-5: snapshot the file before the grant CAS. A capture failure
        // returns here, so the approval is still unconsumed.
        let captured = self.capture_file_checkpoint(run_id, tool_id, args)?;
        let grant = match binding {
            Some((approval_id, tool_call_id)) => self.verify_tool_grant_bound(
                approval_id,
                tool_call_id,
                run_id,
                tool_id,
                args,
                now_unix_secs,
            ),
            None => self.verify_tool_grant(run_id, tool_id, args, now_unix_secs),
        };
        if let Err(refusal) = grant {
            // A refused grant executed nothing: void the capture so a denied
            // call never leaves a restorable checkpoint behind.
            if let Some(captured) = captured.as_ref() {
                self.void_file_checkpoint(run_id, captured);
            }
            return Err(refusal);
        }
        match hiverouter_tools::side_effects::commit_side_effect(
            jail,
            shell_policy,
            prepared,
            cancellation,
        )
        .await
        {
            Ok(executed) => {
                if let Some(captured) = captured.as_ref() {
                    if let Err(error) = self.settle_file_checkpoint(run_id, captured) {
                        // The effect landed but the after-image could not be
                        // recorded. Reporting success would claim a restorable
                        // checkpoint that does not exist; the grant stays
                        // consumed either way (at-most-once, no duplicate).
                        tracing::error!(
                            code = error.code(),
                            run_id = %run_id,
                            seq = captured.seq,
                            path = %captured.path,
                            "side effect committed but its checkpoint after-image could not be recorded"
                        );
                        return Err(error.refusal());
                    }
                }
                Ok(executed)
            }
            Err(error) => {
                if let Some(captured) = captured.as_ref() {
                    // The commit failed after consent was consumed. Settle
                    // truthfully: an atomic file writer does not leave a partial
                    // file, but a failure after the replace must not discard a
                    // real before-image.
                    if let Err(settle) = self.settle_file_checkpoint(run_id, captured) {
                        tracing::error!(
                            code = settle.code(),
                            run_id = %run_id,
                            seq = captured.seq,
                            path = %captured.path,
                            "failed side effect left an incomplete checkpoint that could not be settled"
                        );
                    }
                }
                Err(Self::map_tool_error(error))
            }
        }
    }

    /// Captures the before-image of the one file a granted tool will mutate.
    ///
    /// Returns `None` when no journal is installed or the tool is not a
    /// single-file mutation. Runs **after** the read-only preflight (a doomed
    /// call never creates a journal row) and **before** the grant CAS (a capture
    /// failure never spends consent).
    fn capture_file_checkpoint(
        &self,
        run_id: RunId,
        tool: hiverouter_tools::ToolId,
        args: &serde_json::Value,
    ) -> Result<Option<FileCheckpoint>, Refusal> {
        let Some(journal) = self.checkpoint_journal.as_ref() else {
            return Ok(None);
        };
        let Some(path) = Self::checkpoint_target(tool, args)? else {
            return Ok(None);
        };
        let bridge_root = self.require_jail()?.root();
        if journal.jail().root() != bridge_root {
            // Journalling a path resolved against another root would snapshot
            // the wrong file; refuse before the grant rather than lie.
            tracing::error!(
                bridge_root = %bridge_root.display(),
                journal_root = %journal.jail().root().display(),
                "checkpoint journal was opened over a different workspace root; refusing before the grant"
            );
            return Err(Refusal::InternalError);
        }
        let mut attempts = 0_u8;
        loop {
            let seq = journal.next_seq(run_id).map_err(|error| error.refusal())?;
            match journal.journal_before(run_id, seq, path) {
                Ok(checkpoint) => return Ok(Some(checkpoint)),
                // A concurrent granted call for the same run took this sequence
                // between `next_seq` and the insert. Re-allocate a bounded
                // number of times; a persistent collision is still a refusal
                // before the grant, so no approval is burned.
                Err(CheckpointError::DuplicateSeq { .. }) if attempts < CHECKPOINT_SEQ_ATTEMPTS => {
                    attempts = attempts.saturating_add(1);
                }
                Err(error) => return Err(error.refusal()),
            }
        }
    }

    /// The workspace path a granted tool mutates, when the journal must snapshot
    /// it. Only the two single-file mutation tools have a before-image.
    fn checkpoint_target(
        tool: hiverouter_tools::ToolId,
        args: &serde_json::Value,
    ) -> Result<Option<&str>, Refusal> {
        if !matches!(
            tool,
            hiverouter_tools::ToolId::FileWrite | hiverouter_tools::ToolId::ApplyPatch
        ) {
            return Ok(None);
        }
        // Preflight already validated the schema; a missing/non-string path is
        // still refused by name instead of silently skipping the snapshot.
        match args.get("path").and_then(serde_json::Value::as_str) {
            Some(path) if !path.trim().is_empty() => Ok(Some(path)),
            _ => Err(Refusal::ToolSchema),
        }
    }

    /// Settles the journal entry after a mutation attempt (success or failure).
    ///
    /// A byte-identical outcome (an `apply_patch` rejection or a no-op write)
    /// removes the entry; a real change records the after-image.
    fn settle_file_checkpoint(
        &self,
        run_id: RunId,
        checkpoint: &FileCheckpoint,
    ) -> Result<(), CheckpointError> {
        let Some(journal) = self.checkpoint_journal.as_ref() else {
            // A capture only exists when a journal was installed; losing it
            // here is an internal inconsistency, never a silent skip.
            return Err(CheckpointError::Corrupt(
                "captured a file checkpoint without an installed journal".to_owned(),
            ));
        };
        journal
            .record_after_or_discard(run_id, checkpoint.seq)
            .map(|_| ())
    }

    /// Voids a captured but never-executed checkpoint after a refused grant.
    ///
    /// The entry is incomplete and therefore not restorable even if the void
    /// itself fails; a void failure is logged because a stale incomplete entry
    /// makes a later full-run rewind refuse.
    fn void_file_checkpoint(&self, run_id: RunId, checkpoint: &FileCheckpoint) {
        let Some(journal) = self.checkpoint_journal.as_ref() else {
            return;
        };
        if let Err(error) = journal.void_entry(run_id, checkpoint.seq) {
            tracing::error!(
                code = error.code(),
                run_id = %run_id,
                seq = checkpoint.seq,
                path = %checkpoint.path,
                "refused grant left an incomplete checkpoint that could not be voided; \
                 it is not restorable but may block a full rewind"
            );
        }
    }

    /// Whether the workspace jail is usable.
    #[must_use]
    pub fn file_access_available(&self) -> bool {
        self.jail.is_some()
    }

    /// Details of why the jail is unavailable (diagnostics only, no secrets).
    #[must_use]
    pub fn jail_error(&self) -> Option<&str> {
        self.jail_error.as_deref()
    }

    /// The single approval gate.
    #[must_use]
    pub fn approval_gate(&self) -> &hiverouter_tools::approval::ApprovalGate {
        &self.gate
    }

    fn require_jail(&self) -> Result<&WorkspaceJail, Refusal> {
        self.jail.as_ref().ok_or(Refusal::FileTreeUnavailable)
    }

    fn map_tool_error(error: ToolError) -> Refusal {
        error.refusal()
    }

    /// Executes one read-only git query through the jailed Git wrapper. This is
    /// async because Git is a child process; the core dispatch calls it without
    /// creating an approval request (read-class only).
    pub async fn execute_git_read(
        &self,
        tool: &str,
        args: &serde_json::Value,
    ) -> Result<String, Refusal> {
        let jail = self.require_jail()?;
        let git = Git::new(jail);
        let text = match tool {
            "git_status" => {
                let parsed: GitStatusArgs = serde_json::from_value(args.clone())
                    .map_err(|_| Refusal::SchemaInvalid)?;
                let result = git.status(parsed.cwd.as_deref()).await.map_err(Self::map_tool_error)?;
                serde_json::to_string(&result).map_err(|_| Refusal::InternalError)?
            }
            "git_diff" => {
                let parsed: GitDiffArgs = serde_json::from_value(args.clone())
                    .map_err(|_| Refusal::SchemaInvalid)?;
                let request = GitDiffRequest {
                    cwd: parsed.cwd,
                    staged: parsed.staged.unwrap_or(false),
                    path: parsed.path,
                };
                let result = git.diff(&request).await.map_err(Self::map_tool_error)?;
                serde_json::to_string(&result).map_err(|_| Refusal::InternalError)?
            }
            "git_log" => {
                let parsed: GitLogArgs = serde_json::from_value(args.clone())
                    .map_err(|_| Refusal::SchemaInvalid)?;
                let request = GitLogRequest {
                    cwd: parsed.cwd,
                    limit: parsed.limit.unwrap_or(20),
                };
                let result = git.log(&request).await.map_err(Self::map_tool_error)?;
                serde_json::to_string(&result).map_err(|_| Refusal::InternalError)?
            }
            _ => return Err(Refusal::ToolNotAllowed),
        };
        Ok(text)
    }

    /// `SETTINGS_STATUS` -> `SETTINGS_STATUS_RESULT` (never the key).
    #[must_use]
    pub fn settings_status(&self) -> event::SettingsStatusResult {
        hiverouter_tools::commands::settings_status_command()
    }

    /// `FILE_TREE_LIST` -> `FILE_TREE_RESULT` through the jail.
    ///
    /// # Errors
    /// `E_FILE_TREE_UNAVAILABLE`, `E_PATH_OUTSIDE_JAIL`, `E_PATH_NOT_FOUND`.
    pub fn file_tree_list(
        &self,
        payload: &hiverouter_contract::command::FileTreeList,
    ) -> Result<event::FileTreeResult, Refusal> {
        let jail = self.require_jail()?;
        hiverouter_tools::commands::file_tree_list(jail, payload.path.as_deref())
            .map_err(Self::map_tool_error)
    }

    /// `FILE_READ` -> `FILE_CONTENT` through the jail.
    ///
    /// # Errors
    /// `E_FILE_TREE_UNAVAILABLE`, `E_PATH_OUTSIDE_JAIL`, `E_PATH_NOT_FOUND`,
    /// `E_FILE_TOO_LARGE`, `E_SCHEMA_INVALID` (non-UTF-8 file).
    pub fn file_read(&self, payload: &command::FileRead) -> Result<event::FileContent, Refusal> {
        let jail = self.require_jail()?;
        hiverouter_tools::commands::file_read(jail, payload).map_err(Self::map_tool_error)
    }

    /// `APPROVAL_RESOLVE` -> resolution through the nonce/replay gate, without
    /// touching the run ledger (read-only/preview callers only; the command
    /// wiring uses [`Self::resolve_approval_ledgered`]).
    ///
    /// # Errors
    /// `E_APPROVAL_NOT_FOUND`, `E_APPROVAL_REPLAY`, `E_APPROVAL_TIMEOUT`,
    /// `E_APPROVAL_DECISION_INVALID`, `E_DB_BUSY`.
    pub fn resolve_approval(
        &self,
        payload: &command::ApprovalResolve,
    ) -> Result<ApprovalResolution, Refusal> {
        let now_unix_secs = Timestamp::now().unix_secs().unwrap_or(0);
        self.db.with_connection_refusal(|conn| {
            self.gate
                .resolve_command(
                    conn,
                    &payload.approval_id.to_string(),
                    &payload.nonce,
                    &payload.decision,
                    payload.reason.as_deref(),
                    now_unix_secs,
                )
                .map_err(|error| error.refusal())
        })
    }

    /// Ledger directory for approval-decision rows under the server data dir.
    ///
    /// The ledger is append-only JSONL (`run-ledger.jsonl`); it is runtime state,
    /// never a tracked artifact.
    #[must_use]
    pub fn approval_ledger_dir(data_dir: &Path) -> PathBuf {
        data_dir.join("ledger")
    }

    /// `APPROVAL_RESOLVE` command wiring: consume the nonce once, append exactly
    /// one ledger row for the committed decision, and return the resolution.
    ///
    /// Ordering is load-bearing in both directions:
    /// - the ledger is opened **before** the decision is committed, so a ledger
    ///   that cannot be opened never consumes the nonce;
    /// - the decision UPDATE and the ledger append happen inside one uncommitted
    ///   SQLite transaction. If the append fails, the transaction is dropped
    ///   (rolled back), so the decision is never visible and the nonce stays
    ///   unconsumed; only a decision whose evidence was appended commits.
    ///
    /// A replay/not-found/expired request still returns `Err` before any ledger
    /// write (never ledger on refusal). Only a committed resolution is appended
    /// (`allowed` / `denied` + named code). A commit failure after the append is
    /// the fail-closed direction: evidence may exist for a decision that did not
    /// commit, but an executable grant without evidence cannot.
    ///
    /// The ledger instance is cached per directory behind a mutex: concurrent
    /// resolutions therefore share one monotonic sequence and one append/flush
    /// lock instead of interleaving lines from fresh instances.
    ///
    /// # Errors
    /// See [`Self::resolve_approval`]; additionally `E_INTERNAL_ERROR` when the
    /// ledger cannot be opened or appended (in which case the decision did not
    /// commit and the nonce remains unconsumed).
    pub fn resolve_approval_ledgered(
        &self,
        payload: &command::ApprovalResolve,
        ledger_dir: &Path,
    ) -> Result<ApprovalResolution, Refusal> {
        let now_unix_secs = Timestamp::now().unix_secs().unwrap_or(0);
        // Append-before-commit: the decision UPDATE is executed inside one
        // SQLite transaction, the ledger row is appended while the transaction
        // is still uncommitted, and only then is the transaction committed. An
        // append failure drops the transaction (rollback), so the decision is
        // never visible and the nonce stays unconsumed. A commit failure can
        // leave a ledger row for a decision that did not commit — the
        // fail-closed direction (evidence without an executable grant), never a
        // grant without evidence.
        //
        // The ledger is opened only after the gate accepts the decision, so a
        // refused resolution never creates a ledger file. The one refusal that
        // mutates the row (`E_APPROVAL_TIMEOUT` marks a pending row expired) is
        // committed without a ledger row, preserving the pre-existing TTL
        // consumption semantics.
        let resolution = self.db.with_connection_refusal(|conn| {
            let tx = conn
                .unchecked_transaction()
                .map_err(|error| ToolError::from(error).refusal())?;
            let resolution = match self.gate.resolve_command(
                &tx,
                &payload.approval_id.to_string(),
                &payload.nonce,
                &payload.decision,
                payload.reason.as_deref(),
                now_unix_secs,
            ) {
                Ok(resolution) => resolution,
                Err(error) => {
                    let refusal = error.refusal();
                    if refusal == Refusal::ApprovalTimeout {
                        // Preserve the TTL reaper's `expired` consumption while
                        // still writing no ledger row for a refused decision.
                        tx.commit()
                            .map_err(|error| ToolError::from(error).refusal())?;
                    }
                    return Err(refusal);
                }
            };

            let mut guard = self.ledger.lock().map_err(|_| Refusal::InternalError)?;
            let reopen = match guard.as_ref() {
                Some((dir, _)) => dir.as_path() != ledger_dir,
                None => true,
            };
            if reopen {
                let opened = RunLedger::open_in(ledger_dir).map_err(|error| error.refusal())?;
                *guard = Some((ledger_dir.to_path_buf(), opened));
            }
            let ledger = match guard.as_ref() {
                Some((_, ledger)) => ledger,
                None => return Err(Refusal::InternalError),
            };
            append_approval_decision(ledger, &resolution).map_err(|error| error.refusal())?;
            tx.commit()
                .map_err(|error| ToolError::from(error).refusal())?;
            Ok(resolution)
        })?;
        Ok(resolution)
    }

    /// Pending (undecided) approval requests for a run, oldest first.
    ///
    /// Read-only: it never consumes a nonce, resolves, executes or ledgers.
    ///
    /// # Errors
    /// `E_INTERNAL_ERROR` on a SQLite failure or malformed stored row.
    pub fn pending_approvals(&self, run_id: RunId) -> Result<Vec<ApprovalRequest>, Refusal> {
        self.db.with_connection_refusal(|conn| {
            hiverouter_tools::approval::pending_for_run(conn, run_id)
                .map_err(|error| error.refusal())
        })
    }

    /// Ledger file path for the approval-decision evidence (tests/diagnostics).
    #[must_use]
    pub fn approval_ledger_file(data_dir: &Path) -> PathBuf {
        Self::approval_ledger_dir(data_dir).join(LEDGER_FILE_NAME)
    }

    /// Creates (or returns) the committed approval request for one
    /// model-originated non-read tool call and returns the frozen wire
    /// `APPROVAL_REQUEST` payload — **without executing the tool**.
    ///
    /// The gate commits the row first (single-use nonce, `args_sha256` binding,
    /// `expires_at = now + APPROVAL_TTL_SECS`), so the returned event carries
    /// everything the UI needs to resolve it later. Calling this twice for the
    /// same `(run_id, tool_call_id)` with the same arguments while a pending
    /// request is still unexpired returns that same request (same approval
    /// id/nonce) instead of inserting a duplicate. The same tool-call id with
    /// **different arguments** is refused by name (`E_APPROVAL_REPLAY`) rather
    /// than silently reusing a binding for the wrong arguments; an
    /// expired/decided request is never resurrected.
    ///
    /// The engine owns the rest of the suspension seam: publish the returned
    /// event on the bus with the run filter and persist `awaiting_approval`
    /// (run_state `Effect::RequestApproval`). This method performs no side
    /// effect, no execution, and no ledger write.
    ///
    /// # Errors
    /// - `E_TOOL_NOT_FOUND` for a name outside the frozen tool vocabulary;
    /// - `E_SCHEMA_INVALID` for an empty `tool_call_id`;
    /// - `E_APPROVAL_REPLAY` when the same `(run_id, tool_call_id)` already has a
    ///   pending request bound to different arguments;
    /// - `E_INTERNAL_ERROR` for a read-class tool (no approval is needed or
    ///   permitted) or when the row cannot be committed;
    /// - `E_DB_BUSY` / `E_INTERNAL_ERROR` on database failure.
    pub fn request_model_tool_approval(
        &self,
        run_id: RunId,
        task_id: TaskId,
        tool_call_id: &str,
        tool: &str,
        args: &serde_json::Value,
    ) -> Result<event::ApprovalRequest, Refusal> {
        if tool_call_id.trim().is_empty() {
            return Err(Refusal::SchemaInvalid);
        }
        let digest = hiverouter_tools::approval::hash_args(args);
        let now_unix_secs = Timestamp::now().unix_secs().unwrap_or(0);
        self.db.with_connection_refusal(|conn| {
            let pending = hiverouter_tools::approval::pending_for_run(conn, run_id)
                .map_err(|error| error.refusal())?;
            if let Some(existing) = pending.into_iter().find(|request| {
                request.tool_call_id == tool_call_id && !request.is_expired(now_unix_secs)
            }) {
                if existing.args_sha256 != digest {
                    // Never reuse an approval bound to other arguments: the UI
                    // would present the wrong request and a later grant check
                    // would refuse execution after a decision was consumed.
                    return Err(Refusal::ApprovalReplay);
                }
                return Ok(Self::approval_request_event_with_args(&existing, Some(args)));
            }
            let request = hiverouter_tools::approval::request_tool_approval(
                &self.gate,
                conn,
                run_id,
                task_id,
                tool_call_id,
                tool,
                args,
                now_unix_secs,
            )
            .map_err(|error| error.refusal())?;
            Ok(Self::approval_request_event_with_args(&request, Some(args)))
        })
    }

    /// Wire projection for a fresh request whose arguments are still held in
    /// memory: includes a bounded, redacted preview. Replay projections use
    /// [`Self::approval_request_event`], which now carries the persisted
    /// bounded preview so reloaded approvals are not blind.
    #[must_use]
    pub fn approval_request_event_with_args(
        request: &ApprovalRequest,
        args: Option<&serde_json::Value>,
    ) -> event::ApprovalRequest {
        let mut event = Self::approval_request_event(request);
        event.args_preview = args.map(hiverouter_contract::redact_args_preview);
        event
    }

    /// Wire `APPROVAL_REQUEST` payload for one committed tools approval request.
    ///
    /// The event is a faithful projection: `decision` is absent while the
    /// request is open, and the single-use `nonce`, run/task/tool-call ids, the
    /// `args_sha256` binding and the TTL `expires_at` are all carried so a
    /// pre-1.2 UI can still resolve the command.
    #[must_use]
    pub fn approval_request_event(request: &ApprovalRequest) -> event::ApprovalRequest {
        event::ApprovalRequest {
            approval_id: request.approval_id,
            tool: request.tool.id().to_owned(),
            decision: None,
            nonce: Some(request.nonce.clone()),
            run_id: Some(request.run_id),
            task_id: Some(request.task_id),
            tool_call_id: Some(request.tool_call_id.clone()),
            args_sha256: Some(request.args_sha256.clone()),
            // The persisted preview is bounded and already redacted; replay and
            // CLI review use exactly the same bytes the live approval showed.
            args_preview: request.args_preview.clone(),
            expires_at: Some(request.expires_at.clone()),
        }
    }

    /// Wire `APPROVAL_REQUEST` payloads for every open request of a run,
    /// oldest first.
    ///
    /// Read-only projection over [`Self::pending_approvals`]: it never consumes
    /// a nonce, resolves, executes or ledgers, so it is safe for UI replay.
    ///
    /// # Errors
    /// See [`Self::pending_approvals`].
    pub fn pending_approval_events(
        &self,
        run_id: RunId,
    ) -> Result<Vec<event::ApprovalRequest>, Refusal> {
        let pending = self.pending_approvals(run_id)?;
        Ok(pending
            .iter()
            .map(Self::approval_request_event)
            .collect())
    }

    /// Wire `APPROVAL_REQUEST` payloads for every still-pending approval of a
    /// non-terminal run, oldest first, bounded by `limit`.
    ///
    /// Read-only replay projection over
    /// [`hiverouter_tools::approval::pending_ids_for_active_runs`]: it never
    /// consumes a nonce, resolves, executes or ledgers, so it is safe to
    /// publish after `SERVER_HELLO`. Expired rows are skipped (the boot/live
    /// reaper owns marking them `expired`; replay never resurrects one).
    ///
    /// # Errors
    /// `E_DB_BUSY` / `E_INTERNAL_ERROR` on database or stored-row failure.
    pub fn pending_approval_events_all(
        &self,
        limit: usize,
    ) -> Result<Vec<event::ApprovalRequest>, Refusal> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let now_unix_secs = Timestamp::now().unix_secs().unwrap_or(0);
        self.db.with_connection_refusal(|conn| {
            let ids = hiverouter_tools::approval::pending_ids_for_active_runs(conn, limit)
                .map_err(|error| error.refusal())?;
            let mut events = Vec::with_capacity(ids.len());
            for id in ids {
                let Some(request) = hiverouter_tools::approval::load_request(conn, &id)
                    .map_err(|error| error.refusal())?
                else {
                    continue; // row vanished between query and load: never fabricate
                };
                if request.is_expired(now_unix_secs) {
                    continue; // TTL elapsed before the reaper ran: never replay it
                }
                events.push(Self::approval_request_event(&request));
            }
            Ok(events)
        })
    }

    /// The configured data directory, when the bridge was built with one.
    #[must_use]
    pub fn data_dir(&self) -> Option<&Path> {
        self.data_dir.as_deref()
    }

    /// The real audit directory (`<data_dir>/audit`) used by the JSON audit
    /// sink, when the bridge has a data directory.
    #[must_use]
    pub fn audit_log_dir(&self) -> Option<PathBuf> {
        self.data_dir
            .as_ref()
            .map(|data_dir| crate::http::health::audit_dir(data_dir))
    }

    /// `/diagnose` payload from the tools health/derived census layer.
    ///
    /// The tools layer derives its pure payload with `audit_log_dir: null`; this
    /// bridge owns the `data_dir`, so it inserts the real audit directory, the
    /// whitelisted log file metadata, and the recent refusal scan before the
    /// payload leaves the server. No key or token value is read into the payload.
    ///
    /// # Errors
    /// `E_INTERNAL_ERROR` when the capability census is inconsistent or a
    /// payload fragment cannot be serialised.
    pub fn diagnostics(&self) -> Result<serde_json::Value, Refusal> {
        let db_ok = self.db.schema_version().is_ok();
        let key_present = self.settings_status().key_present;
        let mut value = hiverouter_tools::health::diagnostics(db_ok, key_present)
            .map_err(|error| error.refusal())?;
        let audit_dir = self.audit_log_dir();
        let log_files = self
            .data_dir
            .as_deref()
            .map(crate::http::health::log_file_metadata)
            .unwrap_or_default();
        let audit_scan = audit_dir
            .as_deref()
            .map(|dir| crate::http::health::recent_refusals(dir, 10));
        let recent_refusals = audit_scan
            .as_ref()
            .map(|scan| scan.recent.clone())
            .unwrap_or_default();
        if let Some(object) = value.as_object_mut() {
            object.insert(
                "audit_log_dir".to_owned(),
                serde_json::json!(audit_dir
                    .as_ref()
                    .map(|path| path.to_string_lossy().into_owned())),
            );
            object.insert(
                "log_files".to_owned(),
                serde_json::to_value(log_files).map_err(|_| Refusal::InternalError)?,
            );
            object.insert(
                "audit_scan".to_owned(),
                serde_json::to_value(audit_scan).map_err(|_| Refusal::InternalError)?,
            );
            object.insert(
                "recent_refusals".to_owned(),
                serde_json::to_value(recent_refusals)
                    .map_err(|_| Refusal::InternalError)?,
            );
        }
        Ok(value)
    }

    /// Secret-free self-diagnosis summary for the `harness_diagnose` tool.
    ///
    /// `scope` selects one section (`all`, `health`, `workspace`, `logs`,
    /// `refusals`); an unknown scope is a named schema refusal rather than a
    /// silent default. `limit` bounds the recent-refusal rows (1..=50). The
    /// payload contains no key, bearer token, file body, or caller-supplied
    /// path: log truth is metadata only (name, size, mtime, newest audit file),
    /// refusals carry only timestamp/code/run/tool/permission class, and the
    /// workspace section names the jail root plus a bounded, sorted sample of
    /// root entries so a model that guessed a wrong path can see what exists.
    ///
    /// # Errors
    /// `E_SCHEMA_INVALID` for an unknown scope, `E_INTERNAL_ERROR` when the
    /// capability census or a payload fragment cannot be serialised.
    pub fn harness_diagnose(
        &self,
        scope: &str,
        limit: usize,
    ) -> Result<serde_json::Value, Refusal> {
        let scope = match scope {
            "all" | "health" | "workspace" | "logs" | "refusals" => scope,
            _ => {
                return Err(Refusal::SchemaInvalid);
            }
        };
        let limit = limit.clamp(1, crate::http::health::MAX_RECENT_REFUSALS);
        let db_ok = self.db.schema_version().is_ok();
        let key_present = self.settings_status().key_present;
        let census = hiverouter_tools::standard_census().map_err(|error| error.refusal())?;
        let audit_dir = self.audit_log_dir();
        let log_files = self
            .data_dir
            .as_deref()
            .map(crate::http::health::log_file_metadata)
            .unwrap_or_default();
        let audit_scan = audit_dir
            .as_deref()
            .map(|dir| crate::http::health::recent_refusals(dir, limit));
        let refusals = audit_scan
            .as_ref()
            .map(|scan| scan.recent.clone())
            .unwrap_or_default();
        let include_health = matches!(scope, "all" | "health");
        let include_workspace = matches!(scope, "all" | "workspace");
        let include_logs = matches!(scope, "all" | "logs");
        let include_refusals = matches!(scope, "all" | "refusals");
        Ok(serde_json::json!({
            "schema": "hiverouter.harness_diagnose.v1",
            "scope": scope,
            "generated_at": Timestamp::now().to_string(),
            "health": if include_health {
                serde_json::json!({
                    "db_ok": db_ok,
                    "provider_key_present": key_present,
                    "side_effects_enabled": self.side_effects_enabled,
                    "workspace_root": self.workspace_root.display().to_string(),
                    "jail_available": self.jail.is_some(),
                    "jail_error": self.jail_error.clone(),
                    "data_dir": self.data_dir.as_ref().map(|path| path.display().to_string()),
                    "audit_log_dir": audit_dir.as_ref().map(|path| path.display().to_string()),
                    "tool_count": census.tool_count,
                    "grant_count": census.grant_count(),
                })
            } else {
                serde_json::Value::Null
            },
            "workspace": if include_workspace {
                serde_json::json!({
                    "root": self.workspace_root.display().to_string(),
                    "jail_available": self.jail.is_some(),
                    "jail_error": self.jail_error.clone(),
                    "path_rules": {
                        "relative": true,
                        "separator": "/",
                        "escapes_refused": ["../", "absolute paths", "drive letters", "symlinks"],
                        "denied": ["data directory", "database and SQLite sidecars"],
                        "hint": "use fs_glob or file_list to discover a path; file_read needs an existing file, not a directory",
                    },
                    "top_level": self.workspace_top_level(MAX_DIAGNOSE_TOP_LEVEL),
                    "top_level_limit": MAX_DIAGNOSE_TOP_LEVEL,
                    "diagnostics_by_tier": diagnostics_availability(),
                })
            } else {
                serde_json::Value::Null
            },
            "logs": if include_logs {
                serde_json::json!({
                    "files": log_files,
                    "newest_audit_file": audit_scan.as_ref().and_then(|scan| scan.file.clone()),
                    "audit_bytes": audit_scan.as_ref().map(|scan| scan.bytes),
                    "audit_truncated": audit_scan.as_ref().map(|scan| scan.truncated),
                })
            } else {
                serde_json::Value::Null
            },
            "refusals": if include_refusals {
                serde_json::json!({
                    "catalog_count": census.refusal_count,
                    "recent": refusals,
                    "corrupt_audit_lines": audit_scan.as_ref().map(|scan| scan.corrupt_lines),
                })
            } else {
                serde_json::Value::Null
            },
        }))
    }

    /// Read-only repository symbol search over the persisted repo index.
    ///
    /// Returns only name/kind/path/line/signature, never file bodies. A query
    /// or kind rejected by the index is a named schema refusal.
    ///
    /// # Errors
    /// `E_SCHEMA_INVALID` for an invalid query/kind/limit; `E_INTERNAL_ERROR`
    /// when the index cannot be read.
    pub fn repo_search(
        &self,
        query: &str,
        kind: Option<&str>,
        limit: usize,
    ) -> Result<serde_json::Value, Refusal> {
        let root = self.workspace_root.clone();
        let hits = self
            .db
            .with_connection_refusal(|connection| {
                crate::repo_index::search_workspace_symbols(connection, &root, query, kind, limit)
                    .map_err(|error| match error {
                        crate::repo_index::RepoIndexError::InvalidQuery(_)
                        | crate::repo_index::RepoIndexError::InvalidLimit { .. } => {
                            Refusal::SchemaInvalid
                        }
                        _ => Refusal::InternalError,
                    })
            })?;
        let rows: Vec<serde_json::Value> = hits
            .iter()
            .map(|hit| {
                serde_json::json!({
                    "name": hit.name,
                    "kind": hit.kind,
                    "path": hit.rel_path,
                    "line": hit.line,
                    "signature": hit.signature,
                })
            })
            .collect();
        Ok(serde_json::json!({
            "schema": "hiverouter.repo_search.v1",
            "query": query,
            "kind": kind,
            "count": rows.len(),
            "hits": rows,
        }))
    }

    /// Sorted, bounded names of the workspace root entries for path diagnosis.
    ///
    /// Only names (directories suffixed with `/`) — never file bodies, sizes or
    /// absolute paths. An unavailable jail yields an empty list rather than an
    /// error: the caller already reports `jail_available`.
    fn workspace_top_level(&self, limit: usize) -> Vec<String> {
        let Some(jail) = self.jail.as_ref() else {
            return Vec::new();
        };
        let Ok(entries) = jail.list(None) else {
            return Vec::new();
        };
        let mut names: Vec<String> = entries
            .into_iter()
            .map(|entry| {
                if entry.is_dir {
                    format!("{}/", entry.name)
                } else {
                    entry.name
                }
            })
            .collect();
        names.sort();
        names.truncate(limit);
        names
    }

    /// Contract event for an approval resolution (`approve`/`deny` echo).
    #[must_use]
    pub fn approval_resolved_event(resolution: &ApprovalResolution) -> event::ApprovalResolved {
        let decision = match resolution.decision {
            hiverouter_tools::approval::ApprovalDecision::AllowOnce
            | hiverouter_tools::approval::ApprovalDecision::AllowSession => "approve",
            hiverouter_tools::approval::ApprovalDecision::Deny => "deny",
        };
        event::ApprovalResolved {
            approval_id: resolution.request.approval_id,
            tool: resolution.request.tool.id().to_owned(),
            decision: decision.to_owned(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hiverouter_contract::command::{ApprovalResolve, FileRead, FileTreeList};
    use hiverouter_contract::{ApprovalId, TaskId};
    use hiverouter_tools::shell::ShellPolicy;
    use hiverouter_tools::tool::{AgentTier, PermissionClass, ToolId};

    #[test]
    fn workspace_inside_data_dir_is_readable_but_data_files_stay_denied() {
        let temp = tempfile::tempdir().expect("temp dir");
        let data = temp.path().join("data");
        let workspace = data.join("workspace");
        std::fs::create_dir_all(&workspace).expect("workspace dir");
        std::fs::write(workspace.join("note.txt"), "visible").expect("workspace file");
        let db = Arc::new(Database::open(&data.join("hive.db")).expect("database"));
        ToolsBridge::migrate(&db).expect("approvals migration");
        let bridge = ToolsBridge::new_with_data_dir(&workspace, &data, db);
        let read = bridge
            .file_read(&command::FileRead {
                path: "note.txt".to_owned(),
            })
            .expect("workspace file must be readable");
        assert_eq!(read.text, "visible");
        let denied = bridge.file_read(&command::FileRead {
            path: "../hive.db".to_owned(),
        });
        assert_eq!(denied.expect_err("data file").code(), "E_PATH_OUTSIDE_JAIL");
    }

    fn args() -> serde_json::Value {
        serde_json::json!({"path": "note.txt", "content": "x"})
    }

    fn seeded_bridge() -> (tempfile::TempDir, Arc<Database>, ToolsBridge) {
        let temp = tempfile::tempdir().expect("temp dir");
        let db = Arc::new(Database::open(&temp.path().join("hive.db")).expect("database"));
        ToolsBridge::migrate(&db).expect("approvals migration");
        let bridge =
            ToolsBridge::new_with_data_dir(temp.path(), &temp.path().join("data"), db.clone());
        (temp, db, bridge)
    }

    fn seed(bridge: &ToolsBridge, db: &Database) -> (ApprovalId, String, RunId) {
        let now = Timestamp::now().unix_secs().unwrap_or(0);
        let run: RunId = RunId::new();
        let request = db
            .with_connection_refusal(|conn| {
                bridge
                    .approval_gate()
                    .request(
                        conn,
                        run,
                        TaskId::new(),
                        "call-1",
                        ToolId::FileWrite,
                        PermissionClass::Mutate,
                        &args(),
                        now,
                    )
                    .map_err(|error| error.refusal())
            })
            .expect("seed pending approval");
        (request.approval_id, request.nonce, run)
    }

    /// Bridge whose configured data directory is a real `data/` subtree under
    /// the workspace root (the production default layout).
    fn seeded_data_bridge() -> (
        tempfile::TempDir,
        PathBuf,
        Arc<Database>,
        ToolsBridge,
    ) {
        let temp = tempfile::tempdir().expect("temp dir");
        let data = temp.path().join("data");
        std::fs::create_dir_all(data.join("ledger")).expect("data/ledger");
        std::fs::write(data.join("hiverouter.db"), b"sqlite-state").expect("seed db");
        std::fs::write(data.join("ledger").join("run-ledger.jsonl"), b"{\"seq\":0}\n")
            .expect("seed ledger");
        std::fs::write(temp.path().join("notes-legal.txt"), b"legal workspace file")
            .expect("seed legal file");
        let db = Arc::new(Database::open(&data.join("hive.db")).expect("database"));
        ToolsBridge::migrate(&db).expect("approvals migration");
        let bridge = ToolsBridge::new_with_data_dir(temp.path(), &data, db.clone());
        (temp, data, db, bridge)
    }

    #[test]
    fn compatibility_new_denies_the_database_directory() {
        let temp = tempfile::tempdir().expect("temp dir");
        let store = temp.path().join("store");
        std::fs::create_dir_all(&store).expect("store");
        std::fs::write(store.join("secret.txt"), b"db-adjacent state").expect("secret");
        let db = Arc::new(Database::open(&store.join("hive.db")).expect("database"));
        ToolsBridge::migrate(&db).expect("approvals migration");
        let bridge = ToolsBridge::new(temp.path(), db);
        let error = bridge
            .file_read(&FileRead {
                path: "store/secret.txt".to_owned(),
            })
            .expect_err("database directory");
        assert_eq!(error, Refusal::PathOutsideJail);
    }

    #[test]
    fn with_data_dir_rebuilds_the_jail_for_an_existing_bridge() {
        let temp = tempfile::tempdir().expect("temp dir");
        let store = temp.path().join("store");
        std::fs::create_dir_all(&store).expect("store");
        std::fs::write(store.join("secret.txt"), b"not-a-data-name").expect("secret");
        let db = Arc::new(Database::open(&temp.path().join("hive.db")).expect("database"));
        ToolsBridge::migrate(&db).expect("approvals migration");
        let bridge = ToolsBridge::new(temp.path(), db).with_data_dir(&store);
        let error = bridge
            .file_read(&FileRead {
                path: "store/secret.txt".to_owned(),
            })
            .expect_err("custom data root");
        assert_eq!(error, Refusal::PathOutsideJail);
    }

    #[test]
    fn bridge_jail_refuses_data_dir_reads_and_listing() {
        let (_temp, _data, _db, bridge) = seeded_data_bridge();
        for path in [
            "data/hiverouter.db",
            "data/ledger/run-ledger.jsonl",
            "data/ledger",
            "data",
        ] {
            let error = bridge
                .file_read(&FileRead {
                    path: path.to_owned(),
                })
                .expect_err(path);
            assert_eq!(error, Refusal::PathOutsideJail, "{path}");
        }
        let error = bridge
            .file_tree_list(&FileTreeList {
                path: Some("data".to_owned()),
            })
            .expect_err("data listing");
        assert_eq!(error, Refusal::PathOutsideJail);

        // No regression: a legitimate workspace file is still readable.
        let content = bridge
            .file_read(&FileRead {
                path: "notes-legal.txt".to_owned(),
            })
            .expect("legal read");
        assert_eq!(content.path, "notes-legal.txt");
        assert_eq!(content.text, "legal workspace file");
        let tree = bridge
            .file_tree_list(&FileTreeList { path: None })
            .expect("legal listing");
        assert!(tree
            .entries
            .iter()
            .any(|entry| entry.name == "notes-legal.txt"));
    }

    #[tokio::test]
    async fn approved_file_write_cannot_target_data_dir_or_ledger() {
        let (temp, _data, db, bridge) = seeded_data_bridge();
        let before_db = std::fs::read(temp.path().join("data/hiverouter.db")).expect("db");
        let before_ledger =
            std::fs::read(temp.path().join("data/ledger/run-ledger.jsonl")).expect("ledger");
        for path in ["data/hiverouter.db", "data/ledger/run-ledger.jsonl", "data/new.db"] {
            let args = serde_json::json!({"path": path, "content": "TAMPERED"});
            let error = bridge
                .execute_approved(
                    &ShellPolicy::new(Vec::new()),
                    &hiverouter_tools::tool::ToolRegistry::standard(),
                    AgentTier::WorkerAgent,
                    RunId::new(),
                    "file_write",
                    &args,
                    None,
                )
                .await
                .expect_err(path);
            assert_eq!(error, Refusal::PathOutsideJail, "{path}");
        }
        assert_eq!(
            std::fs::read(temp.path().join("data/hiverouter.db")).expect("db unchanged"),
            before_db
        );
        assert_eq!(
            std::fs::read(temp.path().join("data/ledger/run-ledger.jsonl")).expect("ledger unchanged"),
            before_ledger
        );
        assert!(!temp.path().join("data/new.db").exists());

        // No regression: the same approved-write path succeeds for a workspace
        // file outside the denied subtree.
        let args = args();
        let (approval_id, nonce, run) = seed(&bridge, &db);
        let ledger_dir = ToolsBridge::approval_ledger_dir(&temp.path().join("data"));
        bridge
            .resolve_approval_ledgered(&approve_payload(approval_id, &nonce), &ledger_dir)
            .expect("approve legal write");
        bridge
            .execute_approved(
                &ShellPolicy::new(Vec::new()),
                &hiverouter_tools::tool::ToolRegistry::standard(),
                AgentTier::WorkerAgent,
                run,
                "file_write",
                &args,
                None,
            )
            .await
            .expect("legal approved write");
        assert_eq!(
            std::fs::read(temp.path().join("note.txt")).expect("written"),
            b"x"
        );
    }

    fn approve_payload(approval_id: ApprovalId, nonce: &str) -> ApprovalResolve {
        ApprovalResolve {
            approval_id,
            nonce: nonce.to_owned(),
            decision: "approve".to_owned(),
            reason: Some("operator approved".to_owned()),
        }
    }

    fn deny_payload(approval_id: ApprovalId, nonce: &str) -> ApprovalResolve {
        ApprovalResolve {
            approval_id,
            nonce: nonce.to_owned(),
            decision: "deny".to_owned(),
            reason: None,
        }
    }

    fn ledger_rows(temp: &tempfile::TempDir) -> Vec<serde_json::Value> {
        let file = ToolsBridge::approval_ledger_file(temp.path());
        if !file.exists() {
            return Vec::new();
        }
        std::fs::read_to_string(&file)
            .expect("ledger readable")
            .lines()
            .filter(|line| !line.trim().is_empty())
            .map(|line| serde_json::from_str(line).expect("valid ledger json line"))
            .collect()
    }

    #[test]
    fn approve_resolution_appends_exactly_one_ledger_row() {
        let (temp, db, bridge) = seeded_bridge();
        let (approval_id, nonce, run) = seed(&bridge, &db);
        let ledger_dir = ToolsBridge::approval_ledger_dir(temp.path());
        assert_eq!(ledger_dir, temp.path().join("ledger"));

        let resolution = bridge
            .resolve_approval_ledgered(&approve_payload(approval_id, &nonce), &ledger_dir)
            .expect("approved once");
        assert_eq!(resolution.request.run_id, run);

        let rows = ledger_rows(&temp);
        assert_eq!(rows.len(), 1, "one committed decision = one ledger row");
        assert_eq!(rows[0]["outcome"].as_str(), Some("allowed"));
        assert_eq!(rows[0]["tool"].as_str(), Some("file_write"));
        assert_eq!(rows[0]["permission_class"].as_str(), Some("mutate"));
        assert_eq!(rows[0]["tool_call_id"].as_str(), Some("call-1"));
        assert_eq!(
            rows[0]["run_id"].as_str(),
            Some(run.to_string().as_str()),
            "ledger row binds the real run"
        );
        assert_eq!(
            rows[0]["args_sha256"].as_str(),
            Some(hiverouter_tools::approval::hash_args(&args()).as_str()),
            "ledger row binds the approved argument digest"
        );
        assert!(rows[0]["refusal_code"].is_null());
        assert!(!rows[0].to_string().contains("\"content\""), "argument body is never ledgered");

        // The event published for this decision is truthful.
        let resolved = ToolsBridge::approval_resolved_event(&resolution);
        assert_eq!(resolved.approval_id, approval_id);
        assert_eq!(resolved.tool, "file_write");
        assert_eq!(resolved.decision, "approve");

        // Replaying the same nonce is refused and never appends a second row.
        let replay = bridge
            .resolve_approval_ledgered(&approve_payload(approval_id, &nonce), &ledger_dir)
            .expect_err("replay is refused");
        assert_eq!(replay, Refusal::ApprovalReplay);
        assert_eq!(ledger_rows(&temp).len(), 1, "refusal never ledgers");
    }

    #[test]
    fn deny_resolution_ledgers_one_named_refusal_row() {
        let (temp, db, bridge) = seeded_bridge();
        let (approval_id, nonce, _run) = seed(&bridge, &db);
        let ledger_dir = ToolsBridge::approval_ledger_dir(temp.path());

        let resolution = bridge
            .resolve_approval_ledgered(&deny_payload(approval_id, &nonce), &ledger_dir)
            .expect("denied once");
        let resolved = ToolsBridge::approval_resolved_event(&resolution);
        assert_eq!(resolved.decision, "deny");

        let rows = ledger_rows(&temp);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["outcome"].as_str(), Some("denied"));
        assert_eq!(rows[0]["refusal_code"].as_str(), Some("E_APPROVAL_REQUIRED"));
        assert_eq!(rows[0]["args_sha256"].as_str().map(str::len), Some(64));
    }

    #[test]
    fn refused_resolution_never_writes_a_ledger_row() {
        let (temp, _db, bridge) = seeded_bridge();
        let ledger_dir = ToolsBridge::approval_ledger_dir(temp.path());

        let unknown = bridge
            .resolve_approval_ledgered(
                &approve_payload(ApprovalId::new(), "not-a-request-nonce"),
                &ledger_dir,
            )
            .expect_err("unknown approval id is refused");
        assert_eq!(unknown, Refusal::ApprovalNotFound);
        assert!(ledger_rows(&temp).is_empty(), "refusal must not be ledgered");
        assert!(!ToolsBridge::approval_ledger_file(temp.path()).exists());
    }

    #[test]
    fn pending_approvals_is_read_only_and_clears_after_resolution() {
        let (temp, db, bridge) = seeded_bridge();
        let (approval_id, nonce, run) = seed(&bridge, &db);

        let pending = bridge.pending_approvals(run).expect("read pending");
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].approval_id, approval_id);
        assert!(ledger_rows(&temp).is_empty(), "read-only helper never ledgers");

        bridge
            .resolve_approval_ledgered(&approve_payload(approval_id, &nonce), &ToolsBridge::approval_ledger_dir(temp.path()))
            .expect("resolved");
        assert!(bridge.pending_approvals(run).expect("read pending").is_empty());
        assert_eq!(ledger_rows(&temp).len(), 1);
    }

    #[test]
    fn ledger_location_is_under_the_data_directory() {
        let data_dir = Path::new("data");
        assert_eq!(ToolsBridge::approval_ledger_dir(data_dir), PathBuf::from("data/ledger"));
        assert_eq!(
            ToolsBridge::approval_ledger_file(data_dir),
            PathBuf::from("data/ledger/run-ledger.jsonl")
        );
    }

    #[test]
    fn model_request_builds_wire_event_without_side_effects() {
        let (temp, _db, bridge) = seeded_bridge();
        let run = RunId::new();
        let task = TaskId::new();
        let args = serde_json::json!({"path": "note.txt", "content": "SHOULD-NOT-WRITE"});

        let event = bridge
            .request_model_tool_approval(run, task, "call-model-1", "file_write", &args)
            .expect("approval request committed");

        assert_eq!(event.tool, "file_write");
        assert!(event.decision.is_none(), "the request is open while undecided");
        assert_eq!(event.run_id, Some(run));
        assert_eq!(event.task_id, Some(task));
        assert_eq!(event.tool_call_id.as_deref(), Some("call-model-1"));
        assert_eq!(
            event.args_sha256.as_deref(),
            Some(hiverouter_tools::approval::hash_args(&args).as_str()),
            "the nonce is bound to the exact argument digest"
        );
        let nonce = event.nonce.as_deref().expect("wire event carries the nonce");
        assert_eq!(nonce.len(), 36, "uuid v7 nonce");
        let expires = event
            .expires_at
            .clone()
            .expect("wire event carries the TTL expiry")
            .unix_secs()
            .expect("representable expiry");
        let now = Timestamp::now().unix_secs().unwrap_or(0);
        assert!(expires > now, "expiry is in the future");
        assert!(expires <= now + hiverouter_tools::approval::APPROVAL_TTL_SECS + 1);

        // The row is committed (pending) and the producer executed nothing:
        // no workspace file, no execution, no ledger row.
        let pending = bridge.pending_approvals(run).expect("pending");
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].approval_id, event.approval_id);
        assert!(!temp.path().join("note.txt").exists(), "producer never executes file_write");
        assert!(ledger_rows(&temp).is_empty(), "request is not a decision; no ledger row yet");
    }

    #[test]
    fn repeated_request_for_the_same_pending_tool_call_is_idempotent() {
        let (_temp, _db, bridge) = seeded_bridge();
        let run = RunId::new();
        let task = TaskId::new();
        let args = serde_json::json!({"path": "a.txt", "content": "x"});

        let first = bridge
            .request_model_tool_approval(run, task, "call-dup", "file_write", &args)
            .expect("first request");
        let second = bridge
            .request_model_tool_approval(run, task, "call-dup", "file_write", &args)
            .expect("second request returns the same pending approval");
        assert_eq!(first.approval_id, second.approval_id);
        assert_eq!(first.nonce, second.nonce);
        assert_eq!(bridge.pending_approvals(run).expect("pending").len(), 1);
    }

    #[test]
    fn same_tool_call_id_with_different_arguments_is_refused() {
        let (temp, _db, bridge) = seeded_bridge();
        let run = RunId::new();
        let task = TaskId::new();
        let approved_args = serde_json::json!({"path": "a.txt", "content": "approved"});

        let first = bridge
            .request_model_tool_approval(run, task, "call-args", "file_write", &approved_args)
            .expect("first request");
        let different = serde_json::json!({"path": "a.txt", "content": "DIFFERENT"});

        // Never reuse a pending approval bound to other arguments: silently
        // returning it would present the wrong request to the operator and make a
        // later grant check refuse execution after the decision was consumed.
        let refused = bridge
            .request_model_tool_approval(run, task, "call-args", "file_write", &different)
            .expect_err("different arguments under the same tool_call_id are refused");
        assert_eq!(refused, Refusal::ApprovalReplay);
        assert_eq!(bridge.pending_approvals(run).expect("pending").len(), 1);
        assert!(ledger_rows(&temp).is_empty());

        // The original pending request is still the one the operator resolves,
        // and it still resolves exactly once through the ledgered path.
        let payload = ApprovalResolve {
            approval_id: first.approval_id,
            nonce: first.nonce.clone().expect("nonce"),
            decision: "approve".to_owned(),
            reason: None,
        };
        bridge
            .resolve_approval_ledgered(&payload, &ToolsBridge::approval_ledger_dir(temp.path()))
            .expect("original request resolves");
        assert_eq!(ledger_rows(&temp).len(), 1);
    }

    #[test]
    fn model_request_refuses_read_only_unknown_and_empty_ids() {
        let (temp, _db, bridge) = seeded_bridge();
        let run = RunId::new();
        let task = TaskId::new();

        let read_only = bridge
            .request_model_tool_approval(run, task, "call-read", "file_read", &serde_json::json!({"path": "a"}))
            .expect_err("read tools need no approval");
        assert_eq!(read_only, Refusal::InternalError);

        let unknown = bridge
            .request_model_tool_approval(run, task, "call-unknown", "rm_rf", &serde_json::json!({}))
            .expect_err("unknown vocabulary");
        assert_eq!(unknown, Refusal::ToolNotFound);

        let empty = bridge
            .request_model_tool_approval(run, task, "  ", "file_write", &serde_json::json!({"path": "a", "content": "x"}))
            .expect_err("empty tool_call_id");
        assert_eq!(empty, Refusal::SchemaInvalid);

        assert!(bridge.pending_approvals(run).expect("pending").is_empty());
        assert!(ledger_rows(&temp).is_empty());
    }

    #[test]
    fn pending_approval_events_is_a_read_only_wire_projection() {
        let (temp, _db, bridge) = seeded_bridge();
        let run = RunId::new();
        let task = TaskId::new();

        bridge
            .request_model_tool_approval(run, task, "call-a", "file_write", &serde_json::json!({"path": "a", "content": "x"}))
            .expect("first request");
        bridge
            .request_model_tool_approval(run, task, "call-b", "shell_exec", &serde_json::json!({"program": "git.exe", "args": ["--version"]}))
            .expect("second request");

        let events = bridge.pending_approval_events(run).expect("wire projection");
        assert_eq!(events.len(), 2);
        let mut ids = events
            .iter()
            .map(|event| event.tool_call_id.clone().unwrap_or_default())
            .collect::<Vec<_>>();
        ids.sort();
        assert_eq!(ids, vec!["call-a".to_owned(), "call-b".to_owned()]);
        for event in &events {
            assert!(event.nonce.as_deref().map(str::len) == Some(36));
            assert!(event.expires_at.is_some());
            assert_eq!(event.run_id, Some(run));
            assert_eq!(event.task_id, Some(task));
            assert!(event.args_sha256.as_deref().map(str::len) == Some(64));
            assert!(
                event.args_preview.as_ref().is_some_and(|preview| !preview.is_empty()),
                "replay must carry the persisted bounded preview"
            );
        }
        assert!(ledger_rows(&temp).is_empty(), "projection must not ledger");
    }

    #[test]
    fn produced_request_resolves_through_the_single_ledger_path() {
        let (temp, _db, bridge) = seeded_bridge();
        let run = RunId::new();
        let task = TaskId::new();
        let event = bridge
            .request_model_tool_approval(run, task, "call-resolve", "file_write", &serde_json::json!({"path": "a", "content": "x"}))
            .expect("request");

        let payload = ApprovalResolve {
            approval_id: event.approval_id,
            nonce: event.nonce.clone().expect("nonce"),
            decision: "approve".to_owned(),
            reason: None,
        };
        let resolution = bridge
            .resolve_approval_ledgered(&payload, &ToolsBridge::approval_ledger_dir(temp.path()))
            .expect("approved once");
        assert_eq!(resolution.request.run_id, run);
        assert_eq!(ledger_rows(&temp).len(), 1);

        let replay = bridge
            .resolve_approval_ledgered(&payload, &ToolsBridge::approval_ledger_dir(temp.path()))
            .expect_err("replay refused");
        assert_eq!(replay, Refusal::ApprovalReplay);
        assert_eq!(ledger_rows(&temp).len(), 1, "existing path stays exactly once");
    }

    #[test]
    fn ledger_append_failure_rolls_back_the_committed_decision() {
        let (temp, db, bridge) = seeded_bridge();
        let ledger_dir = ToolsBridge::approval_ledger_dir(temp.path());
        let (first_id, first_nonce, _) = seed(&bridge, &db);
        bridge
            .resolve_approval_ledgered(&approve_payload(first_id, &first_nonce), &ledger_dir)
            .expect("first decision");
        assert_eq!(ledger_rows(&temp).len(), 1, "cached ledger has one row");

        // Simulate an append failure after the decision UPDATE would run by
        // making the cached ledger path unavailable.
        let backup = temp.path().join("ledger-moved");
        std::fs::rename(&ledger_dir, &backup).expect("move ledger");
        let (second_id, second_nonce, second_run) = seed(&bridge, &db);
        let error = bridge
            .resolve_approval_ledgered(&approve_payload(second_id, &second_nonce), &ledger_dir)
            .expect_err("append failure must refuse");
        assert_eq!(error, Refusal::InternalError);
        assert_eq!(
            bridge.approval_decision(&second_id.to_string()).expect("decision"),
            None,
            "a decision whose ledger row failed must not commit"
        );
        assert_eq!(
            bridge.pending_approvals(second_run).expect("pending").len(),
            1,
            "the nonce stays unconsumed so the operator can retry"
        );
        let no_grant = bridge
            .verify_tool_grant(
                second_run,
                hiverouter_tools::ToolId::FileWrite,
                &args(),
                Timestamp::now().unix_secs().unwrap_or(0),
            )
            .expect_err("no executable grant after ledger failure");
        assert_eq!(no_grant, Refusal::ApprovalRequired);
        assert!(ledger_rows(&temp).is_empty(), "no ledger row was written");

        // Restoring the ledger lets the same nonce retry successfully; the
        // append-before-commit ordering leaves no executable grant behind.
        std::fs::rename(&backup, &ledger_dir).expect("restore ledger");
        bridge
            .resolve_approval_ledgered(&approve_payload(second_id, &second_nonce), &ledger_dir)
            .expect("retry after restore");
        assert_eq!(
            bridge.approval_decision(&second_id.to_string()).expect("decision"),
            Some("allow_once".to_owned())
        );
        assert_eq!(ledger_rows(&temp).len(), 2, "one row per committed decision");
    }

    #[tokio::test]
    async fn approved_write_requires_the_exact_bound_tool_call() {
        let (temp, _db, bridge) = seeded_bridge();
        let shell_policy = ShellPolicy::new(Vec::new());
        let registry = hiverouter_tools::tool::ToolRegistry::standard();
        let run = RunId::new();
        let task = TaskId::new();
        let args = serde_json::json!({"path": "bound.txt", "content": "bound"});
        let event = bridge
            .request_model_tool_approval(run, task, "call-bound", "file_write", &args)
            .expect("approval request");
        let approval_id = event.approval_id.to_string();
        let payload = ApprovalResolve {
            approval_id: event.approval_id,
            nonce: event.nonce.clone().expect("nonce"),
            decision: "approve".to_owned(),
            reason: None,
        };
        bridge
            .resolve_approval_ledgered(
                &payload,
                &ToolsBridge::approval_ledger_dir(temp.path()),
            )
            .expect("approved");

        // A different provider tool call cannot consume this grant, and the
        // refused attempt burns nothing.
        let error = bridge
            .execute_approved_bound(
                &shell_policy,
                &registry,
                AgentTier::WorkerAgent,
                run,
                &approval_id,
                "call-other",
                "file_write",
                &args,
                None,
            )
            .await
            .expect_err("tool_call_id binding");
        assert_eq!(error, Refusal::ApprovalRequired);
        assert!(!temp.path().join("bound.txt").exists());

        let executed = bridge
            .execute_approved_bound(
                &shell_policy,
                &registry,
                AgentTier::WorkerAgent,
                run,
                &approval_id,
                "call-bound",
                "file_write",
                &args,
                None,
            )
            .await
            .expect("the exact bound call executes");
        assert!(matches!(
            executed,
            hiverouter_tools::executor::ExecutedTool::Ok { .. }
        ));
        assert_eq!(
            std::fs::read(temp.path().join("bound.txt")).expect("written"),
            b"bound"
        );

        let replay = bridge
            .execute_approved_bound(
                &shell_policy,
                &registry,
                AgentTier::WorkerAgent,
                run,
                &approval_id,
                "call-bound",
                "file_write",
                &args,
                None,
            )
            .await
            .expect_err("single-use grant");
        assert_eq!(replay, Refusal::ApprovalReplay);
        assert_eq!(
            std::fs::read(temp.path().join("bound.txt")).expect("unchanged"),
            b"bound"
        );
    }

    #[test]
    fn consecutive_decisions_share_one_ledger_and_monotonic_sequence() {
        let (temp, db, bridge) = seeded_bridge();
        let ledger_dir = ToolsBridge::approval_ledger_dir(temp.path());
        let (first_id, first_nonce, _first_run) = seed(&bridge, &db);
        let (second_id, second_nonce, _second_run) = seed(&bridge, &db);

        bridge
            .resolve_approval_ledgered(&approve_payload(first_id, &first_nonce), &ledger_dir)
            .expect("first decision");
        bridge
            .resolve_approval_ledgered(&deny_payload(second_id, &second_nonce), &ledger_dir)
            .expect("second decision");

        let rows = ledger_rows(&temp);
        assert_eq!(rows.len(), 2, "two decisions, two rows");
        assert_eq!(rows[0]["seq"].as_u64(), Some(0));
        assert_eq!(
            rows[1]["seq"].as_u64(),
            Some(1),
            "one cached ledger keeps the per-file sequence monotonic"
        );
        assert_eq!(rows[0]["outcome"].as_str(), Some("allowed"));
        assert_eq!(rows[1]["outcome"].as_str(), Some("denied"));
    }

    #[test]
    fn harness_diagnose_is_scope_bounded_and_secret_free() {
        let (_temp, data, _db, bridge) = seeded_data_bridge();
        let audit = data.join("audit");
        std::fs::create_dir_all(&audit).expect("audit dir");
        std::fs::write(
            audit.join("audit.jsonl.2026-11-13"),
            "{\"timestamp\":\"2026-11-13T00:00:00Z\",\"level\":\"WARN\",\"message\":\"refusal\",\"code\":\"E_APPROVAL_TIMEOUT\",\"run_id\":\"run-1\",\"tool\":\"shell_exec\",\"permission_class\":\"execute\"}\n",
        )
        .expect("audit row");

        let value = bridge.harness_diagnose("all", 500).expect("summary");
        assert_eq!(value["schema"], "hiverouter.harness_diagnose.v1");
        assert_eq!(value["scope"], "all");
        assert_eq!(
            value["health"]["audit_log_dir"].as_str().expect("audit dir"),
            data.join("audit").display().to_string()
        );
        assert_eq!(value["logs"]["files"].as_array().expect("files").len(), 4);
        let refusals = value["refusals"]["recent"]
            .as_array()
            .expect("recent refusals");
        assert_eq!(refusals.len(), 1);
        assert_eq!(refusals[0]["code"], "E_APPROVAL_TIMEOUT");
        assert_eq!(refusals[0]["tool"], "shell_exec");
        let rendered = value.to_string();
        assert!(!rendered.contains("Bearer "));
        assert!(!rendered.contains("sk-"));

        assert_eq!(
            bridge
                .harness_diagnose("not-a-scope", 1)
                .expect_err("unknown scope")
                .code(),
            "E_SCHEMA_INVALID"
        );
        let logs_only = bridge.harness_diagnose("logs", 5).expect("logs scope");
        assert!(logs_only["health"].is_null());
        assert!(logs_only["refusals"].is_null());
        assert!(!logs_only["logs"].is_null());
        let refusals_only = bridge
            .harness_diagnose("refusals", 5)
            .expect("refusals scope");
        assert!(refusals_only["health"].is_null());
        assert!(refusals_only["logs"].is_null());
        assert_eq!(
            refusals_only["refusals"]["recent"]
                .as_array()
                .expect("recent")
                .len(),
            1
        );
    }

    #[test]
    fn harness_diagnose_reports_workspace_paths_and_diagnostics_grants() {
        let (_temp, _data, _db, bridge) = seeded_data_bridge();

        let value = bridge
            .harness_diagnose("workspace", 5)
            .expect("workspace scope");
        assert!(value["health"].is_null(), "a scoped call returns only its section");
        assert!(value["logs"].is_null());
        assert!(value["refusals"].is_null());

        let workspace = &value["workspace"];
        assert_eq!(
            workspace["root"].as_str().expect("root"),
            bridge.workspace_root().display().to_string()
        );
        assert_eq!(workspace["jail_available"], true);
        assert!(workspace["jail_error"].is_null());

        let top_level: Vec<&str> = workspace["top_level"]
            .as_array()
            .expect("top level")
            .iter()
            .filter_map(serde_json::Value::as_str)
            .collect();
        assert!(
            top_level.contains(&"notes-legal.txt"),
            "a real root entry must be discoverable: {top_level:?}"
        );
        assert!(top_level.len() <= MAX_DIAGNOSE_TOP_LEVEL);
        let mut sorted = top_level.clone();
        sorted.sort_unstable();
        assert_eq!(top_level, sorted, "the listing is deterministic");

        let rules = &workspace["path_rules"];
        assert_eq!(rules["relative"], true);
        assert!(
            rules["hint"]
                .as_str()
                .expect("hint")
                .contains("fs_glob"),
            "the path hint names the discovery tool"
        );

        // Tier availability is reported from the frozen registry, not restated.
        let registry = hiverouter_tools::tool::ToolRegistry::standard();
        let rows = workspace["diagnostics_by_tier"]
            .as_array()
            .expect("diagnostics_by_tier");
        assert_eq!(rows.len(), hiverouter_tools::AgentTier::all().len());
        for row in rows {
            let label = row["tier"].as_str().expect("tier");
            let tier = hiverouter_tools::AgentTier::all()
                .into_iter()
                .find(|tier| tier.label() == label)
                .expect("known tier");
            assert_eq!(
                row["granted"].as_bool().expect("granted"),
                registry.is_allowed(tier, hiverouter_tools::ToolId::HarnessDiagnose),
                "tier {label} availability must match the registry"
            );
        }

        // `all` keeps every section, and no file body is echoed into it.
        let all = bridge.harness_diagnose("all", 5).expect("all scope");
        assert!(!all["workspace"].is_null());
        assert!(!all["health"].is_null());
        assert!(
            !all.to_string().contains("legal workspace file"),
            "path diagnostics must name entries, never read their contents"
        );
    }

    #[test]
    fn custom_database_file_and_sidecars_are_denied_inside_the_workspace() {
        let temp = tempfile::tempdir().expect("temp");
        let workspace = temp.path().join("workspace");
        std::fs::create_dir_all(&workspace).expect("workspace");
        let db = Arc::new(Database::open(&workspace.join("hive.db")).expect("database"));
        db.migrate().expect("migrations");
        let bridge = ToolsBridge::new_with_data_dir(&workspace, &temp.path().join("data"), db);
        let jail = bridge.jail.as_ref().expect("workspace jail");
        assert!(jail.resolve("hive.db").is_err(), "database file must be denied");
        assert!(jail.resolve("hive.db-wal").is_err(), "database WAL must be denied");
        assert!(jail.resolve("hive.db-shm").is_err(), "database SHM must be denied");
    }
}
