//! HiveMind v1 tools crate — Phase 6 (WPs 6.1–6.14) + Phase 11/12 support modules.
//!
//! One owner for the tool capability surface (CANONICAL C18):
//! - [`tool`] — tool ids, per-tier allowlists, permission classes, schemars
//!   JSON schemas and jsonschema validation;
//! - [`approval`] — the single approval/consent gate (SQLite `UNIQUE(nonce)`,
//!   single-use decisions, TTL expiry, args binding, session scope);
//! - [`commands`] — handlers for the patched frozen commands (`APPROVAL_RESOLVE`,
//!   `SETTINGS_STATUS`, `FILE_TREE_LIST`, `FILE_READ`);
//! - [`jail`] — the single Windows workspace jail (six escape vectors refused);
//! - [`executor`] — the read-only tool executor (only `file_read`/`file_list`;
//!   every other tool is a named `E_TOOL_NOT_ALLOWED` policy refusal);
//! - [`shell`] — approval-gated arbitrary child execution (not an OS sandbox)
//!   with an allowlist, interpreter-host/escape-argument refusals, stdout caps,
//!   timeout and a kill-on-close Job Object;
//! - [`cancel`] — per-run cancellation registry and kill enforcement;
//! - [`ledger`] — append-only JSONL run ledger (args hash, never arg body);
//! - [`sha256`] — the argument-binding digest;
//! - [`audit`] — local-only JSONL audit sink (Phase 11);
//! - [`health`] — `/health` + `/diagnose` router (Phase 11);
//! - [`census`] — derived capability census that fails on drift (Phase 11);
//! - [`cli`] — the agent-native `hiverouter` CLI dispatch (Phase 12);
//! - [`watcher`] — 150 ms-coalesced workspace watcher (Phase 12).
//!
//! Dependency direction: `hiverouter-core` depends on this crate; this crate
//! never depends on core, so core wires [`approval::migrate`], the registry, the
//! command handlers, the health router and the ledger into its server.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod approval;
pub mod audit;
pub mod cancel;
pub mod census;
pub mod cli;
pub mod cli_support;
pub mod commands;
pub mod db_admin;
pub mod env_safety;
pub mod error;
pub mod executor;
pub mod fs_search;
pub mod git;
pub mod health;
pub mod jail;
pub mod ledger;
pub mod patch;
pub mod sha256;
pub mod shell;
pub mod side_effects;
pub mod settings;
pub mod tests_runner;
pub mod tool;
pub mod watcher;

pub use approval::{
    cancel_pending, count_for_run, decision_label, expire, hash_args, migrate as migrate_approvals,
    request_tool_approval, resolve_with_nonce, session_allowed, ApprovalDecision, ApprovalGate,
    ApprovalRequest, ApprovalResolution, APPROVAL_TTL_SECS,
};
pub use audit::{
    init_json_audit, probe_audit_dir, probe_audit_file, read_audit_rows, record_refusal,
    record_tool_call, AuditGuard, AuditRow,
};
pub use cancel::{ProcessRegistry, TerminalOutcome, TerminalState};
pub use census::{
    check_census_drift, derive_census, standard_census, CapabilityCensus, PROMPT_CLAIMED_TOOL_IDS,
};
pub use cli::{CliFailure, CliOutcome, CommandSpec, COMMANDS};
pub use commands::{
    approval_resolve, file_read as file_read_jailed, file_tree_list, settings_status_command,
};
pub use error::ToolError;
pub use executor::{execute_read_only, ExecutedTool};
pub use health::{
    diagnostics, health_report, ready_state, router as health_router, DiagnoseReport, HealthReport,
    ServiceState,
};
pub use jail::{DirEntryInfo, WorkspaceJail, MAX_READ_BYTES};
pub use ledger::{
    append_approval_decision, LedgerOutcome, LedgerRecord, LedgerRow, RunLedger, LEDGER_FILE_NAME,
};
pub use sha256::sha256_hex;
pub use side_effects::{
    commit_side_effect, preflight_side_effect, PreparedSideEffect, PreparedWrite,
    MAX_SIDE_EFFECT_TEXT_BYTES, MAX_WRITE_BYTES,
};
pub use shell::{
    execute as execute_shell, ShellOutput, ShellPolicy, ShellRequest, DEFAULT_MAX_STDERR_BYTES,
    DEFAULT_MAX_STDOUT_BYTES, DEFAULT_TIMEOUT,
};
pub use settings::{require_provider_key, settings_status};
pub use tool::{
    AgentTier, AskParentArgs, FinishArgs, FileListArgs, FileReadArgs, FileWriteArgs, HarnessDiagnoseArgs, RepoSearchArgs,
    PermissionClass, ReportResultArgs, SendToSiblingArgs, ShellExecArgs, SpawnAgentArgs,
    ToolContract, ToolId, ToolRegistry,
};
pub use watcher::{FsChanged, WorkspaceWatcher, WATCH_DEBOUNCE};
