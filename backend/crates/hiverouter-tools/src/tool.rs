//! Agent tool contract + registry (Phase 6 WPs 6.5/6.10, CANONICAL C18).
//!
//! One registry owns: the tool ids (enum-derived, never typed counts), the
//! per-tier allowlists, the permission class per tool, and the JSON Schema of
//! each tool's arguments. `schemars` is the schema source (Wave-A pin
//! `schemars 1.2.3`); validation uses `jsonschema 0.60.2`.
//!
//! Rules:
//! - a tool that is not registered → `E_TOOL_NOT_FOUND` (never a silent drop);
//! - a tool not on the agent tier's allowlist → `E_TOOL_NOT_ALLOWED`;
//! - arguments that violate the tool schema → `E_TOOL_SCHEMA`;
//! - Mutate / Execute / Network tools always require the approval gate;
//!   Read tools do not (read-only hints are not an approval bypass).

use std::collections::BTreeMap;

use hiverouter_contract::Refusal;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use strum::{EnumIter, IntoEnumIterator, IntoStaticStr};

use crate::error::ToolError;

/// The permission class of a tool call (approval gate key).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, EnumIter, IntoStaticStr, Serialize,
    Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum PermissionClass {
    /// Reads workspace state only.
    Read,
    /// Mutates workspace files.
    Mutate,
    /// Executes a child process.
    Execute,
    /// Performs a network request.
    Network,
    /// Requests a governed control-plane action; the HyperAgent admission gate
    /// decides it and records the outcome. It is not a direct workspace mutation.
    Control,
}

impl PermissionClass {
    /// Stable label (ledger/wire).
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            PermissionClass::Read => "read",
            PermissionClass::Mutate => "mutate",
            PermissionClass::Execute => "execute",
            PermissionClass::Network => "network",
            PermissionClass::Control => "control",
        }
    }

    /// Whether an approval is required before execution.
    #[must_use]
    pub fn requires_approval(self) -> bool {
        !matches!(self, PermissionClass::Read | PermissionClass::Control)
    }

    /// Every class, in declaration order (census input).
    #[must_use]
    pub fn all() -> Vec<PermissionClass> {
        PermissionClass::iter().collect()
    }
}

/// Agent tiers (blueprint §2a / Team Report E4).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, EnumIter, IntoStaticStr, Serialize,
    Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum AgentTier {
    /// Orchestrator; owns the run.
    HyperAgent,
    /// Sub-hive; decomposes work.
    SuperAgent,
    /// Executes one task.
    WorkerAgent,
    /// Temporary bounded child; may not spawn or mutate the workspace.
    Subagent,
    /// Invokes approved tools for a task.
    ToolAgent,
}

impl AgentTier {
    /// Stable label.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            AgentTier::HyperAgent => "hyper_agent",
            AgentTier::SuperAgent => "super_agent",
            AgentTier::WorkerAgent => "worker_agent",
            AgentTier::Subagent => "subagent",
            AgentTier::ToolAgent => "tool_agent",
        }
    }

    /// Every tier, in declaration order (census input).
    #[must_use]
    pub fn all() -> Vec<AgentTier> {
        AgentTier::iter().collect()
    }
}

/// The registered tool ids (enum-derived; the census counts these variants).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, EnumIter, IntoStaticStr, Serialize,
    Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum ToolId {
    /// Spawn a child agent.
    SpawnAgent,
    /// Send a short message to this task's parent.
    AskParent,
    /// Send a short message to a sibling under the same parent.
    SendToSibling,
    /// Report a task result to the parent.
    ReportResult,
    /// Finish the current task.
    Finish,
    /// Read a workspace file.
    FileRead,
    /// List a workspace directory.
    FileList,
    /// Write a workspace file.
    FileWrite,
    /// Execute an allowlisted program in the workspace jail.
    ShellExec,
    /// Request a governed detached remote/daemon; HyperAgent admission decides.
    RequestDetached,
    /// Fetch one public HTTPS URL through the approval-gated network seam.
    WebFetch,
    /// Record a SuperAgent vote on one self-evolution proposal.
    VoteEvolution,
    /// Bounded glob over the workspace jail.
    FsGlob,
    /// Bounded literal grep over the workspace jail.
    FsGrep,
    /// Read an exact line range from a workspace file.
    FileReadRange,
    /// Apply one atomic unified diff inside the workspace jail.
    ApplyPatch,
    /// Detect and run the repository test suite.
    RunTests,
    /// Stage paths in the repository index.
    GitStage,
    /// Commit staged changes.
    GitCommit,
    /// Read git status.
    GitStatus,
    /// Read a git diff.
    GitDiff,
    /// Read recent git log.
    GitLog,
    /// Read the harness's own health, log metadata, and refusal diagnostics.
    HarnessDiagnose,
    /// Search the repository symbol index by name/kind.
    RepoSearch,
    /// Ranked recall over this run's persisted memory banks.
    MemoryRecall,
}

impl ToolId {
    /// Stable wire id.
    #[must_use]
    pub fn id(self) -> &'static str {
        match self {
            ToolId::SpawnAgent => "spawn_agent",
            ToolId::AskParent => "ask_parent",
            ToolId::SendToSibling => "send_to_sibling",
            ToolId::ReportResult => "report_result",
            ToolId::Finish => "finish",
            ToolId::FileRead => "file_read",
            ToolId::FileList => "file_list",
            ToolId::FileWrite => "file_write",
            ToolId::ShellExec => "shell_exec",
            ToolId::RequestDetached => "request_detached",
            ToolId::WebFetch => "web_fetch",
            ToolId::VoteEvolution => "vote_evolution",
            ToolId::FsGlob => "fs_glob",
            ToolId::FsGrep => "fs_grep",
            ToolId::FileReadRange => "file_read_range",
            ToolId::ApplyPatch => "apply_patch",
            ToolId::RunTests => "run_tests",
            ToolId::GitStage => "git_stage",
            ToolId::GitCommit => "git_commit",
            ToolId::GitStatus => "git_status",
            ToolId::GitDiff => "git_diff",
            ToolId::GitLog => "git_log",
            ToolId::HarnessDiagnose => "harness_diagnose",
            ToolId::RepoSearch => "repo_search",
            ToolId::MemoryRecall => "memory_recall",
        }
    }

    /// Resolves a wire id.
    #[must_use]
    pub fn from_id(id: &str) -> Option<ToolId> {
        ToolId::iter().find(|tool| tool.id() == id)
    }

    /// Every tool id, in declaration order (census input).
    #[must_use]
    pub fn all() -> Vec<ToolId> {
        ToolId::iter().collect()
    }
}

/// A tool contract: identity, permission class and argument JSON Schema.
#[derive(Debug, Clone, PartialEq)]
pub struct ToolContract {
    /// Enum identity.
    pub id: ToolId,
    /// Stable wire id.
    pub display_name: &'static str,
    /// Human description (also offered to the model).
    pub description: &'static str,
    /// Permission class.
    pub permission_class: PermissionClass,
    /// Argument JSON Schema (schemars-derived; `additionalProperties: false`).
    pub schema: serde_json::Value,
}

impl ToolContract {
    /// Whether the approval gate must run before execution.
    #[must_use]
    pub fn requires_approval(&self) -> bool {
        self.permission_class.requires_approval()
    }

    /// Validates tool arguments against this contract's JSON Schema.
    ///
    /// The refusal names the tool, the first violation path, and the accepted
    /// argument shape (field names only, never the caller's values), so a model
    /// that emitted malformed arguments can correct itself on the next turn
    /// without the harness echoing a file body into an error string.
    ///
    /// # Errors
    /// [`Refusal::ToolSchema`] with the first validation error path.
    pub fn validate_args(&self, args: &serde_json::Value) -> Result<(), ToolError> {
        jsonschema::validate(&self.schema, args).map_err(|error| {
            ToolError::new(
                Refusal::ToolSchema,
                format!(
                    "arguments do not match the {} schema: {error} ({})",
                    self.id.id(),
                    self.accepted_shape_hint()
                ),
            )
        })
    }

    /// Bounded, secret-free description of the accepted argument shape.
    ///
    /// Derived from the contract schema: field names and required fields only.
    /// Values are deliberately never rendered, because a validated argument can
    /// carry a file body (`file_write.content`) or a patch.
    #[must_use]
    pub fn accepted_shape_hint(&self) -> String {
        let Some(object) = self.schema.as_object() else {
            return "accepted shape unavailable".to_owned();
        };
        let mut fields: Vec<&str> = object
            .get("properties")
            .and_then(serde_json::Value::as_object)
            .map(|properties| properties.keys().map(String::as_str).collect())
            .unwrap_or_default();
        fields.sort_unstable();
        let required: Vec<&str> = object
            .get("required")
            .and_then(serde_json::Value::as_array)
            .map(|required| {
                required
                    .iter()
                    .filter_map(serde_json::Value::as_str)
                    .collect()
            })
            .unwrap_or_default();
        format!(
            "accepted fields: [{}]; required: [{}]",
            fields.join(", "),
            required.join(", ")
        )
    }
}

/// Schema helper: derives a schemars schema and forbids unknown properties.
macro_rules! schema_of {
    ($ty:ty) => {{
        let mut schema = serde_json::to_value(schemars::schema_for!($ty))
            .expect("schemars schemas are JSON-serialisable");
        if let Some(object) = schema.as_object_mut() {
            object.insert(
                "additionalProperties".to_owned(),
                serde_json::Value::Bool(false),
            );
        }
        schema
    }};
}

/// Arguments for `spawn_agent`.
#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SpawnAgentArgs {
    /// Role the child should take.
    pub role: String,
    /// Instruction for the child.
    pub instruction: String,
    /// Parent task id (the run row is authoritative; this is a claim only).
    #[serde(default)]
    pub parent_task_id: Option<String>,
}

/// Arguments for `ask_parent`.
#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AskParentArgs {
    /// Bounded message text.
    pub text: String,
}

/// Arguments for `send_to_sibling`.
#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SendToSiblingArgs {
    /// Sibling task id; must share this task's parent and run.
    pub target_task_id: String,
    /// Bounded message text.
    pub text: String,
}

/// Arguments for `report_result`.
#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ReportResultArgs {
    /// Summary of the task result.
    pub summary: String,
    /// Whether the task succeeded.
    pub success: bool,
}

/// Arguments for `finish`.
#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FinishArgs {
    /// Final verdict text.
    pub verdict: String,
}

/// Arguments for `vote_evolution`.
#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct VoteEvolutionArgs {
    /// Proposal id from the read-only proposal ledger.
    pub proposal_id: String,
    /// `approve` or `reject`.
    pub vote: String,
    /// Short rationale (8..=400 characters).
    pub rationale: String,
}

/// Arguments for `web_fetch`.
#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WebFetchArgs {
    /// Public HTTPS URL to fetch once; redirects are not followed.
    pub url: String,
}

/// Arguments for [`ToolId::HarnessDiagnose`].
#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct HarnessDiagnoseArgs {
    /// Diagnostic scope: `all`, `health`, `workspace`, `logs`, or `refusals`.
    /// An unknown scope is a named `E_SCHEMA_INVALID` refusal, never a silent
    /// default.
    #[serde(default = "default_diagnose_scope")]
    pub scope: String,
    /// Maximum recent refusal rows (values are clamped to 1..=50).
    #[serde(default)]
    pub limit: Option<usize>,
}

fn default_diagnose_scope() -> String {
    "all".to_owned()
}

/// Arguments for [`ToolId::RepoSearch`].
#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RepoSearchArgs {
    /// Symbol name fragment (2..=256 bytes).
    pub query: String,
    /// Optional symbol kind (`function`, `struct`, `class`, ...).
    #[serde(default)]
    pub kind: Option<String>,
    /// Maximum hits (1..=50).
    #[serde(default)]
    pub limit: Option<usize>,
}

/// Arguments for [`ToolId::MemoryRecall`].
#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MemoryRecallArgs {
    /// Natural-language query (2..=8192 bytes).
    pub query: String,
    /// Maximum ranked hits (1..=50).
    #[serde(default)]
    pub limit: Option<usize>,
}

/// Arguments for [`ToolId::FileRead`].
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FileReadArgs {
    /// Workspace-relative path.
    pub path: String,
}

/// Arguments for `file_list`.
#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FileListArgs {
    /// Workspace-relative directory path (defaults to the root).
    #[serde(default)]
    pub path: Option<String>,
}

/// Arguments for `file_write`.
#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FileWriteArgs {
    /// Workspace-relative path.
    pub path: String,
    /// Full new file content.
    pub content: String,
    /// Optional content hash the caller believes the file has (stale-write guard).
    #[serde(default)]
    pub expected_sha256: Option<String>,
}

/// Arguments for `shell_exec`.
#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ShellExecArgs {
    /// Bare program name (must be on the shell allowlist; paths are refused).
    pub program: String,
    /// Arguments passed directly (no shell interpolation).
    pub args: Vec<String>,
    /// Workspace-relative working directory.
    #[serde(default)]
    pub cwd: Option<String>,
}

/// Arguments for `fs_glob`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FsGlobArgs {
    /// Glob pattern relative to the jail root (`*`, `?`, `**`, `[...]`).
    pub pattern: String,
    /// Optional workspace-relative root.
    #[serde(default)]
    pub root: Option<String>,
    /// Requested result cap (server clamps).
    #[serde(default)]
    pub max_results: Option<usize>,
}

/// Arguments for `fs_grep`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FsGrepArgs {
    /// Literal substring to find.
    pub pattern: String,
    /// Optional workspace-relative root.
    #[serde(default)]
    pub root: Option<String>,
    /// Optional file glob filter.
    #[serde(default)]
    pub glob: Option<String>,
    /// Whether matching is case-sensitive (ASCII).
    #[serde(default)]
    pub case_sensitive: Option<bool>,
    /// Requested match cap (server clamps).
    #[serde(default)]
    pub max_matches: Option<usize>,
}

/// Arguments for `file_read_range`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FileReadRangeArgs {
    /// Workspace-relative path.
    pub path: String,
    /// 1-based first line.
    pub start_line: u64,
    /// Number of lines to return.
    pub line_count: u64,
}

/// Arguments for `apply_patch`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ApplyPatchArgs {
    /// Workspace-relative target path.
    pub path: String,
    /// Unified diff text (bounded by the server).
    pub patch: String,
    /// Optional expected current SHA-256 (64 lowercase hex).
    #[serde(default)]
    pub expected_sha256: Option<String>,
}

/// Arguments for `git_status`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GitStatusArgs {
    /// Optional workspace-relative working directory.
    #[serde(default)]
    pub cwd: Option<String>,
}

/// Arguments for `git_diff`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GitDiffArgs {
    /// Optional workspace-relative working directory.
    #[serde(default)]
    pub cwd: Option<String>,
    /// Diff the index instead of the worktree.
    #[serde(default)]
    pub staged: Option<bool>,
    /// Optional path filter.
    #[serde(default)]
    pub path: Option<String>,
}

/// Arguments for `git_log`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GitLogArgs {
    /// Optional workspace-relative working directory.
    #[serde(default)]
    pub cwd: Option<String>,
    /// Number of commits to return.
    #[serde(default)]
    pub limit: Option<usize>,
}

/// Arguments for `git_stage`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GitStageArgs {
    /// Optional workspace-relative working directory.
    #[serde(default)]
    pub cwd: Option<String>,
    /// Paths to stage; pass an empty array when `all` is true.
    pub paths: Vec<String>,
    /// Stage all tracked/untracked changes.
    #[serde(default)]
    pub all: Option<bool>,
}

/// Arguments for `git_commit`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GitCommitArgs {
    /// Optional workspace-relative working directory.
    #[serde(default)]
    pub cwd: Option<String>,
    /// Single-line commit message.
    pub message: String,
    /// Optional author identity.
    #[serde(default)]
    pub author: Option<GitAuthorArgs>,
}

/// Author identity for `git_commit`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GitAuthorArgs {
    /// Author name.
    pub name: String,
    /// Author email.
    pub email: String,
}

/// Arguments for `run_tests`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RunTestsArgs {
    /// Optional workspace-relative working directory.
    #[serde(default)]
    pub cwd: Option<String>,
    /// `auto`, `cargo`, `npm`, or `pytest`.
    pub runner: String,
    /// Extra arguments passed directly (no shell interpolation).
    #[serde(default)]
    pub args: Vec<String>,
    /// Optional timeout in seconds (server clamps).
    #[serde(default)]
    pub timeout_secs: Option<u64>,
}

/// Arguments for `request_detached`.
#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RequestDetachedArgs {
    /// `remote` or `daemon`.
    pub kind: String,
    /// Stable display name, unique inside the run's hive.
    pub display_name: String,
    /// Why the caller needs this detached principal; the admission gate records it.
    pub reason: String,
    /// Optional idempotency key; a retry with the same key returns the existing principal.
    #[serde(default)]
    pub request_id: Option<String>,
}

/// The one tool registry (contracts + per-tier allowlists).
#[derive(Debug, Clone)]
pub struct ToolRegistry {
    contracts: BTreeMap<ToolId, ToolContract>,
    allowlists: BTreeMap<AgentTier, Vec<ToolId>>,
}

impl Default for ToolRegistry {
    fn default() -> Self {
        Self::standard()
    }
}

impl ToolRegistry {
    /// Builds the v1 registry: every [`ToolId`] contract plus the frozen tier
    /// allowlists.
    #[must_use]
    pub fn standard() -> Self {
        let contract = |id: ToolId,
                        description: &'static str,
                        permission_class: PermissionClass,
                        schema: serde_json::Value| ToolContract {
            id,
            display_name: id.id(),
            description,
            permission_class,
            schema,
        };

        let mut contracts = BTreeMap::new();
        for contract in [
            contract(
                ToolId::SpawnAgent,
                "Spawn a child agent (depth/fan-out/agent caps enforced before any spawn).",
                PermissionClass::Execute,
                schema_of!(SpawnAgentArgs),
            ),
            contract(
                ToolId::AskParent,
                "Send a bounded message to this task's parent.",
                PermissionClass::Read,
                schema_of!(AskParentArgs),
            ),
            contract(
                ToolId::SendToSibling,
                "Send a bounded message to a sibling; cross-branch targets require operator approval.",
                PermissionClass::Mutate,
                schema_of!(SendToSiblingArgs),
            ),
            contract(
                ToolId::ReportResult,
                "Report a task result to the parent task.",
                PermissionClass::Read,
                schema_of!(ReportResultArgs),
            ),
            contract(
                ToolId::Finish,
                "Finish the current task with a final verdict.",
                PermissionClass::Read,
                schema_of!(FinishArgs),
            ),
            contract(
                ToolId::FileRead,
                "Read a file inside the workspace jail. Paths are workspace-relative: \
                 a leading '/', a drive letter, or '..' is refused. Use fs_glob or file_list \
                 first when the exact path is unknown; a directory is not a file and must be \
                 listed instead.",
                PermissionClass::Read,
                schema_of!(FileReadArgs),
            ),
            contract(
                ToolId::FileList,
                "List a directory inside the workspace jail. Paths are workspace-relative; \
                 the root is the default. Use this to discover a path before file_read.",
                PermissionClass::Read,
                schema_of!(FileListArgs),
            ),
            contract(
                ToolId::FileWrite,
                "Write a file inside the workspace jail (requires approval).",
                PermissionClass::Mutate,
                schema_of!(FileWriteArgs),
            ),
            contract(
                ToolId::ShellExec,
                "Execute an allowlisted program with the user's privileges (requires approval). \
                 This is arbitrary code execution, not an OS sandbox; command-interpreter hosts \
                 and /c, &, |, > or newline arguments are refused.",
                PermissionClass::Execute,
                schema_of!(ShellExecArgs),
            ),
            contract(
                ToolId::RequestDetached,
                "Request one detached remote or daemon. HyperAgent admission checks caller authority, name conflict, run cap, and reason, then records the decision.",
                PermissionClass::Control,
                schema_of!(RequestDetachedArgs),
            ),
            contract(
                ToolId::WebFetch,
                "Fetch one public HTTPS URL (requires operator approval). Redirects, credentials, private networks, and bodies over 1 MiB are refused.",
                PermissionClass::Network,
                schema_of!(WebFetchArgs),
            ),
            contract(
                ToolId::VoteEvolution,
                "Record this SuperAgent vote on one self-evolution proposal. Votes are data; they never apply code.",
                PermissionClass::Control,
                schema_of!(VoteEvolutionArgs),
            ),
            contract(
                ToolId::FsGlob,
                "List workspace paths matching a bounded glob inside the jail.",
                PermissionClass::Read,
                schema_of!(FsGlobArgs),
            ),
            contract(
                ToolId::FsGrep,
                "Search workspace files for a bounded literal substring inside the jail.",
                PermissionClass::Read,
                schema_of!(FsGrepArgs),
            ),
            contract(
                ToolId::FileReadRange,
                "Read an exact line range from one workspace file inside the jail.",
                PermissionClass::Read,
                schema_of!(FileReadRangeArgs),
            ),
            contract(
                ToolId::ApplyPatch,
                "Apply one bounded unified diff atomically inside the workspace jail (requires approval).",
                PermissionClass::Mutate,
                schema_of!(ApplyPatchArgs),
            ),
            contract(
                ToolId::RunTests,
                "Detect and run this repository's test suite with bounded output and timeout (requires approval).",
                PermissionClass::Execute,
                schema_of!(RunTestsArgs),
            ),
            contract(
                ToolId::GitStage,
                "Stage workspace paths in the repository index (requires approval).",
                PermissionClass::Mutate,
                schema_of!(GitStageArgs),
            ),
            contract(
                ToolId::GitCommit,
                "Commit staged changes with hooks disabled (requires approval).",
                PermissionClass::Mutate,
                schema_of!(GitCommitArgs),
            ),
            contract(
                ToolId::GitStatus,
                "Read git status with fixed arguments inside the workspace jail.",
                PermissionClass::Read,
                schema_of!(GitStatusArgs),
            ),
            contract(
                ToolId::GitDiff,
                "Read a git diff with fixed, non-interactive arguments inside the workspace jail.",
                PermissionClass::Read,
                schema_of!(GitDiffArgs),
            ),
            contract(
                ToolId::GitLog,
                "Read recent git log output inside the workspace jail.",
                PermissionClass::Read,
                schema_of!(GitLogArgs),
            ),
            contract(
                ToolId::HarnessDiagnose,
                "Read this harness's own health, workspace path diagnostics, log metadata, \
                 and recent refusal summary. Read-only; returns no secrets or raw log bodies.",
                PermissionClass::Read,
                schema_of!(HarnessDiagnoseArgs),
            ),
            contract(
                ToolId::RepoSearch,
                "Search the repository symbol index by symbol name and optional kind. Read-only; returns path, line, kind, and signature.",
                PermissionClass::Read,
                schema_of!(RepoSearchArgs),
            ),
            contract(
                ToolId::MemoryRecall,
                "Rank this run's persisted memory banks for a query. Read-only; returns scores, provenance, and bounded excerpts, never secrets.",
                PermissionClass::Read,
                schema_of!(MemoryRecallArgs),
            ),
        ] {
            contracts.insert(contract.id, contract);
        }

        let mut allowlists = BTreeMap::new();
        allowlists.insert(
            AgentTier::HyperAgent,
            vec![
                ToolId::SpawnAgent,
                ToolId::ReportResult,
                ToolId::Finish,
                ToolId::FsGlob,
                ToolId::FsGrep,
                ToolId::FileReadRange,
                ToolId::ApplyPatch,
                ToolId::RunTests,
                ToolId::GitStage,
                ToolId::GitCommit,
                ToolId::GitStatus,
                ToolId::GitDiff,
                ToolId::GitLog,
                ToolId::HarnessDiagnose,
                ToolId::RepoSearch,
                ToolId::MemoryRecall,
            ],
        );
        allowlists.insert(
            AgentTier::SuperAgent,
            vec![
                ToolId::SpawnAgent,
                ToolId::AskParent,
                ToolId::SendToSibling,
                ToolId::ReportResult,
                ToolId::Finish,
                ToolId::FileRead,
                ToolId::FileList,
                ToolId::FileWrite,
                ToolId::ShellExec,
                ToolId::RequestDetached,
                ToolId::WebFetch,
                ToolId::VoteEvolution,
                ToolId::FsGlob,
                ToolId::FsGrep,
                ToolId::FileReadRange,
                ToolId::ApplyPatch,
                ToolId::RunTests,
                ToolId::GitStage,
                ToolId::GitCommit,
                ToolId::GitStatus,
                ToolId::GitDiff,
                ToolId::GitLog,
                ToolId::HarnessDiagnose,
                ToolId::RepoSearch,
                ToolId::MemoryRecall,
            ],
        );
        allowlists.insert(
            AgentTier::WorkerAgent,
            vec![
                ToolId::SpawnAgent,
                ToolId::AskParent,
                ToolId::SendToSibling,
                ToolId::ReportResult,
                ToolId::Finish,
                ToolId::FileRead,
                ToolId::FileList,
                ToolId::FileWrite,
                ToolId::ShellExec,
                ToolId::RequestDetached,
                ToolId::WebFetch,
                ToolId::FsGlob,
                ToolId::FsGrep,
                ToolId::FileReadRange,
                ToolId::ApplyPatch,
                ToolId::RunTests,
                ToolId::GitStage,
                ToolId::GitCommit,
                ToolId::GitStatus,
                ToolId::GitDiff,
                ToolId::GitLog,
                ToolId::HarnessDiagnose,
                ToolId::RepoSearch,
                ToolId::MemoryRecall,
            ],
        );
        allowlists.insert(
            AgentTier::Subagent,
            vec![
                ToolId::AskParent,
                ToolId::ReportResult,
                ToolId::Finish,
                ToolId::FileRead,
                ToolId::FileList,
                ToolId::FsGlob,
                ToolId::FsGrep,
                ToolId::FileReadRange,
                ToolId::GitStatus,
                ToolId::GitDiff,
                ToolId::GitLog,
                ToolId::HarnessDiagnose,
                ToolId::RepoSearch,
                ToolId::MemoryRecall,
            ],
        );
        allowlists.insert(
            AgentTier::ToolAgent,
            vec![
                ToolId::FileRead,
                ToolId::FileList,
                ToolId::FileWrite,
                ToolId::ShellExec,
                ToolId::FsGlob,
                ToolId::FsGrep,
                ToolId::FileReadRange,
                ToolId::ApplyPatch,
                ToolId::RunTests,
                ToolId::GitStage,
                ToolId::GitCommit,
                ToolId::GitStatus,
                ToolId::GitDiff,
                ToolId::GitLog,
            ],
        );

        Self {
            contracts,
            allowlists,
        }
    }

    /// The contract for a tool id (registered in the standard registry).
    #[must_use]
    pub fn contract(&self, id: ToolId) -> Option<&ToolContract> {
        self.contracts.get(&id)
    }

    /// Whether `tier` may call `id`.
    #[must_use]
    pub fn is_allowed(&self, tier: AgentTier, id: ToolId) -> bool {
        self.allowlists
            .get(&tier)
            .map_or(false, |allowed| allowed.contains(&id))
    }

    /// The allowlist for a tier.
    #[must_use]
    pub fn allowlist(&self, tier: AgentTier) -> &[ToolId] {
        self.allowlists
            .get(&tier)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    /// Resolves a tool call to its contract, enforcing the tier allowlist.
    ///
    /// # Errors
    /// [`Refusal::ToolNotFound`] for an unregistered id and
    /// [`Refusal::ToolNotAllowed`] when the tier has no grant.
    pub fn resolve(&self, tier: AgentTier, id: ToolId) -> Result<&ToolContract, ToolError> {
        let contract = self.contracts.get(&id).ok_or_else(|| {
            ToolError::new(
                Refusal::ToolNotFound,
                format!("tool id {id:?} is not registered"),
            )
        })?;
        if !self.is_allowed(tier, id) {
            return Err(ToolError::new(
                Refusal::ToolNotAllowed,
                format!("tool {} is not on the {} allowlist", id.id(), tier.label()),
            ));
        }
        Ok(contract)
    }

    /// Resolves a wire tool name ("spawn_agent", ...) and validates its args.
    ///
    /// # Errors
    /// [`Refusal::ToolNotFound`], [`Refusal::ToolNotAllowed`] or
    /// [`Refusal::ToolSchema`].
    pub fn resolve_call(
        &self,
        tier: AgentTier,
        tool_name: &str,
        args: &serde_json::Value,
    ) -> Result<&ToolContract, ToolError> {
        let id = ToolId::from_id(tool_name).ok_or_else(|| {
            ToolError::new(
                Refusal::ToolNotFound,
                format!("tool {tool_name:?} is not in the frozen tool vocabulary"),
            )
        })?;
        let contract = self.resolve(tier, id)?;
        contract.validate_args(args)?;
        Ok(contract)
    }

    /// Registered ids, derived from the map (never a typed count).
    pub fn registered_ids(&self) -> impl Iterator<Item = &ToolId> {
        self.contracts.keys()
    }

    /// Number of registered contracts.
    #[must_use]
    pub fn contract_count(&self) -> usize {
        self.contracts.len()
    }

    /// Number of allowlist grants across all tiers (census input).
    #[must_use]
    pub fn grant_count(&self) -> usize {
        self.allowlists.values().map(Vec::len).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_tool_id_has_exactly_one_contract() {
        let registry = ToolRegistry::standard();
        let enum_ids: Vec<ToolId> = ToolId::all();
        assert_eq!(registry.contract_count(), enum_ids.len(), "census drift: enum vs registry");
        for id in enum_ids {
            assert!(registry.contract(id).is_some(), "missing contract for {id:?}");
        }
        for id in registry.registered_ids() {
            assert!(ToolId::from_id(id.id()).is_some());
        }
    }

    #[test]
    fn schemas_are_valid_and_forbid_unknown_fields() {
        let registry = ToolRegistry::standard();
        for id in ToolId::all() {
            let contract = registry.contract(id).expect("contract");
            let schema = &contract.schema;
            assert!(schema.is_object(), "{id:?} schema is an object");
            assert_eq!(
                schema.get("additionalProperties"),
                Some(&serde_json::Value::Bool(false)),
                "{id:?} schema forbids unknown fields"
            );
            // A deliberately empty object fails the required-field check for
            // every tool except `file_list` (all of its fields are optional).
            let empty = serde_json::json!({});
            if matches!(
                id,
                ToolId::FileList | ToolId::GitStatus | ToolId::GitDiff | ToolId::GitLog | ToolId::HarnessDiagnose
            ) {
                assert!(contract.validate_args(&empty).is_ok());
            } else {
                assert_eq!(
                    contract.validate_args(&empty).expect_err("required").code(),
                    "E_TOOL_SCHEMA"
                );
            }
            jsonschema::validator_for(schema).expect("schema compiles");
        }
    }

    #[test]
    fn tier_allowlist_grants_and_refusals_are_by_name() {
        let registry = ToolRegistry::standard();
        assert!(registry.is_allowed(AgentTier::HyperAgent, ToolId::SpawnAgent));
        assert!(!registry.is_allowed(AgentTier::HyperAgent, ToolId::ShellExec));
        assert!(!registry.is_allowed(AgentTier::HyperAgent, ToolId::FileWrite));
        assert!(registry.is_allowed(AgentTier::WorkerAgent, ToolId::ShellExec));
        assert!(registry.is_allowed(AgentTier::WorkerAgent, ToolId::WebFetch));
        assert!(registry.is_allowed(AgentTier::SuperAgent, ToolId::WebFetch));
        assert!(!registry.is_allowed(AgentTier::HyperAgent, ToolId::WebFetch));
        assert!(!registry.is_allowed(AgentTier::Subagent, ToolId::WebFetch));
        assert!(registry.is_allowed(AgentTier::SuperAgent, ToolId::VoteEvolution));
        assert!(!registry.is_allowed(AgentTier::WorkerAgent, ToolId::VoteEvolution));
        assert!(!registry.is_allowed(AgentTier::Subagent, ToolId::VoteEvolution));
        assert_eq!(
            registry
                .contract(ToolId::WebFetch)
                .expect("web_fetch contract")
                .permission_class,
            PermissionClass::Network
        );
        assert!(!registry.is_allowed(AgentTier::ToolAgent, ToolId::SpawnAgent));
        assert!(!registry.is_allowed(AgentTier::Subagent, ToolId::SpawnAgent));
        assert!(!registry.is_allowed(AgentTier::Subagent, ToolId::FileWrite));
        assert!(!registry.is_allowed(AgentTier::Subagent, ToolId::ShellExec));
        assert!(registry.is_allowed(AgentTier::Subagent, ToolId::AskParent));
        assert!(registry.is_allowed(AgentTier::Subagent, ToolId::FileRead));

        let error = registry
            .resolve(AgentTier::HyperAgent, ToolId::ShellExec)
            .expect_err("not allowed");
        assert_eq!(error.code(), "E_TOOL_NOT_ALLOWED");
        assert!(registry.resolve(AgentTier::WorkerAgent, ToolId::FileRead).is_ok());
    }

    #[test]
    fn resolve_call_validates_args_by_name() {
        let registry = ToolRegistry::standard();
        let ok = registry
            .resolve_call(
                AgentTier::WorkerAgent,
                "file_read",
                &serde_json::json!({"path": "docs/a.txt"}),
            )
            .expect("valid");
        assert_eq!(ok.permission_class, PermissionClass::Read);
        assert!(!ok.requires_approval());

        let unknown = registry
            .resolve_call(AgentTier::WorkerAgent, "rm_rf", &serde_json::json!({}))
            .expect_err("unknown tool");
        assert_eq!(unknown.code(), "E_TOOL_NOT_FOUND");

        let bad_args = registry
            .resolve_call(AgentTier::WorkerAgent, "file_read", &serde_json::json!({"wrong": 1}))
            .expect_err("bad args");
        assert_eq!(bad_args.code(), "E_TOOL_SCHEMA");

        let extra = registry
            .resolve_call(
                AgentTier::WorkerAgent,
                "file_read",
                &serde_json::json!({"path": "a", "extra": true}),
            )
            .expect_err("unknown field");
        assert_eq!(extra.code(), "E_TOOL_SCHEMA");

        let not_allowed = registry
            .resolve_call(AgentTier::HyperAgent, "file_read", &serde_json::json!({"path": "a"}))
            .expect_err("tier");
        assert_eq!(not_allowed.code(), "E_TOOL_NOT_ALLOWED");
    }

    #[test]
    fn mutating_and_executing_tools_require_approval() {
        let registry = ToolRegistry::standard();
        for id in ToolId::all() {
            let contract = registry.contract(id).expect("contract");
            let expected = match contract.permission_class {
                PermissionClass::Read | PermissionClass::Control => false,
                PermissionClass::Mutate | PermissionClass::Execute | PermissionClass::Network => true,
            };
            assert_eq!(contract.requires_approval(), expected, "{id:?}");
        }
        assert!(registry
            .contract(ToolId::FileWrite)
            .expect("contract")
            .requires_approval());
        assert!(registry
            .contract(ToolId::ShellExec)
            .expect("contract")
            .requires_approval());
    }

    #[test]
    fn allowlist_membership_is_always_a_registered_tool() {
        let registry = ToolRegistry::standard();
        for tier in AgentTier::all() {
            for id in registry.allowlist(tier) {
                assert!(ToolId::from_id(id.id()).is_some());
            }
        }
    }

    #[test]
    fn malformed_args_name_the_accepted_shape_without_echoing_values() {
        let registry = ToolRegistry::standard();
        let contract = registry.contract(ToolId::FileRead).expect("file_read");
        assert_eq!(
            contract.accepted_shape_hint(),
            "accepted fields: [path]; required: [path]"
        );

        let secret = "TOP-SECRET-FILE-BODY";
        let error = contract
            .validate_args(&serde_json::json!({"path": "a", "content": secret}))
            .expect_err("unknown field");
        assert_eq!(error.code(), "E_TOOL_SCHEMA");
        let detail = error.detail();
        assert!(detail.contains("accepted fields: [path]"), "{detail}");
        assert!(detail.contains("required: [path]"), "{detail}");
        assert!(
            !detail.contains(secret),
            "a rejected argument value must never be echoed into the refusal: {detail}"
        );
    }

    #[test]
    fn argument_values_are_absent_from_every_contract_shape_hint() {
        let registry = ToolRegistry::standard();
        for id in ToolId::all() {
            let hint = registry
                .contract(id)
                .expect("contract")
                .accepted_shape_hint();
            assert!(hint.is_empty() || hint.starts_with("accepted fields: ["), "{id:?}: {hint}");
            // Only field/required names are rendered: no JSON value punctuation
            // that would indicate a caller-supplied value leaked in.
            assert!(!hint.contains('"'), "{id:?} hint carried a value: {hint}");
        }
    }
}
