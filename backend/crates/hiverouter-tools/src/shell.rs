//! Allowlisted child-process execution inside the workspace jail (WP 6.8).
//!
//! # Truth: this is not an OS sandbox
//!
//! `shell_exec` is **approval-gated arbitrary code execution**. The approved
//! child runs with the same user privileges as the server. Even the
//! `conservative_default` allowlist is not a containment boundary: `git.exe` and
//! `cargo.exe` execute repository-controlled code (hooks, aliases, build
//! scripts) which can in turn run any program the user can run. The checks in
//! this module reduce accidental command-string injection and refuse command
//! interpreters, but they do **not** sandbox code: an operator who approves
//! `shell_exec` is approving arbitrary code execution until an OS sandbox
//! exists.
//!
//! Mechanical contract:
//! - the program must be on the explicit allowlist (by bare executable name);
//!   paths with separators are refused so a call cannot re-point the allowlist;
//! - each allowlisted program carries a [`CommandSpec`] that bounds its argument
//!   count, refuses arguments that re-point the executable, and may require a
//!   fixed leading token (see [`ShellPolicy::conservative_default`]);
//! - command-interpreter hosts ([`SHELL_INTERPRETER_HOSTS`]) are refused even
//!   when a policy lists them;
//! - argument escape forms (`/c`, `/k`, `&`, `|`, `>`, CR/LF) are refused
//!   before any child is spawned;
//! - no shell interpolation: arguments are passed directly to the program;
//! - working directory is resolved through [`WorkspaceJail`] (the child's
//!   initial cwd is inside the jail; the child itself is not filesystem-jailed);
//! - stdout/stderr are capped; the child is bounded by a timeout;
//! - process-tree containment is acquired **before** spawn: a kill-on-close
//!   Windows Job Object (single containment owner: win32job, never cap-std)
//!   holds the child and every descendant, and is closed on timeout,
//!   cancellation, I/O failure, or return, reaping the whole tree;
//! - on a host without that backend the call is refused by name with
//!   [`E_SANDBOX_UNAVAILABLE`] instead of running uncontained;
//! - run cancellation returns `E_TOOL_CANCELLED` after terminating the child.
//!
//! Residual limits (not fixed by this module): the Job Object controls process
//! lifetime only — not filesystem, registry, or network access; the child runs
//! with the server user's privileges and can read and write outside the jail;
//! containment is assigned immediately after spawn, so a child that forks
//! within that window before assignment is not captured; `git`/`cargo` still
//! execute repository-controlled code (hooks, aliases, build scripts).

use std::path::PathBuf;
use std::time::{Duration, Instant};

use hiverouter_contract::Refusal;
use serde::Serialize;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::process::{Child, Command};
use tokio_util::sync::CancellationToken;

use crate::error::ToolError;
use crate::jail::WorkspaceJail;

/// Default stdout cap (1 MiB) and stderr cap (256 KiB).
pub const DEFAULT_MAX_STDOUT_BYTES: usize = 1024 * 1024;
/// Default stderr cap.
pub const DEFAULT_MAX_STDERR_BYTES: usize = 256 * 1024;
/// Default tool timeout (60 s).
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(60);

/// Command-interpreter/script hosts refused regardless of the allowlist.
///
/// Denying these by name keeps a policy (or a future default) from turning one
/// approved call into an unconstrained command string. The list is deliberately
/// exact and documented rather than heuristic; it is **not** a complete
/// arbitrary-code-execution boundary (see the module docs).
pub const SHELL_INTERPRETER_HOSTS: &[&str] = &[
    // Windows also resolves bare names through PATHEXT, so both the bare and
    // `.exe` spellings are denied.
    "cmd",
    "cmd.exe",
    "command",
    "command.com",
    "powershell",
    "powershell.exe",
    "pwsh",
    "pwsh.exe",
    "cscript",
    "cscript.exe",
    "wscript",
    "wscript.exe",
    "mshta",
    "mshta.exe",
    "wsl",
    "wsl.exe",
    "busybox",
    "busybox.exe",
    "sh",
    "sh.exe",
    "bash",
    "bash.exe",
    "zsh",
    "zsh.exe",
    "dash",
    "dash.exe",
    "ksh",
    "ksh.exe",
    "fish",
    "fish.exe",
];

/// Whether a bare program name is a denied command-interpreter host.
#[must_use]
pub fn is_shell_interpreter(program: &str) -> bool {
    // Windows strips trailing dots and spaces from executable names; normalize
    // both so `cmd.exe.` cannot slip past the name check if a policy lists it.
    let program = program.trim().trim_end_matches('.').to_lowercase();
    SHELL_INTERPRETER_HOSTS
        .iter()
        .any(|host| *host == program.as_str())
}

/// Refuses argument forms that a command interpreter would treat as syntax.
///
/// Arguments are normally passed directly to a non-interpreter program, but the
/// listed forms are refused before the approval grant is consumed so a mistaken
/// `cmd /c`-style launch can never become a command string. The exact denied
/// forms are `/c`, `/k`, `&`, `|`, `>` and CR/LF.
///
/// # Errors
/// [`Refusal::ToolNotAllowed`] naming the offending form (the argument value is
/// not echoed into the refusal detail beyond a bounded quoted excerpt).
pub fn validate_arguments(args: &[String]) -> Result<(), ToolError> {
    for argument in args {
        let trimmed = argument.trim();
        if trimmed.eq_ignore_ascii_case("/c") || trimmed.eq_ignore_ascii_case("/k") {
            return Err(ToolError::new(
                Refusal::ToolNotAllowed,
                format!(
                    "shell_exec argument {argument:?} is a command-interpreter switch; \
                     interpreter switches (/c, /k) are refused before execution"
                ),
            ));
        }
        if argument.contains('&')
            || argument.contains('|')
            || argument.contains('>')
            || argument.contains('\n')
            || argument.contains('\r')
        {
            return Err(ToolError::new(
                Refusal::ToolNotAllowed,
                format!(
                    "shell_exec argument {argument:?} contains a shell escape/metacharacter \
                     (&, |, >, or a newline); such arguments are refused before execution"
                ),
            ));
        }
    }
    Ok(())
}

/// Per-command argument policy for one allowlisted executable.
///
/// A spec is the machine-checked half of the command allowlist: it bounds how
/// many arguments one call may carry, refuses exact argument prefixes that
/// re-point the executable at attacker-chosen code or configuration, and can
/// require a fixed leading token (for example only `git --version`).
///
/// The spec validates *shape*, not intent: an allowed program that can itself
/// execute code (`git`, `cargo`) still executes repository-controlled code.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandSpec {
    program: String,
    first_arg_allowlist: Option<Vec<String>>,
    denied_arg_prefixes: Vec<String>,
    max_args: Option<usize>,
}

impl CommandSpec {
    /// Builds a permissive spec (bounds/denials empty) for `program`.
    #[must_use]
    pub fn new(program: impl Into<String>) -> Self {
        Self {
            program: program.into().trim().to_lowercase(),
            first_arg_allowlist: None,
            denied_arg_prefixes: Vec::new(),
            max_args: None,
        }
    }

    /// Requires the first argument to be one of `allowed` (exact,
    /// case-insensitive); a call with no arguments is refused.
    #[must_use]
    pub fn with_first_arg_allowlist(mut self, allowed: impl IntoIterator<Item = String>) -> Self {
        let mut allowed: Vec<String> = allowed
            .into_iter()
            .map(|token| token.trim().to_lowercase())
            .filter(|token| !token.is_empty())
            .collect();
        allowed.sort();
        allowed.dedup();
        self.first_arg_allowlist = Some(allowed);
        self
    }

    /// Refuses any argument that starts with one of `prefixes`.
    ///
    /// The comparison is case-sensitive on purpose: Windows flags such as git's
    /// `-C` and `-c` differ only by case but mean completely different things,
    /// so folding case would either ban `-c` or allow `-C`.
    #[must_use]
    pub fn with_denied_arg_prefixes(mut self, prefixes: impl IntoIterator<Item = String>) -> Self {
        let mut prefixes: Vec<String> = prefixes
            .into_iter()
            .map(|prefix| prefix.trim().to_owned())
            .filter(|prefix| !prefix.is_empty())
            .collect();
        prefixes.sort();
        prefixes.dedup();
        self.denied_arg_prefixes = prefixes;
        self
    }

    /// Caps the argument count for this program (in addition to any global cap).
    #[must_use]
    pub fn with_max_args(mut self, max_args: usize) -> Self {
        self.max_args = Some(max_args);
        self
    }

    /// The bare program name this spec governs.
    #[must_use]
    pub fn program(&self) -> &str {
        &self.program
    }

    /// The configured argument cap, if any.
    #[must_use]
    pub fn max_args(&self) -> Option<usize> {
        self.max_args
    }

    /// The configured leading-token allowlist, if any.
    #[must_use]
    pub fn first_arg_allowlist(&self) -> Option<&[String]> {
        self.first_arg_allowlist.as_deref()
    }

    /// The configured denied argument prefixes.
    #[must_use]
    pub fn denied_arg_prefixes(&self) -> &[String] {
        &self.denied_arg_prefixes
    }

    /// Validates `args` against this command's argument policy.
    ///
    /// # Errors
    /// [`Refusal::ToolNotAllowed`] naming the violated rule (argument count,
    /// denied prefix, or a first token outside the allowlist). The offending
    /// argument is quoted only as a bounded excerpt.
    pub fn validate_args(&self, args: &[String]) -> Result<(), ToolError> {
        if let Some(max) = self.max_args {
            if args.len() > max {
                return Err(ToolError::new(
                    Refusal::ToolNotAllowed,
                    format!(
                        "{} carries {} arguments; this command's policy caps it at {max}",
                        self.program,
                        args.len()
                    ),
                ));
            }
        }
        for argument in args {
            if let Some(prefix) = self
                .denied_arg_prefixes
                .iter()
                .find(|prefix| argument.starts_with(prefix.as_str()))
            {
                return Err(ToolError::new(
                    Refusal::ToolNotAllowed,
                    format!(
                        "{} argument {argument:?} starts with the policy-denied prefix \
                         {prefix:?}; the command policy refuses arguments that re-point \
                         the executable",
                        self.program
                    ),
                ));
            }
        }
        if let Some(allowed) = &self.first_arg_allowlist {
            let first = args.first().map(String::as_str).unwrap_or("");
            if !allowed
                .iter()
                .any(|token| token.eq_ignore_ascii_case(first))
            {
                return Err(ToolError::new(
                    Refusal::ToolNotAllowed,
                    format!(
                        "{} must start with one of [{}]; {active} is not permitted by the \
                         command policy",
                        self.program,
                        allowed.join(", "),
                        active = if first.is_empty() {
                            "an empty argument list".to_owned()
                        } else {
                            format!("{first:?}")
                        }
                    ),
                ));
            }
        }
        Ok(())
    }
}

/// The shell allowlist, per-command argument policies, and resource caps.
#[derive(Debug, Clone)]
pub struct ShellPolicy {
    allowed_programs: Vec<String>,
    command_specs: Vec<CommandSpec>,
    max_stdout_bytes: usize,
    max_stderr_bytes: usize,
    timeout: Duration,
}

impl ShellPolicy {
    /// Creates a policy from the program allowlist (comparison is
    /// case-insensitive on the bare executable name).
    ///
    /// Every allowlisted program starts with a permissive [`CommandSpec`];
    /// call [`ShellPolicy::with_command_spec`] to add argument rules.
    #[must_use]
    pub fn new(allowed_programs: impl IntoIterator<Item = String>) -> Self {
        let mut allowed: Vec<String> = allowed_programs
            .into_iter()
            .map(|program| program.trim().to_lowercase())
            .filter(|program| !program.is_empty())
            .collect();
        allowed.sort();
        allowed.dedup();
        let command_specs = allowed
            .iter()
            .map(|program| CommandSpec::new(program.clone()))
            .collect();
        Self {
            allowed_programs: allowed,
            command_specs,
            max_stdout_bytes: DEFAULT_MAX_STDOUT_BYTES,
            max_stderr_bytes: DEFAULT_MAX_STDERR_BYTES,
            timeout: DEFAULT_TIMEOUT,
        }
    }

    /// Adds or replaces the per-command argument policy for one program.
    ///
    /// A spec for a program that is not on the allowlist is retained but never
    /// matches: the allowlist check runs first.
    #[must_use]
    pub fn with_command_spec(mut self, spec: CommandSpec) -> Self {
        self.command_specs
            .retain(|existing| existing.program() != spec.program());
        self.command_specs.push(spec);
        self
    }

    /// The spec governing `program`, if one exists.
    #[must_use]
    pub fn command_spec(&self, program: &str) -> Option<&CommandSpec> {
        let program = program.trim().to_lowercase();
        self.command_specs
            .iter()
            .find(|spec| spec.program() == program)
    }

    /// The full pre-spawn validation: interpreter denial, allowlist membership,
    /// global argument escape forms, and the program's own argument policy.
    ///
    /// # Errors
    /// [`Refusal::ToolNotAllowed`] for any violated rule; nothing is spawned and
    /// no approval grant is consumed by a caller that validates before commit.
    pub fn validate_invocation(&self, program: &str, args: &[String]) -> Result<(), ToolError> {
        if is_shell_interpreter(program) {
            return Err(ToolError::new(
                Refusal::ToolNotAllowed,
                format!(
                    "program {program:?} is a command-interpreter host; interpreter hosts are \
                     denied regardless of the allowlist"
                ),
            ));
        }
        if !self.allows(program) {
            return Err(ToolError::new(
                Refusal::ToolNotAllowed,
                format!(
                    "program {program:?} is not on the shell allowlist ({}); \
                     path-qualified programs are refused",
                    self.allowed_programs().join(", ")
                ),
            ));
        }
        validate_arguments(args)?;
        if let Some(spec) = self.command_spec(program) {
            spec.validate_args(args)?;
        }
        Ok(())
    }

    /// The default allowlist for v1 developer utilities.
    ///
    /// `cmd.exe` is intentionally **absent**: a command interpreter turns one
    /// approved call into an arbitrary command string. This is not a sandbox —
    /// `git.exe` and `cargo.exe` can execute repository-controlled code with the
    /// user's privileges (hooks, aliases, build scripts) — so `shell_exec`
    /// remains approval-gated arbitrary code execution. The approval gate
    /// (`Execute` permission class) still runs before execution, and every
    /// interpreter host is refused regardless of this list.
    #[must_use]
    pub fn conservative_default() -> Self {
        // `git -C`, `--git-dir`, `--work-tree`, `--exec-path` and the transport
        // `--upload-pack`/`--receive-pack` flags re-point git at an attacker
        // chosen directory, executable, or remote helper; they are refused so
        // the resolved jail working directory is not silently overridden. This
        // does NOT make git safe: aliases (`-c alias.x=!...`) still run
        // arbitrary code with the server's privileges.
        let git = CommandSpec::new("git.exe")
            .with_max_args(64)
            .with_denied_arg_prefixes([
                "-C".to_owned(),
                "--git-dir".to_owned(),
                "--work-tree".to_owned(),
                "--exec-path".to_owned(),
                "--upload-pack".to_owned(),
                "--receive-pack".to_owned(),
            ]);
        // `cargo --config` can inject a `target.<triple>.runner`/rustflags
        // wrapper; the build scripts `cargo test` already runs remain
        // arbitrary code, so this bounds one extra configuration channel only.
        let cargo = CommandSpec::new("cargo.exe")
            .with_max_args(64)
            .with_denied_arg_prefixes(["--config".to_owned()]);
        Self::new(["git.exe".to_owned(), "cargo.exe".to_owned()])
            .with_command_spec(git)
            .with_command_spec(cargo)
    }

    /// Overrides the tool timeout.
    #[must_use]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Overrides the stdout/stderr caps.
    #[must_use]
    pub fn with_stdout_cap(mut self, max_stdout_bytes: usize) -> Self {
        self.max_stdout_bytes = max_stdout_bytes;
        self
    }

    /// The allowlist.
    #[must_use]
    pub fn allowed_programs(&self) -> &[String] {
        &self.allowed_programs
    }

    /// Whether a program name is allowed (bare name only).
    ///
    /// Command-interpreter hosts ([`is_shell_interpreter`]) are never allowed,
    /// even when an explicit policy lists them.
    #[must_use]
    pub fn allows(&self, program: &str) -> bool {
        if program.contains('\\') || program.contains('/') {
            return false;
        }
        if is_shell_interpreter(program) {
            return false;
        }
        self.allowed_programs
            .iter()
            .any(|allowed| allowed == &program.trim().to_lowercase())
    }

    /// Configured timeout.
    #[must_use]
    pub fn timeout(&self) -> Duration {
        self.timeout
    }
}

/// One shell request (arguments passed directly, no shell interpolation).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShellRequest {
    /// Bare program name.
    pub program: String,
    /// Arguments.
    pub args: Vec<String>,
    /// Workspace-relative working directory (`None` = workspace root).
    pub cwd: Option<String>,
}

impl ShellRequest {
    /// Builds a request.
    #[must_use]
    pub fn new(program: impl Into<String>, args: Vec<String>) -> Self {
        Self {
            program: program.into(),
            args,
            cwd: None,
        }
    }

    /// Sets the working directory.
    #[must_use]
    pub fn with_cwd(mut self, cwd: impl Into<String>) -> Self {
        self.cwd = Some(cwd.into());
        self
    }
}

/// Result of an executed child process (non-zero exit is a result, not a
/// refusal: the program ran and its exit code is data).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ShellOutput {
    /// Program executed.
    pub program: String,
    /// Arguments passed.
    pub args: Vec<String>,
    /// Exit code (`None` when terminated by signal).
    pub exit_code: Option<i32>,
    /// Whether the process exited successfully (exit code 0).
    pub success: bool,
    /// Captured stdout (capped).
    pub stdout: String,
    /// Captured stderr (capped).
    pub stderr: String,
    /// Stdout or stderr was truncated at the cap.
    pub truncated: bool,
    /// Wall-clock duration in milliseconds.
    pub duration_ms: u128,
}

/// Executes an allowlisted program inside the jail.
///
/// # Errors
/// [`Refusal::ToolNotAllowed`] for a program outside the allowlist, a
/// separator-bearing program string, or a per-command argument policy
/// violation; [`Refusal::PathOutsideJail`] for an escaping working directory;
/// [`E_SANDBOX_UNAVAILABLE`] when no OS process-tree sandbox exists (refused
/// before spawn, never run uncontained); [`Refusal::ToolTimeout`] when the child
/// exceeds the timeout (whole tree terminated); [`Refusal::ToolCancelled`] when
/// the run is cancelled; [`Refusal::InternalError`] for spawn/I/O failures.
pub async fn execute(
    jail: &WorkspaceJail,
    policy: &ShellPolicy,
    request: &ShellRequest,
    cancellation: Option<&CancellationToken>,
) -> Result<ShellOutput, ToolError> {
    policy.validate_invocation(&request.program, &request.args)?;
    let cwd: PathBuf = match &request.cwd {
        Some(cwd) => jail.resolve(cwd)?,
        None => jail.root().to_path_buf(),
    };
    if !cwd.is_dir() {
        return Err(ToolError::new(
            Refusal::ToolSchema,
            "shell working directory is not a directory inside the jail",
        ));
    }

    // Containment is acquired BEFORE the child exists: a host that cannot
    // provide a process-tree sandbox refuses by name instead of running the
    // command uncontained.
    let sandbox = acquire_process_tree_sandbox()?;

    let started = Instant::now();
    let mut command = Command::new(&request.program);
    command
        .args(&request.args)
        .current_dir(&cwd)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);
    crate::env_safety::sanitize_tokio(&mut command);

    let mut child = command.spawn().map_err(|error| {
        ToolError::new(
            Refusal::InternalError,
            format!("failed to spawn {:?}: {error}", request.program),
        )
    })?;

    // Fail closed: if the child cannot be placed inside the Job Object, it is
    // killed immediately and the call is refused by name rather than reported
    // as contained.
    if let Err(error) = sandbox.contain(&child) {
        let _ = child.start_kill();
        return Err(error);
    }

    let stdout = child.stdout.take().ok_or_else(|| {
        ToolError::new(Refusal::InternalError, "child stdout pipe is unavailable")
    })?;
    let stderr = child.stderr.take().ok_or_else(|| {
        ToolError::new(Refusal::InternalError, "child stderr pipe is unavailable")
    })?;
    let stdout_cap = policy.max_stdout_bytes;
    let stderr_cap = policy.max_stderr_bytes;

    let work = async move {
        let (stdout, stdout_truncated) = read_capped(stdout, stdout_cap).await?;
        let (stderr, stderr_truncated) = read_capped(stderr, stderr_cap).await?;
        let status = child.wait().await?;
        Ok::<_, std::io::Error>((stdout, stdout_truncated, stderr, stderr_truncated, status))
    };

    let timed = async {
        if let Some(token) = cancellation {
            tokio::select! {
                () = token.cancelled() => Outcome::Cancelled,
                result = tokio::time::timeout(policy.timeout, work) => outcome_from(result),
            }
        } else {
            outcome_from(tokio::time::timeout(policy.timeout, work).await)
        }
    }
    .await;

    if !matches!(&timed, Outcome::Finished(..)) {
        // Timeout/cancellation/I/O: close the Job Object now so the whole
        // process tree is terminated before the refusal is returned. The
        // direct child is also killed by `kill_on_drop`.
        drop(sandbox);
    }

    match timed {
        Outcome::Finished(stdout, stdout_truncated, stderr, stderr_truncated, status) => {
            Ok(ShellOutput {
                program: request.program.clone(),
                args: request.args.clone(),
                exit_code: status.code(),
                success: status.success(),
                stdout: String::from_utf8_lossy(&stdout).into_owned(),
                stderr: String::from_utf8_lossy(&stderr).into_owned(),
                truncated: stdout_truncated || stderr_truncated,
                duration_ms: started.elapsed().as_millis(),
            })
        }
        Outcome::TimedOut => Err(ToolError::new(
            Refusal::ToolTimeout,
            format!(
                "{:?} exceeded the {} ms tool timeout and was terminated",
                request.program,
                policy.timeout.as_millis()
            ),
        )),
        Outcome::Cancelled => Err(ToolError::new(
            Refusal::ToolCancelled,
            format!("{:?} was cancelled by run cancellation", request.program),
        )),
        Outcome::Io(detail) => Err(ToolError::new(
            Refusal::InternalError,
            format!("I/O failure while running {:?}: {detail}", request.program),
        )),
    }
}

enum Outcome {
    Finished(Vec<u8>, bool, Vec<u8>, bool, std::process::ExitStatus),
    TimedOut,
    Cancelled,
    Io(String),
}

fn outcome_from(
    result: Result<Result<(Vec<u8>, bool, Vec<u8>, bool, std::process::ExitStatus), std::io::Error>, tokio::time::error::Elapsed>,
) -> Outcome {
    match result {
        Err(_) => Outcome::TimedOut,
        Ok(Err(error)) => Outcome::Io(error.to_string()),
        Ok(Ok((stdout, stdout_truncated, stderr, stderr_truncated, status))) => {
            Outcome::Finished(stdout, stdout_truncated, stderr, stderr_truncated, status)
        }
    }
}

/// Reads at most `cap` bytes while draining the pipe (no deadlock, no
/// unbounded buffering); reports truncation.
async fn read_capped<R: AsyncRead + Unpin>(
    mut reader: R,
    cap: usize,
) -> std::io::Result<(Vec<u8>, bool)> {
    let mut chunk = [0u8; 8192];
    let mut stored = Vec::new();
    let mut truncated = false;
    loop {
        let read = reader.read(&mut chunk).await?;
        if read == 0 {
            break;
        }
        let remaining = cap.saturating_sub(stored.len());
        let take = read.min(remaining);
        if take > 0 {
            stored.extend_from_slice(&chunk[..take]);
        }
        if take < read {
            truncated = true;
        }
    }
    Ok((stored, truncated))
}

/// Named refusal code returned when no OS-level process-tree containment exists.
///
/// The frozen refusal table (`hiverouter-contract/src/refusal.rs`) has no
/// sandbox variant and this module may not add one, so the *wire* code is
/// [`Refusal::InternalError`] and this name is carried verbatim at the start of
/// the error detail. Callers that need a first-class code must add a
/// `SandboxUnavailable` variant to the contract table (out of scope here).
pub const E_SANDBOX_UNAVAILABLE: &str = "E_SANDBOX_UNAVAILABLE";

/// Whether this build can place a child process tree under OS containment.
///
/// Windows uses a kill-on-close Job Object ([`win32job`]); no other backend is
/// implemented, so other hosts report `false` and refuse execution instead of
/// running uncontained.
#[must_use]
pub const fn process_tree_sandbox_available() -> bool {
    cfg!(windows)
}

/// The named instrumentation point for the containment requirement.
///
/// # Errors
/// The [`E_SANDBOX_UNAVAILABLE`] refusal when `available` is false. Factored out
/// so the refusal path is testable on hosts where the sandbox happens to work.
pub fn ensure_process_tree_sandbox(available: bool) -> Result<(), ToolError> {
    if available {
        return Ok(());
    }
    Err(sandbox_unavailable(
        "this host has no OS process-tree sandbox (Windows kill-on-close Job Object); \
         shell_exec refuses rather than run an uncontained command. Update the host to \
         Windows or add a sandbox backend",
    ))
}

/// Builds the [`E_SANDBOX_UNAVAILABLE`] refusal.
fn sandbox_unavailable(detail: impl std::fmt::Display) -> ToolError {
    ToolError::new(
        Refusal::InternalError,
        format!("{E_SANDBOX_UNAVAILABLE}: {detail}"),
    )
}

/// Kill-on-close Job Object containment for one shell child.
///
/// Contract: while this value is alive the child and every descendant it spawns
/// belong to the job; dropping it terminates the whole tree. It is **not** a
/// filesystem or network sandbox: the child keeps the server user's privileges
/// and can read/write outside the jail.
#[cfg(windows)]
struct ProcessTreeSandbox {
    job: win32job::Job,
}

#[cfg(windows)]
impl ProcessTreeSandbox {
    fn create() -> Result<Self, ToolError> {
        let mut info = win32job::ExtendedLimitInfo::new();
        info.limit_kill_on_job_close();
        let job = win32job::Job::create_with_limit_info(&mut info)
            .map_err(|error| sandbox_unavailable(format!("Job Object creation failed: {error}")))?;
        Ok(Self { job })
    }

    fn contain(&self, child: &Child) -> Result<(), ToolError> {
        let handle = child
            .raw_handle()
            .ok_or_else(|| sandbox_unavailable("child process handle is unavailable"))?;
        self.job.assign_process(handle as isize).map_err(|error| {
            sandbox_unavailable(format!(
                "Job Object assignment failed, child terminated: {error}"
            ))
        })
    }
}

/// Non-Windows: there is no containment backend, so acquisition refuses.
#[cfg(not(windows))]
struct ProcessTreeSandbox;

#[cfg(not(windows))]
impl ProcessTreeSandbox {
    fn create() -> Result<Self, ToolError> {
        Err(sandbox_unavailable(
            "no non-Windows process-tree backend is implemented; shell_exec refuses \
             rather than run an uncontained command",
        ))
    }

    fn contain(&self, _child: &Child) -> Result<(), ToolError> {
        Err(sandbox_unavailable(
            "no non-Windows process-tree backend is implemented",
        ))
    }
}

fn acquire_process_tree_sandbox() -> Result<ProcessTreeSandbox, ToolError> {
    ensure_process_tree_sandbox(process_tree_sandbox_available())?;
    ProcessTreeSandbox::create()
}


#[cfg(test)]
mod tests {
    use super::*;

    fn jail() -> (tempfile::TempDir, WorkspaceJail) {
        let temp = tempfile::tempdir().expect("tempdir");
        let jail = WorkspaceJail::open(temp.path()).expect("jail");
        (temp, jail)
    }

    /// Explicit non-interpreter test policy. Tests must never depend on a
    /// command interpreter being allowlisted; `git.exe` is safe to execute on
    /// this host (a version banner, or a bounded external `sleep` alias that
    /// Git for Windows supplies through its own shell).
    fn test_policy() -> ShellPolicy {
        ShellPolicy::new(["git.exe".to_owned()])
    }

    /// `git` alias that runs the Git-for-Windows shell's `sleep` in the
    /// foreground; used to exercise timeout/cancellation deterministically
    /// without depending on PATH-shadowed `ping.exe` flavours.
    fn sleep_args(seconds: &str) -> Vec<String> {
        vec![
            "-c".to_owned(),
            format!("alias.hive-sleep=!sleep {seconds}"),
            "hive-sleep".to_owned(),
        ]
    }

    #[test]
    fn allowlist_accepts_bare_names_only() {
        let policy = ShellPolicy::conservative_default();
        assert!(policy.allows("git.exe"));
        assert!(policy.allows("GIT.EXE"));
        assert!(
            !policy.allows("cmd.exe"),
            "cmd.exe was removed from the default allowlist"
        );
        assert!(
            policy.allows("git.exe") && !policy.allows("powershell.exe"),
            "shell interpreter hosts are never allowed"
        );
        assert!(!policy.allows("C:\\Windows\\System32\\git.exe"));
        assert!(!policy.allows("..\\git.exe"));

        // Even a policy that explicitly lists an interpreter cannot enable it.
        let explicit = ShellPolicy::new(["cmd.exe".to_owned(), "git.exe".to_owned()]);
        assert!(
            !explicit.allows("cmd.exe"),
            "interpreter hosts are denied regardless of the allowlist"
        );
        assert!(explicit.allows("git.exe"));
    }

    #[tokio::test]
    async fn non_allowlisted_program_is_refused_by_name() {
        let (_temp, jail) = jail();
        let policy = test_policy();
        let request = ShellRequest::new("powershell.exe", vec![]);
        let error = execute(&jail, &policy, &request, None)
            .await
            .expect_err("not allowed");
        assert_eq!(error.code(), "E_TOOL_NOT_ALLOWED");
    }

    #[tokio::test]
    async fn interpreter_host_is_refused_even_when_explicitly_allowlisted() {
        let (_temp, jail) = jail();
        let policy = ShellPolicy::new(["cmd.exe".to_owned()]);
        for program in [
            "cmd",
            "cmd.exe",
            "CMD.EXE",
            "cmd.exe.",
            "powershell.exe",
            "pwsh.exe",
            "wsl.exe",
            "sh.exe",
        ] {
            let request = ShellRequest::new(program, vec![]);
            let error = execute(&jail, &policy, &request, None)
                .await
                .expect_err(program);
            assert_eq!(error.code(), "E_TOOL_NOT_ALLOWED", "{program}");
            assert!(
                error.detail().contains("interpreter"),
                "the refusal names the interpreter policy: {program}: {error}"
            );
        }
    }

    #[tokio::test]
    async fn argument_escape_forms_are_refused_before_spawn() {
        let (_temp, jail) = jail();
        let policy = test_policy();
        for argument in ["/c", "/C", "/k", "a&b", "a|b", "a>b", "a\nb", "a\rb"] {
            let request = ShellRequest::new("git.exe", vec![argument.to_owned()]);
            let error = execute(&jail, &policy, &request, None)
                .await
                .expect_err(argument);
            assert_eq!(error.code(), "E_TOOL_NOT_ALLOWED", "{argument:?}");
        }
        // Well-formed direct arguments are not rejected by the escape check.
        validate_arguments(&["--version".to_owned()]).expect("plain argument");
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn allowlisted_command_runs_and_captures_stdout() {
        let (_temp, jail) = jail();
        let policy = test_policy();
        let request = ShellRequest::new("git.exe", vec!["--version".into()]);
        let output = execute(&jail, &policy, &request, None)
            .await
            .expect("executes");
        assert!(output.success, "exit: {:?}", output.exit_code);
        assert!(
            output.stdout.to_lowercase().contains("git version"),
            "stdout: {}",
            output.stdout
        );
        assert_eq!(output.exit_code, Some(0));
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn child_process_does_not_inherit_credentials() {
        // Every canary mimics a credential name a provider, forge, or cloud SDK
        // reads; the child environment is an allowlist, so none may reach it.
        let canaries: [(&str, &str); 5] = [
            (
                hiverouter_provider::API_KEY_ENV,
                "sk-shell-env-test-not-a-real-key",
            ),
            ("GITHUB_TOKEN", "ghp_shell_env_canary_not_a_real_token"),
            (
                "ANTHROPIC_API_KEY",
                "sk-ant-shell-env-canary-not-a-real-key",
            ),
            ("AWS_SECRET_ACCESS_KEY", "aws-shell-env-canary-not-a-secret"),
            ("HIVEROUTER_SESSION_TOKEN", "hvr-shell-env-canary-not-a-token"),
        ];
        let saved: Vec<(&str, Option<std::ffi::OsString>)> = canaries
            .iter()
            .map(|(name, _)| (*name, std::env::var_os(name)))
            .collect();
        for (name, value) in canaries {
            std::env::set_var(name, value);
        }
        let (_temp, jail) = jail();
        let policy = test_policy();
        // `git` runs the external alias through its own shell; the environment
        // printed is the sanitized environment the shell child inherited.
        let request = ShellRequest::new(
            "git.exe",
            vec![
                "-c".into(),
                "alias.hive-env-probe=!env".into(),
                "hive-env-probe".into(),
            ],
        );
        let result = execute(&jail, &policy, &request, None).await;
        for (name, previous) in saved {
            match previous {
                Some(value) => std::env::set_var(name, value),
                None => std::env::remove_var(name),
            }
        }
        let output = result.expect("executes");
        assert!(
            output.success,
            "env probe must run: exit {:?}, stderr {}",
            output.exit_code, output.stderr
        );
        assert!(
            output.stdout.contains("PATH="),
            "env probe must print the child environment: {}",
            output.stdout
        );
        for (name, value) in canaries {
            assert!(
                !output.stdout.contains(value),
                "child inherited {name}: {}",
                output.stdout
            );
        }
        // The sanitizer never whitelists a credential-shaped name.
        for (name, _) in canaries {
            assert!(
                !crate::env_safety::ALLOWED_ENV
                    .iter()
                    .any(|allowed| allowed.eq_ignore_ascii_case(name)),
                "{name} must not be on the child environment allowlist"
            );
        }
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn timeout_terminates_the_child() {
        let (_temp, jail) = jail();
        let policy = test_policy().with_timeout(Duration::from_millis(300));
        let request = ShellRequest::new("git.exe", sleep_args("10"));
        let error = execute(&jail, &policy, &request, None)
            .await
            .expect_err("timeout");
        assert_eq!(error.code(), "E_TOOL_TIMEOUT");
    }

    #[test]
    fn sandbox_unavailable_is_a_named_refusal() {
        ensure_process_tree_sandbox(true).expect("available sandbox passes");
        let error = ensure_process_tree_sandbox(false).expect_err("unavailable");
        assert_eq!(error.code(), "E_INTERNAL_ERROR");
        assert!(
            error.detail().starts_with(E_SANDBOX_UNAVAILABLE),
            "the named refusal must lead the detail: {error}"
        );
        assert!(
            error.detail().contains("no OS process-tree sandbox"),
            "the refusal must say what is missing: {error}"
        );
        assert_eq!(
            process_tree_sandbox_available(),
            cfg!(windows),
            "only Windows has the kill-on-close Job Object backend"
        );
    }

    #[cfg(not(windows))]
    #[tokio::test]
    async fn execute_refuses_without_a_sandbox_backend() {
        let (_temp, jail) = jail();
        let policy = test_policy();
        let request = ShellRequest::new("git.exe", vec!["--version".into()]);
        let error = execute(&jail, &policy, &request, None)
            .await
            .expect_err("no sandbox backend");
        assert_eq!(error.code(), "E_INTERNAL_ERROR");
        assert!(error.detail().starts_with(E_SANDBOX_UNAVAILABLE));
    }

    #[tokio::test]
    async fn cwd_escape_is_refused_before_spawn() {
        let (_temp, jail) = jail();
        let policy = test_policy();
        for cwd in ["..", r"..\..", r"C:\Windows", r"\\server\share"] {
            let request = ShellRequest::new("git.exe", vec!["--version".into()]).with_cwd(cwd);
            let error = execute(&jail, &policy, &request, None)
                .await
                .expect_err(cwd);
            assert_eq!(error.code(), "E_PATH_OUTSIDE_JAIL", "cwd {cwd:?}");
            assert!(
                error.detail().contains("outside the workspace jail")
                    || error.detail().contains("refused"),
                "cwd {cwd:?}: {error}"
            );
        }
        // An in-jail cwd is still accepted, so the check is containment rather
        // than a blanket ban on working directories.
        let request = ShellRequest::new("git.exe", vec!["--version".into()]).with_cwd(".");
        if process_tree_sandbox_available() {
            execute(&jail, &policy, &request, None)
                .await
                .expect("in-jail cwd runs");
        }
    }

    #[test]
    fn per_command_argument_policy_is_enforced() {
        let policy = ShellPolicy::conservative_default();
        for args in [
            vec!["-C".to_owned(), ".".to_owned()],
            vec!["--upload-pack=evil".to_owned()],
            vec!["--git-dir".to_owned(), "outside".to_owned()],
            vec!["--work-tree=C:\\Windows".to_owned()],
        ] {
            let error = policy
                .validate_invocation("git.exe", &args)
                .expect_err(&format!("{args:?}"));
            assert_eq!(error.code(), "E_TOOL_NOT_ALLOWED", "{args:?}");
            assert!(
                error.detail().contains("policy-denied prefix"),
                "{args:?}: {error}"
            );
        }
        // The lowercase `-c` configuration flag is a different flag and stays
        // allowed: the denied-prefix comparison is case-sensitive.
        policy
            .validate_invocation(
                "git.exe",
                &[
                    "-c".to_owned(),
                    "core.fsmonitor=false".to_owned(),
                    "status".to_owned(),
                ],
            )
            .expect("lowercase -c is not -C");
        policy
            .validate_invocation(
                "cargo.exe",
                &["test".to_owned(), "--lib".to_owned()],
            )
            .expect("cargo test is allowed");
        assert_eq!(
            policy
                .validate_invocation("cargo.exe", &["--config=evil.toml".to_owned()])
                .expect_err("cargo --config")
                .code(),
            "E_TOOL_NOT_ALLOWED"
        );
    }

    #[test]
    fn first_argument_allowlist_pins_the_command_shape() {
        let strict = ShellPolicy::new(["git.exe".to_owned()]).with_command_spec(
            CommandSpec::new("git.exe")
                .with_first_arg_allowlist(["--version".to_owned()])
                .with_max_args(1),
        );
        strict
            .validate_invocation("git.exe", &["--version".to_owned()])
            .expect("pinned shape");
        for args in [
            Vec::<String>::new(),
            vec!["-c".to_owned(), "core.fsmonitor=false".to_owned()],
            vec!["--version".to_owned(), "--verbose".to_owned()],
        ] {
            let error = strict
                .validate_invocation("git.exe", &args)
                .expect_err(&format!("{args:?}"));
            assert_eq!(error.code(), "E_TOOL_NOT_ALLOWED", "{args:?}");
        }
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn timeout_kills_the_whole_process_tree() {
        // The marker is the duration of a grandchild `sleep`; if the Job Object
        // did not reap the tree it would linger for days. The marker is unique
        // to this test so a scan of the process table is unambiguous.
        let marker = "424242";
        let (_temp, jail) = jail();
        let policy = test_policy().with_timeout(Duration::from_millis(500));
        let request = ShellRequest::new(
            "git.exe",
            vec![
                "-c".to_owned(),
                format!("alias.hive-tree=!sh -c \"sleep {marker}; sleep 424243\""),
                "hive-tree".to_owned(),
            ],
        );
        let error = execute(&jail, &policy, &request, None)
            .await
            .expect_err("timeout");
        assert_eq!(error.code(), "E_TOOL_TIMEOUT");

        use sysinfo::{ProcessesToUpdate, System};
        let mut system = System::new_all().expect("sysinfo system");
        system.refresh_processes(ProcessesToUpdate::All, true);
        let current = sysinfo::get_current_pid().expect("current pid");
        assert!(
            system.process(current).is_some(),
            "the process table and command lines must be readable for this proof"
        );
        let mut reaped = false;
        for _ in 0..50 {
            system.refresh_processes(ProcessesToUpdate::All, true);
            let alive = system.processes().values().any(|process| {
                process
                    .cmd()
                    .iter()
                    .any(|argument| argument.to_string_lossy().contains(marker))
            });
            if !alive {
                reaped = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(
            reaped,
            "grandchild `sleep {marker}` survived the timeout: the process tree was not killed"
        );
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn stdout_cap_truncates_without_deadlock() {
        let (_temp, jail) = jail();
        let policy = test_policy().with_stdout_cap(4);
        let request = ShellRequest::new("git.exe", vec!["--version".into()]);
        let output = execute(&jail, &policy, &request, None)
            .await
            .expect("executes");
        assert!(output.truncated);
        assert!(output.stdout.len() <= 4, "stdout: {:?}", output.stdout);
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn cancellation_returns_named_refusal() {
        let (_temp, jail) = jail();
        let policy = test_policy().with_timeout(Duration::from_secs(30));
        // Long foreground sleep so the only exit path is cancellation.
        let request = ShellRequest::new("git.exe", sleep_args("10"));
        let token = CancellationToken::new();
        let cancel = token.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            cancel.cancel();
        });
        let error = execute(&jail, &policy, &request, Some(&token))
            .await
            .expect_err("cancelled");
        assert_eq!(error.code(), "E_TOOL_CANCELLED");
    }
}
