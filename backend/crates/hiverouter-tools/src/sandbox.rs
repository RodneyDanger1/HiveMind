//! Phase D1 sandbox truth: admin-free containment, per-run temp, honest status.
//!
//! # What D1 actually provides
//!
//! Phase D1 is **containment**, not virtualization. A contained shell run gets:
//!
//! 1. a kill-on-close Windows Job Object that holds the direct child and every
//!    descendant the job can see (process-tree lifetime only);
//! 2. the explicit program/argument allowlist enforced by [`crate::shell`];
//! 3. an allowlist child environment that drops credentials and also drops
//!    `USERPROFILE` / `HOME`;
//! 4. a per-run temp root at `<workspace>/.sandbox-tmp/<run-id>/` with
//!    `tmp/` for `TEMP`/`TMP` and `work/` as an explicit per-run working
//!    directory.
//!
//! It does **not** isolate the filesystem and does **not** isolate the network.
//! The child keeps the server user's token and can read/write outside the
//! workspace and open sockets. [`SandboxStatus`] states that explicitly
//! (`filesystem_isolated = false`, `network_isolated = false`) and carries a
//! named reason for each unavailable isolation; [`SandboxStatus::isolation_claim`]
//! returns the exact non-claim so no caller can present D1 as a sandbox.
//!
//! # Spawn-to-job-assignment race
//!
//! Containment is acquired (a Job Object is created) before the child exists,
//! but Windows only lets a process be assigned to a job *after* it has been
//! created. A child that forks during the short window before
//! `AssignProcessToJobObject` can leave a descendant outside the job. Closing
//! that window requires creating the process suspended, assigning it while
//! suspended, and resuming its primary thread:
//!
//! ```text
//! CreateProcess(CREATE_SUSPENDED) -> AssignProcessToJobObject -> ResumeThread(primary)
//! ```
//!
//! `std`/`tokio` can request `CREATE_SUSPENDED` through `creation_flags` (safe),
//! but neither exposes the primary *thread* handle, and `win32job` 2.0.3 only
//! wraps job creation/assignment/query. Resuming therefore needs
//! `OpenThread(THREAD_SUSPEND_RESUME)` + `ResumeThread`, or `NtResumeProcess`,
//! through raw Win32 FFI. This crate is `#![forbid(unsafe_code)]` and declares
//! only `win32job` (not `windows`/`windows-sys`), so the call cannot be added
//! from this module without lifting the unsafe ban and adding a dependency —
//! both outside this seat's owned files. Creating a suspended child with no
//! reachable resume path would deadlock every `shell_exec`, so D1 does not do
//! it. [`SandboxStatus::spawn_assignment_race_closed`] is consequently `false`
//! and [`SPAWN_ASSIGNMENT_RACE_BLOCKER`] records the exact blocker. Callers that
//! require a closed window can set
//! [`SandboxProfile::require_spawn_assignment_race_closed`] and receive a named
//! refusal from [`sandbox_preflight`] instead of a false claim.
//!
//! # Error identity
//!
//! The frozen refusal table has no sandbox variant and this seat may not add
//! one, so preflight failures carry a **local** named code on [`SandboxError`]
//! (`E_SANDBOX_ISOLATION_UNAVAILABLE`, `E_SANDBOX_RUN_ID`, `E_SANDBOX_TEMP`).
//! The Lead maps that code to the contract refusal at the boundary; the shell
//! path's existing bridge maps it to `E_INTERNAL_ERROR` with the local code as
//! the leading detail so no new variant is invented here.

use std::fmt;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::Serialize;

use crate::jail::WorkspaceJail;

/// Workspace-relative parent of every per-run sandbox directory.
pub const SANDBOX_TMP_DIR: &str = ".sandbox-tmp";

/// Name of the Phase D1 containment profile.
pub const PROFILE_D1: &str = "d1";

/// Local error code: the requested isolation cannot be provided by D1.
pub const E_SANDBOX_ISOLATION_UNAVAILABLE: &str = "E_SANDBOX_ISOLATION_UNAVAILABLE";
/// Local error code: the run id is not a safe single path segment.
pub const E_SANDBOX_RUN_ID: &str = "E_SANDBOX_RUN_ID";
/// Local error code: the per-run temp/working directory could not be prepared.
pub const E_SANDBOX_TEMP: &str = "E_SANDBOX_TEMP";

/// Named reason: filesystem isolation is unavailable in D1.
pub const REASON_FILESYSTEM_ISOLATION_UNAVAILABLE: &str = "FILESYSTEM_ISOLATION_UNAVAILABLE";
/// Named reason: network isolation is unavailable in D1.
pub const REASON_NETWORK_ISOLATION_UNAVAILABLE: &str = "NETWORK_ISOLATION_UNAVAILABLE";
/// Named reason: spawn-to-job-assignment race is still open.
pub const REASON_SPAWN_ASSIGNMENT_RACE_OPEN: &str = "SPAWN_ASSIGNMENT_RACE_OPEN";
/// Named reason: the host has no process-tree kill backend.
pub const REASON_PROCESS_TREE_KILL_UNAVAILABLE: &str = "PROCESS_TREE_KILL_UNAVAILABLE";

/// Exact blocker for the spawn-to-job-assignment race (see the module docs).
pub const SPAWN_ASSIGNMENT_RACE_BLOCKER: &str =
    "spawn->AssignProcessToJobObject window is open: closing it needs \
     CreateProcess(CREATE_SUSPENDED) + AssignProcessToJobObject + ResumeThread, but \
     std/tokio expose no primary-thread handle and win32job 2.0.3 has no thread/resume \
     API; raw Win32 FFI (OpenThread/ResumeThread or NtResumeProcess) is forbidden by \
     this crate's #![forbid(unsafe_code)] and windows/windows-sys is not a declared \
     dependency, so a suspended child could not be resumed";

/// The exact non-claim D1 is allowed to make.
pub const SANDBOX_CLAIM: &str =
    "process-tree containment + allowlist + environment scrub + per-run temp only; \
     this is NOT a filesystem sandbox and NOT a network sandbox";

/// Requested containment profile for one run.
///
/// The default profile ([`SandboxProfile::d1`]) requests the four containment
/// components D1 implements and no isolation. Requesting filesystem or network
/// isolation through [`SandboxProfile::with_filesystem_isolation`] /
/// [`SandboxProfile::with_network_isolation`] does not enable anything: it makes
/// [`sandbox_preflight`] return a named `E_SANDBOX_ISOLATION_UNAVAILABLE` error
/// before approval instead of silently pretending the request was met.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct SandboxProfile {
    /// Human-readable profile name (`d1` for [`SandboxProfile::d1`]).
    pub name: &'static str,
    /// Require a kill-on-close process-tree Job Object.
    pub require_process_tree_kill: bool,
    /// Require the explicit program/argument allowlist.
    pub require_allowlist: bool,
    /// Require the credential-scrubbing child environment allowlist.
    pub require_environment_scrub: bool,
    /// Require a per-run `TEMP`/`TMP` and working directory.
    pub require_run_temp: bool,
    /// Request filesystem isolation (unavailable in D1).
    pub require_filesystem_isolation: bool,
    /// Request network isolation (unavailable in D1).
    pub require_network_isolation: bool,
    /// Require the spawn-to-job-assignment race to be closed (unavailable in D1).
    pub require_spawn_assignment_race_closed: bool,
}

impl SandboxProfile {
    /// The Phase D1 profile: all four containment components, no isolation.
    #[must_use]
    pub const fn d1() -> Self {
        Self {
            name: PROFILE_D1,
            require_process_tree_kill: true,
            require_allowlist: true,
            require_environment_scrub: true,
            require_run_temp: true,
            require_filesystem_isolation: false,
            require_network_isolation: false,
            require_spawn_assignment_race_closed: false,
        }
    }

    /// Requests filesystem isolation, which D1 cannot provide.
    #[must_use]
    pub const fn with_filesystem_isolation(mut self) -> Self {
        self.require_filesystem_isolation = true;
        self
    }

    /// Requests network isolation, which D1 cannot provide.
    #[must_use]
    pub const fn with_network_isolation(mut self) -> Self {
        self.require_network_isolation = true;
        self
    }

    /// Requires the spawn-to-job-assignment window to be closed.
    #[must_use]
    pub const fn requiring_spawn_assignment_race_closed(mut self) -> Self {
        self.require_spawn_assignment_race_closed = true;
        self
    }
}

impl Default for SandboxProfile {
    fn default() -> Self {
        Self::d1()
    }
}

/// Which D1 containment components are available on this host.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct SandboxContainment {
    /// A kill-on-close Windows Job Object can hold the process tree.
    pub job_object_tree_kill: bool,
    /// The explicit program/argument allowlist is enforced.
    pub allowlist: bool,
    /// The child environment is an allowlist with credential/profile scrubbing.
    pub environment_scrub: bool,
    /// A per-run `TEMP`/`TMP` and working directory can be prepared.
    pub run_temp: bool,
}

impl SandboxContainment {
    /// Whether all four D1 containment components are available.
    #[must_use]
    pub const fn all_available(&self) -> bool {
        self.job_object_tree_kill && self.allowlist && self.environment_scrub && self.run_temp
    }
}

/// One named reason attached to a [`SandboxStatus`] or [`SandboxError`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SandboxReason {
    /// Stable machine code (for example `NETWORK_ISOLATION_UNAVAILABLE`).
    pub code: &'static str,
    /// Non-secret human-readable explanation.
    pub detail: String,
}

/// Honest capability report for one run id and requested profile.
///
/// `filesystem_isolated` and `network_isolated` are always `false` in D1; the
/// matching reason is present in [`SandboxStatus::reasons`]. The `temp_dir` and
/// `working_dir` fields are `Some` only for a prepared [`SandboxRun`]; the pure
/// capability report uses `None`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SandboxStatus {
    /// Run id the report covers.
    pub run_id: String,
    /// Requested profile name.
    pub profile: &'static str,
    /// D1 containment components available on this host.
    pub containment: SandboxContainment,
    /// Always `false` in D1: the child is not filesystem-isolated.
    pub filesystem_isolated: bool,
    /// Always `false` in D1: the child is not network-isolated.
    pub network_isolated: bool,
    /// Always `false` in D1: the spawn-to-job-assignment window is open.
    pub spawn_assignment_race_closed: bool,
    /// Named reasons for every unavailable isolation/limitation.
    pub reasons: Vec<SandboxReason>,
    /// Workspace-relative per-run `TEMP`/`TMP` directory, when prepared.
    pub temp_dir: Option<String>,
    /// Workspace-relative per-run working directory, when prepared.
    pub working_dir: Option<String>,
}

impl SandboxStatus {
    /// Whether every D1 containment component is available.
    #[must_use]
    pub const fn has_available_containment(&self) -> bool {
        self.containment.all_available()
    }

    /// The named reason for `code`, if present.
    #[must_use]
    pub fn reason(&self, code: &str) -> Option<&SandboxReason> {
        self.reasons.iter().find(|reason| reason.code == code)
    }

    /// The exact non-claim a caller may repeat: containment only, never a
    /// filesystem or network sandbox.
    #[must_use]
    pub const fn isolation_claim(&self) -> &'static str {
        SANDBOX_CLAIM
    }
}

/// Reports D1 capability truth without creating any directory.
///
/// This is the pure status API: `containment.run_temp` means the mechanism is
/// implementable, while a prepared [`SandboxRun`] additionally reports the
/// concrete `temp_dir`/`working_dir` it created.
#[must_use]
pub fn sandbox_status(run_id: &str, profile: SandboxProfile) -> SandboxStatus {
    let containment = SandboxContainment {
        job_object_tree_kill: cfg!(windows),
        allowlist: true,
        environment_scrub: true,
        run_temp: true,
    };
    let mut reasons = vec![
        SandboxReason {
            code: REASON_FILESYSTEM_ISOLATION_UNAVAILABLE,
            detail: "D1 runs the child with the server user's token and the workspace \
                     filesystem visible; a child can read and write outside the workspace. \
                     No filesystem namespace, filter, or read-only mount is installed"
                .to_owned(),
        },
        SandboxReason {
            code: REASON_NETWORK_ISOLATION_UNAVAILABLE,
            detail: "D1 installs no network namespace, firewall rule, or proxy boundary; a \
                     child can open the same sockets the server user can open"
                .to_owned(),
        },
        SandboxReason {
            code: REASON_SPAWN_ASSIGNMENT_RACE_OPEN,
            detail: SPAWN_ASSIGNMENT_RACE_BLOCKER.to_owned(),
        },
    ];
    if !containment.job_object_tree_kill {
        reasons.push(SandboxReason {
            code: REASON_PROCESS_TREE_KILL_UNAVAILABLE,
            detail: "this host has no Windows kill-on-close Job Object backend, so the \
                     process tree cannot be contained; shell execution refuses"
                .to_owned(),
        });
    }
    SandboxStatus {
        run_id: run_id.to_owned(),
        profile: profile.name,
        containment,
        filesystem_isolated: false,
        network_isolated: false,
        spawn_assignment_race_closed: false,
        reasons,
        temp_dir: None,
        working_dir: None,
    }
}

/// A prepared per-run sandbox directory set.
///
/// Layout (all inside the canonical workspace):
///
/// ```text
/// .sandbox-tmp/<run-id>/tmp    -> child TEMP and TMP
/// .sandbox-tmp/<run-id>/work   -> explicit per-run working directory
/// ```
///
/// Preparation refuses a run id that is not a safe single path segment, refuses
/// any profile requirement this host cannot meet, creates both directories, and
/// re-canonicalises them inside the workspace so a pre-planted symlink/junction
/// cannot redirect `TEMP` outside the jail.
#[derive(Debug, Clone)]
pub struct SandboxRun {
    run_id: String,
    root: PathBuf,
    temp_dir: PathBuf,
    working_dir: PathBuf,
    profile: SandboxProfile,
}

impl SandboxRun {
    /// Validates the profile and creates the per-run temp and working directories.
    ///
    /// # Errors
    /// [`E_SANDBOX_RUN_ID`] for an unsafe run id, [`E_SANDBOX_ISOLATION_UNAVAILABLE`]
    /// with named reasons when the profile requests something D1 cannot provide,
    /// or [`E_SANDBOX_TEMP`] when the directories cannot be created or resolve
    /// outside the workspace.
    pub fn prepare(
        jail: &WorkspaceJail,
        run_id: &str,
        profile: SandboxProfile,
    ) -> Result<Self, SandboxError> {
        validate_run_id(run_id)?;
        let capability = sandbox_status(run_id, profile);
        let reasons = unavailable_for_profile(&capability, profile);
        if !reasons.is_empty() {
            return Err(SandboxError::isolation_unavailable(run_id, reasons));
        }

        let relative_root = format!("{SANDBOX_TMP_DIR}/{run_id}");
        let root = jail.resolve(&relative_root).map_err(|error| {
            SandboxError::temp(format!(
                "cannot resolve the per-run sandbox directory {relative_root:?}: {}",
                error.detail()
            ))
        })?;
        // The jail's canonical root carries a Windows verbatim prefix
        // (`\\?\C:\...`). Native tools such as w64devkit's `ld.exe` reject that
        // prefix when it appears in a response-file argument (`@\\?\...`), so
        // the child-visible TEMP/working paths are normalised while the jail's
        // canonical root stays the containment authority.
        let temp_dir = child_visible_path(&root.join("tmp"));
        let working_dir = child_visible_path(&root.join("work"));
        fs::create_dir_all(&temp_dir).map_err(|error| {
            SandboxError::temp(format!(
                "cannot create the per-run TEMP directory {}: {error}",
                temp_dir.display()
            ))
        })?;
        fs::create_dir_all(&working_dir).map_err(|error| {
            SandboxError::temp(format!(
                "cannot create the per-run working directory {}: {error}",
                working_dir.display()
            ))
        })?;
        // A pre-planted junction/symlink under the run root must not redirect
        // TEMP or the working directory outside the workspace. Remove the
        // partial tree rather than leaving a directory the caller never sees.
        if let Err(error) =
            canonical_inside(jail, &temp_dir).and_then(|_| canonical_inside(jail, &working_dir))
        {
            let _ = fs::remove_dir_all(&root);
            return Err(error);
        }

        Ok(Self {
            run_id: run_id.to_owned(),
            root,
            temp_dir,
            working_dir,
            profile,
        })
    }

    /// The run id this directory set belongs to.
    #[must_use]
    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    /// Absolute path of `.sandbox-tmp/<run-id>`.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Absolute path used for the child's `TEMP` and `TMP`.
    #[must_use]
    pub fn temp_dir(&self) -> &Path {
        &self.temp_dir
    }

    /// Absolute path of the explicit per-run working directory.
    #[must_use]
    pub fn working_dir(&self) -> &Path {
        &self.working_dir
    }

    /// The requested profile this run was prepared for.
    #[must_use]
    pub const fn profile(&self) -> SandboxProfile {
        self.profile
    }

    /// Status with the concrete `temp_dir`/`working_dir` this run prepared.
    #[must_use]
    pub fn status(&self) -> SandboxStatus {
        let mut status = sandbox_status(&self.run_id, self.profile);
        status.temp_dir = Some(format!("{SANDBOX_TMP_DIR}/{}/tmp", self.run_id));
        status.working_dir = Some(format!("{SANDBOX_TMP_DIR}/{}/work", self.run_id));
        status
    }

    /// Removes the run directory tree.
    ///
    /// Best effort by design: a detached child may briefly still hold a handle
    /// to the working directory after the Job Object closes. A missing tree is
    /// success; any other failure is returned for the caller to log.
    ///
    /// # Errors
    /// The underlying [`io::Error`] when a present tree cannot be removed.
    pub fn cleanup(&self) -> io::Result<()> {
        match fs::remove_dir_all(&self.root) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error),
        }
    }
}

/// Result of a successful [`sandbox_preflight`].
#[derive(Debug, Clone)]
pub struct SandboxPreflight {
    run: SandboxRun,
    status: SandboxStatus,
}

impl SandboxPreflight {
    /// The run id the preflight covers.
    #[must_use]
    pub fn run_id(&self) -> &str {
        self.run.run_id()
    }

    /// The truthful capability/status report captured at preflight time.
    #[must_use]
    pub fn status(&self) -> &SandboxStatus {
        &self.status
    }

    /// The prepared per-run directory set to hand to execution.
    #[must_use]
    pub fn run(&self) -> &SandboxRun {
        &self.run
    }

    /// Takes ownership of the prepared run (for example to keep it alive until
    /// the approval decision, then pass it to execution).
    #[must_use]
    pub fn into_run(self) -> SandboxRun {
        self.run
    }
}

/// Pre-approval check for one run and requested profile.
///
/// The Lead can call this before requesting approval: it validates the run id,
/// refuses any requirement D1 cannot meet with named reasons (filesystem
/// isolation, network isolation, a closed spawn-assignment window, or a missing
/// Job Object backend), and only then creates the per-run temp/working
/// directories. On `Ok`, hand [`SandboxPreflight::run`] to
/// [`crate::shell::execute_in_sandbox`]. If approval is denied instead, call
/// [`SandboxRun::cleanup`] on [`SandboxPreflight::run`] so the empty per-run
/// tree is removed.
///
/// # Errors
/// [`E_SANDBOX_RUN_ID`], [`E_SANDBOX_ISOLATION_UNAVAILABLE`] (with named
/// reasons), or [`E_SANDBOX_TEMP`]. The refusal table is not extended; the Lead
/// maps [`SandboxError::code`] at the boundary.
pub fn sandbox_preflight(
    jail: &WorkspaceJail,
    run_id: &str,
    requested_profile: SandboxProfile,
) -> Result<SandboxPreflight, SandboxError> {
    let run = SandboxRun::prepare(jail, run_id, requested_profile)?;
    let status = run.status();
    Ok(SandboxPreflight { run, status })
}

/// A local sandbox failure carrying a named code the Lead can map.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SandboxError {
    code: &'static str,
    detail: String,
    reasons: Vec<SandboxReason>,
}

impl SandboxError {
    fn new(code: &'static str, detail: impl Into<String>) -> Self {
        Self {
            code,
            detail: detail.into(),
            reasons: Vec::new(),
        }
    }

    fn isolation_unavailable(run_id: &str, reasons: Vec<SandboxReason>) -> Self {
        let codes = reasons
            .iter()
            .map(|reason| reason.code)
            .collect::<Vec<_>>()
            .join(", ");
        Self {
            code: E_SANDBOX_ISOLATION_UNAVAILABLE,
            detail: format!(
                "run {run_id:?} requested isolation D1 cannot provide ({codes}); \
                 shell execution was not started"
            ),
            reasons,
        }
    }

    fn temp(detail: impl Into<String>) -> Self {
        Self::new(E_SANDBOX_TEMP, detail)
    }

    /// The local stable code (`E_SANDBOX_*`) for the Lead to map.
    #[must_use]
    pub fn code(&self) -> &'static str {
        self.code
    }

    /// Non-secret human-readable detail.
    #[must_use]
    pub fn detail(&self) -> &str {
        &self.detail
    }

    /// The named reasons behind this failure (empty for a run-id/temp failure).
    #[must_use]
    pub fn reasons(&self) -> &[SandboxReason] {
        &self.reasons
    }

    /// The named reason codes, in report order.
    #[must_use]
    pub fn reason_codes(&self) -> Vec<&'static str> {
        self.reasons.iter().map(|reason| reason.code).collect()
    }
}

impl fmt::Display for SandboxError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code, self.detail)
    }
}

impl std::error::Error for SandboxError {}

fn validate_run_id(run_id: &str) -> Result<(), SandboxError> {
    if run_id.is_empty() || run_id.len() > 128 {
        return Err(SandboxError::new(
            E_SANDBOX_RUN_ID,
            "run id must be 1..=128 bytes",
        ));
    }
    if run_id == "." || run_id == ".." || run_id.starts_with('.') {
        return Err(SandboxError::new(
            E_SANDBOX_RUN_ID,
            format!("run id {run_id:?} must not be a dot segment or start with '.'"),
        ));
    }
    if !run_id
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(SandboxError::new(
            E_SANDBOX_RUN_ID,
            format!(
                "run id {run_id:?} must contain only ASCII letters, digits, '-', '_', or '.'; \
                 path separators, drive prefixes, and '~' aliases are refused"
            ),
        ));
    }
    Ok(())
}

/// Removes a Windows verbatim prefix from a child-visible path.
///
/// `\\?\C:\dir` becomes `C:\dir`; `\\?\UNC\server\share` becomes
/// `\\server\share`. Non-Windows paths are returned unchanged. Containment is
/// unchanged: the stripped path names the same object, and every prepared
/// directory is re-canonicalised and checked against the jail separately.
fn child_visible_path(path: &Path) -> PathBuf {
    #[cfg(not(windows))]
    {
        path.to_path_buf()
    }
    #[cfg(windows)]
    {
        let text = path.to_string_lossy();
        let Some(rest) = text.strip_prefix(r"\\?\") else {
            return path.to_path_buf();
        };
        match rest.strip_prefix(r"UNC\") {
            Some(unc) => PathBuf::from(format!(r"\\{unc}")),
            None => PathBuf::from(rest),
        }
    }
}

fn canonical_inside(jail: &WorkspaceJail, path: &Path) -> Result<PathBuf, SandboxError> {
    let canonical = fs::canonicalize(path).map_err(|error| {
        SandboxError::temp(format!(
            "cannot canonicalise the prepared sandbox directory {}: {error}",
            path.display()
        ))
    })?;
    if !jail.is_inside(&canonical) {
        return Err(SandboxError::temp(format!(
            "prepared sandbox directory {} resolves outside the workspace root",
            path.display()
        )));
    }
    Ok(canonical)
}

fn unavailable_for_profile(status: &SandboxStatus, profile: SandboxProfile) -> Vec<SandboxReason> {
    let mut reasons = Vec::new();
    if profile.require_filesystem_isolation {
        if let Some(reason) = status.reason(REASON_FILESYSTEM_ISOLATION_UNAVAILABLE) {
            reasons.push(reason.clone());
        }
    }
    if profile.require_network_isolation {
        if let Some(reason) = status.reason(REASON_NETWORK_ISOLATION_UNAVAILABLE) {
            reasons.push(reason.clone());
        }
    }
    if profile.require_process_tree_kill && !status.containment.job_object_tree_kill {
        if let Some(reason) = status.reason(REASON_PROCESS_TREE_KILL_UNAVAILABLE) {
            reasons.push(reason.clone());
        }
    }
    if profile.require_allowlist && !status.containment.allowlist {
        reasons.push(SandboxReason {
            code: "ALLOWLIST_UNAVAILABLE",
            detail: "the explicit program/argument allowlist is not enforced".to_owned(),
        });
    }
    if profile.require_environment_scrub && !status.containment.environment_scrub {
        reasons.push(SandboxReason {
            code: "ENVIRONMENT_SCRUB_UNAVAILABLE",
            detail: "the child environment allowlist is not applied".to_owned(),
        });
    }
    if profile.require_run_temp && !status.containment.run_temp {
        reasons.push(SandboxReason {
            code: "RUN_TEMP_UNAVAILABLE",
            detail: "a per-run TEMP/TMP directory cannot be prepared".to_owned(),
        });
    }
    if profile.require_spawn_assignment_race_closed && !status.spawn_assignment_race_closed {
        if let Some(reason) = status.reason(REASON_SPAWN_ASSIGNMENT_RACE_OPEN) {
            reasons.push(reason.clone());
        }
    }
    reasons
}

#[cfg(test)]
mod tests {
    use super::*;

    fn jail() -> (tempfile::TempDir, WorkspaceJail) {
        let temp = tempfile::tempdir().expect("tempdir");
        let jail = WorkspaceJail::open(temp.path()).expect("jail");
        (temp, jail)
    }

    #[test]
    fn status_never_claims_filesystem_or_network_isolation() {
        let status = sandbox_status("run-status", SandboxProfile::d1());
        assert!(!status.filesystem_isolated);
        assert!(!status.network_isolated);
        assert!(
            status
                .reason(REASON_FILESYSTEM_ISOLATION_UNAVAILABLE)
                .is_some(),
            "the false filesystem claim must carry a named reason"
        );
        assert!(
            status
                .reason(REASON_NETWORK_ISOLATION_UNAVAILABLE)
                .is_some(),
            "the false network claim must carry a named reason"
        );
        assert!(!status.spawn_assignment_race_closed);
        assert!(
            status.reason(REASON_SPAWN_ASSIGNMENT_RACE_OPEN).is_some(),
            "the open spawn race must be stated"
        );
        assert_eq!(
            status.isolation_claim(),
            "process-tree containment + allowlist + environment scrub + per-run temp only; \
             this is NOT a filesystem sandbox and NOT a network sandbox"
        );
        assert!(status
            .isolation_claim()
            .contains("NOT a filesystem sandbox"));
        assert!(status.isolation_claim().contains("NOT a network sandbox"));
    }

    #[test]
    fn d1_containment_components_are_truthfully_reported() {
        let status = sandbox_status("run-components", SandboxProfile::d1());
        assert_eq!(
            status.containment.job_object_tree_kill,
            cfg!(windows),
            "only Windows has the kill-on-close Job Object backend"
        );
        assert!(status.containment.allowlist);
        assert!(status.containment.environment_scrub);
        assert!(status.containment.run_temp);
        assert_eq!(
            status.has_available_containment(),
            cfg!(windows),
            "available containment requires all four components, including the Job Object"
        );
    }

    #[test]
    fn preflight_returns_named_reasons_for_requested_isolation() {
        let (temp, jail) = jail();
        let cases = [
            (
                SandboxProfile::d1().with_filesystem_isolation(),
                REASON_FILESYSTEM_ISOLATION_UNAVAILABLE,
            ),
            (
                SandboxProfile::d1().with_network_isolation(),
                REASON_NETWORK_ISOLATION_UNAVAILABLE,
            ),
            (
                SandboxProfile::d1().requiring_spawn_assignment_race_closed(),
                REASON_SPAWN_ASSIGNMENT_RACE_OPEN,
            ),
        ];
        for (index, (profile, expected_reason)) in cases.into_iter().enumerate() {
            let run_id = format!("run-unavailable-{index}");
            let error = sandbox_preflight(&jail, &run_id, profile)
                .expect_err("unavailable isolation must be refused before approval");
            assert_eq!(error.code(), E_SANDBOX_ISOLATION_UNAVAILABLE);
            assert!(
                error.reason_codes().contains(&expected_reason),
                "missing {expected_reason}: {:?}",
                error.reason_codes()
            );
            assert!(
                !temp.path().join(SANDBOX_TMP_DIR).join(&run_id).exists(),
                "a refused preflight must not leave a run directory"
            );
        }
    }

    #[cfg(windows)]
    #[test]
    fn preflight_prepares_temp_and_working_directories_inside_the_workspace() {
        let (_temp, jail) = jail();
        let preflight =
            sandbox_preflight(&jail, "run-d1", SandboxProfile::d1()).expect("D1 preflight");
        assert_eq!(preflight.run_id(), "run-d1");
        let status = preflight.status();
        assert!(status.temp_dir.is_some());
        assert_eq!(status.temp_dir.as_deref(), Some(".sandbox-tmp/run-d1/tmp"));
        assert_eq!(
            status.working_dir.as_deref(),
            Some(".sandbox-tmp/run-d1/work")
        );
        let run = preflight.run();
        assert!(run.temp_dir().is_dir(), "TEMP directory exists");
        assert!(run.working_dir().is_dir(), "working directory exists");
        // `temp_dir`/`working_dir` are child-visible paths without the Windows
        // verbatim prefix; canonicalise them for the containment check.
        for directory in [run.temp_dir(), run.working_dir()] {
            let canonical = fs::canonicalize(directory).expect("canonical run directory");
            assert!(
                jail.is_inside(&canonical),
                "run directory must be inside the workspace: {directory:?}"
            );
        }
        run.cleanup().expect("cleanup");
        assert!(!run.root().exists(), "cleanup removes the run tree");
    }

    #[test]
    fn unsafe_run_ids_are_refused() {
        let (temp, jail) = jail();
        for run_id in [
            "",
            ".",
            "..",
            "../escape",
            r"..\escape",
            "a/b",
            r"a\b",
            "C:run",
            "run~1",
            ".hidden",
            "run id",
            "run\0id",
        ] {
            let error = SandboxRun::prepare(&jail, run_id, SandboxProfile::d1())
                .err()
                .unwrap_or_else(|| panic!("run id {run_id:?} must be refused"));
            assert_eq!(error.code(), E_SANDBOX_RUN_ID, "run id {run_id:?}");
        }
        assert!(
            !temp.path().join("escape").exists(),
            "a traversal run id must not create anything outside the sandbox root"
        );
    }

    #[test]
    fn run_layout_is_tmp_and_work_under_sandbox_tmp() {
        // Guard rail for the shell wiring: the TEMP/TMP directory is the run's
        // `tmp` child, not the workspace root or the operator profile.
        let (_temp, jail) = jail();
        let run = SandboxRun::prepare(&jail, "run-layout", SandboxProfile::d1()).expect("prepare");
        assert_eq!(
            run.temp_dir().file_name().and_then(|name| name.to_str()),
            Some("tmp")
        );
        assert_eq!(
            run.working_dir().file_name().and_then(|name| name.to_str()),
            Some("work")
        );
        assert_eq!(
            run.root().parent().and_then(|parent| parent.file_name()),
            Some(std::ffi::OsStr::new(SANDBOX_TMP_DIR))
        );
        let canonical = fs::canonicalize(run.temp_dir()).expect("canonical TEMP");
        assert!(
            jail.is_inside(&canonical),
            "TEMP is under the workspace: {:?}",
            run.temp_dir()
        );
        run.cleanup().expect("cleanup");
    }

    #[test]
    fn spawn_assignment_blocker_states_the_exact_windows_api_gap() {
        assert!(SPAWN_ASSIGNMENT_RACE_BLOCKER.contains("CREATE_SUSPENDED"));
        assert!(SPAWN_ASSIGNMENT_RACE_BLOCKER.contains("ResumeThread"));
        assert!(SPAWN_ASSIGNMENT_RACE_BLOCKER.contains("win32job"));
        assert!(SPAWN_ASSIGNMENT_RACE_BLOCKER.contains("forbid(unsafe_code)"));
    }
}
