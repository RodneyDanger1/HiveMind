//! Child-process environment isolation.
//!
//! Tool and CLI children are given an explicit allowlist instead of the parent
//! process environment. This prevents credentials (including
//! `DEEPSEEK_API_KEY`) and unrelated operator state from leaking into a child
//! while preserving the variables Windows and the toolchain need to start.
//!
//! A **contained shell run** additionally receives the Phase D1 environment
//! through [`sanitize_tokio_with_sandbox`] / [`sanitize_std_with_sandbox`]: a
//! per-run `TEMP`/`TMP`, and `USERPROFILE`/`HOME` are removed so the child
//! cannot inherit the operator's profile directory, config discovery paths, or
//! home-relative state. The plain [`sanitize_std`] / [`sanitize_tokio`] entry
//! points keep those profile variables for the separate CLI/daemon/job callers
//! that are not part of the shell sandbox (removing them there would break
//! operator scripts and job programs that legitimately resolve profile state).

use std::path::Path;
use std::process::Command as StdCommand;

/// Variables a child may inherit. Everything else is removed.
///
/// This is the base allowlist. The contained-shell wrappers
/// ([`sanitize_std_with_sandbox`] / [`sanitize_tokio_with_sandbox`]) remove
/// `USERPROFILE`/`HOME` after applying it; non-sandbox CLI/daemon/job callers
/// keep those two variables. Secret-shaped names are never added here; see the
/// unit tests for canaries.
pub const ALLOWED_ENV: &[&str] = &[
    "PATH",
    "PATHEXT",
    "SystemRoot",
    "WINDIR",
    "TEMP",
    "TMP",
    "USERPROFILE",
    "HOME",
    "COMSPEC",
    "OS",
    "PROCESSOR_ARCHITECTURE",
    "NUMBER_OF_PROCESSORS",
];

/// Applies the allowlist to a standard-library command.
pub fn sanitize_std(command: &mut StdCommand) {
    command.env_clear();
    for key in ALLOWED_ENV {
        if let Some(value) = std::env::var_os(key) {
            command.env(key, value);
        }
    }
}

/// Applies the allowlist to a Tokio command.
pub fn sanitize_tokio(command: &mut tokio::process::Command) {
    command.env_clear();
    for key in ALLOWED_ENV {
        if let Some(value) = std::env::var_os(key) {
            command.env(key, value);
        }
    }
}

/// Applies the allowlist plus the per-run sandbox environment to a standard
/// library command.
///
/// `TEMP` and `TMP` are replaced with `temp_dir`; `USERPROFILE` and `HOME` are
/// removed explicitly, so a sandboxed child never inherits them even though the
/// base allowlist retains them for the non-shell CLI/daemon/job callers.
pub fn sanitize_std_with_sandbox(command: &mut StdCommand, temp_dir: &Path) {
    sanitize_std(command);
    apply_sandbox_overrides(command, temp_dir);
}

/// Applies the allowlist plus the per-run sandbox environment to a Tokio
/// command.
///
/// This is the environment every contained shell child receives: a scrubbed
/// allowlist, `TEMP`/`TMP` pointed at the run's `tmp` directory, and no
/// `USERPROFILE`/`HOME`.
pub fn sanitize_tokio_with_sandbox(command: &mut tokio::process::Command, temp_dir: &Path) {
    sanitize_tokio(command);
    apply_sandbox_overrides(command, temp_dir);
}

fn apply_sandbox_overrides<C>(command: &mut C, temp_dir: &Path)
where
    C: SandboxEnvCommand,
{
    command.set_env("TEMP", temp_dir);
    command.set_env("TMP", temp_dir);
    command.remove_env("USERPROFILE");
    command.remove_env("HOME");
}

/// The small command-operations subset shared by `std::process::Command` and
/// `tokio::process::Command` (private, so the two public wrappers above stay
/// concrete).
trait SandboxEnvCommand {
    fn set_env(&mut self, key: &str, value: &Path);
    fn remove_env(&mut self, key: &str);
}

impl SandboxEnvCommand for StdCommand {
    fn set_env(&mut self, key: &str, value: &Path) {
        self.env(key, value);
    }

    fn remove_env(&mut self, key: &str) {
        self.env_remove(key);
    }
}

impl SandboxEnvCommand for tokio::process::Command {
    fn set_env(&mut self, key: &str, value: &Path) {
        self.env(key, value);
    }

    fn remove_env(&mut self, key: &str) {
        self.env_remove(key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(windows)]
    fn probe(command: &mut StdCommand, variable: &str) -> String {
        let output = command
            .args(["/C", &format!("echo %{variable}%")])
            .output()
            .expect("cmd runs");
        String::from_utf8_lossy(&output.stdout).trim().to_owned()
    }

    #[cfg(windows)]
    #[test]
    fn arbitrary_parent_environment_is_not_inherited() {
        let previous = std::env::var_os("HIVEROUTER_ENV_CANARY");
        std::env::set_var("HIVEROUTER_ENV_CANARY", "must-not-reach-child");
        let mut command = StdCommand::new("cmd.exe");
        sanitize_std(&mut command);
        let output = command
            .args(["/C", "echo", "%HIVEROUTER_ENV_CANARY%"])
            .output()
            .expect("cmd runs");
        match previous {
            Some(value) => std::env::set_var("HIVEROUTER_ENV_CANARY", value),
            None => std::env::remove_var("HIVEROUTER_ENV_CANARY"),
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            !stdout.contains("must-not-reach-child"),
            "child inherited a non-allowlisted variable: {stdout}"
        );
    }

    #[cfg(windows)]
    #[test]
    fn sandbox_env_overrides_temp_and_drops_the_operator_profile() {
        let temp = tempfile::tempdir().expect("tempdir");
        let run_temp = temp.path().join("tmp");
        std::fs::create_dir_all(&run_temp).expect("run temp");

        let mut command = StdCommand::new("cmd.exe");
        sanitize_std_with_sandbox(&mut command, &run_temp);
        let temp_value = probe(&mut command, "TEMP");
        let mut command = StdCommand::new("cmd.exe");
        sanitize_std_with_sandbox(&mut command, &run_temp);
        let tmp_value = probe(&mut command, "TMP");
        let expected = run_temp.to_string_lossy().to_lowercase();
        assert_eq!(
            temp_value.to_lowercase(),
            expected,
            "TEMP must be the per-run directory, not the operator TEMP"
        );
        assert_eq!(
            tmp_value.to_lowercase(),
            expected,
            "TMP must be the per-run directory, not the operator TMP"
        );

        let mut command = StdCommand::new("cmd.exe");
        sanitize_std_with_sandbox(&mut command, &run_temp);
        let userprofile = probe(&mut command, "USERPROFILE");
        assert!(
            userprofile == "%USERPROFILE%" || userprofile.is_empty(),
            "USERPROFILE must be absent in the child environment, saw {userprofile:?}"
        );
        let mut command = StdCommand::new("cmd.exe");
        sanitize_std_with_sandbox(&mut command, &run_temp);
        let home = probe(&mut command, "HOME");
        assert!(
            home == "%HOME%" || home.is_empty(),
            "HOME must be absent in the child environment, saw {home:?}"
        );

        let mut command = StdCommand::new("cmd.exe");
        sanitize_std_with_sandbox(&mut command, &run_temp);
        let source = probe(&mut command, "SystemRoot");
        assert!(!source.is_empty(), "SystemRoot stays allowlisted");
    }

    #[test]
    fn path_is_allowlisted_and_secrets_are_not() {
        assert!(ALLOWED_ENV.contains(&"PATH"));
        assert!(!ALLOWED_ENV
            .iter()
            .any(|key| key.eq_ignore_ascii_case("DEEPSEEK_API_KEY")));
        // The base allowlist intentionally keeps USERPROFILE/HOME for the
        // non-shell daemon/job callers; the sandbox wrappers are what remove
        // them (proven by `sandbox_env_overrides_temp_and_drops_the_operator_profile`).
        assert!(ALLOWED_ENV
            .iter()
            .any(|key| key.eq_ignore_ascii_case("USERPROFILE")));
        assert!(ALLOWED_ENV.iter().any(|key| key.eq_ignore_ascii_case("HOME")));
    }
}
