//! Public-surface proof for contained shell execution (Phase D1).
//!
//! Covers the per-run temp override, the dropped operator profile, the cwd
//! escape refusal, and the detached-grandchild process-tree kill.

#![cfg(windows)]

use std::path::Path;
use std::time::Duration;

use hiverouter_tools::jail::WorkspaceJail;
use hiverouter_tools::shell::sandbox::{sandbox_preflight, SandboxProfile};
use hiverouter_tools::shell::{execute_in_sandbox, ShellPolicy, ShellRequest};

fn jail() -> (tempfile::TempDir, WorkspaceJail) {
    let temp = tempfile::tempdir().expect("tempdir");
    let jail = WorkspaceJail::open(temp.path()).expect("jail");
    (temp, jail)
}

fn policy() -> ShellPolicy {
    ShellPolicy::new(["git.exe".to_owned()])
}

/// A native (non-MSYS) environment probe. Git for Windows' MSYS runtime
/// rewrites `TEMP`/`TMP` and synthesises `HOME` for its own shell, so it cannot
/// prove what the direct child inherited; native Python reads the environment
/// exactly as the runner passed it.
fn probe_policy() -> ShellPolicy {
    ShellPolicy::new(["python.exe".to_owned()])
}

fn preflight(jail: &WorkspaceJail, run_id: &str) -> hiverouter_tools::shell::SandboxPreflight {
    sandbox_preflight(jail, run_id, SandboxProfile::d1())
        .unwrap_or_else(|error| panic!("preflight {run_id:?}: {error}"))
}

/// Normalises a Windows path (including a `\\?\` verbatim prefix) to the POSIX
/// spelling Python/Git for Windows print, so expected and observed compare
/// equal.
fn posix(path: &Path) -> String {
    let text = path.to_string_lossy().replace('\\', "/");
    let text = text.strip_prefix("//?/").unwrap_or(&text).to_owned();
    let bytes = text.as_bytes();
    if bytes.len() > 2 && bytes[1] == b':' {
        format!("/{}{}", &text[..1], &text[2..]).to_lowercase()
    } else {
        text.to_lowercase()
    }
}

fn native_env_probe() -> Vec<String> {
    // One argv argument, no shell metacharacters the shell layer refuses
    // (`&`, `|`, `>`, newline). Python is a native Windows process, so the
    // printed values are exactly the environment the runner passed.
    vec![
        "-c".to_owned(),
        "import os; \
         print('TEMP=' + os.environ.get('TEMP', 'unset')); \
         print('TMP=' + os.environ.get('TMP', 'unset')); \
         print('USERPROFILE=' + os.environ.get('USERPROFILE', 'unset')); \
         print('HOME=' + os.environ.get('HOME', 'unset')); \
         print(os.getcwd())"
            .to_owned(),
    ]
}

#[tokio::test]
async fn shell_sandbox_temp_override_and_operator_profile_drop() {
    let (_temp, jail) = jail();
    let run = preflight(&jail, "run-shell-env");
    let request = ShellRequest::new("python.exe", native_env_probe());
    let output = execute_in_sandbox(&jail, &probe_policy(), &request, run.run(), None)
        .await
        .expect("contained shell runs");
    assert!(
        output.success,
        "env probe must run: exit {:?}, stdout {}, stderr {}",
        output.exit_code, output.stdout, output.stderr
    );

    let temp_line = format!("TEMP={}", run.run().temp_dir().to_string_lossy());
    let tmp_line = format!("TMP={}", run.run().temp_dir().to_string_lossy());
    assert!(
        output
            .stdout
            .to_lowercase()
            .contains(&temp_line.to_lowercase()),
        "child TEMP must be the per-run tmp directory ({temp_line}): {}",
        output.stdout
    );
    assert!(
        output
            .stdout
            .to_lowercase()
            .contains(&tmp_line.to_lowercase()),
        "child TMP must be the per-run tmp directory ({tmp_line}): {}",
        output.stdout
    );
    assert!(
        output.stdout.contains("USERPROFILE=unset"),
        "USERPROFILE must be absent from the child environment: {}",
        output.stdout
    );
    assert!(
        output.stdout.contains("HOME=unset"),
        "HOME must be absent from the child environment: {}",
        output.stdout
    );

    // `execute_in_sandbox` defaults cwd to the per-run `work` directory.
    let expected_workdir = posix(run.run().working_dir());
    let observed_workdir = posix(Path::new(
        output.stdout.lines().last().expect("cwd line").trim(),
    ));
    assert_eq!(
        observed_workdir, expected_workdir,
        "default cwd must be the per-run working directory: {}",
        output.stdout
    );

    // The run directory is caller-owned and still present until cleanup; the
    // TEMP override must not have been the operator TEMP.
    assert!(run.run().temp_dir().is_dir());
    run.into_run().cleanup().expect("cleanup");
}

#[tokio::test]
async fn shell_sandbox_cwd_escape_is_refused_before_spawn() {
    let (_temp, jail) = jail();
    let run = preflight(&jail, "run-shell-cwd");
    for cwd in ["..", r"..\..", r"C:\Windows", r"\\server\share"] {
        let request = ShellRequest::new("git.exe", vec!["--version".to_owned()]).with_cwd(cwd);
        let error = execute_in_sandbox(&jail, &policy(), &request, run.run(), None)
            .await
            .expect_err(cwd);
        assert_eq!(error.code(), "E_PATH_OUTSIDE_JAIL", "cwd {cwd:?}");
    }
    run.into_run().cleanup().expect("cleanup");
}

#[tokio::test]
async fn detached_grandchild_is_reaped_when_the_run_returns() {
    // A grandchild backgrounded by a normally-exiting child is orphaned from
    // the direct child but still inside the Job Object; closing the job must
    // reap it. The marker is unique to this test.
    let marker = "424244";
    let (temp, jail) = jail();
    std::fs::write(
        temp.path().join(".hive-detach.sh"),
        format!(
            "#!/bin/sh\n\
             sleep {marker} >/dev/null 2>&1 &\n\
             exit 0\n"
        ),
    )
    .expect("write detach script");

    let run = preflight(&jail, "run-shell-detach");
    let request = ShellRequest::new(
        "git.exe",
        vec![
            "-c".to_owned(),
            "alias.hive-detach=!sh .hive-detach.sh hive-detach".to_owned(),
            "hive-detach".to_owned(),
        ],
    )
    .with_cwd(".");
    let output = execute_in_sandbox(&jail, &policy(), &request, run.run(), None)
        .await
        .expect("detach probe runs");
    assert!(
        output.success,
        "detach probe must exit 0 before the job closes: exit {:?}, stderr {}",
        output.exit_code, output.stderr
    );

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
        "detached grandchild `sleep {marker}` survived: the Job Object did not reap the tree"
    );
    run.into_run().cleanup().expect("cleanup");
}
