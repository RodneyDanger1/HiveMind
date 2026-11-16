//! Public-surface proof for the Phase D1 sandbox status/preflight API.
//!
//! These tests use only the public `hiverouter_tools::shell::sandbox` surface
//! the Lead wires into the approval path.

use hiverouter_tools::jail::WorkspaceJail;
use hiverouter_tools::shell::sandbox::{
    sandbox_preflight, sandbox_status, SandboxProfile, SandboxRun, E_SANDBOX_ISOLATION_UNAVAILABLE,
    E_SANDBOX_RUN_ID, REASON_FILESYSTEM_ISOLATION_UNAVAILABLE,
    REASON_NETWORK_ISOLATION_UNAVAILABLE, REASON_SPAWN_ASSIGNMENT_RACE_OPEN, SANDBOX_TMP_DIR,
};

fn jail() -> (tempfile::TempDir, WorkspaceJail) {
    let temp = tempfile::tempdir().expect("tempdir");
    let jail = WorkspaceJail::open(temp.path()).expect("jail");
    (temp, jail)
}

#[test]
fn sandbox_status_never_claims_filesystem_or_network_isolation() {
    let status = sandbox_status("run-integration-status", SandboxProfile::d1());
    assert!(!status.filesystem_isolated);
    assert!(!status.network_isolated);
    assert!(!status.spawn_assignment_race_closed);
    assert!(status
        .reason(REASON_FILESYSTEM_ISOLATION_UNAVAILABLE)
        .is_some());
    assert!(status
        .reason(REASON_NETWORK_ISOLATION_UNAVAILABLE)
        .is_some());
    assert!(status.reason(REASON_SPAWN_ASSIGNMENT_RACE_OPEN).is_some());
    assert!(status
        .isolation_claim()
        .contains("NOT a filesystem sandbox"));
    assert!(status.isolation_claim().contains("NOT a network sandbox"));
    let json = serde_json::to_string(&status).expect("status serializes");
    assert!(
        json.contains("\"filesystem_isolated\":false"),
        "serialized status must not claim filesystem isolation: {json}"
    );
    assert!(
        json.contains("\"network_isolated\":false"),
        "serialized status must not claim network isolation: {json}"
    );
    assert!(
        !json.contains("\"filesystem_isolated\":true")
            && !json.contains("\"network_isolated\":true"),
        "no sandbox claim may be serialized: {json}"
    );
}

#[test]
fn preflight_refuses_requested_isolation_with_named_reasons() {
    let (temp, jail) = jail();
    let error = sandbox_preflight(
        &jail,
        "run-integration-isolation",
        SandboxProfile::d1().with_filesystem_isolation(),
    )
    .expect_err("filesystem isolation must be refused before approval");
    assert_eq!(error.code(), E_SANDBOX_ISOLATION_UNAVAILABLE);
    assert!(
        error
            .reason_codes()
            .contains(&REASON_FILESYSTEM_ISOLATION_UNAVAILABLE),
        "reasons: {:?}",
        error.reason_codes()
    );
    assert!(
        !temp
            .path()
            .join(SANDBOX_TMP_DIR)
            .join("run-integration-isolation")
            .exists(),
        "refused preflight must not leave a run directory"
    );

    let error = sandbox_preflight(
        &jail,
        "run-integration-network",
        SandboxProfile::d1().with_network_isolation(),
    )
    .expect_err("network isolation must be refused before approval");
    assert_eq!(error.code(), E_SANDBOX_ISOLATION_UNAVAILABLE);
    assert!(error
        .reason_codes()
        .contains(&REASON_NETWORK_ISOLATION_UNAVAILABLE));
}

#[cfg(windows)]
#[test]
fn preflight_prepares_and_cleans_a_per_run_directory() {
    let (_temp, jail) = jail();
    let preflight = sandbox_preflight(&jail, "run-integration-prepare", SandboxProfile::d1())
        .expect("D1 preflight");
    let status = preflight.status();
    assert_eq!(status.run_id, "run-integration-prepare");
    assert_eq!(
        status.temp_dir.as_deref(),
        Some(".sandbox-tmp/run-integration-prepare/tmp")
    );
    assert_eq!(
        status.working_dir.as_deref(),
        Some(".sandbox-tmp/run-integration-prepare/work")
    );
    let run = preflight.into_run();
    assert!(run.temp_dir().is_dir());
    assert!(run.working_dir().is_dir());
    let root = run.root().to_path_buf();
    run.cleanup().expect("cleanup");
    assert!(!root.exists(), "cleanup removes the per-run tree");
}

#[test]
fn unsafe_run_ids_are_refused_at_the_public_preflight() {
    let (temp, jail) = jail();
    for run_id in [
        "../escape",
        r"..\escape",
        "a/b",
        r"a\b",
        "C:run",
        "run~1",
        ".",
    ] {
        let error = sandbox_preflight(&jail, run_id, SandboxProfile::d1())
            .err()
            .unwrap_or_else(|| panic!("run id {run_id:?} must be refused"));
        assert_eq!(error.code(), E_SANDBOX_RUN_ID, "run id {run_id:?}");
    }
    assert!(!temp.path().join("escape").exists());
    assert!(SandboxRun::prepare(&jail, "safe-run_1.2", SandboxProfile::d1()).is_ok());
}
