//! Live-truth acceptance tests for the checkpoint journal (CAP-5) and the
//! checkpoint HTTP surface.
//!
//! This file is deliberately **independent** of `tests/checkpoint_jobs.rs`,
//! `tests/checkpoint_hook.rs`, `tests/capability_checkpoint_restore.rs` and
//! `tests/session_policy_truth.rs`: it owns its fixtures and drives the real
//! SQLite database, the real workspace jail, the real `ToolsBridge` approval
//! path and the real `build_router`/`AppState` HTTP harness. Nothing here is a
//! mock, and the live application database is never opened for writing.
//!
//! What is proven:
//!
//! 1. capture is scoped to `file_write` / `apply_patch`: a granted mutating
//!    `git_stage` and a granted `shell_exec` execute without a journal row;
//! 2. `file_write` and `apply_patch` restore the byte-exact pre-image and the
//!    receipt/DB row/blob agree;
//! 3. a second restore is drift (undo is not redo) and never rewrites a file;
//! 4. unknown checkpoint id, path mismatch and cross-run restores are refused
//!    with the exact stable codes and never consume an approval;
//! 5. an absent run is `E_RUN_NOT_FOUND` at the journal layer and 404 on the
//!    HTTP routes; an *absent entry* of an existing run is a different refusal
//!    (captured below because it currently reuses `E_RUN_NOT_FOUND` on the
//!    detail route and 409 `E_INTERNAL_ERROR` on the restore route);
//! 6. the agent-facing `checkpoint_restore` tool is reachable only through the
//!    approval bridge and returns the full `RestoreReceipt` JSON;
//! 7. `after: null` (unknown / incomplete) is distinct from
//!    `after: {"kind":"absent"}` (the mutation deleted the file), and both the
//!    DB row and the JSON projection are checked;
//! 8. refused/denied grants leave no checkpoint row and no orphan pre-image
//!    blob.

use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;

use hiverouter_contract::command::ApprovalResolve;
use hiverouter_contract::{AgentState, ApprovalId, Refusal, RunId, TaskId, Timestamp};
use hiverouter_core::checkpoint::{
    CheckpointError, CheckpointJournal, FileState, RestoreReceipt, CHECKPOINT_MIGRATION_NAME,
};
use hiverouter_core::config::Config;
use hiverouter_core::db::Database;
use hiverouter_core::http::{self, AppState};
use hiverouter_core::run_checks;
use hiverouter_core::store::RunStore;
use hiverouter_core::ToolsBridge;
use hiverouter_tools::executor::ExecutedTool;
use hiverouter_tools::sha256_hex;
use hiverouter_tools::shell::ShellPolicy;
use hiverouter_tools::tool::{AgentTier, ToolId, ToolRegistry};
use rusqlite::OptionalExtension;
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

// ---------------------------------------------------------------------------
// core fixture: real temp DB + real workspace + real journal + real bridge
// ---------------------------------------------------------------------------

struct CoreFixture {
    _temp: tempfile::TempDir,
    db: Arc<Database>,
    journal: CheckpointJournal,
    bridge: ToolsBridge,
    data_dir: PathBuf,
    root: PathBuf,
}

impl CoreFixture {
    fn new() -> Self {
        let temp = tempfile::tempdir().expect("temp dir");
        let root = temp.path().join("workspace");
        fs::create_dir_all(&root).expect("workspace dir");
        let data_dir = temp.path().join("data");
        let db = Arc::new(Database::open(&temp.path().join("hive.db")).expect("temp database"));
        db.migrate().expect("schema migrations");
        ToolsBridge::migrate(&db).expect("approvals migration");
        assert!(
            db.applied_migrations()
                .expect("applied migrations")
                .iter()
                .any(|(_, name, _)| name == CHECKPOINT_MIGRATION_NAME),
            "the checkpoint journal migration must be part of db::MIGRATIONS"
        );
        let journal = CheckpointJournal::open(db.clone(), &root).expect("journal");
        let bridge = ToolsBridge::new_with_data_dir(&root, &data_dir, db.clone())
            .with_checkpoint_journal(journal.clone());
        Self {
            _temp: temp,
            db,
            journal,
            bridge,
            data_dir,
            root,
        }
    }

    fn insert_run(&self) -> RunId {
        let run_id = RunId::new();
        self.db
            .with_connection(|connection| {
                connection.execute(
                    "INSERT INTO runs(id, project_id, prompt, status, created_at, updated_at)
                     VALUES (?1, 'project', 'checkpoint live truth', 'running', ?2, ?2)",
                    rusqlite::params![run_id.to_string(), Timestamp::now().as_str()],
                )?;
                Ok(())
            })
            .expect("insert run row");
        run_id
    }

    fn write(&self, relative: &str, bytes: &[u8]) {
        let path = self.root.join(relative);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("parent dir");
        }
        fs::write(path, bytes).expect("write fixture file");
    }

    /// Raw `checkpoints` rows, newest sequence first (what an operator audits).
    fn rows(&self, run: RunId) -> Vec<CheckpointRow> {
        self.db
            .with_connection(|connection| {
                let mut statement = connection.prepare(
                    "SELECT checkpoint_id, run_id, seq, path, existed_before,
                            before_sha256, before_size, after_sha256, after_size,
                            completed, created_at, completed_at, restored_at
                     FROM checkpoints WHERE run_id = ?1 ORDER BY seq DESC",
                )?;
                let mapped = statement.query_map(rusqlite::params![run.to_string()], |row| {
                    Ok(CheckpointRow {
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
                })?;
                let mut rows = Vec::new();
                for row in mapped {
                    rows.push(row?);
                }
                Ok(rows)
            })
            .expect("query checkpoint rows")
    }

    fn blob(&self, sha256: &str) -> Option<Vec<u8>> {
        self.db
            .with_connection(|connection| {
                connection
                    .query_row(
                        "SELECT bytes FROM checkpoint_blobs WHERE sha256 = ?1",
                        rusqlite::params![sha256],
                        |row| row.get(0),
                    )
                    .optional()
            })
            .expect("query blob")
    }

    fn blob_count(&self) -> i64 {
        self.db
            .with_connection(|connection| {
                connection.query_row("SELECT COUNT(*) FROM checkpoint_blobs", [], |row| {
                    row.get(0)
                })
            })
            .expect("count blobs")
    }

    fn checkpoint_count(&self) -> i64 {
        self.db
            .with_connection(|connection| {
                connection.query_row("SELECT COUNT(*) FROM checkpoints", [], |row| row.get(0))
            })
            .expect("count checkpoints")
    }

    fn approval_row(&self, approval: &ApprovalId) -> Option<(String, Option<String>)> {
        self.db
            .with_connection(|connection| {
                connection
                    .query_row(
                        "SELECT decision, executed_at FROM approvals WHERE approval_id = ?1",
                        rusqlite::params![approval.to_string()],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .optional()
            })
            .expect("query approval row")
    }
}

#[derive(Debug, Clone)]
struct CheckpointRow {
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

fn present(sha256: String, size: u64) -> FileState {
    FileState::Present { sha256, size }
}

// ---------------------------------------------------------------------------
// approval helpers (the real bridge path)
// ---------------------------------------------------------------------------

fn request(
    fixture: &CoreFixture,
    run: RunId,
    tool_call_id: &str,
    tool: &str,
    args: &Value,
) -> (ApprovalId, String) {
    let event = fixture
        .bridge
        .request_model_tool_approval(run, TaskId::new(), tool_call_id, tool, args)
        .expect("approval request");
    (event.approval_id, event.nonce.expect("request nonce"))
}

fn decide(
    fixture: &CoreFixture,
    approval: ApprovalId,
    nonce: &str,
    decision: &str,
) -> Result<(), Refusal> {
    let payload = ApprovalResolve {
        approval_id: approval,
        nonce: nonce.to_owned(),
        decision: decision.to_owned(),
        reason: None,
    };
    fixture
        .bridge
        .resolve_approval_ledgered(
            &payload,
            &ToolsBridge::approval_ledger_dir(&fixture.data_dir),
        )
        .map(|_| ())
}

fn approve(
    fixture: &CoreFixture,
    run: RunId,
    tool_call_id: &str,
    tool: &str,
    args: &Value,
) -> ApprovalId {
    let (approval, nonce) = request(fixture, run, tool_call_id, tool, args);
    decide(fixture, approval, &nonce, "approve").expect("approval decision");
    approval
}

#[allow(clippy::too_many_arguments)]
async fn execute_bound(
    fixture: &CoreFixture,
    policy: &ShellPolicy,
    tier: AgentTier,
    run: RunId,
    approval: ApprovalId,
    tool_call_id: &str,
    tool: &str,
    args: &Value,
) -> Result<ExecutedTool, Refusal> {
    fixture
        .bridge
        .execute_approved_bound(
            policy,
            &ToolRegistry::standard(),
            tier,
            run,
            &approval.to_string(),
            tool_call_id,
            tool,
            args,
            None,
        )
        .await
}

fn now_unix() -> u64 {
    Timestamp::now().unix_secs().unwrap_or(0)
}

fn is_ok(executed: &ExecutedTool) -> bool {
    matches!(executed, ExecutedTool::Ok { .. })
}

fn refusal_code(executed: &ExecutedTool) -> (&str, &str) {
    match executed {
        ExecutedTool::Refused { code, detail } => (code.as_str(), detail.as_str()),
        ExecutedTool::Ok { .. } => panic!("expected a refusal, got success"),
    }
}

// ---------------------------------------------------------------------------
// 1. capture scope: only file_write / apply_patch get a pre-image
// ---------------------------------------------------------------------------

#[tokio::test]
async fn capture_fires_only_for_file_write_and_apply_patch() {
    let fixture = CoreFixture::new();
    let run = fixture.insert_run();
    fixture.write("seed.txt", b"seed\n");

    // A real repository so the granted mutating git tool can execute.
    let init = Command::new("git")
        .args(["init", "-q"])
        .current_dir(&fixture.root)
        .output()
        .expect("git init runs");
    assert!(
        init.status.success(),
        "git init failed: {}",
        String::from_utf8_lossy(&init.stderr)
    );

    // `git_stage` is a granted Mutate tool that does not write workspace file
    // bytes. It must execute without creating a checkpoint.
    let stage_args = json!({"paths": ["seed.txt"], "all": false});
    let stage_approval = approve(&fixture, run, "call-stage", "git_stage", &stage_args);
    let staged = execute_bound(
        &fixture,
        &ShellPolicy::new(Vec::new()),
        AgentTier::WorkerAgent,
        run,
        stage_approval,
        "call-stage",
        "git_stage",
        &stage_args,
    )
    .await
    .expect("granted git_stage executes");
    assert!(is_ok(&staged), "git_stage must succeed: {staged:?}");
    assert!(
        fixture.rows(run).is_empty(),
        "a grant for a non-file tool must not create a checkpoint"
    );

    // `file_write` is journalled exactly once.
    let write_args = json!({"path": "notes/one.txt", "content": "beta"});
    let write_approval = approve(&fixture, run, "call-write", "file_write", &write_args);
    let written = execute_bound(
        &fixture,
        &ShellPolicy::new(Vec::new()),
        AgentTier::WorkerAgent,
        run,
        write_approval,
        "call-write",
        "file_write",
        &write_args,
    )
    .await
    .expect("granted file_write executes");
    assert!(is_ok(&written), "file_write must succeed: {written:?}");

    // `apply_patch` is journalled exactly once.
    fixture.write("patch.txt", b"old\n");
    let patch_args = json!({
        "path": "patch.txt",
        "patch": "--- a/patch.txt\n+++ b/patch.txt\n@@ -1,1 +1,1 @@\n-old\n+new\n",
    });
    let patch_approval = approve(&fixture, run, "call-patch", "apply_patch", &patch_args);
    let patched = execute_bound(
        &fixture,
        &ShellPolicy::new(Vec::new()),
        AgentTier::WorkerAgent,
        run,
        patch_approval,
        "call-patch",
        "apply_patch",
        &patch_args,
    )
    .await
    .expect("granted apply_patch executes");
    assert!(is_ok(&patched), "apply_patch must succeed: {patched:?}");

    // `shell_exec` is approval-gated arbitrary code execution; it is *not*
    // journalled. A shell mutation therefore cannot be undone through CAP-5.
    let shell_args = json!({"program": "git.exe", "args": ["--version"]});
    let shell_approval = approve(&fixture, run, "call-shell", "shell_exec", &shell_args);
    let shelled = execute_bound(
        &fixture,
        &ShellPolicy::conservative_default(),
        AgentTier::WorkerAgent,
        run,
        shell_approval,
        "call-shell",
        "shell_exec",
        &shell_args,
    )
    .await
    .expect("granted shell_exec executes");
    assert!(is_ok(&shelled), "shell_exec must succeed: {shelled:?}");

    let rows = fixture.rows(run);
    assert_eq!(
        rows.len(),
        2,
        "only file_write and apply_patch are journalled: {rows:?}"
    );
    let mut paths: Vec<&str> = rows.iter().map(|row| row.path.as_str()).collect();
    paths.sort_unstable();
    assert_eq!(paths, vec!["notes/one.txt", "patch.txt"]);
    for row in &rows {
        assert_eq!(
            row.completed, 1,
            "a committed mutation is completed: {row:?}"
        );
        assert!(
            row.completed_at.is_some(),
            "a completed row carries completed_at: {row:?}"
        );
        assert!(row.restored_at.is_none(), "nothing was restored yet");
    }
}

// ---------------------------------------------------------------------------
// 2. file_write: byte-exact restore + receipt + DB/blob truth + second drift
// ---------------------------------------------------------------------------

#[tokio::test]
async fn file_write_restore_is_byte_exact_and_receipt_db_blob_agree() {
    let fixture = CoreFixture::new();
    let run = fixture.insert_run();
    let root = fixture.journal.jail().root().to_path_buf();
    fixture.write("notes/one.txt", b"alpha");

    let args = json!({"path": "notes/one.txt", "content": "beta"});
    let approval = approve(&fixture, run, "call-write", "file_write", &args);
    execute_bound(
        &fixture,
        &ShellPolicy::new(Vec::new()),
        AgentTier::WorkerAgent,
        run,
        approval,
        "call-write",
        "file_write",
        &args,
    )
    .await
    .expect("write executes");
    assert_eq!(
        fs::read(root.join("notes/one.txt")).expect("written"),
        b"beta"
    );

    let rows = fixture.rows(run);
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(row.run_id, run.to_string());
    assert!(
        !row.checkpoint_id.is_empty(),
        "the row carries the uuid the undo tool addresses"
    );
    assert_eq!(row.path, "notes/one.txt");
    assert_eq!(row.seq, 1);
    assert_eq!(row.existed_before, 1);
    let alpha_sha = sha256_hex(b"alpha");
    let beta_sha = sha256_hex(b"beta");
    assert_eq!(row.before_sha256.as_deref(), Some(alpha_sha.as_str()));
    assert_eq!(row.before_size, Some(5));
    assert_eq!(row.after_sha256.as_deref(), Some(beta_sha.as_str()));
    assert_eq!(row.after_size, Some(4));
    assert_eq!(row.completed, 1);
    assert!(row.completed_at.is_some());
    assert!(row.restored_at.is_none());
    assert!(
        Timestamp::parse(&row.created_at).is_ok(),
        "created_at must be a canonical timestamp: {:?}",
        row.created_at
    );
    assert_eq!(
        fixture.blob(&sha256_hex(b"alpha")).expect("pre-image blob"),
        b"alpha",
        "the content-addressed blob must hold the exact pre-image bytes"
    );

    let entry = fixture
        .journal
        .get(run, 1)
        .expect("get entry")
        .expect("entry exists");
    let receipt = fixture
        .journal
        .restore_by_id(&entry.checkpoint_id, "notes/one.txt")
        .expect("restore by id");

    assert_eq!(receipt.checkpoint_id, entry.checkpoint_id);
    assert_eq!(receipt.run_id, run);
    assert_eq!(receipt.seq, 1);
    assert_eq!(receipt.path, "notes/one.txt");
    assert_eq!(
        receipt.observed_after,
        present(sha256_hex(b"beta"), 4),
        "the receipt must name the after-state that was actually present"
    );
    assert_eq!(
        receipt.restored_to,
        present(sha256_hex(b"alpha"), 5),
        "the receipt must name the written pre-image"
    );
    assert_eq!(receipt.bytes_written, 5);
    assert_eq!(
        fs::read(root.join("notes/one.txt")).expect("restored"),
        b"alpha",
        "restore must be byte-exact"
    );

    let after = fixture.rows(run);
    assert!(after[0].restored_at.is_some(), "restore must be recorded");
    assert_eq!(
        after[0].after_sha256.as_deref(),
        row.after_sha256.as_deref(),
        "a restore must not rewrite the recorded after-image"
    );
    assert_eq!(
        after[0].before_sha256.as_deref(),
        row.before_sha256.as_deref(),
        "a restore must not rewrite the recorded pre-image"
    );

    // Undo is not redo: the file now matches the pre-image, not the recorded
    // after-state, so a second restore is drift and must not overwrite.
    let second = fixture
        .journal
        .restore_by_id(&entry.checkpoint_id, "notes/one.txt")
        .expect_err("second restore must be refused");
    assert!(
        matches!(second, CheckpointError::Drift { .. }),
        "expected drift, got {second:?}"
    );
    assert_eq!(
        second.refusal(),
        Refusal::InternalError,
        "drift currently surfaces as E_INTERNAL_ERROR (no dedicated code yet)"
    );
    assert_eq!(
        fs::read(root.join("notes/one.txt")).expect("still alpha"),
        b"alpha"
    );
    assert_eq!(
        fixture.rows(run)[0].restored_at.as_deref(),
        after[0].restored_at.as_deref(),
        "a refused restore must not update restored_at"
    );
}

// ---------------------------------------------------------------------------
// 3. apply_patch: the restored pre-image is the patch base, not the patch text
// ---------------------------------------------------------------------------

#[tokio::test]
async fn apply_patch_restore_returns_the_patch_base_bytes() {
    let fixture = CoreFixture::new();
    let run = fixture.insert_run();
    let root = fixture.journal.jail().root().to_path_buf();
    fixture.write("patch.txt", b"old\n");

    let args = json!({
        "path": "patch.txt",
        "patch": "--- a/patch.txt\n+++ b/patch.txt\n@@ -1,1 +1,1 @@\n-old\n+new\n",
    });
    let approval = approve(&fixture, run, "call-patch", "apply_patch", &args);
    let executed = execute_bound(
        &fixture,
        &ShellPolicy::new(Vec::new()),
        AgentTier::WorkerAgent,
        run,
        approval,
        "call-patch",
        "apply_patch",
        &args,
    )
    .await
    .expect("patch executes");
    let ExecutedTool::Ok { text } = executed else {
        panic!("an applied patch must execute: {executed:?}");
    };
    assert!(text.contains("Applied"), "{text}");
    assert_eq!(fs::read(root.join("patch.txt")).expect("patched"), b"new\n");

    let entries = fixture.journal.entries_for_run(run).expect("entries");
    assert_eq!(entries.len(), 1);
    let entry = &entries[0];
    assert_eq!(
        entry.before,
        present(sha256_hex(b"old\n"), 4),
        "the journal must record the pre-patch base"
    );
    assert_eq!(
        entry.after,
        Some(present(sha256_hex(b"new\n"), 4)),
        "the journal must record the applied result"
    );

    let receipt = fixture
        .journal
        .restore_by_id(&entry.checkpoint_id, "patch.txt")
        .expect("restore patch");
    assert_eq!(receipt.restored_to, present(sha256_hex(b"old\n"), 4));
    assert_eq!(
        fs::read(root.join("patch.txt")).expect("restored"),
        b"old\n"
    );
}

// ---------------------------------------------------------------------------
// 4. null-vs-absent: unknown after-state is NOT the absent after-state
// ---------------------------------------------------------------------------

#[test]
fn after_null_means_unknown_and_kind_absent_means_deleted() {
    let fixture = CoreFixture::new();
    let run = fixture.insert_run();
    let root = fixture.journal.jail().root().to_path_buf();

    // A mutation that deletes a file records `after: {kind: "absent"}`.
    fixture.write("deleted.txt", b"bye");
    fixture
        .journal
        .journal_before(run, 1, "deleted.txt")
        .expect("journal deleted file");
    fs::remove_file(root.join("deleted.txt")).expect("delete fixture file");
    let completed = fixture
        .journal
        .record_after(run, 1)
        .expect("record deletion");
    assert_eq!(
        completed.after,
        Some(FileState::Absent),
        "a deleted file has a known absent after-state, not an unknown one"
    );
    let json = serde_json::to_value(&completed).expect("serialize entry");
    assert_eq!(
        json["after"]["kind"], "absent",
        "known absence must serialize as an object: {json}"
    );

    // An entry whose mutation never settled has `after: null` (unknown), and
    // no restore may guess it.
    fixture
        .journal
        .journal_before(run, 2, "never.txt")
        .expect("journal incomplete entry");
    let incomplete = fixture
        .journal
        .get(run, 2)
        .expect("get incomplete")
        .expect("incomplete entry");
    assert!(incomplete.after.is_none());
    assert!(incomplete.completed_at.is_none());
    let json = serde_json::to_value(&incomplete).expect("serialize incomplete");
    assert!(
        json["after"].is_null(),
        "an unknown after-state must serialize as null: {json}"
    );

    let error = fixture
        .journal
        .restore_by_id(&incomplete.checkpoint_id, "never.txt")
        .expect_err("incomplete restore must be refused");
    assert!(
        matches!(error, CheckpointError::Incomplete { seq: 2, .. }),
        "expected incomplete refusal, got {error:?}"
    );
    assert_eq!(error.refusal(), Refusal::InternalError);
    assert!(
        !root.join("never.txt").exists(),
        "an incomplete entry must not create a file"
    );

    let rows = fixture.rows(run);
    assert_eq!(rows.len(), 2);
    let incomplete_row = rows.iter().find(|row| row.seq == 2).expect("row 2");
    assert_eq!(incomplete_row.completed, 0);
    assert!(incomplete_row.after_sha256.is_none());
    assert!(incomplete_row.after_size.is_none());
    assert!(incomplete_row.completed_at.is_none());
}

// ---------------------------------------------------------------------------
// 5. absent run: capture refuses by name; the core listing does not run-check
// ---------------------------------------------------------------------------

#[test]
fn absent_run_refuses_capture_and_the_http_layer_supplies_the_404() {
    let fixture = CoreFixture::new();
    let absent = RunId::new();

    let error = fixture
        .journal
        .journal_before(absent, 1, "never.txt")
        .expect_err("a checkpoint cannot be bound to a run that does not exist");
    assert!(
        matches!(error, CheckpointError::RunNotFound(_)),
        "got {error:?}"
    );
    assert_eq!(error.code(), Refusal::RunNotFound.code());

    // The core read APIs are not run-checked: an absent run is indistinguishable
    // from a real run with an empty journal. This is why `http.rs` calls
    // `require_run` before every checkpoint route (the 404 is the route's job).
    assert!(
        fixture
            .journal
            .entries_for_run(absent)
            .expect("core listing")
            .is_empty(),
        "entries_for_run is a plain query; it does not prove the run exists"
    );
    let restore = fixture
        .journal
        .restore_entry(absent, 1)
        .expect_err("no checkpoint exists for an absent run");
    assert!(
        matches!(restore, CheckpointError::CheckpointNotFound { .. }),
        "got {restore:?}"
    );
    assert_eq!(restore.code(), Refusal::InternalError.code());
}

// ---------------------------------------------------------------------------
// 6. unknown / path-mismatch / cross-run refusals and grant preservation
// ---------------------------------------------------------------------------

#[tokio::test]
async fn unknown_path_mismatch_and_cross_run_are_refused_without_burning_approval() {
    let fixture = CoreFixture::new();
    let run_a = fixture.insert_run();
    let run_b = fixture.insert_run();
    let root = fixture.journal.jail().root().to_path_buf();
    fixture.write("cross.txt", b"original");

    // A real checkpoint for run A.
    let write_args = json!({"path": "cross.txt", "content": "changed"});
    let write_approval = approve(&fixture, run_a, "call-write", "file_write", &write_args);
    execute_bound(
        &fixture,
        &ShellPolicy::new(Vec::new()),
        AgentTier::WorkerAgent,
        run_a,
        write_approval,
        "call-write",
        "file_write",
        &write_args,
    )
    .await
    .expect("write executes");
    let mut entries = fixture.journal.entries_for_run(run_a).expect("entries");
    assert_eq!(entries.len(), 1);
    let entry = entries.remove(0);
    assert_eq!(
        fs::read(root.join("cross.txt")).expect("changed"),
        b"changed"
    );

    // (a) A checkpoint id that exists nowhere.
    let unknown = fixture
        .journal
        .restore_by_id("01a00000-0000-7000-8000-000000000000", "cross.txt")
        .expect_err("unknown id refused");
    assert!(
        matches!(unknown, CheckpointError::CheckpointIdNotFound(_)),
        "got {unknown:?}"
    );
    assert_eq!(unknown.code(), Refusal::InternalError.code());

    // (b) The recorded id addressed with a different path.
    fixture.write("other.txt", b"other");
    let mismatch = fixture
        .journal
        .restore_by_id(&entry.checkpoint_id, "other.txt")
        .expect_err("path mismatch refused");
    match &mismatch {
        CheckpointError::PathMismatch {
            recorded,
            requested,
            ..
        } => {
            assert_eq!(recorded, "cross.txt");
            assert_eq!(requested, "other.txt");
        }
        other => panic!("expected path mismatch, got {other:?}"),
    }
    assert_eq!(mismatch.code(), Refusal::InternalError.code());

    // (c) The agent tool called from a DIFFERENT run with run A's checkpoint.
    let cross_args = json!({"checkpoint_id": entry.checkpoint_id, "path": "cross.txt"});
    let cross_approval = approve(
        &fixture,
        run_b,
        "call-cross",
        "checkpoint_restore",
        &cross_args,
    );
    let cross = execute_bound(
        &fixture,
        &ShellPolicy::new(Vec::new()),
        AgentTier::WorkerAgent,
        run_b,
        cross_approval,
        "call-cross",
        "checkpoint_restore",
        &cross_args,
    )
    .await
    .expect("cross-run refusal is a refused tool result, not a transport error");
    let (code, detail) = refusal_code(&cross);
    assert_eq!(
        code,
        Refusal::ToolNotAllowed.code(),
        "cross-run currently reuses E_TOOL_NOT_ALLOWED (semantically wrong; see report)"
    );
    assert!(detail.contains("another run"), "{detail}");
    assert_eq!(
        fs::read(root.join("cross.txt")).expect("untouched"),
        b"changed",
        "a cross-run refusal must not write"
    );
    fixture
        .bridge
        .verify_tool_grant_bound(
            &cross_approval.to_string(),
            "call-cross",
            run_b,
            ToolId::CheckpointRestore,
            &cross_args,
            now_unix(),
        )
        .expect("a doomed cross-run restore must not consume the approval");

    // (d) Same run, wrong path: refused as E_SCHEMA_INVALID before the grant CAS.
    let wrong_args = json!({"checkpoint_id": entry.checkpoint_id, "path": "other.txt"});
    let wrong_approval = approve(
        &fixture,
        run_a,
        "call-wrong",
        "checkpoint_restore",
        &wrong_args,
    );
    let wrong = execute_bound(
        &fixture,
        &ShellPolicy::new(Vec::new()),
        AgentTier::WorkerAgent,
        run_a,
        wrong_approval,
        "call-wrong",
        "checkpoint_restore",
        &wrong_args,
    )
    .await
    .expect("path-mismatch refusal is a refused tool result");
    let (code, detail) = refusal_code(&wrong);
    assert_eq!(
        code,
        Refusal::SchemaInvalid.code(),
        "path mismatch currently reuses E_SCHEMA_INVALID (the schema is valid)"
    );
    assert!(detail.contains("path does not match"), "{detail}");
    fixture
        .bridge
        .verify_tool_grant_bound(
            &wrong_approval.to_string(),
            "call-wrong",
            run_a,
            ToolId::CheckpointRestore,
            &wrong_args,
            now_unix(),
        )
        .expect("a wrong-path restore must not consume the approval");

    // (e) Unknown id in the right run: refused as E_INTERNAL_ERROR before CAS.
    let missing_args = json!({
        "checkpoint_id": "01a00000-0000-7000-8000-000000000000",
        "path": "cross.txt",
    });
    let missing_approval = approve(
        &fixture,
        run_a,
        "call-missing",
        "checkpoint_restore",
        &missing_args,
    );
    let missing = execute_bound(
        &fixture,
        &ShellPolicy::new(Vec::new()),
        AgentTier::WorkerAgent,
        run_a,
        missing_approval,
        "call-missing",
        "checkpoint_restore",
        &missing_args,
    )
    .await
    .expect("missing-id refusal is a refused tool result");
    let (code, detail) = refusal_code(&missing);
    assert_eq!(code, Refusal::InternalError.code());
    assert!(detail.contains("not found"), "{detail}");
    fixture
        .bridge
        .verify_tool_grant_bound(
            &missing_approval.to_string(),
            "call-missing",
            run_a,
            ToolId::CheckpointRestore,
            &missing_args,
            now_unix(),
        )
        .expect("an unknown-id restore must not consume the approval");

    // The original file and journal entry are untouched by all refusals.
    assert_eq!(
        fs::read(root.join("cross.txt")).expect("still changed"),
        b"changed"
    );
    let restored = fixture.journal.get(run_a, 1).expect("get").expect("row");
    assert!(restored.restored_at.is_none());
}

// ---------------------------------------------------------------------------
// 7. agent `checkpoint_restore` is approval-gated and returns RestoreReceipt
// ---------------------------------------------------------------------------

#[tokio::test]
async fn agent_checkpoint_restore_requires_a_grant_and_returns_a_receipt() {
    let fixture = CoreFixture::new();
    let run = fixture.insert_run();
    let root = fixture.journal.jail().root().to_path_buf();
    fixture.write("grant.txt", b"original");

    let write_args = json!({"path": "grant.txt", "content": "changed"});
    let write_approval = approve(&fixture, run, "call-write", "file_write", &write_args);
    execute_bound(
        &fixture,
        &ShellPolicy::new(Vec::new()),
        AgentTier::WorkerAgent,
        run,
        write_approval,
        "call-write",
        "file_write",
        &write_args,
    )
    .await
    .expect("write executes");
    let mut entries = fixture.journal.entries_for_run(run).expect("entries");
    assert_eq!(entries.len(), 1);
    let entry = entries.remove(0);
    let restore_args = json!({"checkpoint_id": entry.checkpoint_id, "path": "grant.txt"});

    // No approval row at all: the bridge refuses before any write.
    let unapproved = execute_bound(
        &fixture,
        &ShellPolicy::new(Vec::new()),
        AgentTier::WorkerAgent,
        run,
        ApprovalId::new(),
        "call-restore",
        "checkpoint_restore",
        &restore_args,
    )
    .await
    .expect_err("an unapproved restore must be refused");
    assert_eq!(
        unapproved,
        Refusal::ApprovalNotFound,
        "a fabricated approval id must be E_APPROVAL_NOT_FOUND"
    );
    assert_eq!(
        fs::read(root.join("grant.txt")).expect("unchanged"),
        b"changed"
    );

    // A tier without the tool grant is refused before the approval CAS.
    let tool_tier = execute_bound(
        &fixture,
        &ShellPolicy::new(Vec::new()),
        AgentTier::ToolAgent,
        run,
        ApprovalId::new(),
        "call-tier",
        "checkpoint_restore",
        &restore_args,
    )
    .await
    .expect_err("ToolAgent has no checkpoint_restore grant");
    assert_eq!(tool_tier, Refusal::ToolNotAllowed);
    assert_eq!(
        fs::read(root.join("grant.txt")).expect("unchanged"),
        b"changed"
    );

    // The real approved call restores and returns the full receipt JSON.
    let approval = approve(
        &fixture,
        run,
        "call-restore",
        "checkpoint_restore",
        &restore_args,
    );
    let executed = execute_bound(
        &fixture,
        &ShellPolicy::new(Vec::new()),
        AgentTier::WorkerAgent,
        run,
        approval,
        "call-restore",
        "checkpoint_restore",
        &restore_args,
    )
    .await
    .expect("approved checkpoint_restore executes");
    let ExecutedTool::Ok { text } = executed else {
        panic!("an approved restore must succeed: {executed:?}");
    };
    let receipt: RestoreReceipt = serde_json::from_str(&text).expect("receipt JSON");
    assert_eq!(receipt.checkpoint_id, entry.checkpoint_id);
    assert_eq!(receipt.run_id, run);
    assert_eq!(receipt.seq, 1);
    assert_eq!(receipt.path, "grant.txt");
    assert_eq!(receipt.observed_after, present(sha256_hex(b"changed"), 7));
    assert_eq!(receipt.restored_to, present(sha256_hex(b"original"), 8));
    assert_eq!(receipt.bytes_written, 8);
    assert_eq!(
        fs::read(root.join("grant.txt")).expect("restored"),
        b"original"
    );

    // The grant row is consumed exactly once; a replay is named.
    let (decision, executed_at) = fixture.approval_row(&approval).expect("approval row");
    assert_eq!(decision, "allow_once");
    assert!(
        executed_at.is_some(),
        "a successful restore must consume the single-use grant"
    );
    let replay = execute_bound(
        &fixture,
        &ShellPolicy::new(Vec::new()),
        AgentTier::WorkerAgent,
        run,
        approval,
        "call-restore",
        "checkpoint_restore",
        &restore_args,
    )
    .await
    .expect_err("a replay must be refused");
    assert_eq!(replay, Refusal::ApprovalReplay);
    assert_eq!(
        fs::read(root.join("grant.txt")).expect("still original"),
        b"original"
    );
}

// ---------------------------------------------------------------------------
// 8. denied grant / rejected patch leave no row and no orphan blob
// ---------------------------------------------------------------------------

#[tokio::test]
async fn denied_and_rejected_attempts_leave_no_rows_or_orphan_blobs() {
    let fixture = CoreFixture::new();
    let run = fixture.insert_run();
    let root = fixture.journal.jail().root().to_path_buf();
    fixture.write("denied.txt", b"denied-original");

    // Denied grant: the capture happens, then the refusal voids it.
    let write_args = json!({"path": "denied.txt", "content": "denied-write"});
    let (denied, nonce) = request(&fixture, run, "call-denied", "file_write", &write_args);
    decide(&fixture, denied, &nonce, "deny").expect("deny decision");
    let error = execute_bound(
        &fixture,
        &ShellPolicy::new(Vec::new()),
        AgentTier::WorkerAgent,
        run,
        denied,
        "call-denied",
        "file_write",
        &write_args,
    )
    .await
    .expect_err("a denied grant cannot execute");
    assert_eq!(error, Refusal::ApprovalRequired);
    assert_eq!(
        fs::read(root.join("denied.txt")).expect("unchanged"),
        b"denied-original"
    );
    assert_eq!(
        fixture.checkpoint_count(),
        0,
        "a denied grant must delete the captured checkpoint row"
    );
    assert_eq!(
        fixture.blob_count(),
        0,
        "a denied grant must drop the now-orphan pre-image blob"
    );
    let (decision, executed_at) = fixture.approval_row(&denied).expect("approval row");
    assert_eq!(decision, "deny");
    assert!(executed_at.is_none(), "a denied grant is never consumed");

    // A rejected patch is a completed tool call that wrote nothing.
    fixture.write("reject.txt", b"actual\n");
    let patch_args = json!({
        "path": "reject.txt",
        "patch": "--- a/reject.txt\n+++ b/reject.txt\n@@ -1,1 +1,1 @@\n-missing\n+x\n",
    });
    let patch_approval = approve(&fixture, run, "call-reject", "apply_patch", &patch_args);
    let executed = execute_bound(
        &fixture,
        &ShellPolicy::new(Vec::new()),
        AgentTier::WorkerAgent,
        run,
        patch_approval,
        "call-reject",
        "apply_patch",
        &patch_args,
    )
    .await
    .expect("a rejected patch is still a completed tool call");
    let ExecutedTool::Ok { text } = executed else {
        panic!("expected Ok with the rejected outcome: {executed:?}");
    };
    assert!(text.contains("Rejected"), "{text}");
    assert_eq!(
        fs::read(root.join("reject.txt")).expect("byte-identical"),
        b"actual\n"
    );
    assert_eq!(
        fixture.checkpoint_count(),
        0,
        "no checkpoint may exist for a mutation that did not happen"
    );
    assert_eq!(fixture.blob_count(), 0);
}

// ---------------------------------------------------------------------------
// 9. HTTP surface: statuses, bodies, absent runs, null fields, revert
// ---------------------------------------------------------------------------

struct TestServer {
    addr: std::net::SocketAddr,
    state: Arc<AppState>,
    _shutdown: Option<oneshot::Sender<()>>,
    _handle: tokio::task::JoinHandle<()>,
}

impl TestServer {
    async fn start(temp: &tempfile::TempDir) -> Self {
        let workspace = temp.path().join("workspace");
        fs::create_dir_all(&workspace).expect("workspace dir");
        let mut config = Config::resolve(
            "127.0.0.1:0",
            temp.path().to_str().expect("utf8 temp path"),
            None,
        )
        .expect("test config");
        // The checkpoint routes jail themselves against this root; it must be
        // the fixture's own directory, never the process working directory.
        config.workspace_root = workspace;
        let database = Database::open(config.db_file()).expect("database");
        database.migrate().expect("migrations");
        let state = Arc::new(AppState::new(
            Arc::new(database),
            run_checks(),
            Arc::new(config),
        ));
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local addr");
        let (shutdown, shutdown_rx) = oneshot::channel();
        let served_state = state.clone();
        let handle = tokio::spawn(async move {
            let _ = http::serve(listener, served_state, async move {
                let _ = shutdown_rx.await;
            })
            .await;
        });
        Self {
            addr,
            state,
            _shutdown: Some(shutdown),
            _handle: handle,
        }
    }

    fn token(&self) -> &str {
        self.state.session.token()
    }

    fn store(&self) -> RunStore {
        RunStore::new(self.state.db.clone())
    }

    fn journal(&self) -> CheckpointJournal {
        CheckpointJournal::open(
            self.state.db.clone(),
            self.state.config.workspace_root.clone(),
        )
        .expect("journal")
    }
}

async fn http_json(
    addr: std::net::SocketAddr,
    method: &str,
    path: &str,
    body: Option<&Value>,
    token: Option<&str>,
) -> (u16, Value) {
    let body_text = body.map(ToString::to_string).unwrap_or_default();
    let auth = token
        .map(|token| format!("Authorization: Bearer {token}\r\n"))
        .unwrap_or_default();
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\nAccept: application/json\r\n{auth}Content-Type: application/json\r\nContent-Length: {}\r\n\r\n{body_text}",
        body_text.len()
    );
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    stream
        .write_all(request.as_bytes())
        .await
        .expect("write request");
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await.expect("read response");
    let text = String::from_utf8_lossy(&raw).into_owned();
    let status = text
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse::<u16>().ok())
        .expect("http status line");
    let payload = text.split("\r\n\r\n").nth(1).unwrap_or_default();
    let value = if payload.trim().is_empty() {
        Value::Null
    } else {
        serde_json::from_str(payload)
            .unwrap_or_else(|error| panic!("response body is not JSON ({error}): {payload:?}"))
    };
    (status, value)
}

async fn get(server: &TestServer, path: &str) -> (u16, Value) {
    http_json(server.addr, "GET", path, None, None).await
}

async fn post(server: &TestServer, path: &str, body: &Value) -> (u16, Value) {
    http_json(server.addr, "POST", path, Some(body), Some(server.token())).await
}

#[tokio::test]
async fn http_checkpoint_routes_expose_real_state_and_honest_refusals() {
    let temp = tempfile::tempdir().expect("tempdir");
    let server = TestServer::start(&temp).await;
    let run = server
        .store()
        .create_run(
            "truth-project",
            "http checkpoint truth",
            AgentState::Running,
        )
        .expect("run");
    let journal = server.journal();
    let root = journal.jail().root().to_path_buf();
    fs::create_dir_all(&root).expect("workspace root");
    fs::write(root.join("notes-a.txt"), b"alpha").expect("fixture file");
    journal
        .journal_before(run.id, 1, "notes-a.txt")
        .expect("journal before");
    fs::write(root.join("notes-a.txt"), b"beta").expect("mutate fixture file");
    journal.record_after(run.id, 1).expect("record after");

    // List and detail return the real row.
    let (status, body) = get(&server, &format!("/api/runs/{}/checkpoints", run.id)).await;
    assert_eq!(status, 200, "list: {body}");
    assert_eq!(body["run_id"], run.id.to_string());
    let entries = body["checkpoints"].as_array().expect("checkpoints");
    assert_eq!(entries.len(), 1, "list must expose the real row: {body}");
    assert_eq!(entries[0]["seq"], 1);
    assert_eq!(entries[0]["path"], "notes-a.txt");
    assert_eq!(entries[0]["before"]["kind"], "present");
    assert_eq!(entries[0]["after"]["kind"], "present");
    assert!(entries[0]["restored_at"].is_null());

    let (status, detail) = get(&server, &format!("/api/runs/{}/checkpoints/1", run.id)).await;
    assert_eq!(status, 200, "detail: {detail}");
    assert_eq!(detail["path"], "notes-a.txt");

    // Mismatch witness (pinned current truth): an absent ENTRY of an existing
    // run is reported with the RUN-not-found code on the detail route.
    let (status, response) = get(&server, &format!("/api/runs/{}/checkpoints/99", run.id)).await;
    assert_eq!(status, 404, "absent detail: {response}");
    assert_eq!(
        response["code"], "E_RUN_NOT_FOUND",
        "CURRENT TRUTH: the run exists, so this code misstates the cause: {response}"
    );

    // Mismatch witness: an absent entry on the restore route is a 409
    // E_INTERNAL_ERROR rather than a 404 not-found.
    let (status, response) = post(
        &server,
        &format!("/api/runs/{}/checkpoints/99/restore", run.id),
        &json!({}),
    )
    .await;
    assert_eq!(status, 409, "absent restore: {response}");
    assert_eq!(
        response["code"], "E_INTERNAL_ERROR",
        "CURRENT TRUTH: absent entry maps to a conflict, not a 404: {response}"
    );

    // A restore is a mutation: without the per-boot Bearer token it is refused
    // by the same middleware that guards every other mutating route.
    let restore_path = format!("/api/runs/{}/checkpoints/1/restore", run.id);
    let (status, tokenless) =
        http_json(server.addr, "POST", &restore_path, Some(&json!({})), None).await;
    assert_eq!(status, 401, "tokenless restore: {tokenless}");
    assert_eq!(tokenless["code"], "E_TOKEN_INVALID");
    assert_eq!(
        fs::read(root.join("notes-a.txt")).expect("untouched"),
        b"beta",
        "an unauthenticated restore must not write"
    );

    // The real restore succeeds and returns the run/seq-addressed
    // `RestoredFile` projection (not the agent tool's `RestoreReceipt`).
    let (status, restored) = post(
        &server,
        &format!("/api/runs/{}/checkpoints/1/restore", run.id),
        &json!({}),
    )
    .await;
    assert_eq!(status, 200, "restore: {restored}");
    assert_eq!(restored["path"], "notes-a.txt");
    assert_eq!(restored["undone_seqs"], json!([1]));
    assert_eq!(restored["restored"]["kind"], "present");
    assert_eq!(
        restored["restored"]["sha256"],
        Value::String(sha256_hex(b"alpha"))
    );
    assert_eq!(
        fs::read(root.join("notes-a.txt")).expect("restored"),
        b"alpha"
    );
    assert!(
        restored.get("checkpoint_id").is_none(),
        "CURRENT TRUTH: the HTTP receipt omits checkpoint_id: {restored}"
    );
    assert!(
        restored.get("bytes_written").is_none() && restored.get("restored_at").is_none(),
        "CURRENT TRUTH: the HTTP receipt omits receipt fields the agent tool returns: {restored}"
    );

    // Second restore: drift, 409, file untouched.
    let (status, response) = post(
        &server,
        &format!("/api/runs/{}/checkpoints/1/restore", run.id),
        &json!({}),
    )
    .await;
    assert_eq!(status, 409, "drift restore: {response}");
    assert_eq!(response["code"], "E_INTERNAL_ERROR");
    assert_eq!(
        fs::read(root.join("notes-a.txt")).expect("still alpha"),
        b"alpha"
    );

    // A real run with no entries is an empty list, not an error.
    let empty_run = server
        .store()
        .create_run("truth-project", "no checkpoints", AgentState::Queued)
        .expect("empty run");
    let (status, empty) = get(&server, &format!("/api/runs/{}/checkpoints", empty_run.id)).await;
    assert_eq!(status, 200, "empty list: {empty}");
    assert_eq!(empty["checkpoints"], json!([]));

    // Absent run: every checkpoint route is a 404 E_RUN_NOT_FOUND.
    let absent = RunId::new();
    let (status, body) = get(&server, &format!("/api/runs/{absent}/checkpoints")).await;
    assert_eq!(status, 404, "absent run list: {body}");
    assert_eq!(body["code"], "E_RUN_NOT_FOUND");
    let (status, body) = post(
        &server,
        &format!("/api/runs/{absent}/checkpoints/1/restore"),
        &json!({}),
    )
    .await;
    assert_eq!(status, 404, "absent run restore: {body}");
    assert_eq!(body["code"], "E_RUN_NOT_FOUND");
    let (status, body) = post(
        &server,
        &format!("/api/runs/{absent}/checkpoints/revert"),
        &json!({"sequence": 1}),
    )
    .await;
    assert_eq!(status, 404, "absent run revert: {body}");
    assert_eq!(body["code"], "E_RUN_NOT_FOUND");

    // Rewind: two sequential mutations of one path restore the oldest bytes.
    let rewind_run = server
        .store()
        .create_run("truth-project", "rewind", AgentState::Completed)
        .expect("rewind run");
    fs::write(root.join("sequence.txt"), b"v0").expect("v0");
    journal
        .journal_before(rewind_run.id, 1, "sequence.txt")
        .expect("seq 1");
    fs::write(root.join("sequence.txt"), b"v1").expect("v1");
    journal.record_after(rewind_run.id, 1).expect("after 1");
    journal
        .journal_before(rewind_run.id, 2, "sequence.txt")
        .expect("seq 2");
    fs::write(root.join("sequence.txt"), b"v2").expect("v2");
    journal.record_after(rewind_run.id, 2).expect("after 2");

    let (status, list) = get(&server, &format!("/api/runs/{}/checkpoints", rewind_run.id)).await;
    assert_eq!(status, 200, "rewind list: {list}");
    let seqs: Vec<u64> = list["checkpoints"]
        .as_array()
        .expect("rewind checkpoints")
        .iter()
        .filter_map(|row| row["seq"].as_u64())
        .collect();
    assert_eq!(seqs, vec![2, 1], "list is newest-first");

    let (status, outcome) = post(
        &server,
        &format!("/api/runs/{}/checkpoints/revert", rewind_run.id),
        &json!({"sequence": 1}),
    )
    .await;
    assert_eq!(status, 200, "rewind: {outcome}");
    assert_eq!(outcome["from_seq"], 1);
    assert_eq!(outcome["restored"][0]["undone_seqs"], json!([2, 1]));
    assert_eq!(
        fs::read(root.join("sequence.txt")).expect("rewound"),
        b"v0",
        "rewinding both mutations restores the earliest bytes"
    );

    // A missing `sequence` is a schema refusal, not an empty success.
    let (status, body) = post(
        &server,
        &format!("/api/runs/{}/checkpoints/revert", rewind_run.id),
        &json!({}),
    )
    .await;
    assert_eq!(status, 400, "missing sequence: {body}");
    assert_eq!(body["code"], "E_SCHEMA_INVALID");
}

// ---------------------------------------------------------------------------
// 10. live-shape reproduction: an incomplete row is listed but not restorable
// ---------------------------------------------------------------------------

#[tokio::test]
async fn incomplete_entry_is_listed_as_null_after_and_refused_by_restore() {
    let temp = tempfile::tempdir().expect("tempdir");
    let server = TestServer::start(&temp).await;
    let run = server
        .store()
        .create_run("truth-project", "incomplete row", AgentState::Running)
        .expect("run");
    let journal = server.journal();
    let root = journal.jail().root().to_path_buf();
    fs::create_dir_all(&root).expect("workspace root");
    fs::write(root.join("pending.txt"), b"pending").expect("fixture file");
    journal
        .journal_before(run.id, 1, "pending.txt")
        .expect("capture only");

    let (status, list) = get(&server, &format!("/api/runs/{}/checkpoints", run.id)).await;
    assert_eq!(status, 200, "incomplete list: {list}");
    let row = &list["checkpoints"][0];
    assert!(
        row["after"].is_null(),
        "an unsettled mutation must render as null, never as absent: {row}"
    );
    assert!(row["completed_at"].is_null());

    let (status, response) = post(
        &server,
        &format!("/api/runs/{}/checkpoints/1/restore", run.id),
        &json!({}),
    )
    .await;
    assert_eq!(status, 409, "incomplete restore: {response}");
    assert_eq!(response["code"], "E_INTERNAL_ERROR");
    assert_eq!(
        fs::read(root.join("pending.txt")).expect("unchanged"),
        b"pending"
    );
}

// Keep the warning-free build honest: the server handle must be Send.
#[allow(dead_code)]
fn _assert_server_is_send() {
    fn assert_send<T: Send>() {}
    assert_send::<TestServer>();
}
