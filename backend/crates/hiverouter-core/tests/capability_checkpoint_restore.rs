//! Acceptance tests for the id-addressed checkpoint restore (undo) API.
//!
//! Coverage:
//! * a verified restore receipt for an overwritten file;
//! * restoration of a file the run created (returns to absence);
//! * stale-file refusal: an external edit is never overwritten;
//! * tampered-blob refusal: the content-addressed pre-image is re-hashed;
//! * path-mismatch and outside-jail refusals;
//! * a second restore of the same entry is refused as drift (no guessed redo).

use std::fs;
use std::path::Path;
use std::sync::Arc;

use hiverouter_contract::{Refusal, RunId, Timestamp};
use hiverouter_core::checkpoint::{CheckpointError, CheckpointJournal, FileState};
use hiverouter_core::db::Database;
use hiverouter_tools::sha256_hex;

fn fixture() -> (tempfile::TempDir, Arc<Database>, CheckpointJournal) {
    let temp = tempfile::tempdir().expect("tempdir");
    let workspace = temp.path().join("workspace");
    fs::create_dir_all(&workspace).expect("workspace dir");
    let database = Arc::new(Database::open_in_memory().expect("in-memory database"));
    database.migrate().expect("migrations");
    let journal = CheckpointJournal::open(database.clone(), &workspace).expect("journal");
    (temp, database, journal)
}

fn insert_run(database: &Database) -> RunId {
    let run_id = RunId::new();
    database
        .with_connection(|connection| {
            connection.execute(
                "INSERT INTO runs(id, project_id, prompt, status, created_at, updated_at)
                 VALUES (?1, 'project', 'restore-by-id test', 'running', ?2, ?2)",
                rusqlite::params![run_id.to_string(), Timestamp::now().as_str()],
            )?;
            Ok(())
        })
        .expect("insert run row");
    run_id
}

fn write_file(root: &Path, relative: &str, bytes: &[u8]) {
    let path = root.join(relative);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("parent dir");
    }
    fs::write(path, bytes).expect("write test file");
}

fn present(sha256: String, size: u64) -> FileState {
    FileState::Present { sha256, size }
}

#[test]
fn restore_by_id_returns_a_verified_receipt_and_exact_bytes() {
    let (_temp, database, journal) = fixture();
    let run = insert_run(&database);
    let root = journal.jail().root().to_path_buf();
    write_file(&root, "notes/one.txt", b"alpha");

    let entry = journal
        .journal_before(run, 1, "notes/one.txt")
        .expect("journal before");
    write_file(&root, "notes/one.txt", b"beta");
    journal.record_after(run, 1).expect("record after");

    let receipt = journal
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
        "the receipt must name the pre-image that was written"
    );
    assert_eq!(receipt.bytes_written, 5);
    assert_eq!(
        fs::read(root.join("notes/one.txt")).expect("restored file"),
        b"alpha",
        "restore must be byte-exact"
    );

    let stored = journal.get(run, 1).expect("get").expect("row");
    assert!(stored.restored_at.is_some(), "restore must be journaled");
    assert_eq!(
        journal
            .get_by_id(&entry.checkpoint_id)
            .expect("get by id")
            .expect("row")
            .checkpoint_id,
        entry.checkpoint_id
    );

    // Undo is not redo: the file now matches the pre-image, not the recorded
    // after-state, so a second restore of the same entry is drift.
    let second = journal
        .restore_by_id(&entry.checkpoint_id, "notes/one.txt")
        .expect_err("a second restore must be refused");
    assert!(
        matches!(second, CheckpointError::Drift { .. }),
        "expected drift, got {second:?}"
    );
    assert_eq!(
        fs::read(root.join("notes/one.txt")).expect("file still present"),
        b"alpha"
    );
}

#[test]
fn restoring_a_created_file_returns_it_to_absence() {
    let (_temp, database, journal) = fixture();
    let run = insert_run(&database);
    let root = journal.jail().root().to_path_buf();

    let entry = journal
        .journal_before(run, 1, "created/later.txt")
        .expect("journal absent file");
    assert_eq!(entry.before, FileState::Absent);
    write_file(&root, "created/later.txt", b"new content");
    journal.record_after(run, 1).expect("record after");

    let receipt = journal
        .restore_by_id(&entry.checkpoint_id, "created/later.txt")
        .expect("restore absence");
    assert_eq!(receipt.restored_to, FileState::Absent);
    assert_eq!(receipt.bytes_written, 0);
    assert!(
        !root.join("created/later.txt").exists(),
        "the created file must be removed again"
    );
}

#[test]
fn stale_file_is_refused_and_never_overwritten() {
    let (_temp, database, journal) = fixture();
    let run = insert_run(&database);
    let root = journal.jail().root().to_path_buf();
    write_file(&root, "notes/one.txt", b"alpha");
    let entry = journal
        .journal_before(run, 1, "notes/one.txt")
        .expect("journal before");
    write_file(&root, "notes/one.txt", b"beta");
    journal.record_after(run, 1).expect("record after");

    // External, unjournaled edit after the mutation.
    write_file(&root, "notes/one.txt", b"gamma");

    let error = journal
        .restore_by_id(&entry.checkpoint_id, "notes/one.txt")
        .expect_err("stale file must be refused");
    assert!(
        matches!(error, CheckpointError::Drift { .. }),
        "expected drift, got {error:?}"
    );
    assert_eq!(
        fs::read(root.join("notes/one.txt")).expect("external edit intact"),
        b"gamma",
        "a refused restore must not clobber the external edit"
    );
    assert!(
        journal
            .get(run, 1)
            .expect("get")
            .expect("row")
            .restored_at
            .is_none(),
        "a refused restore must not be marked restored"
    );
}

#[test]
fn tampered_pre_image_blob_is_refused_before_any_write() {
    let (_temp, database, journal) = fixture();
    let run = insert_run(&database);
    let root = journal.jail().root().to_path_buf();
    write_file(&root, "notes/one.txt", b"alpha");
    let entry = journal
        .journal_before(run, 1, "notes/one.txt")
        .expect("journal before");
    write_file(&root, "notes/one.txt", b"beta");
    journal.record_after(run, 1).expect("record after");

    // Corrupt the content-addressed pre-image: the row still points at the
    // alpha digest, but the stored bytes no longer hash to it.
    database
        .with_connection(|connection| {
            connection.execute(
                "UPDATE checkpoint_blobs SET bytes = ?1 WHERE sha256 = ?2",
                rusqlite::params![b"tampered".to_vec(), sha256_hex(b"alpha")],
            )?;
            Ok(())
        })
        .expect("tamper blob");

    let error = journal
        .restore_by_id(&entry.checkpoint_id, "notes/one.txt")
        .expect_err("tampered blob must be refused");
    assert!(
        matches!(error, CheckpointError::Corrupt(_)),
        "expected corruption refusal, got {error:?}"
    );
    assert_eq!(
        fs::read(root.join("notes/one.txt")).expect("mutation intact"),
        b"beta",
        "a tampered pre-image must not be written over the live file"
    );
}

#[test]
fn wrong_path_and_outside_jail_paths_are_refused() {
    let (_temp, database, journal) = fixture();
    let run = insert_run(&database);
    let root = journal.jail().root().to_path_buf();
    write_file(&root, "notes/one.txt", b"alpha");
    let entry = journal
        .journal_before(run, 1, "notes/one.txt")
        .expect("journal before");
    write_file(&root, "notes/one.txt", b"beta");
    journal.record_after(run, 1).expect("record after");

    let mismatch = journal
        .restore_by_id(&entry.checkpoint_id, "notes/two.txt")
        .expect_err("a different path must be refused");
    match &mismatch {
        CheckpointError::PathMismatch {
            recorded,
            requested,
            ..
        } => {
            assert_eq!(recorded, "notes/one.txt");
            assert_eq!(requested, "notes/two.txt");
        }
        other => panic!("expected path mismatch, got {other:?}"),
    }
    assert_eq!(mismatch.code(), Refusal::InternalError.code());

    let escape = journal
        .restore_by_id(&entry.checkpoint_id, "../escape.txt")
        .expect_err("traversal must be refused");
    match &escape {
        CheckpointError::Tool { refusal, .. } => {
            assert_eq!(*refusal, Refusal::PathOutsideJail)
        }
        other => panic!("expected jail refusal, got {other:?}"),
    }
    assert_eq!(
        fs::read(root.join("notes/one.txt")).expect("mutation intact"),
        b"beta"
    );

    let unknown = journal
        .restore_by_id("00000000-0000-0000-0000-000000000000", "notes/one.txt")
        .expect_err("unknown id must be refused");
    assert!(
        matches!(unknown, CheckpointError::CheckpointIdNotFound(_)),
        "expected unknown id, got {unknown:?}"
    );
}

#[test]
fn incomplete_entries_and_empty_ids_are_refused() {
    let (_temp, database, journal) = fixture();
    let run = insert_run(&database);
    let root = journal.jail().root().to_path_buf();
    write_file(&root, "notes/one.txt", b"alpha");
    let entry = journal
        .journal_before(run, 1, "notes/one.txt")
        .expect("journal before");

    // record_after was never called: the after-state is unknown, so drift
    // cannot be proven and the restore must refuse.
    let incomplete = journal
        .restore_by_id(&entry.checkpoint_id, "notes/one.txt")
        .expect_err("incomplete entry must be refused");
    assert!(
        matches!(incomplete, CheckpointError::Incomplete { seq: 1, .. }),
        "expected incomplete refusal, got {incomplete:?}"
    );

    let empty = journal
        .restore_by_id("   ", "notes/one.txt")
        .expect_err("empty id must be refused");
    assert_eq!(empty.code(), Refusal::ToolSchema.code());
    assert_eq!(
        fs::read(root.join("notes/one.txt")).expect("file intact"),
        b"alpha"
    );
}
