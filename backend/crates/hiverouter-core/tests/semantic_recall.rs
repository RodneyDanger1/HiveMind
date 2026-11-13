//! Semantic recall integration tests (lexical TF-IDF + trigram fuzzy fallback).
//!
//! `src/semantic_recall.rs` is compiled here through `#[path]` because this
//! work package was not allowed to touch `lib.rs`. The two shim modules below
//! expose the same `crate::store` / `crate::evolution_gate` paths the module
//! uses once `lib.rs` declares it, so the file under test is byte-identical to
//! the production module.

#![allow(dead_code)]

mod store {
    pub use hiverouter_core::store::*;
}

mod evolution_gate {
    pub use hiverouter_core::evolution_gate::*;
}

#[path = "../src/semantic_recall.rs"]
mod semantic_recall;

use std::sync::Arc;

use hiverouter_contract::{AgentState, Refusal, RunId, TaskId};
use hiverouter_core::db::Database;
use hiverouter_core::entities::TaskRecord;

use semantic_recall::{
    promote_lesson, recall_bank, recall_run, LessonPromotionAck, RecallError, RecallQuery,
    Verification, ALGORITHM,
};
use store::RunStore;

fn store_with_run(label: &str) -> (RunStore, Arc<Database>, RunId, TaskId) {
    let database = Arc::new(Database::open_in_memory().expect("in-memory database"));
    database.migrate().expect("migrations");
    let store = RunStore::new(Arc::clone(&database));
    let run = store
        .create_run("p", label, AgentState::Running)
        .expect("run");
    let task = TaskRecord {
        id: TaskId::new(),
        run_id: run.id,
        parent_task_id: None,
        role: "worker".to_owned(),
        depth: 1,
        spawn_seq: 1,
        state: AgentState::Running,
        rev: 0,
    };
    store.insert_task(&task).expect("task");
    (store, database, run.id, task.id)
}

fn branch_bank(task: TaskId) -> String {
    format!("bank:branch:{task}")
}

#[test]
fn paraphrased_query_recalls_the_stored_entry_with_source_ids() {
    let (store, _database, run_id, task_id) = store_with_run("paraphrase recall");
    let bank = branch_bank(task_id);
    let cache_entry = store
        .record_memory_entry(
            run_id,
            task_id,
            "worker",
            "deploy-cache",
            "Cache entries must be invalidated whenever a deployment ships",
            "fact",
            "unverified",
        )
        .expect("cache entry");
    store
        .record_memory_entry(
            run_id,
            task_id,
            "worker",
            "token-budget",
            "The token budget for a run is capped at 120000 tokens",
            "constraint",
            "unverified",
        )
        .expect("budget entry");

    // Paraphrase: invalidate/cache/deploying vs invalidated/cache/deployment.
    let query = RecallQuery::new("how do we invalidate stale cache after deploying");
    let response = recall_bank(&store, run_id, &bank, &query).expect("recall");

    assert_eq!(response.indexed_entries, 2, "both entries must be indexed");
    assert_eq!(response.algorithm, ALGORITHM);
    assert!(
        response.query_terms.contains(&"invalidate".to_owned()),
        "query stem drifted: {:?}",
        response.query_terms
    );
    let first = response.hits.first().expect("at least one hit");
    assert_eq!(
        first.key, "deploy-cache",
        "the paraphrased entry must rank first: {:?}",
        response.hits
    );
    assert_eq!(first.entry_id, cache_entry, "entry id is the stable source id");
    assert_eq!(first.bank_id, bank, "bank id is part of the source identity");
    assert!(
        first.score > 0.2,
        "a paraphrase sharing three stems must score materially, got {}",
        first.score
    );
    assert_eq!(
        response.hits.len(),
        1,
        "the token-budget entry shares no query signal: {:?}",
        response.hits
    );
}

#[test]
fn unverified_entries_are_labelled_and_promoted_entries_are_verified() {
    let (store, _database, run_id, task_id) = store_with_run("verification labels");
    let bank = branch_bank(task_id);
    let unverified_id = store
        .record_memory_entry(
            run_id,
            task_id,
            "worker",
            "flush-a",
            "Cache flush must run before the new build starts",
            "fact",
            "unverified",
        )
        .expect("unverified entry");
    let verified_id = store
        .record_memory_entry(
            run_id,
            task_id,
            "worker",
            "flush-b",
            "Cache flush uses the build version string as its key",
            "fact",
            "unverified",
        )
        .expect("second entry");
    let promoted = store
        .promote_memory_entry(run_id, &bank, &verified_id, "verified", "operator:test")
        .expect("operator promotion");
    assert!(promoted, "operator promotion must change exactly one row");

    let response = recall_bank(
        &store,
        run_id,
        &bank,
        &RecallQuery::new("cache flush before build"),
    )
    .expect("recall");
    assert_eq!(response.hits.len(), 2, "{:?}", response.hits);

    let unverified = response
        .hits
        .iter()
        .find(|hit| hit.entry_id == unverified_id)
        .expect("unverified hit");
    assert_eq!(
        unverified.provenance.verification,
        Verification::Unverified
    );
    assert_eq!(unverified.provenance.trust.as_deref(), Some("unverified"));
    assert_eq!(unverified.provenance.source_kind, "agent_result");
    assert!(
        !unverified.provenance.verified,
        "an unverified row must never carry verified=true"
    );

    let verified = response
        .hits
        .iter()
        .find(|hit| hit.entry_id == verified_id)
        .expect("verified hit");
    assert_eq!(verified.provenance.verification, Verification::Verified);
    assert_eq!(verified.provenance.trust.as_deref(), Some("verified"));
    assert!(verified.provenance.verified);
}

#[test]
fn empty_store_refuses_by_name() {
    let (store, _database, run_id, _task_id) = store_with_run("empty store");
    let error = recall_run(&store, run_id, &RecallQuery::new("cache invalidation"))
        .expect_err("an empty run must refuse, not return a plausible empty result");
    assert!(
        matches!(&error, RecallError::EmptyStore { scope } if scope == &format!("run:{run_id}")),
        "wrong refusal: {error:?}"
    );
    assert_eq!(error.code(), "E_RECALL_STORE_EMPTY");
    assert!(
        error.message().contains("no readable memory entries"),
        "message must name the condition: {}",
        error.message()
    );
}

#[test]
fn corrupt_persisted_entry_refuses_by_name() {
    let (store, database, run_id, task_id) = store_with_run("corrupt store");
    let bank = branch_bank(task_id);
    database
        .with_connection(|connection| {
            connection.execute(
                "INSERT OR IGNORE INTO memory_banks
                 (id, kind, scope_id, run_id, owner_task_id, parent_bank_id,
                  display_name, rev, created_at, closed_at)
                 VALUES (?1, 'branch', ?2, ?3, ?4, ?5, 'Corrupt', 0, ?6, NULL)",
                rusqlite::params![
                    bank,
                    task_id.to_string(),
                    run_id.to_string(),
                    task_id.to_string(),
                    format!("bank:run:{run_id}"),
                    "2026-11-13T00:00:00Z"
                ],
            )?;
            // A blank entry key is accepted by SQLite but is never produced by
            // the validated write path: recall must refuse it by name.
            connection.execute(
                "INSERT INTO memory_entries
                 (id, bank_id, entry_key, kind, value, source_kind, source_task_id,
                  source_run_id, trust, status, created_at, updated_at)
                 VALUES ('entry:corrupt', ?1, '   ', 'fact',
                         'cache flush notes written by a corrupted importer',
                         'import', NULL, NULL, 'unverified', 'active', ?2, ?2)",
                rusqlite::params![bank, "2026-11-13T00:00:00Z"],
            )?;
            Ok(())
        })
        .expect("plant corrupt row");

    let error = recall_bank(&store, run_id, &bank, &RecallQuery::new("cache flush"))
        .expect_err("a corrupt persisted store must refuse");
    assert!(
        matches!(
            &error,
            RecallError::CorruptStore { entry_id, reason }
                if entry_id == "entry:corrupt" && *reason == "blank entry key"
        ),
        "wrong refusal: {error:?}"
    );
    assert_eq!(error.code(), "E_RECALL_STORE_CORRUPT");
    assert!(error.message().contains("entry:corrupt"));
}

#[test]
fn lesson_promotion_requires_explicit_ack_and_verified_receipt() {
    let directory = tempfile::tempdir().expect("tempdir");
    let ack = LessonPromotionAck::explicit("operator:riley").expect("ack");
    assert_eq!(
        LessonPromotionAck::explicit("   "),
        Err(Refusal::SchemaInvalid),
        "an acknowledgement without an operator must not exist"
    );

    // No receipt on disk: promotion is refused, never silently applied.
    assert_eq!(
        promote_lesson(
            directory.path(),
            "p-missing",
            "recall",
            "stale cache entries",
            "prefer verified entries",
            &ack
        ),
        Err(Refusal::ApprovalRequired)
    );

    write_valid_receipt(directory.path(), "p-valid");
    let receipt = promote_lesson(
        directory.path(),
        "p-valid",
        "recall",
        "stale cache entries",
        "prefer verified entries",
        &ack,
    )
    .expect("verified receipt must promote");
    assert_eq!(receipt.candidate_id, "p-valid");
    assert_eq!(receipt.task_count, 8);
    assert_eq!(receipt.receipt_sha256.len(), 64);
    assert_eq!(receipt.operator, "operator:riley");
    assert!(!receipt.weights_changed, "promotion must not change weights");
    assert!(!receipt.code_changed, "promotion must not change code");
    let audit = receipt.audit_line();
    assert!(audit.contains("\"weights_changed\":false"), "{audit}");
    assert!(audit.contains("\"code_changed\":false"), "{audit}");
}

fn write_valid_receipt(root: &std::path::Path, proposal: &str) {
    let path = root
        .join("evolution")
        .join("eval")
        .join(proposal)
        .join("receipt.json");
    std::fs::create_dir_all(path.parent().expect("parent")).expect("dir");
    let value = serde_json::json!({
        "schema": "hivemind.evolution-eval.v1",
        "proposal_id": proposal,
        "head_commit": "d8c00fdc208f014b4d26eea51408e1c2e74ed9eb",
        "task_set": { "id": "held-out", "sha256": "a".repeat(64), "n": 8 },
        "evaluator": { "id": "eval", "sha256": "b".repeat(64) },
        "baseline": { "pass_rate": 0.5, "ci_lower": 0.4, "ci_upper": 0.6 },
        "candidate": { "pass_rate": 0.9, "ci_lower": 0.8, "ci_upper": 0.95 },
        "observed_at": "2026-11-10T00:00:00Z",
        "command": "run suite",
        "artifacts": [{ "sha256": "c".repeat(64) }]
    });
    std::fs::write(&path, value.to_string()).expect("write receipt");
}
