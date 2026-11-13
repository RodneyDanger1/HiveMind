//! Memory-context contract tests: hard caps/truncation, provenance labels,
//! task/tree isolation, secret redaction, empty-query refusal, and real store
//! wiring.
//!
//! `src/memory_context.rs` is compiled here through `#[path]` because this
//! work package is not allowed to touch `lib.rs`. The two shim modules expose
//! the same `crate::store` / `crate::semantic_recall` paths the module uses
//! once `lib.rs` declares it, so the file under test is byte-identical to the
//! production module.

#![allow(dead_code)]

mod store {
    pub use hiverouter_core::store::*;
}

mod semantic_recall {
    pub use hiverouter_core::semantic_recall::*;
}

#[path = "../src/memory_context.rs"]
mod memory_context;

use std::sync::Arc;

use hiverouter_contract::{AgentState, RunId, TaskId};
use hiverouter_core::db::Database;
use hiverouter_core::entities::TaskRecord;
use hiverouter_core::store::RunStore;

use hiverouter_core::semantic_recall::Verification;
use memory_context::{
    build_context, build_for_task, estimate_tokens, MemoryContextError, MemoryContextLimits,
    MemoryScope, MemoryTier, ScoringMode,
};

fn entry(
    bank: &str,
    key: &str,
    value: &str,
    trust: Option<&str>,
    created_at: &str,
) -> hiverouter_core::store::MemoryEntryRecord {
    hiverouter_core::store::MemoryEntryRecord {
        id: format!("entry:{bank}:{key}"),
        bank_id: bank.to_owned(),
        key: key.to_owned(),
        kind: if trust.is_none() { "note" } else { "fact" }.to_owned(),
        value: value.to_owned(),
        source_kind: "agent_result".to_owned(),
        trust: trust.map(str::to_owned),
        status: "active".to_owned(),
        created_at: created_at.to_owned(),
    }
}

fn scope(
    run_id: RunId,
    task_id: TaskId,
    parent_task_id: Option<TaskId>,
    ancestors: Vec<TaskId>,
) -> MemoryScope {
    MemoryScope::new(run_id, task_id, parent_task_id).with_ancestors(ancestors)
}

#[test]
fn context_is_hard_capped_and_reports_truncation() {
    let run_id = RunId::new();
    let task_id = TaskId::new();
    let bank = format!("bank:branch:{task_id}");
    let entries: Vec<_> = (0..12)
        .map(|index| {
            entry(
                &bank,
                &format!("note-{index}"),
                &format!("deploy note {index} {}", "cache ".repeat(40)),
                Some("verified"),
                "2026-11-13T00:00:00Z",
            )
        })
        .collect();

    let context = build_context(
        &entries,
        &scope(run_id, task_id, None, Vec::new()),
        "deploy cache",
        MemoryContextLimits::new(500, 1_000),
    )
    .expect("bounded context");

    assert!(
        context.rendered.chars().count() <= 500,
        "rendered chars {} exceed the 500 cap",
        context.rendered.chars().count()
    );
    assert!(
        context.estimated_tokens <= 1_000,
        "estimated tokens {} exceed the token cap",
        context.estimated_tokens
    );
    assert!(context.truncated, "omitting entries must set truncated");
    assert!(context.omitted_entries > 0, "the cap must omit entries");
    assert_eq!(
        context.entries.len() + context.omitted_entries,
        context.candidate_entries,
        "every candidate is either rendered or explicitly omitted"
    );
    assert!(
        context.rendered.contains("entries omitted"),
        "truncation must be explicit in the block: {}",
        context.rendered
    );
    assert!(
        context.entries.iter().any(|entry| entry.clipped),
        "the entry at the cap boundary must be marked clipped"
    );
}

#[test]
fn token_cap_is_enforced_independently_of_the_character_cap() {
    let run_id = RunId::new();
    let task_id = TaskId::new();
    let bank = format!("bank:branch:{task_id}");
    let entries: Vec<_> = (0..10)
        .map(|index| {
            entry(
                &bank,
                &format!("fact-{index}"),
                &format!("deploy cache detail {index} {}", "x".repeat(60)),
                Some("unverified"),
                "2026-11-13T00:00:00Z",
            )
        })
        .collect();

    let context = build_context(
        &entries,
        &scope(run_id, task_id, None, Vec::new()),
        "deploy cache",
        MemoryContextLimits::new(4_000, 40),
    )
    .expect("token-capped context");

    assert!(
        context.estimated_tokens <= 40,
        "estimated tokens {} exceed the 40-token cap",
        context.estimated_tokens
    );
    assert!(
        context.rendered.chars().count() <= 4_000,
        "the character cap must still hold"
    );
    assert_eq!(
        context.estimated_tokens,
        estimate_tokens(&context.rendered),
        "the reported estimate must describe the rendered block"
    );
    assert!(context.truncated);
    assert!(context.omitted_entries > 0);
}

#[test]
fn rendered_lines_carry_verification_and_tier_labels() {
    let run_id = RunId::new();
    let task_id = TaskId::new();
    let parent = TaskId::new();
    let run_bank = format!("bank:run:{run_id}");
    let hive_bank = format!("bank:hive:{parent}");
    let branch_bank = format!("bank:branch:{task_id}");
    let entries = vec![
        entry(
            &run_bank,
            "policy",
            "run policy: deploys require cache flush",
            Some("verified"),
            "2026-11-13T00:00:02Z",
        ),
        entry(
            &hive_bank,
            "scope",
            "hive constraint: cache uses rustls",
            Some("unverified"),
            "2026-11-13T00:00:01Z",
        ),
        entry(
            &branch_bank,
            "note",
            "branch note: deploy log is local",
            None,
            "2026-11-13T00:00:00Z",
        ),
    ];
    let context = build_context(
        &entries,
        &scope(run_id, task_id, Some(parent), vec![parent]),
        "deploy cache policy",
        MemoryContextLimits::default(),
    )
    .expect("labelled context");

    assert_eq!(context.scoring, ScoringMode::SemanticRecall);
    assert!(
        context.rendered.contains("[verified][run] policy = "),
        "verified run row must be labelled: {}",
        context.rendered
    );
    assert!(
        context.rendered.contains("[unverified][hive] scope = "),
        "unverified hive row must be labelled: {}",
        context.rendered
    );
    assert!(
        context.rendered.contains("[unknown][branch] note = "),
        "unlabelled row must render as unknown, never verified: {}",
        context.rendered
    );

    let run_entry = context
        .entries
        .iter()
        .find(|entry| entry.key == "policy")
        .expect("run entry");
    assert_eq!(run_entry.verification, Verification::Verified);
    assert!(run_entry.verified);
    assert_eq!(run_entry.tier, MemoryTier::Run);

    let note_entry = context
        .entries
        .iter()
        .find(|entry| entry.key == "note")
        .expect("note entry");
    assert_eq!(note_entry.verification, Verification::Unknown);
    assert!(
        !note_entry.verified,
        "an unlabelled row must never carry verified=true"
    );
    assert_eq!(note_entry.tier, MemoryTier::Branch);
}

#[test]
fn sibling_foreign_and_role_banks_are_isolated_out() {
    let run_id = RunId::new();
    let foreign_run = RunId::new();
    let task_id = TaskId::new();
    let parent = TaskId::new();
    let sibling = TaskId::new();
    let entries = vec![
        entry(
            &format!("bank:run:{run_id}"),
            "run-visible",
            "run fact: deploy log required",
            Some("verified"),
            "2026-11-13T00:00:05Z",
        ),
        entry(
            &format!("bank:branch:{task_id}"),
            "own-branch",
            "own fact: cache flush required",
            Some("unverified"),
            "2026-11-13T00:00:04Z",
        ),
        entry(
            &format!("bank:branch:{parent}"),
            "parent-branch",
            "parent fact: deploy version key",
            Some("unverified"),
            "2026-11-13T00:00:03Z",
        ),
        entry(
            &format!("bank:hive:{parent}"),
            "hive-visible",
            "hive fact: rustls only",
            Some("unverified"),
            "2026-11-13T00:00:02Z",
        ),
        entry(
            &format!("bank:branch:{sibling}"),
            "sibling-secret",
            "sibling leak must not render",
            Some("verified"),
            "2026-11-13T00:00:06Z",
        ),
        entry(
            &format!("bank:run:{foreign_run}"),
            "foreign-secret",
            "foreign run leak must not render",
            Some("verified"),
            "2026-11-13T00:00:06Z",
        ),
        entry(
            "bank:agent_role:worker",
            "role-secret",
            "role-global leak must not render",
            Some("verified"),
            "2026-11-13T00:00:06Z",
        ),
    ];

    let context = build_context(
        &entries,
        &scope(run_id, task_id, Some(parent), vec![parent]),
        "deploy cache version",
        MemoryContextLimits::default(),
    )
    .expect("isolated context");

    assert_eq!(
        context.excluded_out_of_scope, 3,
        "sibling, foreign-run, and role banks must all be excluded"
    );
    for visible in [
        "run-visible",
        "own-branch",
        "parent-branch",
        "hive-visible",
    ] {
        assert!(
            context.entries.iter().any(|entry| entry.key == visible),
            "visible key {visible} missing: {:?}",
            context.entries
        );
    }
    for secret in ["sibling-secret", "foreign-secret", "role-secret"] {
        assert!(
            !context.rendered.contains(secret),
            "out-of-scope key {secret} leaked into the block: {}",
            context.rendered
        );
    }
}

#[test]
fn secret_shaped_text_is_redacted_before_rendering() {
    let run_id = RunId::new();
    let task_id = TaskId::new();
    let bank = format!("bank:branch:{task_id}");
    let entries = vec![
        entry(
            &bank,
            "rotation",
            "rotate sk-live-ABCDEF1234567890 now and password=hunter2",
            Some("verified"),
            "2026-11-13T00:00:00Z",
        ),
        entry(
            &bank,
            "api_key",
            "plain-looking-token-value",
            Some("verified"),
            "2026-11-13T00:00:01Z",
        ),
    ];

    let context = build_context(
        &entries,
        &scope(run_id, task_id, None, Vec::new()),
        "rotate password",
        MemoryContextLimits::default(),
    )
    .expect("redacted context");

    assert!(
        !context.rendered.contains("sk-live-ABCDEF1234567890"),
        "credential survived in the block: {}",
        context.rendered
    );
    assert!(
        !context.rendered.contains("hunter2"),
        "password survived in the block: {}",
        context.rendered
    );
    assert!(
        !context.rendered.contains("plain-looking-token-value"),
        "a value under a sensitive key must be redacted wholesale: {}",
        context.rendered
    );
    assert!(
        context.rendered.contains("[REDACTED]"),
        "redaction must be explicit in the block: {}",
        context.rendered
    );
    assert!(
        context.redactions >= 3,
        "expected at least three redactions, got {}",
        context.redactions
    );
    assert!(
        context
            .entries
            .iter()
            .all(|entry| !entry.value.contains("sk-live")),
        "the structured result must not retain the credential either"
    );
}

#[test]
fn empty_or_tokenless_query_is_refused_by_name() {
    let run_id = RunId::new();
    let task_id = TaskId::new();
    let bank = format!("bank:branch:{task_id}");
    let entries = vec![entry(
        &bank,
        "note",
        "cache flush before deploy",
        Some("verified"),
        "2026-11-13T00:00:00Z",
    )];

    for query in ["", "   ", "...", "!!!"] {
        let error = build_context(
            &entries,
            &scope(run_id, task_id, None, Vec::new()),
            query,
            MemoryContextLimits::default(),
        )
        .expect_err("a tokenless query must refuse");
        assert!(
            matches!(error, MemoryContextError::EmptyQuery),
            "query {query:?} got {error:?}"
        );
        assert_eq!(error.code(), "E_MEMORY_CONTEXT_QUERY_EMPTY");
    }
}

#[test]
fn scope_with_no_readable_entry_refuses_with_counts() {
    let run_id = RunId::new();
    let task_id = TaskId::new();
    let sibling = TaskId::new();
    let entries = vec![entry(
        &format!("bank:branch:{sibling}"),
        "sibling",
        "not visible here",
        Some("verified"),
        "2026-11-13T00:00:00Z",
    )];

    let error = build_context(
        &entries,
        &scope(run_id, task_id, None, Vec::new()),
        "visible",
        MemoryContextLimits::default(),
    )
    .expect_err("no readable rows must refuse");
    assert!(
        matches!(
            &error,
            MemoryContextError::EmptyScope {
                excluded_out_of_scope: 1,
                ..
            }
        ),
        "wrong refusal: {error:?}"
    );
    assert_eq!(error.code(), "E_MEMORY_CONTEXT_EMPTY_SCOPE");
    assert!(error.message().contains("out_of_scope=1"), "{}", error.message());
}

fn insert_task(
    store: &RunStore,
    run_id: RunId,
    parent_task_id: Option<TaskId>,
    role: &str,
    depth: u8,
    spawn_seq: u64,
) -> TaskRecord {
    let task = TaskRecord {
        id: TaskId::new(),
        run_id,
        parent_task_id,
        role: role.to_owned(),
        depth,
        spawn_seq,
        state: AgentState::Running,
        rev: 0,
    };
    store.insert_task(&task).expect("task");
    task
}

#[test]
fn store_wiring_derives_the_tree_and_reads_only_visible_banks() {
    let database = Arc::new(Database::open_in_memory().expect("in-memory database"));
    database.migrate().expect("migrations");
    let store = RunStore::new(Arc::clone(&database));

    let run = store
        .create_run("p", "memory context wiring", AgentState::Running)
        .expect("run");
    let hyper = insert_task(&store, run.id, None, "planner", 0, 1);
    let super_task = insert_task(&store, run.id, Some(hyper.id), "super", 1, 2);
    let worker = insert_task(&store, run.id, Some(super_task.id), "worker", 2, 3);
    let sibling = insert_task(&store, run.id, Some(super_task.id), "worker", 2, 4);

    let run_entry = store
        .record_memory_entry(
            run.id,
            hyper.id,
            "hyper",
            "run-policy",
            "run policy: deploy log required",
            "constraint",
            "unverified",
        )
        .expect("run entry");
    store
        .record_memory_entry(
            run.id,
            super_task.id,
            "super",
            "hive-fact",
            "hive fact: rustls only for deploy",
            "fact",
            "unverified",
        )
        .expect("hive entry");
    store
        .record_memory_entry(
            run.id,
            worker.id,
            "worker",
            "own-note",
            "own note: cache flush before deploy",
            "fact",
            "unverified",
        )
        .expect("own entry");
    store
        .record_memory_entry(
            run.id,
            sibling.id,
            "worker",
            "sibling-note",
            "sibling only cache detail",
            "fact",
            "unverified",
        )
        .expect("sibling entry");

    let run_bank = format!("bank:run:{}", run.id);
    assert!(
        store
            .promote_memory_entry(run.id, &run_bank, &run_entry, "verified", "operator:test")
            .expect("promotion"),
        "operator promotion must change the run row"
    );

    let foreign_run = store
        .create_run("p", "foreign run", AgentState::Running)
        .expect("foreign run");
    let foreign_hyper = insert_task(&store, foreign_run.id, None, "planner", 0, 1);
    store
        .record_memory_entry(
            foreign_run.id,
            foreign_hyper.id,
            "hyper",
            "foreign-note",
            "foreign run memory must not leak",
            "fact",
            "verified",
        )
        .expect("foreign entry");

    let derived = MemoryScope::derive(&store, run.id, worker.id).expect("scope");
    assert_eq!(derived.parent_task_id, Some(super_task.id));
    assert_eq!(
        derived.ancestor_task_ids,
        vec![super_task.id, hyper.id],
        "ancestors must come from the persisted tree, nearest first"
    );
    assert!(derived
        .allowed_banks()
        .contains(&format!("bank:hive:{}", super_task.id)));

    let context = build_for_task(
        &store,
        run.id,
        worker.id,
        "deploy cache rustls",
        MemoryContextLimits::default(),
    )
    .expect("wired context");

    assert_eq!(context.scoring, ScoringMode::SemanticRecall);
    assert!(
        context.rendered.contains("[verified][run] run-policy = "),
        "run row must be visible and labelled: {}",
        context.rendered
    );
    for visible in ["hive-fact", "own-note"] {
        assert!(
            context.entries.iter().any(|entry| entry.key == visible),
            "visible key {visible} missing: {:?}",
            context.entries
        );
    }
    for hidden in ["sibling-note", "foreign-note"] {
        assert!(
            !context.rendered.contains(hidden),
            "hidden key {hidden} leaked: {}",
            context.rendered
        );
    }
    assert_eq!(
        context.excluded_out_of_scope, 0,
        "build_for_task must never load an out-of-scope bank at all"
    );
}
