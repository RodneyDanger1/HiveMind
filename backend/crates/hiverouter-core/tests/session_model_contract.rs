//! Session/continuation model contract tests (real SQLite, real migrations).
//!
//! The module is compiled directly from `src/session_model.rs` because it is
//! not yet registered in `lib.rs` (see `docs/SESSION-MODEL.md` for the wiring
//! still required). Every test runs against a temp-file database migrated with
//! the production migration list, so the SQL under test is the production
//! schema, not a fixture copy.

#[path = "../src/session_model.rs"]
mod session_model;

use std::sync::Arc;

use hiverouter_contract::{AgentState, TaskId};
use hiverouter_core::db::Database;
use hiverouter_core::entities::{ChatRole, PermissionPolicy, TaskRecord};
use hiverouter_core::store::RunStore;
use session_model::{
    assess_continuation, attach_run, continuation_context, latest_resumable_run, session_run_model,
    AttachOutcome, ContextSource, ContinueDecision, ContinueReason, SessionModelError,
    DEFAULT_CONTINUATION_BUDGET_BYTES, MAX_CONTINUATION_BUDGET_BYTES, MAX_PENDING_TODOS,
    MAX_PENDING_TODO_TEXT_BYTES, MAX_SESSION_RUNS,
};

struct Fixture {
    db: Arc<Database>,
    store: RunStore,
    _temp: tempfile::TempDir,
}

impl Fixture {
    fn new() -> Self {
        let temp = tempfile::tempdir().expect("temp dir");
        let db = Arc::new(Database::open(&temp.path().join("hiverouter.db")).expect("open db"));
        db.migrate().expect("migrate");
        Self {
            store: RunStore::new(db.clone()),
            db,
            _temp: temp,
        }
    }

    fn session(&self, project: &str) -> String {
        self.store
            .create_session(
                project,
                "session",
                "C:\\workspace",
                PermissionPolicy::Classic,
            )
            .expect("create session")
            .id
    }

    fn run(&self, project: &str) -> hiverouter_contract::RunId {
        self.store
            .create_run(project, "prompt", AgentState::Queued)
            .expect("create run")
            .id
    }

    fn read<T>(&self, f: impl FnOnce(&rusqlite::Connection) -> T) -> T {
        let mut slot: Option<T> = None;
        self.db
            .with_connection(|connection| {
                slot = Some(f(connection));
                Ok(())
            })
            .expect("read");
        slot.expect("read closure ran")
    }
}

fn task(fixture: &Fixture, run_id: hiverouter_contract::RunId, state: AgentState) -> TaskId {
    let task = TaskRecord {
        id: TaskId::new(),
        run_id,
        parent_task_id: None,
        role: "worker".to_owned(),
        depth: 0,
        spawn_seq: 1,
        state,
        rev: 0,
    };
    fixture.store.insert_task(&task).expect("insert task");
    task.id
}

#[test]
fn attach_records_run_ids_in_order_and_reports_last_completed_and_pending_work() {
    let fixture = Fixture::new();
    let session_id = fixture.session("project-1");
    let first = fixture.run("project-1");
    let second = fixture.run("project-1");
    let third = fixture.run("project-1");

    // Attach deliberately out of creation order: the ledger must follow run
    // order, not attach order.
    for run in [third, first, second] {
        let outcome = fixture
            .read(|connection| attach_run(connection, &session_id, &run.to_string()))
            .expect("attach run");
        assert_eq!(outcome, AttachOutcome::Attached);
    }

    fixture
        .store
        .set_run_status(first, AgentState::Failed, Some("E_PROVIDER"))
        .expect("fail first");
    fixture
        .store
        .set_run_status(second, AgentState::Completed, None)
        .expect("complete second");
    fixture
        .store
        .set_run_status(third, AgentState::Running, None)
        .expect("run third");
    // Completed run exists among the runs, but its `updated_at` is older than
    // the failed/first run? No: completion is the only completed row, so it is
    // the last completed run regardless of position.
    fixture
        .store
        .add_todo(&session_id, "open todo")
        .expect("open todo");
    let done = fixture
        .store
        .add_todo(&session_id, "done todo")
        .expect("done todo");
    fixture
        .store
        .set_todo_state(&session_id, &done.id, "done")
        .expect("settle todo");
    task(&fixture, first, AgentState::Running);

    let model = fixture
        .read(|connection| session_run_model(connection, &session_id))
        .expect("session model");
    assert_eq!(
        model.run_ids,
        vec![first.to_string(), second.to_string(), third.to_string()]
    );
    assert_eq!(model.run_count, 3);
    assert!(!model.runs_truncated);
    assert_eq!(model.runs[0].ordinal, 1);
    assert_eq!(model.runs[2].ordinal, 3);
    assert_eq!(model.runs[0].status, AgentState::Failed);
    assert_eq!(model.runs[0].error_code.as_deref(), Some("E_PROVIDER"));
    assert_eq!(
        model
            .last_completed_run
            .as_ref()
            .map(|run| run.run_id.clone()),
        Some(second.to_string())
    );
    assert_eq!(model.pending_work.active_run_count, 1);
    assert_eq!(model.pending_work.active_runs[0].run_id, third.to_string());
    assert_eq!(
        model.pending_work.active_runs[0].status,
        AgentState::Running
    );
    assert_eq!(model.pending_work.open_todo_count, 1);
    assert_eq!(model.pending_work.open_todos[0].text, "open todo");
    assert_eq!(model.pending_work.unfinished_task_count, 1);
    assert!(!model.pending_work.is_empty());
}

#[test]
fn attach_is_idempotent_and_refuses_cross_project_foreign_and_archived_sessions() {
    let fixture = Fixture::new();
    let session_id = fixture.session("project-a");
    let other = fixture.session("project-a");
    let run = fixture.run("project-a");
    let run_id = run.to_string();

    let first_attach = fixture
        .read(|connection| attach_run(connection, &session_id, &run_id))
        .expect("first attach");
    assert_eq!(first_attach, AttachOutcome::Attached);
    let second_attach = fixture
        .read(|connection| attach_run(connection, &session_id, &run_id))
        .expect("second attach");
    assert_eq!(second_attach, AttachOutcome::AlreadyAttached);
    // A run already bound to one session may not be stolen by another.
    assert!(matches!(
        fixture.read(|connection| attach_run(connection, &other, &run_id)),
        Err(SessionModelError::RunInOtherSession(_, _))
    ));

    let foreign = fixture.run("project-b");
    assert!(matches!(
        fixture.read(|connection| attach_run(connection, &session_id, &foreign.to_string())),
        Err(SessionModelError::ProjectMismatch { .. })
    ));

    assert!(matches!(
        fixture.read(|connection| attach_run(connection, &session_id, "missing-run")),
        Err(SessionModelError::RunNotFound(_))
    ));

    let unbound = fixture.run("project-a");
    let unbound_id = unbound.to_string();
    fixture
        .store
        .set_session_archived(&session_id, true)
        .expect("archive");
    assert!(matches!(
        fixture.read(|connection| attach_run(connection, &session_id, &unbound_id)),
        Err(SessionModelError::SessionArchived(_))
    ));

    assert!(matches!(
        fixture.read(|connection| attach_run(connection, "missing", &unbound_id)),
        Err(SessionModelError::SessionNotFound(_))
    ));
}

#[test]
fn continuation_context_is_byte_bounded_and_cites_persisted_provenance() {
    let fixture = Fixture::new();
    let session_id = fixture.session("project-1");
    let first = fixture.run("project-1");
    let second = fixture.run("project-1");
    fixture
        .read(|connection| attach_run(connection, &session_id, &first.to_string()))
        .expect("attach first");
    fixture
        .read(|connection| attach_run(connection, &session_id, &second.to_string()))
        .expect("attach second");
    fixture
        .store
        .append_message(first, ChatRole::User, "first user message")
        .expect("m1");
    fixture
        .store
        .append_message(first, ChatRole::Assistant, "first assistant reply")
        .expect("m2");
    fixture
        .store
        .append_message(second, ChatRole::System, "second run context")
        .expect("m3");
    let child = task(&fixture, second, AgentState::Completed);
    fixture
        .store
        .record_task_result(second, child, "task result from child")
        .expect("result");

    let context = fixture
        .read(|connection| continuation_context(connection, &session_id, 30))
        .expect("context");
    assert_eq!(context.budget_bytes, 30);
    assert_eq!(context.used_bytes, 30);
    assert_eq!(context.total_items, 4);
    assert_eq!(context.omitted_items, 2);
    assert!(context.truncated);
    assert_eq!(context.items.len(), 2);
    // Newest-first selection kept the newest item whole and the next-newest
    // item as a byte-capped prefix; output is ascending (oldest first).
    assert_eq!(context.items[0].text, "second r");
    assert!(context.items[0].truncated);
    assert_eq!(context.items[0].original_bytes, 18);
    assert_eq!(context.items[1].text, "task result from child");
    assert!(!context.items[1].truncated);
    assert_eq!(context.items[1].source, ContextSource::TaskResult);
    assert_eq!(context.items[1].source.as_str(), "task_result");
    assert_eq!(context.items[1].record_id, child.to_string());
    assert_eq!(context.items[1].run_id, second.to_string());
    assert_eq!(context.items[1].role, None);
    assert_eq!(context.original_bytes, 18 + 22);
    assert_eq!(context.source_run_ids, vec![second.to_string()]);
    // Provenance for the message prefix is the real message row.
    assert_eq!(context.items[0].source, ContextSource::ChatMessage);
    assert_eq!(context.items[0].role.as_deref(), Some("system"));
    assert_eq!(context.items[0].run_id, second.to_string());
    assert!(!context.items[0].record_id.is_empty());

    let full = fixture
        .read(|connection| continuation_context(connection, &session_id, 1024))
        .expect("full context");
    assert_eq!(full.items.len(), 4);
    assert_eq!(full.used_bytes, 79);
    assert_eq!(full.original_bytes, 79);
    assert_eq!(full.omitted_items, 0);
    assert!(!full.truncated);
    assert_eq!(
        full.items
            .iter()
            .map(|item| item.text.as_str())
            .collect::<Vec<_>>(),
        vec![
            "first user message",
            "first assistant reply",
            "second run context",
            "task result from child",
        ]
    );
    assert_eq!(
        full.source_run_ids,
        vec![first.to_string(), second.to_string()]
    );
}

#[test]
fn continuation_context_excludes_other_sessions_and_rejects_invalid_budgets() {
    let fixture = Fixture::new();
    let session_id = fixture.session("project-1");
    let other_session = fixture.session("project-1");
    let mine = fixture.run("project-1");
    let theirs = fixture.run("project-1");
    fixture
        .read(|connection| attach_run(connection, &session_id, &mine.to_string()))
        .expect("attach mine");
    fixture
        .read(|connection| attach_run(connection, &other_session, &theirs.to_string()))
        .expect("attach theirs");
    fixture
        .store
        .append_message(mine, ChatRole::User, "mine")
        .expect("mine");
    fixture
        .store
        .append_message(theirs, ChatRole::User, "theirs")
        .expect("theirs");
    let unbound = fixture.run("project-1");
    fixture
        .store
        .append_message(unbound, ChatRole::User, "unattached run")
        .expect("unbound");

    let context = fixture
        .read(|connection| continuation_context(connection, &session_id, 64))
        .expect("context");
    assert_eq!(context.items.len(), 1);
    assert_eq!(context.items[0].text, "mine");
    assert_eq!(context.total_items, 1);

    for bad in [0, MAX_CONTINUATION_BUDGET_BYTES + 1] {
        assert!(matches!(
            fixture.read(|connection| continuation_context(connection, &session_id, bad)),
            Err(SessionModelError::InvalidBudget(_))
        ));
    }
    assert!(matches!(
        fixture.read(|connection| continuation_context(connection, "missing", 64)),
        Err(SessionModelError::SessionNotFound(_))
    ));

    let empty_session = fixture.session("project-1");
    let empty = fixture
        .read(|connection| continuation_context(connection, &empty_session, 64))
        .expect("empty context");
    assert!(empty.is_empty());
    assert_eq!(empty.used_bytes, 0);
    assert!(!empty.truncated);

    let default_budget = fixture
        .read(|connection| {
            continuation_context(connection, &session_id, DEFAULT_CONTINUATION_BUDGET_BYTES)
        })
        .expect("default budget");
    assert_eq!(default_budget.used_bytes, 4);
}

#[test]
fn continuation_context_truncates_on_utf8_boundaries() {
    let fixture = Fixture::new();
    let session_id = fixture.session("project-1");
    let run = fixture.run("project-1");
    fixture
        .read(|connection| attach_run(connection, &session_id, &run.to_string()))
        .expect("attach");
    // "héllo" is 6 UTF-8 bytes; é is 2 bytes.
    fixture
        .store
        .append_message(run, ChatRole::User, "héllo")
        .expect("multi-byte message");

    let one = fixture
        .read(|connection| continuation_context(connection, &session_id, 1))
        .expect("budget 1");
    assert_eq!(one.used_bytes, 1);
    assert_eq!(one.items[0].text, "h");
    assert!(one.items[0].truncated);

    let two = fixture
        .read(|connection| continuation_context(connection, &session_id, 2))
        .expect("budget 2 (inside é)");
    assert_eq!(two.used_bytes, 1);
    assert_eq!(two.items[0].text, "h");
    assert!(two.items[0].truncated);

    let three = fixture
        .read(|connection| continuation_context(connection, &session_id, 3))
        .expect("budget 3");
    assert_eq!(three.items[0].text, "hé");
    assert_eq!(three.used_bytes, 3);
    assert!(three.items[0].truncated);

    let six = fixture
        .read(|connection| continuation_context(connection, &session_id, 6))
        .expect("budget 6");
    assert_eq!(six.items[0].text, "héllo");
    assert!(!six.items[0].truncated);
}

#[test]
fn continuation_decision_accepts_failed_and_interrupted_and_refuses_live_work() {
    let fixture = Fixture::new();
    let session_id = fixture.session("project-1");
    let cases = [
        (
            AgentState::Failed,
            ContinueDecision::ContinueSession,
            ContinueReason::FailedRunContinuable,
        ),
        (
            AgentState::Interrupted,
            ContinueDecision::ContinueSession,
            ContinueReason::InterruptedRunContinuable,
        ),
        (
            AgentState::Running,
            ContinueDecision::Refuse,
            ContinueReason::RunStillActive,
        ),
        (
            AgentState::Queued,
            ContinueDecision::Refuse,
            ContinueReason::RunStillActive,
        ),
        (
            AgentState::AwaitingApproval,
            ContinueDecision::Refuse,
            ContinueReason::ApprovalPending,
        ),
        (
            AgentState::Completed,
            ContinueDecision::StartNewRun,
            ContinueReason::RunCompleted,
        ),
        (
            AgentState::Cancelled,
            ContinueDecision::StartNewRun,
            ContinueReason::RunCancelled,
        ),
        (
            AgentState::Killed,
            ContinueDecision::StartNewRun,
            ContinueReason::RunKilled,
        ),
    ];
    for (state, expected_decision, expected_reason) in cases {
        let run = fixture.run("project-1");
        fixture
            .store
            .set_run_status(run, state, None)
            .expect("set status");
        fixture
            .read(|connection| attach_run(connection, &session_id, &run.to_string()))
            .expect("attach");
        let assessment = fixture
            .read(|connection| assess_continuation(connection, &session_id, &run.to_string()))
            .expect("assessment");
        assert_eq!(assessment.status, state);
        assert_eq!(assessment.decision, expected_decision, "state {state}");
        assert_eq!(assessment.reason, expected_reason, "state {state}");
        assert_eq!(
            assessment.allows_continuation(),
            expected_decision != ContinueDecision::Refuse
        );
        assert_eq!(assessment.reason_code(), expected_reason.code());
    }

    // Archived sessions refuse even a failed run.
    let archived_session = fixture.session("project-1");
    let failed = fixture.run("project-1");
    fixture
        .store
        .set_run_status(failed, AgentState::Failed, None)
        .expect("fail");
    fixture
        .read(|connection| attach_run(connection, &archived_session, &failed.to_string()))
        .expect("attach failed");
    fixture
        .store
        .set_session_archived(&archived_session, true)
        .expect("archive");
    let archived = fixture
        .read(|connection| assess_continuation(connection, &archived_session, &failed.to_string()))
        .expect("archived assessment");
    assert_eq!(archived.decision, ContinueDecision::Refuse);
    assert_eq!(archived.reason, ContinueReason::SessionArchived);

    // Identity problems are errors, not assessments.
    let other_session = fixture.session("project-1");
    assert!(matches!(
        fixture.read(|connection| {
            assess_continuation(connection, &other_session, &failed.to_string())
        }),
        Err(SessionModelError::RunNotInSession(_, _))
    ));
    assert!(matches!(
        fixture.read(|connection| assess_continuation(connection, &session_id, "missing-run")),
        Err(SessionModelError::RunNotFound(_))
    ));
    assert!(matches!(
        fixture.read(|connection| assess_continuation(connection, "missing", "missing-run")),
        Err(SessionModelError::SessionNotFound(_))
    ));
}

#[test]
fn latest_resumable_run_prefers_the_newest_failed_or_interrupted_run() {
    let fixture = Fixture::new();
    let session_id = fixture.session("project-1");
    assert_eq!(
        fixture
            .read(|connection| latest_resumable_run(connection, &session_id))
            .expect("empty"),
        None
    );
    let failed = fixture.run("project-1");
    let interrupted = fixture.run("project-1");
    let completed = fixture.run("project-1");
    for run in [failed, interrupted, completed] {
        fixture
            .read(|connection| attach_run(connection, &session_id, &run.to_string()))
            .expect("attach");
    }
    fixture
        .store
        .set_run_status(failed, AgentState::Failed, None)
        .expect("fail");
    fixture
        .store
        .set_run_status(interrupted, AgentState::Interrupted, None)
        .expect("interrupt");
    fixture
        .store
        .set_run_status(completed, AgentState::Completed, None)
        .expect("complete");
    // The completed run is newest but not resumable.
    assert_eq!(
        fixture
            .read(|connection| latest_resumable_run(connection, &session_id))
            .expect("resumable"),
        Some(interrupted.to_string())
    );
    assert!(matches!(
        fixture.read(|connection| latest_resumable_run(connection, "missing")),
        Err(SessionModelError::SessionNotFound(_))
    ));
}

#[test]
fn session_model_caps_the_run_ledger_and_pending_summary() {
    let fixture = Fixture::new();
    let session_id = fixture.session("project-1");
    let mut ids = Vec::new();
    for _ in 0..(MAX_SESSION_RUNS + 1) {
        let run = fixture
            .store
            .create_run_scoped(
                "project-1",
                "prompt",
                AgentState::Completed,
                Some(session_id.as_str()),
                PermissionPolicy::Classic,
            )
            .expect("create scoped run");
        ids.push(run.id.to_string());
    }
    let model = fixture
        .read(|connection| session_run_model(connection, &session_id))
        .expect("model");
    assert_eq!(model.run_count, MAX_SESSION_RUNS + 1);
    assert!(model.runs_truncated);
    assert_eq!(model.run_ids.len(), MAX_SESSION_RUNS);
    assert_eq!(model.run_ids.first(), Some(&ids[1]));
    assert_eq!(model.run_ids.last(), Some(&ids[MAX_SESSION_RUNS]));
    assert_eq!(model.runs[0].ordinal, 2);

    // Pending summary: 13 open todos (10 listed) and an over-long todo text.
    let long_text = "x".repeat(MAX_PENDING_TODO_TEXT_BYTES + 50);
    fixture
        .store
        .add_todo(&session_id, &long_text)
        .expect("long todo");
    for index in 0..12 {
        fixture
            .store
            .add_todo(&session_id, &format!("todo {index}"))
            .expect("todo");
    }
    let model = fixture
        .read(|connection| session_run_model(connection, &session_id))
        .expect("model with todos");
    assert_eq!(model.pending_work.open_todo_count, 13);
    assert_eq!(model.pending_work.open_todos.len(), MAX_PENDING_TODOS);
    assert!(model.pending_work.open_todos_truncated);
    let long = model
        .pending_work
        .open_todos
        .iter()
        .find(|todo| todo.truncated)
        .expect("truncated todo listed");
    assert_eq!(long.text.len(), MAX_PENDING_TODO_TEXT_BYTES);
}
