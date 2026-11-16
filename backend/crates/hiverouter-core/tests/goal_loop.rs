//! Bounded goal-loop state-machine tests (real SQLite, no sleeps).
//!
//! `src/goal_loop.rs` is compiled through `#[path]` because this work package
//! owns module files only and must not edit `lib.rs`. The `db` shim exposes the
//! `crate::db::Migration` type used by the migration helper.

#![allow(dead_code)]

mod db {
    pub use hiverouter_core::db::*;
}

#[path = "../src/goal_loop.rs"]
mod goal_loop;

use hiverouter_contract::{RunId, Timestamp};
use rusqlite::Connection;

use goal_loop::{
    begin_continuation, complete_goal, create_goal, get_goal, goal_audit, goal_continuations,
    goal_loop_migration, goal_rounds, pause_goal, record_round, record_self_edit_proposal,
    resume_goal, ContinuationDecision, GoalError, GoalState, NewGoal, ResumeOutcome, RoundOutcome,
    RoundStatus, StopReason, GOAL_LOOP_SCHEMA, SELF_EDIT_KIND,
};

fn at(seconds: u64) -> Timestamp {
    Timestamp::from_unix_secs(seconds)
}

fn connection() -> Connection {
    let connection = Connection::open_in_memory().expect("in-memory sqlite");
    connection
        .execute_batch("PRAGMA foreign_keys = ON;")
        .expect("foreign keys");
    connection
        .execute_batch(GOAL_LOOP_SCHEMA)
        .expect("goal schema");
    connection
}

fn goal(
    connection: &Connection,
    max_rounds: u32,
    max_calls: u32,
    interval: u64,
) -> goal_loop::GoalLoop {
    create_goal(
        connection,
        &NewGoal::new(
            RunId::new(),
            "bounded goal",
            max_rounds,
            max_calls,
            interval,
        ),
        at(1_000),
    )
    .expect("goal")
}

fn begin(connection: &Connection, goal_id: &str, seconds: u64) -> ContinuationDecision {
    begin_continuation(connection, goal_id, at(seconds)).expect("begin continuation")
}

fn record(
    connection: &Connection,
    goal_id: &str,
    round: u32,
    outcome: &RoundOutcome,
    seconds: u64,
) -> goal_loop::RoundReport {
    record_round(connection, goal_id, round, outcome, at(seconds)).expect("record round")
}

#[test]
fn migration_helper_sql_is_accepted_and_has_an_inverse() {
    let connection = Connection::open_in_memory().expect("sqlite");
    let migration = goal_loop_migration(33);
    assert_eq!(migration.name, "bounded_goal_loop");
    connection
        .execute_batch(migration.up)
        .expect("forward migration");
    let table_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'goal_loops'",
            [],
            |row| row.get(0),
        )
        .expect("table count");
    assert_eq!(table_count, 1);
    connection
        .execute_batch(migration.down)
        .expect("inverse migration");
    let table_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'goal_loops'",
            [],
            |row| row.get(0),
        )
        .expect("table count");
    assert_eq!(table_count, 0);
}

#[test]
fn duplicate_delivery_yields_one_turn_and_one_round() {
    let connection = connection();
    let goal = goal(&connection, 8, 8, 60);

    let first = begin(&connection, &goal.goal_id, 1_000);
    let ContinuationDecision::Continue { round, .. } = &first else {
        panic!("first continuation must start: {first:?}");
    };
    assert_eq!(round.round, 1);
    assert_eq!(
        round.marker,
        goal_loop::round_marker(&goal.goal_id, 1),
        "the round marker must be deterministic per goal/round"
    );

    // Same turn delivered twice: the second request creates no second round and
    // no second continuation row.
    let duplicate = begin(&connection, &goal.goal_id, 1_001);
    assert!(
        matches!(
            duplicate,
            ContinuationDecision::Stop {
                reason: StopReason::InFlight { round: 1, .. },
                ..
            }
        ),
        "duplicate delivery must reuse the in-flight turn: {duplicate:?}"
    );
    assert_eq!(
        goal_rounds(&connection, &goal.goal_id)
            .expect("rounds")
            .len(),
        1
    );
    assert_eq!(
        goal_continuations(&connection, &goal.goal_id)
            .expect("continuations")
            .len(),
        1
    );

    // Completing the round does not free the interval.
    let report = record(
        &connection,
        &goal.goal_id,
        1,
        &RoundOutcome::new(1, true),
        1_002,
    );
    assert!(!report.duplicate);
    let too_soon = begin(&connection, &goal.goal_id, 1_030);
    assert!(
        matches!(
            too_soon,
            ContinuationDecision::Stop {
                reason: StopReason::IntervalNotElapsed,
                ..
            }
        ),
        "{too_soon:?}"
    );
    assert_eq!(
        goal_continuations(&connection, &goal.goal_id)
            .expect("continuations")
            .len(),
        1
    );

    // The next interval starts exactly one new round.
    let next = begin(&connection, &goal.goal_id, 1_060);
    let ContinuationDecision::Continue { round, .. } = &next else {
        panic!("next interval must start: {next:?}");
    };
    assert_eq!(round.round, 2);
    assert_ne!(round.marker, goal_loop::round_marker(&goal.goal_id, 1));
    assert_eq!(
        goal_rounds(&connection, &goal.goal_id)
            .expect("rounds")
            .len(),
        2
    );
    assert_eq!(
        goal_continuations(&connection, &goal.goal_id)
            .expect("continuations")
            .len(),
        2
    );
}

#[test]
fn duplicate_round_completion_is_idempotent() {
    let connection = connection();
    let goal = goal(&connection, 8, 8, 10);
    begin(&connection, &goal.goal_id, 1_000);
    let outcome = RoundOutcome::new(2, true);
    let first = record(&connection, &goal.goal_id, 1, &outcome, 1_001);
    assert_eq!(first.goal.calls_used, 2);
    let replay = record(&connection, &goal.goal_id, 1, &outcome, 1_002);
    assert!(replay.duplicate);
    assert_eq!(
        replay.goal.calls_used, 2,
        "replay must not count calls twice"
    );
    assert_eq!(replay.round.status, RoundStatus::Completed);
}

#[test]
fn round_budget_stops_before_creating_a_round() {
    let connection = connection();
    let goal = goal(&connection, 1, 10, 10);
    begin(&connection, &goal.goal_id, 1_000);
    record(
        &connection,
        &goal.goal_id,
        1,
        &RoundOutcome::new(0, true),
        1_001,
    );
    let stopped = begin(&connection, &goal.goal_id, 1_100);
    assert!(
        matches!(
            stopped,
            ContinuationDecision::Stop {
                reason: StopReason::RoundBudgetExhausted,
                ..
            }
        ),
        "{stopped:?}"
    );
    assert_eq!(
        get_goal(&connection, &goal.goal_id)
            .expect("goal")
            .stop_reason
            .as_deref(),
        Some("round_budget_exhausted")
    );
    let audit = goal_audit(&connection, &goal.goal_id).expect("audit");
    assert!(audit
        .iter()
        .any(|event| event.action == "continuation_stopped"));
}

#[test]
fn call_budget_is_enforced_and_cannot_be_overspent() {
    let connection = connection();
    let goal = goal(&connection, 8, 1, 10);
    begin(&connection, &goal.goal_id, 1_000);
    let over = record_round(
        &connection,
        &goal.goal_id,
        1,
        &RoundOutcome::new(2, true),
        at(1_001),
    )
    .expect_err("a round may not overspend the goal budget");
    assert!(
        matches!(
            over,
            GoalError::CallBudgetExceeded {
                used: 0,
                requested: 2,
                max: 1
            }
        ),
        "{over:?}"
    );
    let round = goal_loop::get_round(&connection, &goal.goal_id, 1).expect("round");
    assert_eq!(
        round.status,
        RoundStatus::InFlight,
        "the refused record must not complete the round"
    );

    record(
        &connection,
        &goal.goal_id,
        1,
        &RoundOutcome::new(1, true),
        1_001,
    );
    let stopped = begin(&connection, &goal.goal_id, 1_100);
    assert!(
        matches!(
            stopped,
            ContinuationDecision::Stop {
                reason: StopReason::CallBudgetExhausted,
                ..
            }
        ),
        "{stopped:?}"
    );
}

#[test]
fn paused_goal_refuses_continuation_until_resumed() {
    let connection = connection();
    let goal = goal(&connection, 8, 8, 10);
    pause_goal(&connection, &goal.goal_id, at(1_001), "operator:test").expect("pause");
    let paused = begin(&connection, &goal.goal_id, 1_002);
    assert!(
        matches!(
            paused,
            ContinuationDecision::Stop {
                reason: StopReason::Paused,
                ..
            }
        ),
        "{paused:?}"
    );
    assert_eq!(
        goal_rounds(&connection, &goal.goal_id)
            .expect("rounds")
            .len(),
        0
    );

    let report =
        resume_goal(&connection, &goal.goal_id, at(1_003), "operator:test").expect("resume");
    assert_eq!(report.outcome, ResumeOutcome::Resumed);
    assert_eq!(report.goal.state, GoalState::Active);
    assert!(matches!(
        begin(&connection, &goal.goal_id, 1_003),
        ContinuationDecision::Continue { .. }
    ));
    let audit = goal_audit(&connection, &goal.goal_id).expect("audit");
    assert!(audit.iter().any(|event| event.action == "goal_paused"));
    assert!(audit.iter().any(|event| event.action == "goal_resumed"));
}

#[test]
fn completed_goal_is_terminal() {
    let connection = connection();
    let goal = goal(&connection, 8, 8, 10);
    let completed = complete_goal(
        &connection,
        &goal.goal_id,
        at(1_001),
        "operator:test",
        "objective met",
    )
    .expect("complete");
    assert_eq!(completed.state, GoalState::Completed);
    assert_eq!(completed.stop_reason.as_deref(), Some("completed"));
    assert!(matches!(
        begin(&connection, &goal.goal_id, 1_002),
        ContinuationDecision::Stop {
            reason: StopReason::Completed,
            ..
        }
    ));
    assert!(matches!(
        resume_goal(&connection, &goal.goal_id, at(1_003), "operator:test"),
        Err(GoalError::InvalidTransition {
            from: GoalState::Completed,
            ..
        })
    ));
}

#[test]
fn three_blocked_rounds_block_the_goal() {
    let connection = connection();
    let goal = goal(&connection, 8, 8, 10);
    for index in 0..3_u32 {
        let start = 1_000 + u64::from(index) * 10;
        begin(&connection, &goal.goal_id, start);
        record(
            &connection,
            &goal.goal_id,
            index + 1,
            &RoundOutcome::new(0, true)
                .blocked()
                .with_detail("blocked on input"),
            start + 1,
        );
    }
    let blocked = get_goal(&connection, &goal.goal_id).expect("goal");
    assert_eq!(blocked.state, GoalState::Blocked);
    assert_eq!(blocked.stop_reason.as_deref(), Some("blocked_after_three"));
    assert_eq!(blocked.blocked_rounds, 3);
    assert!(matches!(
        begin(&connection, &goal.goal_id, 1_100),
        ContinuationDecision::Stop {
            reason: StopReason::Blocked,
            ..
        }
    ));
    assert!(matches!(
        complete_goal(
            &connection,
            &goal.goal_id,
            at(1_101),
            "operator:test",
            "close blocked goal"
        ),
        Err(GoalError::InvalidTransition {
            from: GoalState::Blocked,
            ..
        })
    ));
    let audit = goal_audit(&connection, &goal.goal_id).expect("audit");
    assert!(audit.iter().any(|event| event.action == "goal_blocked"));
}

#[test]
fn three_no_progress_rounds_persist_the_marker_and_block() {
    let connection = connection();
    let goal = goal(&connection, 8, 8, 10);
    for index in 0..3_u32 {
        let start = 1_000 + u64::from(index) * 10;
        begin(&connection, &goal.goal_id, start);
        record(
            &connection,
            &goal.goal_id,
            index + 1,
            &RoundOutcome::new(0, false),
            start + 1,
        );
    }
    let rounds = goal_rounds(&connection, &goal.goal_id).expect("rounds");
    assert!(rounds.iter().all(goal_loop::GoalRound::no_progress));
    let blocked = get_goal(&connection, &goal.goal_id).expect("goal");
    assert_eq!(blocked.state, GoalState::Blocked);
    assert_eq!(
        blocked.stop_reason.as_deref(),
        Some("no_progress_after_three")
    );
}

#[test]
fn crash_resume_reuses_the_inflight_marker_without_duplication() {
    let directory = tempfile::tempdir().expect("tempdir");
    let path = directory.path().join("goals.db");

    let goal_id;
    let marker;
    {
        let connection = Connection::open(&path).expect("sqlite");
        connection
            .execute_batch("PRAGMA foreign_keys = ON;")
            .expect("fk");
        connection.execute_batch(GOAL_LOOP_SCHEMA).expect("schema");
        let goal = create_goal(
            &connection,
            &NewGoal::new(RunId::new(), "crash resume", 8, 8, 60),
            at(1_000),
        )
        .expect("goal");
        goal_id = goal.goal_id;
        let decision = begin(&connection, &goal_id, 1_000);
        let ContinuationDecision::Continue { round, .. } = decision else {
            panic!("continuation must start");
        };
        marker = round.marker;
        // Drop the connection without recording the round: the process crashed.
    }

    let connection = Connection::open(&path).expect("reopen");
    connection
        .execute_batch("PRAGMA foreign_keys = ON;")
        .expect("fk");
    let report =
        resume_goal(&connection, &goal_id, at(1_010), "operator:test").expect("resume after crash");
    assert_eq!(
        report.outcome,
        ResumeOutcome::InFlight {
            round: 1,
            marker: marker.clone()
        },
        "resume must reattach to the same marker"
    );
    assert_eq!(goal_rounds(&connection, &goal_id).expect("rounds").len(), 1);
    assert_eq!(
        goal_continuations(&connection, &goal_id)
            .expect("continuations")
            .len(),
        1
    );

    record(&connection, &goal_id, 1, &RoundOutcome::new(1, true), 1_010);
    let next = begin(&connection, &goal_id, 1_060);
    let ContinuationDecision::Continue { round, .. } = next else {
        panic!("next interval must continue");
    };
    assert_eq!(round.round, 2);
    assert_ne!(round.marker, marker);
    assert_eq!(goal_rounds(&connection, &goal_id).expect("rounds").len(), 2);
}

#[test]
fn expired_inflight_lease_is_abandoned_and_reclaimed() {
    let directory = tempfile::tempdir().expect("tempdir");
    let path = directory.path().join("goals.db");
    let goal_id;
    {
        let connection = Connection::open(&path).expect("sqlite");
        connection
            .execute_batch("PRAGMA foreign_keys = ON;")
            .expect("fk");
        connection.execute_batch(GOAL_LOOP_SCHEMA).expect("schema");
        goal_id = create_goal(
            &connection,
            &NewGoal::new(RunId::new(), "expired lease", 8, 8, 60),
            at(1_000),
        )
        .expect("goal")
        .goal_id;
        begin(&connection, &goal_id, 1_000);
    }
    let connection = Connection::open(&path).expect("reopen");
    connection
        .execute_batch("PRAGMA foreign_keys = ON;")
        .expect("fk");
    let report = resume_goal(
        &connection,
        &goal_id,
        at(1_000 + goal_loop::IN_FLIGHT_LEASE_SECS + 1),
        "operator:test",
    )
    .expect("resume");
    assert_eq!(
        report.outcome,
        ResumeOutcome::Reclaimed { abandoned_round: 1 }
    );
    let round = goal_loop::get_round(&connection, &goal_id, 1).expect("round");
    assert_eq!(round.status, RoundStatus::Abandoned);
    let audit = goal_audit(&connection, &goal_id).expect("audit");
    assert!(audit.iter().any(|event| event.action == "round_abandoned"));
}

#[test]
fn self_edit_proposal_is_persisted_but_never_applied_or_merged() {
    let connection = connection();
    let goal = goal(&connection, 8, 8, 10);
    begin(&connection, &goal.goal_id, 1_000);
    record(
        &connection,
        &goal.goal_id,
        1,
        &RoundOutcome::new(0, true),
        1_001,
    );
    let receipt = record_self_edit_proposal(
        &connection,
        &goal.goal_id,
        1,
        "Replace the duplicate parser branch with the shared helper.",
        at(1_002),
    )
    .expect("proposal");
    assert_eq!(receipt.kind, SELF_EDIT_KIND);
    assert!(!receipt.applied);
    assert!(receipt.requires_operator_approval);
    assert!(!receipt.merge_performed);
    assert!(!receipt.approval_bypassed);

    let (applied, requires): (i64, i64) = connection
        .query_row(
            "SELECT applied, requires_operator_approval FROM goal_proposals
             WHERE proposal_id = ?1",
            rusqlite::params![receipt.proposal_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("proposal row");
    assert_eq!(applied, 0, "schema and code must keep applied = 0");
    assert_eq!(requires, 1);
    assert_eq!(
        get_goal(&connection, &goal.goal_id).expect("goal").state,
        GoalState::Active,
        "a proposal must not complete or block the goal"
    );
    let audit = goal_audit(&connection, &goal.goal_id).expect("audit");
    let proposal_event = audit
        .iter()
        .find(|event| event.action == "self_edit_proposed")
        .expect("proposal audit");
    assert!(proposal_event.detail_json.contains("\"applied\":false"));
    assert!(proposal_event
        .detail_json
        .contains("\"requires_operator_approval\":true"));
    assert!(proposal_event
        .detail_json
        .contains("\"approval_bypassed\":false"));
}
