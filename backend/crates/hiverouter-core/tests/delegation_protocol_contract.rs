//! Delegation-protocol contract tests: the CEO -> manager -> team lead ->
//! assistant tree, its bounds, its bottom-up result ledger, and the root
//! per-tree digest.
//!
//! The module is included from `src/` rather than re-exported by `lib.rs`
//! because this change owns only `delegation_protocol.rs` and test files; when
//! `lib.rs` gains `pub mod delegation_protocol;` these tests can switch to
//! `use hiverouter_core::delegation_protocol::*` with no other edit.

#[path = "../src/delegation_protocol.rs"]
mod delegation_protocol;

use delegation_protocol::{
    decompose, synthesize_batch, DelegationLimits, DelegationTree, ReportLedger, Seat, SeatReport,
    SeatSpec, SeatStatus, SeatTier, MAX_DELEGATION_FANOUT,
};
use hiverouter_contract::{Refusal, TaskId};
use hiverouter_tools::{ToolId, ToolRegistry};

fn registry() -> ToolRegistry {
    ToolRegistry::standard()
}

fn limits() -> DelegationLimits {
    DelegationLimits::default()
}

fn tools_for(tier: SeatTier) -> Vec<ToolId> {
    match tier {
        SeatTier::Ceo => Vec::new(),
        SeatTier::Manager => vec![ToolId::FileRead, ToolId::ReportResult],
        SeatTier::TeamLead => vec![ToolId::FileRead, ToolId::FileWrite],
        SeatTier::Assistant => vec![ToolId::FileRead, ToolId::GitDiff],
    }
}

/// CEO -> manager -> team lead -> assistant, using the real tier tool lists.
fn four_seat_tree() -> DelegationTree {
    let ceo = Seat::root(
        TaskId::new(),
        "Deliver the delegation protocol without over-spawning the team.",
    );
    let manager = Seat::under(
        &ceo,
        TaskId::new(),
        "Own the backend track and report the verified outcome.",
        tools_for(SeatTier::Manager),
        Vec::new(),
    )
    .expect("manager");
    let lead = Seat::under(
        &manager,
        TaskId::new(),
        "Ship the protocol with tests and observed evidence.",
        tools_for(SeatTier::TeamLead),
        Vec::new(),
    )
    .expect("lead");
    let assistant = Seat::under(
        &lead,
        TaskId::new(),
        "Run the delegation suite and report the observed result.",
        tools_for(SeatTier::Assistant),
        Vec::new(),
    )
    .expect("assistant");
    DelegationTree::new(vec![ceo, manager, lead, assistant])
}

fn seat_ids(tree: &DelegationTree) -> (TaskId, TaskId, TaskId, TaskId) {
    let seats = tree.seats();
    assert_eq!(seats.len(), 4, "helper expects the four-seat tree");
    (
        seats[0].seat_id,
        seats[1].seat_id,
        seats[2].seat_id,
        seats[3].seat_id,
    )
}

fn successful_reports(
    ceo: TaskId,
    manager: TaskId,
    lead: TaskId,
    assistant: TaskId,
) -> Vec<SeatReport> {
    vec![
        SeatReport::completed(ceo, "The release is ready and verified."),
        SeatReport::completed(manager, "The backend track finished."),
        SeatReport::completed(lead, "The protocol shipped with tests."),
        SeatReport::completed(assistant, "The delegation suite passed."),
    ]
}

#[test]
fn happy_path_bottom_up_synthesis_produces_root_digest() {
    let registry = registry();
    let limits = limits();
    let tree = four_seat_tree();
    let (ceo, manager, lead, assistant) = seat_ids(&tree);
    tree.validate(&registry, &limits).expect("valid tree");

    let mut ledger = ReportLedger::new(tree.clone(), &registry, &limits).expect("ledger");
    // Before any result, every seat is pending in depth order (root first).
    assert_eq!(ledger.pending_seats(), vec![ceo, manager, lead, assistant]);

    ledger
        .submit(SeatReport::completed(assistant, "The delegation suite passed."))
        .expect("assistant report");
    ledger
        .submit(SeatReport::completed(lead, "The protocol shipped with tests."))
        .expect("lead report");
    ledger
        .submit(SeatReport::completed(manager, "The backend track finished."))
        .expect("manager report");
    ledger
        .submit(SeatReport::completed(ceo, "The release is ready and verified."))
        .expect("ceo report");
    assert!(ledger.is_complete());

    let synthesis = ledger.synthesize().expect("synthesis");
    assert!(synthesis.succeeded);
    assert_eq!(synthesis.tree_root, ceo);
    assert_eq!(synthesis.counts.total, 4);
    assert_eq!(synthesis.counts.completed, 4);
    assert_eq!(synthesis.counts.failed, 0);
    assert_eq!(synthesis.counts.blocked, 0);
    // Deepest seat first: bottom-up synthesis, CEO last.
    assert_eq!(synthesis.entries.first().expect("entry").seat_id, assistant);
    assert_eq!(synthesis.entries.last().expect("entry").seat_id, ceo);
    assert_eq!(synthesis.root_digest.len(), 64);
    assert!(synthesis
        .root_digest
        .chars()
        .all(|character| character.is_ascii_hexdigit()));
    assert!(synthesis.summary.contains("Only Completed seats count as success"));
    assert!(synthesis.summary.contains(&synthesis.root_digest));

    // The same tree and results in any arrival order produce one digest.
    let (ceo, manager, lead, assistant) = seat_ids(&tree);
    let reversed = successful_reports(ceo, manager, lead, assistant)
        .into_iter()
        .rev()
        .collect();
    let again = synthesize_batch(tree, &registry, &limits, reversed).expect("batch synthesis");
    assert_eq!(again.root_digest, synthesis.root_digest);
    assert_eq!(again.summary, synthesis.summary);
}

#[test]
fn fanout_over_the_cap_is_refused() {
    let registry = registry();
    let limits = limits();
    let ceo = Seat::root(TaskId::new(), "Deliver a bounded set of artifacts.");

    let spec = |index: usize| SeatSpec {
        seat_id: TaskId::new(),
        brief: format!("Deliver bounded artifact number {index} with a test."),
        tools: vec![ToolId::FileRead],
        depends_on: Vec::new(),
    };
    let at_cap: Vec<SeatSpec> = (0..MAX_DELEGATION_FANOUT).map(spec).collect();
    decompose(&ceo, &at_cap, &registry, &limits).expect("eight children are allowed");

    let over_cap: Vec<SeatSpec> = (0..=MAX_DELEGATION_FANOUT).map(spec).collect();
    let error = decompose(&ceo, &over_cap, &registry, &limits).expect_err("nine is too many");
    assert_eq!(error.refusal(), Refusal::FanoutLimit);

    // The tree validator refuses the same shape even if a caller bypasses
    // `decompose` and assembles seats directly.
    let mut seats = vec![ceo.clone()];
    for index in 0..=MAX_DELEGATION_FANOUT {
        seats.push(
            Seat::under(
                &ceo,
                TaskId::new(),
                format!("Deliver bounded artifact number {index} with a test."),
                Vec::new(),
                Vec::new(),
            )
            .expect("child seat"),
        );
    }
    let tree = DelegationTree::new(seats);
    let error = tree.validate(&registry, &limits).expect_err("fanout");
    assert_eq!(error.refusal(), Refusal::FanoutLimit);
}

#[test]
fn missing_brief_is_refused() {
    let registry = registry();
    let limits = limits();
    let ceo = Seat::root(TaskId::new(), "Deliver one bounded artifact.");

    for brief in [String::new(), "   ".to_owned(), "\n\t".to_owned()] {
        let specs = vec![SeatSpec {
            seat_id: TaskId::new(),
            brief,
            tools: Vec::new(),
            depends_on: Vec::new(),
        }];
        let error = decompose(&ceo, &specs, &registry, &limits).expect_err("missing brief");
        assert_eq!(error.refusal(), Refusal::SchemaInvalid);
        assert!(error.detail().contains("brief"), "{}", error.detail());
    }

    // A brief over the bound is refused with the observed size, never truncated.
    let long = "Deliver ".to_owned() + &"detail ".repeat(200);
    let specs = vec![SeatSpec {
        seat_id: TaskId::new(),
        brief: long,
        tools: Vec::new(),
        depends_on: Vec::new(),
    }];
    let error = decompose(&ceo, &specs, &registry, &limits).expect_err("long brief");
    assert_eq!(error.refusal(), Refusal::SchemaInvalid);
    assert!(error.detail().contains("cap"), "{}", error.detail());
}

#[test]
fn duplicate_result_is_refused() {
    let registry = registry();
    let limits = limits();
    let tree = four_seat_tree();
    let (_, _, _, assistant) = seat_ids(&tree);
    let mut ledger = ReportLedger::new(tree.clone(), &registry, &limits).expect("ledger");

    let report = SeatReport::completed(assistant, "The suite passed.");
    ledger.submit(report.clone()).expect("first report");
    let error = ledger.submit(report).expect_err("second report");
    assert_eq!(error.refusal(), Refusal::SchemaInvalid);
    assert!(error.detail().contains("duplicate"), "{}", error.detail());

    // A result for a seat that is not in the tree is a named missing task.
    let unknown = SeatReport::completed(TaskId::new(), "not part of this tree");
    let error = ledger.submit(unknown).expect_err("unknown seat");
    assert_eq!(error.refusal(), Refusal::TaskNotFound);

    // The batch entry point refuses duplicates the same way.
    let (ceo, manager, lead, assistant) = seat_ids(&tree);
    let mut reports = successful_reports(ceo, manager, lead, assistant);
    reports.push(SeatReport::completed(assistant, "duplicate"));
    let error = synthesize_batch(tree, &registry, &limits, reports).expect_err("duplicate");
    assert_eq!(error.refusal(), Refusal::SchemaInvalid);
}

#[test]
fn blocked_child_is_not_counted_as_success() {
    let registry = registry();
    let limits = limits();
    let tree = four_seat_tree();
    let (ceo, manager, lead, assistant) = seat_ids(&tree);
    // Phase 1: a blocked child forbids every Completed ancestor.
    let mut ledger = ReportLedger::new(tree.clone(), &registry, &limits).expect("ledger");
    ledger
        .submit(SeatReport::blocked(assistant, "The test runner is missing."))
        .expect("assistant blocked");
    let error = ledger
        .submit(SeatReport::completed(lead, "Everything is done."))
        .expect_err("completed over a blocked child");
    assert_eq!(error.refusal(), Refusal::SchemaInvalid);
    assert!(error.detail().contains("Completed"), "{}", error.detail());
    ledger
        .submit(
            SeatReport::blocked(lead, "Its assistant could not run the suite.")
                .with_refusal(Refusal::ToolNotFound),
        )
        .expect("lead blocked");
    let error = ledger
        .submit(SeatReport::completed(manager, "Ship it."))
        .expect_err("manager cannot complete over a blocked child");
    assert_eq!(error.refusal(), Refusal::SchemaInvalid);
    ledger
        .submit(SeatReport::blocked(manager, "The lead could not run the suite."))
        .expect("manager blocked");
    let error = ledger
        .submit(SeatReport::completed(ceo, "Release is ready."))
        .expect_err("CEO cannot complete over a blocked child");
    assert_eq!(error.refusal(), Refusal::SchemaInvalid);
    ledger
        .submit(SeatReport::blocked(ceo, "One team is blocked; the release is not ready."))
        .expect("ceo blocked");

    let synthesis = ledger.synthesize().expect("synthesis");
    assert!(!synthesis.succeeded, "blocked work is not success");
    assert_eq!(synthesis.counts.total, 4);
    assert_eq!(synthesis.counts.completed, 0, "nothing completed");
    assert_eq!(synthesis.counts.blocked, 4);
    assert_eq!(synthesis.counts.failed, 0);
    assert!(synthesis.summary.contains("0/4 seats completed"));
    assert!(synthesis.summary.contains("BLOCKED"));
    assert_eq!(synthesis.root_digest.len(), 64);

    // Phase 2: one completed assistant beside a blocked branch is still a
    // failed aggregation, and the completed count never absorbs the blocked work.
    let mut ledger = ReportLedger::new(tree, &registry, &limits).expect("ledger");
    ledger
        .submit(SeatReport::completed(assistant, "One assistant task finished."))
        .expect("assistant report");
    ledger
        .submit(SeatReport::blocked(lead, "The release gate is unavailable."))
        .expect("lead blocked");
    let error = ledger
        .submit(SeatReport::completed(manager, "Ship it."))
        .expect_err("manager cannot complete over a blocked child");
    assert_eq!(error.refusal(), Refusal::SchemaInvalid);
    ledger
        .submit(SeatReport::blocked(manager, "The lead could not finish."))
        .expect("manager blocked");
    let error = ledger
        .submit(SeatReport::completed(ceo, "Release is ready."))
        .expect_err("CEO cannot complete over a blocked child");
    assert_eq!(error.refusal(), Refusal::SchemaInvalid);
    ledger
        .submit(SeatReport::blocked(ceo, "One team is blocked; the release is not ready."))
        .expect("ceo blocked");

    let synthesis = ledger.synthesize().expect("synthesis");
    assert!(!synthesis.succeeded, "blocked work is not success");
    assert_eq!(synthesis.counts.completed, 1, "only the assistant completed");
    assert_eq!(synthesis.counts.blocked, 3);
    assert!(synthesis.summary.contains("1/4 seats completed"));
    assert_eq!(synthesis.root_digest.len(), 64);
}

#[test]
fn failed_child_never_counts_as_success() {
    let registry = registry();
    let limits = limits();
    let tree = four_seat_tree();
    let (ceo, manager, lead, assistant) = seat_ids(&tree);
    let reports = vec![
        SeatReport::failed(assistant, Refusal::ProviderTimeout, "The provider timed out."),
        SeatReport::failed(lead, Refusal::ProviderTimeout, "Its assistant never finished."),
        SeatReport::failed(manager, Refusal::ProviderTimeout, "Its lead never finished."),
        SeatReport::failed(ceo, Refusal::ProviderTimeout, "No branch finished."),
    ];
    let synthesis = synthesize_batch(tree, &registry, &limits, reports).expect("synthesis");
    assert!(!synthesis.succeeded);
    assert_eq!(synthesis.counts.failed, 4);
    assert_eq!(synthesis.counts.completed, 0);
    assert_eq!(synthesis.entries[0].status, SeatStatus::Failed);
    assert_eq!(
        synthesis.entries[0].refusal,
        Some(Refusal::ProviderTimeout)
    );
}

#[test]
fn parent_before_child_is_a_bottom_up_violation() {
    let registry = registry();
    let limits = limits();
    let tree = four_seat_tree();
    let (ceo, _, _, _) = seat_ids(&tree);
    let mut ledger = ReportLedger::new(tree, &registry, &limits).expect("ledger");
    let error = ledger
        .submit(SeatReport::completed(ceo, "Nothing delegated."))
        .expect_err("parent before child");
    assert_eq!(error.refusal(), Refusal::SchemaInvalid);
    assert!(error.detail().contains("bottom-up"), "{}", error.detail());
}

#[test]
fn assistant_seat_may_not_decompose_and_tools_stay_in_tier() {
    let registry = registry();
    let limits = limits();
    let tree = four_seat_tree();
    let assistant = tree
        .seats()
        .iter()
        .find(|seat| seat.tier == SeatTier::Assistant)
        .expect("assistant");
    let spec = SeatSpec {
        seat_id: TaskId::new(),
        brief: "Do more work under the assistant.".to_owned(),
        tools: Vec::new(),
        depends_on: Vec::new(),
    };
    let error = decompose(assistant, &[spec], &registry, &limits).expect_err("no delegation");
    assert_eq!(error.refusal(), Refusal::ToolNotAllowed);

    let lead = tree
        .seats()
        .iter()
        .find(|seat| seat.tier == SeatTier::TeamLead)
        .expect("lead");
    let spec = SeatSpec {
        seat_id: TaskId::new(),
        brief: "Write a file without approval.".to_owned(),
        tools: vec![ToolId::FileWrite],
        depends_on: Vec::new(),
    };
    let error = decompose(lead, &[spec], &registry, &limits).expect_err("tool bound");
    assert_eq!(error.refusal(), Refusal::ToolNotAllowed);
}
