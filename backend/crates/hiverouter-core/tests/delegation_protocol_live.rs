//! Live contract tests for the delegation protocol (`src/delegation_protocol.rs`).
//!
//! These tests use the library module, real planner `PlanNode`s and real
//! `TaskRecord` rows. The result body is supplied through the
//! `TaskResultSource` persistence port, so a rebuild cannot pass by inventing
//! an in-memory success body.

use std::collections::HashMap;
use std::sync::Arc;

use hiverouter_contract::{AgentState, Refusal, RunId, TaskId};
use hiverouter_core::db::Database;
use hiverouter_core::delegation_protocol::{
    synthesize_batch, DelegationBuilder, DelegationLimits, DelegationTree, PlanNodeSeat,
    ReportLedger, Seat, SeatCounts, SeatReport, SeatSpec, SeatStatus, SeatTier, TaskResultSource,
    TaskRow,
};
use hiverouter_core::entities::TaskRecord;
use hiverouter_core::planner::{Plan, PlanNode};
use hiverouter_core::store::RunStore;
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

struct Ids {
    ceo: TaskId,
    manager: TaskId,
    lead: TaskId,
    assistant: TaskId,
}

/// CEO -> manager -> team lead -> assistant, using the real tier tool lists.
fn four_seat_tree() -> (DelegationTree, Ids) {
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
    let ids = Ids {
        ceo: ceo.seat_id,
        manager: manager.seat_id,
        lead: lead.seat_id,
        assistant: assistant.seat_id,
    };
    (
        DelegationTree::new(vec![ceo, manager, lead, assistant]),
        ids,
    )
}

fn task_row(seat: &Seat, run: RunId, state: AgentState, spawn_seq: u64) -> TaskRecord {
    let role = if seat.is_root() {
        // The engine persists the run root as `planner`; `SeatTier::from_label`
        // maps that to the CEO seat.
        "planner".to_owned()
    } else {
        seat.tier.planner_role().to_owned()
    };
    TaskRecord {
        id: seat.seat_id,
        run_id: run,
        parent_task_id: seat.parent_seat_id,
        role,
        depth: seat.depth,
        spawn_seq,
        state,
        rev: 1,
    }
}

#[derive(Debug, Default)]
struct ScriptedResults {
    bodies: HashMap<TaskId, String>,
    refusals: HashMap<TaskId, Refusal>,
}

impl ScriptedResults {
    fn with_body(mut self, task_id: TaskId, body: &str) -> Self {
        self.bodies.insert(task_id, body.to_owned());
        self
    }

    fn with_refusal(mut self, task_id: TaskId, refusal: Refusal) -> Self {
        self.refusals.insert(task_id, refusal);
        self
    }
}

impl TaskResultSource for ScriptedResults {
    fn task_result(&self, task_id: TaskId) -> Result<Option<String>, Refusal> {
        Ok(self.bodies.get(&task_id).cloned())
    }

    fn task_refusal(&self, task_id: TaskId) -> Result<Option<Refusal>, Refusal> {
        Ok(self.refusals.get(&task_id).copied())
    }
}

fn rows_for(
    tree: &DelegationTree,
    run: RunId,
    states: &HashMap<TaskId, AgentState>,
) -> Vec<TaskRecord> {
    tree.seats()
        .iter()
        .enumerate()
        .map(|(index, seat)| {
            let state = states
                .get(&seat.seat_id)
                .copied()
                .unwrap_or(AgentState::Completed);
            task_row(seat, run, state, index as u64 + 1)
        })
        .collect()
}

fn completed_rows_and_results(
    tree: &DelegationTree,
    run: RunId,
    ids: &Ids,
) -> (Vec<TaskRecord>, ScriptedResults) {
    let rows = rows_for(tree, run, &HashMap::new());
    let results = ScriptedResults::default()
        .with_body(ids.assistant, "The delegation suite passed.")
        .with_body(ids.lead, "The protocol shipped with tests.")
        .with_body(ids.manager, "The backend track finished.")
        .with_body(ids.ceo, "The release is ready and verified.");
    (rows, results)
}

#[test]
fn plan_node_objective_becomes_the_seat_brief() {
    let registry = registry();
    let limits = limits();
    let ceo = Seat::root(
        TaskId::new(),
        "Deliver the release without over-spawning the team.",
    );
    let manager_id = TaskId::new();
    let lead_id = TaskId::new();
    let assistant_id = TaskId::new();
    let plan = Plan::new(vec![
        PlanNode {
            node_id: manager_id,
            role: "superagent".to_owned(),
            deps: Vec::new(),
            depth: 1,
            parent: None,
            objective: Some("Own the backend track and report the verified outcome.".to_owned()),
        },
        PlanNode {
            node_id: lead_id,
            role: "agent".to_owned(),
            deps: Vec::new(),
            depth: 2,
            parent: Some(manager_id),
            objective: Some("Ship the protocol with tests and observed evidence.".to_owned()),
        },
        PlanNode {
            node_id: assistant_id,
            role: "subagent".to_owned(),
            deps: Vec::new(),
            depth: 3,
            parent: Some(lead_id),
            objective: Some("Run the delegation suite and report the observed result.".to_owned()),
        },
    ]);
    plan.validate().expect("planner plan is valid");

    // One node mapped directly, with the exact objective as brief.
    let manager = Seat::from_plan_node(
        &ceo,
        &plan.nodes[0],
        tools_for(SeatTier::Manager),
        &registry,
        &limits,
    )
    .expect("manager seat");
    assert_eq!(
        manager.brief,
        plan.nodes[0].objective.clone().expect("objective")
    );
    assert_eq!(manager.tier, SeatTier::Manager);
    assert_eq!(manager.depth, 1);
    assert_eq!(manager.parent_seat_id, Some(ceo.seat_id));

    // A missing objective is refused; there is no whole-run-prompt fallback.
    let mut bare = plan.nodes[0].clone();
    bare.node_id = TaskId::new();
    bare.objective = None;
    let error = Seat::from_plan_node(
        &ceo,
        &bare,
        tools_for(SeatTier::Manager),
        &registry,
        &limits,
    )
    .expect_err("missing objective");
    assert_eq!(error.refusal(), Refusal::SchemaInvalid);
    assert!(
        error.detail().contains("no objective"),
        "{}",
        error.detail()
    );

    // The whole plan maps to one validated delegation tree.
    let tree = DelegationTree::from_plan(ceo, &plan, &registry, &limits)
        .expect("plan becomes a delegation tree");
    for node in &plan.nodes {
        let seat = tree.seat(node.node_id).expect("planned seat");
        assert_eq!(seat.brief, node.objective.clone().expect("objective"));
        assert!(seat.tools.is_empty(), "the plan grants no tools by itself");
    }
    tree.validate(&registry, &limits).expect("validated tree");

    // The explicit view type carries the same fields for callers that cannot
    // depend on `planner::PlanNode`.
    let root = Seat::root(TaskId::new(), "Deliver two bounded artifacts.");
    let view = PlanNodeSeat::new(
        TaskId::new(),
        None,
        "superagent",
        1,
        Some("Own one bounded artifact and report the verified result."),
        &[],
    );
    let from_view = Seat::from_plan_seat(
        &root,
        view,
        tools_for(SeatTier::Manager),
        &registry,
        &limits,
    )
    .expect("view maps");
    assert_eq!(
        from_view.brief,
        "Own one bounded artifact and report the verified result."
    );
}

#[test]
fn report_ledger_rebuilds_from_task_records_and_rejects_fake_success() {
    let registry = registry();
    let limits = limits();
    let run = RunId::new();
    let (tree, ids) = four_seat_tree();
    let (rows, results) = completed_rows_and_results(&tree, run, &ids);

    let ledger = ReportLedger::from_task_records(tree.clone(), &registry, &limits, &rows, &results)
        .expect("rebuilt ledger");
    assert!(ledger.is_complete());
    assert_eq!(ledger.pending_seats(), Vec::<TaskId>::new());
    let synthesis = ledger.synthesize().expect("synthesis");
    assert!(synthesis.succeeded);
    assert_eq!(
        synthesis.counts,
        SeatCounts {
            total: 4,
            completed: 4,
            failed: 0,
            blocked: 0,
            cancelled: 0,
            killed: 0,
        }
    );
    assert_eq!(synthesis.subtree_digests.len(), 4);
    assert_eq!(synthesis.root_digest.len(), 64);
    assert_eq!(
        synthesis.subtree_digest(ids.ceo),
        Some(synthesis.root_digest.as_str())
    );

    // A Completed task with no recorded result is refused, not given a
    // synthetic success body.
    let missing = ScriptedResults::default()
        .with_body(ids.lead, "lead result")
        .with_body(ids.manager, "manager result")
        .with_body(ids.ceo, "root result");
    let error = ReportLedger::from_task_records(tree.clone(), &registry, &limits, &rows, &missing)
        .expect_err("missing completed body");
    assert_eq!(error.refusal(), Refusal::SchemaInvalid);
    assert!(
        error.detail().contains("no recorded result"),
        "{}",
        error.detail()
    );
    assert!(error.detail().contains("invent"), "{}", error.detail());

    // An empty recorded body is equally refused.
    let empty = ScriptedResults::default()
        .with_body(ids.assistant, "")
        .with_body(ids.lead, "lead result")
        .with_body(ids.manager, "manager result")
        .with_body(ids.ceo, "root result");
    let error = ReportLedger::from_task_records(tree.clone(), &registry, &limits, &rows, &empty)
        .expect_err("empty completed body");
    assert_eq!(error.refusal(), Refusal::SchemaInvalid);

    // A still-running task cannot be rebuilt as any terminal outcome.
    let mut running = rows.clone();
    running[3].state = AgentState::Running;
    let error =
        ReportLedger::from_task_records(tree.clone(), &registry, &limits, &running, &results)
            .expect_err("non-terminal row");
    assert_eq!(error.refusal(), Refusal::SchemaInvalid);
    assert!(
        error.detail().contains("not terminal"),
        "{}",
        error.detail()
    );

    // A row outside the tree is not silently dropped.
    let mut extra = rows.clone();
    extra.push(TaskRecord {
        id: TaskId::new(),
        run_id: run,
        parent_task_id: Some(ids.ceo),
        role: "agent".to_owned(),
        depth: 1,
        spawn_seq: 99,
        state: AgentState::Completed,
        rev: 1,
    });
    let error = ReportLedger::from_task_records(tree.clone(), &registry, &limits, &extra, &results)
        .expect_err("extra row");
    assert_eq!(error.refusal(), Refusal::SchemaInvalid);
    assert!(error.detail().contains("not a seat"), "{}", error.detail());

    // The port receives the same row shape as `TaskRow`.
    let views: Vec<TaskRow> = rows
        .iter()
        .map(|row| {
            TaskRow::new(
                row.id,
                row.run_id,
                row.parent_task_id,
                row.role.clone(),
                row.depth,
                row.state,
            )
        })
        .collect();
    ReportLedger::rebuild_from_task_rows(tree, &registry, &limits, &views, &results)
        .expect("public row-shape rebuild");
}

#[test]
fn status_mapping_keeps_killed_and_cancelled_out_of_success() {
    assert_eq!(
        SeatStatus::from_agent_state(AgentState::Completed),
        Some(SeatStatus::Completed)
    );
    assert_eq!(
        SeatStatus::from_agent_state(AgentState::Failed),
        Some(SeatStatus::Failed)
    );
    assert_eq!(
        SeatStatus::from_agent_state(AgentState::Cancelled),
        Some(SeatStatus::Cancelled)
    );
    assert_eq!(
        SeatStatus::from_agent_state(AgentState::Killed),
        Some(SeatStatus::Killed)
    );
    assert_eq!(
        SeatStatus::from_agent_state(AgentState::Interrupted),
        Some(SeatStatus::Cancelled),
        "an interrupted task is not a success"
    );
    assert_eq!(SeatStatus::from_agent_state(AgentState::Running), None);
    assert_eq!(SeatStatus::from_agent_state(AgentState::Queued), None);
    assert_eq!(SeatStatus::from_label("killed"), Some(SeatStatus::Killed));
    assert_eq!(SeatStatus::from_label("done"), None);
    assert!(SeatStatus::Completed.is_success());
    for status in [
        SeatStatus::Failed,
        SeatStatus::Blocked,
        SeatStatus::Cancelled,
        SeatStatus::Killed,
    ] {
        assert!(!status.is_success(), "{} is not success", status.label());
    }
}

#[test]
fn blocked_child_and_parent_claim_from_rows_are_refused() {
    let registry = registry();
    let limits = limits();
    let run = RunId::new();
    let (tree, ids) = four_seat_tree();

    // Live path: a blocked child keeps its parent from claiming Completed.
    let mut ledger = ReportLedger::new(tree.clone(), &registry, &limits).expect("ledger");
    ledger
        .submit(SeatReport::blocked(
            ids.assistant,
            "The delegation test runner is missing.",
        ))
        .expect("assistant blocked");
    let error = ledger
        .submit(SeatReport::completed(ids.lead, "Everything is done."))
        .expect_err("completed over blocked child");
    assert_eq!(error.refusal(), Refusal::SchemaInvalid);
    assert!(error.detail().contains("Completed"), "{}", error.detail());

    // Rebuilt path: the durable rows encode the same lie; the rebuild refuses.
    let states = HashMap::from([(ids.assistant, AgentState::Failed)]);
    let rows = rows_for(&tree, run, &states);
    let results = ScriptedResults::default()
        .with_body(ids.assistant, "BLOCKED: tool not available")
        .with_refusal(ids.assistant, Refusal::ToolNotFound)
        .with_body(ids.lead, "Everything is done.")
        .with_body(ids.manager, "The backend track finished.")
        .with_body(ids.ceo, "The release is ready.");
    let error = ReportLedger::from_task_records(tree, &registry, &limits, &rows, &results)
        .expect_err("completed parent over failed child");
    assert_eq!(error.refusal(), Refusal::SchemaInvalid);
    assert!(
        error.detail().contains("cannot be Completed"),
        "{}",
        error.detail()
    );
    assert!(error.detail().contains("failed"), "{}", error.detail());
}

#[test]
fn duplicate_result_and_parent_before_child_are_refused() {
    let registry = registry();
    let limits = limits();
    let (tree, ids) = four_seat_tree();

    let mut ledger = ReportLedger::new(tree.clone(), &registry, &limits).expect("ledger");
    ledger
        .submit(SeatReport::completed(ids.assistant, "The suite passed."))
        .expect("first assistant result");
    let error = ledger
        .submit(SeatReport::completed(
            ids.assistant,
            "The suite passed again.",
        ))
        .expect_err("duplicate result");
    assert_eq!(error.refusal(), Refusal::SchemaInvalid);
    assert!(error.detail().contains("duplicate"), "{}", error.detail());

    let mut ledger = ReportLedger::new(tree.clone(), &registry, &limits).expect("ledger");
    let error = ledger
        .submit(SeatReport::completed(ids.ceo, "Nothing delegated."))
        .expect_err("parent before child");
    assert_eq!(error.refusal(), Refusal::SchemaInvalid);
    assert!(error.detail().contains("bottom-up"), "{}", error.detail());

    // The batch entry point refuses the duplicate too.
    let reports = vec![
        SeatReport::completed(ids.assistant, "assistant"),
        SeatReport::completed(ids.lead, "lead"),
        SeatReport::completed(ids.manager, "manager"),
        SeatReport::completed(ids.ceo, "root"),
        SeatReport::completed(ids.assistant, "assistant duplicate"),
    ];
    let error = synthesize_batch(tree, &registry, &limits, reports).expect_err("batch duplicate");
    assert_eq!(error.refusal(), Refusal::SchemaInvalid);
}

#[test]
fn manager_delegates_once() {
    let registry = registry();
    let limits = limits();
    let ceo = Seat::root(
        TaskId::new(),
        "Deliver two bounded artifacts without over-spawning the team.",
    );
    let ceo_id = ceo.seat_id;
    let mut builder = DelegationBuilder::new(ceo).expect("builder");

    // The CEO decomposes once into one manager.
    let manager_spec = SeatSpec {
        seat_id: TaskId::new(),
        brief: "Own one bounded artifact and report the verified outcome.".to_owned(),
        tools: tools_for(SeatTier::Manager),
        depends_on: Vec::new(),
    };
    let managers = builder
        .decompose(ceo_id, &[manager_spec], &registry, &limits)
        .expect("CEO decomposition");
    let manager_id = managers.children[0].seat_id;

    // The manager decomposes once into one team lead.
    let lead_spec = |seat_id: TaskId| SeatSpec {
        seat_id,
        brief: "Ship one bounded deliverable with tests and observed evidence.".to_owned(),
        tools: tools_for(SeatTier::TeamLead),
        depends_on: Vec::new(),
    };
    builder
        .decompose(manager_id, &[lead_spec(TaskId::new())], &registry, &limits)
        .expect("first manager decomposition");

    // A second decomposition from the same manager is refused before any new
    // child is created.
    let before = builder.tree().seats().len();
    let error = builder
        .decompose(manager_id, &[lead_spec(TaskId::new())], &registry, &limits)
        .expect_err("second manager decomposition");
    assert_eq!(error.refusal(), Refusal::SchemaInvalid);
    assert!(
        error.detail().contains("already delegated"),
        "{}",
        error.detail()
    );
    assert_eq!(builder.tree().seats().len(), before);
    assert_eq!(builder.decomposed_managers(), {
        let mut managers = vec![ceo_id, manager_id];
        managers.sort();
        managers
    });

    let tree = builder.build(&registry, &limits).expect("validated tree");
    assert_eq!(tree.children_of(manager_id).len(), 1);
    assert_eq!(tree.seats().len(), before);
}

#[test]
fn root_digest_and_sibling_subtree_digests_are_stable_and_scoped() {
    let registry = registry();
    let limits = limits();
    let ceo = Seat::root(
        TaskId::new(),
        "Deliver two independent branches and synthesize the verified result.",
    );
    let manager = Seat::under(
        &ceo,
        TaskId::new(),
        "Own both branches and report the verified outcome.",
        tools_for(SeatTier::Manager),
        Vec::new(),
    )
    .expect("manager");
    let branch_a = Seat::under(
        &manager,
        TaskId::new(),
        "Ship branch A with tests and observed evidence.",
        tools_for(SeatTier::TeamLead),
        Vec::new(),
    )
    .expect("branch A");
    let branch_b = Seat::under(
        &manager,
        TaskId::new(),
        "Ship branch B with tests and observed evidence.",
        tools_for(SeatTier::TeamLead),
        Vec::new(),
    )
    .expect("branch B");
    let ceo_id = ceo.seat_id;
    let manager_id = manager.seat_id;
    let branch_a_id = branch_a.seat_id;
    let branch_b_id = branch_b.seat_id;
    let tree = DelegationTree::new(vec![ceo, manager, branch_a, branch_b]);

    let base_reports = vec![
        SeatReport::completed(branch_a_id, "branch A result v1"),
        SeatReport::completed(branch_b_id, "branch B result"),
        SeatReport::completed(manager_id, "manager result"),
        SeatReport::completed(ceo_id, "root result"),
    ];
    let first = synthesize_batch(tree.clone(), &registry, &limits, base_reports.clone())
        .expect("first synthesis");
    assert!(first.succeeded);
    assert_eq!(first.subtree_digests.len(), 4);
    for (seat_id, digest) in first.per_tree_digests() {
        assert_eq!(digest.len(), 64, "seat {seat_id} digest");
        assert!(digest.bytes().all(|byte| byte.is_ascii_hexdigit()));
    }
    assert_eq!(
        first.subtree_digest(ceo_id),
        Some(first.root_digest.as_str())
    );
    assert!(first.subtree_digest(TaskId::new()).is_none());

    let changed_reports = vec![
        SeatReport::completed(branch_a_id, "branch A result v2"),
        SeatReport::completed(branch_b_id, "branch B result"),
        SeatReport::completed(manager_id, "manager result"),
        SeatReport::completed(ceo_id, "root result"),
    ];
    let second =
        synthesize_batch(tree, &registry, &limits, changed_reports).expect("second synthesis");
    assert_ne!(
        first.subtree_digest(branch_a_id),
        second.subtree_digest(branch_a_id),
        "changed branch digest changes"
    );
    assert_eq!(
        first.subtree_digest(branch_b_id),
        second.subtree_digest(branch_b_id),
        "sibling branch digest is unaffected"
    );
    assert_ne!(
        first.subtree_digest(manager_id),
        second.subtree_digest(manager_id),
        "ancestor subtree digest covers the changed branch"
    );
    assert_ne!(first.root_digest, second.root_digest);
    assert_eq!(
        second.subtree_digest(ceo_id),
        Some(second.root_digest.as_str())
    );
}

#[test]
fn killed_and_cancelled_rows_rebuild_without_counting_as_success() {
    let registry = registry();
    let limits = limits();
    let run = RunId::new();
    let (tree, ids) = four_seat_tree();
    let states = HashMap::from([
        (ids.assistant, AgentState::Completed),
        (ids.lead, AgentState::Cancelled),
        (ids.manager, AgentState::Killed),
        (ids.ceo, AgentState::Failed),
    ]);
    let rows = rows_for(&tree, run, &states);
    let results = ScriptedResults::default().with_body(ids.assistant, "The suite passed.");
    let ledger = ReportLedger::from_task_records(tree.clone(), &registry, &limits, &rows, &results)
        .expect("rebuilt degraded ledger");
    let synthesis = ledger.synthesize().expect("synthesis");
    assert!(!synthesis.succeeded);
    assert_eq!(synthesis.counts.completed, 1);
    assert_eq!(synthesis.counts.cancelled, 1);
    assert_eq!(synthesis.counts.killed, 1);
    assert_eq!(synthesis.counts.failed, 1);
    assert_eq!(synthesis.counts.blocked, 0);
    assert!(synthesis.summary.contains("KILLED"));
    assert!(synthesis
        .summary
        .contains("Only Completed seats count as success"));

    // A parent completion over the cancelled branch cannot be asserted from
    // rows (the assistant completed, but its lead did not).
    let mut lie = rows;
    lie[0].state = AgentState::Completed;
    lie[1].state = AgentState::Completed;
    let results = ScriptedResults::default()
        .with_body(ids.assistant, "assistant")
        .with_body(ids.lead, "lead")
        .with_body(ids.manager, "manager")
        .with_body(ids.ceo, "root");
    let error = ReportLedger::from_task_records(tree, &registry, &limits, &lie, &results)
        .expect_err("completed over cancelled child");
    assert_eq!(error.refusal(), Refusal::SchemaInvalid);
    assert!(
        error.detail().contains("cannot be Completed"),
        "{}",
        error.detail()
    );
}

#[test]
fn run_store_is_the_production_task_result_source() {
    let db = Database::open_in_memory().expect("in-memory database");
    db.migrate().expect("schema migrations");
    let store = RunStore::new(Arc::new(db));
    let missing = TaskId::new();
    assert_eq!(
        TaskResultSource::task_result(&store, missing).expect("no database refusal"),
        None,
        "a task with no recorded result is None, never an invented body"
    );
    assert_eq!(
        TaskResultSource::task_refusal(&store, missing).expect("no database refusal"),
        None
    );
}
