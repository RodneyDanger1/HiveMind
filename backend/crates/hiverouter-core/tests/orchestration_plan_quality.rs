//! Orchestration plan-quality gates: the planner prompt contract, the canonical
//! role ladder, and the DAG shapes that used to reach dispatch before failing.
//!
//! These tests read the *embedded* assets, so they pin the bytes the release
//! binary actually ships rather than a developer checkout of the same file.

use std::collections::BTreeMap;

use hiverouter_contract::{Refusal, TaskId};
use hiverouter_core::planner::{
    Plan, PlanNode, BRANCH_LEAD_ROLES, MAX_PLAN_FANOUT, SUBAGENT_ROLES, WORKER_ROLES,
};
use hiverouter_core::prompts::{
    load_personas_embedded, MessageRole, PersonaTier, TemplateRegistry,
};

fn registry() -> TemplateRegistry {
    TemplateRegistry::load_embedded().expect("embedded templates load")
}

fn render_planner(task: &str) -> Vec<hiverouter_core::prompts::RenderedMessage> {
    let mut inputs = BTreeMap::new();
    inputs.insert("task".to_owned(), task.to_owned());
    registry()
        .render("planner", &inputs)
        .expect("planner template renders")
}

fn node(role: &str, depth: u8) -> PlanNode {
    PlanNode {
        node_id: TaskId::new(),
        role: role.to_owned(),
        deps: Vec::new(),
        depth,
        parent: None,
    }
}

fn child(role: &str, depth: u8, parent: TaskId) -> PlanNode {
    PlanNode {
        node_id: TaskId::new(),
        role: role.to_owned(),
        deps: Vec::new(),
        depth,
        parent: Some(parent),
    }
}

#[test]
fn planner_prompt_is_a_terse_structured_contract() {
    let messages = render_planner("Write a README, a CLI help page, and a release note");
    assert_eq!(messages.len(), 2, "planner is a system + user template");
    assert_eq!(messages[0].role, MessageRole::System);
    assert_eq!(messages[1].role, MessageRole::User);

    let system = &messages[0].content;
    assert!(system.contains("HiveMind planner"), "{system}");
    // Machine-readable output contract: one object, exact keys, no prose.
    for required in [
        "\"nodes\"",
        "\"id\"",
        "\"role\"",
        "\"deps\"",
        "\"depth\"",
        "\"parent\"",
        "No markdown fences",
        "no prose",
    ] {
        assert!(
            system.contains(required),
            "planner schema term {required:?} missing from the system prompt"
        );
    }
    // The branch-budget rules that stop a 3-item request becoming 5 leads.
    for required in [
        "one depth-1 superagent per deliverable",
        "Three requested items means exactly three depth-1 nodes, never five",
        "a step: plan, analyse, design, implement, test, review",
        "at most 8 depth-1 leads",
    ] {
        assert!(
            system.contains(required),
            "planner branch-budget rule {required:?} missing"
        );
    }
    // Repair guidance: a rejected plan is fixed by editing fields, not by
    // growing the DAG on the one repair call.
    assert!(system.contains("If a plan is rejected"));
    assert!(system.contains("Never add nodes to repair a plan"));
    // No unresolved template placeholder may reach the model.
    assert!(
        !system.contains("{{"),
        "unresolved placeholder in system prompt"
    );
    assert!(!system.contains("${env:"), "forbidden env reference");
}

#[test]
fn planner_user_turn_carries_the_task_once() {
    let task = "Refactor the parser and add tests for it";
    let messages = render_planner(task);
    let user = &messages[1].content;
    assert!(user.contains(task), "task must reach the user turn: {user}");
    assert!(!user.contains("{{ task }}"), "placeholder substituted");
    assert_eq!(user.matches(task).count(), 1, "task must not be repeated");
}

#[test]
fn branch_lead_and_worker_prompts_enforce_a_terse_result_contract() {
    let registry = registry();
    let inputs = BTreeMap::new();
    for template_id in ["superagent", "subagent", "agent", "code-reviewer"] {
        let messages = registry
            .render(template_id, &inputs)
            .unwrap_or_else(|error| panic!("{template_id} renders: {error:?}"));
        let system = messages
            .iter()
            .find(|message| message.role == MessageRole::System)
            .map(|message| message.content.clone())
            .expect("a system section");
        assert!(
            system.contains("RESULT:") && system.contains("BLOCKED:"),
            "{template_id} must pin a labelled result contract: {system}"
        );
        assert!(
            system.contains("No preamble") || system.contains("no preamble"),
            "{template_id} must forbid preamble prose: {system}"
        );
        assert!(
            !system.contains("{{"),
            "{template_id} system prompt must not leak placeholders it cannot render"
        );
    }
}

#[test]
fn canonical_role_ladder_matches_the_shipped_personas() {
    let registry = registry();
    let personas = load_personas_embedded(&registry).expect("embedded personas");
    let find = |id: &str| {
        personas
            .iter()
            .find(|persona| persona.id == id)
            .unwrap_or_else(|| panic!("persona {id} missing"))
    };
    for id in BRANCH_LEAD_ROLES {
        assert_eq!(
            find(id).tier,
            PersonaTier::Super,
            "{id} must be a SubAgent tier"
        );
    }
    for id in WORKER_ROLES {
        assert_eq!(
            find(id).tier,
            PersonaTier::Worker,
            "{id} must be a worker tier"
        );
    }
    for id in SUBAGENT_ROLES {
        assert_eq!(
            find(id).tier,
            PersonaTier::Worker,
            "{id} executes in the worker tier"
        );
    }
    // A depth-1 lead must be able to spawn its workers.
    let lead = find("superagent");
    assert!(
        lead.allowed_tool_ids
            .iter()
            .any(|tool| tool == "spawn_agent"),
        "a branch lead without spawn_agent cannot own a branch"
    );
}

#[test]
fn over_spawned_request_is_refused_before_dispatch() {
    // Five SuperAgents for a three-item request is the live over-spawn shape;
    // the root fan-out cap must refuse anything above the engine cap up front.
    let mut nodes = Vec::new();
    for _ in 0..=MAX_PLAN_FANOUT {
        nodes.push(node("super", 1));
    }
    assert_eq!(Plan::new(nodes).validate(), Err(Refusal::FanoutLimit));

    // The fan-out cap is per parent, not only per root.
    let lead = node("super", 1);
    let mut nodes = vec![lead.clone()];
    for _ in 0..=MAX_PLAN_FANOUT {
        nodes.push(child("worker", 2, lead.node_id));
    }
    assert_eq!(Plan::new(nodes).validate(), Err(Refusal::FanoutLimit));
}

#[test]
fn three_item_plan_with_one_lead_each_is_accepted() {
    let mut nodes = Vec::new();
    for _ in 0..3 {
        let lead = node("superagent", 1);
        let mut worker = node("agent", 2);
        worker.parent = Some(lead.node_id);
        nodes.push(lead);
        nodes.push(worker);
    }
    let plan = Plan::new(nodes);
    plan.validate().expect("three leads with one worker each");
    assert_eq!(plan.topo_order().expect("order").len(), 6);
}

#[test]
fn role_tool_mismatch_is_refused_at_plan_time() {
    // Unknown labels used to surface as E_PERSONA_UNKNOWN only after the root
    // task row existed.
    assert_eq!(
        Plan::new(vec![node("researcher", 1)]).validate(),
        Err(Refusal::PersonaUnknown)
    );
    // A canonical persona id may not self-select a different rank.
    let lead = node("super", 1);
    let mut sub = node("subagent", 2);
    sub.parent = Some(lead.node_id);
    assert_eq!(
        Plan::new(vec![lead, sub]).validate(),
        Err(Refusal::ToolNotAllowed)
    );
}

#[test]
fn dependency_cycles_across_the_parent_edge_are_refused() {
    // A depth-3 node depending on its grandparent deadlocks the readiness loop:
    // the grandparent waits for the parent chain that waits for this node.
    let lead = node("super", 1);
    let mut agent = node("worker", 2);
    agent.parent = Some(lead.node_id);
    let mut sub = node("subagent", 3);
    sub.parent = Some(agent.node_id);
    sub.deps = vec![lead.node_id];
    assert_eq!(
        Plan::new(vec![lead, agent, sub]).validate(),
        Err(Refusal::SchemaInvalid)
    );

    // Duplicate deps are a schema error, not a phantom cycle.
    let lead = node("super", 1);
    let mut agent = node("worker", 2);
    agent.parent = Some(lead.node_id);
    agent.deps = vec![lead.node_id, lead.node_id];
    let plan = Plan::new(vec![lead, agent]);
    assert_eq!(plan.validate(), Err(Refusal::SchemaInvalid));
    assert_eq!(plan.topo_order().expect("order").len(), 2);
}
