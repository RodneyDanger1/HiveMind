//! Planner-capability truth: a plan may only choose a seat that can perform the
//! seat's declared deliverable.
//!
//! The planner emits roles, not tools. `Plan::validate` checks the role ladder,
//! depth, parent, deps and fan-out; it does **not** check that the chosen
//! persona can execute the node's objective. This file:
//!
//! 1. pins the content-writer file-write plan that used to complete with no
//!    artifact (the seat now has `file_write`, so the plan is accepted and the
//!    seat is capable);
//! 2. records the pre-fix allowlist and proves the same plan shape was accepted
//!    while the seat could not write — the plan validator was never the gate;
//! 3. characterizes the remaining objective→capability gap: a file-write
//!    objective on a read-only worker (`code-reviewer`, `chatbot`) is still
//!    accepted by `Plan::validate`, and the planner prompt names roles without
//!    their capabilities.
//!
//! The gap is deliberately pinned by a passing characterization test. When a
//! plan-time capability gate lands, `planner_has_no_objective_capability_gate_today`
//! will fail and must be flipped to assert the named refusal.

use std::collections::BTreeMap;

use hiverouter_contract::TaskId;
use hiverouter_core::planner::{Plan, PlanNode, BRANCH_LEAD_ROLES, SUBAGENT_ROLES, WORKER_ROLES};
use hiverouter_core::prompts::{
    load_personas_embedded, MessageRole, Persona, PersonaTier, TemplateRegistry,
};
use hiverouter_core::tool_loop::tool_specs_for_tier;
use hiverouter_tools::tool::{AgentTier, ToolRegistry};

/// The exact pre-fix content-writer allowlist from `2be5dff^` (the commit that
/// fixed the live false-completion defect). It is evidence, not a guess.
const PRE_FIX_CONTENT_WRITER_ALLOWLIST: &[&str] = &[
    "file_read",
    "file_list",
    "ask_parent",
    "send_to_sibling",
    "report_result",
    "finish",
    "web_fetch",
];

/// A file-creation objective a planner can emit for a content deliverable.
const FILE_WRITE_OBJECTIVE: &str =
    "Write the requested artifact to workspace path planner-capability-proof.md and return the path.";

fn loaded_personas() -> Vec<Persona> {
    let templates = TemplateRegistry::load_embedded().expect("embedded templates");
    load_personas_embedded(&templates).expect("embedded personas")
}

fn persona<'a>(personas: &'a [Persona], id: &str) -> &'a Persona {
    personas
        .iter()
        .find(|persona| persona.id == id)
        .unwrap_or_else(|| panic!("shipped persona {id} is missing"))
}

/// The tier the engine authorizes for a canonical persona (`subagent` executes
/// at `AgentTier::Subagent` even though its record declares the worker tier).
fn effective_tier(persona: &Persona) -> AgentTier {
    if SUBAGENT_ROLES.contains(&persona.id.as_str()) {
        AgentTier::Subagent
    } else {
        match persona.tier {
            PersonaTier::Hyper => AgentTier::HyperAgent,
            PersonaTier::Super => AgentTier::SuperAgent,
            PersonaTier::Worker => AgentTier::WorkerAgent,
            PersonaTier::Tool => AgentTier::ToolAgent,
        }
    }
}

fn has_tool(persona: &Persona, tool: &str) -> bool {
    persona
        .allowed_tool_ids
        .iter()
        .any(|allowed| allowed == tool)
}

fn effective_tools<'a>(registry: &ToolRegistry, persona: &'a Persona) -> Vec<&'a str> {
    let tier = effective_tier(persona);
    let callable: Vec<&str> = registry.allowlist(tier).iter().map(|id| id.id()).collect();
    persona
        .allowed_tool_ids
        .iter()
        .map(String::as_str)
        .filter(|tool| callable.contains(tool))
        .collect()
}

fn can_deliver_file_write(registry: &ToolRegistry, persona: &Persona) -> bool {
    effective_tools(registry, persona)
        .iter()
        .any(|tool| *tool == "file_write")
}

/// The canonical planner branch shape for one deliverable: a depth-1
/// `superagent` branch lead with one depth-2 worker.
fn lead_and_worker(worker_role: &str, worker_objective: &str) -> Plan {
    let lead = PlanNode {
        objective: Some("Own the requested artifact branch and return its path.".to_owned()),
        node_id: TaskId::new(),
        role: "superagent".to_owned(),
        deps: Vec::new(),
        depth: 1,
        parent: None,
    };
    let worker = PlanNode {
        objective: Some(worker_objective.to_owned()),
        node_id: TaskId::new(),
        role: worker_role.to_owned(),
        deps: Vec::new(),
        depth: 2,
        parent: Some(lead.node_id),
    };
    Plan::new(vec![lead, worker])
}

#[test]
fn content_writer_file_write_plan_is_accepted_and_its_seat_can_write() {
    let personas = loaded_personas();
    let registry = ToolRegistry::standard();
    let plan = lead_and_worker("content-writer", FILE_WRITE_OBJECTIVE);

    plan.validate()
        .expect("the planner's superagent -> content-writer file-write plan must validate");

    let writer = plan
        .nodes
        .iter()
        .find(|node| node.role == "content-writer")
        .expect("the plan has a content-writer node");
    assert!(
        writer
            .objective
            .as_deref()
            .is_some_and(|objective| objective == FILE_WRITE_OBJECTIVE),
        "the seat brief is the node objective"
    );

    let writer_persona = persona(&personas, "content-writer");
    assert_eq!(
        effective_tier(writer_persona),
        AgentTier::WorkerAgent,
        "content-writer executes at the worker tier"
    );
    assert!(
        has_tool(writer_persona, "file_write"),
        "the content-writer persona must advertise file_write"
    );
    assert!(
        can_deliver_file_write(&registry, writer_persona),
        "the content-writer seat's effective toolset must contain file_write"
    );
    // The production adapter advertises the tier specs filtered by this persona
    // allowlist, so the model must be offered file_write on this seat.
    assert!(
        tool_specs_for_tier(&registry, AgentTier::WorkerAgent)
            .iter()
            .any(|spec| spec.function.name == "file_write"),
        "the worker tool specs must include file_write"
    );

    // The branch lead can also create the artifact itself, so a one-node
    // branch with no worker is not an incapable seat either.
    let lead_persona = persona(&personas, "superagent");
    assert!(has_tool(lead_persona, "spawn_agent"));
    assert!(
        can_deliver_file_write(&registry, lead_persona),
        "the branch lead must be able to create a new file when it does not delegate"
    );
}

#[test]
fn pre_fix_content_writer_plan_was_accepted_but_incapable() {
    // The same plan shape validated before the fix: the defect was a missing
    // persona capability, not a malformed plan.
    let plan = lead_and_worker("content-writer", FILE_WRITE_OBJECTIVE);
    assert_eq!(
        plan.validate(),
        Ok(()),
        "plan validation accepted the pre-fix shape because it does not inspect capability"
    );

    let registry = ToolRegistry::standard();
    let personas = loaded_personas();
    let mut pre_fix = persona(&personas, "content-writer").clone();
    pre_fix.allowed_tool_ids = PRE_FIX_CONTENT_WRITER_ALLOWLIST
        .iter()
        .map(|tool| (*tool).to_owned())
        .collect();

    assert!(
        !has_tool(&pre_fix, "file_write"),
        "the pre-fix persona had no file_write"
    );
    assert!(
        !can_deliver_file_write(&registry, &pre_fix),
        "the pre-fix content-writer seat could not create the declared artifact"
    );
}

#[test]
fn planner_role_ladder_resolves_to_shipped_personas() {
    let personas = loaded_personas();

    for &role in BRANCH_LEAD_ROLES {
        let lead = persona(&personas, role);
        assert_eq!(effective_tier(lead), AgentTier::SuperAgent);
        assert!(
            has_tool(lead, "spawn_agent"),
            "{role} must be able to delegate"
        );
    }
    for &role in WORKER_ROLES {
        assert_eq!(
            effective_tier(persona(&personas, role)),
            AgentTier::WorkerAgent,
            "{role} must execute at the worker tier"
        );
    }
    for &role in SUBAGENT_ROLES {
        assert_eq!(
            effective_tier(persona(&personas, role)),
            AgentTier::Subagent,
            "{role} must execute at the subagent tier"
        );
    }
}

#[test]
fn the_planner_has_write_capable_workers_for_a_file_deliverable() {
    let personas = loaded_personas();
    let registry = ToolRegistry::standard();
    let writers: Vec<&str> = WORKER_ROLES
        .iter()
        .copied()
        .filter(|role| can_deliver_file_write(&registry, persona(&personas, role)))
        .collect();

    assert!(
        writers.contains(&"content-writer"),
        "the writer seat the live defect selected must be write-capable: {writers:?}"
    );
    assert!(writers.contains(&"agent"));
    assert!(writers.contains(&"data-analyst"));
    assert!(
        !writers.contains(&"code-reviewer") && !writers.contains(&"chatbot"),
        "intentionally read-only workers must not be write-capable: {writers:?}"
    );
}

#[test]
fn planner_prompt_lists_roles_without_their_capabilities() {
    let templates = TemplateRegistry::load_embedded().expect("embedded templates");
    let mut inputs = BTreeMap::new();
    inputs.insert("task".to_owned(), "Write a README file".to_owned());
    let messages = templates
        .render("planner", &inputs)
        .expect("planner template renders");
    let system = messages
        .iter()
        .find(|message| message.role == MessageRole::System)
        .map(|message| message.content.as_str())
        .expect("planner system message");

    // The roster is present...
    assert!(system.contains("content-writer"), "{system}");
    assert!(system.contains("code-reviewer"), "{system}");
    // ...but no capability column tells the model which roles can create files.
    for capability in ["file_write", "write-capable", "capability"] {
        assert!(
            !system.contains(capability),
            "planner prompt now states {capability:?}; update this known-gap pin"
        );
    }
}

#[test]
fn planner_has_no_objective_capability_gate_today() {
    // Known gap. `Plan::validate` accepts a read-only worker for an objective
    // that explicitly asks it to write a file. The planner can therefore choose
    // an incapable seat without a named refusal; the run may complete with no
    // artifact. When a plan-time capability gate lands this test must flip to
    // assert the refusal.
    let personas = loaded_personas();
    let registry = ToolRegistry::standard();

    for read_only in ["code-reviewer", "chatbot"] {
        let plan = lead_and_worker(read_only, FILE_WRITE_OBJECTIVE);
        assert_eq!(
            plan.validate(),
            Ok(()),
            "Plan::validate currently accepts a {read_only} seat for a file-write objective"
        );
        let seat = persona(&personas, read_only);
        assert!(
            !can_deliver_file_write(&registry, seat),
            "{read_only} must remain read-only"
        );
    }
}
