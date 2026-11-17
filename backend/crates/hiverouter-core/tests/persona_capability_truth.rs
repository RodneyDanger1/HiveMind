//! Persona-vs-registry capability truth.
//!
//! The census compares the registry, tier allowlists and a hardcoded prompt
//! claim list. It cannot see the shipped persona files, so a registry tool can
//! be fully wired but advertised to no agent. This test closes that gap: the
//! union of every shipped persona allowlist must equal the registered tool set.
//!
//! The second half derives the effective agent×tool matrix from code: every
//! shipped persona is intersected with the registry allowlist of the tier its
//! role actually executes at, so a persona cannot advertise a tool its seat
//! cannot call. It also pins the write/terminal-tool contract: a persona that
//! advertises a workspace mutation and a terminal tool (`finish` /
//! `report_result`) must advertise `run_tests`, because `terminal_verified`
//! refuses a terminal call after a successful mutation until a full-suite test
//! run passes. A writer that cannot verify therefore must not advertise the
//! terminal tools at all.

use std::collections::BTreeSet;

use hiverouter_core::execution::terminal_verified;
use hiverouter_core::planner::{BRANCH_LEAD_ROLES, SUBAGENT_ROLES, WORKER_ROLES};
use hiverouter_core::prompts::{load_personas_embedded, Persona, PersonaTier, TemplateRegistry};
use hiverouter_core::tool_loop::{ToolCallPlan, ToolExecution};
use hiverouter_tools::census::PROMPT_CLAIMED_TOOL_IDS;
use hiverouter_tools::tool::{AgentTier, ToolId, ToolRegistry};

/// The pre-fix content-writer allowlist, copied from the commit that fixed the
/// live false-completion defect (`2be5dff^`). It is the before state the RED
/// test is written against, not a hand-written guess.
const PRE_FIX_CONTENT_WRITER_ALLOWLIST: &[&str] = &[
    "file_read",
    "file_list",
    "ask_parent",
    "send_to_sibling",
    "report_result",
    "finish",
    "web_fetch",
];

/// Tools whose successful call mutates the workspace and therefore arms the
/// engine's terminal-verification gate.
const MUTATING_TOOL_IDS: &[&str] = &[
    "file_write",
    "apply_patch",
    "shell_exec",
    "checkpoint_restore",
];

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

/// The registry tier the engine actually authorizes for a canonical persona.
///
/// `task_agent_tier` maps the `subagent` role to `AgentTier::Subagent` even
/// though its persona record declares the worker tier; every other canonical
/// persona maps through its declared tier.
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

fn tier_tool_ids(registry: &ToolRegistry, tier: AgentTier) -> BTreeSet<&'static str> {
    registry.allowlist(tier).iter().map(|id| id.id()).collect()
}

/// Persona allowlist ∩ the real tier allowlist, i.e. what the seat can call.
fn effective_tools<'a>(registry: &ToolRegistry, persona: &'a Persona) -> BTreeSet<&'a str> {
    let callable = tier_tool_ids(registry, effective_tier(persona));
    persona
        .allowed_tool_ids
        .iter()
        .map(String::as_str)
        .filter(|tool| callable.contains(*tool))
        .collect()
}

/// Advertised tool ids that the persona's real tier would refuse.
fn uncallable_tools<'a>(registry: &ToolRegistry, persona: &'a Persona) -> Vec<&'a str> {
    let callable = tier_tool_ids(registry, effective_tier(persona));
    persona
        .allowed_tool_ids
        .iter()
        .map(String::as_str)
        .filter(|tool| !callable.contains(*tool))
        .collect()
}

fn has_tool(persona: &Persona, tool: &str) -> bool {
    persona
        .allowed_tool_ids
        .iter()
        .any(|allowed| allowed == tool)
}

fn call(tool: &str) -> ToolCallPlan {
    ToolCallPlan {
        tool_call_id: format!("call-{tool}"),
        tool: tool.to_owned(),
        args: if tool == "run_tests" {
            serde_json::json!({ "args": [] })
        } else {
            serde_json::json!({})
        },
        args_sha256: "0".repeat(64),
        turn: 1,
    }
}

fn executed(tool: &str) -> ToolExecution {
    let text = if tool == "run_tests" {
        // `terminal_verified` parses this exact bounded prefix.
        r#"{"success":true,"exit_code":0,"stdout":"ok"}"#.to_owned()
    } else {
        "{}".to_owned()
    };
    ToolExecution::ok(format!("call-{tool}"), text)
}

#[test]
fn every_registered_tool_is_advertised_by_at_least_one_shipped_persona() {
    let templates = TemplateRegistry::load_embedded().expect("embedded templates");
    let personas = load_personas_embedded(&templates).expect("embedded personas");

    let registered: BTreeSet<&str> = ToolId::all().iter().map(|tool| tool.id()).collect();
    let advertised: BTreeSet<&str> = personas
        .iter()
        .flat_map(|persona| persona.allowed_tool_ids.iter().map(String::as_str))
        .collect();

    let missing: Vec<&&str> = registered.difference(&advertised).collect();
    assert!(
        missing.is_empty(),
        "registered tools are advertised to no shipped persona: {missing:?}"
    );
    let unknown: Vec<&&str> = advertised.difference(&registered).collect();
    assert!(
        unknown.is_empty(),
        "shipped personas advertise unknown tools: {unknown:?}"
    );

    // The hardcoded prompt claim list must equal the actual registry, so this
    // test also fails when one side is edited alone.
    let claimed: BTreeSet<&str> = PROMPT_CLAIMED_TOOL_IDS.iter().copied().collect();
    assert_eq!(
        claimed, registered,
        "PROMPT_CLAIMED_TOOL_IDS drifted from the registry"
    );
}

#[test]
fn a_worker_persona_can_reach_the_full_coding_surface() {
    let templates = TemplateRegistry::load_embedded().expect("embedded templates");
    let personas = load_personas_embedded(&templates).expect("embedded personas");
    let registry = ToolRegistry::standard();
    let agent = personas
        .iter()
        .find(|persona| persona.id == "agent")
        .expect("agent persona");
    for tool in [
        "fs_glob",
        "fs_grep",
        "file_read_range",
        "apply_patch",
        "run_tests",
        "git_stage",
        "git_commit",
        "git_status",
        "git_diff",
        "git_log",
    ] {
        let id = ToolId::from_id(tool).expect("tool id");
        assert!(
            agent.allowed_tool_ids.iter().any(|allowed| allowed == tool),
            "agent persona lacks {tool}"
        );
        assert!(
            registry.is_allowed(AgentTier::WorkerAgent, id),
            "worker tier lacks {tool}"
        );
    }
}

#[test]
fn agent_tool_matrix_is_derived_from_the_shipped_personas() {
    let personas = loaded_personas();
    let registry = ToolRegistry::standard();

    println!("persona\ttier\tadvertised\teffective\tmutating_tools\teffective_tools");
    let mut covered = BTreeSet::new();
    for persona in &personas {
        let tier = effective_tier(persona);
        let uncallable = uncallable_tools(&registry, persona);
        assert!(
            uncallable.is_empty(),
            "{} advertises tools the {} tier cannot call: {uncallable:?}",
            persona.id,
            tier.label()
        );

        let effective = effective_tools(&registry, persona);
        for tool in &effective {
            covered.insert(*tool);
        }
        let mutating: Vec<&str> = MUTATING_TOOL_IDS
            .iter()
            .copied()
            .filter(|tool| effective.contains(*tool))
            .collect();
        println!(
            "{}\t{}\t{}\t{}\tmutating={:?}\teffective={:?}",
            persona.id,
            tier.label(),
            persona.allowed_tool_ids.len(),
            effective.len(),
            mutating,
            effective.iter().collect::<Vec<_>>()
        );
    }

    let registered: BTreeSet<&str> = ToolId::all().iter().map(|tool| tool.id()).collect();
    let missing: Vec<&&str> = registered.difference(&covered).collect();
    assert!(
        missing.is_empty(),
        "registered tools advertised by no shipped persona: {missing:?}"
    );

    // Every role the planner may emit has a shipped persona to resolve.
    for &role in BRANCH_LEAD_ROLES
        .iter()
        .chain(SUBAGENT_ROLES)
        .chain(WORKER_ROLES)
    {
        let seat = persona(&personas, role);
        assert!(
            effective_tier(seat) != AgentTier::ToolAgent,
            "planner role {role} resolved to the production-less ToolAgent tier"
        );
    }
}

#[test]
fn write_capable_seats_include_the_content_writer_and_branch_lead() {
    let personas = loaded_personas();
    let registry = ToolRegistry::standard();

    for id in ["content-writer", "data-analyst", "superagent", "agent"] {
        let seat = persona(&personas, id);
        assert!(
            has_tool(seat, "file_write"),
            "{id} must advertise file_write; its declared artifact work cannot be delivered otherwise"
        );
        assert!(
            effective_tools(&registry, seat).contains("file_write"),
            "{id}'s file_write must survive the persona ∩ tier intersection"
        );
    }

    // The branch lead keeps its delegation path as well as its direct write path.
    let lead = persona(&personas, "superagent");
    assert!(
        has_tool(lead, "spawn_agent"),
        "a branch lead without spawn_agent cannot own a branch"
    );

    // These seats are intentionally read-only; advertising a write would be a
    // capability claim the runtime refuses.
    for id in ["code-reviewer", "chatbot", "subagent", "planner"] {
        let seat = persona(&personas, id);
        assert!(
            !has_tool(seat, "file_write"),
            "{id} is intentionally read-only and must not advertise file_write"
        );
    }
}

#[test]
fn pre_fix_content_writer_allowlist_could_not_write() {
    let personas = loaded_personas();
    let registry = ToolRegistry::standard();

    let mut pre_fix = persona(&personas, "content-writer").clone();
    pre_fix.allowed_tool_ids = PRE_FIX_CONTENT_WRITER_ALLOWLIST
        .iter()
        .map(|tool| (*tool).to_owned())
        .collect();

    assert!(
        !has_tool(&pre_fix, "file_write"),
        "the pre-fix allowlist is the evidence: it has no file_write"
    );
    assert!(
        !effective_tools(&registry, &pre_fix).contains("file_write"),
        "the pre-fix content-writer seat could not create the declared artifact"
    );

    // The shipped post-fix persona must differ from that pre-fix state.
    assert!(
        has_tool(persona(&personas, "content-writer"), "file_write"),
        "the current content-writer persona is not the post-fix state"
    );
}

#[test]
fn a_mutating_persona_with_finish_must_advertise_run_tests() {
    // The engine refuses a terminal call after a successful mutation unless a
    // full-suite `run_tests` result follows it.
    let write_then_finish = [
        (call("file_write"), executed("file_write")),
        (call("finish"), executed("finish")),
    ];
    assert!(
        !terminal_verified(&[], &write_then_finish),
        "file_write alone must not verify a task"
    );

    let write_test_finish = [
        (call("file_write"), executed("file_write")),
        (call("run_tests"), executed("run_tests")),
        (call("finish"), executed("finish")),
    ];
    assert!(
        terminal_verified(&[], &write_test_finish),
        "a passing full-suite test run must verify the mutation"
    );

    for seat in loaded_personas() {
        let mutates = MUTATING_TOOL_IDS.iter().any(|tool| has_tool(&seat, tool));
        let advertises_terminal = has_tool(&seat, "finish") || has_tool(&seat, "report_result");
        if mutates && advertises_terminal {
            assert!(
                has_tool(&seat, "run_tests"),
                "{} advertises a workspace mutation and a terminal tool but no run_tests; \
                 finish/report_result would be refused after the mutation",
                seat.id
            );
        }
    }
}

#[test]
fn persona_validation_does_not_reject_an_ungranted_tool_today() {
    // Known gap: `Persona::validate` checks the template and spawn caps but not
    // `allowed_tool_ids`. An imported profile can therefore advertise a tool its
    // tier will refuse at call time; the provider offers it and the model burns
    // a turn on E_TOOL_NOT_ALLOWED. When validation flips, update this pin.
    let templates = TemplateRegistry::load_embedded().expect("embedded templates");
    let personas = load_personas_embedded(&templates).expect("embedded personas");
    let registry = ToolRegistry::standard();
    let mut writer = persona(&personas, "content-writer").clone();
    writer.allowed_tool_ids.push("vote_evolution".to_owned());

    assert_eq!(
        writer.validate(&templates),
        Ok(()),
        "Persona::validate accepted a WorkerAgent persona advertising SuperAgent-only vote_evolution"
    );
    assert!(
        uncallable_tools(&registry, &writer).contains(&"vote_evolution"),
        "the worker tier must refuse the falsely advertised tool"
    );
}

#[test]
fn write_capable_worker_roster_is_explicit() {
    let personas = loaded_personas();
    let registry = ToolRegistry::standard();
    let writers: BTreeSet<&str> = WORKER_ROLES
        .iter()
        .copied()
        .filter(|role| effective_tools(&registry, persona(&personas, role)).contains("file_write"))
        .collect();

    assert_eq!(
        writers,
        ["agent", "content-writer", "data-analyst"]
            .into_iter()
            .collect::<BTreeSet<_>>()
    );
    assert!(
        !writers.contains("code-reviewer") && !writers.contains("chatbot"),
        "read-only workers must stay out of the write-capable roster"
    );
}
