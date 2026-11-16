//! Registry-layer contract for `checkpoint_restore` (the id-addressed undo tool).
//!
//! Scope: the capability surface only. This file proves that the tool is
//! registered with a Mutate contract and a schema, that the derived census,
//! the tier allowlists and the prompt claims all count it, that HyperAgent /
//! SuperAgent / WorkerAgent may call it, and that Subagent may not. The
//! execution/side-effect lane is asserted through `EXECUTOR_DISPOSITIONS`
//! (`approval_executor`); wiring the effect itself is a separate change.

use std::collections::BTreeSet;

use hiverouter_core::prompts::{load_personas_embedded, TemplateRegistry};
use hiverouter_tools::census::{EXECUTOR_DISPOSITIONS, PROMPT_CLAIMED_TOOL_IDS};
use hiverouter_tools::tool::{AgentTier, PermissionClass, ToolId, ToolRegistry};
use hiverouter_tools::{standard_census, CheckpointRestoreArgs};

const TOOL: &str = "checkpoint_restore";

fn valid_args() -> serde_json::Value {
    serde_json::json!({"checkpoint_id": "01J9Z0CKPT0000000000000000", "path": "notes/one.txt"})
}

#[test]
fn census_counts_checkpoint_restore_in_registry_allowlists_claims_and_lanes() {
    let registry = ToolRegistry::standard();
    let census = standard_census().expect("the capability census has no drift");
    let id = ToolId::from_id(TOOL).expect("checkpoint_restore is a registered tool id");

    assert_eq!(id.id(), TOOL);
    assert!(registry.contract(id).is_some(), "the registry has a contract");
    assert_eq!(census.tool_count, ToolId::all().len());
    assert_eq!(census.tool_count, registry.contract_count());
    assert!(census.registry_tool_ids.iter().any(|tool| tool == TOOL));
    assert!(census.allowlist_tool_ids.iter().any(|tool| tool == TOOL));
    assert!(PROMPT_CLAIMED_TOOL_IDS.contains(&TOOL));
    assert!(census.prompt_claim_tool_ids.iter().any(|tool| tool == TOOL));

    // Exactly one execution lane, and it is the approval lane: an undo writes
    // the workspace and must consume the same single-use grant as the mutation.
    assert_eq!(
        census.executor_dispositions.get(TOOL).map(String::as_str),
        Some("approval_executor")
    );
    assert_eq!(
        EXECUTOR_DISPOSITIONS
            .iter()
            .filter(|(tool, _)| *tool == TOOL)
            .count(),
        1,
        "a tool may not have two execution lanes"
    );
    assert_eq!(
        census.executor_dispositions.len(),
        census.registry_tool_ids.len(),
        "every advertised tool has exactly one declared lane"
    );

    // The census counts are derived: the total is the sum of the per-tier
    // allowlists, and each tier's census rows equal its registry allowlist.
    let derived_grants: usize = AgentTier::all()
        .iter()
        .map(|tier| registry.allowlist(*tier).len())
        .sum();
    assert_eq!(census.grant_count(), derived_grants);
    for tier in AgentTier::all() {
        let census_ids: BTreeSet<&str> = census.tools_by_tier[tier.label()]
            .iter()
            .map(String::as_str)
            .collect();
        let registry_ids: BTreeSet<&str> = registry
            .allowlist(tier)
            .iter()
            .map(|granted| granted.id())
            .collect();
        assert_eq!(
            census_ids,
            registry_ids,
            "census rows drifted from the {} registry allowlist",
            tier.label()
        );
    }
}

#[test]
fn schema_accepts_a_valid_pair_and_rejects_missing_or_unknown_fields() {
    let registry = ToolRegistry::standard();
    let contract = registry
        .contract(ToolId::CheckpointRestore)
        .expect("checkpoint_restore contract");

    registry
        .resolve_call(AgentTier::WorkerAgent, TOOL, &valid_args())
        .expect("a valid checkpoint_id + path pair validates");

    // The exported typed args accept the same pair (lib.rs export is load-bearing
    // for the core executor that will deserialize the model's arguments).
    let parsed: CheckpointRestoreArgs =
        serde_json::from_value(valid_args()).expect("typed args deserialize");
    assert_eq!(parsed.checkpoint_id, "01J9Z0CKPT0000000000000000");
    assert_eq!(parsed.path, "notes/one.txt");

    // Missing checkpoint_id.
    let missing_id = serde_json::json!({"path": "notes/one.txt"});
    let error = contract.validate_args(&missing_id).expect_err("missing id");
    assert_eq!(error.code(), "E_TOOL_SCHEMA");
    assert!(
        error.detail().contains("accepted fields: [checkpoint_id, path]"),
        "the refusal names the accepted shape: {}",
        error.detail()
    );

    // Missing path.
    let missing_path = serde_json::json!({"checkpoint_id": "ckpt-01"});
    let error = contract.validate_args(&missing_path).expect_err("missing path");
    assert_eq!(error.code(), "E_TOOL_SCHEMA");

    // Unknown field (deny_unknown_fields + additionalProperties:false).
    let unknown = serde_json::json!({
        "checkpoint_id": "ckpt-01",
        "path": "notes/one.txt",
        "force": true
    });
    let error = contract.validate_args(&unknown).expect_err("unknown field");
    assert_eq!(error.code(), "E_TOOL_SCHEMA");

    // Wrong field type.
    let wrong_type = serde_json::json!({"checkpoint_id": 7, "path": "notes/one.txt"});
    let error = contract.validate_args(&wrong_type).expect_err("wrong type");
    assert_eq!(error.code(), "E_TOOL_SCHEMA");

    // Empty object is refused (both fields are required).
    let error = contract
        .validate_args(&serde_json::json!({}))
        .expect_err("empty args");
    assert_eq!(error.code(), "E_TOOL_SCHEMA");

    assert_eq!(
        contract.schema.get("additionalProperties"),
        Some(&serde_json::Value::Bool(false)),
        "the schema forbids unknown fields"
    );
    assert_eq!(
        contract.schema.get("type").and_then(serde_json::Value::as_str),
        Some("object"),
        "the schema describes an argument object"
    );
    let properties = contract
        .schema
        .get("properties")
        .and_then(serde_json::Value::as_object)
        .expect("the schema names its properties");
    assert!(properties.contains_key("checkpoint_id"));
    assert!(properties.contains_key("path"));
    assert_eq!(properties.len(), 2, "no undeclared argument fields");
}

#[test]
fn grant_matrix_allows_hyper_super_and_worker_and_refuses_subagent() {
    let registry = ToolRegistry::standard();
    let id = ToolId::CheckpointRestore;

    for tier in [
        AgentTier::HyperAgent,
        AgentTier::SuperAgent,
        AgentTier::WorkerAgent,
    ] {
        assert!(
            registry.is_allowed(tier, id),
            "{} must be able to undo its own journaled mutation",
            tier.label()
        );
        registry
            .resolve_call(tier, TOOL, &valid_args())
            .expect("a granted tier resolves the call");
    }

    for tier in [AgentTier::Subagent, AgentTier::ToolAgent] {
        assert!(
            !registry.is_allowed(tier, id),
            "{} must not hold mutation authority",
            tier.label()
        );
        let error = registry
            .resolve_call(tier, TOOL, &valid_args())
            .expect_err("an ungranted tier is refused by name");
        assert_eq!(error.code(), "E_TOOL_NOT_ALLOWED");
    }

    let contract = registry.contract(id).expect("contract");
    assert_eq!(contract.display_name, TOOL);
    assert_eq!(contract.permission_class, PermissionClass::Mutate);
    assert!(
        contract.requires_approval(),
        "a Mutate tool is always approval-gated"
    );
}

#[test]
fn shipped_personas_advertise_the_restore_only_on_granted_tiers() {
    let templates = TemplateRegistry::load_embedded().expect("embedded templates");
    let personas = load_personas_embedded(&templates).expect("embedded personas");

    for persona_id in ["superagent", "agent"] {
        let persona = personas
            .iter()
            .find(|persona| persona.id == persona_id)
            .unwrap_or_else(|| panic!("{persona_id} persona is shipped"));
        assert!(
            persona
                .allowed_tool_ids
                .iter()
                .any(|tool| tool == TOOL),
            "the {persona_id} persona must advertise {TOOL} so the model can use it"
        );
    }

    let subagent = personas
        .iter()
        .find(|persona| persona.id == "subagent")
        .expect("subagent persona");
    assert!(
        !subagent.allowed_tool_ids.iter().any(|tool| tool == TOOL),
        "the subagent persona must not advertise mutation authority"
    );
}
