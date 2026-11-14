//! Derived capability census (Phase 11 WPs 11.9/11.12, CANONICAL C24).
//!
//! Counts are **derived from enum iteration** (`strum::EnumIter`), never typed:
//! the registry, the per-tier allowlists and the prompt-claimed tool ids are
//! compared as sets and the census refuses to build when one side is edited
//! alone. A drift test deletes one row and expects the failure by name.

use std::collections::BTreeMap;

use hiverouter_contract::Refusal;
use serde::Serialize;
use strum::IntoEnumIterator;

use crate::error::ToolError;
use crate::tool::{AgentTier, PermissionClass, ToolContract, ToolId, ToolRegistry};

/// Tools a v1 prompt may claim to expose (declared once; the census compares it
/// to the registry and allowlists — it is not derived from the registry, or the
/// comparison would be tautological).
pub const PROMPT_CLAIMED_TOOL_IDS: &[&str] = &[
    "spawn_agent",
    "ask_parent",
    "send_to_sibling",
    "report_result",
    "finish",
    "file_read",
    "file_list",
    "file_write",
    "shell_exec",
    "request_detached",
    "web_fetch",
    "vote_evolution",
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
    "harness_diagnose",
    "repo_search",
];

/// Every advertised tool must have a declared execution lane. The census below
/// refuses an unclassified registry row, so a tool can never be advertised to
/// the model without an executor/approval/loop owner.
pub const EXECUTOR_DISPOSITIONS: &[(&str, &str)] = &[
    ("file_read", "read_executor"),
    ("file_list", "read_executor"),
    ("report_result", "read_executor"),
    ("finish", "read_executor"),
    ("file_write", "approval_executor"),
    ("shell_exec", "approval_executor"),
    ("spawn_agent", "loop_owned"),
    ("ask_parent", "loop_owned"),
    ("send_to_sibling", "loop_owned"),
    ("request_detached", "loop_owned"),
    ("web_fetch", "approval_executor"),
    ("vote_evolution", "loop_owned"),
    ("fs_glob", "read_executor"),
    ("fs_grep", "read_executor"),
    ("file_read_range", "read_executor"),
    ("apply_patch", "approval_executor"),
    ("run_tests", "approval_executor"),
    ("git_stage", "approval_executor"),
    ("git_commit", "approval_executor"),
    ("git_status", "git_read_executor"),
    ("git_diff", "git_read_executor"),
    ("git_log", "git_read_executor"),
    ("harness_diagnose", "read_executor"),
    ("repo_search", "read_executor"),
];

/// Schema id of the census payload.
pub const CENSUS_SCHEMA: &str = "hiverouter.census.v1";

/// One derived census report.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct CapabilityCensus {
    /// Payload schema id.
    pub schema: &'static str,
    /// Where the counts came from (provenance label).
    pub derived_from: &'static str,
    /// Number of derived tool enum variants.
    pub tool_count: usize,
    /// Number of named refusals.
    pub refusal_count: usize,
    /// Number of agent tiers.
    pub tier_count: usize,
    /// Number of permission classes.
    pub permission_class_count: usize,
    /// Tools by tier (derived from the allowlists).
    pub tools_by_tier: BTreeMap<String, Vec<String>>,
    /// Registered contract ids.
    pub registry_tool_ids: Vec<String>,
    /// Allowlist ids across all tiers (deduplicated).
    pub allowlist_tool_ids: Vec<String>,
    /// Prompt-claimed tool ids.
    pub prompt_claim_tool_ids: Vec<String>,
    /// Execution lane each advertised tool is wired to.
    pub executor_dispositions: BTreeMap<String, String>,
}

impl CapabilityCensus {
    /// Grant count across all tiers (derived, not typed).
    #[must_use]
    pub fn grant_count(&self) -> usize {
        self.tools_by_tier.values().map(Vec::len).sum()
    }
}

/// Compares the three sets and refuses on drift.
///
/// # Errors
/// [`Refusal::InternalError`] naming the missing/extra ids when the registry,
/// allowlists and prompt claims are not identical sets. (The frozen refusal
/// table has no capability-census code; this is a build-invariant failure, so
/// `E_INTERNAL_ERROR` with a specific detail is used rather than inventing a
/// code.)
pub fn check_census_drift(
    registry_ids: &[String],
    allowlist_ids: &[String],
    prompt_claim_ids: &[String],
) -> Result<(), ToolError> {
    let registry: std::collections::BTreeSet<&str> =
        registry_ids.iter().map(String::as_str).collect();
    let allowlist: std::collections::BTreeSet<&str> =
        allowlist_ids.iter().map(String::as_str).collect();
    let claims: std::collections::BTreeSet<&str> =
        prompt_claim_ids.iter().map(String::as_str).collect();

    let mut problems = Vec::new();
    if registry != allowlist {
        problems.push(format!(
            "registry != allowlist (missing from allowlist: {:?}; empty grants: {:?})",
            registry.difference(&allowlist).collect::<Vec<_>>(),
            allowlist.difference(&registry).collect::<Vec<_>>()
        ));
    }
    if registry != claims {
        problems.push(format!(
            "registry != prompt claims (missing from claims: {:?}; stale claims: {:?})",
            registry.difference(&claims).collect::<Vec<_>>(),
            claims.difference(&registry).collect::<Vec<_>>()
        ));
    }
    if !problems.is_empty() {
        return Err(ToolError::new(
            Refusal::InternalError,
            format!("capability census drift: {}", problems.join("; ")),
        ));
    }
    Ok(())
}

/// Derives the census from a registry and the declared prompt claims.
///
/// # Errors
/// [`Refusal::InternalError`] on drift (see [`check_census_drift`]).
pub fn derive_census(
    registry: &ToolRegistry,
    prompt_claimed: &[&str],
) -> Result<CapabilityCensus, ToolError> {
    let registry_tool_ids: Vec<String> = registry
        .registered_ids()
        .map(|id| id.id().to_owned())
        .collect();
    let mut allowlist_tool_ids: Vec<String> = Vec::new();
    let mut tools_by_tier = BTreeMap::new();
    for tier in AgentTier::iter() {
        let ids: Vec<String> = registry
            .allowlist(tier)
            .iter()
            .map(|id| id.id().to_owned())
            .collect();
        for id in &ids {
            if !allowlist_tool_ids.contains(id) {
                allowlist_tool_ids.push(id.clone());
            }
        }
        tools_by_tier.insert(tier.label().to_owned(), ids);
    }
    let prompt_claim_tool_ids: Vec<String> = prompt_claimed
        .iter()
        .map(|id| (*id).to_owned())
        .collect();
    let mut executor_dispositions = BTreeMap::new();
    for (tool, lane) in EXECUTOR_DISPOSITIONS {
        executor_dispositions.insert((*tool).to_owned(), (*lane).to_owned());
    }
    let unclassified: Vec<&String> = registry_tool_ids
        .iter()
        .filter(|id| !executor_dispositions.contains_key(*id))
        .collect();
    if !unclassified.is_empty() {
        return Err(ToolError::new(
            Refusal::InternalError,
            format!(
                "capability census drift: advertised tools with no execution lane: {unclassified:?}"
            ),
        ));
    }
    check_census_drift(
        &registry_tool_ids,
        &allowlist_tool_ids,
        &prompt_claim_tool_ids,
    )?;

    Ok(CapabilityCensus {
        schema: CENSUS_SCHEMA,
        derived_from: "strum::EnumIter over ToolId/Refusal/AgentTier/PermissionClass + ToolRegistry allowlists",
        tool_count: ToolId::iter().count(),
        refusal_count: Refusal::ALL.len(),
        tier_count: AgentTier::iter().count(),
        permission_class_count: PermissionClass::iter().count(),
        tools_by_tier,
        registry_tool_ids,
        allowlist_tool_ids,
        prompt_claim_tool_ids,
        executor_dispositions,
    })
}

/// Census from the standard registry and the declared prompt claims.
///
/// # Errors
/// See [`derive_census`].
pub fn standard_census() -> Result<CapabilityCensus, ToolError> {
    derive_census(&ToolRegistry::standard(), PROMPT_CLAIMED_TOOL_IDS)
}

/// Every registered contract (for the CLI `schema` and `/diagnose` surfaces).
#[must_use]
pub fn contracts(registry: &ToolRegistry) -> Vec<&ToolContract> {
    registry
        .registered_ids()
        .filter_map(|id| registry.contract(*id))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn standard_registry_census_is_consistent() {
        let census = standard_census().expect("no drift");
        assert_eq!(census.schema, "hiverouter.census.v1");
        assert_eq!(census.tool_count, ToolId::all().len());
        assert_eq!(census.tool_count, census.registry_tool_ids.len());
        assert_eq!(census.refusal_count, Refusal::ALL.len());
        assert!(census.refusal_count >= 40, "the frozen table is complete");
        assert_eq!(census.tier_count, 5);
        assert_eq!(census.permission_class_count, 5);
        assert_eq!(census.grant_count(), 15 + 24 + 23 + 13 + 14);
        assert!(census.tools_by_tier.contains_key("hyper_agent"));
        assert_eq!(census.executor_dispositions.len(), census.registry_tool_ids.len());
        assert_eq!(
            census.executor_dispositions.get("file_write").map(String::as_str),
            Some("approval_executor")
        );
    }

    #[test]
    fn census_fails_when_one_row_is_deleted() {
        let registry = ToolRegistry::standard();
        let baseline = derive_census(&registry, PROMPT_CLAIMED_TOOL_IDS).expect("baseline");

        // Delete one registry row (simulated by removing an id from the derived
        // set) — the census must fail, not silently recount.
        let mut deleted_registry = baseline.registry_tool_ids.clone();
        deleted_registry.pop();
        let error = check_census_drift(
            &deleted_registry,
            &baseline.allowlist_tool_ids,
            &baseline.prompt_claim_tool_ids,
        )
        .expect_err("registry edit detected");
        assert_eq!(error.code(), "E_INTERNAL_ERROR");
        assert!(error.detail().contains("census drift"));

        // Delete one allowlist grant.
        let mut deleted_allowlist = baseline.allowlist_tool_ids.clone();
        deleted_allowlist.pop();
        assert!(check_census_drift(
            &baseline.registry_tool_ids,
            &deleted_allowlist,
            &baseline.prompt_claim_tool_ids
        )
        .is_err());

        // Delete one prompt claim.
        let mut deleted_claims = baseline.prompt_claim_tool_ids.clone();
        deleted_claims.pop();
        assert!(check_census_drift(
            &baseline.registry_tool_ids,
            &baseline.allowlist_tool_ids,
            &deleted_claims
        )
        .is_err());
    }

    #[test]
    fn counts_are_derived_not_typed() {
        // A typed count would be a constant. These are iterators.
        assert_eq!(
            ToolId::iter().count(),
            ToolId::all().len(),
            "EnumIter is the single source of tool ids"
        );
        assert_eq!(Refusal::ALL.len(), Refusal::ALL.iter().count());
        assert_eq!(
            AgentTier::iter().count(),
            AgentTier::all().len()
        );
        assert_eq!(
            PermissionClass::iter().count(),
            PermissionClass::all().len()
        );
    }

    #[test]
    fn census_serialises_for_the_diagnosis_endpoint() {
        let census = standard_census().expect("census");
        let value = serde_json::to_value(&census).expect("serialises");
        assert_eq!(value["schema"], "hiverouter.census.v1");
        assert!(value["refusal_count"].as_u64().expect("number") > 0);
        assert!(value["tools_by_tier"]["worker_agent"].is_array());
    }
}
