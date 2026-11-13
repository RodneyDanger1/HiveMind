//! CEO -> manager -> team lead -> assistant delegation protocol.
//!
//! This module is the bounded contract for one delegation tree:
//!
//! - a **manager** turns its brief into child [`Seat`]s, each with a plain-English
//!   bounded [`Seat::brief`], an owner [`SeatTier`], the tools that tier may hold,
//!   and sibling-only dependencies ([`decompose`]);
//! - the tree is validated against the same depth / fan-out / agent caps the
//!   engine enforces ([`DelegationTree::validate`], [`DelegationLimits::from_caps`]);
//! - results are received **bottom-up** ([`ReportLedger::submit`]): a child must
//!   report before its parent, a seat cannot be reported twice, a result must
//!   name a known seat, and a parent may not claim `Completed` over a child that
//!   did not complete;
//! - the completed tree is **synthesized** into a deterministic summary plus a
//!   root per-tree SHA-256 digest over the structure and every result
//!   ([`TreeSynthesis::root_digest`]).
//!
//! Failure/blocked seats are never counted as success: [`SeatCounts`] counts each
//! status separately and [`TreeSynthesis::succeeded`] is true only when every
//! seat in the tree (root included) is `Completed`.
//!
//! # Wiring (this change edits no other file)
//!
//! **`lib.rs`** — add `pub mod delegation_protocol;` next to `pub mod planner;`
//! (and, when convenient, re-export `DelegationTree`, `DelegationLimits`,
//! `SeatTier`, `ReportLedger`, `SeatReport`, `TreeSynthesis`).
//!
//! **`planner.rs`** — `seat.seat_id` is the `TaskId` that becomes
//! `PlanNode::node_id`; `seat.tier.planner_role()` is the canonical role string
//! the planner already accepts (`hyper`, `superagent`, `agent`, `subagent`); and
//! `seat.depth` is the plan depth. The CEO is the run root at depth 0 and is
//! never a plan node, so a manager seat at depth 1 becomes a depth-1
//! `superagent` node. Call `DelegationTree::validate(&registry, &limits)` before
//! the root row is persisted so an invalid delegation shape is refused at plan
//! time, exactly as `Plan::validate` refuses fan-out before dispatch.
//!
//! **`execution.rs`** — seats are task rows (`seat_id` is a real `TaskId`).
//! Build the ledger once with `ReportLedger::new(tree, &registry, &limits)` and
//! submit a report when each task settles: `AgentState::Completed` maps to
//! `SeatReport::completed`, while `Failed`/`Cancelled`/`Killed`/`Interrupted`
//! map to `SeatReport::failed`/`cancelled`/`blocked` (never to `completed`) with
//! the recorded `Refusal` when one exists. `submit` refuses a parent report that
//! arrives before its children and a parent that claims `Completed` over a
//! non-completed child. The run root's final turn should consume
//! `TreeSynthesis::summary`, and `Store::write_run_digest` should record
//! `TreeSynthesis::root_digest`: it is computed over the whole tree plus every
//! result, so an unchanged tree and results always produce the same digest and
//! any changed status or body changes it.
//!
//! **`assets/prompts/superagent.prompty`** — the manager prompt should require
//! child seats with a plain-English brief of 1..=[`MAX_BRIEF_CHARS`] characters
//! and at least [`MIN_BRIEF_WORDS`] words, an owner tier from the strict ladder,
//! tools from that tier's registry allowlist, and sibling-only dependencies.
//! `SeatTier::planner_role` is the bridge between the delegation vocabulary and
//! the role ladder the planner prompt already carries.

use std::cmp::Reverse;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;

use hiverouter_contract::{Refusal, TaskId};
use hiverouter_tools::{sha256_hex, AgentTier, ToolId, ToolRegistry};
use serde::{Deserialize, Serialize};

/// Maximum delegation depth: CEO (0) -> manager (1) -> team lead (2) ->
/// assistant (3). Mirrors `caps::MAX_SPAWN_DEPTH`.
pub const MAX_DELEGATION_DEPTH: u8 = 3;
/// Maximum children one seat may own. Mirrors `caps::MAX_FANOUT`.
pub const MAX_DELEGATION_FANOUT: usize = 8;
/// Maximum seats in one delegation tree, root included. Mirrors `caps::MAX_AGENTS`.
pub const MAX_DELEGATION_SEATS: usize = 64;
/// Maximum characters in one plain-English seat brief.
pub const MAX_BRIEF_CHARS: usize = 600;
/// Minimum plain-English words in one seat brief.
pub const MIN_BRIEF_WORDS: usize = 2;
/// Maximum characters in one accepted seat result.
pub const MAX_REPORT_CHARS: usize = 8_000;
/// Maximum characters in the synthesized summary.
pub const MAX_SUMMARY_CHARS: usize = 12_000;
/// Body characters retained per seat in the synthesized summary.
pub const SUMMARY_BODY_CHARS: usize = 400;
/// Domain separator for the root per-tree digest.
const DIGEST_DOMAIN: &str = "hivemind.delegation-tree.v1";

/// A named refusal plus the exact seat/reason that produced it.
///
/// The refusal code is the stable identity (`E_*`); `detail` is the human
/// context (which seat, which bound) and is never used for control flow.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DelegationError {
    refusal: Refusal,
    detail: String,
}

impl DelegationError {
    /// Builds an error from a named refusal and a context string.
    #[must_use]
    pub fn new(refusal: Refusal, detail: impl Into<String>) -> Self {
        Self {
            refusal,
            detail: detail.into(),
        }
    }

    /// The named refusal (stable `E_*` identity).
    #[must_use]
    pub fn refusal(&self) -> Refusal {
        self.refusal
    }

    /// The stable refusal code (`E_*`).
    #[must_use]
    pub fn code(&self) -> &'static str {
        self.refusal.code()
    }

    /// The human context for this refusal.
    #[must_use]
    pub fn detail(&self) -> &str {
        &self.detail
    }
}

impl fmt::Display for DelegationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.refusal.code(), self.detail)
    }
}

impl std::error::Error for DelegationError {}

fn invalid(detail: impl Into<String>) -> DelegationError {
    DelegationError::new(Refusal::SchemaInvalid, detail)
}

/// One rung of the delegation ladder.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SeatTier {
    /// Run owner; holds the root brief and the final synthesis.
    Ceo,
    /// Decomposes one objective into team leads.
    Manager,
    /// Owns one bounded deliverable and its assistants.
    TeamLead,
    /// Executes one bounded task; may not delegate further.
    Assistant,
}

impl SeatTier {
    /// Every tier, shallowest first.
    pub const ALL: &'static [SeatTier] = &[
        SeatTier::Ceo,
        SeatTier::Manager,
        SeatTier::TeamLead,
        SeatTier::Assistant,
    ];

    /// Ladder depth (CEO = 0, assistant = 3).
    #[must_use]
    pub fn depth(self) -> u8 {
        match self {
            SeatTier::Ceo => 0,
            SeatTier::Manager => 1,
            SeatTier::TeamLead => 2,
            SeatTier::Assistant => 3,
        }
    }

    /// Stable wire label.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            SeatTier::Ceo => "ceo",
            SeatTier::Manager => "manager",
            SeatTier::TeamLead => "team_lead",
            SeatTier::Assistant => "assistant",
        }
    }

    /// Parses a wire/planner label; accepts the contract and the planner role
    /// aliases so a plan built from `planner_role()` round-trips.
    #[must_use]
    pub fn from_label(label: &str) -> Option<Self> {
        match label
            .trim()
            .to_ascii_lowercase()
            .replace(['-', ' '], "_")
            .as_str()
        {
            "ceo" | "chief" | "chief_executive" | "chief_executive_officer" => Some(SeatTier::Ceo),
            "manager" | "super" | "superagent" | "super_agent" => Some(SeatTier::Manager),
            "team_lead" | "teamlead" | "lead" | "agent" | "worker" | "worker_agent" => {
                Some(SeatTier::TeamLead)
            }
            "assistant" | "subagent" | "sub_agent" => Some(SeatTier::Assistant),
            _ => None,
        }
    }

    /// The only tier this tier may delegate to; `None` for an assistant.
    #[must_use]
    pub fn direct_child(self) -> Option<SeatTier> {
        match self {
            SeatTier::Ceo => Some(SeatTier::Manager),
            SeatTier::Manager => Some(SeatTier::TeamLead),
            SeatTier::TeamLead => Some(SeatTier::Assistant),
            SeatTier::Assistant => None,
        }
    }

    /// The engine tier whose tool allowlist this seat inherits.
    #[must_use]
    pub fn agent_tier(self) -> AgentTier {
        match self {
            SeatTier::Ceo => AgentTier::HyperAgent,
            SeatTier::Manager => AgentTier::SuperAgent,
            SeatTier::TeamLead => AgentTier::WorkerAgent,
            SeatTier::Assistant => AgentTier::Subagent,
        }
    }

    /// Canonical planner role for this tier (`planner::PlanNode::role`).
    #[must_use]
    pub fn planner_role(self) -> &'static str {
        match self {
            SeatTier::Ceo => "hyper",
            SeatTier::Manager => "superagent",
            SeatTier::TeamLead => "agent",
            SeatTier::Assistant => "subagent",
        }
    }

    /// Every tool the engine grants this tier, in registry order.
    #[must_use]
    pub fn allowed_tools(self, registry: &ToolRegistry) -> Vec<ToolId> {
        registry.allowlist(self.agent_tier()).to_vec()
    }
}

/// One delegated seat: a bounded brief, an owner tier, the owner's tools, and
/// the sibling seats whose results it consumes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Seat {
    /// Seat id; this is the task id the engine persists.
    pub seat_id: TaskId,
    /// Parent seat (`None` only for the CEO root).
    pub parent_seat_id: Option<TaskId>,
    /// Owner tier.
    pub tier: SeatTier,
    /// Ladder depth (CEO = 0).
    pub depth: u8,
    /// Plain-English brief; validated by [`validate_brief`].
    pub brief: String,
    /// Tools the owner may use; each must be on the tier allowlist.
    pub tools: Vec<ToolId>,
    /// Sibling seats whose results must land first.
    pub depends_on: Vec<TaskId>,
}

impl Seat {
    /// Creates the CEO root seat with no tools and no dependencies.
    #[must_use]
    pub fn root(seat_id: TaskId, brief: impl Into<String>) -> Self {
        Self {
            seat_id,
            parent_seat_id: None,
            tier: SeatTier::Ceo,
            depth: 0,
            brief: brief.into(),
            tools: Vec::new(),
            depends_on: Vec::new(),
        }
    }

    /// Creates the CEO root seat with an explicit tool set.
    #[must_use]
    pub fn root_with_tools(seat_id: TaskId, brief: impl Into<String>, tools: Vec<ToolId>) -> Self {
        Self {
            tools,
            ..Self::root(seat_id, brief)
        }
    }

    /// Creates a child seat one rung below `parent`.
    ///
    /// This derives the tier and depth from the parent so the ladder can never
    /// skip a level at construction time; the full tree is still validated by
    /// [`DelegationTree::validate`].
    ///
    /// # Errors
    /// `E_TOOL_NOT_ALLOWED` when `parent` is an assistant (it may not delegate);
    /// `E_SPAWN_DEPTH` when the depth counter would overflow.
    pub fn under(
        parent: &Seat,
        seat_id: TaskId,
        brief: impl Into<String>,
        tools: Vec<ToolId>,
        depends_on: Vec<TaskId>,
    ) -> Result<Self, DelegationError> {
        let tier = parent.tier.direct_child().ok_or_else(|| {
            DelegationError::new(
                Refusal::ToolNotAllowed,
                format!(
                    "{} seat {} may not delegate: the ladder ends at assistant",
                    parent.tier.label(),
                    parent.seat_id
                ),
            )
        })?;
        let depth = parent.depth.checked_add(1).ok_or_else(|| {
            DelegationError::new(
                Refusal::SpawnDepth,
                format!("seat {} depth would overflow", parent.seat_id),
            )
        })?;
        Ok(Self {
            seat_id,
            parent_seat_id: Some(parent.seat_id),
            tier,
            depth,
            brief: brief.into(),
            tools,
            depends_on,
        })
    }

    /// Whether this is the CEO root.
    #[must_use]
    pub fn is_root(&self) -> bool {
        self.parent_seat_id.is_none()
    }
}

/// The manager's request for one child seat.
///
/// The tier is **not** part of the request: it is derived from the manager's
/// rung, so a manager cannot ask for a level it may not own.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SeatSpec {
    /// Requested seat id (must be unique in the tree).
    pub seat_id: TaskId,
    /// Plain-English brief; validated before a seat exists.
    pub brief: String,
    /// Tools requested for the child; bounded by the child tier allowlist.
    pub tools: Vec<ToolId>,
    /// Sibling seats in the same decomposition whose results must land first.
    #[serde(default)]
    pub depends_on: Vec<TaskId>,
}

/// A validated decomposition: the manager plus its new child seats.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Decomposition {
    /// The manager seat that owns these children.
    pub manager_seat_id: TaskId,
    /// Validated children, in the caller's order.
    pub children: Vec<Seat>,
}

impl Decomposition {
    /// Looks a child up by id.
    #[must_use]
    pub fn child(&self, seat_id: TaskId) -> Option<&Seat> {
        self.children.iter().find(|seat| seat.seat_id == seat_id)
    }

    /// Child ids in declaration order.
    #[must_use]
    pub fn ids(&self) -> Vec<TaskId> {
        self.children.iter().map(|seat| seat.seat_id).collect()
    }
}

/// Effective delegation bounds.
///
/// The defaults mirror [`crate::caps`] (once wired); use
/// [`DelegationLimits::from_caps`] to derive them from the run's live caps so
/// the protocol and the engine can never disagree.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DelegationLimits {
    /// Maximum ladder depth (assistant = 3).
    pub max_depth: u8,
    /// Maximum children per seat.
    pub max_fanout: usize,
    /// Maximum seats in the tree, root included.
    pub max_seats: usize,
    /// Maximum characters per brief.
    pub max_brief_chars: usize,
    /// Maximum characters per accepted result body.
    pub max_report_chars: usize,
}

impl Default for DelegationLimits {
    fn default() -> Self {
        Self {
            max_depth: MAX_DELEGATION_DEPTH,
            max_fanout: MAX_DELEGATION_FANOUT,
            max_seats: MAX_DELEGATION_SEATS,
            max_brief_chars: MAX_BRIEF_CHARS,
            max_report_chars: MAX_REPORT_CHARS,
        }
    }
}

impl DelegationLimits {
    /// Derives the delegation bounds from the engine caps.
    ///
    /// The wiring call is
    /// `DelegationLimits::from_caps(caps.max_depth, caps.max_fanout, caps.max_agents)`.
    #[must_use]
    pub fn from_caps(max_depth: u8, max_fanout: usize, max_agents: u64) -> Self {
        Self {
            max_depth,
            max_fanout,
            max_seats: usize::try_from(max_agents).unwrap_or(usize::MAX),
            ..Self::default()
        }
    }
}

/// Validates one plain-English brief.
///
/// Rules: non-empty after trimming, at least [`MIN_BRIEF_WORDS`] alphabetic
/// words, no control characters other than newline/tab, and at most
/// `limits.max_brief_chars` characters.
///
/// # Errors
/// `E_SCHEMA_INVALID` with the exact failing rule in [`DelegationError::detail`].
pub fn validate_brief(
    brief: &str,
    limits: &DelegationLimits,
) -> Result<(), DelegationError> {
    let trimmed = brief.trim();
    if trimmed.is_empty() {
        return Err(invalid(
            "brief is empty: every delegated seat needs a plain-English instruction",
        ));
    }
    if trimmed.chars().any(|character| {
        character.is_control() && character != '\n' && character != '\t'
    }) {
        return Err(invalid(
            "brief contains a control character; send plain text only",
        ));
    }
    let characters = trimmed.chars().count();
    if characters > limits.max_brief_chars {
        return Err(invalid(format!(
            "brief is {characters} characters; the cap is {}",
            limits.max_brief_chars
        )));
    }
    let words = trimmed
        .split_whitespace()
        .filter(|word| word.chars().any(char::is_alphabetic))
        .count();
    if words < MIN_BRIEF_WORDS {
        return Err(invalid(format!(
            "brief has {words} plain-English word(s); at least {MIN_BRIEF_WORDS} are required"
        )));
    }
    Ok(())
}

/// Validates a tool grant against the seat tier's real registry allowlist.
///
/// The vendor allowlist in `hiverouter-tools` is the authority: this only checks
/// membership, so a delegation can never widen a tier's capability.
///
/// # Errors
/// `E_TOOL_NOT_FOUND` for an unregistered id, `E_TOOL_NOT_ALLOWED` for a tool
/// outside the tier, `E_SCHEMA_INVALID` for a duplicate grant.
pub fn validate_tools(
    registry: &ToolRegistry,
    tier: SeatTier,
    tools: &[ToolId],
) -> Result<(), DelegationError> {
    let mut seen = HashSet::with_capacity(tools.len());
    for tool in tools {
        if !seen.insert(*tool) {
            return Err(invalid(format!(
                "tool {} is granted twice to a {} seat",
                tool.id(),
                tier.label()
            )));
        }
        if registry.contract(*tool).is_none() {
            return Err(DelegationError::new(
                Refusal::ToolNotFound,
                format!("tool {} is not registered", tool.id()),
            ));
        }
        if !registry.is_allowed(tier.agent_tier(), *tool) {
            return Err(DelegationError::new(
                Refusal::ToolNotAllowed,
                format!(
                    "tool {} is not on the {} allowlist",
                    tool.id(),
                    tier.agent_tier().label()
                ),
            ));
        }
    }
    Ok(())
}

/// Decomposes a manager's brief into validated, bounded child seats.
///
/// The child tier is derived from the manager's rung (CEO -> manager -> team
/// lead -> assistant); the child depth is the manager depth + 1. The whole
/// decomposition is refused before anything is persisted if it breaks the
/// ladder, the depth cap, the fan-out cap, brief bounds, the tool allowlist, or
/// the sibling dependency DAG.
///
/// # Errors
/// `E_TOOL_NOT_ALLOWED` when the manager is an assistant or requests a tier its
/// rung cannot own, or a tool outside the child tier; `E_SPAWN_DEPTH` past the
/// depth cap; `E_FANOUT_LIMIT` over the fan-out cap; `E_CYCLE` for a dependency
/// cycle; `E_TASK_NOT_FOUND` for a dependency outside the decomposition;
/// `E_SCHEMA_INVALID` for empty input, duplicate/self ids, duplicate deps, or an
/// invalid brief.
pub fn decompose(
    manager: &Seat,
    specs: &[SeatSpec],
    registry: &ToolRegistry,
    limits: &DelegationLimits,
) -> Result<Decomposition, DelegationError> {
    if manager.tier.depth() != manager.depth {
        return Err(invalid(format!(
            "{} seat {} claims depth {}",
            manager.tier.label(),
            manager.seat_id,
            manager.depth
        )));
    }
    validate_brief(&manager.brief, limits)?;
    let child_tier = manager.tier.direct_child().ok_or_else(|| {
        DelegationError::new(
            Refusal::ToolNotAllowed,
            format!(
                "{} seat {} may not decompose: the ladder ends at assistant",
                manager.tier.label(),
                manager.seat_id
            ),
        )
    })?;
    if specs.is_empty() {
        return Err(invalid(format!(
            "manager {} decomposed into zero seats",
            manager.seat_id
        )));
    }
    if specs.len() > limits.max_fanout {
        return Err(DelegationError::new(
            Refusal::FanoutLimit,
            format!(
                "manager {} requested {} children; the cap is {}",
                manager.seat_id,
                specs.len(),
                limits.max_fanout
            ),
        ));
    }
    let child_depth = manager.depth.checked_add(1).ok_or_else(|| {
        DelegationError::new(
            Refusal::SpawnDepth,
            format!("manager {} depth would overflow", manager.seat_id),
        )
    })?;
    if child_depth > limits.max_depth {
        return Err(DelegationError::new(
            Refusal::SpawnDepth,
            format!(
                "manager {} would create depth {child_depth}; the cap is {}",
                manager.seat_id, limits.max_depth
            ),
        ));
    }
    let mut ids = HashSet::with_capacity(specs.len());
    for spec in specs {
        if spec.seat_id == manager.seat_id || Some(spec.seat_id) == manager.parent_seat_id {
            return Err(invalid(format!(
                "child id {} collides with an ancestor of manager {}",
                spec.seat_id, manager.seat_id
            )));
        }
        if !ids.insert(spec.seat_id) {
            return Err(invalid(format!(
                "duplicate child seat id {} in one decomposition",
                spec.seat_id
            )));
        }
    }
    let mut dependency_map: HashMap<TaskId, Vec<TaskId>> = HashMap::with_capacity(specs.len());
    for spec in specs {
        let mut unique = HashSet::with_capacity(spec.depends_on.len());
        for dependency in &spec.depends_on {
            if *dependency == spec.seat_id {
                return Err(DelegationError::new(
                    Refusal::Cycle,
                    format!("seat {} depends on itself", spec.seat_id),
                ));
            }
            if !ids.contains(dependency) {
                return Err(DelegationError::new(
                    Refusal::TaskNotFound,
                    format!(
                        "seat {} depends on {} which is not in this decomposition",
                        spec.seat_id, dependency
                    ),
                ));
            }
            if !unique.insert(*dependency) {
                return Err(invalid(format!(
                    "seat {} repeats dependency {}",
                    spec.seat_id, dependency
                )));
            }
        }
        dependency_map.insert(spec.seat_id, spec.depends_on.clone());
    }
    if has_dependency_cycle(&specs.iter().map(|spec| spec.seat_id).collect::<Vec<_>>(), &dependency_map)
    {
        return Err(DelegationError::new(
            Refusal::Cycle,
            format!("manager {} children form a dependency cycle", manager.seat_id),
        ));
    }
    let mut children = Vec::with_capacity(specs.len());
    for spec in specs {
        validate_brief(&spec.brief, limits)?;
        validate_tools(registry, child_tier, &spec.tools)?;
        children.push(Seat {
            seat_id: spec.seat_id,
            parent_seat_id: Some(manager.seat_id),
            tier: child_tier,
            depth: child_depth,
            brief: spec.brief.clone(),
            tools: spec.tools.clone(),
            depends_on: spec.depends_on.clone(),
        });
    }
    Ok(Decomposition {
        manager_seat_id: manager.seat_id,
        children,
    })
}

/// One validated delegation tree.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DelegationTree {
    seats: Vec<Seat>,
}

impl DelegationTree {
    /// Builds a tree from seats; call [`DelegationTree::validate`] before use.
    #[must_use]
    pub fn new(seats: Vec<Seat>) -> Self {
        Self { seats }
    }

    /// All seats in declaration order.
    #[must_use]
    pub fn seats(&self) -> &[Seat] {
        &self.seats
    }

    /// The single CEO root, if the tree has one.
    #[must_use]
    pub fn root(&self) -> Option<&Seat> {
        self.seats.iter().find(|seat| seat.is_root())
    }

    /// Looks a seat up by id.
    #[must_use]
    pub fn seat(&self, seat_id: TaskId) -> Option<&Seat> {
        self.seats.iter().find(|seat| seat.seat_id == seat_id)
    }

    /// Direct children of a seat, ordered by seat id.
    #[must_use]
    pub fn children_of(&self, seat_id: TaskId) -> Vec<&Seat> {
        let mut children: Vec<&Seat> = self
            .seats
            .iter()
            .filter(|seat| seat.parent_seat_id == Some(seat_id))
            .collect();
        children.sort_by_key(|seat| seat.seat_id);
        children
    }

    /// Shallowest-first deterministic seat order.
    #[must_use]
    pub fn depth_order(&self) -> Vec<TaskId> {
        let mut ordered: Vec<&Seat> = self.seats.iter().collect();
        ordered.sort_by_key(|seat| (seat.depth, seat.seat_id));
        ordered.into_iter().map(|seat| seat.seat_id).collect()
    }

    /// Deepest-first deterministic seat order (bottom-up synthesis).
    #[must_use]
    pub fn bottom_up_order(&self) -> Vec<TaskId> {
        let mut ordered: Vec<&Seat> = self.seats.iter().collect();
        ordered.sort_by_key(|seat| (Reverse(seat.depth), seat.seat_id));
        ordered.into_iter().map(|seat| seat.seat_id).collect()
    }

    /// Validates the whole tree before a single task row is persisted.
    ///
    /// Checks: exactly one CEO root at depth 0; unique ids; a strict ladder edge
    /// (tier *and* depth) between every parent and child; an acyclic parent
    /// chain; the seat cap; the per-parent fan-out cap; sibling-only,
    /// duplicate-free, acyclic dependencies; valid briefs; and tier-bounded
    /// tools.
    ///
    /// # Errors
    /// `E_SCHEMA_INVALID`, `E_TASK_NOT_FOUND`, `E_TOOL_NOT_ALLOWED`,
    /// `E_SPAWN_DEPTH`, `E_FANOUT_LIMIT`, `E_AGENT_LIMIT`, or `E_CYCLE` with the
    /// offending seat in the detail.
    pub fn validate(
        &self,
        registry: &ToolRegistry,
        limits: &DelegationLimits,
    ) -> Result<(), DelegationError> {
        if self.seats.is_empty() {
            return Err(invalid("a delegation tree needs at least the CEO seat"));
        }
        if self.seats.len() > limits.max_seats {
            return Err(DelegationError::new(
                Refusal::AgentLimit,
                format!(
                    "tree has {} seats; the cap is {}",
                    self.seats.len(),
                    limits.max_seats
                ),
            ));
        }
        let mut by_id: HashMap<TaskId, &Seat> = HashMap::with_capacity(self.seats.len());
        for seat in &self.seats {
            if by_id.insert(seat.seat_id, seat).is_some() {
                return Err(invalid(format!("duplicate seat id {}", seat.seat_id)));
            }
        }
        let mut parentless = self.seats.iter().filter(|seat| seat.is_root());
        let root = parentless
            .next()
            .ok_or_else(|| invalid("tree has no CEO root (no parentless seat)"))?;
        if parentless.next().is_some() {
            return Err(invalid("tree has more than one parentless seat"));
        }
        if root.tier != SeatTier::Ceo || root.depth != 0 {
            return Err(invalid(format!(
                "root {} must be the CEO at depth 0, not a {} at depth {}",
                root.seat_id,
                root.tier.label(),
                root.depth
            )));
        }
        for seat in &self.seats {
            self.validate_seat(seat, &by_id, registry, limits)?;
        }
        for seat in &self.seats {
            let mut seen = HashSet::new();
            let mut cursor = Some(seat.seat_id);
            while let Some(id) = cursor {
                if !seen.insert(id) {
                    return Err(invalid(format!(
                        "parent chain of seat {} forms a cycle",
                        seat.seat_id
                    )));
                }
                cursor = by_id.get(&id).and_then(|node| node.parent_seat_id);
            }
        }
        let mut children_per_parent: HashMap<TaskId, usize> =
            HashMap::with_capacity(self.seats.len());
        for seat in &self.seats {
            if let Some(parent) = seat.parent_seat_id {
                *children_per_parent.entry(parent).or_insert(0) += 1;
            }
        }
        if let Some((parent, count)) = children_per_parent
            .iter()
            .find(|(_, count)| **count > limits.max_fanout)
        {
            return Err(DelegationError::new(
                Refusal::FanoutLimit,
                format!(
                    "seat {parent} owns {count} children; the cap is {}",
                    limits.max_fanout
                ),
            ));
        }
        let mut groups: HashMap<Option<TaskId>, Vec<&Seat>> =
            HashMap::with_capacity(self.seats.len());
        for seat in &self.seats {
            groups.entry(seat.parent_seat_id).or_default().push(seat);
        }
        for (parent, group) in &groups {
            if parent.is_none() {
                continue;
            }
            let ids: Vec<TaskId> = group.iter().map(|seat| seat.seat_id).collect();
            let dependency_map: HashMap<TaskId, Vec<TaskId>> = group
                .iter()
                .map(|seat| (seat.seat_id, seat.depends_on.clone()))
                .collect();
            if has_dependency_cycle(&ids, &dependency_map) {
                return Err(DelegationError::new(
                    Refusal::Cycle,
                    format!(
                        "children of {} form a dependency cycle",
                        parent.expect("non-root group has a parent")
                    ),
                ));
            }
        }
        Ok(())
    }

    fn validate_seat(
        &self,
        seat: &Seat,
        by_id: &HashMap<TaskId, &Seat>,
        registry: &ToolRegistry,
        limits: &DelegationLimits,
    ) -> Result<(), DelegationError> {
        if seat.tier.depth() != seat.depth {
            return Err(invalid(format!(
                "{} seat {} claims depth {}",
                seat.tier.label(),
                seat.seat_id,
                seat.depth
            )));
        }
        if seat.depth > limits.max_depth {
            return Err(DelegationError::new(
                Refusal::SpawnDepth,
                format!(
                    "seat {} is at depth {}; the cap is {}",
                    seat.seat_id, seat.depth, limits.max_depth
                ),
            ));
        }
        validate_brief(&seat.brief, limits)?;
        validate_tools(registry, seat.tier, &seat.tools)?;
        let mut unique_dependencies = HashSet::with_capacity(seat.depends_on.len());
        for dependency in &seat.depends_on {
            if *dependency == seat.seat_id {
                return Err(DelegationError::new(
                    Refusal::Cycle,
                    format!("seat {} depends on itself", seat.seat_id),
                ));
            }
            if !unique_dependencies.insert(*dependency) {
                return Err(invalid(format!(
                    "seat {} repeats dependency {}",
                    seat.seat_id, dependency
                )));
            }
            let dependency_seat = by_id.get(dependency).ok_or_else(|| {
                DelegationError::new(
                    Refusal::TaskNotFound,
                    format!(
                        "seat {} depends on {} which is not in the tree",
                        seat.seat_id, dependency
                    ),
                )
            })?;
            if dependency_seat.parent_seat_id != seat.parent_seat_id {
                return Err(invalid(format!(
                    "seat {} depends on {} outside its sibling group",
                    seat.seat_id, dependency
                )));
            }
        }
        match seat.parent_seat_id {
            None => {
                if !seat.depends_on.is_empty() {
                    return Err(invalid(format!(
                        "CEO root {} may not depend on a seat",
                        seat.seat_id
                    )));
                }
            }
            Some(parent_id) => {
                let parent = by_id.get(&parent_id).ok_or_else(|| {
                    DelegationError::new(
                        Refusal::TaskNotFound,
                        format!("seat {} names missing parent {parent_id}", seat.seat_id),
                    )
                })?;
                if parent.tier.direct_child() != Some(seat.tier) {
                    return Err(DelegationError::new(
                        Refusal::ToolNotAllowed,
                        format!(
                            "{} seat {} cannot own {} seat {}",
                            parent.tier.label(),
                            parent.seat_id,
                            seat.tier.label(),
                            seat.seat_id
                        ),
                    ));
                }
                if parent.depth.checked_add(1) != Some(seat.depth) {
                    return Err(invalid(format!(
                        "seat {} is at depth {} under parent {} at depth {}",
                        seat.seat_id, seat.depth, parent.seat_id, parent.depth
                    )));
                }
            }
        }
        Ok(())
    }

    /// Starts a validated report ledger for this tree.
    ///
    /// # Errors
    /// Every refusal [`DelegationTree::validate`] can produce.
    pub fn report_ledger(
        self,
        registry: &ToolRegistry,
        limits: &DelegationLimits,
    ) -> Result<ReportLedger, DelegationError> {
        ReportLedger::new(self, registry, limits)
    }
}

impl Default for DelegationTree {
    fn default() -> Self {
        Self { seats: Vec::new() }
    }
}

/// Kahn cycle check over one sibling group (or one decomposition).
fn has_dependency_cycle(ids: &[TaskId], dependencies: &HashMap<TaskId, Vec<TaskId>>) -> bool {
    let mut indegree: HashMap<TaskId, usize> = ids
        .iter()
        .map(|id| (*id, dependencies.get(id).map_or(0, Vec::len)))
        .collect();
    let mut ready: Vec<TaskId> = indegree
        .iter()
        .filter(|(_, count)| **count == 0)
        .map(|(id, _)| *id)
        .collect();
    let mut settled = 0usize;
    while let Some(id) = ready.pop() {
        settled += 1;
        for candidate in ids {
            if dependencies
                .get(candidate)
                .is_some_and(|list| list.contains(&id))
            {
                if let Some(entry) = indegree.get_mut(candidate) {
                    *entry = entry.saturating_sub(1);
                    if *entry == 0 {
                        ready.push(*candidate);
                    }
                }
            }
        }
    }
    settled != ids.len()
}

/// Terminal status of one seat's result.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SeatStatus {
    /// The seat finished its brief and produced a result.
    Completed,
    /// The seat failed for a recorded reason.
    Failed,
    /// The seat could not start or finish because an input/tool was missing.
    Blocked,
    /// The seat was cancelled/killed/interrupted before finishing.
    Cancelled,
}

impl SeatStatus {
    /// Stable label.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            SeatStatus::Completed => "completed",
            SeatStatus::Failed => "failed",
            SeatStatus::Blocked => "blocked",
            SeatStatus::Cancelled => "cancelled",
        }
    }

    /// Only a completed seat counts as success.
    #[must_use]
    pub fn is_success(self) -> bool {
        matches!(self, SeatStatus::Completed)
    }
}

/// One seat's bottom-up result.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SeatReport {
    /// The seat this result belongs to.
    pub seat_id: TaskId,
    /// Terminal status.
    pub status: SeatStatus,
    /// Result text (or the failure/blocked explanation).
    pub body: String,
    /// The recorded named refusal, when the engine has one.
    #[serde(default)]
    pub refusal: Option<Refusal>,
}

impl SeatReport {
    /// A completed result. The body must be non-empty for the report to be
    /// accepted.
    #[must_use]
    pub fn completed(seat_id: TaskId, body: impl Into<String>) -> Self {
        Self {
            seat_id,
            status: SeatStatus::Completed,
            body: body.into(),
            refusal: None,
        }
    }

    /// A failed result with the recorded refusal.
    #[must_use]
    pub fn failed(seat_id: TaskId, refusal: Refusal, body: impl Into<String>) -> Self {
        Self {
            seat_id,
            status: SeatStatus::Failed,
            body: body.into(),
            refusal: Some(refusal),
        }
    }

    /// A blocked result: an input or tool the seat could not obtain.
    #[must_use]
    pub fn blocked(seat_id: TaskId, body: impl Into<String>) -> Self {
        Self {
            seat_id,
            status: SeatStatus::Blocked,
            body: body.into(),
            refusal: None,
        }
    }

    /// Attaches a recorded refusal to a blocked/cancelled report.
    #[must_use]
    pub fn with_refusal(mut self, refusal: Refusal) -> Self {
        self.refusal = Some(refusal);
        self
    }

    /// A cancelled result.
    #[must_use]
    pub fn cancelled(seat_id: TaskId, body: impl Into<String>) -> Self {
        Self {
            seat_id,
            status: SeatStatus::Cancelled,
            body: body.into(),
            refusal: None,
        }
    }
}

/// Per-status counts over the whole tree (root included).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SeatCounts {
    /// Total seats.
    pub total: usize,
    /// Seats that completed.
    pub completed: usize,
    /// Seats that failed.
    pub failed: usize,
    /// Seats that blocked.
    pub blocked: usize,
    /// Seats that were cancelled.
    pub cancelled: usize,
}

impl SeatCounts {
    /// True when every seat completed.
    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.total > 0 && self.completed == self.total
    }
}

/// One seat's contribution to the synthesis.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SeatSynthesisEntry {
    /// Seat id.
    pub seat_id: TaskId,
    /// Owner tier.
    pub tier: SeatTier,
    /// Ladder depth.
    pub depth: u8,
    /// Terminal status.
    pub status: SeatStatus,
    /// SHA-256 of the exact accepted result body.
    pub body_sha256: String,
    /// Result body length in characters.
    pub body_chars: usize,
    /// Recorded refusal, when any.
    pub refusal: Option<Refusal>,
}

/// Deterministic synthesis of one fully reported delegation tree.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TreeSynthesis {
    /// CEO root seat id.
    pub tree_root: TaskId,
    /// Root per-tree digest over the structure and every result.
    pub root_digest: String,
    /// Per-status counts.
    pub counts: SeatCounts,
    /// True only when every seat completed.
    pub succeeded: bool,
    /// Per-seat entries, deepest first (bottom-up).
    pub entries: Vec<SeatSynthesisEntry>,
    /// Human-readable bottom-up synthesis for the root prompt.
    pub summary: String,
}

/// Receives seat results bottom-up and refuses lies.
#[derive(Clone, Debug)]
pub struct ReportLedger {
    tree: DelegationTree,
    limits: DelegationLimits,
    reports: BTreeMap<TaskId, SeatReport>,
}

impl ReportLedger {
    /// Validates the tree and opens an empty ledger.
    ///
    /// # Errors
    /// Every refusal [`DelegationTree::validate`] can produce.
    pub fn new(
        tree: DelegationTree,
        registry: &ToolRegistry,
        limits: &DelegationLimits,
    ) -> Result<Self, DelegationError> {
        tree.validate(registry, limits)?;
        Ok(Self {
            tree,
            limits: *limits,
            reports: BTreeMap::new(),
        })
    }

    /// The validated tree.
    #[must_use]
    pub fn tree(&self) -> &DelegationTree {
        &self.tree
    }

    /// The report for a seat, if it has been accepted.
    #[must_use]
    pub fn report(&self, seat_id: TaskId) -> Option<&SeatReport> {
        self.reports.get(&seat_id)
    }

    /// Whether every seat has reported.
    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.reports.len() == self.tree.seats().len()
    }

    /// Seats that have not reported yet, shallowest first.
    #[must_use]
    pub fn pending_seats(&self) -> Vec<TaskId> {
        self.tree
            .depth_order()
            .into_iter()
            .filter(|seat_id| !self.reports.contains_key(seat_id))
            .collect()
    }

    /// Accepts one seat result.
    ///
    /// Enforced here: the seat must exist in the tree, it may report once, the
    /// report shape must be honest (no `Completed` with a refusal or an empty
    /// body), every direct child must already have reported, and a parent may
    /// not claim `Completed` while a direct child did not complete.
    ///
    /// # Errors
    /// `E_TASK_NOT_FOUND` for an unknown seat; `E_SCHEMA_INVALID` for a duplicate
    /// result, an out-of-order (parent before child) result, an empty completed
    /// body, an oversize body, or a `Completed` claim over a non-completed child.
    pub fn submit(&mut self, report: SeatReport) -> Result<(), DelegationError> {
        let seat_id = report.seat_id;
        if self.tree.seat(seat_id).is_none() {
            return Err(DelegationError::new(
                Refusal::TaskNotFound,
                format!("result references unknown seat {seat_id}"),
            ));
        }
        if self.reports.contains_key(&seat_id) {
            return Err(invalid(format!("duplicate result for seat {seat_id}")));
        }
        validate_report(&report, &self.limits)?;
        let children: Vec<TaskId> = self
            .tree
            .children_of(seat_id)
            .into_iter()
            .map(|seat| seat.seat_id)
            .collect();
        for child_id in &children {
            let child_report = self.reports.get(child_id).ok_or_else(|| {
                invalid(format!(
                    "bottom-up violation: seat {seat_id} reported before child {child_id}"
                ))
            })?;
            if report.status == SeatStatus::Completed
                && child_report.status != SeatStatus::Completed
            {
                return Err(invalid(format!(
                    "seat {seat_id} cannot be Completed while child {child_id} is {}",
                    child_report.status.label()
                )));
            }
        }
        self.reports.insert(seat_id, report);
        Ok(())
    }

    /// Consumes the ledger and synthesizes the tree.
    ///
    /// # Errors
    /// `E_SCHEMA_INVALID` when any seat is still missing a result;
    /// `E_INTERNAL_ERROR` if a validated tree/report pair cannot be re-read.
    pub fn synthesize(self) -> Result<TreeSynthesis, DelegationError> {
        let total = self.tree.seats().len();
        if self.reports.len() != total {
            return Err(invalid(format!(
                "cannot synthesize: {} of {total} seats reported",
                self.reports.len()
            )));
        }
        build_synthesis(&self.tree, &self.reports)
    }
}

fn validate_report(
    report: &SeatReport,
    limits: &DelegationLimits,
) -> Result<(), DelegationError> {
    let characters = report.body.chars().count();
    if characters > limits.max_report_chars {
        return Err(invalid(format!(
            "seat {} result is {characters} characters; the cap is {}",
            report.seat_id, limits.max_report_chars
        )));
    }
    match report.status {
        SeatStatus::Completed => {
            if let Some(refusal) = report.refusal {
                return Err(invalid(format!(
                    "seat {} is Completed but carries refusal {}",
                    report.seat_id,
                    refusal.code()
                )));
            }
            if report.body.trim().is_empty() {
                return Err(invalid(format!(
                    "seat {} is Completed with an empty result",
                    report.seat_id
                )));
            }
        }
        SeatStatus::Failed => {
            if report.body.trim().is_empty() {
                return Err(invalid(format!(
                    "seat {} is Failed with an empty explanation",
                    report.seat_id
                )));
            }
        }
        SeatStatus::Blocked => {
            if report.body.trim().is_empty() {
                return Err(invalid(format!(
                    "seat {} is Blocked with an empty reason",
                    report.seat_id
                )));
            }
        }
        SeatStatus::Cancelled => {}
    }
    Ok(())
}

/// Submits a complete batch of results bottom-up and synthesizes the tree.
///
/// The batch may arrive in any order: reports are sorted deepest-first before
/// submission, so the ledger's bottom-up rule still holds. Duplicate or unknown
/// seats are refused exactly as in [`ReportLedger::submit`].
///
/// # Errors
/// Every refusal from [`ReportLedger::new`], [`ReportLedger::submit`] and
/// [`ReportLedger::synthesize`].
pub fn synthesize_batch(
    tree: DelegationTree,
    registry: &ToolRegistry,
    limits: &DelegationLimits,
    reports: Vec<SeatReport>,
) -> Result<TreeSynthesis, DelegationError> {
    let mut ledger = ReportLedger::new(tree, registry, limits)?;
    let depths: HashMap<TaskId, u8> = ledger
        .tree()
        .seats()
        .iter()
        .map(|seat| (seat.seat_id, seat.depth))
        .collect();
    let mut ordered = reports;
    ordered.sort_by_key(|report| {
        (
            Reverse(depths.get(&report.seat_id).copied().unwrap_or(0)),
            report.seat_id,
        )
    });
    for report in ordered {
        ledger.submit(report)?;
    }
    ledger.synthesize()
}

fn build_synthesis(
    tree: &DelegationTree,
    reports: &BTreeMap<TaskId, SeatReport>,
) -> Result<TreeSynthesis, DelegationError> {
    let root = tree
        .root()
        .ok_or_else(|| DelegationError::new(Refusal::InternalError, "validated tree has no root"))?;
    let mut counts = SeatCounts::default();
    let mut entries = Vec::with_capacity(tree.seats().len());
    for seat_id in tree.bottom_up_order() {
        let seat = tree.seat(seat_id).ok_or_else(|| {
            DelegationError::new(
                Refusal::InternalError,
                format!("seat {seat_id} vanished from a validated tree"),
            )
        })?;
        let report = reports.get(&seat_id).ok_or_else(|| {
            DelegationError::new(
                Refusal::InternalError,
                format!("seat {seat_id} has no report during synthesis"),
            )
        })?;
        match report.status {
            SeatStatus::Completed => counts.completed += 1,
            SeatStatus::Failed => counts.failed += 1,
            SeatStatus::Blocked => counts.blocked += 1,
            SeatStatus::Cancelled => counts.cancelled += 1,
        }
        entries.push(SeatSynthesisEntry {
            seat_id,
            tier: seat.tier,
            depth: seat.depth,
            status: report.status,
            body_sha256: sha256_hex(report.body.as_bytes()),
            body_chars: report.body.chars().count(),
            refusal: report.refusal,
        });
    }
    counts.total = tree.seats().len();
    let succeeded = counts.completed == counts.total;
    let root_digest = tree_digest(tree, reports)?;
    let summary = build_summary(tree, reports, &counts, &root_digest)?;
    Ok(TreeSynthesis {
        tree_root: root.seat_id,
        root_digest,
        counts,
        succeeded,
        entries,
        summary,
    })
}

fn build_summary(
    tree: &DelegationTree,
    reports: &BTreeMap<TaskId, SeatReport>,
    counts: &SeatCounts,
    root_digest: &str,
) -> Result<String, DelegationError> {
    let root = tree
        .root()
        .ok_or_else(|| DelegationError::new(Refusal::InternalError, "validated tree has no root"))?;
    let mut summary = format!(
        "Delegation tree {}: {}/{} seats completed; {} failed, {} blocked, {} cancelled. \
         Only Completed seats count as success. Root digest: {}.\n",
        root.seat_id,
        counts.completed,
        counts.total,
        counts.failed,
        counts.blocked,
        counts.cancelled,
        root_digest
    );
    for seat_id in tree.bottom_up_order() {
        let seat = tree.seat(seat_id).ok_or_else(|| {
            DelegationError::new(
                Refusal::InternalError,
                format!("seat {seat_id} vanished from a validated tree"),
            )
        })?;
        let report = reports.get(&seat_id).ok_or_else(|| {
            DelegationError::new(
                Refusal::InternalError,
                format!("seat {seat_id} has no report during summary"),
            )
        })?;
        let refusal = report
            .refusal
            .map_or_else(String::new, |refusal| format!(" {}", refusal.code()));
        let line = format!(
            "- [{}] {} {} (depth {}){}: {}\n",
            report.status.label().to_ascii_uppercase(),
            seat.tier.label(),
            seat.seat_id,
            seat.depth,
            refusal,
            clip_plain(&report.body, SUMMARY_BODY_CHARS)
        );
        if summary.chars().count() + line.chars().count() > MAX_SUMMARY_CHARS {
            summary.push_str("[summary truncated]\n");
            break;
        }
        summary.push_str(&line);
    }
    Ok(summary)
}

fn tree_digest(
    tree: &DelegationTree,
    reports: &BTreeMap<TaskId, SeatReport>,
) -> Result<String, DelegationError> {
    let root = tree
        .root()
        .ok_or_else(|| DelegationError::new(Refusal::InternalError, "validated tree has no root"))?;
    let mut canonical = String::new();
    canonical.push_str(DIGEST_DOMAIN);
    canonical.push('\n');
    canonical.push_str(&format!("root={}\n", root.seat_id));
    canonical.push_str(&format!("seats={}\n", tree.seats().len()));
    let mut ordered: Vec<&Seat> = tree.seats().iter().collect();
    ordered.sort_by_key(|seat| (seat.depth, seat.seat_id));
    for seat in ordered {
        let report = reports.get(&seat.seat_id).ok_or_else(|| {
            DelegationError::new(
                Refusal::InternalError,
                format!("seat {} has no report during digest", seat.seat_id),
            )
        })?;
        let parent = seat
            .parent_seat_id
            .map_or_else(|| "none".to_owned(), |id| id.to_string());
        let refusal = report
            .refusal
            .map_or("none", |refusal| refusal.code());
        let mut tools: Vec<&str> = seat.tools.iter().map(|tool| tool.id()).collect();
        tools.sort_unstable();
        let mut dependencies: Vec<String> = seat
            .depends_on
            .iter()
            .map(ToString::to_string)
            .collect();
        dependencies.sort_unstable();
        canonical.push_str(&format!(
            "seat={} parent={} tier={} depth={} status={} refusal={} brief_sha256={} \
             tools={} deps={} body_sha256={}\n",
            seat.seat_id,
            parent,
            seat.tier.label(),
            seat.depth,
            report.status.label(),
            refusal,
            sha256_hex(seat.brief.as_bytes()),
            tools.join(","),
            dependencies.join(","),
            sha256_hex(report.body.as_bytes()),
        ));
    }
    let digest = sha256_hex(canonical.as_bytes());
    if digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(DelegationError::new(
            Refusal::InternalError,
            "sha256 digest is not 64 hex characters",
        ));
    }
    Ok(digest)
}

fn clip_plain(text: &str, max_chars: usize) -> String {
    let mut clipped = String::new();
    let mut taken = 0usize;
    for character in text.trim().chars() {
        if taken >= max_chars {
            break;
        }
        clipped.push(if character.is_control() {
            ' '
        } else {
            character
        });
        taken += 1;
    }
    clipped
}

#[cfg(test)]
mod tests {
    use super::*;

    fn registry() -> ToolRegistry {
        ToolRegistry::standard()
    }

    fn limits() -> DelegationLimits {
        DelegationLimits::default()
    }

    fn tier_tools(tier: SeatTier) -> Vec<ToolId> {
        match tier {
            SeatTier::Ceo => Vec::new(),
            SeatTier::Manager => vec![ToolId::FileRead, ToolId::ReportResult],
            SeatTier::TeamLead => vec![ToolId::FileRead, ToolId::FileWrite],
            SeatTier::Assistant => vec![ToolId::FileRead, ToolId::GitDiff],
        }
    }

    fn full_tree() -> DelegationTree {
        let ceo = Seat::root(
            TaskId::new(),
            "Deliver the next release without over-spawning the team.",
        );
        let manager = Seat::under(
            &ceo,
            TaskId::new(),
            "Own the backend track and report the verified outcome.",
            tier_tools(SeatTier::Manager),
            Vec::new(),
        )
        .expect("manager");
        let lead = Seat::under(
            &manager,
            TaskId::new(),
            "Ship the delegation protocol with tests and evidence.",
            tier_tools(SeatTier::TeamLead),
            Vec::new(),
        )
        .expect("lead");
        let assistant = Seat::under(
            &lead,
            TaskId::new(),
            "Run the delegation test suite and report the observed result.",
            tier_tools(SeatTier::Assistant),
            Vec::new(),
        )
        .expect("assistant");
        DelegationTree::new(vec![ceo, manager, lead, assistant])
    }

    #[test]
    fn tier_ladder_is_strict_and_maps_to_the_engine() {
        assert_eq!(SeatTier::Ceo.depth(), 0);
        assert_eq!(SeatTier::Manager.depth(), 1);
        assert_eq!(SeatTier::TeamLead.depth(), 2);
        assert_eq!(SeatTier::Assistant.depth(), 3);
        assert_eq!(SeatTier::Ceo.direct_child(), Some(SeatTier::Manager));
        assert_eq!(SeatTier::Manager.direct_child(), Some(SeatTier::TeamLead));
        assert_eq!(SeatTier::TeamLead.direct_child(), Some(SeatTier::Assistant));
        assert_eq!(SeatTier::Assistant.direct_child(), None);
        assert_eq!(SeatTier::Manager.agent_tier(), AgentTier::SuperAgent);
        assert_eq!(SeatTier::Assistant.agent_tier(), AgentTier::Subagent);
        assert_eq!(SeatTier::TeamLead.planner_role(), "agent");
        assert_eq!(SeatTier::from_label("team-lead"), Some(SeatTier::TeamLead));
        assert_eq!(SeatTier::from_label("subagent"), Some(SeatTier::Assistant));
        assert_eq!(SeatTier::from_label("researcher"), None);
    }

    #[test]
    fn brief_bounds_refuse_empty_long_and_fragment() {
        assert_eq!(
            validate_brief("   ", &limits()).map_err(|error| error.refusal()),
            Err(Refusal::SchemaInvalid)
        );
        assert_eq!(
            validate_brief("Fix", &limits()).map_err(|error| error.refusal()),
            Err(Refusal::SchemaInvalid),
            "one word is not a bounded brief"
        );
        let long = "word ".repeat(MAX_BRIEF_CHARS);
        let error = validate_brief(&long, &limits()).expect_err("too long");
        assert_eq!(error.refusal(), Refusal::SchemaInvalid);
        assert!(error.detail().contains("cap"));
        validate_brief("Fix the login redirect and add a regression test.", &limits())
            .expect("plain-English brief");
    }

    #[test]
    fn decomposition_derives_tier_and_depth() {
        let ceo = Seat::root(TaskId::new(), "Deliver two independent artifacts.");
        let specs = vec![
            SeatSpec {
                seat_id: TaskId::new(),
                brief: "Write the operator runbook for the release.".to_owned(),
                tools: vec![ToolId::FileRead],
                depends_on: Vec::new(),
            },
            SeatSpec {
                seat_id: TaskId::new(),
                brief: "Write the rollback checklist for the release.".to_owned(),
                tools: vec![ToolId::FileRead],
                depends_on: Vec::new(),
            },
        ];
        let decomposition =
            decompose(&ceo, &specs, &registry(), &limits()).expect("valid decomposition");
        assert_eq!(decomposition.manager_seat_id, ceo.seat_id);
        assert_eq!(decomposition.children.len(), 2);
        for child in &decomposition.children {
            assert_eq!(child.tier, SeatTier::Manager);
            assert_eq!(child.depth, 1);
            assert_eq!(child.parent_seat_id, Some(ceo.seat_id));
            assert_eq!(child.tools, vec![ToolId::FileRead]);
        }
    }

    #[test]
    fn assistant_cannot_decompose() {
        let ceo = Seat::root(TaskId::new(), "Deliver one bounded artifact.");
        let manager = Seat::under(
            &ceo,
            TaskId::new(),
            "Own the artifact and report the verified outcome.",
            Vec::new(),
            Vec::new(),
        )
        .expect("manager");
        let lead = Seat::under(
            &manager,
            TaskId::new(),
            "Ship the artifact with tests and evidence.",
            Vec::new(),
            Vec::new(),
        )
        .expect("lead");
        let assistant = Seat::under(
            &lead,
            TaskId::new(),
            "Run the artifact test suite and report the observed result.",
            Vec::new(),
            Vec::new(),
        )
        .expect("assistant");
        let spec = SeatSpec {
            seat_id: TaskId::new(),
            brief: "Do another piece of the same work.".to_owned(),
            tools: Vec::new(),
            depends_on: Vec::new(),
        };
        let error = decompose(&assistant, std::slice::from_ref(&spec), &registry(), &limits())
            .expect_err("assistant cannot delegate");
        assert_eq!(error.refusal(), Refusal::ToolNotAllowed);
    }

    #[test]
    fn tool_grant_is_bounded_by_the_child_tier_allowlist() {
        let ceo = Seat::root(TaskId::new(), "Deliver one bounded artifact.");
        let manager = Seat::under(
            &ceo,
            TaskId::new(),
            "Own the artifact and report the verified outcome.",
            Vec::new(),
            Vec::new(),
        )
        .expect("manager");
        let lead = Seat::under(
            &manager,
            TaskId::new(),
            "Ship the artifact with tests and evidence.",
            Vec::new(),
            Vec::new(),
        )
        .expect("lead");
        let spec = SeatSpec {
            seat_id: TaskId::new(),
            brief: "Write the artifact file into the workspace.".to_owned(),
            tools: vec![ToolId::FileWrite],
            depends_on: Vec::new(),
        };
        let error = decompose(&lead, std::slice::from_ref(&spec), &registry(), &limits())
            .expect_err("assistant may not hold file_write");
        assert_eq!(error.refusal(), Refusal::ToolNotAllowed);
        assert_eq!(error.detail().contains("file_write"), true);
    }

    #[test]
    fn tree_rejects_duplicate_seat_ids() {
        let ceo = Seat::root(TaskId::new(), "Deliver one bounded artifact.");
        let duplicate = TaskId::new();
        let first = Seat::under(
            &ceo,
            duplicate,
            "Write the operator runbook for the release.",
            Vec::new(),
            Vec::new(),
        )
        .expect("child");
        let second = Seat::under(
            &ceo,
            duplicate,
            "Write the rollback checklist for the release.",
            Vec::new(),
            Vec::new(),
        )
        .expect("child");
        let tree = DelegationTree::new(vec![ceo, first, second]);
        let error = tree.validate(&registry(), &limits()).expect_err("duplicate");
        assert_eq!(error.refusal(), Refusal::SchemaInvalid);
    }

    #[test]
    fn sibling_dependency_cycle_is_refused() {
        let ceo = Seat::root(TaskId::new(), "Deliver one bounded artifact.");
        let first = Seat::under(
            &ceo,
            TaskId::new(),
            "Write the operator runbook for the release.",
            Vec::new(),
            Vec::new(),
        )
        .expect("child");
        let second_id = TaskId::new();
        let second = Seat::under(
            &ceo,
            second_id,
            "Write the rollback checklist for the release.",
            Vec::new(),
            vec![first.seat_id],
        )
        .expect("child");
        let mut first = first;
        first.depends_on = vec![second_id];
        let tree = DelegationTree::new(vec![ceo, first, second]);
        let error = tree.validate(&registry(), &limits()).expect_err("cycle");
        assert_eq!(error.refusal(), Refusal::Cycle);
    }

    #[test]
    fn full_tree_validates_and_digest_is_order_independent() {
        let registry = registry();
        let limits = limits();
        let tree = full_tree();
        tree.validate(&registry, &limits).expect("valid tree");
        let ids: Vec<TaskId> = tree.seats().iter().map(|seat| seat.seat_id).collect();
        let reports = vec![
            SeatReport::completed(ids[3], "The delegation test suite passed."),
            SeatReport::completed(ids[2], "The protocol ships with tests."),
            SeatReport::completed(ids[1], "The backend track is verified."),
            SeatReport::completed(ids[0], "The release is ready."),
        ];
        let first = synthesize_batch(tree.clone(), &registry, &limits, reports.clone())
            .expect("synthesis");
        let reversed: Vec<SeatReport> = reports.into_iter().rev().collect();
        let second = synthesize_batch(tree, &registry, &limits, reversed).expect("synthesis");
        assert!(first.succeeded);
        assert_eq!(first.counts.completed, 4);
        assert_eq!(first.counts.failed, 0);
        assert_eq!(first.counts.blocked, 0);
        assert_eq!(first.root_digest, second.root_digest);
        assert_eq!(first.summary, second.summary);
        assert_eq!(first.root_digest.len(), 64);
    }

    #[test]
    fn status_change_changes_the_root_digest() {
        let registry = registry();
        let limits = limits();
        let tree = full_tree();
        let ids: Vec<TaskId> = tree.seats().iter().map(|seat| seat.seat_id).collect();
        let completed = synthesize_batch(
            tree.clone(),
            &registry,
            &limits,
            vec![
                SeatReport::completed(ids[3], "done"),
                SeatReport::completed(ids[2], "done"),
                SeatReport::completed(ids[1], "done"),
                SeatReport::completed(ids[0], "done"),
            ],
        )
        .expect("synthesis");
        let degraded = synthesize_batch(
            tree,
            &registry,
            &limits,
            vec![
                SeatReport::blocked(ids[3], "the test runner is missing"),
                SeatReport::blocked(ids[2], "the test runner is missing"),
                SeatReport::blocked(ids[1], "the test runner is missing"),
                SeatReport::blocked(ids[0], "the test runner is missing"),
            ],
        )
        .expect("synthesis");
        assert!(!degraded.succeeded);
        assert_ne!(completed.root_digest, degraded.root_digest);
    }
}
