//! Plan DAG produced by one provider planning call, validated before dispatch
//! (blueprint WP 4.5/4.7).

use std::collections::{HashMap, HashSet};

use hiverouter_contract::{Refusal, TaskId};
use serde::{Deserialize, Serialize};

/// Maximum plan nodes / super-steps (blueprint §2a: 40-call ceiling).
pub const MAX_PLAN_NODES: usize = 40;

/// Maximum branch leads the root may carry and maximum children one planned
/// parent may carry. Both equal the engine fan-out cap, so a plan that passes
/// validation can never fail mid-dispatch on `E_FANOUT_LIMIT` after some task
/// rows were already persisted.
pub const MAX_PLAN_FANOUT: usize = crate::caps::MAX_FANOUT;

/// Canonical SuperAgent persona ids. These may only appear at depth 1.
pub const BRANCH_LEAD_ROLES: &[&str] = &["superagent"];

/// Canonical worker persona ids. These may only appear at depth 2.
pub const WORKER_ROLES: &[&str] = &[
    "agent",
    "content-writer",
    "data-analyst",
    "code-reviewer",
    "chatbot",
];

/// Canonical Subagent persona id. It may only appear at depth 3.
pub const SUBAGENT_ROLES: &[&str] = &["subagent"];

/// The only depth a canonical persona id may occupy, if it is one.
///
/// Legacy class aliases (`super`, `worker`, ...) return `None`: the engine
/// resolves them by class and keeps its historical acceptance. Canonical
/// persona ids are what the planner prompt asks for, so a canonical id at the
/// wrong depth is refused before any task row exists.
fn canonical_role_depth(role: &str) -> Option<u8> {
    let role = role.trim().to_ascii_lowercase();
    if BRANCH_LEAD_ROLES.contains(&role.as_str()) {
        Some(1)
    } else if WORKER_ROLES.contains(&role.as_str()) {
        Some(2)
    } else if SUBAGENT_ROLES.contains(&role.as_str()) {
        Some(3)
    } else {
        None
    }
}

/// One planned node.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanNode {
    /// Stable node id (becomes the task id).
    pub node_id: TaskId,
    /// Agent role/tier name.
    pub role: String,
    /// Nodes that must finish before this node.
    pub deps: Vec<TaskId>,
    /// Spawn depth (1-based, hard-capped at 3 by [`crate::caps`]).
    pub depth: u8,
    /// Planned parent node. `None` means the run root (legacy flat plans).
    ///
    /// When present, the parent must be another planned node at `depth - 1`.
    #[serde(default)]
    pub parent: Option<TaskId>,
    /// Plain-English objective for this seat. The executor gives each seat its
    /// own brief instead of the whole run prompt; `None` keeps the legacy
    /// whole-prompt fallback for plans that predate objectives.
    #[serde(default)]
    pub objective: Option<String>,
}

/// One plan: an acyclic DAG of nodes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Plan {
    /// Nodes in executor order (validation recomputes the order).
    pub nodes: Vec<PlanNode>,
}

impl Plan {
    /// Creates a plan.
    #[must_use]
    pub fn new(nodes: Vec<PlanNode>) -> Self {
        Self { nodes }
    }

    /// Validates the plan DAG before dispatch: non-empty, <= 40 nodes, unique
    /// ids, unique deps, known deps, a tree-shaped parent chain, no cycle across
    /// either dependency direction, resolvable roles at their ladder depth, and
    /// a fan-out the engine can actually spawn.
    ///
    /// # Errors
    /// `E_CALL_LIMIT` when over the node cap; `E_FANOUT_LIMIT` when the root
    /// would carry more than [`MAX_PLAN_FANOUT`] branch leads or one parent more
    /// than that many children (refused here, not mid-dispatch); `E_PERSONA_UNKNOWN`
    /// for a role the engine cannot resolve; `E_TOOL_NOT_ALLOWED` for a canonical
    /// persona id at the wrong depth; `E_SCHEMA_INVALID` for duplicate ids,
    /// duplicate/unknown deps, a parent/dependency cycle (both directions) or a
    /// non-tree parent chain.
    pub fn validate(&self) -> Result<(), Refusal> {
        if self.nodes.is_empty() {
            return Err(Refusal::SchemaInvalid);
        }
        if self.nodes.len() > MAX_PLAN_NODES {
            return Err(Refusal::CallLimit);
        }
        let mut known = HashSet::new();
        for node in &self.nodes {
            if !known.insert(node.node_id) {
                return Err(Refusal::SchemaInvalid);
            }
        }
        for node in &self.nodes {
            if node.role.trim().is_empty() || node.depth == 0 {
                return Err(Refusal::SchemaInvalid);
            }
            if node.depth > 3 {
                return Err(Refusal::SpawnDepth);
            }
            let mut unique_deps = HashSet::with_capacity(node.deps.len());
            for dep in &node.deps {
                if !known.contains(dep) || !unique_deps.insert(*dep) {
                    return Err(Refusal::SchemaInvalid);
                }
            }
        }
        let by_id: HashMap<TaskId, &PlanNode> =
            self.nodes.iter().map(|node| (node.node_id, node)).collect();
        for node in &self.nodes {
            match node.parent {
                Some(parent_id) => {
                    if parent_id == node.node_id {
                        return Err(Refusal::SchemaInvalid);
                    }
                    let parent = by_id.get(&parent_id).ok_or(Refusal::SchemaInvalid)?;
                    if parent.depth.saturating_add(1) != node.depth {
                        return Err(Refusal::SchemaInvalid);
                    }
                    // A parent is already the ordering barrier (the parent
                    // waits for its children). Keeping it in `deps` makes the
                    // parent wait for the child too: an execution deadlock that
                    // used to surface late as E_SCHEMA_INVALID.
                    if node.deps.contains(&parent_id) {
                        return Err(Refusal::SchemaInvalid);
                    }
                }
                None if node.depth > 1 => {
                    // A depth-2+ node with no parent would be attached to the
                    // root by the engine, violating the strict class hierarchy.
                    return Err(Refusal::SchemaInvalid);
                }
                None => {}
            }
            // The parent edge is an aggregation barrier in both directions: the
            // parent waits for the child, so a dependency on a descendant is a
            // cycle...
            for dep in &node.deps {
                let mut cursor = Some(*dep);
                while let Some(id) = cursor {
                    if id == node.node_id {
                        return Err(Refusal::SchemaInvalid);
                    }
                    cursor = by_id.get(&id).and_then(|item| item.parent);
                }
            }
            // ...and a dependency on an ancestor is the mirror cycle: the
            // ancestor waits for this node's parent chain, which waits for this
            // node. Only the descendant direction used to be checked, so a plan
            // could pass validation and then deadlock in the readiness loop.
            let mut ancestor = node.parent;
            while let Some(id) = ancestor {
                if node.deps.contains(&id) {
                    return Err(Refusal::SchemaInvalid);
                }
                ancestor = by_id.get(&id).and_then(|item| item.parent);
            }
            // A parent chain that loops would make the hierarchy non-tree-shaped
            // even though the dependency graph is acyclic.
            let mut seen = HashSet::new();
            let mut current = Some(node.node_id);
            while let Some(id) = current {
                if !seen.insert(id) {
                    return Err(Refusal::SchemaInvalid);
                }
                current = by_id.get(&id).and_then(|item| item.parent);
            }
        }
        // Role and fan-out policy. Both are checked here (before the engine
        // persists the root) so a shape the dispatch loop would refuse anyway
        // fails as one named plan refusal instead of a half-built graph.
        let mut root_children = 0usize;
        let mut children: HashMap<TaskId, usize> = HashMap::new();
        for node in &self.nodes {
            if crate::execution::role_class(&node.role).is_none() {
                return Err(Refusal::PersonaUnknown);
            }
            if canonical_role_depth(&node.role).is_some_and(|depth| depth != node.depth) {
                return Err(Refusal::ToolNotAllowed);
            }
            match node.parent {
                Some(parent) => *children.entry(parent).or_insert(0) += 1,
                None => root_children = root_children.saturating_add(1),
            }
        }
        if root_children > MAX_PLAN_FANOUT
            || children.values().any(|count| *count > MAX_PLAN_FANOUT)
        {
            return Err(Refusal::FanoutLimit);
        }
        if self.topo_order()?.len() != self.nodes.len() {
            return Err(Refusal::SchemaInvalid);
        }
        Ok(())
    }

    /// Kahn topological order of node ids (parents before children).
    ///
    /// # Errors
    /// `E_SCHEMA_INVALID` when the graph contains a cycle.
    pub fn topo_order(&self) -> Result<Vec<TaskId>, Refusal> {
        let mut indegree: HashMap<TaskId, usize> =
            self.nodes.iter().map(|node| (node.node_id, 0)).collect();
        for node in &self.nodes {
            // Count each distinct dependency once. Validation refuses duplicate
            // deps by name; counting them twice here would make a duplicate-dep
            // DAG look like a cycle (`E_SCHEMA_INVALID`) instead.
            let unique_deps: HashSet<TaskId> = node.deps.iter().copied().collect();
            for dep in unique_deps {
                if let Some(entry) = indegree.get_mut(&node.node_id) {
                    *entry += 1;
                    let _ = dep;
                }
            }
        }
        let mut ready: Vec<TaskId> = self
            .nodes
            .iter()
            .filter(|node| indegree.get(&node.node_id) == Some(&0))
            .map(|node| node.node_id)
            .collect();
        let mut ordered = Vec::with_capacity(self.nodes.len());
        while let Some(id) = ready.pop() {
            ordered.push(id);
            for node in &self.nodes {
                if node.deps.contains(&id) {
                    if let Some(entry) = indegree.get_mut(&node.node_id) {
                        *entry = entry.saturating_sub(1);
                        if *entry == 0 {
                            ready.push(node.node_id);
                        }
                    }
                }
            }
        }
        if ordered.len() != self.nodes.len() {
            return Err(Refusal::SchemaInvalid);
        }
        Ok(ordered)
    }

    /// Leaf nodes (no other node depends on them) in topological order.
    ///
    /// # Errors
    /// Propagates the topological-order refusal.
    pub fn leaf_order(&self) -> Result<Vec<TaskId>, Refusal> {
        let mut depended_on = HashSet::new();
        for node in &self.nodes {
            for dep in &node.deps {
                depended_on.insert(*dep);
            }
            if let Some(parent) = node.parent {
                depended_on.insert(parent);
            }
        }
        Ok(self
            .topo_order()?
            .into_iter()
            .filter(|id| !depended_on.contains(id))
            .collect())
    }

    /// Aggregator nodes (they have a child/dependent) in execution order:
    /// deepest first, then reverse topological order. A parent must run after
    /// the children whose results it consumes.
    ///
    /// # Errors
    /// Propagates the topological-order refusal.
    pub fn aggregation_order(&self) -> Result<Vec<TaskId>, Refusal> {
        let topo = self.topo_order()?;
        let rank: HashMap<TaskId, usize> = topo
            .iter()
            .enumerate()
            .map(|(index, id)| (*id, index))
            .collect();
        let mut aggregators: Vec<&PlanNode> = self
            .nodes
            .iter()
            .filter(|node| {
                self.nodes.iter().any(|other| {
                    other.parent == Some(node.node_id) || other.deps.contains(&node.node_id)
                })
            })
            .collect();
        aggregators.sort_by(|left, right| {
            right
                .depth
                .cmp(&left.depth)
                .then_with(|| rank[&right.node_id].cmp(&rank[&left.node_id]))
        });
        Ok(aggregators.iter().map(|node| node.node_id).collect())
    }

    /// Looks a node up by id.
    #[must_use]
    pub fn node(&self, node_id: TaskId) -> Option<&PlanNode> {
        self.nodes.iter().find(|node| node.node_id == node_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(role: &str, deps: Vec<TaskId>, depth: u8) -> PlanNode {
        PlanNode {
            objective: None,
            node_id: TaskId::new(),
            role: role.to_owned(),
            deps,
            depth,
            parent: None,
        }
    }

    #[test]
    fn valid_dag_orders_parents_first() {
        let first = node("super", Vec::new(), 1);
        let mut second = node("worker", Vec::new(), 2);
        second.parent = Some(first.node_id);
        let plan = Plan::new(vec![first.clone(), second.clone()]);
        plan.validate().expect("valid plan");
        let order = plan.topo_order().expect("order");
        assert_eq!(order.len(), 2);
        assert!(order.contains(&first.node_id));
        assert!(order.contains(&second.node_id));
        assert_eq!(plan.leaf_order().expect("leaves"), vec![second.node_id]);
    }

    #[test]
    fn parent_cannot_also_be_a_dependency() {
        // A parent already waits for the child; adding the reverse dependency
        // makes both sides wait and no node is dispatch-ready.
        let first = node("super", Vec::new(), 1);
        let mut second = node("worker", vec![first.node_id], 2);
        second.parent = Some(first.node_id);
        let plan = Plan::new(vec![first, second]);
        assert_eq!(plan.validate(), Err(Refusal::SchemaInvalid));
    }

    #[test]
    fn parentless_depth_two_is_refused() {
        let plan = Plan::new(vec![node("worker", Vec::new(), 2)]);
        assert_eq!(plan.validate(), Err(Refusal::SchemaInvalid));
    }

    #[test]
    fn cycle_is_refused_at_plan_validation() {
        let mut first = node("super", Vec::new(), 1);
        let mut second = node("worker", Vec::new(), 2);
        second.parent = Some(first.node_id);
        first.deps = vec![second.node_id];
        let plan = Plan::new(vec![first, second]);
        assert_eq!(plan.validate(), Err(Refusal::SchemaInvalid));
    }

    #[test]
    fn node_cap_is_forty() {
        let nodes: Vec<PlanNode> = (0..41).map(|_| node("worker", Vec::new(), 1)).collect();
        let plan = Plan::new(nodes);
        assert_eq!(plan.validate(), Err(Refusal::CallLimit));
    }

    #[test]
    fn unknown_dependency_is_refused() {
        let plan = Plan::new(vec![node("worker", vec![TaskId::new()], 1)]);
        assert_eq!(plan.validate(), Err(Refusal::SchemaInvalid));
    }

    #[test]
    fn parented_hierarchy_is_valid() {
        let mut super_agent = node("super", Vec::new(), 1);
        let mut agent = node("worker", Vec::new(), 2);
        agent.parent = Some(super_agent.node_id);
        let subagent = {
            let mut sub = node("subagent", Vec::new(), 3);
            sub.parent = Some(agent.node_id);
            sub
        };
        // Give the first planned node a real root parent only at insertion; the
        // planner validates the planned-node chain above it.
        super_agent.parent = None;
        let plan = Plan::new(vec![super_agent, agent, subagent]);
        plan.validate().expect("valid parent chain");
        assert_eq!(plan.nodes[1].parent, Some(plan.nodes[0].node_id));
        assert_eq!(plan.nodes[2].parent, Some(plan.nodes[1].node_id));
        assert_eq!(
            plan.aggregation_order().expect("aggregation order"),
            vec![plan.nodes[1].node_id, plan.nodes[0].node_id]
        );
    }

    #[test]
    fn parent_depth_mismatch_is_refused() {
        let mut parent = node("super", Vec::new(), 1);
        parent.parent = None;
        let mut child = node("worker", Vec::new(), 3);
        child.parent = Some(parent.node_id);
        let plan = Plan::new(vec![parent, child]);
        assert_eq!(plan.validate(), Err(Refusal::SchemaInvalid));
    }

    #[test]
    fn self_parent_is_refused() {
        let mut item = node("worker", Vec::new(), 1);
        let id = item.node_id;
        item.parent = Some(id);
        let plan = Plan::new(vec![item]);
        assert_eq!(plan.validate(), Err(Refusal::SchemaInvalid));
    }

    #[test]
    fn ancestor_dependency_is_a_cycle_and_is_refused() {
        // The parent edge makes each ancestor wait for its descendants, so a
        // depth-3 node depending on its depth-1 grandparent is the mirror of the
        // descendant cycle: the whole branch deadlocks in the readiness loop.
        let mut lead = node("super", Vec::new(), 1);
        let mut agent = node("worker", Vec::new(), 2);
        agent.parent = Some(lead.node_id);
        let mut sub = node("subagent", vec![lead.node_id], 3);
        sub.parent = Some(agent.node_id);
        lead.parent = None;
        let plan = Plan::new(vec![lead, agent, sub]);
        assert_eq!(plan.validate(), Err(Refusal::SchemaInvalid));
    }

    #[test]
    fn duplicate_dependency_is_refused_not_reported_as_a_cycle() {
        let mut lead = node("super", Vec::new(), 1);
        let mut agent = node("worker", Vec::new(), 2);
        agent.parent = Some(lead.node_id);
        agent.deps = vec![lead.node_id, lead.node_id];
        lead.parent = None;
        let plan = Plan::new(vec![lead, agent]);
        assert_eq!(plan.validate(), Err(Refusal::SchemaInvalid));
        // A duplicate dep is not a graph cycle: ordering still completes.
        assert_eq!(plan.topo_order().expect("ordering").len(), 2);
    }

    #[test]
    fn unknown_planned_role_is_refused_before_dispatch() {
        // The engine refuses `researcher` mid-dispatch with E_PERSONA_UNKNOWN;
        // the plan must refuse it before the root row exists.
        let plan = Plan::new(vec![node("researcher", Vec::new(), 1)]);
        assert_eq!(plan.validate(), Err(Refusal::PersonaUnknown));
    }

    #[test]
    fn canonical_role_at_the_wrong_depth_is_refused() {
        let mut lead = node("super", Vec::new(), 1);
        let mut worker = node("superagent", Vec::new(), 2);
        worker.parent = Some(lead.node_id);
        lead.parent = None;
        let plan = Plan::new(vec![lead, worker]);
        assert_eq!(plan.validate(), Err(Refusal::ToolNotAllowed));

        let worker_at_lead_depth = Plan::new(vec![node("agent", Vec::new(), 1)]);
        assert_eq!(
            worker_at_lead_depth.validate(),
            Err(Refusal::ToolNotAllowed)
        );
    }

    #[test]
    fn root_fanout_over_the_engine_cap_is_refused() {
        let nodes: Vec<PlanNode> = (0..=MAX_PLAN_FANOUT)
            .map(|_| node("super", Vec::new(), 1))
            .collect();
        let plan = Plan::new(nodes);
        assert_eq!(plan.validate(), Err(Refusal::FanoutLimit));
    }

    #[test]
    fn per_parent_fanout_over_the_engine_cap_is_refused() {
        let mut lead = node("super", Vec::new(), 1);
        lead.parent = None;
        let mut nodes = vec![lead.clone()];
        for _ in 0..=MAX_PLAN_FANOUT {
            let mut worker = node("worker", Vec::new(), 2);
            worker.parent = Some(lead.node_id);
            nodes.push(worker);
        }
        let plan = Plan::new(nodes);
        assert_eq!(plan.validate(), Err(Refusal::FanoutLimit));
    }

    #[test]
    fn a_three_item_request_has_a_minimal_valid_shape() {
        // Exactly one lead per requested item, one worker per lead: the shape
        // the planner prompt asks for, accepted without a node-count refusal.
        let mut nodes = Vec::new();
        for _ in 0..3u8 {
            let lead = node("superagent", Vec::new(), 1);
            let mut worker = node("agent", Vec::new(), 2);
            worker.parent = Some(lead.node_id);
            nodes.push(lead);
            nodes.push(worker);
        }
        let plan = Plan::new(nodes);
        plan.validate().expect("three-item plan is valid");
        assert_eq!(plan.topo_order().expect("order").len(), 6);
    }
}
