# Hierarchical Delegation — how a real multi-agent org should work vs. how HiveMind actually works

Status: **research + design, read-only audit.** No code was edited, nothing was launched or committed.
Date: 2026-11-13. Repo: `C:\Users\Riley\Desktop\hiverouter-v1`.

Evidence classes used below, always labelled:

- **[C]** confirmed by reading the running code at the cited `file:line`.
- **[W]** confirmed by an official/primary web source, URL cited at the point of use.
- **[A]** assumption / management-theory convention — **not** verified, do not build on it without a check.
- **[?]** unknown — what would settle it is named.

---

## 0. Research note (skill: research)

**Questions this doc had to close before any design was allowed:**

1. What do production hierarchical multi-agent systems actually do about: delegation briefs, span of control, bottom-up aggregation, peer/upward messaging, shared memory, escalation?
2. What does HiveMind do today, exactly, at `file:line` — plan, root control, spawn matrix, prompts, memory scope, monitor, synthesis?
3. Where do (1) and (2) contradict the intended `CEO → manager → team-lead → assistant` model?
4. What is the *minimal* set of seams and tests that closes the gap without inventing APIs?

All four are closed. Remaining unknowns are in §6 and are non-blocking for the design.

---

## 1. What a real hierarchical multi-agent organisation does

### 1.1 The four-role model the operator asked for

`CEO → manager → team-lead → assistant`. In HiveMind's own vocabulary the four tiers already exist as `AgentTier` (`hiverouter-tools/src/tool.rs:76-87` [C]): `HyperAgent`, `SuperAgent`, `WorkerAgent`, `Subagent`. The natural one-to-one reading, used throughout this doc:

| Org role | HiveMind tier | Node kind on the wire |
|---|---|---|
| CEO | `HyperAgent` (root, `role: "planner"`) | `hyperagent` |
| Manager | `SuperAgent` (branch lead) | `superagent` |
| Team lead | `WorkerAgent` (wire role `agent`/`worker`) | `worker` |
| Assistant | `Subagent` | `subagent` |

The mapping is an **interpretation**, not a code fact: the code has four tiers but nowhere names them "CEO/manager/team-lead/assistant". If the operator intends Manager = team-lead and Team lead = manager, every conclusion about *who aggregates* flips. **[?]** — settled by naming the intended aggregation owner per depth in the spec.

### 1.2 What primary sources actually establish

**Anthropic, "How we built our multi-agent research system"** ([anthropic.com](https://www.anthropic.com/engineering/multi-agent-research-system)) [W]:

- Orchestrator-worker: a lead agent plans, spawns parallel subagents, subagents return findings, the lead synthesises and decides whether to spawn more.
- *Delegation quality is the orchestrator's job.* "Each subagent needs an objective, an output format, guidance on the tools and sources to use, and clear task boundaries. Without detailed task descriptions, agents duplicate work, leave gaps…". Their failure example: short briefs like "research the semiconductor shortage" caused duplicated work across subagents.
- *Effort must be scaled to complexity in the prompt* ("1 agent with 3-10 tool calls" … "more than 10 subagents with clearly divided responsibilities"), because agents otherwise over-spawn (they cite spawning 50 subagents for simple queries).
- *Synchronous lead execution is an acknowledged bottleneck*: "the lead agent can't steer subagents, subagents can't coordinate, and the entire system can be blocked while waiting for a single subagent".
- *Artifacts beat telephone*: "Direct subagent outputs can bypass the main coordinator… Subagents call tools to store their work in external systems, then pass lightweight references back to the coordinator."
- Cost shape: multi-agent ≈ 15× chat tokens, so hierarchy must exist to *compress*, not to multiply round-trips.

**CrewAI, Hierarchical Process** ([docs.crewai.com](https://docs.crewai.com/en/learn/hierarchical-process)) [W]:

- A `manager` agent (auto-created or explicit) "coordinates the workflow, delegates tasks, and validates outcomes"; the doc calls this "a clear chain of command".
- Three ordered duties: **task delegation** (assign by role/capability), **result validation** (manager evaluates outcomes against standards), **sequential progression** (manager oversight imposes a logical order).
- Delegation is now **disabled by default** — delegation is treated as a privileged, explicitly-enabled capability, not a default.

**OpenAI Agents SDK, Agent orchestration** ([openai.github.io](https://openai.github.io/openai-agents-python/multi_agent/)) [W]:

- Two patterns, explicitly contrasted: **agents-as-tools** — "A manager agent keeps control of the conversation and calls specialist agents… You want one agent to own the final answer"; and **handoffs** — "a specialist becomes the active agent for the rest of the turn".
- Guidance: use agents-as-tools when a specialist helps with a bounded subtask but must not take over the user-facing conversation. That is precisely the CEO/manager relationship; handoff is not.
- "Orchestrating via code… makes tasks more deterministic and predictable" vs. LLM-decided flow — the SDK frames this as a deliberate trade, which is why the *engine* (not the root model) must own caps.

**A2A Protocol specification** ([a2a-protocol.org](https://a2a-protocol.org/latest/specification/)) [W] — read at table-of-contents depth; only structural claims made here:

- The spec's own structure is a **task lifecycle** model: `Task` / `TaskStatus` / `TaskState`, `Message`, `Artifact`, plus `Get Task`, `Cancel Task`, `Subscribe to Task`, streaming status updates, and a **context identifier** that groups related tasks (§3.1.3–3.1.6, §3.4.1, §4.1.1–4.1.7 in the fetched TOC).
- Design implication, stated as a mapping rather than an API claim: a hierarchy needs (a) a durable task object with status, (b) typed messages, (c) artifacts passed by reference, (d) a *subscribe* channel so a supervisor observes without blocking, (e) a context id above task id. HiveMind has (a)–(c) in a home-grown form; it has no subscribe channel and no per-tree context object (§3, C6/C7).

### 1.3 Span of control

**[A]** Management theory (Graicunas-style span-of-control reasoning) says a supervisor's effective span is small and grows combinatorially with cross-links, so deep hierarchies use narrow spans. **I did not fetch a primary source for this and will not cite a number.** What *is* verifiable in-repo is the disagreement between the enforced span and the prompt-level span: engine `MAX_FANOUT = 8` (`caps.rs:11` [C]) vs. the SuperAgent prompt's "At most 3 workers per branch" (`assets/prompts/superagent.prompty` [C]) vs. persona `max_children: 8` for `superagent` (`assets/prompts/personas.json` [C]). Three different numbers describe one limit; see C9.

### 1.4 Invariants a hierarchical org implementation must satisfy

Derived from 1.2–1.3, and used as the audit rubric in §3:

- **I1 Delegation is downward and one level at a time** — no level skipping, no self-promotion.
- **I2 Each seat gets a brief, not a prompt** — objective, output contract, boundaries, tool grant, budget.
- **I3 Results aggregate bottom-up through the chain** — a manager's input is its *direct reports'* reports; the CEO reads manager reports, not raw assistant transcripts.
- **I4 Peers talk sideways, subordinates talk up** — a peer bus with a scope boundary, and an escalation path that terminates at a human.
- **I5 Shared memory is scoped to the tree, not to the run** — branch-private by default, tree-shared on purpose, run-global only for constraints.
- **I6 The supervisor observes; it does not micro-manage mid-flight** — observation must be cheap, non-authoritative, and must not become a second scheduler.
- **I7 The top synthesises from evidence and refuses to invent** — unfinished work is reported unfinished.
- **I8 Caps are enforced by the engine, not by prompt text** — a model cannot widen its own authority.

---

## 2. Confirmed current behaviour (audit)

Everything in this section is **[C]**: read from the file at the cited line.

### 2.1 Topology: planner → HyperAgent root → SuperAgent → Agent → Subagent

| Step | Where | What it actually does |
|---|---|---|
| One planning call per run | `hiverouter-core/src/execution.rs:2300-2309` | claims a provider call, builds `planning_prompt`, calls `executor.plan` |
| One repair attempt | `execution.rs:2310-2328` | only on `SchemaInvalid`/`ProviderInvalidResponse`; counted as its own call |
| Plan validation | `execution.rs:2329` → `planner.rs:97-211` | ≤40 nodes, depth ≤3, unique ids/deps, tree-shaped parent chain, no parent∈deps, role resolution, fan-out ≤8 |
| Root task | `execution.rs:2332-2346` | `role: "planner"`, `depth: 0`, node kind `hyperagent`, label `HyperAgent` |
| Plan → task rows | `execution.rs:2376-2415` | depth-sorted insert; `parent_id = node.parent.unwrap_or(root.id)`; each node re-validated against its parent tier |
| Root control turn | `execution.rs:5030-5091` | 2 bounded turns, root-local tools only |
| Dispatch | `execution.rs:2487`, `2783-2857` | sequential unless `parallel_dispatch > 1` (default `1`, `execution.rs:1082`) |
| Settlement | `execution.rs:2952-3000`, `2924-2950` | drain spawn queue, then force-settle Queued/Running (≤64 iterations) |
| Monitor | `execution.rs:2704-2765` | one read-only review, advisory |
| Final synthesis | `execution.rs:2616-2699`, `2766` | root answers from a flat digest of **every** task |

Plan node shape: `PlanNode { node_id, role, deps, depth, parent }` (`planner.rs:54-68`) [C]. Depth discipline: depth-1 = `superagent`, depth-2 = `agent|content-writer|data-analyst|code-reviewer|chatbot`, depth-3 = `subagent` (`planner.rs:19-31`, `39-50`) [C]. A canonical role at the wrong depth is refused with `E_TOOL_NOT_ALLOWED` (`planner.rs:194-196`) [C]. A depth-2+ node with no parent is refused (`planner.rs:144-148`) [C] — the hierarchy edge is mandatory, deps alone are not a hierarchy.

**Aggregation order exists but is only used as an ordering hint**, not as the reporting rule: `Plan::aggregation_order()` returns aggregator nodes "deepest first, then reverse topological order" (`planner.rs:285-308`) [C], and the root's synthesis ignores it (§2.7).

### 2.2 Prompts (the actual briefs)

- Planner prompt asset `assets/prompts/planner.prompty` [C]: JSON-only DAG; explicit role ladder; a "Branch budget" section that counts deliverables, forbids phase-leads ("plan, analyse, design, implement, test, review, document, integrate, synthesise are steps… not deliverables"), and caps at 8 leads / 8 children / 40 nodes.
- Render path: `execution.rs:3004-3032` — template render, then `prompt_with_authorized_lessons` → `prompt_with_capability_context` → `prompt_with_session_todos`.
- SuperAgent brief (`superagent.prompty`) [C]: one objective, "You own the branch result, not the whole run", spawn only independent work, "At most 3 workers per branch", ask_parent only for authority/uninferable fact, and a **fixed 12-line result contract** (`RESULT:/CHANGES:/EVIDENCE:/RISKS:/BLOCKED:`).
- Agent brief (`agent.prompty`) [C]: bounded task, verify mutations, report `BLOCKED` instead of improvising, same 12-line contract.
- Subagent brief (`subagent.prompty`) [C]: narrow, "do not spawn", one `ask_parent`, 8-line contract.
- Per-task prompt at runtime: `execute_task` (`execution.rs:4943-4959`) — if a stored instruction exists, the prompt is `"Task objective:\n{instruction}\n\nRun context (shared goal, not task authority):\n{run.prompt}"`; otherwise the raw run prompt. So **a planned node with no instruction gets no objective at all** — see C4.
- Aggregation prompt injection — **implemented twice, byte-identical today**: `build_task_prompt` (`execution.rs:2861-2918`, parallel path only, call site `:2816`) and inline in the sequential loop (`execution.rs:2512-2559`, the default path). Both append `DIRECT CHILD RESULT (this task's own child) id (role X): …`, append dependency results as `Completed dependency …`, and add `"Aggregate the completed child results below into this task's final answer. Do not invent results for children that have none."` when the node has children [C]. So planned bottom-up aggregation **does** exist; the defect is duplication (C2), not absence.
- Model-spawned children are joined **inline**: `execute_spawn_agent` (`execution.rs:4700-4811`) spawns through `spawn_sub_hive`, removes the child from the deferred queue (`:4752`), runs it synchronously (`:4763`), and returns its bounded text as the tool result (`:4765-4773`). So a team-lead that spawns an assistant does see the assistant's output in its own turn [C]. A root-control `spawn_superagent` is *not* inline: it is enqueued (`execution.rs:2005`) and drained at `:2593` via `execute_task`, which injects no child results (`:4943-4959`).

### 2.3 Spawn allowlists (two independent gates, plus a third)

1. **Tier matrix** — `validate_spawn_role` (`execution.rs:513-544`) [C]: HyperAgent → superagent/super aliases only; SuperAgent → worker/agent/`content-writer`/`data-analyst`/`code-reviewer`/`chatbot` but **not** subagent; WorkerAgent → subagent only; Subagent and ToolAgent → nothing.
2. **Class matrix** — `class_may_spawn` (`execution.rs:488-495`) [C]: `hyper→super`, `super→agent`, `agent→subagent`, else false. Re-checked in `spawn_sub_hive` (`execution.rs:1949-1960`) and for every planned node (`execution.rs:2399-2414`).
3. **Tool-visibility allowlist** — `ToolRegistry::allowlist` (`hiverouter-tools/src/tool.rs:776-866`) [C]:
   - `HyperAgent`: `spawn_agent, report_result, finish, fs_glob, fs_grep, file_read_range, apply_patch, run_tests, git_*, harness_diagnose`. **No `ask_parent`, no `send_to_sibling`, no `file_read`, no `file_write`.**
   - `SuperAgent`: everything the worker has **plus** `request_detached`, `web_fetch`, `vote_evolution`.
   - `WorkerAgent`: includes `spawn_agent`, `ask_parent`, `send_to_sibling`, `request_detached`.
   - `Subagent`: read-only + `ask_parent` + `report_result` + `finish`; **no spawn, no `send_to_sibling`**.
   - Offered to the model by `tool_specs_for_tier` (`tool_loop.rs:254-267`) [C].
4. **Persona scoping overrides the tier list** — personas are loaded from `assets/prompts/personas.json`, and when a persona was seeded into a task's context, an out-of-persona tool is refused (`execution.rs:1824-1832`) [C]. Only personas whose `allowed_tool_ids` contain `spawn_agent` can spawn: `superagent` and `agent` — **not** `content-writer`, `data-analyst`, `code-reviewer`, `chatbot`, `subagent` [C, personas.json].
5. **Root control matrix** — `root_checkpoint` (`execution.rs:5046-5091`) [C]: root tools are `spawn_superagent, assign_task, spawn_remote_agent, spawn_daemon, propose_improvement, read_memory_summary, finish`; `MAX_ROOT_TOOL_CALLS = 4` (`:5084`); exactly 2 turns (`:5093`); turn 2 refuses creation (`:5127-5145`); `spawn_superagent` is refused when the planner already produced >1 node (`has_planned_branch = plan.nodes.len() > 1`, `:5079`, `:5148`) so control cannot silently expand topology; `assign_task` only targets a **direct child of root** (`:5214-5236`).
6. **Caps** — `caps.rs:9-18` [C]: depth 3, fan-out 8, 64 agents/run, 128 provider calls/run; `check_spawn` (`caps.rs:52-63`) [C]. `spawn_sub_hive` also enforces `depth ≥ 2` for subagents (`execution.rs:1977-1979`) [C].

### 2.4 Memory scopes

- Table `memory_banks(kind, scope_id, parent_bank_id, owner_task_id)` (`db.rs:395-435`) [C]; bank ids are `bank:run:<run>`, `bank:branch:<task>`, `bank:hive:<superagent task>` (`store.rs:1834`, `store.rs:264`) [C].
- **Read set** — `memory_read_set` (`store.rs:2687-2740`) [C]: exactly four banks — own branch, **parent** branch, run bank, and `nearest_hive_bank`. Sibling and foreign-run banks are not addressable (doc comment `:2687-2688`, test `store.rs:4062-4070` shows parent visible / sibling hidden).
- `nearest_hive_bank` (`store.rs:3264-3292`) [C]: walks ≤8 ancestors, returns the first `class == "super"` task's hive bank, stops (returns `None`) at `class == "hyper"`. So a team-lead/assistant reads **its manager's shared hive bank** without reading siblings. This is the one real "tree-shared memory" seam that exists today.
- **Write** — `record_agent_memory_scoped(run, task, scope_id, agent_key, key, value)` (`store.rs:1811-1823`) [C], and the bank is auto-created from the task's class (`store.rs:1834-1932`) [C]. The engine itself writes two run-bank entries at plan time: `engine/prompt` (prompt truncated to 2 000 chars, `execution.rs:2350-2357`) and `engine/plan` (role:depth digest, `execution.rs:2365-2372`) [C].
- **Context seeding** — `seed_task_context` (`execution.rs:1647-1719`) [C]: inherit the parent's facts **minus** keys prefixed `a2a:` and `roster:` (`:1655-1662`); attach the persona's allowed tools/output schema; insert ≤8 memory entries as `memory:run:*`/`memory:*`; insert ≤16 roster facts `roster:<peer>:role|parent`; insert ≤8 A2A messages as `a2a:<from>:<kind>:<id>`.
- **Context rewire rules** — `context.rs:15`, `161-198` [C]: clearing facts scoped to the old parent, keeping root facts, injecting the new parent's facts.
- **HyperAgent read projection** — `memory_hyper_summary` (`store.rs:2742-2756`, `hyper_summary_rows` `:3296-3314`) [C]: run + hive banks only, never a raw sibling bank.
- What does **not** exist: cross-run memory, embeddings/vector recall in this path (semantic recall scans banks by literal/lexical means — `semantic_recall.rs:803-870` [C]), and any memory write by a *peer* into another branch's scope.

### 2.5 Chat bus and escalation

- Messages are durable rows with kinds `ask_parent` / `send_to_sibling` (`store.rs:1597`, `:1328`, `:1406`) [C]; surfaced in-prompt as bounded `a2a:` facts (`execution.rs:1704-1717`) and mid-task as a continuation delta (`a2a_updates_for_task`, `execution.rs:4880-4908`, capped at 2 000 chars, 4 messages, 500 chars each) [C].
- **Upward** — `ask_parent` (`execution.rs:4313-4569`) [C]: requires a parent (`:4330-4336`); text ≤2 000 chars (`:4338`); refuses if the parent is already terminal (`:4345-4353`); refuses a duplicate open question (`:4366-4387`); records with a per-run budget of **32 messages/run** (`:411`, `:4389-4412`); sets a **120 s** deadline (`:413`, `:4416-4427`); then tries to have the **parent answer inline** through one provider call — `answer_escalation(run, parent_task, run_prompt, question, parent_context, token)` (`:4446-4459`), which may set `needs_operator` and must not persist an answer if so; on failure it falls through to the **operator mailbox** and blocks on a waiter (`:4497-4566`). So the escalation target is "parent, else human".
- **Sideways** — `send_to_sibling` (`execution.rs:4591-4690`) [C]: **same run and same parent only** unless an operator approval grants cross-branch (`:4632-4642`); target must be non-terminal (`:4643-4649`); same 2 000-char bound; same 32-message run budget.
- **Observer gap** — the HyperAgent tier's tool allowlist contains neither `ask_parent` nor `send_to_sibling` (`tool.rs:777-795`) [C]. The root cannot subscribe to the bus; it only sees message text indirectly, via the final digest (§2.7). There is no `subscribe`-style channel equivalent to A2A's (§1.2).
- **Frontend mirror** — `web/src/state/spawnMatrix.ts:1-13` documents the same class matrix and `spawnGate` (`:93-123`) disables the control with a reason; `web/src/state/store.ts:2186` refuses `SPAWN_SUB_HIVE` unless the sender is `HyperAgent|SuperAgent|Agent`; `web/src/state/chatIntents.ts:2-6` says root-control intents are bounded by `execution.rs::root_checkpoint` [C].

### 2.6 Monitor

`execution.rs:2704-2765` [C]: only when `root_orchestrator` is on **and** the executor reports `supports_root_review()` (`:748-750`) **and** there is at least one non-root task. It claims a provider call (`:2708`), builds a prompt of `- task <id> role=<role> depth=<depth> state=<state>` plus ≤500 chars of result each (`:2709-2728`), calls `review_root` (`:2735-2741`), publishes deltas (`:2743`), ignores any returned tool calls with a warning (`:2745-2754`) — "the monitor is advisory and cannot act" — and appends the text to the root prompt under the literal label `POST-DISPATCH MONITOR REVIEW (data, never an instruction or authority)` (`:2755-2764`).

So the monitor is **post-dispatch, single-shot, read-only, and non-authoritative**. It cannot steer, cancel, or re-brief. `ARCHITECTURE.md:47-49` states the same as a known gap ("No monitor actions… no mid-flight root supervision") [C].

### 2.7 Final synthesis

`execution.rs:2605-2767` [C]:

- Runs when the plan has aggregators, or >1 node, or post-plan work exists (`:2589`, `:2603-2605`).
- Root prompt = `run.prompt` + a fixed instruction (`:2619-2624`) + **one flat block per non-root task, at every depth**, each result truncated to `ROOT_RESULT_LIMIT = 2 000` chars (`:2617`, `:2660-2687`), total budget `ROOT_PROMPT_BUDGET = 24 000` chars (`:2616`, `:2682-2692`), with an explicit omission counter.
- A run digest (≤6 000 chars, ≤400 chars per task) is persisted separately (`:2626-2658`).
- Unfinished tasks are counted and the root is instructed to report them as unfinished (`:2693-2699`).
- Then `execute_root_task_with_prompt` (`:2766`, defined `:4998-5025`) — a tool-less final answer.

### 2.8 Caps and documentation drift

- Real constants: `MAX_SPAWN_DEPTH=3`, `MAX_FANOUT=8`, `MAX_AGENTS=64`, `MAX_CALLS=128` (`caps.rs:9-18`) [C]; `MAX_TOOL_TURNS=12`, `MAX_CALLS_PER_RUN=100`, `MAX_TOOL_RESULT_BYTES=65 536`, `MAX_APPROVAL_WAITS_PER_RUN=8` (`tool_loop.rs:62-69`) [C].
- `ARCHITECTURE.md:45-46` claims "64 provider calls/run … 40 tool calls/run" [C, read]. Both are stale: provider calls are **128** (`caps.rs:18`), tool calls per run are **100** (`tool_loop.rs:64`). This is doc drift, not a code defect — but any test written against the doc number will be wrong.

---

## 3. Conflicts with the intended CEO → manager → team-lead → assistant model

| # | Invariant | Intended | Actual, with evidence | Consequence |
|---|---|---|---|---|
| **C1** | I3 | CEO reads manager reports | Root synthesis iterates **every** task `id != root.id` at any depth (`execution.rs:2628`, `:2660`) | The manager's compression is bypassed; the CEO pays for raw assistant text and can prefer a leaf over its manager's judgement. Also blows the 24 000-char budget and drops results (omission counter, `:2688-2692`). |
| **C2** | I3 | Each manager aggregates its own direct reports, once, in one place | Aggregation exists but is **duplicated**: identical coalescing logic in the sequential loop (`execution.rs:2512-2559`) and in `build_task_prompt` (`:2861-2918`). Inline `spawn_agent` children are joined synchronously and their text returned as the tool result (`:4750-4773`) — correct. But a root-control `spawn_superagent` branch is deferred (`:2005`, drained `:2593`) and executed by `execute_task`, which injects **no** child results (`:4943-4959`) | Not a missing feature — a drift seam. Any future change to the result contract (e.g. adding a `CONFIDENCE` label) must be made twice or one dispatch mode silently regresses. The deferred-branch case is the one path where a manager genuinely cannot aggregate. |
| **C3** | I1 | One level at a time | Enforced correctly (`execution.rs:513-544`, `:488-495`) — **no conflict**. But the root control turn may inject an extra depth-1 lead only when the plan is flat (`:5079`, `:5148`), so topology can be decided by two different authorities | Not a violation, a *split authority*: the plan and the root control can disagree about how many branches exist. Keep it, but document which one wins (§4.1 rule R3). |
| **C4** | I2 | Every seat gets a brief | Planned nodes carry only `role`, `deps`, `depth`, `parent` (`planner.rs:54-68`) — **no objective field**. `execute_task` passes the raw run prompt when no instruction was stored (`execution.rs:4949-4956`); only root-control `assign_task`/`spawn_superagent` store an instruction (`:5180`, `:5220`) | The planner can create a manager/team-lead with no brief at all. This is exactly the Anthropic failure mode ("short briefs cause duplicated work"). The 12-line result contracts are good, but they are *output* contracts with no matching *input* brief. |
| **C5** | I2 | Delegation brief names boundaries + budget | Persona prompts carry boundaries and retry limits (`superagent.prompty`: "At most 3 workers", "retry a failing step at most twice"; `agent.prompty`: retries ≤3) [C], but nothing in the engine passes a per-task budget | Prompt-only discipline; the engine's real limits (12 turns, 100 calls) are invisible to the seat it constrains. A team-lead cannot scale effort because it cannot see its budget. |
| **C6** | I4 | Peer bus inside the tree; upward escalation | `send_to_sibling` is *same-parent only* (`execution.rs:4632-4642`); `ask_parent` targets the parent and then the human (`:4446-4556`). Both are capped at 32 messages/run for the **whole run** (`:411`) | Team-leads in different branches of the same manager cannot talk at all (they share a parent? no — they share a parent only if siblings under the same manager; a team-lead under manager A cannot reach a team-lead under manager B). The "per-tree bus" the operator wants does not exist; and 32 messages is a run-wide budget that a single noisy branch can exhaust. |
| **C7** | I6 | HyperAgent observes the tree | The root's tool allowlist has no `ask_parent`/`send_to_sibling` (`tool.rs:777-795`); the only tree-wide view is the post-dispatch monitor (`execution.rs:2704-2765`), which is single-shot, post-hoc, and cannot act | "HyperAgent observer" is currently a **report**, not an observer. There is no subscribe channel (contrast A2A `Subscribe to Task`, §1.2) and no mid-flight visibility. `ARCHITECTURE.md:47-49` admits it. |
| **C8** | I5 | Tree-scoped shared memory | Read set = own + parent + run + nearest **SuperAgent** hive bank (`store.rs:2687-2740`, `:3264-3292`). Manager hive banks exist and are readable by descendants — good. But nothing lets a manager **write** into its own hive bank on behalf of the tree in a first-class way, and no API prevents a manager from writing branch-private facts that its team-leads then never see (inheritance happens once, at seed time, `execution.rs:1647-1719`) | Memory is *snapshot-inherited*, not *shared*. A fact learned by a team-lead after seeding never reaches the manager; a fact written by the manager after seeding never reaches an already-seeded child (mid-task refresh only re-reads the A2A inbox, `execution.rs:4914-4940`, and the memory bank, `:4975` — but the bank refresh happens once at task start, `:4966-4976`). |
| **C9** | I8 | One span of control, engine-enforced | Three numbers for one limit: engine 8 (`caps.rs:11`), persona `max_children: 8` (`personas.json`), SuperAgent prompt "At most 3 workers per branch" (`superagent.prompty`), planner prompt "at most 8 children per parent" (`planner.prompty`) | The prompt-level span (3) and the engine span (8) disagree. A model obeying 8 produces a branch its own brief called over-spawning; a model obeying 3 under-uses capacity. Pick one and make the other a derived constant. |
| **C10** | I7 | CEO refuses to invent | Root is instructed not to invent and to report unfinished work (`execution.rs:2619-2624`, `:2693-2699`), and the digest marks `no recorded result` (`:2646-2651`) — **no conflict** | Keep. This is the strongest part of the current design. |

**Root cause statement.** HiveMind has both halves of a hierarchy *for planned work* — authority flows down through three independent gates, and direct-child reports flow back up — so the model is structurally closer to correct than the flat root prompt suggests. Two things break the org model instead: (a) the root's final synthesis **bypasses every manager** by reading raw text at all depths (§2.7, C1), and (b) the **brief** half is absent for planned seats (§3, C4), so the downward flow carries an identity but no objective. Fixing (a) and (b), and collapsing the duplicated aggregation (C2), closes most of the gap without new tables or tiers.

---

## 4. Target design (pseudocode — no invented APIs)

Rules: only types and functions that exist today are named (`PlanNode`, `TaskRecord`, `ScopedContext`, `GraphAuthority`, `RunStore::record_agent_message*`, `memory_read_set`, `memory_hyper_summary`, `ToolRegistry::allowlist`). Anything not in the repo is marked `NEW:` and must be added as a seam in §5, not assumed.

### 4.1 Delegation

```text
plan_run(run):
  # R1  the planner must emit a brief, not just a role
  plan = executor.plan(run.prompt)                    # unchanged call
  plan.validate()                                     # unchanged
  for node in plan.nodes:                             # NEW: brief gate
      if node.objective is empty or len(node.objective) < 8 or > 800:
          refuse(E_SCHEMA_INVALID, node.node_id, "planned node has no brief")
  # R2  cardinality is a plan property, validated before any row exists
  assert one depth-1 lead per deliverable, not per phase

root_control(run, root, plan):
  # R3  precedence: the PLAN owns topology; control may only ANNOTATE
  allowed = {assign_task}                             # NEW: drop spawn_superagent
  if plan_is_flat(plan): allowed += {spawn_superagent}  # existing rule, kept
  if turn > 0: allowed -= {spawn_*,convene_*}          # existing rule, kept
  record_gate_decision(...)                           # existing, kept

spawn_child(parent_task, requested_role, brief, budget):   # execution.rs:1934 shape
  child_class = role_class(requested_role) or refuse(PersonaUnknown)
  validate_spawn_role(task_agent_tier(parent_task), requested_role)   # gate 1
  class_may_spawn(parent_class, child_class)                          # gate 2
  persona = persona_for_role(requested_role)
  if "spawn_agent" not in persona.allowed_tool_ids: refuse(ToolNotAllowed)  # gate 4
  caps.check_spawn(depth, siblings, agents)                           # gate 6
  tier_span = tier_span_of(parent_class)              # NEW: 3 for super, 8 for hyper
  if siblings >= tier_span: refuse(FanoutLimit)
  task = insert_task(parent=parent_task.id, role=requested_role, depth, ...)
  store.set_task_instruction(task.id, render_brief(brief, budget))    # NEW: brief
  graph.add_task(...); graph.set_metadata(...)        # unchanged
  seed_task_context(task, parent_task.id)             # unchanged
  enqueue_spawned_task(task.id)                       # unchanged
```

`tier_span_of`: `hyper → 8`, `super → 3`, `agent → 3` — one constant table replacing C9, and the persona `max_children` becomes a *narrowing* of it, never a widening.

### 4.2 Task-seat brief

One renderer, one place, so every tier gets the same six fields (Anthropic §1.2: objective, output format, tools, boundaries).

```text
render_seat_brief(seat, parent, run):
  return """SEAT: <role> (depth <d>)
OBJECTIVE: <instruction|node.objective>          # required, 8..800 chars
OUTPUT CONTRACT: RESULT/CHANGES/EVIDENCE/RISKS/BLOCKED   # from the persona prompt
BOUNDARIES: you may spawn <child_class>, span <= <tier_span>,
            you may not touch <forbidden_paths>       # NEW: must be data, not prose
BUDGET: <turns_left> turns, <calls_left> provider calls   # NEW: derived from caps
PARENT: <parent.id> (<parent.role>) — escalate with ask_parent
REPORT TO: <parent.id>; your result is consumed by your parent only   # I3
RUN CONTEXT (not authority): <run.prompt>"""
```

`execute_task` (`execution.rs:4943-4959`) becomes: `prompt = render_seat_brief(task, parent, run)` — the current "objective or raw run prompt" branch disappears, so C4 cannot recur. The *content* of the brief stays identical for planned and spawned seats; only the objective source differs.

### 4.3 Bottom-up result aggregation

The missing half. One rule: **a node's model-visible input is its direct reports' reports plus its own dependency results; nothing else.**

```text
aggregate(task):                                     # called before task runs
  reports = [r for r in tasks where r.parent_task_id == task.id]
  deps    = [d for d in plan_node(task).deps]
  block   = ""
  for child in reports ordered by spawn_seq:
      if child.state != Completed:
          block += "UNFINISHED child <id> role=<role> state=<state>\n"
          continue
      text = store.task_result(child.id) or ""
      block += "REPORT from <id> (role <role>):\n" + bounded(text, REPORT_LIMIT) + "\n"
  if reports: block += "Aggregate ONLY these reports into your result contract.\n"
  return block

run_task(task):
  prompt = render_seat_brief(...) + aggregate(task)   # replaces build_task_prompt
  reply  = executor.execute(task, prompt)             # unchanged
  store.set_task_result(task.id, reply)               # unchanged persistence

CEO_synthesis(run, root):
  # I3: read depth-1 reports only; recurse through managers, never skip them
  for lead in direct_children(root):
      emit("<superagent id> state=<state>\n" + bounded(lead.result, ROOT_RESULT_LIMIT))
  # no depth-2/3 rows here. If a lead has no result, say so.
  prompt += monitor_note (data-labelled, existing)
  finalize_root(root, prompt)                         # existing call
```

Two consequences to make explicit:

- **Depth-2/3 text is reachable only if a manager copies it into its own report.** That is the compression the hierarchy exists for; the 12-line contract already forces it.
- **The same `aggregate()` must run in all three execution shapes** — sequential (`execution.rs:2490-2586`), parallel (`:2783-2857`), and deferred spawn-queue (`:2952-3000` → `execute_task`). Today the third shape has none; that is the one behavioural fix in S3, the rest is deduplication.
- **`ROOT_PROMPT_BUDGET` (24 000) now bounds *reports*, not raw transcripts**, so the omission counter (`execution.rs:2688-2692`) should become ~0 for a legal tree (≤8 leads × 2 000 chars = 16 000).

### 4.4 Per-tree chat bus + HyperAgent observer

Keep the durable message rows. Add *scope* and *subscribers* — nothing else.

```text
Message = { id, run_id, tree_id, from_task, to_task, kind, text, status, deadline }
# NEW field tree_id = id of the depth-1 lead that owns this branch (root's own id for the root)

send(kind, from, to, text):
  scope = {
    "ask_parent":       to == parent(from),
    "send_to_sibling":  tree_id(to) == tree_id(from) and to != from,   # NEW: tree scope
    "report_up":        to == parent(from),                            # report_result exists
    "broadcast_down":   is_ancestor(from, to),                         # NEW, managers only
  }[kind] or refuse(ToolNotAllowed)
  budget = per_tree_budget(tree_id(from))            # NEW: not per run (fixes C6)
  store.record_agent_message_limited(..., budget)

observe(watcher_task, tree_id):                      # NEW: HyperAgent observer
  # pull-based, non-blocking, no provider call per message
  messages = store.agent_messages_for_tree(run, tree_id, since=cursor, limit=64)
  cursor = max(messages.id)
  # render as data, same discipline as the monitor note:
  #   "TREE <id> EVENT <kind> from=<id> to=<id> state=<status>: <bounded text>"
  # the observer may NOT act (no spawn/cancel/re-brief in v1)
```

`ask_parent` keeps its current ladder: parent inline answer → operator mailbox → 120 s deadline (`execution.rs:4431-4566`) — the human is still the terminal escalation, which is correct and should not change.

The HyperAgent observer is a **read-only consumer of the same rows** the monitor already reads (`execution.rs:2713-2728`). v1 exposes it as one extra `review_root`-style call between dispatch phases; it must remain `supports_root_review()`-gated so test doubles keep deterministic budgets (`execution.rs:748-750`).

### 4.5 Shared memory read/write

```text
write(scope, key, value, trust):
  # scope is explicit, never inferred from the caller's rank
  case scope of
    "branch":  bank = "bank:branch:" + task.id        # private (today's default)
    "tree":    bank = "bank:hive:" + tree_id          # manager-owned, tree-readable
    "run":     bank = "bank:run:" + run_id            # constraints only
  require: "tree" writes are allowed only when caller owns tree_id
  record_agent_memory_scoped(run, task, bank_scope, role, key, value)   # existing

read(task):
  banks = [run, parent_branch, own_branch, nearest_hive_bank(task)]     # existing
  entries = memory_read_set(run, task.id, parent, limit=8)              # existing
  # NEW: a manager also reads its tree bank on EVERY turn, not once at seed time
  return entries
```

The only structural change needed for C8 is **refresh at each turn instead of once at task start**: `execute_task_with_prompt` already calls `refresh_memory_bank` + `refresh_a2a_inbox` once (`execution.rs:4975-4976`); the target is to call the same two functions between tool turns, so a fact written by a manager mid-branch reaches its team-lead's next turn.

### 4.6 Escalation ladder (unchanged, but make the rungs explicit)

```text
escalate(child, question):
  1. parent inline answer   (1 provider call, parent gets its own context)   # exists
  2. operator mailbox       (durable row + waiter + 120 s deadline)          # exists
  3. expire → child gets E_CALL_LIMIT-style refusal, reports BLOCKED         # exists
  # target adds: 1b. if parent is itself BLOCKED, the parent may escalate with its own
  #   ask_parent — this already works because the parent has the tool; the root does NOT,
  #   so a top-level ambiguity correctly lands on the operator.
```

---

## 5. Minimal code seams and tests

### 5.1 Seams (smallest set, in dependency order)

| # | Seam | File / symbol | Change | Fixes |
|---|---|---|---|---|
| S1 | Plan brief field | `hiverouter-core/src/planner.rs:54-68` `PlanNode` (`#[serde(default)] objective: String`) + `validate()` before fan-out check | add optional `objective`; refuse 8..800 violation | C4 |
| S2 | Seat brief renderer | `execution.rs:4943-4959` `execute_task` + new `render_seat_brief` next to `build_task_prompt` (`:2861`) | one renderer for planned and spawned seats | C4, C5 |
| S3 | One aggregation path | extract `execution.rs:2512-2559` + `:2861-2918` into one `aggregate(task) -> String`; call it from both dispatch modes and from `execute_task` (`:4943-4959`) | one implementation; deferred branches gain aggregation | C2 |
| S4 | Root digest input | `execution.rs:2660-2692` loop | filter to `task.parent_task_id == Some(root.id)` | C1 |
| S5 | Span of control table | `caps.rs` new `fn tier_span(class) -> usize`; consult in `spawn_sub_hive` (`execution.rs:1961-1973`) and plan validation (`planner.rs:202-206`) | one number per tier | C9 |
| S6 | Message scope | `store.rs:1328/1597` signature + `execution.rs:4632-4642` | add `tree_id` (or derive by ancestor walk ≤8 like `nearest_hive_bank`, `store.rs:3264`) | C6 |
| S7 | Per-tree budget | `execution.rs:411` `MAX_AGENT_MESSAGES_PER_RUN` → per-tree counter | count by tree, not run | C6 |
| S8 | Observer | `execution.rs:2704-2765` region + `ProviderExecutor::review_root` (`:756-765`) | second read-only call between phases, same data discipline | C7 |
| S9 | Per-turn memory/a2a refresh | `execution.rs:4975-4976` → inside the tool-turn loop near `:6389` | refresh between turns | C8 |
| S10 | Docs | `ARCHITECTURE.md:45-46` | 128 provider calls, 100 tool calls | §2.8 |

Not needed: new tables, new tiers, new refusal codes, frontend changes (`spawnMatrix.ts` already encodes the class matrix and the **root may not spawn Agents** rule).

### 5.2 Tests (name each one after the conflict it locks)

| Test | File (existing suite) | Asserts |
|---|---|---|
| `planned_node_without_brief_is_refused` | `hiverouter-core/tests/orchestration_plan_quality.rs` | S1 refusal is named, before any task row |
| `team_lead_sees_only_direct_reports` | `tests/orchestration_plan_quality.rs` | build/aggregate output contains a child report and **not** a grandchild report |
| `root_synthesis_reads_only_depth_one` | `tests/hyperagent_governance.rs` | root prompt excludes depth-2/3 results; unfinished lead is labelled unfinished |
| `span_of_control_matches_prompt_and_engine` | `tests/scale_s0.rs` | 4th child under a SuperAgent refused when tier span is 3; 8 still allowed under the root |
| `cross_branch_message_refused_same_tree_allowed` | `tests/suspension.rs` (has `send_to_sibling` at `:233`, `:569`) | same-tree peer allowed; cross-tree refused by name |
| `message_budget_is_per_tree_not_per_run` | `tests/suspension.rs` | exhausting branch A's budget leaves branch B usable |
| `monitor_is_read_only_and_data_labelled` | `tests/hyperagent_governance.rs` | monitor tool calls ignored (`:2745-2754`) + label present |
| `memory_written_mid_branch_reaches_child_next_turn` | `tests/mem_isolation.rs` | C8: fact after seed is visible on the child's next turn; sibling still cannot see it |
| `architecture_caps_match_caps_rs` | `tests/prompt_assets_wiring.rs` or a doc-truth test | §2.8 drift cannot return |

Each test must be *run* and its output quoted before the seam is called done (`AGENTS.md` §22).

---

## 6. Unknowns

- **[?]** Which tier the operator means by "manager" vs. "team-lead" (§1.1). Changes C1/C2 ownership. Settled by: one sentence in the spec, e.g. "SuperAgent aggregates its branch; the root never reads below depth 1."
- **[?]** Whether `parallel_dispatch > 1` is ever enabled in production. It changes whether C2 is severe (sequential: managers never aggregate) or mild. Settled by: the runtime config at `http.rs:444` in a real launch.
- **[?]** Whether `root_orchestrator` is on in the shipped profile. `RunEngine` defaults it `false` (`execution.rs:1081`, `:1376`); the production wiring path was not read. If off, there is no root control *and* no monitor, and C7 is total.
- **[?]** A2A field-level semantics beyond the TOC (§1.2). Not relied on; would need a section-level read of §4.1 before any wire-format work.
- **[A]** Span-of-control numbers are convention, not fetched primary evidence. Do not cite a number from this doc.

---

## 7. Report

**Done**

| Item | Evidence |
|---|---|
| Four primary sources read and cited (Anthropic, CrewAI, OpenAI Agents SDK, A2A spec) | §1.2, URLs inline |
| Full audit of plan → root control → spawn matrix → prompts → memory → monitor → synthesis | §2, all `file:line` read in this session |
| Ten conflicts, each tied to an invariant and a line | §3 |
| Pseudocode for delegation, brief, aggregation, tree bus + observer, shared memory | §4 |
| Minimal seams S1–S10 + nine tests | §5 |

**Not done**

| Item | Reason |
|---|---|
| No code edited, nothing committed or launched | Explicit constraint; this file is the only writable artifact |
| No runtime observation of a live run | Would need a provider key and a launch; out of scope for a read-only audit |
| No A2A section-level read | TOC-level only was enough for the structural claims; deeper reading would not change the design |

**Unverified**

| Claim | What would prove it |
|---|---|
| Tier mapping CEO/manager/team-lead/assistant = Hyper/Super/Worker/Subagent | Operator confirmation (§6) |
| `root_orchestrator` and `parallel_dispatch` effective values in the shipped build | Read the production `RunEngine` construction path and a live `/api` health payload (`http.rs:4754` exposes `parallel_dispatch`) |
| Span-of-control guidance | A fetched primary management source; none used |
| That S3/S4 keep the root prompt under 24 000 chars for a legal 8-branch tree | A unit test asserting prompt length for a synthetic 8×2×2 tree |
