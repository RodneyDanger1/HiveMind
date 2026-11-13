# Delegation Audit — persisted evidence (2026-11-13)

- **Repo:** `C:\Users\Riley\Desktop\hiverouter-v1` @ `946abd4` ("wip: preserve multi-agent capability and UI patches")
- **Captured:** 2026-11-13T16:33:46-06:00 (2026-11-13T22:33:46Z)
- **Mode:** read-only. No code edit, no commit, no window launch, no server start.
- **Live endpoint:** `http://127.0.0.1:8099` — **DOWN** (`Invoke-WebRequest /api/health` → "Unable to connect to the remote server", 16:21:10-06:00). Live probe skipped; no live UI claim is made.
- **Token:** `%LOCALAPPDATA%\HiveMind\launch.token` exists (32 bytes, mtime 2026-11-11T03:02:03). **Not read** — no live endpoint needed it.
- **Persisted source:** `%LOCALAPPDATA%\HiveMind\hiverouter.db` (30,502,912 B; WAL 16,777,216 B, last write 14:53:48-06:00) opened with `sqlite3 -readonly` (3.53.4). Counts: 26 runs, 117 tasks, 37 task_results, 37 sessions, 116 session_events, 21 approvals, 23 run_usage, 97 agent_memory / 97 memory_journal.
- **Reference DBs rejected as empty:** `docs/evidence/live/uiux-gate-20261113-111911/data/hiverouter.db`, `ledgers/visual-dock-8099/data/hiverouter.db` (0 runs / 0 tasks each).

Evidence classes used below: **VERIFIED** = DB row or executed command output; **READ** = source line read; **INFERRED** = conclusion from both.

---

## Findings table

| ID | Sev | Pattern | Run / task | Code seam | Expected | Actual (evidence) | UI symptom |
|---|---|---|---|---|---|---|---|
| D1 | **P0** | Blocked child reported completed | `01a1d694-f3e5-…` run; task `01a1d694-facb-719a-9936-ab15573dd37d` (agent, d2) | `execution.rs:6367-6392` → `complete_task` `execution.rs:6456-6492`; guard `execution.rs:2646` | `report_result(success=false)` settles the task as blocked/failed; parent synthesis counts it unfinished | Task `state='completed'`, result text `report_result success=false: BLOCKED — tool not available…` (VERIFIED). Same shape: run `01a1d677-92d6…`, task `…d14b0271966e` state `completed`, result `report_result success=false: BLOCKED — could not read HIVE-STATUS.md` (VERIFIED) | Agent card/run shows a successful child; run shows `completed`; the block is only prose inside the answer |
| D2 | **P0** | Approval still pending after terminal run | run `01a1d6a7-6a98-71f6-acb9-b8c70bea1491` | reaper `http.rs:256-264` (boot only); list `http.rs:795`, `http.rs:2553-2584` | Terminal run reaps/settles its pending approvals; `/api/approvals` never lists a row whose run is terminal | `approvals` row `decision NULL`, `expires_at 2026-11-13T20:46:59Z`; run `failed E_TOKEN_BUDGET_EXCEEDED` at `20:45:29Z` (VERIFIED). Row still `decision NULL` at 14:50 local, ~3.8 h later | Approvals panel offers an actionable `shell_exec` for a run that is already dead; resolving it can never execute |
| D3 | **P1** | One expired approval terminalizes the whole run; completed sibling is stranded | run `01a1d560-8607-71e4-bdab-94ef16ae23bd` | `execution.rs:4236-4299` (`finalize_approval_timeout`), `4186/4227/4282` | Fail the waiting task, keep the run able to synthesize already-completed children, or surface partial evidence with the failure | 4 tasks, 1 `completed`; approvals: 2 `allow_once`, 1 `expired` (`decided_at == expires_at == 14:49:41Z`); run `failed E_APPROVAL_TIMEOUT` (VERIFIED) | Run header `failed`; the one completed child answer is never shown as a result of the run |
| D4 | **P1** | Monitor has no authority and sees truncated results | run `01a1d694-f3e5-76e8-9c27-e836f7a5e84b` (planner `…facc`) | `execution.rs:2724-2743` (500-char cap), `2733` (`if let Ok(Some(text))` silently skips unreadable result), `2760-2779` (tool calls ignored, note appended as data); `provider_executor.rs:56,728-752` | Monitor audit is bounded **and** labelled as machine-truncated; unreadable results are reported, not silently omitted; advisory text never becomes user-visible synthesis prose | Final planner answer contains `Monitoring note (advisory) … the parent's stored result was truncated in the monitor excerpt` and `POST-DISPATCH MONITOR REVIEW` text (VERIFIED in `task_results`) | Final answer tells the user their stored result is "truncated" — a monitor-prompt artifact presented as product data; no repair is possible because the monitor cannot act |
| D5 | **P1** | One `E_CALL_LIMIT` / one token code for five different budgets; no numbers persisted | runs `01a1d6a5` (51 calls, 23 turns), `01a1d6ad` (80 calls, 44 turns), `01a1d6a7` (61 calls, 35 turns), `01a1d650` (42), `01a1d5ee` (41), `01a1d5e9` (21) | `caps.rs:18` `MAX_CALLS=128`; `tool_loop.rs:62` `MAX_TOOL_TURNS=12`, `:64` `MAX_CALLS_PER_RUN=100`, `:69` `MAX_APPROVAL_WAITS_PER_RUN=8`; `budget.rs:272-323` (`call_cap`, `token_budget_combined`) | Refusal names the limit that tripped and the measured/spent value; DB keeps the detail | `runs.error_code` only (`E_CALL_LIMIT`, `E_TOKEN_BUDGET_EXCEEDED`); `runs` has no detail column (VERIFIED schema). Failures at 21/41/42/51 calls cannot be the 128- or 100-call cap, so they are per-task turn/approval caps under the same code (INFERRED) | Run detail shows a bare `E_CALL_LIMIT`; user cannot tell whether to raise calls, turns, approvals, or tokens |
| D6 | **P2** | Plan refusal (`E_SCHEMA_INVALID`) loses its reason; no task rows | runs `01a1d6ac-3810…` (0 calls, 0 tasks), `01a1d6a4-05bd…` (0/0), `01a1d5ba-f849…` (0/0) | `execution.rs:2329` `plan.validate()?`, `2310-2328` one repair attempt; `runs` schema (`db.rs`) has `error_code` only | The refusal's `detail` (offending node/role/depth) reaches the run record and UI | Run row stores only `E_SCHEMA_INVALID`; no task, no detail (VERIFIED) | "Run failed: E_SCHEMA_INVALID" with no node, role, or reason to fix |
| D7 | **P2** | Silent partial scan in tool evidence | run `01a1d694` child `…ab15573dd37d` result | tool result text (VERIFIED); `repo_index.rs:369-386` skipped counters | A scan that skipped 157 files says so via `complete:false`/counts | Text: `fs_grep("harness_diagnose") -> {"matches":[],"files_scanned":379,"files_skipped":157,"truncated":false}` — skipped ≠ truncated, no completeness flag | "No matches" reads as proof of absence when 29% of files were skipped |
| D8 | **P3 / UNVERIFIED** | Duplicate parent/child answers | run `01a1d6a0`, superagent `…7b3a` results 1044 vs 1568 chars; run `01a1d6a4` planner `…8a34` 3263 chars restates children | aggregation/synthesis path | Child answer and parent answer are distinct deliverables | Near-duplicate paraphrase only; **no byte-identical duplicates** in `chat_messages` or `task_results` (`GROUP BY text HAVING count(*)>1` → 0 rows, VERIFIED) | Two very similar answers in the transcript — cosmetic, not a duplicate-write bug |

---

## Top 5 detail (P0/P1)

### D1 — Blocked child persisted as `Completed` (P0)

**Chain:** `report_result` executes with `success=false` → `execution.rs:6376-6385` computes `report_text = "report_result success=false: {summary}"` → `execution.rs:6386-6392` builds a terminal reply and calls `complete_task` **unconditionally** → `execution.rs:6483-6492` writes the text, `record_task_result`, then `update_task_state(task.id, AgentState::Completed)`.

- The failure is encoded **only as a string prefix**. No `Blocked`/`Failed` state transition exists on this path.
- `execution.rs:2646` (`if child.state != AgentState::Completed`) is the root prompt's "unfinished children" gate. Because the blocked child is `Completed`, the root is told nothing is unfinished (`unfinished_children == 0`) — directly contradicting the child's own text.
- The engine's own monitor noticed it in run `01a1d694`: `The depth-2 task's state reads Completed while its bounded result is an explicit failure, so the state masks that it yielded no successful result` (persisted in planner result `…facc`). This is the product describing its own defect.

**Evidence rows (VERIFIED):**
```
tasks: 01a1d694-facb-719a-9936-ab15573dd37d | completed   (role=agent, depth=2)
task_results: "report_result success=false: BLOCKED — tool not available. … no such tool is in my granted toolset …"
tasks: 01a1d677-9993-76ef-8d7a-d14b0271966e | completed
task_results: "report_result success=false: BLOCKED — could not read HIVE-STATUS.md; no UI problems repor…"
```

**Expected:** `success=false` maps to a blocked terminal state (or `Failed` with the reason), the parent's unfinished count increments, and `TreeSynthesis::succeeded` is false — per `delegation_protocol.rs:39-45` ("`Failed`/`Cancelled`/`Killed`/`Interrupted` map to … never to `completed`").

**Seam:** `execution.rs:6367-6392` (state decision) and `execution.rs:2646` (parent accounting).

### D2 — Pending approval outlives its run (P0)

**Chain:** run `01a1d6a7` fails on token budget at `20:45:29Z`. Its approval was created `20:44:59Z`, expires `20:46:59Z`, never decided. The only reaper is `http.rs:256` `ToolsBridge::expire_pending_approvals(db, now_unix_secs)` executed **at boot** ("consumed restart-orphaned pending approvals at boot; no waiter survives restart", `http.rs:260`). No run-terminal path calls it.

- `GET /api/approvals` (`http.rs:795`) → `pending_approvals` (`http.rs:2553-2584`) → `pending_approval_events(run_id)` / `pending_approval_events_all(20)`. No run-status filter is visible in the handler.
- Contrast: run `01a1d560`'s timed-out row **is** settled (`decision='expired'`) because a waiter was armed and `finalize_approval_timeout` (`execution.rs:4236-4299`) ran.

**Evidence (VERIFIED):**
```
approvals: run 01a1d6a7… | shell_exec | execute | expires 2026-11-13T20:46:59Z | decision NULL
runs:      01a1d6a7… | failed | E_TOKEN_BUDGET_EXCEEDED | updated 2026-11-13T20:45:29Z
```

**Expected:** a terminal run settles its pending approvals (`expired`/`cancelled`), and the approval list excludes rows whose run is terminal.

**Seam:** `http.rs:256-264` (boot-only reaper) + `http.rs:2553-2584` (unfiltered list).

### D3 — One expired approval fails the run and strands a completed child (P1)

Run `01a1d560` (4 tasks): child `completed`, two `allow_once`, one `expired` at exactly `expires_at` (`14:49:41Z`), run `failed E_APPROVAL_TIMEOUT`. `finalize_approval_timeout` (`execution.rs:4236-4299`) always takes the run terminal via `StateEvent::Timeout`/`Fail`; it does not let the root synthesize an already-completed child. No run-terminal reconciliation of completed children is visible. **UI symptom:** run header `failed`; the completed child's answer never becomes the run's answer.

**Seam:** `execution.rs:4273-4286` (unconditional run transition) vs. the partial-synthesis path at `execution.rs:2646-2714`.

### D4 — Monitor: truncated view, no authority, prose leak (P1)

- Per-task result is cut to **500 chars** (`execution.rs:2737`) and the review note to **2 000** (`:2772`).
- Unreadable results are **silently skipped**: `execution.rs:2733` `if let Ok(Some(text)) = self.store.task_result(task.id)` — no log, no marker in `monitor_prompt`. The monitor then reviews an incomplete ledger.
- Tool calls from the monitor are discarded (`execution.rs:2760-2769`), so it cannot repair anything; its text is appended as "data, never an instruction or authority" (`:2774-2777`) — yet the root model restated that data in its user-visible answer.
- Evidence: `task_results` for planner `01a1d694-facc-7152-9dfc-bf9dd034f9e4` contains `Monitoring note (advisory)` + `the parent's stored result was truncated in the monitor excerpt`.

**Expected:** truncation is labelled as a monitor-input artifact (e.g. `[monitor: result truncated at 500 chars]`), unreadable results are named, and advisory notes never surface as product facts.

**Seam:** `execution.rs:2724-2743`, `2760-2779`; `provider_executor.rs:56,728-752`.

### D5 — Budget refusals collapse five caps into two codes (P1)

Caps actually in force: `caps.rs:18` `MAX_CALLS=128`; `tool_loop.rs:62` `MAX_TOOL_TURNS=12`, `:64` `MAX_CALLS_PER_RUN=100`, `:69` `MAX_APPROVAL_WAITS_PER_RUN=8`; `budget.rs:280` `call_cap`; `:310` `token_budget_combined`.

Observed failures (VERIFIED): `E_CALL_LIMIT` at 21, 41, 42, 51 tool calls — none can be the 128/100 run cap, so they are per-task turn or approval-wait caps reported under the run-level code (INFERRED). `E_TOKEN_BUDGET_EXCEEDED` at 61/77/80 calls with measured totals 5 020 / 6 160 / 6 129 tokens (`run_usage`), and no budget value is persisted with the run (`runs` schema has no detail/limit columns).

**Expected:** `refusal.rs` already carries bounded detail (`budget.rs:283-286`, `:313-316`); that detail must reach `runs` (or a run event) and the UI, and per-task turn exhaustion should be distinguishable from run-call exhaustion.

**Seam:** `execution.rs:6302-6314` / `tool_loop.rs:308-337` (code selection) and `db.rs` `runs` (no detail column).

---

## Cross-cutting: spawn failures and why no partial tree was observed

`plan.validate()` runs at `execution.rs:2329`, **before** the root row (`:2342`). All three `E_SCHEMA_INVALID` runs therefore have **0 task rows** (VERIFIED) — this is correct ordering.

However the plan-node insert loop is non-atomic: children are inserted one by one at `execution.rs:2436`, and two mid-loop `return Err(Refusal::SchemaInvalid)` sites exist (`:2389` unknown parent, `:2507` unresolved node). A plan that passes `plan.validate()` but references a parent not yet inserted would leave the root plus already-inserted children committed. No live run exercises this (INFERRED risk, not a confirmed defect). Recommendation: wrap the tree insert in one transaction.

---

## Probe corrections (kept, per trace discipline)

1. **Prefix collision is not a PK violation.** Eight-char ULID prefixes repeat within a run (`01a1d694-facb-…` has two sibling tasks). Early `substr(task_id,1,13)` listings looked like duplicate PKs; the full `task_id` values differ and `GROUP BY text HAVING count(*)>1` returns 0 rows. No duplicate-write defect.
2. **Empty DB ≠ no evidence.** The newest `docs/evidence/live/...` DB (602 KB) has 0 runs; the live DB is `%LOCALAPPDATA%\HiveMind\hiverouter.db`.
3. **`file:…?mode=ro` fails on a WAL DB here; `sqlite3 -readonly` works.** Do not treat the resulting "no such table" error as schema absence.

## Not covered / UNVERIFIED

- No live UI capture (8099 down). All UI symptoms above are derived from persisted rows + source, not screenshots.
- No token read, no provider call, no approval resolve, no run started.
- `fs_grep` completeness code path was not read to its sink; D7 rests on the tool result text plus `repo_index.rs` skipped counters.
- Duplicate-answer behavior (D8) is paraphrase-only; no byte-identical duplicate exists in the DB.

## Tooling side effect (disclosed, not intended)

The first discovery queries opened four **repository** SQLite artifacts without `-readonly` (`sqlite3 <db> ".tables"`). SQLite checkpointed each WAL into the main file and removed the `-shm`/`-wal` sidecars. `git status` now shows, for each of:
`docs/evidence/live/uiux-gate-20261113-111911/data/`, `ledgers/visual-dock-8099/data/`, `runtime-test/data/`, `runtime-test/pkg-final/data/` —
`hiverouter.db` modified (WAL pages merged; see sizes below) and `hiverouter.db-shm` / `hiverouter.db-wal` deleted.

| Artifact | db before → after | wal before → after |
|---|---|---|
| uiux-gate-… | 602,112 → 602,112 | 11,387,712 → 0 |
| visual-dock-8099 | 4,096 → 602,112 | 2,307,232 → 0 |
| runtime-test/data | 4,096 → 69,632 | 94,792 → 0 |
| runtime-test/pkg-final | 4,096 → 90,112 | 127,752 → 0 |

Logical content is preserved (WAL is merged, not dropped; re-read of the evidence DB still returns 0 runs / 0 tasks), but this is a **binary artifact mutation outside the allowed write scope**. It was not a code edit and no commit was made. Restoration is left to the parent (`git checkout --` on those four paths) because it is a git write and I did not want to compound the mutation.

The **live** DB (`%LOCALAPPDATA%\HiveMind\hiverouter.db`) was only ever opened with `-readonly`; it is not in the repository and was not modified by this audit.
