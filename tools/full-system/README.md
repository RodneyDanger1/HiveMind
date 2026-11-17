# HiveMind full-system campaign — AUXILIARY ONLY

`run.ps1` is a deterministic regression harness. It is **not** the live-app
acceptance test and must not be reported as production-ready. Live acceptance
is defined in `docs/plans/FULL-SYSTEM-LIVE-VERIFICATION.md` and requires the
installed `HiveMind.exe` window, the real provider, and real UI approvals.

The auxiliary harness currently executes:

- preflight identity, installed binary, MCP surface diff;
- a real loopback server with a local scripted provider;
- one scripted run with an approval-gated `file_write`;
- approval replay refusal, checkpoint list/restore, session continuation;
- a second deterministic run (replay equivalence);
- a provider-failure run that must fail closed;
- four independent watchdog processes and hash-chained ledgers;
- secret scan, process cleanup, JSON/Markdown report.

The local provider stub is an **external test double** only. The real RunEngine,
planner, approval gate, tools, SQLite ledger, checkpoints and HTTP/WS contracts
are exercised. No real provider key is used and no paid call is made.

## Run

```powershell
powershell -ExecutionPolicy Bypass -File tools\full-system\run.ps1
```

Artifacts land in `%TEMP%\hivemind-fs\<campaign-id>\`:

- `REPORT.json` / `REPORT.md`
- `evidence\provider.jsonl`, `run-detail.json`, `approvals.json`, `checkpoints.json`, `restore.json`, `continue.json`
- `ledgers\{liveness,wire,artifact,defect}.jsonl` plus `*-REPORT.json`
- `events.jsonl` (campaign event stream)

The runner exits `0` only for `PASS`; `FAIL` exits `1`; a harness/watchdog error
is reported and must never be treated as PASS. Checks not yet wired are emitted
as `NOT_IMPLEMENTED`, not silently skipped.

## Watchdog seal contract

A watchdog only reports PASS by writing `<role>-REPORT.json` **atomically**
(temp file, `fsync`, rename) with `sealed: true`, `ok: true`, `exit_code: 0` and
`last_hash` equal to the `record_hash` of a final `sealed` ledger record. The
ledger chain must verify end to end:

```
record_hash = sha256(`${prev_hash}\n${JSON.stringify(record)}`)
```

The first record of a ledger has `prev_hash: "GENESIS"`. A second run against
the same root resumes the existing chain and links to the previous seal; it
refuses to append to a chain that no longer verifies
(`WATCHDOG_LEDGER_CHAIN_INVALID`).

**Acceptance is not "the ledger file has bytes".** Use the exported verifier:

```js
import { verifyWatchdogSeal } from './tools/full-system/watchdog.mjs';

const verdict = verifyWatchdogSeal({ root: campaignRoot, role: 'liveness' });
if (!verdict.pass) {
  // NOT PASS — verdict.reasons carries named codes, e.g.
  // REPORT_MISSING, LEDGER_NOT_SEALED, SEAL_OVERDUE, WATCHDOG_FAILED,
  // EXIT_CODE_NONZERO, REPORT_HASH_MISMATCH, LEDGER_CHAIN_INVALID.
}
```

A watchdog killed before it seals (SIGKILL, power loss, `taskkill /F`) leaves a
ledger with records and **no** sealed report. A watchdog that hangs while alive
never reaches its seal inside `required_seal_by_utc` (recorded in the
`watchdog-start` record). Both cases are NOT PASS; neither is detectable from
"the ledger exists and is non-empty".

### Observation policy

| Situation | Ledger record | Report effect |
|---|---|---|
| endpoint answers | `health` / `wire` / `artifacts` / `defect-signal`, then `heartbeat` | observation counted, consecutive-failure counter reset |
| one request times out | `warn observation-timeout` (`error_code: WATCHDOG_OBSERVATION_TIMEOUT`) | warning only; run continues |
| one connection is refused | `warn observation-error` (`error_code: ECONNREFUSED`) | warning only; run continues |
| `WATCHDOG_MAX_CONSECUTIVE_FAILURES` reached | `error watchdog-failure`, then `sealed` | `ok: false`, `failure_code: WATCHDOG_CONSECUTIVE_OBSERVATION_FAILURES`, exit `1` |
| heartbeat cadence exceeds `WATCHDOG_GAP_MS` | `warn heartbeat-gap` (`gap_ms`, `threshold_ms`) | counted in `gaps` / `max_gap_ms`; not a failure by itself |

Every observation uses its **own socket** (`keepAlive: false`). A pooled
connection left half-dead by an earlier abort must never make a later
observation time out without having reached the product: a timeout record has to
mean "this request produced no response", not "the pool was poisoned".

### Environment

| Variable | Default | Meaning |
|---|---|---|
| `WATCHDOG_ROOT` | required | campaign root; ledger goes to `<root>/ledgers/<role>.jsonl` |
| `WATCHDOG_ROLE` | `liveness` | `liveness`, `wire`, `artifact`, `defect` (pattern `^[a-z][a-z0-9-]{0,31}$`) |
| `WATCHDOG_URL` | unset | base URL; unset means the role records no observation |
| `WATCHDOG_LAUNCH`, `WATCHDOG_TOKEN` | empty | request headers; never written to the ledger or report |
| `WATCHDOG_INTERVAL_MS` | `1000` | sleep between observations |
| `WATCHDOG_DURATION_MS` | `60000` | observation window |
| `WATCHDOG_TIMEOUT_MS` | `2000` | per-request budget |
| `WATCHDOG_MAX_CONSECUTIVE_FAILURES` | `5` | bound before the run is marked failed |
| `WATCHDOG_GAP_MS` | `max(4 x interval, 1200)` | heartbeat gap threshold |
| `WATCHDOG_SEAL_GRACE_MS` | `5000` | slack added to `required_seal_by_utc` |
| `WATCHDOG_WORKSPACE`, `WATCHDOG_DATA` | `<root>/workspace`, `<root>/data` | artifact role scan roots |

Config is validated at the boundary; an invalid role or number exits `2`
without touching the ledger. Roles are fixed to the four documented names —
`WATCHDOG_ROLE` is used in a file name, so an unrecognised value is rejected
rather than joined into a path.

### Tests

```powershell
node --test tools\full-system\test\watchdog.test.mjs
```

The suite is black-box over the real CLI against a real loopback endpoint and
covers: chain recomputation and linkage, body-tamper and record-deletion
detection, forged-report rejection, transient timeout → warning → PASS, refused
connection classified as `observation-error`, bounded consecutive failures →
`ok:false` seal, heartbeat gap detection, a SIGKILLed watchdog (ledger non-empty
but NOT PASS), seal-deadline expiry, and chain resumption across two runs.

## Known harness blind spot (unfixed, outside this tool)

`orchestrator.mjs` computes `WATCHDOG/01` as:

```js
const watchdogOk = Object.values(ledgerReports).every((entry) => entry.exists && entry.bytes > 0);
```

That predicate passes any watchdog that wrote at least one byte — including one
that was killed, one that sealed `ok:false`, and one whose report is missing or
unverifiable. Applied to the committed evidence in
`docs/evidence/live-campaign-20261117-150903`, the orchestrator passes all four
roles while `verifyWatchdogSeal` rejects all four:

| role | orchestrator `WATCHDOG/01` | `verifyWatchdogSeal` | reasons |
|---|---|---|---|
| liveness | pass | NOT PASS | `LEDGER_NOT_SEALED, REPORT_NOT_SEALED, WATCHDOG_FAILED, EXIT_CODE_NONZERO` |
| wire | pass | NOT PASS | `REPORT_NOT_SEALED, EXIT_CODE_NONZERO, REPORT_HASH_MISMATCH` |
| artifact | pass | NOT PASS | `REPORT_NOT_SEALED, EXIT_CODE_NONZERO, REPORT_HASH_MISMATCH` |
| defect | pass | NOT PASS | `REPORT_NOT_SEALED, EXIT_CODE_NONZERO, REPORT_HASH_MISMATCH` |

The pre-fix reports were also unverifiable by construction: the report was
written before the `sealed` record was appended, so `last_hash` committed to the
seal's parent, never to the final ledger state. Fixing `WATCHDOG/01` requires an
edit to `orchestrator.mjs`, which the watchdog change does not own.

## Current limits

The executable slice does not yet run the live provider lane, Playwright UI
battery, soak/fuzz/migration batteries, or the 40-node scale lane. These are
listed as `NOT_IMPLEMENTED` in the report. The MCP package check is expected to
fail while the packaged 3-tool server and the documented 7-tool server disagree.

The watchdog transport is an explicit HTTP client with one socket per request.
Unlike `fetch`, it does not follow redirects: a `3xx` is recorded as an
HTTP status, not followed. None of the four observation endpoints redirect, so
this is declared rather than exercised against the live server.
