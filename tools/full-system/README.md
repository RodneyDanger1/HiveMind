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

## Current limits

The executable slice does not yet run the live provider lane, Playwright UI
battery, soak/fuzz/migration batteries, or the 40-node scale lane. These are
listed as `NOT_IMPLEMENTED` in the report. The MCP package check is expected to
fail while the packaged 3-tool server and the documented 7-tool server disagree.
