# HiveMind 1.2.0-dev portable release

Built from `4fb3228` on
`integration/capability-absorb`, 2026-11-13, schema **32**.

- Archive: `HiveMind-1.2.0-dev-win64.zip`
- SHA-256: `C4CDFCCAAE86DCE07F7E1A6B0A54C0F5103D99575BE65EEC5EEC2A48D189F335`
  (also in `HiveMind-1.2.0-dev-win64.zip.sha256`)
- Includes: `HiveMind.exe` launcher, `hiverouter.exe`, web UI, packaged CLI,
  MCP stdio server, installer/uninstaller scripts, `BUILD.json`, `CHECKSUMS.txt`.

## Run portably

1. Extract the zip to a folder.
2. Run `HiveMind.exe`. It starts one local server on 8099-8199, waits for
   `/health`, and opens the app window.
3. Without a provider key the app runs in a truthful degraded mode:
   `E_KEY_MISSING` refusals; reads, sessions, jobs, evals and UI still work.

## Install per user

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\install-desktop.ps1 -SkipProvider
```

Shortcuts are created for the Desktop and Start Menu. Data lives under
`%LOCALAPPDATA%\HiveMind`; the app under `%LOCALAPPDATA%\Programs\HiveMind\app`.
To remove: `uninstall-desktop.ps1` (use `-PurgeData` only if you intend to delete
user data).

## Provider key

Use `set-provider-key.ps1` (DPAPI, per user) or set `DEEPSEEK_API_KEY` /
`HIVEROUTER_API_KEY` for the selected provider profile before starting.

## CLI / MCP

```powershell
.\hivemind.cmd status --json
.\hivemind.cmd sessions --json
.\hivemind.cmd session-create --name work --policy plan --json
.\hivemind.cmd job-start --session <uuid> --program git.exe --args=[] --json
```

`hivemind-mcp.mjs` is a read-only stdio MCP server (`hivemind_status`,
`hivemind_runs`, `hivemind_jobs`).

## Verification at this build

- Full backend workspace gate: PASS
- Web typecheck/unit/build: PASS
- Final live walkthrough: 11/11, schema 32
- Full offline battery: web/docs/QA/health PASS (backend runs separately)
- Schema v32; `CHECKSUMS.txt` excludes only itself.

Known residuals are documented in `docs/audit/ROUND-127-FINAL-AUDIT.md`: no PTY,
no verified keyed coding run in this environment, and a plain-text reply after a
mutation can still complete without a terminal tool call.
