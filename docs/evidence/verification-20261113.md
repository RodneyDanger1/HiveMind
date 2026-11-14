# Verification — 2026-11-13

Build under test: installed `aef810e` (source `aef810e`; latest docs `4c8963f`).

## Automated

- `cargo test --workspace` — PASS (all workspace test binaries, exit 0)
- `cargo test -p hiverouter-tools --lib` — 183 passed
- `cargo test -p hiverouter-core --test tool_allowlist_census` — 2 passed
- Web `npx tsc --noEmit` — PASS
- Web `npx vitest run` — 49 files / 441 tests passed
- `tools/hivemind-mcp`: `node --test test/smoke.test.mjs` — PASS

## Live capability tests

- Workspace read + `harness_diagnose` — jail_available true, real file content.
- Three independent deliverables — 3 SuperAgents + 3 workers, distinct per-seat briefs/results.
- Missing-file read — exact `E_PATH_NOT_FOUND`, no fabrication.
- Cross-run session memory — second run retrieved `ORCHID-42`.
- `repo_search("Agent", limit 5)` — 5 symbols with paths/lines.
- Approval end-to-end — two `shell_exec` approvals queued, approved, run completed.
- Hide/Show Chat — both controls work; canvas expands when chat hides.
- Splash — version `v1.2.0-dev` visible; screenshot in `docs/evidence/screenshots/hivemind-splash.png`.

## Known limits

- Checkpoint restore is not exposed as an agent tool.
- Chat bus, delegation protocol, semantic recall, HiveWire, and the session-continue route are library-only.
- Approval requests are per side effect; parallel workers can queue several identical-looking requests.
- Canvas hive artwork still clips at the right edge on a wide window.