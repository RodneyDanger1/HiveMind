# HiveMind final state — 2026-11-14

## Git
- HEAD: `19101ad` (pushed to `hive/master`)
- Tag: `v1.2.1-dev-20261113`
- Working tree: clean

## Installed build
- Installed commit: `a88d4a0`
- Install path: `%LOCALAPPDATA%\Programs\HiveMind`
- UI: centered scaled hive, dock-aware graph viewport, Data checkpoint restore panel, versioned splash

## Release
- File: `release/HiveMind-1.2.0-dev-win64.zip`
- SHA-256: `da88bb94f41f1e2685e2e823a8fac6e3b3a0af958e3145e173e3eeb627330fd7`
- `.sha256` file matches the zip

## Runtime
- `/health`: `ok`, schema 32
- Running server PID at audit: 44904
- HiveMind windows: 1

## Gates
- Backend: `cargo test --workspace` PASS (last full run after tool wiring)
- Web: 442 tests PASS, `tsc` clean
- Live: workspace read, harness_diagnose, 3-branch delegation with distinct briefs, missing-file refusal, cross-run memory (`ORCHID-42`), approval approve/deny, repo_search, memory_recall, session continue via HTTP + MCP, Hide/Show Chat

## Known open
- Checkpoint restore is UI/HTTP only, not an agent tool.
- Chat bus, delegation protocol, and HiveWire are library-only.
- No OS sandbox, network remotes, embeddings/ANN, autonomous loop, or 40-node live proof.
- Hive art is now centered/scaled but remains a stylistic work-in-progress.