# HiveMind Full-System Live Verification — Corrected Plan

**Status:** corrected after live execution. This document supersedes any claim that the scripted-provider harness is the product acceptance test.

## Acceptance rule

The product is accepted only by driving the **installed `HiveMind.exe` window with the real DeepSeek provider and real UI approvals**. The local scripted-provider harness is retained only as a fast regression/engine harness. It is **not** a live-app PASS and must never be labeled production-ready.

## Live campaign evidence (2026-11-17)

| Run | Live behavior | Artifact | Policy | Result |
|---|---|---|---|---|
| `01a1eb57-1f04-7308-9360-6dfe523ef198` | Prompt asked for `live-proof.md`; planner chose `content-writer`; run completed | **none** | trusted | **FAIL — false success**: content-writer has no `file_write` |
| `01a1eb57-e6d6-767b-a072-7c7b46e0dc69` | second prompt arrived truncated (UI automation issue, not model) | none | trusted | invalid input; rerun |
| `01a1eb58-ffb0-75cc-b559-45e27be7e254` | real model created `live-proof.md` via agent | 11 bytes, checkpoint seq 1 | trusted | PASS for artifact; no human approval because trusted auto-grants |
| `01a1eb59-a5ea-72f8-91b1-19e794f65e49` | real model created `live-approval.md` | 16 bytes | trusted | PASS artifact; auto-granted |
| `01a1eb5a-3b83-7583-8328-95c5a2128263` | real model created `live-approval-2.md`; two `shell_exec` cards shown; operator clicked **Approve** twice in the live window; run resumed to completed | 16 bytes, checkpoint seq 1 | trusted (UI local policy) | PASS approval UI |
| `01a1eb7a-1539-753a-bc95-ee9a4a3e07d7` | after the clean `e47d35a` install, real model created `live-content-writer-proof.md`; monitor shows `FILE_WRITE OK`; 4/4 tasks completed | 22 bytes `CONTENT WRITER LIVE OK`, checkpoint seq 1 | trusted (UI local policy) | **PASS live file creation** |

Live UI screenshot: `docs/evidence/screenshots/live-approval-completed.png` and `live-content-writer-fixed.png` (completed runs, 4/4, 100% green, workers, artifacts).
Approval screenshot set: `live-approval-card.png`, `live-submitted.png`, `live-typed.png`.

## Live defects found and status

1. **P1 — false completion without deliverable (persona fix in `e47d35a`; systemic gate still open).** The planner selected `content-writer` for a file-creation task, but that persona had no `file_write`; the run ended `completed` with no artifact. Fix: added `file_write` to the content-writer persona; its persona/prompt tests pass. The clean live run produced the file and checkpoint, but used the generic `agent` role, so content-writer live use is still unverified.
2. **UI policy state, not a server bug.** The live window keeps its own local policy (`trusted`) and sends it with `SUBMIT_PROMPT`; changing the session through HTTP does not change that local control. The run row correctly stored `trusted`, and `shell_exec` still asked because it is outside the workspace path. Test procedure: set policy through the visible UI control (or `localStorage`) and verify the next run row.
3. **P2 — shell execution refusals after approval.** The chat log showed `SHELL_EXEC REFUSED` for commands after the approval card; inspect tool results and shell policy rather than treating approval as execution success.
4. **P2 — liveness watchdog seal failure.** `liveness-REPORT.json` has `ok:false` with `TimeoutError` after 185 records; the final health poll must be bounded and sealed as a completed observation, not a failed watchdog. Watchdog code now catches observation timeouts; rerun still needed.
5. **P2 — UI checkpoint restore is disabled for a terminal run.** The live Data panel lists the checkpoint (`live-content-writer-proof.md`, seq 1) but says “This run is terminal (completed); its journal is read-only history.” The HTTP restore route works and was exercised in the auxiliary harness, but the live UI exposes no Restore button after completion. Decide whether restoring completed-run files is intended; if yes, enable it and test the confirm path in the live window.
6. **Harness gap — live UI specs.** `live-app.spec.ts` + `approval-flow.spec.ts` against the open app: **6 passed, 17 failed**; many failures are expected because the specs do not open the launcher URL with its launch token and some use fixture assumptions. Record as **test-harness mismatch**, not 17 product defects; run a launch-token-aware live suite next.

## Four live watchdogs

Watchdogs were spawned as four separate processes against the real app and real data/workspace:

- `W1 liveness` → `docs/evidence/live-campaign-20261117-150903/ledgers/liveness.jsonl`
- `W2 wire` → `wire.jsonl` (240 records, sealed, 0 gaps)
- `W3 artifact` → `artifact.jsonl` (116 KB)
- `W4 defect` → `defects.jsonl` (107 KB)

Requirements:
- each writes append-only, hash-chained JSONL with heartbeat and evidence;
- W4 deduplicates defects and records hypotheses/probes;
- supervisor seals and reconciles;
- a failed/timed-out watchdog is a HARNESS failure, never PASS;
- W1 must treat the final health timeout as a bounded observation, not an unhandled rejection.

## Corrected test plan

### Priority 0 — live app acceptance (required)
1. Launch `HiveMind.exe` once; assert one window and strict `/health` schema+boot_id.
2. Set the active session policy to `classic`; verify the **next run row** is classic before submitting.
3. Submit a file-deliverable prompt from the live composer using the UIA `set_value` path (not character typing; typing truncated two prompts).
4. Assert the planner selects a persona with `file_write`; assert the approval card appears; click **Approve** in the live window; assert run resumes and completes.
5. Assert artifact bytes, checkpoint entry, run detail, usage, logs, and UI state agree.
6. Repeat with a deliberate **Deny** and assert no artifact; repeat checkpoint restore; repeat session continuation.
7. Capture screenshots and watchdogs for every live run.

### Priority 1 — live defect fixes before any PASS
- content-writer/file-write planning mismatch;
- session-policy propagation;
- shell-exec approval vs execution truth;
- watchdog final timeout.

### Priority 2 — deterministic harness (regression only)
The scripted-provider harness (`tools/full-system`) may run after live acceptance as a fast regression. It must be labeled `AUXILIARY`, and its checks cannot make a live-app PASS. Its current 21 core PASS results are engine evidence, not UI/live evidence.

### Priority 3 — remaining batteries
UI approval/snapshot/restore, crash/restart, path/security, provider failure, caps, scale, clean install, accessibility, fuzz/soak. Each failure must produce a defect bundle (screenshot, DOM, console, WS/HTTP, DB, source suspect, repro).

## Stop conditions

- No live PASS while any P0/P1 live defect is open.
- No “completed” run may be accepted without its declared artifact.
- No run policy may differ from the session policy without an explicit named reason.
- No watchdog failure may be reported as PASS.
- A truncated prompt is invalid input, not a model result; discard and resubmit.
