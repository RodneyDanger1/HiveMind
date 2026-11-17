# Checkpoint restore browser harness

Drives the real `DiffReviewPanel` / `DataPanel` in Chrome (installed channel, no
browser download) to assert the completed-run restore states that the live P2
defect hid.

## Run

From `web/`:

```powershell
# 1. start the dev server (the harness page lives under tests/)
node node_modules/vite/bin/vite.js --port 5173 --strictPort

# 2. in a second shell
node tests/restoreHarness/drive.mjs
```

Expected: `ALL RESTORE HARNESS CHECKS PASSED` (exit 0).

## What it asserts

- completed run (`runStatus: "completed"`) renders `Restore`;
- first click changes it to `Confirm restore`;
- confirm emits the real `POST /api/runs/{run_id}/checkpoints/{seq}/restore`
  through `restoreCheckpoint`, with `Authorization: Bearer <token>` from
  `GET /api/session`;
- the success receipt renders `path`, `restored` and `undone_seqs`;
- a `409 E_CHECKPOINT_DRIFT` refusal renders as an alert, emits no receipt, and
  does not refresh the host preview;
- the Data panel issues `FILE_TREE_LIST` and re-reads the restored path with
  `FILE_READ` after a confirmed restore.

## Boundaries

- `window.fetch` and the `UiApi` socket are test doubles; the components, the
  restore helper and the rendered states are production code.
- `FILE_READ` is asserted as a request: no `FILE_CONTENT` is fabricated, so the
  harness does not claim the preview text changed. That last hop needs the live
  server (see the manual step in the seat report).
