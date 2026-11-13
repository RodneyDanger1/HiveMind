# Session / continuation model

Status: implemented in `backend/crates/hiverouter-core/src/session_model.rs`,
verified by `backend/crates/hiverouter-core/tests/session_model_contract.rs`.
Production registration and transport wiring are **not** applied yet; this
document records the exact changes still required.

## 1. What a session is here

A session is a persisted grouping of runs plus the work attached to it:

| Fact | Source of truth (existing schema) |
| --- | --- |
| Session identity/project/archive | `sessions` (migration v25, `archived_at` v32) |
| Which runs belong to the session, in order | `runs.session_id`, ordered by `(created_at, rowid)` |
| Last completed run | `runs` where `status = 'completed'`, latest `updated_at` |
| Pending work | active `runs`, open `todos` (v25), non-terminal `tasks` of the session's runs |
| Continuation context | `chat_messages` (v2) and `task_results` (v5) of the session's runs |

No table, column, cache or materialized summary is added. Every value is
derived when asked, so the model cannot drift from the store. The only write in
the module is `attach_run`, which fills the existing nullable
`runs.session_id` column.

## 2. Invariants

1. **Order is creation order.** `run_ids` is ordered by
   `(runs.created_at ASC, runs.rowid ASC)`. `rowid` is the deterministic
   tie-break for timestamps that share a second. Attach order never affects
   order.
2. **A run belongs to at most one session.** `attach_run` refuses a run bound
   to another session and refuses a project mismatch. Re-attaching to the same
   session is idempotent.
3. **Context is persisted-only.** Every `ContinuationContext::items[]` entry is
   a stored `chat_messages` or `task_results` row (or a prefix of one when
   truncated). The model never invents, summarizes or rewrites content; there
   is no model call in this module.
4. **Bounded.** Run ledger ≤ 500 entries; active runs ≤ 20; open todos ≤ 10
   (todo text ≤ 200 bytes); context text ≤ caller budget (≤ 256 KiB). Every
   trim is reported (`runs_truncated`, `active_runs_truncated`,
   `open_todos_truncated`, per-item `truncated`, `omitted_items`).
5. **Provenance is explicit.** Each context item names its source table
   (`source`), owning `run_id`, source row identity (`record_id`), role (for
   messages), and timestamp.
6. **No mid-run resume.** The engine has no execution-level resume (see the
   existing `session_resume` handler note, `http.rs`). "Continue" always means
   *create a new run in the same session, seeded from persisted history*.

## 3. Public API (Rust)

All functions take `&rusqlite::Connection`; the caller supplies it (the server
uses `Database::with_connection`, tests use a temp-file database with the real
migrations). Signatures:

```rust
pub fn attach_run(&Connection, session_id: &str, run_id: &str)
    -> Result<AttachOutcome, SessionModelError>;

pub fn session_run_model(&Connection, session_id: &str)
    -> Result<SessionRunModel, SessionModelError>;

pub fn continuation_context(&Connection, session_id: &str, budget_bytes: usize)
    -> Result<ContinuationContext, SessionModelError>;

pub fn assess_continuation(&Connection, session_id: &str, run_id: &str)
    -> Result<ContinueAssessment, SessionModelError>;

pub fn latest_resumable_run(&Connection, session_id: &str)
    -> Result<Option<String>, SessionModelError>;
```

### `SessionRunModel`

```jsonc
{
  "session_id": "...",
  "project_id": "...",
  "archived": false,
  "run_ids": ["run-1", "run-2"],       // ascending session order, capped
  "runs": [{ "run_id", "ordinal", "status", "prompt", "error_code", "created_at" }],
  "run_count": 2,                       // true total
  "runs_truncated": false,
  "last_completed_run": { "run_id", "completed_at" } | null,
  "pending_work": {
    "active_runs": [{ "run_id", "status", "created_at" }],
    "active_run_count": 0,
    "active_runs_truncated": false,
    "open_todos": [{ "todo_id", "seq", "text", "truncated", "created_at" }],
    "open_todo_count": 0,
    "open_todos_truncated": false,
    "unfinished_task_count": 0
  }
}
```

`PendingWork::is_empty()` is true when there are no active runs, no open todos
and no non-terminal tasks.

### `ContinuationContext`

```jsonc
{
  "session_id": "...",
  "budget_bytes": 8192,
  "used_bytes": 79,
  "original_bytes": 79,
  "items": [{
    "source": "chat_message" | "task_result",
    "run_id": "...",
    "record_id": "...",       // message id, or task id for task results
    "role": "user" | null,
    "created_at": "...",
    "text": "...",            // persisted text or its prefix
    "original_bytes": 18,
    "truncated": false
  }],
  "total_items": 4,
  "omitted_items": 0,
  "truncated": false,
  "source_run_ids": ["run-1", "run-2"]
}
```

Selection is newest-first until the text budget is exhausted; items are
returned oldest-first. Within a run, items are ordered by `created_at`; a
timestamp tie puts chat messages before task results, then `record_id` breaks
the tie. When the newest item alone exceeds the budget, its prefix is kept and
flagged `truncated`, so the budget is always honored. The budget counts stored
text bytes only; provenance fields are metadata and add no stored payload.
Budgets outside `1..=MAX_CONTINUATION_BUDGET_BYTES` (256 KiB) are rejected,
never clamped.

### `ContinueAssessment`

```jsonc
{
  "session_id": "...",
  "run_id": "...",
  "status": "failed",
  "decision": "continue" | "start_new_run" | "refuse",
  "reason": "failed_run_continuable",
  "detail": "the run failed; its persisted messages ..."
}
```

| Persisted run state | Decision | Reason code |
| --- | --- | --- |
| `failed` | `continue` | `failed_run_continuable` |
| `interrupted` | `continue` | `interrupted_run_continuable` |
| `queued`, `planning`, `thinking`, `running`, `streaming` | `refuse` | `E_RUN_ACTIVE` |
| `awaiting_approval` | `refuse` | `E_APPROVAL_PENDING` |
| `completed` | `start_new_run` | `run_completed` |
| `cancelled` | `start_new_run` | `run_cancelled` |
| `killed` | `start_new_run` | `run_killed` |
| client-only states | `refuse` | `E_RUN_STATE` |
| any state in an archived session | `refuse` | `E_SESSION_ARCHIVED` |

**"Paused" in v1.** There is no persisted `paused` state (grep of
`src/*.rs` returns no `Paused`). Two persistence-level pauses exist:

* `awaiting_approval` — the run is parked on an approval decision. The approval
  waiter is in-memory, so continuing or restarting the run here would duplicate
  a run whose approval may still resolve in-process. Refused until the
  approval is resolved.
* `interrupted` — recovery marks runs active at crash time as `interrupted`
  (`recovery.rs`), and boot recovery settles their tasks. There is no live
  execution left, so a continuation run may be seeded from the history.

### Errors (`SessionModelError`)

| Variant | Named code | Meaning |
| --- | --- | --- |
| `SessionNotFound` | `E_SESSION_NOT_FOUND` | unknown session id |
| `SessionArchived` | `E_SESSION_ARCHIVED` | archived session cannot accept/attach runs |
| `RunNotFound` | `E_RUN_NOT_FOUND` | unknown run id |
| `RunNotInSession` | `E_RUN_NOT_IN_SESSION` | run exists but is not bound to the session |
| `RunInOtherSession` | `E_RUN_IN_OTHER_SESSION` | attach target is bound elsewhere |
| `ProjectMismatch` | `E_SESSION_PROJECT_MISMATCH` | run and session projects differ |
| `InvalidBudget` | `E_CONTEXT_BUDGET_INVALID` | budget outside `1..=262144` |
| `UnknownRunState` | `E_RUN_STATE_UNKNOWN` | stored status not in `AgentState` |
| `AttachContended` | `E_ATTACH_CONTENDED` | guarded attach lost a race |
| `Sqlite` | – | storage failure |

Identity problems are errors; state problems are assessments. That keeps
HTTP mapping unambiguous (404 vs 409).

## 4. Required wiring

### 4.1 Register the module (`lib.rs`)

Add one line to `backend/crates/hiverouter-core/src/lib.rs`:

```rust
pub mod session_model;
```

and, if the server should use the short path, extend the existing re-export
block with:

```rust
pub use session_model::{
    assess_continuation, attach_run, continuation_context, latest_resumable_run,
    session_run_model, AttachOutcome, ContinueAssessment, ContinueDecision, ContinueReason,
    ContinuationContext, ContextItem, ContextSource, SessionModelError, SessionRunModel,
};
```

Until that line lands, the file is not part of the library. The integration
test compiles it directly with `#[path = "../src/session_model.rs"]` so the
tested code is exactly the file that will be registered — there is no copy or
fixture implementation.

### 4.2 HTTP `POST /api/sessions/:id/continue`

**Route.** In `http.rs::build_router`, next to the existing session routes
(`http.rs:694-716`):

```rust
.route(
    "/api/sessions/{session_id}/continue",
    post(session_continue),
)
```

**Handler.** Place it next to `session_resume` (`http.rs:1145`). It reuses the
existing engine submission path (`state.engine`, `http.rs:91`;
`RunEngine::submit_prompt_scoped`, `execution.rs:1859`), so the run is created,
bound to the session, scheduled and announced exactly like a
`SUBMIT_PROMPT` run — no second execution path.

```rust
async fn session_continue(
    State(state): State<Arc<AppState>>,
    Path(session_id): Path<String>,
    headers: HeaderMap,
    Json(body): Json<serde_json::Value>,
) -> Response {
    // Body: { run_id?: string, prompt: string, max_context_bytes?: number }
    let Some(prompt) = body.get("prompt").and_then(serde_json::Value::as_str) else {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
    };
    if prompt.trim().is_empty() {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::PromptEmpty);
    }
    let budget = body
        .get("max_context_bytes")
        .and_then(serde_json::Value::as_u64)
        .map_or(session_model::DEFAULT_CONTINUATION_BUDGET_BYTES, |value| {
            usize::try_from(value).unwrap_or(usize::MAX)
        });

    let store = crate::store::RunStore::new(state.db.clone());
    let session = match store.session(&session_id) {
        Ok(session) => session,
        Err(refusal) => return refusal_response(StatusCode::NOT_FOUND, &refusal),
    };

    // Resolve the run: explicit body.run_id, else the newest failed/interrupted run.
    let selected = match body.get("run_id").and_then(serde_json::Value::as_str) {
        Some(run_id) => Some(run_id.to_owned()),
        None => match state
            .db
            .with_connection(|conn| Ok(session_model::latest_resumable_run(conn, &session_id)))
        {
            Ok(inner) => match inner {
                Ok(candidate) => candidate,
                Err(error) => return session_model_error_response(error),
            },
            Err(_) => return refusal_response(StatusCode::SERVICE_UNAVAILABLE, &Refusal::InternalError),
        },
    };
    let Some(run_id) = selected else {
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({
                "error": "E_NO_RESUMABLE_RUN",
                "detail": "the session has no failed or interrupted run to continue",
            })),
        )
            .into_response();
    };

    let assessment = match state.db.with_connection(|conn| {
        Ok(session_model::assess_continuation(conn, &session_id, &run_id))
    }) {
        Ok(inner) => match inner {
            Ok(assessment) => assessment,
            Err(error) => return session_model_error_response(error),
        },
        Err(_) => return refusal_response(StatusCode::SERVICE_UNAVAILABLE, &Refusal::InternalError),
    };
    if !assessment.allows_continuation() {
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({
                "error": assessment.reason_code(),
                "detail": assessment.detail,
                "run_id": assessment.run_id,
                "status": assessment.status.as_str(),
            })),
        )
            .into_response();
    }

    let context = match state.db.with_connection(|conn| {
        Ok(session_model::continuation_context(conn, &session_id, budget))
    }) {
        Ok(inner) => match inner {
            Ok(context) => context,
            Err(error) => return session_model_error_response(error),
        },
        Err(_) => return refusal_response(StatusCode::SERVICE_UNAVAILABLE, &Refusal::InternalError),
    };

    // Compose the continuation prompt: the user's instruction first, then the
    // persisted context with per-item provenance. No stored text is rewritten.
    let composed = compose_continuation_prompt(prompt, &context);
    if composed.chars().count() > hiverouter_contract::command::MAX_PROMPT_CHARS {
        return (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(serde_json::json!({
                "error": "E_CONTINUATION_PROMPT_TOO_LARGE",
                "detail": "reduce max_context_bytes or shorten the prompt",
                "used_bytes": context.used_bytes,
            })),
        )
            .into_response();
    }

    // Stable correlation: honor an Idempotency-Key header so a retried
    // continuation cannot create two runs.
    let correlation = headers
        .get("idempotency-key")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| Uuid::parse_str(value).ok())
        .unwrap_or_else(Uuid::now_v7);
    let run = match state.engine.submit_prompt_scoped(
        &composed,
        &session.project_id,
        Some(&session_id),
        Some(session.permission_policy),
        correlation,
    ) {
        Ok(run) => run,
        Err(refusal) => return refusal_response(StatusCode::CONFLICT, &refusal),
    };

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "session_id": session_id,
            "run_id": run.id.to_string(),
            "decision": assessment.decision,
            "reason": assessment.reason,
            "reason_code": assessment.reason_code(),
            "continued_run_id": assessment.run_id,
            "context": {
                "budget_bytes": context.budget_bytes,
                "used_bytes": context.used_bytes,
                "original_bytes": context.original_bytes,
                "item_count": context.items.len(),
                "total_items": context.total_items,
                "omitted_items": context.omitted_items,
                "truncated": context.truncated,
                "source_run_ids": context.source_run_ids,
            },
        })),
    )
        .into_response()
}
```

`compose_continuation_prompt` is a small helper in `http.rs`:

```rust
fn compose_continuation_prompt(instruction: &str, context: &session_model::ContinuationContext) -> String {
    let mut prompt = String::with_capacity(instruction.len() + context.used_bytes + 256);
    prompt.push_str(instruction);
    prompt.push_str("\n\n--- persisted session context (oldest first) ---\n");
    for item in &context.items {
        prompt.push_str(&format!(
            "[{} run={} record={}{} at={}]\n",
            item.source.as_str(),
            item.run_id,
            item.record_id,
            item.role.as_deref().map_or(String::new(), |role| format!(" role={role}")),
            item.created_at,
        ));
        prompt.push_str(&item.text);
        prompt.push('\n');
    }
    prompt
}
```

Error mapping helper:

```rust
fn session_model_error_response(error: session_model::SessionModelError) -> Response {
    use session_model::SessionModelError as E;
    let (status, code) = match &error {
        E::SessionNotFound(_) => (StatusCode::NOT_FOUND, "E_SESSION_NOT_FOUND"),
        E::RunNotFound(_) => (StatusCode::NOT_FOUND, "E_RUN_NOT_FOUND"),
        E::SessionArchived(_) => (StatusCode::CONFLICT, "E_SESSION_ARCHIVED"),
        E::RunNotInSession(_, _) => (StatusCode::CONFLICT, "E_RUN_NOT_IN_SESSION"),
        E::ProjectMismatch { .. } => (StatusCode::CONFLICT, "E_SESSION_PROJECT_MISMATCH"),
        E::InvalidBudget(_) => (StatusCode::BAD_REQUEST, "E_CONTEXT_BUDGET_INVALID"),
        E::UnknownRunState { .. } => (StatusCode::INTERNAL_SERVER_ERROR, "E_RUN_STATE_UNKNOWN"),
        E::AttachContended(_) | E::Sqlite(_) => {
            (StatusCode::SERVICE_UNAVAILABLE, "E_INTERNAL_ERROR")
        }
    };
    (status, Json(serde_json::json!({ "error": code, "detail": error.to_string() }))).into_response()
}
```

Status contract:

| Case | Status |
| --- | --- |
| continuation run created | 200 |
| malformed body / bad budget | 400 |
| unknown session or run | 404 |
| refused (active/approval/archived), no resumable run, engine refusal | 409 |
| composed prompt too large | 422 |
| storage failure | 503 |

Notes for the implementer:

* `with_connection` returns `Result<T, DbError>` and its closure returns
  `rusqlite::Result<T>`, hence the `Ok(SessionResult<T>)` nesting above; do not
  unwrap storage errors into 500 silently — map `DbError` to 503.
* Do **not** call `attach_run` here. `submit_prompt_scoped` binds the new run
  to `Some(&session_id)` itself. `attach_run` exists for backfilling runs that
  were created before the session existed.
* Do not add a migration. The handler uses only v25/v32 columns.
* `GET /api/sessions/{id}/resume` (`http.rs:1145`) keeps its current meaning
  (client reconnect hint). `/continue` is the mutating continuation endpoint.

### 4.3 CLI `run --continue`

`main.rs` currently boots the server and has no subcommand parser
(`Config::from_args`, `main.rs:49`). Add a pre-boot branch in `run()`, before
`Config::from_args`:

```rust
if args.first().map(String::as_str) == Some("run") {
    return run_continue_command(&args[1..]);
}
```

Planned syntax:

```
hiverouter run --continue <SESSION_ID> [--run <RUN_ID>]
                [--prompt <TEXT>|--prompt -] [--budget-bytes <N>]
                [--bind <ADDR>]
```

Exact wiring (`run_continue_command` in `main.rs`):

1. Parse flags; `--prompt -` reads stdin, otherwise `--prompt` is required.
2. Resolve the server address: `--bind` else `HIVEROUTER_BIND` else the same
   default `Config::from_args` uses. The command is a **client** of the running
   server; it does not boot a second engine (booting a second `RunEngine` would
   duplicate recovery, scheduling and event publication).
3. `POST http://<bind>/api/sessions/<SESSION_ID>/continue` with
   `reqwest::Client`, JSON body
   `{ "run_id": <opt>, "prompt": <text>, "max_context_bytes": <opt> }`,
   header `idempotency-key: <uuid v7>` generated once per invocation.
4. On 200: print `run_id`, `decision`, `reason_code`, context byte counts to
   stdout, exit 0.
5. On 409: print the returned `error`/`detail` to stderr, exit 2 (a named
   refusal, matching the server's refusal convention).
6. On 4xx/5xx/transport error: print status and body to stderr, exit 1.

This keeps one execution path and one recovery owner. A direct-DB CLI mode is
deliberately not specified: it would need to construct `RunEngine`, bus,
graph, snapshots and provider config, i.e. a second server boot.

### 4.4 Client/UI

Out of scope for this task. A UI continuation action should call the new
endpoint and surface `reason_code` verbatim; the response contains no rendered
prompt text, only provenance and byte accounting.

## 5. Verification

Command (from `backend/`):

```powershell
& "$env:USERPROFILE\.cargo\bin\cargo.exe" test -p hiverouter-core --test session_model_contract
```

The test target compiles the production source file via `#[path]`, opens a
temp-file SQLite database, applies the production migration list
(`Database::migrate`, schema v32), and exercises:

1. `attach_records_run_ids_in_order_and_reports_last_completed_and_pending_work`
2. `attach_is_idempotent_and_refuses_cross_project_foreign_and_archived_sessions`
3. `continuation_context_is_byte_bounded_and_cites_persisted_provenance`
4. `continuation_context_excludes_other_sessions_and_rejects_invalid_budgets`
5. `continuation_context_truncates_on_utf8_boundaries`
6. `continuation_decision_accepts_failed_and_interrupted_and_refuses_live_work`
7. `latest_resumable_run_prefers_the_newest_failed_or_interrupted_run`
8. `session_model_caps_the_run_ledger_and_pending_summary`

Observed result (2026-11-13) from the command above on a temp-file database
migrated to schema v32:

```
running 8 tests
test attach_is_idempotent_and_refuses_cross_project_foreign_and_archived_sessions ... ok
test attach_records_run_ids_in_order_and_reports_last_completed_and_pending_work ... ok
test continuation_context_excludes_other_sessions_and_rejects_invalid_budgets ... ok
test continuation_context_is_byte_bounded_and_cites_persisted_provenance ... ok
test continuation_context_truncates_on_utf8_boundaries ... ok
test continuation_decision_accepts_failed_and_interrupted_and_refuses_live_work ... ok
test latest_resumable_run_prefers_the_newest_failed_or_interrupted_run ... ok
test session_model_caps_the_run_ledger_and_pending_summary ... ok

test result: ok. 8 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.71s
```

The test run is the compile gate for the module until `lib.rs` is wired.
Storage errors and invalid inputs were exercised in the error-path assertions
(unknown session/run, cross-project attach, foreign-session attach, archived
session, zero/oversized budget, non-UTF-8-boundary budget cuts).

## 6. Non-goals (explicit)

* No mid-run execution resume; the engine has none and this model does not add
  one.
* No schema/migration change, no new table, no cached session summary.
* No LLM summarization or prompt rewriting inside the model; context items are
  byte-for-byte persisted text (or UTF-8-boundary prefixes).
* No cross-session/cross-project context leakage: every query joins through
  `runs.session_id`.
* No automatic continuation after a failure; a caller must request it.
* No UI changes and no commit in this task.
