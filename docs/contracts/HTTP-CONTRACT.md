# HiveMind local HTTP + WebSocket contract

Status: observed from code on 2026-11-13.
Sources: `backend/crates/hiverouter-core/src/http.rs`,
`backend/crates/hiverouter-core/src/ws.rs`,
`backend/crates/hiverouter-contract/src/{envelope,command,event}.rs`,
`backend/crates/hiverouter-core/src/health.rs`,
`tools/hivemind-cli.mjs`.

This document describes the loopback surface that `tools/hivemind-mcp` calls. It
is a read-only description; no route in this document was changed. Where the
docs and code disagree, the code is authoritative and the mismatch is called
out explicitly.

## 1. Transport and security gates

- Bind: loopback only (`127.0.0.1:8099` by default; `--bind` accepts a loopback
  socket address).
- Base URL: `http://127.0.0.1:8099` (or `http://localhost:8099`).
- **Host gate** (`host_guard`, `http.rs:849`): the `Host` header must be the
  loopback HiveMind server. A foreign Host is rejected with
  `403 E_ORIGIN_REJECTED` and body
  `{"code","message","how_to_fix"}`.
- **Origin gate**: a request that carries an `Origin` header must match the
  server's origin allowlist; a request without `Origin` is accepted only from a
  loopback peer. UI requests carry an Origin; plain local clients may omit it.
- **Launcher token** (`launch.token`, env `HIVEROUTER_LAUNCH_TOKEN`): when the
  server was started with a launch token, every `GET /api/*` **except**
  `/api/session` requires it in the `x-hivemind-launch` header. `/api/session`
  also requires it. `/health` and `/diagnose` do not.
  Reading requires launch token even though it is a GET (`mutation_auth`,
  `http.rs:877`).
- **Session token** (per-boot): every **mutating** `/api/*` request (any method
  other than GET/HEAD/OPTIONS) requires
  `Authorization: Bearer <session.token>` or `x-hivemind-token: <token>`
  (`http.rs:877`). Read routes accept (and ignore) a Bearer token.
- `GET /api/session` is the **only** route that returns the per-boot token.
- Refusal bodies use the shape
  `{"code": "E_*", "message": "...", "how_to_fix": "...", "retryable": bool}`.
  The session/launch-token gates at `http.rs:4837` and `http.rs:877` omit
  `retryable`; the `/api/logs` route adds `"detail"` (`log_route_error`,
  `http.rs:1083`). Response header `cache-control: no-store` is set on session,
  logs, approvals, run detail and refusal responses.

## 2. Read routes

### 2.1 `GET /health`

No launch/session token. Handler `http.rs:4780`; fields from
`HealthResponse` (`http.rs:622`).

| Field | Type | Meaning |
|---|---|---|
| `status` | `"ok" \| "degraded" \| "failing"` | required checks + database version |
| `version` | string | crate version |
| `schema` | string | `hiverouter.v1` |
| `uptime_secs` | integer | process uptime |
| `pid` | integer | OS pid |
| `boot_id` | string | per-boot identity |
| `data_dir` | string | effective data directory |
| `workspace_root` | string | file-tool workspace root |
| `checks` | array | `{name, ok, required, ...}` check records |
| `database.path` | string | SQLite path |
| `database.schema_version` | integer \| null | applied migration version |
| `database.error_code` | string \| null | refusal code when unreadable |
| `transport.last_seq` | integer | current server event sequence |
| `transport.graph_nodes` | integer \| null | in-memory graph nodes |
| `transport.allowed_origins` | integer | allowlist size (values omitted) |
| `prompt_assets.loaded` | bool | embedded planner templates loaded |
| `prompt_assets.error_code` | string \| null | asset-load refusal |

HTTP status is `200` when required checks pass, `503` when a required check
fails. A `failing` body is still JSON with the same fields.

### 2.2 `GET /api/session`

Handler `http.rs:4837`. Requires `x-hivemind-launch` when a launch token is
configured. Response:

```json
{
  "token": "<per-boot session token>",
  "schema": "hiverouter.v1",
  "last_seq": 123,
  "ws": "/ws"
}
```

`token` must never be logged or returned by a tool. `last_seq` is the sequence
to pass to `/ws`.

### 2.3 `GET /api/logs?source=<source>&tail=<n>`

Handler `http.rs:1003`; response struct `LogTail` (`health.rs:126`). Requires
the launcher token when configured; no Bearer required.

- `source`: exactly one of `launcher`, `stdout`, `stderr`, `audit`. Required.
- `tail`: optional positive integer; server default 200, cap 5000
  (`DEFAULT_TAIL_LINES`, `MAX_TAIL_LINES` in `health.rs`).
- Unknown query parameters are rejected (`400 E_SCHEMA_INVALID`, with
  `detail` naming the unknown key).
- Whitelisted files only: `launcher.log`, `server.stdout.log`,
  `server.stderr.log`, audit source. The path is never caller-controlled.
- Response body is capped at 64 KiB and server-redacted
  (`health::read_log_tail_with_secrets`, which also redacts the session token).

| Field | Type | Meaning |
|---|---|---|
| `schema` | string | `hiverouter.logs.v1` |
| `source` | string | requested source |
| `file` | string | whitelisted relative path (forward slashes) |
| `tail` | integer | requested tail after clamping |
| `total_bytes` | integer | size on disk at read time |
| `returned_bytes` | integer | returned redacted body size |
| `line_count` | integer | trailing lines in `content` |
| `lines_omitted` | integer | lines dropped by caps |
| `truncated` | bool | true when the 64 KiB cap cut the start |
| `content` | string | redacted log body |

### 2.4 `GET /api/history/runs`

Handler `http.rs:2055`. Requires launch token (when configured). Returns
`{"runs": [...]}` with each row:

```json
{
  "run_id": "uuid",
  "project_id": "workspace",
  "status": "queued|running|completed|failed|cancelled|interrupted",
  "error_code": null,
  "created_at": "RFC3339"
}
```

The server passes a fixed limit of **200** to `history.runs(200)`; there is no
`limit` query parameter. Clients that need fewer slice locally.

### 2.5 `GET /api/runs/active`

Handler `http.rs:2635`. Requires launch token. Read-only projection; never
fabricates a run.

```json
{
  "active_run_id": "uuid|null",
  "latest_active_run": {
    "run_id": "uuid",
    "project_id": "workspace",
    "status": "running",
    "error_code": null,
    "created_at": "RFC3339"
  }
}
```

`latest_active_run` is `null` when there is no non-terminal persisted run.
`active_run_id` is the live engine slot (`null` when idle).

### 2.6 `GET /api/runs/{run_id}/detail`

Handler `http.rs:4355`. Requires launch token. This is the per-run status
projection used by `hivemind_run_status`.

```json
{
  "run_id": "uuid",
  "project_id": "workspace",
  "status": "running",
  "error_code": null,
  "created_at": "RFC3339",
  "updated_at": "RFC3339",
  "task_count": 3,
  "state_counts": { "running": 2, "completed": 1 },
  "tasks": [
    {
      "task_id": "uuid",
      "parent_task_id": "uuid|null",
      "role": "string",
      "class": "string|null",
      "profile_id": "string|null",
      "state": "string",
      "depth": 0,
      "rev": 4
    }
  ],
  "usage": null,
  "memory_banks": 0
}
```

`usage` is `null` or `{prompt_tokens, completion_tokens, total_tokens}`.
Unknown run id: `404 E_RUN_NOT_FOUND`; invalid UUID: `404 E_RUN_NOT_FOUND`.

### 2.7 `GET /api/approvals?run_id=<uuid>`

Handler `http.rs:2555`. Requires launch token. **Read-only**: nonces are not
consumed, no execution is triggered, nothing is resolved.

```json
{
  "run_id": "uuid|null",
  "count": 1,
  "approvals": [ /* APPROVAL_REQUEST event envelopes */ ]
}
```

Each approval is the event envelope (Section 3.3) for `APPROVAL_REQUEST`.
Observed approval payload fields from `hiverouter-tools/src/approval.rs:90`:
`approval_id`, `run_id`, `task_id`, `tool_call_id`, `tool_name`,
`args_sha256`, `args_preview`, `permission_class`, `created_at`,
`expires_at`, `nonce`. The nonce is part of the read projection; a client
must not print it or use it without an explicit human decision.

### 2.8 `GET /api/history/runs/{run_id}/state?revision=<n>`

Handler `http.rs:2100`. Requires launch token. Uses
`history.state_at(run_id, revision)`.

```json
{
  "run_id": "uuid",
  "revision": 7,
  "schema_version": "hiverouter.v1",
  "run_status": "running|null",
  "graph": { "...": "history graph projection" }
}
```

**Observed code/docs mismatch:** `docs/AGENT-CLI.md` describes `state` as
"latest if `--revision` omitted", but the handler requires `revision` and
returns `400 E_SCHEMA_INVALID` when it is absent. The CLI always requires
`--revision` (`tools/hivemind-cli.mjs` `state` args), so the code path is
consistent; only the prose is stale.

### 2.9 Other read routes (not used by this MCP server)

`GET /api/runtime`, `/api/skills`, `/api/sessions`, `/api/agents/*`,
`/api/memory/*`, `/api/approvals`, `/api/templates`, `/api/workspace`,
`/api/jobs`, `/api/provider/*`, `/api/eval/*`, `/api/remotes`, `/api/daemons`.
See the router at `http.rs:689` for the complete list. Mutating routes
(`POST`, `DELETE`, and `PATCH`-style actions) require the Bearer token from
Section 1.

## 3. WebSocket `GET /ws`

Upgrade handler `ws.rs:482`; the connection URL is
`ws://127.0.0.1:8099/ws?token=<session.token>&last_seq=<n>`.
The `Origin` header must be the server origin (`http://127.0.0.1:8099`);
missing or foreign Origin is rejected during the upgrade with a JSON refusal.

### 3.1 Envelope

Commands and events share one envelope
(`hiverouter-contract/src/envelope.rs:36`):

```json
{
  "schema": "hiverouter.v1",
  "type": "COMMAND|EVENT",
  "name": "SUBMIT_PROMPT",
  "id": "uuid",
  "correlation_id": "uuid",
  "run_id": "uuid|null",
  "task_id": "uuid|null",
  "seq": null,
  "ts": "RFC3339",
  "payload": {}
}
```

`seq` is a server sequence number on events; commands send `null`.
`correlation_id` links a command to its ACK/ERROR.

### 3.2 Handshake

1. Open the WebSocket with the session token and `last_seq`.
2. Send `{"type":"hello","last_seq":<n>}` as the first frame.
3. The server replies with `SERVER_HELLO`
   (`payload: {schema, last_seq}`) and may replay events after `last_seq`.
4. Send command envelopes; the server emits a typed `ACK` or typed `ERROR` for
   every command. A command is never silently dropped.

### 3.3 Events used here

- `ACK` payload: `{command, correlation_id}` (`event.rs:142`).
- `ERROR` payload: `{code, message, how_to_fix, retryable}`
  (`event.rs` `ErrorPayload`).
- `RUN_CREATED` payload: `{run_id, status:"queued"}` (`event.rs:195`), and the
  envelope also carries `run_id`.
- `RUN_STATUS` payload: `{run_id, status, error_code?}`. Terminal statuses:
  `completed`, `failed`, `cancelled`, `interrupted`.
- `APPROVAL_REQUEST`: envelope for one pending approval; its payload is the
  approval record described in Section 2.7.

### 3.4 `SUBMIT_PROMPT` command

Payload (`hiverouter-contract/src/command.rs:167`):

```json
{
  "text": "1..8000 characters",
  "project_id": "workspace",
  "session_id": "optional uuid",
  "permission_policy": "classic|plan|standard|trusted"
}
```

- `text` is required and capped at 8000 chars (`MAX_PROMPT_CHARS`).
- `project_id` is required by the payload; the CLI and this MCP server default
  it to `"workspace"`.
- `session_id` and `permission_policy` are optional; omitting
  `permission_policy` uses `classic`.
- Keyless servers refuse with `E_KEY_MISSING` **before** `create_run`; no run
  is created.
- On success the server publishes `ACK` and `RUN_CREATED`. The ACK may arrive
  before or after `RUN_CREATED`; clients must accept both orders and may read
  the run id from either frame.

## 4. MCP mapping (`tools/hivemind-mcp`)

| MCP tool | Underlying call |
|---|---|
| `hivemind_health` | `GET /health` |
| `hivemind_run` | WS `SUBMIT_PROMPT`, wait for `ACK` + `RUN_CREATED` |
| `hivemind_run_status` | `GET /api/runs/{run_id}/detail`, or `GET /api/runs/active` without `run_id` |
| `hivemind_logs` | `GET /api/logs?source=&tail=` |
| `hivemind_runs` | `GET /api/history/runs` |
| `hivemind_approvals` | `GET /api/approvals?run_id=` (read-only) |

The MCP server never exposes the session token, never returns the
`/api/session` body, and never sends the launch token anywhere except the
loopback HiveMind server. It refuses non-loopback base URLs.

## 5. Versioning notes

- Contract schema id: `hiverouter.v1` (`hiverouter-contract` `SCHEMA_VERSION`).
- `initialize` negotiates MCP `2024-11-05`, `2025-03-26` or `2025-06-18`.
- No behavior in this document is inferred from UI code; every route/field is
  cited to the Rust handler or struct that emits it.
