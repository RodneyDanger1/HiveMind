# HiveMind MCP server (stdio)

A dependency-free Node.js MCP server that exposes the local HiveMind server to
an MCP client (Claude Desktop, DeepSeek Harness, or any JSON-RPC 2.0 MCP host).
It speaks newline-delimited JSON-RPC 2.0 on stdin/stdout; stderr is diagnostics
only.

It drives the **real** local HiveMind HTTP + WebSocket API described in
[`docs/contracts/HTTP-CONTRACT.md`](../../docs/contracts/HTTP-CONTRACT.md).
There are no mock responses and no test-only branches in production code.

## Requirements

- Node.js 20+ (uses global `fetch`, `AbortController`, ESM).
- A running HiveMind server on loopback (default `http://127.0.0.1:8099`).
- `launch.token` in `%LOCALAPPDATA%\HiveMind\launch.token` (written by
  `tools/start-hivemind.ps1` / `HiveMind.exe`). The token is read at call time
  and never printed, logged, or returned.

## Install

No `npm install` is needed (zero external dependencies). Point the MCP client at
`server.mjs`:

```json
{
  "mcpServers": {
    "hivemind": {
      "command": "node",
      "args": ["C:\\Users\\Riley\\Desktop\\hiverouter-v1\\tools\\hivemind-mcp\\server.mjs"]
    }
  }
}
```

Optional flags / environment:

| Setting | Meaning |
|---|---|
| `--url <origin>` | Loopback base URL (default `HIVEROUTER_URL` / CLI state / `http://127.0.0.1:8099`) |
| `--data-dir <path>` | HiveMind data dir (default `HIVEROUTER_DATA_DIR` / `%LOCALAPPDATA%\HiveMind`) |
| `HIVEROUTER_URL` | Same as `--url` |
| `HIVEROUTER_DATA_DIR` | Same as `--data-dir` |

Only `http(s)` loopback origins (`127.0.0.0/8`, `localhost`, `::1`) are accepted;
a non-loopback URL or a URL with a path/credentials is refused. The token is
never sent off-host.

## Tools

| Tool | Call | Notes |
|---|---|---|
| `hivemind_health` | `GET /health` | status, schema, checks, transport counters |
| `hivemind_run` | WS `SUBMIT_PROMPT` | returns after `ACK` + `RUN_CREATED`; **not** after the run finishes |
| `hivemind_run_status` | `GET /api/runs/{id}/detail` or `/api/runs/active` | omit `run_id` for the active projection |
| `hivemind_logs` | `GET /api/logs?source=&tail=` | `source` required; `tail` 1..5000 (default 200) |
| `hivemind_runs` | `GET /api/history/runs` | newest first, server caps at 200 |
| `hivemind_continue` | Continue a session; POST /api/sessions/{id}/continue with bounded prior context. |
| `hivemind_approvals` | `GET /api/approvals?run_id=` | read-only; never consumes a nonce |

`hivemind_run` arguments: `prompt` (required, 1..8000 chars), `project_id`
(default `workspace`), `session_id` (optional), `permission_policy`
(`classic|plan|standard|trusted`).

The server refuses unknown tools and invalid arguments as MCP tool errors
(`isError: true`) with a named `E_*` code and never echoes a token.

## Test

```powershell
cd tools\hivemind-mcp
node --test test/smoke.test.mjs
```

The smoke test spawns this server as a child process and drives it over stdio.
It starts a fake loopback HiveMind (real `node:http` server plus a real
WebSocket upgrade) that enforces the launch token, Bearer token, and Origin,
then exercises `initialize`, `tools/list`, `tools/call` for health, logs, run
status, approvals and prompt submission, plus the invalid-argument and
unknown-tool error paths. It asserts that neither the launch token nor the
session token appears in MCP output or stderr.

The test is an integration test; the fake server is a test double for the HTTP
and WebSocket transports only (permitted under AGENTS.md §7). The MCP server
itself has no fake path.

## Security notes

- The session token from `/api/session` is used only as a Bearer header and is
  never returned by a tool.
- HTTP error messages are redacted against both tokens before they reach the
  client.
- The server only connects to a loopback origin and does not follow redirects.
- `hivemind_approvals` is deliberately read-only: resolving an approval is a
  human decision and belongs to the UI/CLI, not an autonomous MCP client.
