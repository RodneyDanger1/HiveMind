#!/usr/bin/env node
/**
 * Real stdio smoke test for tools/hivemind-mcp/server.mjs.
 *
 * Starts a fake HiveMind loopback server (HTTP + WebSocket upgrade) and drives
 * the MCP server exactly as a client would: newline-delimited JSON-RPC over
 * the child process stdin/stdout. No direct calls into server internals.
 */
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { createHash } from 'node:crypto';
import fs from 'node:fs';
import http from 'node:http';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const SERVER = path.join(HERE, '..', 'server.mjs');
const LAUNCH_TOKEN = 'launch-token-for-smoke-test';
const SESSION_TOKEN = 'session-token-for-smoke-test';
const RUN_ID = '11111111-1111-4111-8111-111111111111';
const DETAIL_ID = '22222222-2222-4222-8222-222222222222';

function json(res, status, body) {
  res.writeHead(status, { 'content-type': 'application/json', 'cache-control': 'no-store' });
  res.end(JSON.stringify(body));
}

function authorized(req) {
  return (
    req.headers['x-hivemind-launch'] === LAUNCH_TOKEN &&
    req.headers.authorization === `Bearer ${SESSION_TOKEN}`
  );
}

function startFakeHiveMind() {
  const state = {
    calls: [],
    wsCommands: [],
    launchHeaderSeen: false,
    authorizationSeen: false,
  };
  const sockets = new Set();
  const server = http.createServer((req, res) => {
    const url = new URL(req.url, 'http://127.0.0.1');
    state.calls.push(`${req.method} ${url.pathname}${url.search}`);
    if (url.pathname === '/health') {
      json(res, 200, {
        status: 'ok',
        version: '0.0.0-smoke',
        schema: 'hiverouter.v1',
        uptime_secs: 5,
        pid: 1234,
        boot_id: 'fake-boot',
        data_dir: 'C:/fake',
        workspace_root: 'C:/fake/workspace',
        checks: [{ name: 'database', ok: true, required: true }],
        database: { path: 'C:/fake/hiverouter.db', schema_version: 12, error_code: null },
        transport: { last_seq: 4, graph_nodes: 0, allowed_origins: 2 },
        prompt_assets: { loaded: true, error_code: null },
      });
      return;
    }
    if (url.pathname === '/api/session') {
      if (req.headers['x-hivemind-launch'] !== LAUNCH_TOKEN) {
        json(res, 403, {
          code: 'E_ORIGIN_REJECTED',
          message: 'the launcher token is required to read the session',
          how_to_fix: 'start HiveMind through its launcher',
        });
        return;
      }
      state.launchHeaderSeen = true;
      json(res, 200, {
        token: SESSION_TOKEN,
        schema: 'hiverouter.v1',
        last_seq: 4,
        ws: '/ws',
      });
      return;
    }
    if (!authorized(req)) {
      json(res, 403, {
        code: 'E_ORIGIN_REJECTED',
        message: 'this read route requires the launcher token',
        how_to_fix: 'start HiveMind through its launcher',
      });
      return;
    }
    state.launchHeaderSeen = true;
    state.authorizationSeen = true;
    if (url.pathname === '/api/logs') {
      const source = url.searchParams.get('source');
      const tail = Number(url.searchParams.get('tail'));
      if (!['launcher', 'stdout', 'stderr', 'audit'].includes(source) || !Number.isInteger(tail)) {
        json(res, 400, {
          code: 'E_SCHEMA_INVALID',
          message: 'bad log query',
          how_to_fix: 'send source and tail',
        });
        return;
      }
      // Contains the session token on purpose: the MCP server must redact it.
      json(res, 200, {
        schema: 'hivemind.logs.v1',
        source,
        file: 'server.stdout.log',
        tail,
        total_bytes: 120,
        returned_bytes: 60,
        line_count: 2,
        lines_omitted: 0,
        truncated: false,
        content: `hello\nsession=${SESSION_TOKEN}`,
      });
      return;
    }
    if (url.pathname === '/api/runs/active') {
      json(res, 200, { active_run_id: null, latest_active_run: null });
      return;
    }
    if (url.pathname === `/api/runs/${DETAIL_ID}/detail`) {
      json(res, 200, {
        run_id: DETAIL_ID,
        project_id: 'workspace',
        status: 'running',
        error_code: null,
        created_at: '2026-01-01T00:00:00Z',
        updated_at: '2026-01-01T00:00:05Z',
        task_count: 1,
        state_counts: { running: 1 },
        tasks: [],
        usage: null,
        memory_banks: 0,
      });
      return;
    }
    if (url.pathname === '/api/history/runs') {
      json(res, 200, { runs: [] });
      return;
    }
    if (url.pathname === '/api/approvals') {
      json(res, 200, { run_id: null, count: 0, approvals: [] });
      return;
    }
    json(res, 404, { code: 'E_NOT_FOUND', message: 'not found', how_to_fix: 'nope' });
  });

  server.on('connection', (socket) => {
    sockets.add(socket);
    socket.once('close', () => sockets.delete(socket));
  });

  server.on('upgrade', (req, socket) => {
    sockets.add(socket);
    socket.once('close', () => sockets.delete(socket));
    const url = new URL(req.url, 'http://127.0.0.1');
    const key = req.headers['sec-websocket-key'];
    const origin = req.headers.origin;
    const launch = req.headers['x-hivemind-launch'];
    state.lastUpgrade = {
      url: req.url,
      pathname: url.pathname,
      token: url.searchParams.get('token'),
      keyType: typeof key,
      origin,
      launch,
    };
    if (
      url.pathname !== '/ws' ||
      url.searchParams.get('token') !== SESSION_TOKEN ||
      typeof key !== 'string' ||
      typeof origin !== 'string' ||
      launch !== LAUNCH_TOKEN
    ) {
      socket.end('HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\n\r\n');
      return;
    }
    state.launchHeaderSeen = true;
    const accept = createHash('sha1')
      .update(`${key}258EAFA5-E914-47DA-95CA-C5AB0DC85B11`)
      .digest('base64');
    socket.write(
      'HTTP/1.1 101 Switching Protocols\r\n' +
        'Upgrade: websocket\r\n' +
        'Connection: Upgrade\r\n' +
        `Sec-WebSocket-Accept: ${accept}\r\n\r\n`,
    );
    sendEvent(socket, {
      type: 'EVENT',
      name: 'SERVER_HELLO',
      correlation_id: null,
      run_id: null,
      seq: 4,
      payload: { schema: 'hiverouter.v1', last_seq: 4 },
    });
    let buffer = Buffer.alloc(0);
    socket.on('data', (chunk) => {
      buffer = Buffer.concat([buffer, chunk]);
      for (;;) {
        if (buffer.length < 2) return;
        const first = buffer[0];
        const second = buffer[1];
        const opcode = first & 0x0f;
        const masked = (second & 0x80) !== 0;
        let length = second & 0x7f;
        let offset = 2;
        if (length === 126) {
          if (buffer.length < 4) return;
          length = buffer.readUInt16BE(2);
          offset = 4;
        } else if (length === 127) {
          if (buffer.length < 10) return;
          length = Number(buffer.readBigUInt64BE(2));
          offset = 10;
        }
        let mask = null;
        if (masked) {
          if (buffer.length < offset + 4) return;
          mask = buffer.subarray(offset, offset + 4);
          offset += 4;
        }
        if (buffer.length < offset + length) return;
        const payload = Buffer.from(buffer.subarray(offset, offset + length));
        buffer = buffer.subarray(offset + length);
        if (mask) {
          for (let index = 0; index < payload.length; index += 1) {
            payload[index] ^= mask[index % 4];
          }
        }
        if (opcode === 0x8) {
          socket.end();
          return;
        }
        if (opcode !== 0x1) continue;
        let frame;
        try {
          frame = JSON.parse(payload.toString('utf8'));
        } catch {
          continue;
        }
        if (frame.type === 'hello') continue;
        if (frame.type === 'COMMAND' && frame.name === 'SUBMIT_PROMPT') {
          state.wsCommands.push(frame);
          sendEvent(socket, {
            type: 'EVENT',
            name: 'ACK',
            correlation_id: frame.correlation_id,
            run_id: null,
            seq: 5,
            payload: { command: 'SUBMIT_PROMPT', correlation_id: frame.correlation_id },
          });
          setTimeout(() => {
            sendEvent(socket, {
              type: 'EVENT',
              name: 'RUN_CREATED',
              correlation_id: frame.correlation_id,
              run_id: RUN_ID,
              seq: 6,
              payload: { run_id: RUN_ID, status: 'queued' },
            });
          }, 10);
        }
      }
    });
  });

  return new Promise((resolve) => {
    server.listen(0, '127.0.0.1', () => {
      const address = server.address();
      resolve({
        url: `http://127.0.0.1:${address.port}`,
        state,
        close: () =>
          new Promise((done) => {
            for (const socket of sockets) socket.destroy();
            sockets.clear();
            server.close(() => done());
            setTimeout(done, 1_000).unref();
          }),
      });
    });
  });
}

function sendEvent(socket, event) {
  const payload = Buffer.from(JSON.stringify(event), 'utf8');
  let header;
  if (payload.length < 126) {
    header = Buffer.from([0x81, payload.length]);
  } else if (payload.length < 65_536) {
    header = Buffer.alloc(4);
    header[0] = 0x81;
    header[1] = 126;
    header.writeUInt16BE(payload.length, 2);
  } else {
    header = Buffer.alloc(10);
    header[0] = 0x81;
    header[1] = 127;
    header.writeBigUInt64BE(BigInt(payload.length), 2);
  }
  socket.write(Buffer.concat([header, payload]));
}

class McpProcess {
  constructor(env) {
    this.child = spawn(process.execPath, [SERVER], {
      stdio: ['pipe', 'pipe', 'pipe'],
      env: { ...process.env, ...env },
      windowsHide: true,
    });
    this.nextId = 1;
    this.buffer = '';
    this.pending = new Map();
    this.stderr = '';
    this.stdout = '';
    this.exited = new Promise((resolve) => this.child.once('exit', resolve));
    this.child.stdout.setEncoding('utf8');
    this.child.stdout.on('data', (chunk) => {
      this.stdout += chunk;
      this.buffer += chunk;
      for (;;) {
        const newline = this.buffer.indexOf('\n');
        if (newline < 0) break;
        const line = this.buffer.slice(0, newline).trim();
        this.buffer = this.buffer.slice(newline + 1);
        if (!line) continue;
        const message = JSON.parse(line);
        const waiter = this.pending.get(message.id);
        if (waiter) {
          this.pending.delete(message.id);
          clearTimeout(waiter.timer);
          waiter.resolve(message);
        }
      }
    });
    this.child.stderr.setEncoding('utf8');
    this.child.stderr.on('data', (chunk) => {
      this.stderr += chunk;
    });
  }

  request(method, params) {
    const id = this.nextId;
    this.nextId += 1;
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(id);
        reject(new Error(`timed out waiting for ${method} (id ${id})`));
      }, 10_000);
      this.pending.set(id, { resolve, reject, timer });
      this.child.stdin.write(`${JSON.stringify({ jsonrpc: '2.0', id, method, params })}\n`);
    });
  }

  notify(method, params) {
    this.child.stdin.write(`${JSON.stringify({ jsonrpc: '2.0', method, params })}\n`);
  }

  async close() {
    if (this.child.exitCode !== null) return;
    this.child.stdin.end();
    const exited = await Promise.race([
      this.exited.then(() => true),
      new Promise((resolve) => setTimeout(() => resolve(false), 1_500)),
    ]);
    if (!exited && this.child.exitCode === null) this.child.kill();
    await this.exited;
  }
}

function toolPayload(response) {
  assert.equal(response.result.isError, false, JSON.stringify(response));
  return JSON.parse(response.result.content[0].text);
}

test(
  'HiveMind MCP stdio server: initialize, list, and real HTTP/WS tool calls',
  { timeout: 30_000 },
  async (t) => {
  const fake = await startFakeHiveMind();
  const dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'hivemind-mcp-smoke-'));
  fs.writeFileSync(path.join(dataDir, 'launch.token'), `${LAUNCH_TOKEN}\n`, 'utf8');
  const mcp = new McpProcess({ HIVEROUTER_URL: fake.url, HIVEROUTER_DATA_DIR: dataDir });
  t.after(async () => {
    await mcp.close();
    await fake.close();
    fs.rmSync(dataDir, { recursive: true, force: true });
  });

  const init = await mcp.request('initialize', {
    protocolVersion: '2024-11-05',
    capabilities: {},
    clientInfo: { name: 'smoke', version: '1.0.0' },
  });
  assert.equal(init.result.protocolVersion, '2024-11-05');
  assert.equal(init.result.serverInfo.name, 'hivemind');
  assert.equal(init.result.capabilities.tools.listChanged, false);
  mcp.notify('notifications/initialized', {});

  const list = await mcp.request('tools/list', {});
  const names = list.result.tools.map((tool) => tool.name).sort();
  assert.deepEqual(names, [
    'hivemind_approvals',
    'hivemind_health',
    'hivemind_logs',
    'hivemind_run',
    'hivemind_run_status',
    'hivemind_runs',
  ]);

  const health = toolPayload(
    await mcp.request('tools/call', { name: 'hivemind_health', arguments: {} }),
  );
  assert.equal(health.status, 'ok');
  assert.equal(health.database.schema_version, 12);

  const logs = toolPayload(
    await mcp.request('tools/call', {
      name: 'hivemind_logs',
      arguments: { source: 'stdout', tail: 10 },
    }),
  );
  assert.equal(logs.source, 'stdout');
  assert.equal(logs.tail, 10);
  assert.match(logs.content, /hello/);
  assert.equal(
    JSON.stringify(logs).includes(SESSION_TOKEN),
    false,
    'session token must be redacted from tool output',
  );
  assert.equal(JSON.stringify(logs).includes(LAUNCH_TOKEN), false);

  const detail = toolPayload(
    await mcp.request('tools/call', {
      name: 'hivemind_run_status',
      arguments: { run_id: DETAIL_ID },
    }),
  );
  assert.equal(detail.run_id, DETAIL_ID);
  assert.equal(detail.status, 'running');

  const active = toolPayload(
    await mcp.request('tools/call', { name: 'hivemind_run_status', arguments: {} }),
  );
  assert.equal(active.latest_active_run, null);

  const rawSubmitted = await mcp.request('tools/call', {
    name: 'hivemind_run',
    arguments: {
      prompt: 'summarize the README',
      project_id: 'smoke-project',
      permission_policy: 'plan',
    },
  });
  assert.equal(
    rawSubmitted.result.isError,
    false,
    JSON.stringify({ response: rawSubmitted, upgrade: fake.state.lastUpgrade }),
  );
  const submitted = toolPayload(rawSubmitted);  assert.equal(submitted.accepted, true);
  assert.equal(submitted.run_id, RUN_ID);
  assert.equal(fake.state.wsCommands.length, 1);
  assert.equal(fake.state.wsCommands[0].payload.text, 'summarize the README');
  assert.equal(fake.state.wsCommands[0].payload.project_id, 'smoke-project');
  assert.equal(fake.state.wsCommands[0].payload.permission_policy, 'plan');
  assert.equal(fake.state.wsCommands[0].schema, 'hiverouter.v1');

  const approvals = toolPayload(
    await mcp.request('tools/call', { name: 'hivemind_approvals', arguments: {} }),
  );
  assert.equal(approvals.count, 0);

  const unknown = await mcp.request('tools/call', { name: 'not_a_tool', arguments: {} });
  assert.equal(unknown.result.isError, true);
  assert.match(unknown.result.content[0].text, /E_UNKNOWN_TOOL/);

  const invalid = await mcp.request('tools/call', {
    name: 'hivemind_logs',
    arguments: { source: '../etc/passwd' },
  });
  assert.equal(invalid.result.isError, true);
  assert.match(invalid.result.content[0].text, /E_SCHEMA_INVALID/);

  assert.equal(fake.state.launchHeaderSeen, true);
  assert.equal(fake.state.authorizationSeen, true);
  assert.equal(mcp.stderr.includes(LAUNCH_TOKEN), false, 'launch token must not be logged');
  assert.equal(mcp.stderr.includes(SESSION_TOKEN), false, 'session token must not be logged');

  await mcp.close();
});
