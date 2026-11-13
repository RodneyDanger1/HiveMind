#!/usr/bin/env node
/**
 * HiveMind MCP server (stdio, JSON-RPC 2.0, newline-delimited).
 *
 * The server exposes a small, auditable tool surface over the real HiveMind
 * loopback HTTP + WebSocket contract:
 *
 *   hivemind_health       GET  /health
 *   hivemind_run          WS   SUBMIT_PROMPT (wait for RUN_CREATED)
 *   hivemind_run_status   GET  /api/runs/{id}/detail or /api/runs/active
 *   hivemind_logs         GET  /api/logs?source=&tail=
 *   hivemind_runs         GET  /api/history/runs
 *   hivemind_approvals    GET  /api/approvals?run_id=
 *
 * No third-party dependencies. The launcher token is read from
 * %LOCALAPPDATA%\HiveMind\launch.token at call time (or HIVEROUTER_DATA_DIR).
 * It is never printed, logged, returned, or embedded in an error message.
 *
 * Usage: node server.mjs [--url http://127.0.0.1:8099] [--data-dir <path>]
 */
import { createHash, randomBytes, randomUUID } from 'node:crypto';
import fs from 'node:fs';
import net from 'node:net';
import os from 'node:os';
import path from 'node:path';
import process from 'node:process';

const SERVER_NAME = 'hivemind';
const SERVER_VERSION = '1.0.0';
const SERVER_SCHEMA = 'hiverouter.v1';
const SUPPORTED_PROTOCOLS = ['2024-11-05', '2025-03-26', '2025-06-18'];
const DEFAULT_PROTOCOL = '2024-11-05';
const MAX_MESSAGE_BYTES = 4 * 1024 * 1024;
const DEFAULT_TIMEOUT_MS = 15_000;
const MAX_PROMPT_CHARS = 8_000;
const DEFAULT_PROJECT = 'workspace';
const LOG_SOURCES = ['launcher', 'stdout', 'stderr', 'audit'];
const PERMISSION_POLICIES = ['classic', 'plan', 'standard', 'trusted'];

// ---------------------------------------------------------------------------
// Configuration and errors
// ---------------------------------------------------------------------------

function parseArgv(argv) {
  const options = { url: null, dataDir: null };
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    const read = () => {
      const value = argv[index + 1];
      if (typeof value !== 'string' || value.length === 0) {
        throw new HiveMindError('E_CLI_USAGE', `${arg} requires a value`);
      }
      index += 1;
      return value;
    };
    if (arg === '--url') options.url = read();
    else if (arg === '--data-dir') options.dataDir = read();
    else if (arg === '--help' || arg === '-h') {
      process.stderr.write(
        'HiveMind MCP stdio server. Options: --url <loopback base url> --data-dir <path>\n',
      );
      process.exit(0);
    } else {
      throw new HiveMindError('E_CLI_USAGE', `unknown argument: ${arg}`);
    }
  }
  return options;
}

class HiveMindError extends Error {
  constructor(code, message, howToFix = null) {
    super(message);
    this.name = 'HiveMindError';
    this.code = code;
    this.howToFix = howToFix;
  }
}

function dataDirectory(options) {
  if (options.dataDir) return path.resolve(options.dataDir);
  if (process.env.HIVEROUTER_DATA_DIR) return path.resolve(process.env.HIVEROUTER_DATA_DIR);
  const localAppData = process.env.LOCALAPPDATA || path.join(os.homedir(), 'AppData', 'Local');
  return path.join(localAppData, 'HiveMind');
}

function readLaunchToken(options) {
  try {
    const token = fs
      .readFileSync(path.join(dataDirectory(options), 'launch.token'), 'utf8')
      .trim();
    return token.length > 0 ? token : null;
  } catch {
    return null;
  }
}

function readCliStateUrl(options) {
  try {
    const state = JSON.parse(
      fs.readFileSync(path.join(dataDirectory(options), 'agent-cli-state.json'), 'utf8'),
    );
    return typeof state.url === 'string' ? state.url : null;
  } catch {
    return null;
  }
}

/** Only a loopback origin is accepted: the token must never leave the host. */
function resolveBaseUrl(options) {
  const raw =
    options.url ||
    process.env.HIVEROUTER_URL ||
    process.env.HIVEMIND_URL ||
    readCliStateUrl(options) ||
    'http://127.0.0.1:8099';
  let parsed;
  try {
    parsed = new URL(raw);
  } catch {
    throw new HiveMindError('E_CLI_UNSAFE', 'HIVEROUTER_URL is not a valid URL');
  }
  if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') {
    throw new HiveMindError('E_CLI_UNSAFE', 'HiveMind URL must be http or https');
  }
  const host = parsed.hostname.replace(/^\[|\]$/g, '').toLowerCase();
  const loopback =
    host === 'localhost' || host === '::1' || host === '0:0:0:0:0:0:0:1' || host.startsWith('127.');
  if (!loopback) {
    throw new HiveMindError('E_CLI_UNSAFE', 'HiveMind URL must be a loopback address');
  }
  if (parsed.pathname !== '/' || parsed.search !== '' || parsed.hash !== '') {
    throw new HiveMindError('E_CLI_UNSAFE', 'HiveMind URL must be an origin without a path');
  }
  parsed.username = '';
  parsed.password = '';
  return parsed.origin;
}

// ---------------------------------------------------------------------------
// HTTP transport
// ---------------------------------------------------------------------------

class HiveMindClient {
  constructor(baseUrl, options) {
    this.baseUrl = baseUrl;
    this.options = options;
    this.session = null;
  }

  launchToken() {
    return readLaunchToken(this.options);
  }

  async getSession(force = false) {
    if (!force && this.session !== null) return this.session;
    const headers = { accept: 'application/json' };
    const launch = this.launchToken();
    if (launch) headers['x-hivemind-launch'] = launch;
    const body = await this.requestJson('/api/session', {
      headers,
      authenticated: false,
      redactValues: false,
    });
    if (
      !body ||
      typeof body.token !== 'string' ||
      !body.token ||
      typeof body.ws !== 'string'
    ) {
      throw new HiveMindError(
        'E_SCHEMA_INVALID',
        'GET /api/session did not return token/ws/last_seq',
      );
    }
    this.session = body;
    return body;
  }

  async authedHeaders() {
    const session = await this.getSession();
    const headers = {
      accept: 'application/json',
      authorization: `Bearer ${session.token}`,
    };
    const launch = this.launchToken();
    if (launch) headers['x-hivemind-launch'] = launch;
    return headers;
  }

  /** GET a read route. `authenticated` adds the per-boot Bearer token. */
  async getJson(pathname, { authenticated = true, timeoutMs = DEFAULT_TIMEOUT_MS } = {}) {
    const headers = authenticated
      ? await this.authedHeaders()
      : { accept: 'application/json' };
    return this.requestJson(pathname, { headers, authenticated, timeoutMs });
  }

  async requestJson(
    pathname,
    { headers, authenticated, timeoutMs = DEFAULT_TIMEOUT_MS, redactValues = true },
  ) {
    const url = `${this.baseUrl}${pathname}`;
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    let response;
    try {
      response = await fetch(url, {
        method: 'GET',
        headers,
        redirect: 'error',
        signal: controller.signal,
      });
    } catch (failure) {
      if (failure?.name === 'AbortError') {
        throw new HiveMindError('E_TIMEOUT', `GET ${pathname} timed out after ${timeoutMs}ms`);
      }
      throw new HiveMindError(
        'E_SERVER_UNREACHABLE',
        `cannot reach the HiveMind server at ${this.baseUrl}`,
      );
    } finally {
      clearTimeout(timer);
    }
    const text = await response.text();
    if (Buffer.byteLength(text, 'utf8') > MAX_MESSAGE_BYTES) {
      throw new HiveMindError('E_RESPONSE_TOO_LARGE', `GET ${pathname} returned too much data`);
    }
    let body = null;
    try {
      body = text.length === 0 ? null : JSON.parse(text);
    } catch {
      body = null;
    }
    if (!response.ok) {
      const code =
        body && typeof body.code === 'string'
          ? body.code
          : response.status === 401
            ? 'E_TOKEN_INVALID'
            : response.status === 403
              ? 'E_ORIGIN_REJECTED'
              : response.status === 404
                ? 'E_NOT_FOUND'
                : 'E_HTTP_ERROR';
      const message =
        body && typeof body.message === 'string'
          ? body.message
          : `GET ${pathname} returned HTTP ${response.status}`;
      const howToFix =
        body && typeof body.how_to_fix === 'string' ? body.how_to_fix : null;
      if (authenticated && response.status === 401) this.session = null;
      throw new HiveMindError(code, this.redact(message), howToFix);
    }
    if (body === null) {
      throw new HiveMindError('E_SCHEMA_INVALID', `GET ${pathname} did not return JSON`);
    }
    return redactValues ? this.redactValue(body) : body;
  }

  redact(value) {
    let text = String(value);
    for (const secret of [this.launchToken(), this.session?.token]) {
      if (typeof secret === 'string' && secret.length >= 8) {
        text = text.split(secret).join('[redacted]');
      }
    }
    return text;
  }

  redactValue(value) {
    return JSON.parse(this.redact(JSON.stringify(value)));
  }
}

// ---------------------------------------------------------------------------
// Minimal WebSocket client (the server rejects a missing loopback Origin, and
// Node's built-in WebSocket cannot set one).
// ---------------------------------------------------------------------------

class MiniWebSocket {
  constructor() {
    this.listeners = new Map();
    this.buffer = Buffer.alloc(0);
    this.pendingMessages = [];
    this.fragments = [];
    this.open = false;
    this.closed = false;
    this.socket = null;
    this.extraHeaders = [];
  }

  static connect(url, timeoutMs = 10_000, extraHeaders = []) {
    return new Promise((resolve, reject) => {
      const socket = new MiniWebSocket();
      socket.extraHeaders = extraHeaders;
      socket._connect(url, resolve, reject, timeoutMs);
    });
  }

  addEventListener(type, callback, options = {}) {
    const entry = { callback, once: Boolean(options.once) };
    const list = this.listeners.get(type) || [];
    list.push(entry);
    this.listeners.set(type, list);
    if (type === 'message' && this.pendingMessages.length > 0) {
      const queued = this.pendingMessages.splice(0);
      queueMicrotask(() => queued.forEach((data) => this._emit('message', { data })));
    }
    if (type === 'open' && this.open) queueMicrotask(() => this._emit('open', {}));
  }

  _emit(type, event) {
    for (const entry of [...(this.listeners.get(type) || [])]) {
      if (entry.once) {
        this.listeners.set(
          type,
          (this.listeners.get(type) || []).filter((item) => item !== entry),
        );
      }
      try {
        entry.callback(event);
      } catch {
        // A listener must not corrupt the frame stream.
      }
    }
  }

  _connect(rawUrl, resolve, reject, timeoutMs) {
    let parsed;
    try {
      parsed = new URL(rawUrl);
    } catch {
      reject(new HiveMindError('E_CLI_USAGE', 'invalid WebSocket URL'));
      return;
    }
    if (parsed.protocol !== 'ws:' && parsed.protocol !== 'wss:') {
      reject(new HiveMindError('E_CLI_UNSAFE', 'only ws/wss loopback URLs are allowed'));
      return;
    }
    const host = parsed.hostname.replace(/^\[|\]$/g, '');
    const port = Number(parsed.port || (parsed.protocol === 'wss:' ? 443 : 80));
    const origin = `${parsed.protocol === 'wss:' ? 'https:' : 'http:'}//${parsed.host}`;
    if (origin.includes('@')) {
      reject(new HiveMindError('E_CLI_UNSAFE', 'WebSocket URL must not embed credentials'));
      return;
    }
    const key = randomBytes(16).toString('base64');
    let handshake =
      `GET ${parsed.pathname}${parsed.search} HTTP/1.1\r\n` +
      `Host: ${parsed.host}\r\n` +
      'Upgrade: websocket\r\n' +
      'Connection: Upgrade\r\n' +
      `Sec-WebSocket-Key: ${key}\r\n` +
      'Sec-WebSocket-Version: 13\r\n' +
      `Origin: ${origin}\r\n`;
    for (const header of this.extraHeaders) handshake += `${header}\r\n`;
    handshake += '\r\n';
    let settled = false;
    let handshakeBuffer = Buffer.alloc(0);
    const timer = setTimeout(() => {
      if (!settled) {
        settled = true;
        reject(new HiveMindError('E_WS_TIMEOUT', 'WebSocket handshake timed out'));
      }
      this.socket?.destroy();
    }, timeoutMs);
    const socket = net.connect({ host, port });
    this.socket = socket;
    socket.once('error', (error) => {
      if (!settled) {
        settled = true;
        clearTimeout(timer);
        reject(new HiveMindError('E_WS_DISCONNECTED', `WebSocket connect failed: ${error.message}`));
      } else if (!this.closed) {
        this.closed = true;
        this._emit('error', { message: error.message });
        this._emit('close', { code: 1006 });
      }
    });
    socket.once('close', () => {
      if (!settled) {
        settled = true;
        clearTimeout(timer);
        reject(new HiveMindError('E_WS_DISCONNECTED', 'WebSocket closed during handshake'));
      } else if (!this.closed) {
        this.closed = true;
        this._emit('close', { code: 1006 });
      }
    });
    socket.on('data', (chunk) => {
      if (!settled) {
        handshakeBuffer = Buffer.concat([handshakeBuffer, chunk]);
        const end = handshakeBuffer.indexOf('\r\n\r\n');
        if (end < 0) return;
        const header = handshakeBuffer.subarray(0, end).toString('latin1');
        const rest = handshakeBuffer.subarray(end + 4);
        if (!/^HTTP\/1\.1 101\b/.test(header)) {
          settled = true;
          clearTimeout(timer);
          reject(
            new HiveMindError(
              'E_WS_DISCONNECTED',
              `WebSocket upgrade rejected: ${header.split('\r\n')[0]}`,
            ),
          );
          socket.destroy();
          return;
        }
        const expected = createHash('sha1')
          .update(`${key}258EAFA5-E914-47DA-95CA-C5AB0DC85B11`)
          .digest('base64');
        const acceptLine = header
          .split('\r\n')
          .find((line) => line.toLowerCase().startsWith('sec-websocket-accept:'));
        if (!acceptLine || acceptLine.split(':').slice(1).join(':').trim() !== expected) {
          settled = true;
          clearTimeout(timer);
          reject(
            new HiveMindError('E_WS_DISCONNECTED', 'WebSocket accept key did not match'),
          );
          socket.destroy();
          return;
        }
        settled = true;
        clearTimeout(timer);
        this.open = true;
        if (rest.length > 0) this.buffer = Buffer.concat([this.buffer, rest]);
        this._parseFrames();
        this._emit('open', {});
        resolve(this);
        return;
      }
      this.buffer = Buffer.concat([this.buffer, chunk]);
      this._parseFrames();
    });
    socket.once('connect', () => {
      socket.write(handshake, 'latin1');
    });
  }

  _parseFrames() {
    while (this.buffer.length >= 2) {
      const first = this.buffer[0];
      const second = this.buffer[1];
      const fin = (first & 0x80) !== 0;
      const opcode = first & 0x0f;
      const masked = (second & 0x80) !== 0;
      let length = second & 0x7f;
      let offset = 2;
      if (length === 126) {
        if (this.buffer.length < 4) return;
        length = this.buffer.readUInt16BE(2);
        offset = 4;
      } else if (length === 127) {
        if (this.buffer.length < 10) return;
        const big = this.buffer.readBigUInt64BE(2);
        if (big > 16_777_216n) {
          this.close();
          return;
        }
        length = Number(big);
        offset = 10;
      }
      if (masked) offset += 4;
      if (this.buffer.length < offset + length) return;
      const payload = this.buffer.subarray(offset, offset + length);
      this.buffer = this.buffer.subarray(offset + length);
      if (opcode === 0x8) {
        this.close();
        return;
      }
      if (opcode === 0x9) {
        this._sendFrame(0xa, payload);
        continue;
      }
      if (opcode === 0xa) continue;
      if (opcode === 0x1 || opcode === 0x0) {
        if (opcode === 0x1) this.fragments = [payload];
        else this.fragments.push(payload);
        if (!fin) continue;
        const text = Buffer.concat(this.fragments).toString('utf8');
        this.fragments = [];
        if (this.listeners.has('message')) this._emit('message', { data: text });
        else this.pendingMessages.push(text);
      }
      // Binary frames are outside the frozen protocol and are ignored.
    }
  }

  _sendFrame(opcode, payload) {
    if (!this.open || !this.socket || this.closed) {
      throw new HiveMindError('E_WS_DISCONNECTED', 'WebSocket is not open');
    }
    const mask = randomBytes(4);
    const body = Buffer.from(payload);
    let header;
    if (body.length < 126) {
      header = Buffer.from([0x80 | opcode, 0x80 | body.length]);
    } else if (body.length < 65_536) {
      header = Buffer.alloc(4);
      header[0] = 0x80 | opcode;
      header[1] = 0x80 | 126;
      header.writeUInt16BE(body.length, 2);
    } else {
      header = Buffer.alloc(10);
      header[0] = 0x80 | opcode;
      header[1] = 0x80 | 127;
      header.writeBigUInt64BE(BigInt(body.length), 2);
    }
    const masked = Buffer.alloc(body.length);
    for (let index = 0; index < body.length; index += 1) {
      masked[index] = body[index] ^ mask[index % 4];
    }
    this.socket.write(Buffer.concat([header, mask, masked]));
  }

  send(data) {
    this._sendFrame(0x1, Buffer.from(String(data), 'utf8'));
  }

  close() {
    if (this.closed) return;
    this.closed = true;
    try {
      if (this.open) this._sendFrame(0x8, Buffer.alloc(0));
    } catch {
      // The peer may already be gone.
    }
    this.socket?.destroy();
    this._emit('close', { code: 1000 });
  }
}

function commandEnvelope(name, payload) {
  const id = randomUUID();
  return {
    schema: SERVER_SCHEMA,
    type: 'COMMAND',
    name,
    id,
    correlation_id: id,
    run_id: null,
    task_id: null,
    seq: null,
    ts: new Date().toISOString(),
    payload,
  };
}

async function wsSession(client, timeoutMs = DEFAULT_TIMEOUT_MS) {
  const session = await client.getSession(true);
  const wsUrl = new URL(session.ws, client.baseUrl);
  wsUrl.protocol = wsUrl.protocol === 'https:' ? 'wss:' : 'ws:';
  wsUrl.searchParams.set('token', session.token);
  wsUrl.searchParams.set('last_seq', String(session.last_seq ?? 0));
  const headers = [];
  const launch = client.launchToken();
  if (launch) headers.push(`X-HiveMind-Launch: ${launch}`);
  const socket = await MiniWebSocket.connect(wsUrl.href, 10_000, headers);
  const queue = [];
  const waiters = [];
  socket.addEventListener('message', (event) => {
    let value;
    try {
      value = JSON.parse(typeof event.data === 'string' ? event.data : String(event.data));
    } catch {
      return;
    }
    queue.push(value);
    for (const waiter of [...waiters]) {
      if (!waiter.predicate(value)) continue;
      waiters.splice(waiters.indexOf(waiter), 1);
      clearTimeout(waiter.timer);
      waiter.resolve(value);
    }
  });
  socket.addEventListener('close', (event) => {
    for (const waiter of waiters.splice(0)) {
      clearTimeout(waiter.timer);
      waiter.reject(
        new HiveMindError('E_WS_DISCONNECTED', `WebSocket closed (code ${event.code})`),
      );
    }
  });
  socket.addEventListener('error', () => {
    // The close handler carries the actionable failure.
  });
  socket.send(JSON.stringify({ type: 'hello', last_seq: session.last_seq ?? 0 }));
  const next = (predicate, waitMs = timeoutMs) => {
    const foundIndex = queue.findIndex(predicate);
    if (foundIndex >= 0) return Promise.resolve(queue.splice(foundIndex, 1)[0]);
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        const index = waiters.findIndex((waiter) => waiter.resolve === resolve);
        if (index >= 0) waiters.splice(index, 1);
        reject(new HiveMindError('E_EVENT_TIMEOUT', 'no matching server event arrived'));
      }, waitMs);
      waiters.push({ predicate, resolve, reject, timer });
    });
  };
  return {
    session,
    socket,
    next,
    close: () => {
      try {
        socket.close();
      } catch {
        // Already closed.
      }
    },
  };
}

async function submitPrompt(client, payload, timeoutMs) {
  const session = await wsSession(client);
  try {
    const envelope = commandEnvelope('SUBMIT_PROMPT', payload);
    session.socket.send(JSON.stringify(envelope));
    const response = await session.next((frame) => {
      if (frame?.type !== 'EVENT') return false;
      if (frame.name === 'ACK') {
        return frame.payload?.correlation_id === envelope.correlation_id;
      }
      if (frame.name === 'ERROR') {
        return (
          frame.correlation_id === envelope.correlation_id ||
          frame.payload?.correlation_id === envelope.correlation_id
        );
      }
      return false;
    }, timeoutMs);
    if (response.name === 'ERROR') {
      const refusal = response.payload || {};
      throw new HiveMindError(
        typeof refusal.code === 'string' ? refusal.code : 'E_REFUSED',
        typeof refusal.message === 'string' ? refusal.message : 'the command was refused',
        typeof refusal.how_to_fix === 'string' ? refusal.how_to_fix : null,
      );
    }
    // RUN_CREATED may be emitted before or after the ACK; look for it after.
    const created = await session.next(
      (frame) => frame?.type === 'EVENT' && frame.name === 'RUN_CREATED',
      timeoutMs,
    );
    const runId =
      (typeof created.payload?.run_id === 'string' && created.payload.run_id) ||
      (typeof created.run_id === 'string' && created.run_id) ||
      (typeof response.payload?.run_id === 'string' && response.payload.run_id) ||
      null;
    return {
      accepted: runId !== null,
      run_id: runId,
      ack: response.payload ?? null,
      run_created: created.payload ?? null,
    };
  } finally {
    session.close();
  }
}

// ---------------------------------------------------------------------------
// Tool definitions
// ---------------------------------------------------------------------------

const TOOLS = [
  {
    name: 'hivemind_health',
    description: 'Read the local HiveMind server health, schema version and checks (GET /health).',
    inputSchema: { type: 'object', properties: {}, additionalProperties: false },
  },
  {
    name: 'hivemind_run',
    description:
      'Submit a prompt to the local HiveMind engine over its WebSocket COMMAND contract. Returns once the server acknowledges and publishes RUN_CREATED (not when the run finishes).',
    inputSchema: {
      type: 'object',
      properties: {
        prompt: { type: 'string', minLength: 1, maxLength: MAX_PROMPT_CHARS },
        project_id: { type: 'string', minLength: 1, maxLength: 200 },
        session_id: { type: 'string', minLength: 8, maxLength: 80 },
        permission_policy: { type: 'string', enum: PERMISSION_POLICIES },
      },
      required: ['prompt'],
      additionalProperties: false,
    },
  },
  {
    name: 'hivemind_run_status',
    description:
      'Read one run detail (GET /api/runs/{run_id}/detail), or the live/active run projection when run_id is omitted (GET /api/runs/active).',
    inputSchema: {
      type: 'object',
      properties: { run_id: { type: 'string', minLength: 8, maxLength: 80 } },
      additionalProperties: false,
    },
  },
  {
    name: 'hivemind_logs',
    description:
      'Read a bounded, server-redacted log tail from GET /api/logs. Source is required by the server; tail defaults to 200 and is capped at 5000.',
    inputSchema: {
      type: 'object',
      properties: {
        source: { type: 'string', enum: LOG_SOURCES },
        tail: { type: 'integer', minimum: 1, maximum: 5000 },
      },
      required: ['source'],
      additionalProperties: false,
    },
  },
  {
    name: 'hivemind_runs',
    description: 'List committed HiveMind runs, newest first (GET /api/history/runs).',
    inputSchema: { type: 'object', properties: {}, additionalProperties: false },
  },
  {
    name: 'hivemind_approvals',
    description:
      'List pending approvals read-only (GET /api/approvals); never consumes a nonce and never resolves one.',
    inputSchema: {
      type: 'object',
      properties: { run_id: { type: 'string', minLength: 8, maxLength: 80 } },
      additionalProperties: false,
    },
  },
];

function requireObject(args) {
  if (args === undefined || args === null) return {};
  if (typeof args !== 'object' || Array.isArray(args)) {
    throw new HiveMindError('E_SCHEMA_INVALID', 'tool arguments must be an object');
  }
  return args;
}

function optionalString(args, name, { min = 1, max = 200 } = {}) {
  if (args[name] === undefined) return null;
  if (typeof args[name] !== 'string') {
    throw new HiveMindError('E_SCHEMA_INVALID', `${name} must be a string`);
  }
  const value = args[name].trim();
  if (value.length < min || value.length > max) {
    throw new HiveMindError('E_SCHEMA_INVALID', `${name} length is out of range`);
  }
  return value;
}

async function callTool(client, name, rawArgs) {
  const args = requireObject(rawArgs);
  switch (name) {
    case 'hivemind_health':
      return client.getJson('/health', { authenticated: false });
    case 'hivemind_run': {
      if (typeof args.prompt !== 'string') {
        throw new HiveMindError('E_SCHEMA_INVALID', 'prompt is required and must be a string');
      }
      if (args.prompt.length < 1 || args.prompt.length > MAX_PROMPT_CHARS) {
        throw new HiveMindError(
          'E_SCHEMA_INVALID',
          `prompt must be 1..${MAX_PROMPT_CHARS} characters`,
        );
      }
      // Preserve the prompt exactly: trimming would change code blocks and
      // leading indentation the user intentionally supplied.
      const prompt = args.prompt;
      const projectId = optionalString(args, 'project_id') || DEFAULT_PROJECT;
      const sessionId = optionalString(args, 'session_id');
      const policy = optionalString(args, 'permission_policy', { max: 20 });
      if (policy !== null && !PERMISSION_POLICIES.includes(policy)) {
        throw new HiveMindError('E_SCHEMA_INVALID', 'permission_policy is not recognised');
      }
      const payload = { text: prompt, project_id: projectId };
      if (sessionId !== null) payload.session_id = sessionId;
      if (policy !== null) payload.permission_policy = policy;
      return submitPrompt(client, payload, 30_000);
    }
    case 'hivemind_run_status': {
      const runId = optionalString(args, 'run_id', { min: 8, max: 80 });
      if (runId === null) return client.getJson('/api/runs/active');
      return client.getJson(`/api/runs/${encodeURIComponent(runId)}/detail`);
    }
    case 'hivemind_logs': {
      const source = optionalString(args, 'source', { max: 20 });
      if (source === null || !LOG_SOURCES.includes(source)) {
        throw new HiveMindError(
          'E_SCHEMA_INVALID',
          'source must be one of: launcher, stdout, stderr, audit',
        );
      }
      let tail = 200;
      if (args.tail !== undefined) {
        if (!Number.isInteger(args.tail) || args.tail < 1 || args.tail > 5000) {
          throw new HiveMindError('E_SCHEMA_INVALID', 'tail must be an integer in 1..5000');
        }
        tail = args.tail;
      }
      return client.getJson(`/api/logs?source=${encodeURIComponent(source)}&tail=${tail}`);
    }
    case 'hivemind_runs':
      return client.getJson('/api/history/runs');
    case 'hivemind_approvals': {
      const runId = optionalString(args, 'run_id', { min: 8, max: 80 });
      return client.getJson(
        runId === null
          ? '/api/approvals'
          : `/api/approvals?run_id=${encodeURIComponent(runId)}`,
      );
    }
    default:
      throw new HiveMindError('E_UNKNOWN_TOOL', `unknown tool: ${String(name)}`);
  }
}

// ---------------------------------------------------------------------------
// JSON-RPC / MCP
// ---------------------------------------------------------------------------

function send(message) {
  process.stdout.write(`${JSON.stringify(message)}\n`);
}

function respond(id, result) {
  send({ jsonrpc: '2.0', id, result });
}

function respondError(id, code, message, data) {
  const error = { code, message };
  if (data !== undefined) error.data = data;
  send({ jsonrpc: '2.0', id, error });
}

let client;
try {
  const cliOptions = parseArgv(process.argv.slice(2));
  client = new HiveMindClient(resolveBaseUrl(cliOptions), cliOptions);
} catch (failure) {
  // Configuration failures happen before any request; no token is involved.
  process.stderr.write(`hivemind-mcp: ${failure?.message || 'configuration error'}\n`);
  process.exit(2);
}
let initialized = false;

async function handle(request) {
  const id = request?.id;
  const isNotification = id === undefined;
  if (!request || request.jsonrpc !== '2.0' || typeof request.method !== 'string') {
    if (!isNotification) respondError(id ?? null, -32600, 'invalid JSON-RPC request');
    return;
  }
  const method = request.method;
  if (method === 'initialize') {
    const requested = request.params?.protocolVersion;
    const protocolVersion = SUPPORTED_PROTOCOLS.includes(requested)
      ? requested
      : DEFAULT_PROTOCOL;
    initialized = true;
    respond(id, {
      protocolVersion,
      capabilities: { tools: { listChanged: false } },
      serverInfo: { name: SERVER_NAME, version: SERVER_VERSION },
      instructions:
        'HiveMind local control surface. Read-only diagnostics plus prompt submission and status.',
    });
    return;
  }
  if (method === 'notifications/initialized' || method === 'notifications/cancelled') return;
  if (method === 'ping') {
    respond(id, {});
    return;
  }
  if (!initialized) {
    if (!isNotification) {
      respondError(id, -32002, 'server is not initialized; send initialize first');
    }
    return;
  }
  if (method === 'tools/list') {
    respond(id, { tools: TOOLS });
    return;
  }
  if (method === 'tools/call') {
    const name = request.params?.name;
    if (typeof name !== 'string' || name.length === 0) {
      respondError(id, -32602, 'tools/call requires a tool name');
      return;
    }
    try {
      const result = await callTool(client, name, request.params?.arguments);
      respond(id, {
        content: [{ type: 'text', text: JSON.stringify(result, null, 2) }],
        isError: false,
      });
    } catch (failure) {
      const code = failure instanceof HiveMindError ? failure.code : 'E_INTERNAL';
      const message =
        failure instanceof HiveMindError ? failure.message : 'tool execution failed';
      respond(id, {
        content: [
          {
            type: 'text',
            text: JSON.stringify({ code, message, how_to_fix: failure?.howToFix ?? null }),
          },
        ],
        isError: true,
      });
    }
    return;
  }
  if (!isNotification) respondError(id, -32601, `method not found: ${method}`);
}

// Newline-delimited JSON-RPC on stdio. stderr is diagnostics only.
let buffer = '';
let pending = 0;
let ending = false;

function finishIfDrained() {
  if (ending && pending === 0) process.exit(0);
}

process.stdin.setEncoding('utf8');
process.stdin.on('data', (chunk) => {
  buffer += chunk;
  for (;;) {
    const newline = buffer.indexOf('\n');
    if (newline < 0) break;
    const line = buffer.slice(0, newline).trim();
    buffer = buffer.slice(newline + 1);
    if (line.length === 0) continue;
    let request;
    try {
      request = JSON.parse(line);
    } catch {
      respondError(null, -32700, 'parse error');
      continue;
    }
    pending += 1;
    void handle(request)
      .catch(() => {
        const id = request?.id;
        if (id !== undefined) respondError(id, -32603, 'internal error');
      })
      .finally(() => {
        pending -= 1;
        finishIfDrained();
      });
  }
  // A single unterminated message that exceeds the cap is a protocol failure.
  if (Buffer.byteLength(buffer, 'utf8') > MAX_MESSAGE_BYTES) {
    buffer = '';
    respondError(null, -32700, 'parse error: message too large');
  }
});
process.stdin.on('end', () => {
  ending = true;
  finishIfDrained();
});
process.on('uncaughtException', () => {
  // Never write a stack trace: it can contain request data. Exit loudly.
  process.stderr.write('hivemind-mcp: internal error\n');
  process.exit(1);
});
