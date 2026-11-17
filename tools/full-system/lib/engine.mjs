/**
 * Real-engine driver for the HiveMind full-system harness.
 *
 * Starts a loopback server with the deterministic provider stub, creates a
 * session, submits real runs, resolves real approvals, reads real checkpoints,
 * and tears everything down by owned PID only.
 */
import { spawn, spawnSync } from 'node:child_process';
import { createServer } from 'node:net';
import { createHash, randomUUID } from 'node:crypto';
import {
  appendFileSync,
  existsSync,
  mkdirSync,
  readFileSync,
  readdirSync,
  statSync,
  writeFileSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';

const REPO = resolve(import.meta.dirname, '..', '..', '..');
const CLI = join(REPO, 'tools', 'hivemind-cli.mjs');
const DEFAULT_SERVER = join(process.env.LOCALAPPDATA || '', 'Programs', 'HiveMind', 'hiverouter.exe');

export function sha256File(path) {
  return createHash('sha256').update(readFileSync(path)).digest('hex');
}

export function freePort() {
  return new Promise((resolvePort, reject) => {
    const server = createServer();
    server.once('error', reject);
    server.listen(0, '127.0.0.1', () => {
      const port = server.address().port;
      server.close(() => resolvePort(port));
    });
  });
}

export function createCampaignRoot(prefix = 'campaign') {
  const id = `${prefix}-${new Date().toISOString().replace(/[:.]/g, '-')}-${randomUUID().slice(0, 8)}`;
  const root = join(tmpdir(), 'hivemind-fs', id);
  for (const child of ['data', 'workspace', 'evidence', 'ledgers', 'fixtures', 'backups']) {
    mkdirSync(join(root, child), { recursive: true });
  }
  writeFileSync(join(root, 'campaign.json'), JSON.stringify({ id, root, started_at: new Date().toISOString() }, null, 2));
  return { id, root };
}

export function writeEvent(root, event) {
  const line = JSON.stringify({ ts: new Date().toISOString(), ...event });
  appendFileSync(join(root, 'events.jsonl'), `${line}\n`);
}

export async function waitForHealth(url, timeoutMs = 20_000) {
  const deadline = Date.now() + timeoutMs;
  let last = null;
  while (Date.now() < deadline) {
    try {
      const response = await fetch(`${url}/health`, { signal: AbortSignal.timeout(2_000) });
      if (response.ok) return response.json();
      last = `${response.status}`;
    } catch (error) {
      last = String(error);
    }
    await new Promise((resolveWait) => setTimeout(resolveWait, 200));
  }
  throw new Error(`health did not become ready: ${last}`);
}

export function cliJson({ url, dataDir, args, timeout = 20_000 }) {
  const result = spawnSync(process.execPath, [CLI, ...args], {
    env: { ...process.env, HIVEROUTER_URL: url, HIVEROUTER_DATA_DIR: dataDir },
    encoding: 'utf8',
    timeout,
  });
  let parsed = null;
  try { parsed = JSON.parse(result.stdout); } catch { parsed = null; }
  return {
    status: result.status,
    stdout: result.stdout || '',
    stderr: result.stderr || '',
    json: parsed,
  };
}

export async function startCampaign({
  root,
  mode = 'single-leaf',
  serverPath = process.env.HIVEROUTER_SERVER_BIN || DEFAULT_SERVER,
  serverEnv = {},
  healthTimeoutMs = 20_000,
} = {}) {
  if (!existsSync(serverPath)) throw new Error(`server binary missing: ${serverPath}`);
  const providerPort = await freePort();
  const serverPort = await freePort();
  const launch = randomUUID();
  const dataDir = join(root, 'data');
  const workspaceRoot = join(root, 'workspace');
  const evidenceDir = join(root, 'evidence');
  for (const dir of [dataDir, workspaceRoot, evidenceDir]) mkdirSync(dir, { recursive: true });
  writeFileSync(join(dataDir, 'launch.token'), launch, 'utf8');

  const provider = spawn(process.execPath, [join(REPO, 'tools', 'full-system', 'mock-provider.mjs')], {
    env: {
      ...process.env,
      MOCK_PROVIDER_PORT: String(providerPort),
      MOCK_PROVIDER_LOG: join(evidenceDir, 'provider.jsonl'),
      MOCK_PROVIDER_MODE: mode,
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  const providerLog = [];
  provider.stdout.on('data', (chunk) => providerLog.push(String(chunk)));
  provider.stderr.on('data', (chunk) => providerLog.push(String(chunk)));

  let providerReady = false;
  for (let i = 0; i < 100; i += 1) {
    try {
      const response = await fetch(`http://127.0.0.1:${providerPort}/health`, { signal: AbortSignal.timeout(500) });
      if (response.ok) { providerReady = true; break; }
    } catch {}
    await new Promise((resolveWait) => setTimeout(resolveWait, 100));
  }
  if (!providerReady) throw new Error('mock provider did not start');

  const server = spawn(serverPath, [
    '--bind', `127.0.0.1:${serverPort}`,
    '--data-dir', dataDir,
    '--workspace-root', workspaceRoot,
  ], {
    env: {
      ...process.env,
      HIVEROUTER_LAUNCH_TOKEN: launch,
      HIVEROUTER_BASE_URL: `http://127.0.0.1:${providerPort}`,
      HIVEROUTER_WIRE_MODEL: 'scripted-model',
      HIVEROUTER_PROVIDER: 'on',
      DEEPSEEK_API_KEY: 'sk-scripted-offline',
      HIVEROUTER_SIDE_EFFECTS: '1',
      ...serverEnv,
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  const serverLog = [];
  server.stdout.on('data', (chunk) => serverLog.push(String(chunk)));
  server.stderr.on('data', (chunk) => serverLog.push(String(chunk)));

  const base = `http://127.0.0.1:${serverPort}`;
  let health;
  try {
    health = await waitForHealth(base, healthTimeoutMs);
  } catch (error) {
    try { server.kill(); } catch {}
    try { provider.kill(); } catch {}
    throw new Error(`${error}; server log: ${serverLog.join('').slice(-2000)}`);
  }
  const sessionResponse = await fetch(`${base}/api/session`, {
    headers: { 'x-hivemind-launch': launch, accept: 'application/json' },
  });
  if (!sessionResponse.ok) throw new Error(`/api/session ${sessionResponse.status}`);
  const session = await sessionResponse.json();
  writeFileSync(join(evidenceDir, 'session.json'), JSON.stringify(session, null, 2));

  const headers = () => ({
    authorization: `Bearer ${session.token}`,
    'x-hivemind-launch': launch,
    accept: 'application/json',
  });

  async function json(path, init = {}) {
    const response = await fetch(`${base}${path}`, {
      ...init,
      headers: { ...headers(), ...(init.headers || {}) },
      signal: AbortSignal.timeout(10_000),
    });
    const text = await response.text();
    let body = null;
    try { body = JSON.parse(text); } catch { body = null; }
    return { status: response.status, ok: response.ok, body, text };
  }

  function submit(prompt, { sessionId = null, policy = 'classic', wait = false } = {}) {
    const args = ['submit', '--prompt', prompt, '--json'];
    if (sessionId) args.push('--session', sessionId);
    if (policy) args.push('--policy', policy);
    if (wait) args.push('--wait');
    return cliJson({ url: base, dataDir, args });
  }

  async function approvePending(runId, { denyFirst = false, max = 20 } = {}) {
    const decisions = [];
    let denied = false;
    for (let i = 0; i < max; i += 1) {
      const pending = await json(`/api/approvals?run_id=${encodeURIComponent(runId)}`);
      if (!pending.ok) break;
      const items = pending.body?.approvals || pending.body?.items || [];
      if (items.length === 0) break;
      for (const approval of items) {
        const approvalId = approval.approval_id;
        const nonce = approval.nonce;
        const tool = approval.tool || approval.tool_name || '';
        const decision = denyFirst && !denied ? 'deny' : 'approve';
        if (decision === 'deny') denied = true;
        const args = [decision, '--approval', approvalId, '--nonce', nonce, '--yes', '--json'];
        if (decision === 'deny') args.push('--reason', 'scripted harness deny proof');
        const result = cliJson({ url: base, dataDir, args, timeout: 15_000 });
        decisions.push({ tool, decision, approval_id: approvalId, nonce, status: result.status, code: result.json?.error?.code || null });
      }
      await new Promise((resolveWait) => setTimeout(resolveWait, 250));
    }
    return decisions;
  }

  async function waitRun(runId, { timeoutMs = 60_000, denyFirst = false, autoApprove = true } = {}) {
    const deadline = Date.now() + timeoutMs;
    let detail = null;
    let approvals = [];
    let approvalsSeen = 0;
    while (Date.now() < deadline) {
      const response = await json(`/api/runs/${encodeURIComponent(runId)}/detail`);
      if (response.ok) {
        detail = response.body;
        if (['completed', 'failed', 'cancelled', 'interrupted'].includes(detail?.status)) {
          if (autoApprove) approvals.push(...await approvePending(runId, { denyFirst }));
          break;
        }
      }
      if (autoApprove) {
        const before = approvalsSeen;
        approvalsSeen = (await json(`/api/approvals?run_id=${encodeURIComponent(runId)}`)).body?.approvals?.length ?? approvalsSeen;
        if (approvalsSeen > 0) {
          const decided = await approvePending(runId, { denyFirst });
          approvals.push(...decided);
        }
      }
      await new Promise((resolveWait) => setTimeout(resolveWait, 400));
    }
    return { detail, approvals };
  }

  async function createSession(name = 'full-system') {
    return json('/api/sessions', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        name,
        project_id: 'workspace',
        workspace_root: workspaceRoot,
        permission_policy: 'classic',
      }),
    });
  }

  async function checkpoints(runId) {
    return json(`/api/runs/${encodeURIComponent(runId)}/checkpoints`);
  }

  async function restoreCheckpoint(runId, seq) {
    return json(`/api/runs/${encodeURIComponent(runId)}/checkpoints/${seq}/restore`, { method: 'POST' });
  }

  async function continueSession(sessionId, prompt) {
    return json(`/api/sessions/${encodeURIComponent(sessionId)}/continue`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ prompt }),
    });
  }

  function stop() {
    for (const proc of [server, provider]) {
      try { if (proc && !proc.killed) proc.kill(); } catch {}
    }
    writeFileSync(join(evidenceDir, 'server.log'), serverLog.join(''));
    writeFileSync(join(evidenceDir, 'provider.log'), providerLog.join(''));
  }

  return {
    root,
    base,
    dataDir,
    workspaceRoot,
    evidenceDir,
    providerPort,
    serverPort,
    launch,
    session,
    health,
    serverPid: server.pid,
    providerPid: provider.pid,
    json,
    submit,
    approvePending,
    waitRun,
    createSession,
    checkpoints,
    restoreCheckpoint,
    continueSession,
    stop,
  };
}
