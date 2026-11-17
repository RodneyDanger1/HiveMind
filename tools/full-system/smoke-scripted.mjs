#!/usr/bin/env node
/** First deterministic smoke: mock provider -> real server -> CLI submit -> run detail. */
import { spawn, spawnSync } from 'node:child_process';
import { mkdirSync, readFileSync, rmSync, writeFileSync, existsSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { randomUUID } from 'node:crypto';

const ROOT = resolve(import.meta.dirname, '..', '..');
const INSTALL = join(process.env.LOCALAPPDATA || '', 'Programs', 'HiveMind');
const SERVER = join(INSTALL, 'hiverouter.exe');
const CLI = join(ROOT, 'tools', 'hivemind-cli.mjs');
const CAMPAIGN = `smoke-${new Date().toISOString().replace(/[:.]/g, '-')}`;
const BASE = join(tmpdir(), 'hivemind-fs', CAMPAIGN);
const DATA = join(BASE, 'data');
const WS = join(BASE, 'workspace');
const EVID = join(BASE, 'evidence');
for (const dir of [DATA, WS, EVID, join(WS, 'golden')]) mkdirSync(dir, { recursive: true });

function freePort() {
  return new Promise((resolvePort) => {
    const s = createServer();
    s.listen(0, '127.0.0.1', () => {
      const p = s.address().port;
      s.close(() => resolvePort(p));
    });
  });
}
import { createServer } from 'node:net';

const providerPort = await freePort();
const serverPort = await freePort();
const launch = randomUUID();
writeFileSync(join(DATA, 'launch.token'), launch, 'utf8');

const provider = spawn(process.execPath, [join(ROOT, 'tools', 'full-system', 'mock-provider.mjs')], {
  env: {
    ...process.env,
    MOCK_PROVIDER_PORT: String(providerPort),
    MOCK_PROVIDER_LOG: join(EVID, 'provider.jsonl'),
    MOCK_PROVIDER_MODE: process.env.MOCK_PROVIDER_MODE || 'single-leaf',
  },
  stdio: ['ignore', 'pipe', 'pipe'],
});
provider.stdout.on('data', (d) => process.stdout.write(`[provider] ${d}`));
provider.stderr.on('data', (d) => process.stderr.write(`[provider:err] ${d}`));

for (let i = 0; i < 50; i += 1) {
  try {
    const r = await fetch(`http://127.0.0.1:${providerPort}/health`);
    if (r.ok) break;
  } catch {}
  await new Promise((r) => setTimeout(r, 100));
}

const server = spawn(SERVER, [
  '--bind', `127.0.0.1:${serverPort}`,
  '--data-dir', DATA,
  '--workspace-root', WS,
], {
  env: {
    ...process.env,
    HIVEROUTER_LAUNCH_TOKEN: launch,
    HIVEROUTER_BASE_URL: `http://127.0.0.1:${providerPort}`,
    HIVEROUTER_WIRE_MODEL: 'scripted-model',
    HIVEROUTER_PROVIDER: 'on',
    DEEPSEEK_API_KEY: 'sk-scripted-offline',
    HIVEROUTER_SIDE_EFFECTS: '1',
  },
  stdio: ['ignore', 'pipe', 'pipe'],
});
server.stdout.on('data', (d) => process.stdout.write(`[server] ${d}`));
server.stderr.on('data', (d) => process.stderr.write(`[server:err] ${d}`));

async function stop() {
  for (const p of [server, provider]) {
    if (p && !p.killed) {
      try { p.kill(); } catch {}
    }
  }
  await new Promise((r) => setTimeout(r, 500));
}

try {
  let health = null;
  for (let i = 0; i < 120; i += 1) {
    try {
      const r = await fetch(`http://127.0.0.1:${serverPort}/health`);
      if (r.ok) { health = await r.json(); break; }
    } catch {}
    await new Promise((r) => setTimeout(r, 250));
  }
  if (!health) throw new Error('server health did not become ready');
  console.log('HEALTH', JSON.stringify({ status: health.status, schema: health.database?.schema_version || health.schema }));

  const sessionResponse = await fetch(`http://127.0.0.1:${serverPort}/api/session`, {
    headers: { 'x-hivemind-launch': launch },
  });
  if (!sessionResponse.ok) throw new Error(`session ${sessionResponse.status}`);
  const session = await sessionResponse.json();
  writeFileSync(join(EVID, 'session.json'), JSON.stringify(session, null, 2));

  const prompt = 'Read the workspace and report what the golden project contains.';
  const submitted = spawnSync(process.execPath, [CLI, 'submit', '--prompt', prompt, '--json'], {
    env: { ...process.env, HIVEROUTER_URL: `http://127.0.0.1:${serverPort}`, HIVEROUTER_DATA_DIR: DATA },
    encoding: 'utf8',
    timeout: 30_000,
  });
  writeFileSync(join(EVID, 'submit.stdout.txt'), submitted.stdout || '');
  writeFileSync(join(EVID, 'submit.stderr.txt'), submitted.stderr || '');
  console.log('SUBMIT_EXIT', submitted.status);
  console.log((submitted.stdout || '').slice(0, 2000));
  if (submitted.status !== 0) throw new Error('submit failed');

  let runId = null;
  try { runId = JSON.parse(submitted.stdout).run_id; } catch {}
  if (!runId) {
    const m = (submitted.stdout || '').match(/[0-9a-f]{8}-[0-9a-f-]{27,}/i);
    runId = m?.[0] ?? null;
  }
  console.log('RUN_ID', runId);
  if (!runId) throw new Error('no run id');

  const auth = {
    authorization: `Bearer ${session.token}`,
    'x-hivemind-launch': launch,
    accept: 'application/json',
  };
  let detail = null;
  for (let i = 0; i < 120; i += 1) {
    const r = await fetch(`http://127.0.0.1:${serverPort}/api/runs/${runId}/detail`, { headers: auth });
    if (r.ok) {
      detail = await r.json();
      if (detail.status === 'completed' || detail.status === 'failed' || detail.status === 'cancelled' || detail.status === 'interrupted') break;
    }
    const approvals = await fetch(`http://127.0.0.1:${serverPort}/api/approvals?run_id=${runId}`, { headers: auth });
    if (approvals.ok) {
      const body = await approvals.json();
      const items = body.approvals || body.items || [];
      for (const a of items) {
        const approval = spawnSync(process.execPath, [
          CLI, 'approve', '--approval', a.approval_id, '--nonce', a.nonce, '--yes', '--json',
        ], { env: { ...process.env, HIVEROUTER_URL: `http://127.0.0.1:${serverPort}`, HIVEROUTER_DATA_DIR: DATA }, encoding: 'utf8', timeout: 15_000 });
        console.log('APPROVE', a.tool || a.tool_name, approval.status);
      }
    }
    await new Promise((r) => setTimeout(r, 500));
  }
  writeFileSync(join(EVID, 'detail.json'), JSON.stringify(detail, null, 2));
  console.log('DETAIL', JSON.stringify({ status: detail?.status, task_count: detail?.task_count, error_code: detail?.error_code, usage: detail?.usage }));
} finally {
  await stop();
}
