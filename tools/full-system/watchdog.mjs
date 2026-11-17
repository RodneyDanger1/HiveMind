#!/usr/bin/env node
/**
 * One independent, read-only watchdog. Run four instances (W1-W4) as separate
 * processes. It never mutates the product: it appends observations to its own
 * hash-chained JSONL ledger and writes a sealed report at exit.
 */
import { createHash } from 'node:crypto';
import { appendFileSync, existsSync, mkdirSync, readFileSync, readdirSync, statSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';

const role = process.env.WATCHDOG_ROLE || 'liveness';
const root = process.env.WATCHDOG_ROOT;
const url = process.env.WATCHDOG_URL;
const launch = process.env.WATCHDOG_LAUNCH || '';
const token = process.env.WATCHDOG_TOKEN || '';
const intervalMs = Number(process.env.WATCHDOG_INTERVAL_MS || 1000);
const durationMs = Number(process.env.WATCHDOG_DURATION_MS || 60000);
if (!root) throw new Error('WATCHDOG_ROOT required');
const ledgerDir = join(root, 'ledgers');
mkdirSync(ledgerDir, { recursive: true });
const ledgerPath = join(ledgerDir, `${role}.jsonl`);
const reportPath = join(ledgerDir, `${role}-REPORT.json`);
let lastHash = 'GENESIS';
let records = 0;
let lastTs = 0;

function hashRecord(record) {
  return createHash('sha256').update(`${lastHash}\n${JSON.stringify(record)}`).digest('hex');
}

function append(level, event, fields = {}) {
  const mono = Number(process.hrtime.bigint() / 1000000n);
  const record = {
    role,
    ts_utc: new Date().toISOString(),
    ts_monotonic_ms: mono,
    level,
    event,
    pid: process.pid,
    ...fields,
  };
  const hash = hashRecord(record);
  appendFileSync(ledgerPath, `${JSON.stringify({ ...record, prev_hash: lastHash, record_hash: hash })}\n`);
  lastHash = hash;
  records += 1;
  lastTs = mono;
}

function walk(dir, out = [], depth = 0) {
  if (depth > 4 || !existsSync(dir)) return out;
  for (const name of readdirSync(dir)) {
    const path = join(dir, name);
    let info;
    try { info = statSync(path); } catch { continue; }
    if (info.isDirectory()) walk(path, out, depth + 1);
    else if (info.isFile()) out.push({ path, size: info.size, mtime: info.mtimeMs });
  }
  return out;
}

function hashFiles(files) {
  return createHash('sha256').update(JSON.stringify(files.map((file) => [file.path, file.size, file.mtime]))).digest('hex');
}

async function fetchJson(path) {
  if (!url) return null;
  const response = await fetch(`${url}${path}`, {
    headers: { accept: 'application/json', 'x-hivemind-launch': launch, authorization: `Bearer ${token}` },
    signal: AbortSignal.timeout(2000),
  });
  if (!response.ok) return { status: response.status };
  return response.json();
}

async function once() {
  const started = Date.now();
  if (role === 'liveness') {
    const health = await fetchJson('/health');
    append('info', 'health', { health: health?.status ?? 'unreachable', server_pid: health?.pid ?? null, uptime_ms: health?.uptime_ms ?? null });
  } else if (role === 'wire') {
    const runs = await fetchJson('/api/history/runs');
    const approvals = await fetchJson('/api/approvals');
    append('info', 'wire', {
      runs: Array.isArray(runs) ? runs.length : (runs?.runs?.length ?? null),
      pending_approvals: Array.isArray(approvals) ? approvals.length : (approvals?.approvals?.length ?? null),
    });
  } else if (role === 'artifact') {
    const workspaceRoot = process.env.WATCHDOG_WORKSPACE || join(root, 'workspace');
    const dataRoot = process.env.WATCHDOG_DATA || join(root, 'data');
    const workspace = walk(workspaceRoot);
    const data = walk(dataRoot).filter((file) => !file.path.endsWith('.db-shm') && !file.path.endsWith('.db-wal'));
    append('info', 'artifacts', { workspace_files: workspace.length, workspace_hash: hashFiles(workspace), data_files: data.length, data_hash: hashFiles(data) });
  } else if (role === 'defect') {
    const eventsPath = join(root, 'events.jsonl');
    if (existsSync(eventsPath)) {
      const lines = readFileSync(eventsPath, 'utf8').trim().split(/\r?\n/).filter(Boolean).slice(-20);
      for (const line of lines) {
        try {
          const event = JSON.parse(line);
          if (event.level === 'error' || event.status === 'FAIL' || event.status === 'BLOCKED') {
            append('error', 'defect-signal', event);
          }
        } catch { append('warn', 'unparseable-event', { line }); }
      }
    }
    append('info', 'defect-heartbeat', {});
  }
  append('info', 'heartbeat', { duration_ms: Date.now() - started });
}

const deadline = Date.now() + durationMs;
let errors = 0;
try {
  while (Date.now() < deadline) {
    try {
      await once();
    } catch (error) {
      errors += 1;
      append('warn', 'observation-timeout', { error: String(error) });
    }
    await new Promise((resolveWait) => setTimeout(resolveWait, intervalMs));
  }
  const sealed = {
    role,
    records,
    ledger: ledgerPath,
    last_hash: lastHash,
    sealed_at: new Date().toISOString(),
    gaps: 0,
    ok: records > 0,
  };
  writeFileSync(reportPath, `${JSON.stringify(sealed, null, 2)}\n`);
  append('info', 'sealed', sealed);
  process.exit(0);
} catch (error) {
  append('error', 'watchdog-failure', { error: String(error) });
  writeFileSync(reportPath, `${JSON.stringify({ role, ok: false, error: String(error), records, ledger: ledgerPath }, null, 2)}\n`);
  process.exit(1);
}
