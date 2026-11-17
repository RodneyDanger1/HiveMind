#!/usr/bin/env node
/**
 * One independent, read-only watchdog. Run four instances (W1-W4) as separate
 * processes. It never mutates the product: it appends observations to its own
 * hash-chained JSONL ledger and writes a sealed report at exit.
 *
 * ── Ledger record shape ──────────────────────────────────────────────────────
 * { role, ts_utc, ts_monotonic_ms, level, event, pid, ...fields,
 *   prev_hash, record_hash }
 * record_hash = sha256(`${prev_hash}\n${JSON.stringify(recordWithoutChainFields)}`)
 * The first record of a ledger has prev_hash = "GENESIS". A second run against
 * the same root resumes the existing chain (it does not fork it) and refuses to
 * append to a chain that no longer verifies.
 *
 * ── Seal contract ────────────────────────────────────────────────────────────
 * The watchdog only reports PASS by writing `<role>-REPORT.json` atomically
 * (temp file + rename, fsynced) with `sealed: true`, `ok: true`, `exit_code: 0`
 * and `last_hash` equal to the hash of a final `sealed` ledger record. A process
 * killed before that point (SIGKILL, power loss) leaves a ledger with records
 * and NO sealed report — which is NOT PASS. `verifyWatchdogSeal()` below is the
 * acceptance function; a bare "the ledger file has bytes" check is not one.
 *
 * ── Failure policy ───────────────────────────────────────────────────────────
 * A single observation timeout/error is a `warn` record and the run continues.
 * WATCHDOG_MAX_CONSECUTIVE_FAILURES consecutive failures (default 5) mark the
 * watchdog failed: it seals ok:false with failure_code
 * WATCHDOG_CONSECUTIVE_OBSERVATION_FAILURES and exits 1. A successful
 * observation resets the counter.
 *
 * ── Heartbeat gaps ───────────────────────────────────────────────────────────
 * The gap between consecutive heartbeat records is measured against
 * WATCHDOG_GAP_MS (default max(4 x interval, 1200)). A larger gap is recorded as
 * a `heartbeat-gap` warning, counted in the report (`gaps`, `max_gap_ms`) and is
 * a cadence defect, not by itself a failure.
 *
 * Env: WATCHDOG_ROOT (required), WATCHDOG_ROLE, WATCHDOG_URL, WATCHDOG_LAUNCH,
 * WATCHDOG_TOKEN, WATCHDOG_INTERVAL_MS, WATCHDOG_DURATION_MS,
 * WATCHDOG_TIMEOUT_MS, WATCHDOG_MAX_CONSECUTIVE_FAILURES, WATCHDOG_GAP_MS,
 * WATCHDOG_SEAL_GRACE_MS, WATCHDOG_WORKSPACE, WATCHDOG_DATA.
 */
import { createHash } from 'node:crypto';
import {
  appendFileSync, closeSync, existsSync, fsyncSync, mkdirSync, openSync,
  readFileSync, readdirSync, renameSync, statSync, writeFileSync,
} from 'node:fs';
import { Agent as HttpAgent, request as httpRequest } from 'node:http';
import { Agent as HttpsAgent, request as httpsRequest } from 'node:https';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';

const ROLE_PATTERN = /^[a-z][a-z0-9-]{0,31}$/;
const CHAIN_FIELDS = ['prev_hash', 'record_hash'];
const MAX_RESPONSE_BYTES = 4 * 1024 * 1024;

/** sha256 over the chain linkage and the record body, exactly as written. */
export function recordHash(prevHash, record) {
  return createHash('sha256').update(`${prevHash}\n${JSON.stringify(record)}`).digest('hex');
}

function stripChainFields(entry) {
  const body = {};
  for (const key of Object.keys(entry)) {
    if (!CHAIN_FIELDS.includes(key)) body[key] = entry[key];
  }
  return body;
}

/**
 * Verify a whole ledger text: per-record self-hash and per-record linkage.
 * Returns every defect with its 1-based line, never just the first one.
 */
export function verifyLedgerText(text) {
  const errors = [];
  const entries = [];
  const lines = String(text).split(/\r?\n/).filter((line) => line.trim() !== '');
  let expectedPrev = 'GENESIS';
  let lastHash = 'GENESIS';
  for (let index = 0; index < lines.length; index += 1) {
    const line = index + 1;
    let entry;
    try {
      entry = JSON.parse(lines[index]);
    } catch (error) {
      errors.push({ line, code: 'LEDGER_JSON_INVALID', detail: String(error?.message ?? error) });
      continue;
    }
    if (entry === null || typeof entry !== 'object' || Array.isArray(entry)) {
      errors.push({ line, code: 'LEDGER_RECORD_NOT_OBJECT', detail: typeof entry });
      continue;
    }
    const body = stripChainFields(entry);
    const recordedPrev = entry.prev_hash;
    const recordedHash = entry.record_hash;
    if (typeof recordedPrev !== 'string' || typeof recordedHash !== 'string') {
      errors.push({ line, code: 'LEDGER_CHAIN_FIELDS_MISSING', detail: `prev=${typeof recordedPrev} hash=${typeof recordedHash}` });
    } else {
      const recomputed = recordHash(recordedPrev, body);
      if (recomputed !== recordedHash) {
        errors.push({ line, code: 'LEDGER_RECORD_HASH_MISMATCH', detail: `recorded ${recordedHash.slice(0, 16)}… recomputed ${recomputed.slice(0, 16)}…` });
      }
      if (recordedPrev !== expectedPrev) {
        errors.push({ line, code: 'LEDGER_PREV_HASH_MISMATCH', detail: `expected ${expectedPrev.slice(0, 16)}… found ${recordedPrev.slice(0, 16)}…` });
      }
      expectedPrev = recordedHash;
      lastHash = recordedHash;
    }
    entries.push(entry);
  }
  return { ok: errors.length === 0, errors, records: entries.length, lastHash, entries };
}

function readJsonFile(path) {
  return JSON.parse(readFileSync(path, 'utf8'));
}

function writeFileAtomic(path, text) {
  const tmp = `${path}.tmp`;
  const fd = openSync(tmp, 'w');
  try {
    writeFileSync(fd, text);
    fsyncSync(fd);
  } finally {
    closeSync(fd);
  }
  renameSync(tmp, path);
}

/**
 * Acceptance function for one watchdog role. PASS requires a verified chain, a
 * final `sealed` record, and a sealed report that agrees with it. Anything else
 * (missing report, missing seal, ok:false, non-zero exit, seal overdue) is NOT
 * PASS and comes back with a named reason list.
 */
export function verifyWatchdogSeal({ root, role = 'liveness', now = Date.now() } = {}) {
  if (!root) throw new Error('verifyWatchdogSeal requires { root }');
  if (!ROLE_PATTERN.test(String(role))) throw new Error(`verifyWatchdogSeal: invalid role ${JSON.stringify(role)}`);
  const reasons = [];
  const ledgerPath = join(root, 'ledgers', `${role}.jsonl`);
  const reportPath = join(root, 'ledgers', `${role}-REPORT.json`);
  const ledgerExists = existsSync(ledgerPath);
  const reportExists = existsSync(reportPath);

  let report = null;
  let chain = { ok: false, errors: [], records: 0, lastHash: null, entries: [] };
  if (!ledgerExists) {
    reasons.push({ code: 'LEDGER_MISSING', detail: ledgerPath });
  } else {
    chain = verifyLedgerText(readFileSync(ledgerPath, 'utf8'));
    if (chain.records === 0) reasons.push({ code: 'LEDGER_EMPTY', detail: ledgerPath });
    if (!chain.ok) reasons.push({ code: 'LEDGER_CHAIN_INVALID', detail: chain.errors.slice(0, 5) });
  }

  if (!reportExists) {
    reasons.push({ code: 'REPORT_MISSING', detail: reportPath });
  } else {
    try {
      report = readJsonFile(reportPath);
    } catch (error) {
      reasons.push({ code: 'REPORT_JSON_INVALID', detail: String(error?.message ?? error) });
    }
  }

  const last = chain.entries.at(-1) ?? null;
  const sealedRecord = last && last.event === 'sealed' ? last : null;
  if (!sealedRecord) {
    if (last) reasons.push({ code: 'LEDGER_NOT_SEALED', detail: `last event ${String(last.event)} at ${String(last.ts_utc)}` });
    else if (ledgerExists && chain.records === 0) reasons.push({ code: 'LEDGER_NOT_SEALED', detail: 'ledger has no records' });
    const start = chain.entries.find((entry) => entry.event === 'watchdog-start') ?? null;
    const requiredBy = typeof start?.required_seal_by_utc === 'string' ? start.required_seal_by_utc : null;
    if (requiredBy && !Number.isNaN(Date.parse(requiredBy)) && Date.parse(requiredBy) < now) {
      reasons.push({ code: 'SEAL_OVERDUE', detail: `watchdog started at ${start.ts_utc} and required a seal by ${requiredBy}` });
    }
  }

  if (report && typeof report === 'object' && !Array.isArray(report)) {
    if (report.sealed !== true) reasons.push({ code: 'REPORT_NOT_SEALED', detail: 'sealed !== true' });
    if (report.ok !== true) reasons.push({ code: 'WATCHDOG_FAILED', detail: `ok=${String(report.ok)} failure_code=${String(report.failure_code ?? null)} reason=${String(report.reason ?? null)}` });
    if (report.exit_code !== 0) reasons.push({ code: 'EXIT_CODE_NONZERO', detail: String(report.exit_code) });
    if (sealedRecord) {
      if (report.last_hash !== sealedRecord.record_hash) {
        reasons.push({ code: 'REPORT_HASH_MISMATCH', detail: `report ${String(report.last_hash)} ledger ${String(sealedRecord.record_hash)}` });
      }
      if (sealedRecord.ok !== report.ok) {
        reasons.push({ code: 'SEAL_REPORT_DISAGREE', detail: `sealed.ok=${String(sealedRecord.ok)} report.ok=${String(report.ok)}` });
      }
      if (sealedRecord.records !== report.records) {
        reasons.push({ code: 'SEAL_REPORT_DISAGREE', detail: `sealed.records=${String(sealedRecord.records)} report.records=${String(report.records)}` });
      }
    }
  }

  return {
    pass: reasons.length === 0,
    role,
    reasons,
    report,
    chain: { ok: chain.ok, records: chain.records, lastHash: chain.lastHash, errors: chain.errors },
  };
}

function monotonicMs() {
  return Number(process.hrtime.bigint() / 1000000n);
}

function describeError(error) {
  const name = error?.name || error?.constructor?.name || 'Error';
  const message = error?.message !== undefined ? String(error.message) : String(error);
  const code = error?.cause?.code ?? error?.code ?? null;
  const isTimeout = name === 'TimeoutError'
    || code === 'WATCHDOG_OBSERVATION_TIMEOUT'
    || /timed? ?out|aborted due to timeout/i.test(message);
  return { name, message, code, isTimeout };
}

class ConfigError extends Error {
  constructor(message) {
    super(message);
    this.name = 'ConfigError';
    this.code = 'WATCHDOG_CONFIG_INVALID';
  }
}

class WatchdogError extends Error {
  constructor(code, message) {
    super(message);
    this.name = 'WatchdogError';
    this.code = code;
  }
}

function readNumber(name, fallback, { min = 1, max = Number.MAX_SAFE_INTEGER } = {}) {
  const raw = process.env[name];
  if (raw === undefined || raw === '') return fallback;
  const value = Number(raw);
  if (!Number.isFinite(value) || value < min || value > max) {
    throw new ConfigError(`${name} must be a number in [${min}, ${max}], got ${JSON.stringify(raw)}`);
  }
  return value;
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

export function main() {
  const role = process.env.WATCHDOG_ROLE || 'liveness';
  const root = process.env.WATCHDOG_ROOT;
  if (!root) throw new ConfigError('WATCHDOG_ROOT required');
  if (!ROLE_PATTERN.test(role)) throw new ConfigError(`WATCHDOG_ROLE must match ${ROLE_PATTERN}, got ${JSON.stringify(role)}`);

  const url = process.env.WATCHDOG_URL || null;
  const launch = process.env.WATCHDOG_LAUNCH || '';
  const token = process.env.WATCHDOG_TOKEN || '';
  const intervalMs = readNumber('WATCHDOG_INTERVAL_MS', 1000, { min: 0, max: 3_600_000 });
  const durationMs = readNumber('WATCHDOG_DURATION_MS', 60000, { min: 1, max: 86_400_000 });
  const timeoutMs = readNumber('WATCHDOG_TIMEOUT_MS', 2000, { min: 1, max: 600_000 });
  const maxConsecutiveFailures = Math.floor(readNumber('WATCHDOG_MAX_CONSECUTIVE_FAILURES', 5, { min: 1, max: 1000 }));
  const gapThresholdMs = readNumber('WATCHDOG_GAP_MS', Math.max(intervalMs * 4, 1200), { min: 1, max: 86_400_000 });
  const sealGraceMs = readNumber('WATCHDOG_SEAL_GRACE_MS', 5000, { min: 0, max: 600_000 });

  const ledgerDir = join(root, 'ledgers');
  mkdirSync(ledgerDir, { recursive: true });
  const ledgerPath = join(ledgerDir, `${role}.jsonl`);
  const reportPath = join(ledgerDir, `${role}-REPORT.json`);

  const state = {
    lastHash: 'GENESIS',
    written: 0,
    priorRecords: 0,
    observations: 0,
    heartbeats: 0,
    failures: 0,
    timeouts: 0,
    warnings: 0,
    gaps: 0,
    maxGapMs: 0,
    consecutiveFailures: 0,
    maxConsecutiveFailuresSeen: 0,
    lastHeartbeatMono: null,
    sealed: false,
  };

  if (existsSync(ledgerPath) && statSync(ledgerPath).size > 0) {
    const existing = verifyLedgerText(readFileSync(ledgerPath, 'utf8'));
    if (!existing.ok || existing.records === 0) {
      const report = {
        role,
        ok: false,
        sealed: true,
        exit_code: 1,
        failure_code: 'WATCHDOG_LEDGER_CHAIN_INVALID',
        reason: `existing ledger does not verify: ${JSON.stringify(existing.errors.slice(0, 3))}`,
        records: 0,
        ledger_records: existing.records,
        ledger: ledgerPath,
        last_hash: null,
        gaps: 0,
        max_gap_ms: 0,
        sealed_at: new Date().toISOString(),
      };
      writeFileAtomic(reportPath, `${JSON.stringify(report, null, 2)}\n`);
      process.stderr.write(`watchdog ${role}: existing ledger chain invalid, refusing to append\n`);
      process.exit(1);
    }
    state.priorRecords = existing.records;
    state.lastHash = existing.lastHash;
  }

  function append(level, event, fields = {}) {
    const record = {
      role,
      ts_utc: new Date().toISOString(),
      ts_monotonic_ms: monotonicMs(),
      level,
      event,
      pid: process.pid,
      ...fields,
    };
    const hash = recordHash(state.lastHash, record);
    appendFileSync(ledgerPath, `${JSON.stringify({ ...record, prev_hash: state.lastHash, record_hash: hash })}\n`);
    state.lastHash = hash;
    state.written += 1;
    return { ...record, prev_hash: null, record_hash: hash };
  }

  function totalRecords() {
    return state.priorRecords + state.written;
  }

  function seal({ ok, exitCode, failureCode = null, reason = null }) {
    if (state.sealed) return null;
    state.sealed = true;
    const sealedRecord = append(ok ? 'info' : 'error', 'sealed', {
      ok,
      exit_code: exitCode,
      failure_code: failureCode,
      reason,
      gaps: state.gaps,
      max_gap_ms: state.maxGapMs,
      records: totalRecords() + 1,
      exit_by: ok ? 'duration-elapsed' : 'failure',
    });
    const report = {
      role,
      ok,
      sealed: true,
      exit_code: exitCode,
      failure_code: failureCode,
      reason,
      records: totalRecords(),
      ledger_records: totalRecords(),
      ledger: ledgerPath,
      chain_from: state.priorRecords > 0 ? 'resumed' : 'GENESIS',
      last_hash: sealedRecord.record_hash,
      gaps: state.gaps,
      max_gap_ms: state.maxGapMs,
      gap_threshold_ms: gapThresholdMs,
      observations: state.observations,
      heartbeats: state.heartbeats,
      failures: state.failures,
      timeouts: state.timeouts,
      warnings: state.warnings,
      consecutive_failures: state.consecutiveFailures,
      max_consecutive_failures_seen: state.maxConsecutiveFailuresSeen,
      max_consecutive_failures_allowed: maxConsecutiveFailures,
      interval_ms: intervalMs,
      duration_ms: durationMs,
      timeout_ms: timeoutMs,
      seal_grace_ms: sealGraceMs,
      started_at: new Date(startedAtMs).toISOString(),
      sealed_at: new Date().toISOString(),
      observed_duration_ms: Date.now() - startedAtMs,
      required_seal_by_utc: requiredSealBy,
      pid: process.pid,
    };
    writeFileAtomic(reportPath, `${JSON.stringify(report, null, 2)}\n`);
    return report;
  }

  function finish({ ok, exitCode, failureCode = null, reason = null }) {
    try {
      seal({ ok, exitCode, failureCode, reason });
    } catch (error) {
      process.stderr.write(`watchdog ${role}: seal failed: ${String(error?.stack ?? error)}\n`);
      process.exit(1);
    }
    process.exit(exitCode);
  }

  process.on('uncaughtException', (error) => {
    const info = describeError(error);
    finish({ ok: false, exitCode: 1, failureCode: 'WATCHDOG_UNCAUGHT_EXCEPTION', reason: `${info.name}: ${info.message}` });
  });
  process.on('unhandledRejection', (error) => {
    const info = describeError(error);
    finish({ ok: false, exitCode: 1, failureCode: 'WATCHDOG_UNHANDLED_REJECTION', reason: `${info.name}: ${info.message}` });
  });
  for (const signal of ['SIGINT', 'SIGTERM']) {
    process.on(signal, () => finish({ ok: false, exitCode: 1, failureCode: 'WATCHDOG_SIGNALLED', reason: signal }));
  }

  // One socket per observation (keepAlive: false). A pooled connection that a
  // previous abort left half-dead must never make a later observation time out
  // without having reached the product: a timeout record has to mean "this
  // request produced no response", not "the pool was poisoned".
  const plainAgent = new HttpAgent({ keepAlive: false });
  const secureAgent = new HttpsAgent({ keepAlive: false });

  function fetchJson(path) {
    if (!url) return Promise.resolve(null);
    const target = new URL(`${url}${path}`);
    const secure = target.protocol === 'https:';
    const send = secure ? httpsRequest : httpRequest;
    return new Promise((resolve, reject) => {
      let settled = false;
      const settle = (fn, value) => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        fn(value);
      };
      const req = send(target, {
        method: 'GET',
        agent: secure ? secureAgent : plainAgent,
        headers: { accept: 'application/json', 'x-hivemind-launch': launch, authorization: `Bearer ${token}` },
      }, (response) => {
        const status = response.statusCode ?? 0;
        if (status < 200 || status >= 300) {
          response.resume();
          settle(resolve, { status });
          return;
        }
        let body = '';
        let bytes = 0;
        response.setEncoding('utf8');
        response.on('data', (chunk) => {
          bytes += Buffer.byteLength(chunk);
          if (bytes > MAX_RESPONSE_BYTES) {
            const error = new Error(`response exceeded ${MAX_RESPONSE_BYTES} bytes for ${path}`);
            error.name = 'Error';
            error.code = 'WATCHDOG_RESPONSE_TOO_LARGE';
            req.destroy(error);
            settle(reject, error);
            return;
          }
          body += chunk;
        });
        response.on('end', () => {
          try {
            settle(resolve, JSON.parse(body));
          } catch (error) {
            const invalid = new Error(`invalid JSON from ${path}: ${String(error?.message ?? error)}`);
            invalid.name = 'Error';
            invalid.code = 'WATCHDOG_RESPONSE_INVALID_JSON';
            settle(reject, invalid);
          }
        });
        response.on('error', (error) => settle(reject, error));
      });
      const timer = setTimeout(() => {
        const error = new Error(`observation timed out after ${timeoutMs}ms: GET ${path}`);
        error.name = 'TimeoutError';
        error.code = 'WATCHDOG_OBSERVATION_TIMEOUT';
        settle(reject, error);
        req.destroy(error);
      }, timeoutMs);
      timer.unref();
      req.on('error', (error) => settle(reject, error));
      req.end();
    });
  }

  async function observeOnce() {
    if (role === 'liveness') {
      const health = await fetchJson('/health');
      const healthStatus = typeof health?.status === 'string' || typeof health?.status === 'number' ? health.status : null;
      append('info', 'health', { health: healthStatus, server_pid: health?.pid ?? null, uptime_ms: health?.uptime_ms ?? null });
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
    // The gap is the interval between consecutive heartbeat records — exactly
    // the cadence a reader of the ledger sees. It is measured here, at the
    // heartbeat, not between the end of one observation and the start of the
    // next (which would only ever report the sleep duration).
    const heartbeatMono = monotonicMs();
    if (state.lastHeartbeatMono !== null) {
      const gapMs = heartbeatMono - state.lastHeartbeatMono;
      if (gapMs > gapThresholdMs) {
        state.gaps += 1;
        state.maxGapMs = Math.max(state.maxGapMs, gapMs);
        append('warn', 'heartbeat-gap', { gap_ms: gapMs, threshold_ms: gapThresholdMs });
      }
    }
    append('info', 'heartbeat', { duration_ms: Date.now() - iterationStart });
    state.heartbeats += 1;
    state.lastHeartbeatMono = heartbeatMono;
  }

  const startedAtMs = Date.now();
  const deadline = startedAtMs + durationMs;
  const requiredSealBy = new Date(deadline + intervalMs + timeoutMs + sealGraceMs).toISOString();
  let iterationStart = startedAtMs;

  append('info', 'watchdog-start', {
    interval_ms: intervalMs,
    duration_ms: durationMs,
    timeout_ms: timeoutMs,
    gap_threshold_ms: gapThresholdMs,
    max_consecutive_failures: maxConsecutiveFailures,
    seal_grace_ms: sealGraceMs,
    required_seal_by_utc: requiredSealBy,
    chain_from: state.priorRecords > 0 ? 'resumed' : 'GENESIS',
    ledger_records_before: state.priorRecords,
  });

  (async () => {
    while (Date.now() < deadline) {
      iterationStart = Date.now();
      try {
        await observeOnce();
        state.observations += 1;
        state.consecutiveFailures = 0;
      } catch (error) {
        const info = describeError(error);
        state.failures += 1;
        state.consecutiveFailures += 1;
        state.maxConsecutiveFailuresSeen = Math.max(state.maxConsecutiveFailuresSeen, state.consecutiveFailures);
        state.warnings += 1;
        if (info.isTimeout) state.timeouts += 1;
        append('warn', info.isTimeout ? 'observation-timeout' : 'observation-error', {
          error_name: info.name,
          error_message: info.message,
          error_code: info.code,
          aborted: info.isTimeout,
          consecutive_failures: state.consecutiveFailures,
          max_consecutive_failures: maxConsecutiveFailures,
          iteration_ms: Date.now() - iterationStart,
        });
        if (state.consecutiveFailures >= maxConsecutiveFailures) {
          throw new WatchdogError(
            'WATCHDOG_CONSECUTIVE_OBSERVATION_FAILURES',
            `${state.consecutiveFailures} consecutive observation failures (limit ${maxConsecutiveFailures}); last error ${info.name}: ${info.message}`,
          );
        }
      }
      await new Promise((resolveWait) => setTimeout(resolveWait, intervalMs));
    }
    finish({ ok: true, exitCode: 0 });
  })().catch((error) => {
    const info = describeError(error);
    const code = error?.code && String(error.code).startsWith('WATCHDOG_') ? error.code : 'WATCHDOG_RUNTIME_FAILURE';
    append('error', 'watchdog-failure', { error_name: info.name, error_message: info.message, error_code: info.code, failure_code: code });
    finish({ ok: false, exitCode: 1, failureCode: code, reason: `${info.name}: ${info.message}` });
  });
}

const invokedDirectly = process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;
if (invokedDirectly) {
  try {
    main();
  } catch (error) {
    const info = describeError(error);
    process.stderr.write(`watchdog: ${info.name}: ${info.message}\n`);
    process.exit(2);
  }
}
