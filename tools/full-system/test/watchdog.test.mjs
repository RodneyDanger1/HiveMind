/**
 * Watchdog ledger-truth suite.
 *
 * These are black-box tests over the real CLI: every case spawns
 * tools/full-system/watchdog.mjs as its own process against a real loopback
 * HTTP endpoint, then reads the ledger and the sealed report from disk. The
 * only test doubles are the external observation endpoint (permitted: HTTP
 * client boundary) and the process kill used to simulate a dead watchdog.
 *
 * Run: node --test tools/full-system/test/watchdog.test.mjs
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { createHash } from 'node:crypto';
import {
  cpSync, existsSync, mkdtempSync, readFileSync, statSync, writeFileSync,
} from 'node:fs';
import { createServer } from 'node:http';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

import { verifyLedgerText, verifyWatchdogSeal } from '../watchdog.mjs';

const WATCHDOG = join(dirname(fileURLToPath(import.meta.url)), '..', 'watchdog.mjs');
const ROLE = 'liveness';

// ── helpers ─────────────────────────────────────────────────────────────────

function makeRoot() {
  return mkdtempSync(join(tmpdir(), 'watchdog-test-'));
}

function ledgerPathOf(root, role = ROLE) {
  return join(root, 'ledgers', `${role}.jsonl`);
}

function reportPathOf(root, role = ROLE) {
  return join(root, 'ledgers', `${role}-REPORT.json`);
}

function readLedgerText(root, role = ROLE) {
  const path = ledgerPathOf(root, role);
  return existsSync(path) ? readFileSync(path, 'utf8') : '';
}

function readEntries(root, role = ROLE) {
  return readLedgerText(root, role).trim().split(/\r?\n/).filter(Boolean).map((line) => JSON.parse(line));
}

function readReport(root, role = ROLE) {
  const path = reportPathOf(root, role);
  return existsSync(path) ? JSON.parse(readFileSync(path, 'utf8')) : null;
}

/** Independent recomputation of the documented chain formula. */
function expectedHash(prevHash, entry) {
  const body = { ...entry };
  delete body.prev_hash;
  delete body.record_hash;
  return createHash('sha256').update(`${prevHash}\n${JSON.stringify(body)}`).digest('hex');
}

/**
 * mode: 'ok' | 'hang' | 'slow' | 'hang-at'
 * The ENDPOINT is the external dependency under test; it is a real HTTP server,
 * not a mock of the watchdog's own logic. `hang-at` hangs the 1-based request
 * numbers in `hangAt` and answers everything else, which makes a transient
 * timeout deterministic without relying on connection-pool behaviour.
 */
function startHealthServer(mode, options = {}) {
  const hits = { total: 0 };
  const server = createServer((req, res) => {
    hits.total += 1;
    const send = () => {
      if (res.destroyed || res.writableEnded) return;
      res.setHeader('content-type', 'application/json');
      res.end(JSON.stringify({ status: 'ok', pid: process.pid, uptime_ms: 5 }));
    };
    if (mode === 'ok') { send(); return; }
    if (mode === 'slow') { setTimeout(send, options.delayMs ?? 250); return; }
    if (mode === 'hang') { res.on('close', () => {}); return; }
    if (mode === 'hang-at') {
      if ((options.hangAt ?? []).includes(hits.total)) { res.on('close', () => {}); return; }
      send();
      return;
    }
    throw new Error(`unknown server mode ${mode}`);
  });
  return new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', () => {
      const { port } = server.address();
      resolve({
        port,
        url: `http://127.0.0.1:${port}`,
        hits,
        close: () => { try { server.closeAllConnections(); } catch { /* older runtime */ } server.close(); },
      });
    });
  });
}

function runWatchdog({ root, url, role = ROLE, env = {} }) {
  const child = spawn(process.execPath, [WATCHDOG], {
    env: {
      ...process.env,
      WATCHDOG_ROLE: role,
      WATCHDOG_ROOT: root,
      WATCHDOG_URL: url,
      ...env,
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let stdout = '';
  let stderr = '';
  child.stdout.on('data', (chunk) => { stdout += chunk; });
  child.stderr.on('data', (chunk) => { stderr += chunk; });
  const done = new Promise((resolve) => {
    child.on('close', (code, signal) => resolve({ code, signal, stdout, stderr }));
  });
  return { child, done };
}

async function waitFor(predicate, { timeoutMs = 4000, stepMs = 20, label = 'condition' } = {}) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, stepMs));
  }
  throw new Error(`waitFor timed out: ${label}`);
}

// ── tests ───────────────────────────────────────────────────────────────────

test('healthy run seals a report whose last_hash closes a verified hash chain', async () => {
  const server = await startHealthServer('ok');
  const root = makeRoot();
  try {
    const run = runWatchdog({
      root,
      url: server.url,
      env: {
        WATCHDOG_INTERVAL_MS: '60',
        WATCHDOG_DURATION_MS: '700',
        WATCHDOG_TIMEOUT_MS: '400',
        WATCHDOG_TOKEN: 'token-must-never-reach-the-ledger',
      },
    });
    const result = await run.done;
    assert.equal(result.code, 0, `expected exit 0, got ${result.code}: ${result.stderr}`);

    const text = readLedgerText(root);
    const entries = readEntries(root);
    assert.ok(entries.length >= 5, `expected several records, got ${entries.length}`);

    // Independent chain walk (does not trust the production verifier).
    let prev = 'GENESIS';
    for (const [index, entry] of entries.entries()) {
      assert.equal(entry.prev_hash, prev, `line ${index + 1} prev_hash linkage`);
      assert.equal(entry.record_hash, expectedHash(prev, entry), `line ${index + 1} record_hash recomputation`);
      prev = entry.record_hash;
    }
    assert.equal(entries[0].prev_hash, 'GENESIS');
    assert.equal(entries.at(-1).event, 'sealed', 'last record must be the seal');

    const productionChain = verifyLedgerText(text);
    assert.equal(productionChain.ok, true, JSON.stringify(productionChain.errors));
    assert.equal(productionChain.lastHash, entries.at(-1).record_hash);

    const report = readReport(root);
    assert.equal(report.sealed, true);
    assert.equal(report.ok, true);
    assert.equal(report.exit_code, 0);
    assert.equal(report.failure_code, null);
    assert.equal(report.chain_from, 'GENESIS');
    assert.equal(report.last_hash, entries.at(-1).record_hash);
    assert.equal(report.records, entries.length);
    assert.equal(report.ledger_records, entries.length);
    assert.equal(report.failures, 0);
    assert.equal(report.timeouts, 0);
    assert.equal(report.heartbeats, report.observations);

    const verdict = verifyWatchdogSeal({ root });
    assert.equal(verdict.pass, true, JSON.stringify(verdict.reasons));

    // Atomic seal leaves no temp file behind, and no token reaches the ledger.
    assert.equal(existsSync(`${reportPathOf(root)}.tmp`), false);
    assert.equal(text.includes('token-must-never-reach-the-ledger'), false);
    assert.equal(JSON.stringify(report).includes('token-must-never-reach-the-ledger'), false);
  } finally {
    server.close();
  }
});

test('tampering, truncation, or a forged report cannot verify', async () => {
  const server = await startHealthServer('ok');
  const root = makeRoot();
  try {
    const result = await runWatchdog({
      root,
      url: server.url,
      env: { WATCHDOG_INTERVAL_MS: '40', WATCHDOG_DURATION_MS: '400', WATCHDOG_TIMEOUT_MS: '300' },
    }).done;
    assert.equal(result.code, 0, result.stderr);

    const lines = readLedgerText(root).trim().split(/\r?\n/);
    assert.ok(lines.length >= 4);

    // (a) mutate a body field of a non-final record
    const edited = lines.slice();
    const victim = JSON.parse(edited[2]);
    victim.duration_ms = Number(victim.duration_ms ?? 0) + 1;
    edited[2] = JSON.stringify(victim);
    const bodyTamper = verifyLedgerText(edited.join('\n'));
    assert.equal(bodyTamper.ok, false);
    assert.ok(bodyTamper.errors.some((error) => error.code === 'LEDGER_RECORD_HASH_MISMATCH' && error.line === 3),
      JSON.stringify(bodyTamper.errors));

    // (b) delete a record, which must break linkage
    const removed = lines.slice();
    removed.splice(1, 1);
    const linkTamper = verifyLedgerText(removed.join('\n'));
    assert.equal(linkTamper.ok, false);
    assert.ok(linkTamper.errors.some((error) => error.code === 'LEDGER_PREV_HASH_MISMATCH'),
      JSON.stringify(linkTamper.errors));

    // (c) a forged report (ok:true, wrong last_hash) must not PASS
    const forgedRoot = makeRoot();
    cpSync(join(root, 'ledgers'), join(forgedRoot, 'ledgers'), { recursive: true });
    const forgedPath = reportPathOf(forgedRoot);
    const forged = JSON.parse(readFileSync(forgedPath, 'utf8'));
    forged.last_hash = 'f'.repeat(64);
    writeFileSync(forgedPath, `${JSON.stringify(forged, null, 2)}\n`);
    const forgedVerdict = verifyWatchdogSeal({ root: forgedRoot });
    assert.equal(forgedVerdict.pass, false);
    assert.ok(forgedVerdict.reasons.some((reason) => reason.code === 'REPORT_HASH_MISMATCH'),
      JSON.stringify(forgedVerdict.reasons));

    // (d) a report claiming success over a ledger the watchdog never sealed
    const unsealedRoot = makeRoot();
    cpSync(join(root, 'ledgers'), join(unsealedRoot, 'ledgers'), { recursive: true });
    const unsealedLedger = readLedgerText(unsealedRoot).trim().split(/\r?\n/).slice(0, -1).join('\n');
    writeFileSync(ledgerPathOf(unsealedRoot), `${unsealedLedger}\n`);
    const unsealedVerdict = verifyWatchdogSeal({ root: unsealedRoot });
    assert.equal(unsealedVerdict.pass, false);
    assert.ok(unsealedVerdict.reasons.some((reason) => reason.code === 'LEDGER_NOT_SEALED'),
      JSON.stringify(unsealedVerdict.reasons));
  } finally {
    server.close();
  }
});

test('a transient observation timeout is a warning and the run still seals PASS', async () => {
  const server = await startHealthServer('hang-at', { hangAt: [1, 4] });
  const root = makeRoot();
  try {
    const result = await runWatchdog({
      root,
      url: server.url,
      env: {
        WATCHDOG_INTERVAL_MS: '40',
        WATCHDOG_DURATION_MS: '1800',
        WATCHDOG_TIMEOUT_MS: '400',
        WATCHDOG_MAX_CONSECUTIVE_FAILURES: '5',
      },
    }).done;
    assert.equal(result.code, 0, `transient timeout must not fail the run: ${result.stderr}`);

    const entries = readEntries(root);
    const timeouts = entries.filter((entry) => entry.event === 'observation-timeout');
    assert.ok(timeouts.length >= 2, `expected 2+ timeout warnings, got ${timeouts.length}`);
    for (const entry of timeouts) {
      assert.equal(entry.level, 'warn');
      assert.equal(entry.aborted, true);
      assert.equal(entry.error_name, 'TimeoutError');
      assert.equal(entry.error_code, 'WATCHDOG_OBSERVATION_TIMEOUT');
      assert.equal(entry.max_consecutive_failures, 5);
    }

    // A success between the two transient failures proves the counter reset
    // rather than accumulated: both failures must report consecutive_failures of 1.
    const first = entries.findIndex((entry) => entry.event === 'observation-timeout');
    const last = entries.findLastIndex((entry) => entry.event === 'observation-timeout');
    assert.ok(entries.slice(first, last).some((entry) => entry.event === 'health'),
      'a successful observation must sit between the transient timeouts');
    assert.deepEqual(timeouts.map((entry) => entry.consecutive_failures), [1, 1]);

    const report = readReport(root);
    assert.equal(report.ok, true);
    assert.equal(report.sealed, true);
    assert.equal(report.exit_code, 0);
    assert.equal(report.timeouts, 2);
    assert.equal(report.failures, 2);
    assert.equal(report.consecutive_failures, 0, 'the last observation succeeded, so the counter is back to zero');
    assert.equal(report.max_consecutive_failures_seen, 1);
    assert.ok(report.max_consecutive_failures_seen < report.failures, 'failures were not consecutive');

    const verdict = verifyWatchdogSeal({ root });
    assert.equal(verdict.pass, true, JSON.stringify(verdict.reasons));
  } finally {
    server.close();
  }
});

test('a refused connection is an observation-error, not a timeout, and is still bounded', async () => {
  const probe = await startHealthServer('ok');
  const closedPort = probe.port;
  probe.close();
  const root = makeRoot();
  try {
    const result = await runWatchdog({
      root,
      url: `http://127.0.0.1:${closedPort}`,
      env: {
        WATCHDOG_INTERVAL_MS: '20',
        WATCHDOG_DURATION_MS: '15000',
        WATCHDOG_TIMEOUT_MS: '500',
        WATCHDOG_MAX_CONSECUTIVE_FAILURES: '2',
        WATCHDOG_SEAL_GRACE_MS: '0',
      },
    }).done;
    assert.notEqual(result.code, 0, 'a permanently unreachable endpoint must fail');

    const entries = readEntries(root);
    const errors = entries.filter((entry) => entry.event === 'observation-error');
    assert.equal(errors.length, 2, JSON.stringify(entries.map((entry) => entry.event)));
    for (const entry of errors) {
      assert.equal(entry.level, 'warn');
      assert.equal(entry.aborted, false);
      assert.equal(entry.error_code, 'ECONNREFUSED');
    }

    const report = readReport(root);
    assert.equal(report.ok, false);
    assert.equal(report.failure_code, 'WATCHDOG_CONSECUTIVE_OBSERVATION_FAILURES');
    assert.equal(report.timeouts, 0, 'a refused connection must not be recorded as a timeout');
    assert.equal(report.failures, 2);
    assert.equal(verifyWatchdogSeal({ root }).pass, false);
  } finally {
    /* server already closed above */
  }
});

test('bounded consecutive failures mark the watchdog failed and seal ok:false', async () => {
  const server = await startHealthServer('hang');
  const root = makeRoot();
  try {
    const result = await runWatchdog({
      root,
      url: server.url,
      env: {
        WATCHDOG_INTERVAL_MS: '20',
        WATCHDOG_DURATION_MS: '15000',
        WATCHDOG_TIMEOUT_MS: '100',
        WATCHDOG_MAX_CONSECUTIVE_FAILURES: '3',
        WATCHDOG_SEAL_GRACE_MS: '0',
      },
    }).done;
    assert.notEqual(result.code, 0, 'a dead endpoint must not exit 0');

    const entries = readEntries(root);
    const timeouts = entries.filter((entry) => entry.event === 'observation-timeout');
    assert.equal(timeouts.length, 3, 'the bound, not the duration, must stop the run');
    assert.equal(entries.at(-1).event, 'sealed');

    const report = readReport(root);
    assert.equal(report.ok, false);
    assert.equal(report.sealed, true);
    assert.equal(report.exit_code, 1);
    assert.equal(report.failure_code, 'WATCHDOG_CONSECUTIVE_OBSERVATION_FAILURES');
    assert.match(report.reason, /3 consecutive observation failures/);
    assert.equal(report.consecutive_failures, 3);
    assert.equal(report.max_consecutive_failures_seen, 3);
    assert.equal(report.last_hash, entries.at(-1).record_hash);
    assert.ok(report.observed_duration_ms < 5000,
      `must fail on the bound, not the clock; observed ${report.observed_duration_ms}ms`);

    const verdict = verifyWatchdogSeal({ root });
    assert.equal(verdict.pass, false);
    assert.ok(verdict.reasons.some((reason) => reason.code === 'WATCHDOG_FAILED'), JSON.stringify(verdict.reasons));
    assert.ok(verdict.reasons.some((reason) => reason.code === 'EXIT_CODE_NONZERO'), JSON.stringify(verdict.reasons));
  } finally {
    server.close();
  }
});

test('a heartbeat gap is detected and measured instead of being reported as zero', async () => {
  const server = await startHealthServer('slow', { delayMs: 250 });
  const root = makeRoot();
  try {
    const result = await runWatchdog({
      root,
      url: server.url,
      env: {
        WATCHDOG_INTERVAL_MS: '30',
        WATCHDOG_DURATION_MS: '900',
        WATCHDOG_TIMEOUT_MS: '1000',
        WATCHDOG_GAP_MS: '120',
      },
    }).done;
    assert.equal(result.code, 0, result.stderr);

    const entries = readEntries(root);
    const gapRecords = entries.filter((entry) => entry.event === 'heartbeat-gap');
    assert.ok(gapRecords.length >= 2, `expected heartbeat-gap records, got ${gapRecords.length}`);
    for (const entry of gapRecords) {
      assert.equal(entry.level, 'warn');
      assert.equal(entry.threshold_ms, 120);
      assert.ok(entry.gap_ms > entry.threshold_ms, `gap ${entry.gap_ms} must exceed threshold`);
    }

    const report = readReport(root);
    assert.equal(report.ok, true);
    assert.ok(report.gaps >= 2, `gaps must be counted, got ${report.gaps}`);
    assert.ok(report.max_gap_ms > 120, `max_gap_ms must be measured, got ${report.max_gap_ms}`);
    assert.equal(report.gap_threshold_ms, 120);
    assert.equal(report.timeouts, 0);

    const verdict = verifyWatchdogSeal({ root });
    assert.equal(verdict.pass, true, JSON.stringify(verdict.reasons));
  } finally {
    server.close();
  }
});

test('a dead watchdog cannot be reported as PASS by a ledger-exists check', async () => {
  const server = await startHealthServer('ok');
  const root = makeRoot();
  try {
    const run = runWatchdog({
      root,
      url: server.url,
      env: {
        WATCHDOG_INTERVAL_MS: '30',
        WATCHDOG_DURATION_MS: '1000',
        WATCHDOG_TIMEOUT_MS: '100',
        WATCHDOG_SEAL_GRACE_MS: '0',
      },
    });
    await waitFor(() => readEntries(root).length >= 2, { label: 'ledger to start' });
    run.child.kill('SIGKILL');
    const result = await run.done;
    assert.ok(result.signal === 'SIGKILL' || result.code !== 0, `kill did not take: ${JSON.stringify(result)}`);

    // The ledger is non-empty — the current harness predicate ("exists and bytes > 0").
    const ledgerStat = statSync(ledgerPathOf(root));
    const naiveLedgerOnlyCheck = ledgerStat.size > 0;
    assert.equal(naiveLedgerOnlyCheck, true, 'precondition: the naive check passes on a dead watchdog');

    // …yet nothing is sealed.
    assert.equal(existsSync(reportPathOf(root)), false, 'a killed watchdog must not leave a report');
    const entries = readEntries(root);
    assert.notEqual(entries.at(-1).event, 'sealed');
    assert.equal(entries.some((entry) => entry.event === 'sealed'), false);
    assert.equal(existsSync(`${reportPathOf(root)}.tmp`), false, 'no partial report may be left behind');

    const verdict = verifyWatchdogSeal({ root });
    assert.equal(verdict.pass, false, 'a dead watchdog must never PASS');
    const codes = verdict.reasons.map((reason) => reason.code);
    assert.ok(codes.includes('REPORT_MISSING'), JSON.stringify(codes));
    assert.ok(codes.includes('LEDGER_NOT_SEALED'), JSON.stringify(codes));

    // After the seal deadline the same ledger is also flagged as overdue, which
    // is what catches a hung-but-alive watchdog that can never seal.
    const start = entries.find((entry) => entry.event === 'watchdog-start');
    const requiredBy = Date.parse(start.required_seal_by_utc);
    await waitFor(() => Date.now() > requiredBy + 50, { label: 'seal deadline to pass' });
    const overdueVerdict = verifyWatchdogSeal({ root });
    assert.equal(overdueVerdict.pass, false);
    assert.ok(overdueVerdict.reasons.some((reason) => reason.code === 'SEAL_OVERDUE'),
      JSON.stringify(overdueVerdict.reasons));
  } finally {
    server.close();
  }
});

test('a second run resumes the chain instead of forking it', async () => {
  const server = await startHealthServer('ok');
  const root = makeRoot();
  const env = { WATCHDOG_INTERVAL_MS: '30', WATCHDOG_DURATION_MS: '400', WATCHDOG_TIMEOUT_MS: '300' };
  try {
    const first = await runWatchdog({ root, url: server.url, env }).done;
    assert.equal(first.code, 0, first.stderr);
    const firstEntries = readEntries(root);
    const firstReport = readReport(root);
    assert.equal(firstReport.records, firstEntries.length);

    const second = await runWatchdog({ root, url: server.url, env }).done;
    assert.equal(second.code, 0, second.stderr);
    const all = readEntries(root);
    assert.equal(all.length > firstEntries.length, true);
    assert.equal(all[firstEntries.length].prev_hash, firstEntries.at(-1).record_hash,
      'the resumed run must link to the previous seal');

    const chain = verifyLedgerText(readLedgerText(root));
    assert.equal(chain.ok, true, JSON.stringify(chain.errors));
    assert.equal(all.filter((entry) => entry.event === 'sealed').length, 2);
    assert.equal(all.filter((entry) => entry.event === 'watchdog-start').length, 2);

    const secondReport = readReport(root);
    assert.equal(secondReport.chain_from, 'resumed');
    assert.equal(secondReport.records, all.length);
    assert.equal(secondReport.ledger_records, all.length);
    assert.equal(secondReport.last_hash, all.at(-1).record_hash);

    const verdict = verifyWatchdogSeal({ root });
    assert.equal(verdict.pass, true, JSON.stringify(verdict.reasons));
  } finally {
    server.close();
  }
});

test('an invalid role or an invalid bound is refused at the boundary', async () => {
  const root = makeRoot();
  const traversal = await runWatchdog({
    root,
    url: 'http://127.0.0.1:9',
    role: '../escape',
    env: { WATCHDOG_DURATION_MS: '200' },
  }).done;
  assert.equal(traversal.code, 2, traversal.stderr);
  assert.match(traversal.stderr, /WATCHDOG_ROLE/);
  assert.equal(existsSync(join(root, 'ledgers')), false);

  const badBound = await runWatchdog({
    root,
    url: 'http://127.0.0.1:9',
    env: { WATCHDOG_INTERVAL_MS: 'not-a-number', WATCHDOG_DURATION_MS: '200' },
  }).done;
  assert.equal(badBound.code, 2, badBound.stderr);
  assert.match(badBound.stderr, /WATCHDOG_INTERVAL_MS/);

  const zeroBound = await runWatchdog({
    root,
    url: 'http://127.0.0.1:9',
    env: { WATCHDOG_DURATION_MS: '200', WATCHDOG_MAX_CONSECUTIVE_FAILURES: '0' },
  }).done;
  assert.equal(zeroBound.code, 2, zeroBound.stderr);
  assert.match(zeroBound.stderr, /WATCHDOG_MAX_CONSECUTIVE_FAILURES/);
});
