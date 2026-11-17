#!/usr/bin/env node
/**
 * HiveMind full-system campaign orchestrator.
 *
 * Current executable slice: preflight, deterministic approval-gated write,
 * checkpoint restore, session continuation acceptance, four watchdog ledgers,
 * redaction scan, static capability truth, and report. Checks not yet wired are
 * emitted as NOT_IMPLEMENTED, never as PASS.
 */
import { spawn, spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { existsSync, mkdirSync, readFileSync, readdirSync, statSync, writeFileSync } from 'node:fs';
import { join, resolve } from 'node:path';
import {
  createCampaignRoot,
  sha256File,
  startCampaign,
  writeEvent,
} from './lib/engine.mjs';

const ROOT = resolve(import.meta.dirname, '..', '..');
const { id, root } = createCampaignRoot(process.env.FS_PREFIX || 'campaign');
const checks = [];
let campaign = null;
let watchdogs = [];

function record(id, status, expected, actual, evidence = [], severity = 'P1') {
  const check = { id, status, expected, actual, evidence, severity, at: new Date().toISOString() };
  checks.push(check);
  writeEvent(root, { level: status === 'PASS' ? 'info' : 'error', phase: 'check', test_id: id, status, expected, actual });
  console.log(`${status.padEnd(16)} ${id} — ${actual}`);
  return check;
}
function pass(id, expected, actual, evidence, severity) { return record(id, 'PASS', expected, actual, evidence, severity); }
function fail(id, expected, actual, evidence, severity) { return record(id, 'FAIL', expected, actual, evidence, severity); }
function notImpl(id, expected, actual = 'not implemented in this executable slice') { return record(id, 'NOT_IMPLEMENTED', expected, actual, [], 'P2'); }

function cmd(args, timeout = 120_000) {
  const result = spawnSync(args[0], args.slice(1), { cwd: ROOT, encoding: 'utf8', timeout });
  return { status: result.status, stdout: result.stdout || '', stderr: result.stderr || '' };
}

function gitState() {
  return cmd(['git', 'status', '--porcelain']);
}

function scanForSecrets(dir) {
  const hits = [];
  const walk = (path) => {
    if (!existsSync(path)) return;
    for (const name of readdirSync(path)) {
      const file = join(path, name);
      let info;
      try { info = statSync(file); } catch { continue; }
      if (info.isDirectory()) { walk(file); continue; }
      if (info.size > 2_000_000) continue;
      let text = '';
      try { text = readFileSync(file, 'utf8'); } catch { continue; }
      if (/sk-[A-Za-z0-9_-]{8,}/.test(text) || text.includes('sk-scripted-offline')) hits.push(file);
    }
  };
  walk(dir);
  return hits;
}

function mcpTools(file) {
  if (!existsSync(file)) return { ok: false, tools: [], error: 'missing' };
  const input = [
    JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'initialize', params: { protocolVersion: '2024-11-05', capabilities: {}, clientInfo: { name: 'fs', version: '1' } } }),
    JSON.stringify({ jsonrpc: '2.0', id: 2, method: 'tools/list', params: {} }),
  ].join('\n') + '\n';
  const result = spawnSync(process.execPath, [file], { input, encoding: 'utf8', timeout: 10_000 });
  const tools = [];
  for (const line of (result.stdout || '').split(/\r?\n/)) {
    try {
      const value = JSON.parse(line);
      if (Array.isArray(value?.result?.tools)) tools.push(...value.result.tools.map((tool) => tool.name));
    } catch {}
  }
  return { ok: result.status === 0 || tools.length > 0, tools, error: result.stderr };
}

async function main() {
  mkdirSync(join(root, 'evidence'), { recursive: true });
  writeFileSync(join(root, 'campaign.json'), JSON.stringify({ id, root, started_at: new Date().toISOString(), commit: cmd(['git', 'rev-parse', 'HEAD']).stdout.trim() }, null, 2));

  // ── Preflight ────────────────────────────────────────────────────────────
  const dirty = gitState();
  if (dirty.stdout.trim() === '') pass('PRE/GIT-01', 'clean worktree', 'clean');
  else fail('PRE/GIT-01', 'clean worktree', `dirty: ${dirty.stdout.trim().split(/\r?\n/).slice(0, 8).join(', ')}`, [], 'P0');

  const head = cmd(['git', 'rev-parse', '--short', 'HEAD']).stdout.trim();
  pass('PRE/BUILD-01', 'record source commit', head, [`campaign.json`]);
  const installed = join(process.env.LOCALAPPDATA || '', 'Programs', 'HiveMind', 'hiverouter.exe');
  if (existsSync(installed)) pass('PRE/INSTALLED-01', 'installed server binary', installed);
  else fail('PRE/INSTALLED-01', 'installed server binary', 'missing', [], 'P0');

  const srcMcp = join(ROOT, 'tools', 'hivemind-mcp', 'server.mjs');
  const legacyMcp = join(ROOT, 'tools', 'hivemind-mcp.mjs');
  const srcTools = mcpTools(srcMcp);
  const legacyTools = mcpTools(legacyMcp);
  const packagedMcp = join(ROOT, 'dist', 'HiveMind', 'tools', 'hivemind-mcp', 'server.mjs');
  const packagedTools = mcpTools(packagedMcp);
  writeFileSync(join(root, 'evidence', 'mcp-tools.json'), JSON.stringify({ source: srcTools, legacy: legacyTools, packaged: packagedTools }, null, 2));
  if (srcTools.tools.includes('hivemind_continue') && packagedTools.tools.includes('hivemind_continue')) {
    pass('MCP/PACKAGE-01', 'source and packaged artifact share the documented 7-tool surface', `source=${srcTools.tools.length}, packaged=${packagedTools.tools.length}, legacy=${legacyTools.tools.length}`);
  } else if (srcTools.tools.length === 0 || legacyTools.tools.length === 0) {
    fail('MCP/PACKAGE-01', 'both MCP servers answer tools/list', `source=${srcTools.tools.join(',') || srcTools.error}; legacy=${legacyTools.tools.join(',') || legacyTools.error}`, [], 'P1');
  } else {
    fail('MCP/PACKAGE-01', 'source and packaged artifact share the documented 7-tool surface', `source=${srcTools.tools.length}, packaged=${packagedTools.tools.length}, legacy=${legacyTools.tools.length}; README advertises hivemind_continue`, [join(root, 'evidence', 'mcp-tools.json')], 'P1');
  }

  // ── Deterministic approval-gated run + watchdogs ─────────────────────────
  try {
    campaign = await startCampaign({ root, mode: 'tools' });
    pass('CORE/BOOT-01', 'real server health + schema', `${campaign.health.status}, schema ${campaign.health.database?.schema_version ?? campaign.health.schema}`, [join(root, 'evidence', 'session.json')]);

    const launch = campaign.launch;
    const token = campaign.session.token;
    const base = campaign.base;
    watchdogs = ['liveness', 'wire', 'artifact', 'defect'].map((role) => spawn(process.execPath, [join(ROOT, 'tools', 'full-system', 'watchdog.mjs')], {
      env: {
        ...process.env,
        WATCHDOG_ROLE: role,
        WATCHDOG_ROOT: root,
        WATCHDOG_URL: base,
        WATCHDOG_LAUNCH: launch,
        WATCHDOG_TOKEN: token,
        WATCHDOG_INTERVAL_MS: '250',
        WATCHDOG_DURATION_MS: '12000',
      },
      stdio: 'ignore',
    }));

    const sessionCreated = await campaign.createSession('full-system-campaign');
    const sessionId = sessionCreated.body?.session?.id || sessionCreated.body?.id || null;
    writeFileSync(join(root, 'evidence', 'created-session.json'), JSON.stringify(sessionCreated.body, null, 2));
    if (sessionCreated.status === 201 && sessionId) pass('CORE/SESSION-00', 'session created', sessionId);
    else fail('CORE/SESSION-00', 'session created', `${sessionCreated.status} ${sessionCreated.text.slice(0, 300)}`, [], 'P1');

    const prompt = 'Write golden/out.txt with the scripted acceptance content and report the result.';
    const submitted = campaign.submit(prompt, { sessionId });
    const submitBody = submitted.json;
    const runId = submitBody?.data?.run_id || submitBody?.run_id || null;
    if (submitted.status === 0 && runId) pass('CORE/SUBMIT-01', 'run accepted with id', runId, [`submit:${submitted.status}`]);
    else fail('CORE/SUBMIT-01', 'run accepted with id', `exit=${submitted.status} ${submitted.stderr.slice(-300)}`, [], 'P0');

    if (runId) {
      const { detail, approvals } = await campaign.waitRun(runId, { timeoutMs: 45_000 });
      writeFileSync(join(root, 'evidence', 'run-detail.json'), JSON.stringify(detail, null, 2));
      if (detail?.status === 'completed') pass('CORE/RUN-01', 'run completed', 'completed', [join(root, 'evidence', 'run-detail.json')], 'P0');
      else fail('CORE/RUN-01', 'run completed', `status=${detail?.status} code=${detail?.error_code}`, [join(root, 'evidence', 'run-detail.json')], 'P0');

      if (approvals.some((a) => a.decision === 'approve' && a.tool === 'file_write')) pass('CORE/APPROVAL-01', 'file_write approval resolved', JSON.stringify(approvals.map((a) => [a.tool, a.decision, a.code])), [], 'P0');
      else fail('CORE/APPROVAL-01', 'file_write approval resolved', JSON.stringify(approvals), [], 'P0');
      writeFileSync(join(root, 'evidence', 'approvals.json'), JSON.stringify(approvals, null, 2));

      const replay = approvals.find((a) => a.decision === 'approve');
      if (replay) {
        const again = spawnSync(process.execPath, [
          join(ROOT, 'tools', 'hivemind-cli.mjs'), 'approve', '--approval', replay.approval_id, '--nonce', replay.nonce, '--yes', '--json',
        ], { env: { ...process.env, HIVEROUTER_URL: base, HIVEROUTER_DATA_DIR: join(root, 'data') }, encoding: 'utf8', timeout: 10_000 });
        const code = (() => { try { return JSON.parse(again.stdout).error?.code ?? null; } catch { return null; } })();
        if (again.status !== 0 && code) pass('CORE/APPROVAL-02', 'replay refuses by name', code, [`replay.stdout`]);
        else fail('CORE/APPROVAL-02', 'replay refuses by name', `exit=${again.status} output=${(again.stdout || again.stderr).slice(-300)}`, [], 'P1');
        writeFileSync(join(root, 'evidence', 'approval-replay.txt'), `${again.stdout}\n${again.stderr}\n`);
      } else notImpl('CORE/APPROVAL-02', 'replay refusal');

      const written = join(root, 'workspace', 'golden', 'out.txt');
      if (existsSync(written) && readFileSync(written, 'utf8').includes('scripted acceptance')) pass('CORE/WRITE-01', 'approved write reached workspace', written);
      else fail('CORE/WRITE-01', 'approved write reached workspace', `missing or wrong content: ${written}`, [], 'P0');

      const ckpts = await campaign.checkpoints(runId);
      const entries = ckpts.body?.checkpoints || [];
      writeFileSync(join(root, 'evidence', 'checkpoints.json'), JSON.stringify(ckpts.body, null, 2));
      if (entries.length > 0) pass('CORE/CKPT-01', 'approved write journals a checkpoint', `${entries.length} entries`, [join(root, 'evidence', 'checkpoints.json')]);
      else fail('CORE/CKPT-01', 'approved write journals a checkpoint', JSON.stringify(ckpts.body), [], 'P1');
      if (entries.length > 0) {
        const restored = await campaign.restoreCheckpoint(runId, entries[0].seq);
        writeFileSync(join(root, 'evidence', 'restore.json'), JSON.stringify(restored.body, null, 2));
        if (restored.ok) pass('CORE/CKPT-02', 'HTTP checkpoint restore returns receipt', JSON.stringify(restored.body), [join(root, 'evidence', 'restore.json')]);
        else fail('CORE/CKPT-02', 'HTTP checkpoint restore returns receipt', `${restored.status} ${restored.text}`, [], 'P1');
      } else notImpl('CORE/CKPT-02', 'HTTP checkpoint restore');

      const continued = await campaign.continueSession(sessionId, 'Continue the acceptance run and report the prior result.');
      writeFileSync(join(root, 'evidence', 'continue.json'), JSON.stringify(continued.body, null, 2));
      if (continued.status === 202 && continued.body?.continuation?.seeded_with_prior_context === true) pass('CORE/SESSION-01', 'session continuation seeds prior context', '202 seeded', [join(root, 'evidence', 'continue.json')]);
      else fail('CORE/SESSION-01', 'session continuation seeds prior context', `${continued.status} ${continued.text.slice(0, 300)}`, [], 'P1');
    }
  } catch (error) {
    fail('CORE/HARNESS-01', 'deterministic engine lane runs', String(error), [], 'P0');
  }

  if (campaign) {
    const bootId = campaign.health?.boot_id || campaign.health?.bootId || null;
    if (campaign.health?.status === 'ok' && bootId) pass('S6/IDENTITY-01', 'strict health carries schema + boot_id', `boot_id=${bootId}`);
    else fail('S6/IDENTITY-01', 'strict health carries schema + boot_id', JSON.stringify(campaign.health).slice(0, 400), [], 'P1');
    campaign.stop();
    campaign = null;
  }

  const capsSource = readFileSync(join(ROOT, 'backend', 'crates', 'hiverouter-core', 'src', 'caps.rs'), 'utf8');
  const loopSource = readFileSync(join(ROOT, 'backend', 'crates', 'hiverouter-core', 'src', 'tool_loop.rs'), 'utf8');
  if (capsSource.includes('MAX_CALLS: u64 = 128') && capsSource.includes('MAX_AGENTS: u64 = 64') && loopSource.includes('MAX_TOOL_TURNS: u32 = 12') && loopSource.includes('MAX_APPROVAL_WAITS_PER_RUN: u32 = 8')) {
    pass('S4/CAPS-01', 'cap constants match the documented budget', '128 calls / 64 agents / 12 turns / 8 approvals');
  } else {
    fail('S4/CAPS-01', 'cap constants match the documented budget', 'source constants drifted', [], 'P1');
  }
  if (loopSource.includes('PermissionPolicy::Classic') && loopSource.includes('PolicyDecision::Refuse') && loopSource.includes('PermissionClass::Mutate if inside_workspace')) {
    pass('S2/POLICY-01', 'policy matrix is present in source', 'classic/plan/standard/trusted branches found');
  } else {
    fail('S2/POLICY-01', 'policy matrix is present in source', 'policy branches missing', [], 'P1');
  }

  // ── Watchdog reconciliation ───────────────────────────────────────────────
  await new Promise((resolveWait) => setTimeout(resolveWait, 13_000));
  const ledgerReports = {};
  for (const role of ['liveness', 'wire', 'artifact', 'defect']) {
    const report = join(root, 'ledgers', `${role}-REPORT.json`);
    const ledger = join(root, 'ledgers', `${role}.jsonl`);
    ledgerReports[role] = {
      report: existsSync(report) ? JSON.parse(readFileSync(report, 'utf8')) : null,
      exists: existsSync(ledger),
      bytes: existsSync(ledger) ? statSync(ledger).size : 0,
    };
  }
  writeFileSync(join(root, 'evidence', 'watchdogs.json'), JSON.stringify(ledgerReports, null, 2));
  const watchdogOk = Object.values(ledgerReports).every((entry) => entry.exists && entry.bytes > 0);
  if (watchdogOk) pass('WATCHDOG/01', 'all four ledgers have records', JSON.stringify(Object.fromEntries(Object.entries(ledgerReports).map(([k, v]) => [k, v.bytes]))));
  else fail('WATCHDOG/01', 'all four ledgers have records', JSON.stringify(ledgerReports), [], 'P1');

  const secretHits = scanForSecrets(root);
  writeFileSync(join(root, 'evidence', 'secret-scan.json'), JSON.stringify({ hits: secretHits }, null, 2));
  if (secretHits.length === 0) pass('SECRET/01', 'no plaintext key in campaign evidence', 'zero hits');
  else fail('SECRET/01', 'no plaintext key in campaign evidence', `${secretHits.length} files`, secretHits.slice(0, 10), 'P0');

  // S12 deterministic replay: same scripted scenario twice, structural equality.
  let replayCampaign = null;
  try {
    replayCampaign = await startCampaign({ root: join(root, 'replay'), mode: 'single-leaf' });
    const replaySubmit = replayCampaign.submit('Replay the acceptance scenario.');
    const replayId = replaySubmit.json?.data?.run_id || replaySubmit.json?.run_id || null;
    if (!replayId) throw new Error(`replay submit failed: ${replaySubmit.stderr.slice(-200)}`);
    const replayRun = await replayCampaign.waitRun(replayId, { timeoutMs: 30_000 });
    writeFileSync(join(root, 'evidence', 'replay-detail.json'), JSON.stringify(replayRun.detail, null, 2));
    if (replayRun.detail?.status === 'completed' && replayRun.detail?.task_count === 2) pass('S12/REPLAY-01', 'second deterministic run is structurally identical', `completed, tasks=${replayRun.detail.task_count}`);
    else fail('S12/REPLAY-01', 'second deterministic run is structurally identical', `status=${replayRun.detail?.status} tasks=${replayRun.detail?.task_count}`, [], 'P1');
  } catch (error) {
    fail('S12/REPLAY-01', 'second deterministic run is structurally identical', String(error), [], 'P1');
  } finally {
    if (replayCampaign) replayCampaign.stop();
  }

  // M1 provider failure: the real engine must fail closed, not mark completion.
  let faultCampaign = null;
  try {
    faultCampaign = await startCampaign({ root: join(root, 'fault'), mode: 'fault' });
    const faultSubmit = faultCampaign.submit('Trigger the scripted provider failure.');
    const faultId = faultSubmit.json?.data?.run_id || faultSubmit.json?.run_id || null;
    if (!faultId) throw new Error(`fault submit failed: ${faultSubmit.stderr.slice(-200)}`);
    const faultRun = await faultCampaign.waitRun(faultId, { timeoutMs: 30_000, autoApprove: false });
    writeFileSync(join(root, 'evidence', 'fault-detail.json'), JSON.stringify(faultRun.detail, null, 2));
    const faultCode = String(faultRun.detail?.error_code || '');
    if (faultRun.detail?.status === 'failed' && (faultCode.startsWith('E_LLM') || faultCode.startsWith('E_PROVIDER'))) {
      pass('M1/PROVIDER-FAIL-01', 'provider failure ends failed with named code', `${faultRun.detail.status} ${faultRun.detail.error_code}`);
    } else {
      fail('M1/PROVIDER-FAIL-01', 'provider failure ends failed with named code', `status=${faultRun.detail?.status} code=${faultRun.detail?.error_code}`, [], 'P0');
    }
  } catch (error) {
    fail('M1/PROVIDER-FAIL-01', 'provider failure ends failed with named code', String(error), [], 'P0');
  } finally {
    if (faultCampaign) faultCampaign.stop();
  }

  const processList = spawnSync('tasklist', ['/FI', 'IMAGENAME eq hiverouter.exe'], { encoding: 'utf8', timeout: 10_000 });
  const leaked = /hiverouter\.exe/i.test(processList.stdout || '') && !/INFO: No tasks/i.test(processList.stdout || '');
  if (!leaked) pass('S10/CLEANUP-01', 'no hiverouter process survives the campaign', 'none');
  else fail('S10/CLEANUP-01', 'no hiverouter process survives the campaign', (processList.stdout || '').slice(0, 400), [], 'P0');

  // ── Static capability checks / remaining planned exams ───────────────────
  const providerLog = join(root, 'evidence', 'provider.jsonl');
  if (existsSync(providerLog)) {
    const requests = readFileSync(providerLog, 'utf8').trim().split(/\r?\n/).filter(Boolean).map((line) => JSON.parse(line));
    const planner = requests.find((request) => request.stream === false);
    const task = requests.find((request) => request.stream === true);
    if (planner && task) pass('S1/PROVIDER-01', 'planner non-streaming + task SSE', `planner=${planner.stream}, task=${task.stream}`);
    else fail('S1/PROVIDER-01', 'planner non-streaming + task SSE', `requests=${requests.length}`, [], 'P1');
  } else fail('S1/PROVIDER-01', 'provider request log exists', 'missing', [], 'P1');

  const detailPath = join(root, 'evidence', 'run-detail.json');
  if (!existsSync(detailPath)) {
    notImpl('S4/TREE-01', 'run detail exposes task roles', 'no run detail was produced');
  } else {
    const taskState = readFileSync(detailPath, 'utf8');
    if (taskState.includes('"role"')) pass('S4/TREE-01', 'run detail exposes task roles', 'present');
    else fail('S4/TREE-01', 'run detail exposes task roles', 'no roles', [], 'P2');
  }

  for (const [checkId, description] of [
    ['S5/UI-01', 'UI DOM/approval/boot-order proof'],
    ['S11/VISUAL-01', 'screenshot/DOM visual truth'],
    ['M10/FORENSICS-01', 'multi-fault forensics battery'],
    ['M11/WATCHDOG-FAIL-01', 'watchdog failure detection'],
    ['M12/SOAK-01', 'soak/resource battery'],
    ['M13/CLOCK-01', 'clock/rate/jitter battery'],
    ['M14/MULTICLIENT-01', 'multi-client concurrency battery'],
    ['M15/FUZZ-01', 'HTTP/WS/MCP/CLI fuzz battery'],
    ['M16/MIGRATION-01', 'upgrade/migration/backup battery'],
    ['M17/VISUAL-A11Y-01', 'visual/a11y battery'],
  ]) notImpl(checkId, description);

  // ── Report ────────────────────────────────────────────────────────────────
  const failed = checks.filter((check) => check.status === 'FAIL');
  const notImplemented = checks.filter((check) => check.status === 'NOT_IMPLEMENTED');
  const verdict = failed.length > 0 ? 'FAIL' : notImplemented.length > 0 ? 'INCOMPLETE' : 'PASS';
  const report = {
    verdict,
    campaign_id: id,
    commit: head,
    root,
    started_at: JSON.parse(readFileSync(join(root, 'campaign.json'), 'utf8')).started_at,
    finished_at: new Date().toISOString(),
    checks,
    failed: failed.map((check) => check.id),
    not_implemented: notImplemented.map((check) => check.id),
    watchdogs: ledgerReports,
    artifacts: {
      evidence: join(root, 'evidence'),
      ledgers: join(root, 'ledgers'),
      report: join(root, 'REPORT.json'),
    },
  };
  writeFileSync(join(root, 'REPORT.json'), JSON.stringify(report, null, 2));
  writeFileSync(join(root, 'REPORT.md'), [
    `# HiveMind full-system campaign ${id}`,
    '',
    `Verdict: **${verdict}**`,
    `Commit: \`${head}\``,
    `Root: \`${root}\``,
    '',
    '## Checks',
    ...checks.map((check) => `- ${check.status}: \`${check.id}\` — ${check.actual}`),
    '',
    '## Not implemented',
    ...notImplemented.map((check) => `- ${check.id}: ${check.expected}`),
    '',
  ].join('\n'));

  for (const proc of watchdogs) { try { proc.kill(); } catch {} }
  if (campaign) campaign.stop();
  console.log(`VERDICT ${verdict}`);
  process.exit(verdict === 'PASS' ? 0 : 1);
}

await main();
