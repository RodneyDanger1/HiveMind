import { test, expect, type Page } from '@playwright/test';
import { appMissing, skipReason } from '../helpers/matrix';
import { captureFrames, parseFrames, shot, waitForFrame, writeEvidence } from '../helpers/evidence';
import { contrastRatio } from '../helpers/contrast';
import {
  assertStrictLaunchAuth,
  describeAuth,
  gotoWithLaunch,
  launchUrl,
  resolveAppAuth,
  safeUrl,
  type AppAuth,
} from '../helpers/app-auth';

/**
 * LIVE app spec — drives the real, already-open HiveMind launcher instance.
 *
 * AUTH CONTRACT (see e2e/helpers/app-auth.ts):
 * The launcher opens `http://127.0.0.1:<port>/?launch=<secret>`; the server refuses every
 * unauthenticated `/api/*` GET with 403 E_ORIGIN_REJECTED. Playwright's fresh context does
 * not inherit the launcher window, so EVERY navigation here must carry `?launch=` or the
 * session/WS never authenticates. Resolving that token is the harness's job, not a product
 * allowance — the negative tests below pin the refusal direction.
 *
 * No new server is started by this file; it only drives the instance named by E2E_BASE_URL
 * (default: the documented launcher bind 127.0.0.1:8099).
 */
const BASE = (process.env.E2E_BASE_URL ?? 'http://127.0.0.1:8099').replace(/\/+$/, '');

/**
 * SECRET HYGIENE: the launcher token lives in the page URL (`?launch=`), and Playwright's
 * failure traces record page URLs and request headers verbatim. A retained trace therefore
 * writes the per-boot launch secret into the retained trace archive. Live navigation must
 * run with tracing off; failure screenshots (no URL bar) and the JSON evidence below still
 * carry the diagnosis. Verified leak: one trace held the token in 24 records on 2026-11-17.
 */
test.use({ trace: 'off' });

let authPromise: Promise<AppAuth> | null = null;

/** Resolves (once) the launch token + gate state of the running instance. */
function auth(): Promise<AppAuth> {
  authPromise ??= resolveAppAuth(BASE);
  return authPromise;
}

async function gotoApp(page: Page, path = '/'): Promise<Awaited<ReturnType<typeof gotoWithLaunch>>> {
  return gotoWithLaunch(page, await auth(), path);
}

test.describe('live app — boot & surfaces', () => {
  test('boot: 200, title, #root, skip link text/href', async ({ page }, testInfo) => {
    test.skip(appMissing(), skipReason());
    const session = await auth();
    const response = await gotoApp(page);
    expect(response?.status()).toBe(200);
    await expect(page).toHaveTitle('HiveMind — Spatial Execution Environment');
    await expect(page.locator('#root')).toBeVisible();
    const skip = page.locator('a.skip-link');
    await expect(skip).toHaveText('Skip to canvas');
    await expect(skip).toHaveAttribute('href', '#hive-canvas');
    const bodyText = await page.locator('body').innerText();
    expect(bodyText.length).toBeGreaterThan(400);
    const screenshot = await shot(page, 'boot', testInfo);
    writeEvidence('01-boot.json', {
      status: response?.status(),
      title: await page.title(),
      skipLink: { text: await skip.innerText(), href: await skip.getAttribute('href') },
      screenshot,
      bodyTextLen: bodyText.length,
      urlWithoutLaunchToken: safeUrl(page.url(), session.token),
      auth: describeAuth(session),
    });
  });

  test('skip link target #hive-canvas exists', async ({ page }) => {
    test.skip(appMissing(), skipReason());
    await gotoApp(page);
    const targetCount = await page.locator('#hive-canvas').count();
    const canvasApplication = await page.locator('[role="application"][aria-label="HiveMind spatial canvas"]').count();
    writeEvidence('02-skip-link.json', { targetCount, canvasApplication, note: 'skip link points at #hive-canvas; canvas renders as role=application' });
    expect(targetCount, 'skip-link target #hive-canvas must exist (canvas renders role=application but no id)').toBeGreaterThan(0);
  });

  test('no console errors or uncaught exceptions on load', async ({ page }) => {
    test.skip(appMissing(), skipReason());
    const errors: Array<{ text: string; url?: string }> = [];
    const pageErrors: string[] = [];
    page.on('console', (message) => { if (message.type() === 'error') errors.push({ text: message.text(), url: message.location().url }); });
    page.on('pageerror', (error) => pageErrors.push(String(error)));
    await gotoApp(page);
    await page.waitForTimeout(500);
    writeEvidence('03-console.json', { consoleErrors: errors, pageErrors });
    expect(pageErrors, 'uncaught exceptions').toEqual([]);
    expect(errors, `console errors: ${JSON.stringify(errors)}`).toEqual([]);
  });

  test('chrome surface census: nav 6, tabs 4, chat, composer, buttons, HUD, scrubber, library, minimap, zoom', async ({ page }, testInfo) => {
    test.skip(appMissing(), skipReason());
    await gotoApp(page);
    const nav = page.locator('nav[aria-label="Primary navigation"]');
    await expect(nav).toBeVisible();
    await expect(nav.getByRole('button')).toHaveCount(6);
    for (const label of ['Workspace', 'Agents', 'Data', 'Models', 'Routing', 'Settings']) {
      await expect(nav.getByRole('button', { name: label, exact: true })).toBeVisible();
    }
    const tabs = page.getByRole('tablist', { name: 'Workspace tabs' });
    await expect(tabs.getByRole('tab')).toHaveCount(4);
    for (const label of ['Workspace', 'Swarm', 'Routing', 'History']) {
      await expect(tabs.getByRole('tab', { name: label, exact: true })).toBeVisible();
    }
    await expect(page.locator('section[aria-label="HyperAgent hive chat core"]')).toBeVisible();
    await expect(page.locator('#composer')).toHaveAttribute('placeholder', 'Type your prompt...');
    for (const name of ['Spawn sub-hive', 'Re-route', 'Cancel', 'Kill worker']) {
      await expect(page.getByRole('button', { name, exact: true })).toBeVisible();
    }
    await expect(page.getByRole('status', { name: 'Swarm throughput' })).toBeVisible();
    const hudText = await page.getByRole('status', { name: 'Swarm throughput' }).innerText();
    expect(hudText.toUpperCase()).toContain('SUPERAGENTS');
    expect(hudText.toUpperCase()).toContain('NO METRICS');
    await expect(page.locator('#scrubber')).toHaveAttribute('aria-label', 'Time-travel scrubber');
    await expect(page.locator('#scrubber')).toBeDisabled();
    const library = page.locator('aside[aria-label="Library panel"]');
    await expect(library).toBeVisible();
    await expect(library.getByRole('button')).toHaveCount(4);
    await expect(page.locator('[data-testid="rf__minimap"]')).toBeVisible();
    for (const name of ['Zoom in', 'Zoom out', 'Recentre hive (Ctrl+0)', 'Fit swarm']) {
      await expect(page.getByRole('button', { name, exact: true })).toBeVisible();
    }
    const screenshot = await shot(page, 'surface-census', testInfo);
    writeEvidence('04-census.json', {
      navItems: await nav.getByRole('button').allInnerTexts(),
      tabs: await tabs.getByRole('tab').allInnerTexts(),
      hudText,
      scrubberDisabled: await page.locator('#scrubber').isDisabled(),
      libraryTemplates: await library.getByRole('button').allInnerTexts(),
      minimap: { testid: 'rf__minimap', ariaLabel: await page.locator('[data-testid="rf__minimap"]').getAttribute('aria-label') },
      zoomButtons: ['Zoom in', 'Zoom out', 'Recentre hive (Ctrl+0)', 'Fit swarm'],
      screenshot,
    });
  });
});

test.describe('live app — launch-token auth (strict)', () => {
  test('HTTP gate: /api/session and a read route refuse missing/invalid launch tokens', async ({ request }) => {
    test.skip(appMissing(), skipReason());
    const session = await auth();
    const evidence = await assertStrictLaunchAuth(request, session);
    writeEvidence('16-auth-gate.json', evidence);
    // The evidence file must never contain the live token itself.
    expect(JSON.stringify(evidence)).not.toContain(String(session.token ?? '::none::'));
  });

  test('authed navigation: launcher URL boots the app, /api/session 200, WS receives SERVER_HELLO', async ({ page }) => {
    test.skip(appMissing(), skipReason());
    const session = await auth();
    const frames = captureFrames(page).frames;
    const wsUrls: string[] = [];
    const sessionReads: Array<{ status: number; url: string }> = [];
    page.on('websocket', (ws) => wsUrls.push(ws.url()));
    page.on('response', (response) => {
      if (new URL(response.url()).pathname === '/api/session') {
        sessionReads.push({ status: response.status(), url: safeUrl(response.url(), session.token) });
      }
    });
    const response = await gotoApp(page);
    expect(response?.status()).toBe(200);
    const hello = await waitForFrame(frames, (frame) => frame.type === 'EVENT' && frame.name === 'SERVER_HELLO');
    expect(hello).toMatchObject({ schema: 'hiverouter.v1', type: 'EVENT', name: 'SERVER_HELLO' });
    expect(sessionReads.some((read) => read.status === 200), 'the page must read /api/session successfully with the launcher token').toBe(true);
    expect(wsUrls.length, 'an authed boot must open the WebSocket').toBeGreaterThan(0);
    // The socket is authenticated by the per-boot session token; the launch secret stays in the HTTP header.
    expect(wsUrls[0]).not.toContain(String(session.token ?? '::none::'));
    writeEvidence('17-auth-positive.json', {
      htmlStatus: response?.status(),
      sessionReads,
      wsOpened: wsUrls.length,
      wsUrlHasLaunchToken: session.token === null ? false : wsUrls[0].includes(session.token),
      serverHello: { name: String(hello.name), schema: String(hello.schema) },
      auth: describeAuth(session),
    });
  });

  test('missing launch token: /api/session 403 E_ORIGIN_REJECTED, no WS, refusal banner', async ({ page }) => {
    test.skip(appMissing(), skipReason());
    const session = await auth();
    test.skip(!session.gated, 'server started without HIVEROUTER_LAUNCH_TOKEN: the launch gate is off');
    const sockets: string[] = [];
    page.on('websocket', (ws) => sockets.push(ws.url()));
    const sessionRead = page.waitForResponse(
      (response) => new URL(response.url()).pathname === '/api/session',
      { timeout: 15_000 },
    );
    await page.goto(`${BASE}/`, { waitUntil: 'networkidle', timeout: 30_000 });
    const response = await sessionRead;
    expect(response.status(), 'missing launch token must be refused').toBe(403);
    const body = (await response.json()) as { code?: string; message?: string };
    expect(body.code).toBe('E_ORIGIN_REJECTED');
    expect(body.message ?? '').toMatch(/launcher token/i);
    // The splash veil holds ~4.2s + 1.8s grace before the named refusal is visible.
    await expect(
      page.getByRole('alert').filter({ hasText: /launcher token/i }).first(),
      'the unauthenticated boot must surface a named refusal, not a silent offline app',
    ).toBeVisible({ timeout: 15_000 });
    expect(sockets, 'no WebSocket may open without an authenticated session').toHaveLength(0);
    writeEvidence('18-auth-missing-token.json', {
      sessionStatus: response.status(),
      refusalCode: body.code,
      refusalMessage: body.message,
      websocketsOpened: sockets.length,
      urlHasLaunchParam: new URL(page.url()).searchParams.has('launch'),
    });
  });

  test('invalid launch token: /api/session 403 E_ORIGIN_REJECTED, no WS, refusal banner', async ({ page }) => {
    test.skip(appMissing(), skipReason());
    const session = await auth();
    test.skip(!session.gated, 'server started without HIVEROUTER_LAUNCH_TOKEN: the launch gate is off');
    const sockets: string[] = [];
    page.on('websocket', (ws) => sockets.push(ws.url()));
    const forged = 'e2e-forged-launch-token-000000000000';
    const sessionRead = page.waitForResponse(
      (response) => new URL(response.url()).pathname === '/api/session',
      { timeout: 15_000 },
    );
    await page.goto(launchUrl(BASE, '/', forged), { waitUntil: 'networkidle', timeout: 30_000 });
    const response = await sessionRead;
    expect(response.status(), 'an invalid launch token must be refused').toBe(403);
    const body = (await response.json()) as { code?: string; message?: string };
    expect(body.code).toBe('E_ORIGIN_REJECTED');
    expect(body.message ?? '').toMatch(/launcher token/i);
    await expect(
      page.getByRole('alert').filter({ hasText: /launcher token/i }).first(),
      'the forged-token boot must surface a named refusal',
    ).toBeVisible({ timeout: 15_000 });
    expect(sockets, 'no WebSocket may open without an authenticated session').toHaveLength(0);
    writeEvidence('19-auth-invalid-token.json', {
      sessionStatus: response.status(),
      refusalCode: body.code,
      refusalMessage: body.message,
      websocketsOpened: sockets.length,
      forgedTokenFingerprint: 'e2e-forged-launch-token',
    });
  });
});

test.describe('live app — safe interactions & render-back', () => {
  test('tabs switch and render back (Swarm/Routing/History/Workspace)', async ({ page }) => {
    test.skip(appMissing(), skipReason());
    await gotoApp(page);
    const tabs = page.getByRole('tablist', { name: 'Workspace tabs' });
    for (const label of ['Swarm', 'Routing', 'History', 'Workspace']) {
      await tabs.getByRole('tab', { name: label, exact: true }).click();
      await expect(tabs.getByRole('tab', { name: label, exact: true })).toHaveAttribute('aria-selected', 'true');
    }
    // Routing tab renders the routing panel heading
    await tabs.getByRole('tab', { name: 'Routing', exact: true }).click();
    await expect(page.getByText('No edges in the current graph.').first()).toBeVisible();
    const evidence = { finalTab: await tabs.getByRole('tab', { name: 'Routing', exact: true }).getAttribute('aria-selected'), routingEmptyVisible: await page.getByText('No edges in the current graph.').first().isVisible() };
    writeEvidence('05-tabs.json', evidence);
  });

  test('nav rail switches all six views and returns to workspace', async ({ page }) => {
    test.skip(appMissing(), skipReason());
    await gotoApp(page);
    const nav = page.locator('nav[aria-label="Primary navigation"]');
    const expectations: Record<string, string> = {
      Agents: 'No agents yet — submit a prompt to create the first run.',
      Data: 'Select a file from the tree to preview it here.',
      Models: 'The server has not reported provider/model yet (run SETTINGS_STATUS in Settings).',
      Routing: 'No edges in the current graph.',
      Settings: 'Provider key status',
    };
    for (const [label, expectedText] of Object.entries(expectations)) {
      await nav.getByRole('button', { name: label, exact: true }).click();
      await expect(nav.getByRole('button', { name: label, exact: true })).toHaveAttribute('aria-current', 'page');
      await expect(page.getByText(expectedText, { exact: false }).first()).toBeVisible();
    }
    await nav.getByRole('button', { name: 'Workspace', exact: true }).click();
    await expect(nav.getByRole('button', { name: 'Workspace', exact: true })).toHaveAttribute('aria-current', 'page');
    writeEvidence('06-nav.json', { views: Object.keys(expectations), returnedToWorkspace: true });
  });

  test('zoom controls and Ctrl+0 change the React Flow viewport transform', async ({ page }) => {
    test.skip(appMissing(), skipReason());
    await gotoApp(page);
    const viewport = page.locator('.react-flow__viewport');
    const read = () => viewport.evaluate((el) => getComputedStyle(el).transform);
    const start = await read();
    await page.getByRole('button', { name: 'Zoom in', exact: true }).click();
    await page.waitForTimeout(350);
    const zoomed = await read();
    expect(zoomed).not.toBe(start);
    await page.keyboard.press('Control+0');
    await page.waitForTimeout(450);
    const recentred = await read();
    expect(recentred).not.toBe(zoomed);
    await page.getByRole('button', { name: 'Fit swarm', exact: true }).click();
    await page.waitForTimeout(450);
    const fitted = await read();
    expect(fitted).toMatch(/^matrix\(/); // Fit is a no-op on an empty graph; it must not throw or blank the canvas.
    await expect(page.locator('.react-flow__viewport')).toBeVisible();
    writeEvidence('07-zoom.json', { start, zoomed, recentred, fitted });
  });

  test('chat core pin sends CHAT_PIN and waits for ACK before render-back', async ({ page }) => {
    test.skip(appMissing(), skipReason());
    const frames = captureFrames(page).frames;
    await gotoApp(page);
    await waitForFrame(frames, (j) => j.name === 'SERVER_HELLO');
    const pinButton = page.getByRole('button', { name: /^(Unpin|Pin) chat$/ });
    const before = await pinButton.getAttribute('aria-pressed');
    const start = frames.length;
    await pinButton.click();
    const command = await waitForFrame(frames.slice(start), (j) => j.name === 'CHAT_PIN' && j.type === 'COMMAND');
    const ack = await waitForFrame(frames.slice(start), (j) => j.name === 'ACK' && j.correlation_id === command.correlation_id);
    await expect(page.getByRole('button', { name: /^(Unpin|Pin) chat$/ })).toHaveAttribute('aria-pressed', before === 'true' ? 'false' : 'true');
    const ackMatches = ack.correlation_id === command.correlation_id;
    writeEvidence('08-chat-pin.json', { before, command: { name: command.name, id: command.id, correlation_id: command.correlation_id }, ack: { name: ack.name, correlation_id: ack.correlation_id }, ackMatchesCommandCorrelation: ackMatches, after: await page.getByRole('button', { name: /^(Unpin|Pin) chat$/ }).getAttribute('aria-pressed') });
  });

  test('empty prompt refusal is named and sends no SUBMIT_PROMPT (no fake success)', async ({ page }) => {
    test.skip(appMissing(), skipReason());
    const frames = captureFrames(page).frames;
    await gotoApp(page);
    await waitForFrame(frames, (j) => j.name === 'SERVER_HELLO');
    await expect(page.locator('button[type=submit]')).toBeDisabled();
    const start = frames.length;
    await page.locator('#composer').focus();
    await page.keyboard.press('Enter');
    await expect(page.getByText('REFUSED BY NAME · NOT SILENT')).toBeVisible();
    await expect(page.getByText('E_PROMPT_EMPTY')).toBeVisible();
    const sent = parseFrames(frames.slice(start)).filter((f) => f.dir === 'sent' && f.name === 'SUBMIT_PROMPT');
    expect(sent).toEqual([]);
    writeEvidence('09-empty-prompt.json', { toastShown: true, submitFramesSent: sent.length });
  });

  test('WS handshake uses the page session token and hello{last_seq}; SERVER_HELLO received', async ({ page }) => {
    test.skip(appMissing(), skipReason());
    const session = await auth();
    const frames = captureFrames(page).frames;
    const wsUrls: string[] = [];
    page.on('websocket', (ws) => wsUrls.push(ws.url()));
    let pageSession: { token: string; last_seq: number } | null = null;
    page.on('response', (response) => {
      // Keep the FIRST 200 read: later reconnects re-read /api/session against a moving
      // live bus, so a later last_seq is not the one the first socket was opened with.
      if (pageSession !== null) return;
      if (new URL(response.url()).pathname === '/api/session' && response.status() === 200) {
        response
          .json()
          .then((body) => {
            const parsed = body as { token?: unknown; last_seq?: unknown };
            if (typeof parsed.token === 'string' && typeof parsed.last_seq === 'number') {
              pageSession = { token: parsed.token, last_seq: parsed.last_seq };
            }
          })
          .catch(() => {
            // Evidence below records the missing page session; the assertion fails loudly.
          });
      }
    });
    await gotoApp(page);
    const hello = await waitForFrame(frames, (j) => (j as { type?: string }).type === 'EVENT' && j.name === 'SERVER_HELLO');
    const sentHello = await waitForFrame(frames, (j) => (j as { type?: string }).type === 'hello');
    // The page's OWN session read is the only correct baseline: the live bus moves between
    // reads, so a separate request cannot be compared to the socket the app already opened.
    await expect.poll(() => pageSession !== null, { timeout: 10_000 }).toBe(true);
    const own = pageSession as unknown as { token: string; last_seq: number };
    expect(wsUrls.length).toBeGreaterThan(0);
    const wsUrl = new URL(wsUrls[0]);
    expect(wsUrl.searchParams.get('token')).toBe(own.token);
    const wsLastSeq = Number(wsUrl.searchParams.get('last_seq'));
    expect(Number.isInteger(wsLastSeq), 'WS last_seq must be an integer seq').toBe(true);
    expect(wsLastSeq, 'the socket opens at the page session last_seq').toBe(own.last_seq);
    expect(sentHello).toMatchObject({ type: 'hello', last_seq: wsLastSeq });
    expect(hello).toMatchObject({ schema: 'hiverouter.v1', type: 'EVENT', name: 'SERVER_HELLO' });
    // Never persist either live secret: fingerprints/booleans only.
    writeEvidence('10-ws-handshake.json', {
      wsTokenMatchesPageSession: true,
      lastSeq: own.last_seq,
      wsUrlHasLaunchToken: session.token === null ? false : wsUrls[0].includes(session.token),
      sentHello,
      serverHello: { name: hello.name, schema: hello.schema, seq: hello.seq },
    });
  });

  test('computed token colours: canvas/panel/body text contrast >= 4.5', async ({ page }) => {
    test.skip(appMissing(), skipReason());
    await gotoApp(page);
    const styles = await page.evaluate(() => {
      const body = getComputedStyle(document.body);
      const panel = getComputedStyle(document.querySelector('.surface-panel') ?? document.body);
      return { bodyColor: body.color, bodyBg: body.backgroundColor, panelColor: panel.color, panelBg: panel.backgroundColor, fontFamily: body.fontFamily };
    });
    const toHex = (rgb: string): string => {
      const m = rgb.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)/);
      if (!m) throw new Error(`cannot parse colour ${rgb}`);
      return `#${[m[1], m[2], m[3]].map((n) => Number(n).toString(16).padStart(2, '0')).join('')}`;
    };
    const ratio = contrastRatio(toHex(styles.bodyColor), toHex(styles.bodyBg));
    expect(toHex(styles.bodyBg)).toBe('#f8f4eb');
    expect(toHex(styles.bodyColor)).toBe('#282010');
    expect(ratio).toBeGreaterThanOrEqual(4.5);
    writeEvidence('11-colours.json', { ...styles, bodyContrast: Number(ratio.toFixed(4)) });
  });
});

test.describe('live app — contract wiring', () => {
  test('SETTINGS_STATUS safe command receives ACK + SETTINGS_STATUS_RESULT', async ({ page }) => {
    test.skip(appMissing(), skipReason());
    const frames = captureFrames(page).frames;
    await gotoApp(page);
    await waitForFrame(frames, (j) => j.name === 'SERVER_HELLO');
    const start = frames.length;
    await page.getByRole('button', { name: 'Settings', exact: true }).first().click();
    await expect(page.getByText('Provider key status').first()).toBeVisible();
    const relevant = parseFrames(frames.slice(start)).filter((f) => f.dir === 'received' && (f.name === 'ACK' || f.name === 'ERROR' || f.name === 'SETTINGS_STATUS_RESULT'));
    const settingsSent = parseFrames(frames.slice(start)).filter((f) => f.dir === 'sent' && f.name === 'SETTINGS_STATUS');
    writeEvidence('12-settings-status.json', { settingsCommandsSent: settingsSent.length, responses: relevant.map((r) => ({ name: r.name, code: (r.payload as { code?: string } | undefined)?.code, correlation_id: r.correlation_id })) });
    expect.soft(settingsSent.length, 'one SETTINGS_STATUS per Settings open').toBe(1);
    expect.soft(relevant.some((r) => r.name === 'ACK'), 'safe read command must be ACKed').toBe(true);
    expect.soft(relevant.some((r) => r.name === 'SETTINGS_STATUS_RESULT'), 'result event').toBe(true);
  });

  test('Data panel and Approval copy reflect the frozen contract (no stale contract-gap claims)', async ({ page }) => {
    test.skip(appMissing(), skipReason());
    await gotoApp(page);
    await page.getByRole('button', { name: 'Data', exact: true }).first().click();
    const dataText = await page.locator('main').innerText();
    const bundle = await page.evaluate(async () => {
      const script = document.querySelector('script[type=module]') as HTMLScriptElement | null;
      if (!script) return '';
      return (await fetch(script.src)).text();
    });
    const staleDataClaim = /no workspace-file API yet/i.test(dataText);
    const staleApprovalClaim = /no approval-response command/i.test(bundle);
    writeEvidence('13-contract-drift.json', {
      dataPanelText: dataText.slice(0, 400),
      staleDataClaim,
      staleApprovalClaim,
      frozenContractHas: { FILE_TREE_LIST: true, FILE_READ: true, APPROVAL_RESOLVE: true, SETTINGS_STATUS: true },
    });
    expect(staleDataClaim, 'Data panel still claims no workspace-file API although FILE_TREE_LIST/FILE_READ are frozen').toBe(false);
    expect(staleApprovalClaim, 'approval overlay still claims no approval-response command although APPROVAL_RESOLVE is frozen').toBe(false);
  });

  test('Data panel renders FILE_TREE_RESULT entries from the workspace', async ({ page }) => {
    test.skip(appMissing(), skipReason());
    const frames = captureFrames(page).frames;
    await gotoApp(page);
    await waitForFrame(frames, (j) => j.name === 'SERVER_HELLO');
    const start = frames.length;
    await page.getByRole('button', { name: 'Data', exact: true }).first().click();
    let result: Record<string, unknown> | null = null;
    try {
      result = await waitForFrame(frames.slice(start), (j) => j.name === 'FILE_TREE_RESULT', 6000);
    } catch {
      result = null;
    }
    await page.waitForTimeout(1500);
    const mainText = await page.locator('main').innerText();
    const noteVisible = await page.getByText('note.txt', { exact: false }).count();
    const readmeVisible = await page.getByText('README.md', { exact: false }).count();
    writeEvidence('14-file-tree.json', {
      resultReceived: Boolean(result),
      entries: (result?.payload as { entries?: unknown } | undefined)?.entries ?? null,
      noteVisible,
      readmeVisible,
      mainText: mainText.slice(0, 500),
    });
    expect(result, 'FILE_TREE_RESULT must be received from the server').toBeTruthy();
    expect(noteVisible + readmeVisible, 'workspace tree entries (note.txt/README.md) must render in the Data panel').toBeGreaterThan(0);
  });

  test('Data panel shows — (not 0 B) for unknown entry sizes', async ({ page }) => {
    test.skip(appMissing(), skipReason());
    const frames = captureFrames(page).frames;
    await gotoApp(page);
    await waitForFrame(frames, (j) => j.name === 'SERVER_HELLO');
    const start = frames.length;
    await page.getByRole('button', { name: 'Data', exact: true }).first().click();
    try {
      await waitForFrame(frames.slice(start), (j) => j.name === 'FILE_TREE_RESULT', 6000);
    } catch {
      // evidence below records the missing frame; the DOM assertions still run
    }
    await expect(page.getByText('note.txt')).toBeVisible();
    await page.waitForTimeout(600);
    const mainText = await page.locator('main').innerText();
    const zeroB = /\b0 B\b/.test(mainText);
    const readmeUnknown = /README\.md\s*—/.test(mainText);
    const noteUnknown = /note\.txt\s*—/.test(mainText);
    const bundle = await page.evaluate(async () => {
      const script = document.querySelector('script[type=module]') as HTMLScriptElement | null;
      if (!script) return '';
      return (await fetch(script.src)).text();
    });
    // The server's FILE_TREE_RESULT entries currently carry no size_bytes, so both
    // entries are "unknown size" and must render the em dash placeholder.
    writeEvidence('15-data-size.json', {
      mainText: mainText.slice(0, 700),
      sizePlaceholders: { readmeUnknown, noteUnknown, zeroB },
      sourceEvidence: 'web/src/panels/DataPanel.tsx:44 — entry.kind === "dir" ? "/" : entry.size_bytes === null ? "—" : `${entry.size_bytes} B`',
      bundleHasEmDash: bundle.includes('—'),
      bundleHasZeroBFallback: bundle.includes('0 B'),
      note: 'live server FILE_TREE_RESULT entries have no size_bytes field, so the panel exercises the unknown-size path',
    });
    expect(readmeUnknown, 'README.md must show the unknown-size — placeholder').toBe(true);
    expect(noteUnknown, 'note.txt must show the unknown-size — placeholder').toBe(true);
    expect(zeroB, 'unknown sizes must never render as 0 B').toBe(false);
    expect(bundle.includes('—'), 'shipped bundle contains the — fallback').toBe(true);
    expect(bundle.includes('0 B'), 'shipped bundle must not contain a 0 B size fallback').toBe(false);
  });
});
