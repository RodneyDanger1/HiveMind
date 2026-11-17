/**
 * Playwright driver for the checkpoint-restore harness.
 *
 * Run (from `web/`, with the Vite dev server already running):
 *   node tests/restoreHarness/drive.mjs
 *
 * It asserts the states the live defect hid: a completed run still offers
 * Restore -> Confirm restore, the confirm posts the real restore route with the
 * bearer token, the receipt/refusal render from the server body, and the Data
 * panel re-reads the tree and the restored preview.
 *
 * This drives the real panels in a real browser against a stubbed HTTP
 * transport; it is NOT a live-app run (no server was started for this seat).
 */
import { chromium } from '../../../e2e/node_modules/playwright/index.mjs';

const URL = process.env.HARNESS_URL ?? 'http://127.0.0.1:5173/tests/restoreHarness/index.html';
const PATH = 'live-content-writer-proof.md';

const failures = [];
function check(name, ok, detail = '') {
  if (ok) {
    console.log(`PASS ${name}`);
  } else {
    failures.push(`${name}: ${detail}`);
    console.log(`FAIL ${name}${detail === '' ? '' : ` — ${detail}`}`);
  }
}

async function launch() {
  for (const channel of ['chrome', 'msedge']) {
    try {
      return await chromium.launch({ channel, headless: true });
    } catch (error) {
      console.log(`INFO ${channel} channel unavailable: ${String(error)}`);
    }
  }
  throw new Error('no installed Chrome/Edge channel (no browser download is allowed)');
}

const browser = await launch();
const page = await browser.newPage();
const pageErrors = [];
const badResponses = [];
page.on('pageerror', (error) => pageErrors.push(`pageerror: ${String(error)}`));
page.on('response', (response) => {
  if (response.status() >= 400) {
    badResponses.push(`${response.status()} ${response.request().resourceType()} ${response.url()}`);
  }
});
page.on('console', (message) => {
  if (message.type() === 'error') {
    pageErrors.push(`console: ${message.text()}`);
  }
});

const calls = () => page.evaluate(() => window.__calls);
const sends = () => page.evaluate(() => window.__sends);
const receipts = () => page.evaluate(() => window.__receipts);

async function confirmRestore(scope) {
  const root = page.locator(`#${scope}`);
  const restore = root.getByRole('button', { name: 'Restore', exact: true });
  await restore.waitFor({ state: 'visible' });
  await restore.click();
  const confirm = root.getByRole('button', { name: 'Confirm restore', exact: true });
  await confirm.waitFor({ state: 'visible' });
  await confirm.click();
}

try {
  await page.goto(URL, { waitUntil: 'load' });

  // 1) completed run: Restore is offered, then asks for confirmation
  const success = page.locator('#success');
  const successRestore = success.getByRole('button', { name: 'Restore', exact: true });
  await successRestore.waitFor({ state: 'visible' });
  check('completed run renders a Restore button', true);
  await successRestore.click();
  await success
    .getByRole('button', { name: 'Confirm restore', exact: true })
    .waitFor({ state: 'visible' });
  check('first click changes Restore to Confirm restore', true);
  await success
    .getByRole('button', { name: 'Confirm restore', exact: true })
    .click();
  await success.locator('p', { hasText: 'undone sequence(s) 1' }).first().waitFor({ state: 'visible' });
  const receiptText = await success.locator('p', { hasText: 'undone sequence(s) 1' }).first().innerText();
  check(
    'receipt renders the restored path and undone sequence',
    receiptText.includes(PATH) && receiptText.includes('absent'),
    receiptText,
  );
  await page.waitForFunction(() => window.__calls.some((call) => call.method === 'POST'));
  const successCalls = await calls();
  const post = successCalls.find(
    (call) => call.method === 'POST' && call.url.endsWith('/api/runs/run-live/checkpoints/1/restore'),
  );
  check('confirm emits the real restore POST', post !== undefined, JSON.stringify(successCalls));
  check(
    'restore POST carries the session bearer token',
    post?.authorization === 'Bearer harness-token',
    String(post?.authorization),
  );
  check(
    'restore POST is preceded by GET /api/session',
    successCalls.some((call) => call.method === 'GET' && call.url.endsWith('/api/session')),
  );
  const successReceipts = await receipts();
  check(
    'onRestored fires once with the receipt path',
    successReceipts.filter((entry) => entry === `restored:${PATH}`).length === 1,
    JSON.stringify(successReceipts),
  );

  // 2) refused restore: alert carries the server refusal, no receipt, no refresh
  await page.locator('#error').getByRole('button', { name: 'Restore', exact: true }).waitFor({ state: 'visible' });
  check('failed (terminal) run also renders a Restore button', true);
  const receiptsBeforeRefusal = (await receipts()).length;
  const sendsBeforeRefusal = (await sends()).length;
  await confirmRestore('error');
  const alert = page.locator('#error [role="alert"]');
  await alert.waitFor({ state: 'visible' });
  const alertText = await alert.innerText();
  check(
    'refusal renders the server code and message',
    alertText.includes('E_CHECKPOINT_DRIFT') &&
      alertText.includes('no longer matches the recorded after-state'),
    alertText,
  );
  check(
    'refusal does not emit a success receipt',
    (await receipts()).length === receiptsBeforeRefusal,
    JSON.stringify(await receipts()),
  );
  check(
    'refusal does not trigger the post-restore refresh',
    (await sends()).length === sendsBeforeRefusal,
    JSON.stringify(await sends()),
  );
  check(
    'refusal POSTs the real route for its run',
    (await calls()).some(
      (call) => call.method === 'POST' && call.url.endsWith('/api/runs/run-drift/checkpoints/1/restore'),
    ),
  );
  check(
    'earlier success receipt is not cleared by a later refusal',
    (await receipts()).includes(`restored:${PATH}`),
  );

  // 3) Data panel: restore refreshes the tree and re-reads the open preview
  await page.locator('#refresh').getByRole('button', { name: 'Restore', exact: true }).waitFor({ state: 'visible' });
  check('Data panel completed-run checkpoint renders Restore', true);
  await confirmRestore('refresh');
  await page
    .locator('#refresh p', { hasText: 'undone sequence(s) 1' })
    .first()
    .waitFor({ state: 'visible' });
  await page.waitForFunction(
    () =>
      window.__sends.some((send) => send.name === 'FILE_TREE_LIST') &&
      window.__sends.some(
        (send) => send.name === 'FILE_READ' && send.payload.path === 'live-content-writer-proof.md',
      ),
    undefined,
    { timeout: 10_000 },
  );
  const refreshSends = await sends();
  check(
    'Data panel refreshes FILE_TREE_LIST after restore',
    refreshSends.some((send) => send.name === 'FILE_TREE_LIST'),
    JSON.stringify(refreshSends),
  );
  check(
    'Data panel re-reads the restored preview path',
    refreshSends.some(
      (send) => send.name === 'FILE_READ' && send.payload.path === PATH,
    ),
    JSON.stringify(refreshSends),
  );

  check('no uncaught browser errors', pageErrors.length === 0, pageErrors.join(' | '));
  console.log(`INFO non-2xx network responses: ${JSON.stringify(badResponses)}`);
} catch (error) {
  failures.push(`driver: ${String(error)}`);
  console.log(`FAIL driver — ${String(error)}`);
  for (const message of pageErrors) {
    console.log(`INFO browser — ${message}`);
  }
} finally {
  await browser.close();
}

if (failures.length > 0) {
  console.log(`\nFAILED (${failures.length})`);
  for (const failure of failures) {
    console.log(` - ${failure}`);
  }
  process.exit(1);
}
console.log('\nALL RESTORE HARNESS CHECKS PASSED');
