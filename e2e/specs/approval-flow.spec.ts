import { test, expect, type Page } from '@playwright/test';
import {
  eventFrame,
  installMockWs,
  sendApprovalRequest,
  sendMalformedEvent,
  startDistHarness,
  type DistHarness,
  type MockWsHarness,
} from '../helpers/replay-harness';
import type { ApprovalRequestPayload } from '../../web/src/api/contract';

/**
 * WAVE 3-4 / SEAT 7 (+ Wave-4 expiry/resolving assertions) — e2e adoption & truth for the W12 approval overlay.
 *
 * Transport double only (`startDistHarness` + `installMockWs`, auto-ACK of COMMANDs);
 * no live server, no provider call, no key, no build/run by this seat.
 *
 * ADOPTED VALIDATED HELPERS (`e2e/helpers/replay-harness.ts`):
 *   - `sendApprovalRequest` → validated APPROVAL_REQUEST builder (required
 *     approval_id/tool; 1.2 fields validated when present, args_sha256 64-hex,
 *     expires_at parseable ISO-8601, never fabricated).
 *   - `sendMalformedEvent` → deliberate decoder-refusal payloads only.
 *   - `ws.waitForCommand('APPROVAL_RESOLVE')` / `ws.approvalResolveCommands()` →
 *     parsed COMMAND capture (`{name, payload, raw, correlationId, at}`); no hand-rolled
 *     JSON parsing in this spec.
 *
 * EXACT ACTUAL SELECTORS — AS OF 2026-11-17 the approval card moved from the modal overlay
 * into the chat transcript. `web/src/panels/ApprovalOverlay.tsx:248-255` returns null
 * whenever the chat is visible AND the composer is mounted, delegating the decision to
 * `web/src/chat/ChatCore.tsx:1112-1232` (`role="alert"`, `data-testid="chat-inline-approval"`,
 * aria-label "Approval needed: file_write"). The modal is now only a fallback for a hidden
 * chat. Pinning the old `dialog` selector tested a surface the product deliberately no longer
 * shows on the default (chat-visible) layout.
 *
 *   - card: `[data-testid="chat-inline-approval"]` (role=alert,
 *     aria-label "Approval needed: file_write"); the old modal name
 *     "Tool approval requested: file_write" (STRINGS.approval.title) is no longer rendered.
 *   - expiry: a `span[title="expires in 2m 00s" | "expired" | "unknown"]`
 *   - actions: `button[aria-label="Approve tool call for file_write"]` /
 *     `"Deny tool call for file_write"`; disabled when nonce is null, TTL expired, or a
 *     gesture is pending **until the server APPROVAL_RESOLVED removes the card** (an ACK
 *     alone does not close the card and does not re-enable the actions).
 *   - nonce-less copy: STRINGS.approval.nonceMissing (`did not carry the nonce`).
 *   - KNOWN SURFACE GAP (recorded, not asserted as correct): the inline card renders the
 *     tool + expiry only; run_id / task_id / tool_call_id / approval_id / args_sha256 are
 *     no longer displayed. They remain in the store but the modal that showed them is
 *     suppressed while chat is visible. Reported as a truth/evidence-loss finding.
 *
 * PHASE 1 (logic-gatekeeper — Wave-4 expiry/resolving verification):
 * {
 *   "phase1": {
 *     "architecture": "Mock WS -> app decoder (optional nonce/run/task/tool_call/args/expiry) -> store.approvals + resolvingApprovals -> ChatCore inline approval card (role=alert, tool + expiry + two labelled actions; the modal ApprovalOverlay is the hidden-chat fallback) -> store.resolveApproval -> COMMAND APPROVAL_RESOLVE -> mock ACK; only a server APPROVAL_RESOLVED removes the card. This spec owns neither product nor helper code.",
 *     "dataFlow": "Event payloads and operator input are untrusted. Rendering must send nothing; a gesture may send at most one APPROVAL_RESOLVE carrying the exact approval_id/nonce/decision; ACK must not close or imply resolution and must not re-enable the actions; expiry must make the card read-only (disabled actions); malformed frames must stop at the decoder with no card.",
 *     "edgeCases": [
 *       "validated builder payload renders the tool, a live expiry and both enabled actions, with zero commands before a click (ids/args hash are no longer rendered — see KNOWN SURFACE GAP)",
 *       "approve: one command with decision=approve and exact nonce; card stays open and BOTH actions stay disabled after the mock ACK until APPROVAL_RESOLVED",
 *       "same-task double click sends exactly one command (store resolving guard; stability window)",
 *       "deny: one command with decision=deny, exact nonce and reason 'operator denied in chat' (chat origin; the modal path keeps 'operator denied')",
 *       "duplicate APPROVAL_REQUEST with the same id replaces the card by id (no stacked cards)",
 *       "nonce-less request: both actions disabled, honest gap copy, nothing sent",
 *       "expired approval: read-only — expiry text 'expired' and both actions disabled, nothing sent",
 *       "malformed APPROVAL_REQUEST is refused as E_SCHEMA_INVALID with no card"
 *     ],
 *     "rootCause": "Wave-2 encoded stale product gaps (hand-built frames; expired state displayed without gating actions). Wave-3/4 code now gates expiry in canResolveApproval and keeps a panel pending lock until APPROVAL_RESOLVED removes the card, so this spec pins read-only expiry, disabled actions while resolving (including after ACK), and validated helper payloads instead of encoding gaps."
 *   }
 * }
 */

const TOOL = 'file_write';
const HASH_A = 'a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90';
const APPROVE_LABEL = `Approve tool call for ${TOOL}`;
const DENY_LABEL = `Deny tool call for ${TOOL}`;

let harness: DistHarness;

test.beforeAll(async () => {
  harness = await startDistHarness();
});

test.afterAll(async () => {
  await harness.close();
});

async function openConnected(page: Page): Promise<MockWsHarness> {
  const ws = await installMockWs(page);
  await page.goto(harness.url);
  await expect(page.getByText(/Connected/).first()).toBeVisible({ timeout: 15_000 });
  return ws;
}

function approvalPayload(overrides: Partial<ApprovalRequestPayload> = {}): ApprovalRequestPayload {
  const expiresAt = new Date(Date.now() + 120_000).toISOString();
  return {
    approval_id: 'approval-e2e-1',
    tool: TOOL,
    nonce: 'nonce-e2e-1',
    run_id: 'run-e2e-1',
    task_id: 'task-e2e-1',
    tool_call_id: 'call-e2e-1',
    args_sha256: HASH_A,
    expires_at: expiresAt,
    ...overrides,
  };
}

function approvalCard(page: Page) {
  // Inline chat card (ApprovalOverlay renders the modal only when the chat is hidden).
  return page.locator('[data-testid="chat-inline-approval"]');
}

/** Two clicks in the same JS task: React has not re-rendered yet, so only the store guard can dedupe. */
async function doubleClickInSameTask(page: Page, label: string): Promise<void> {
  await page.evaluate((ariaLabel) => {
    const button = document.querySelector<HTMLButtonElement>(`button[aria-label="${ariaLabel}"]`);
    if (!button) {
      throw new Error(`approval action button not found: ${ariaLabel}`);
    }
    button.click();
    button.click();
  }, label);
}

test.describe('W12 approval flow — validated builder, labelled actions, read-only expiry (Wave 4)', () => {
  test('request renders tool, expiry and both enabled actions; nothing is sent on render', async ({ page }) => {
    const ws = await openConnected(page);
    sendApprovalRequest(ws, approvalPayload(), 1);

    const card = approvalCard(page);
    await expect(card).toBeVisible({ timeout: 10_000 });
    await expect(card).toHaveAttribute('role', 'alert');
    await expect(card).toHaveAttribute('aria-label', `Approval needed: ${TOOL}`);
    await expect(card).toContainText(TOOL);
    // The inline card keeps the expiry as a titled span; the modal's
    // dd[aria-live="polite"] field set no longer exists on this surface.
    await expect(card.locator('span[title]').filter({ hasText: /expires in / })).toBeVisible();
    test.info().annotations.push({
      type: 'surface-gap',
      description:
        'inline approval card renders tool+expiry only; run/task/tool_call/approval ids and args_sha256 are not displayed while chat is visible (modal suppressed at ApprovalOverlay.tsx:253)',
    });

    await expect(card.getByRole('button', { name: APPROVE_LABEL })).toBeEnabled();
    await expect(card.getByRole('button', { name: DENY_LABEL })).toBeEnabled();
    expect(ws.approvalResolveCommands()).toHaveLength(0);
  });

  test('approve sends one APPROVAL_RESOLVE with the exact nonce after the click; ACK does not close the card', async ({ page }) => {
    const ws = await openConnected(page);
    sendApprovalRequest(ws, approvalPayload(), 1);
    const card = approvalCard(page);
    await expect(card).toBeVisible({ timeout: 10_000 });

    await card.getByRole('button', { name: APPROVE_LABEL }).click();
    const command = await ws.waitForCommand('APPROVAL_RESOLVE');
    expect(command.payload?.approval_id).toBe('approval-e2e-1');
    expect(command.payload?.nonce).toBe('nonce-e2e-1');
    expect(command.payload?.decision).toBe('approve');

    // No optimistic success and NO ACK unlock: the card stays open and both actions remain
    // disabled after the mock ACK until the server APPROVAL_RESOLVED removes the approval.
    await expect(card).toBeVisible();
    await expect(card.getByRole('status')).toContainText(/Resolving approval/);
    await expect(card.getByRole('button', { name: APPROVE_LABEL })).toBeDisabled();
    await expect(card.getByRole('button', { name: DENY_LABEL })).toBeDisabled();
    expect(ws.approvalResolveCommands()).toHaveLength(1);

    ws.route().send(
      eventFrame('APPROVAL_RESOLVED', { approval_id: 'approval-e2e-1', tool: TOOL, decision: 'approve' }, 2),
    );
    await expect(approvalCard(page)).toHaveCount(0, { timeout: 10_000 });
    expect(ws.approvalResolveCommands()).toHaveLength(1);
  });

  test('a same-task double click cannot send two commands (store resolving guard)', async ({ page }) => {
    const ws = await openConnected(page);
    sendApprovalRequest(ws, approvalPayload({ approval_id: 'approval-e2e-double', nonce: 'nonce-e2e-double' }), 1);
    const card = approvalCard(page);
    await expect(card).toBeVisible({ timeout: 10_000 });

    await doubleClickInSameTask(page, APPROVE_LABEL);
    await ws.waitForCommand('APPROVAL_RESOLVE');
    // Stability window: a second command must not arrive after the first was captured.
    // (Asserting length immediately is racy: the duplicate could land after the read.)
    await expect
      .poll(
        async () => {
          const count = ws.approvalResolveCommands().length;
          await page.waitForTimeout(75);
          return ws.approvalResolveCommands().length === count ? count : -1;
        },
        { timeout: 2_000 },
      )
      .toBe(1);

    ws.route().send(
      eventFrame('APPROVAL_RESOLVED', { approval_id: 'approval-e2e-double', tool: TOOL, decision: 'approve' }, 2),
    );
    await expect(approvalCard(page)).toHaveCount(0, { timeout: 10_000 });
    expect(ws.approvalResolveCommands()).toHaveLength(1);
  });

  test('deny sends decision=deny with the exact nonce and operator reason', async ({ page }) => {
    const ws = await openConnected(page);
    sendApprovalRequest(ws, approvalPayload({ approval_id: 'approval-e2e-deny', nonce: 'nonce-e2e-deny' }), 1);
    const card = approvalCard(page);
    await expect(card).toBeVisible({ timeout: 10_000 });

    await card.getByRole('button', { name: DENY_LABEL }).click();
    const command = await ws.waitForCommand('APPROVAL_RESOLVE');
    expect(command.payload?.approval_id).toBe('approval-e2e-deny');
    expect(command.payload?.nonce).toBe('nonce-e2e-deny');
    expect(command.payload?.decision).toBe('deny');
    // The chat-origin deny marks its origin (web/src/chat/ChatCore.tsx:865); the modal
    // path keeps the bare 'operator denied' (ApprovalOverlay.tsx:278). The behaviour is
    // unchanged — a deny carries the exact nonce and a named operator reason.
    expect(command.payload?.reason).toBe('operator denied in chat');

    ws.route().send(
      eventFrame('APPROVAL_RESOLVED', { approval_id: 'approval-e2e-deny', tool: TOOL, decision: 'deny' }, 2),
    );
    await expect(approvalCard(page)).toHaveCount(0, { timeout: 10_000 });
    expect(ws.approvalResolveCommands()).toHaveLength(1);
  });

  test('duplicate APPROVAL_REQUEST with the same id replaces the card instead of stacking dialogs', async ({ page }) => {
    const ws = await openConnected(page);
    sendApprovalRequest(ws, approvalPayload(), 1);
    const card = approvalCard(page);
    await expect(card).toBeVisible({ timeout: 10_000 });

    // A replayed/re-sent request for the same approval_id must replace by id (store
    // reducer filters by approval_id); it must never stack a second card or send a command.
    sendApprovalRequest(ws, approvalPayload(), 2);
    await expect(page.locator('[data-testid="chat-inline-approval"]')).toHaveCount(1);
    await expect(approvalCard(page)).toBeVisible();
    expect(ws.approvalResolveCommands()).toHaveLength(0);
  });

  test('request without nonce is non-resolvable: actions disabled, nothing sent, gap named', async ({ page }) => {
    const ws = await openConnected(page);
    sendApprovalRequest(ws, { approval_id: 'approval-e2e-nonce-less', tool: TOOL, decision: null, nonce: null }, 1);
    const card = approvalCard(page);
    await expect(card).toBeVisible({ timeout: 10_000 });
    await expect(card.getByRole('button', { name: APPROVE_LABEL })).toBeDisabled();
    await expect(card.getByRole('button', { name: DENY_LABEL })).toBeDisabled();
    await expect(card).toContainText(/did not carry the nonce/);
    expect(ws.approvalResolveCommands()).toHaveLength(0);
  });

  test('expired approval is read-only: expiry rendered, both actions disabled, nothing sent', async ({ page }) => {
    const ws = await openConnected(page);
    const past = new Date(Date.now() - 5_000).toISOString();
    sendApprovalRequest(
      ws,
      approvalPayload({ approval_id: 'approval-e2e-expired', nonce: 'nonce-e2e-expired', expires_at: past }),
      1,
    );
    const card = approvalCard(page);
    await expect(card).toBeVisible({ timeout: 10_000 });
    await expect(card).toContainText('expired');

    // Wave-4 verified: expiry is read-only — isApprovalExpired() feeds canResolveApproval(),
    // so both actions are disabled and nothing is sent or claimed resolved.
    await expect(card.getByRole('button', { name: APPROVE_LABEL })).toBeDisabled();
    await expect(card.getByRole('button', { name: DENY_LABEL })).toBeDisabled();
    expect(ws.approvalResolveCommands()).toHaveLength(0);
  });

  test('malformed APPROVAL_REQUEST is refused by the decoder (E_SCHEMA_INVALID, no card)', async ({ page }) => {
    const ws = await openConnected(page);
    sendMalformedEvent(ws, 'APPROVAL_REQUEST', { tool: TOOL, nonce: 'nonce-x', args_sha256: HASH_A }, 1);
    await expect(page.getByRole('alert').filter({ hasText: 'E_SCHEMA_INVALID' }).first()).toBeVisible({
      timeout: 10_000,
    });
    await expect(page.locator('[data-testid="chat-inline-approval"]')).toHaveCount(0);
    expect(ws.approvalResolveCommands()).toHaveLength(0);
  });
});
