/**
 * Bottom chat dock usability pins.
 *
 * These are regression tests for the operator-reported failures:
 *   - the run refusal and session block consumed the transcript,
 *   - the composer could be pushed off-screen,
 *   - the approval card lived outside the conversation,
 *   - the dock carried two Hide controls.
 *
 * Pure geometry lives in `src/chat/chatDock.ts`; the DOM assertions render the
 * real `ChatCore` with the same store fixture style as `chatTranscriptCap.test`.
 */
import { createElement, type ReactElement } from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { describe, expect, it } from 'vitest';

import type { UiApi } from '../src/api/client';
import {
  ChatCore,
  DEFAULT_CHAT_HEIGHT,
  DOCK_TRAY_HEIGHT,
  MAX_CHAT_HEIGHT,
  MIN_CHAT_HEIGHT,
  clampChatHeight,
  dockHeight,
  readChatHeight,
} from '../src/chat/ChatCore';
import chatCoreSource from '../src/chat/ChatCore.tsx?raw';
import { UiStoreContext } from '../src/state/context';
import {
  createUiStore,
  type ApprovalRecord,
  type UiState,
} from '../src/state/store';
import { STRINGS } from '../src/state/strings';

const SESSION = { token: 'test-token', schema: 'hiverouter.v1', last_seq: 0, ws: '/ws' };

function fakeApi(): UiApi {
  return {
    kind: 'ws',
    async open() {
      return SESSION;
    },
    close() {
      /* no session to close in a static render */
    },
    async send() {
      /* commands are not sent by static renders */
    },
    subscribe() {
      return () => undefined;
    },
    subscribeStatus() {
      return () => undefined;
    },
    subscribeClientError() {
      return () => undefined;
    },
  } as unknown as UiApi;
}

function renderWith(node: ReactElement, state: Partial<UiState> = {}): string {
  const store = createUiStore(fakeApi());
  store.setState({ connection: 'open' as const, chat: { pinned: true, visible: true }, ...state });
  (store as unknown as { getInitialState: () => UiState }).getInitialState = () =>
    store.getState();
  return renderToStaticMarkup(
    createElement(UiStoreContext.Provider, { value: store }, node),
  );
}

const countOf = (source: string, needle: string): number =>
  source.split(needle).length - 1;

function approval(overrides: Partial<ApprovalRecord> = {}): ApprovalRecord {
  return {
    approvalId: 'approval-1',
    tool: 'file_write',
    nonce: 'nonce-1',
    decision: null,
    requestedAt: Date.now(),
    runId: 'run-1',
    taskId: null,
    toolCallId: null,
    argsSha256: null,
    argsPreview: '{"path":"src/a.ts"}',
    expiresAt: null,
    ...overrides,
  };
}

describe('chat dock geometry', () => {
  it('keeps the 560px default and clamps every resize path', () => {
    expect(DEFAULT_CHAT_HEIGHT).toBe(560);
    expect(MIN_CHAT_HEIGHT).toBeGreaterThanOrEqual(400);
    expect(MAX_CHAT_HEIGHT).toBeGreaterThan(DEFAULT_CHAT_HEIGHT);
    expect(clampChatHeight(100)).toBe(MIN_CHAT_HEIGHT);
    expect(clampChatHeight(10_000)).toBe(MAX_CHAT_HEIGHT);
    expect(clampChatHeight(560.4)).toBe(560);
    expect(clampChatHeight(Number.NaN)).toBe(DEFAULT_CHAT_HEIGHT);
    expect(clampChatHeight(Number.POSITIVE_INFINITY)).toBe(DEFAULT_CHAT_HEIGHT);
  });

  it('adds the tray height only while a tray tab is open', () => {
    expect(dockHeight(560, null)).toBe(560);
    expect(dockHeight(560, 'controls')).toBe(560 + DOCK_TRAY_HEIGHT);
    expect(dockHeight(560, 'state')).toBe(560 + DOCK_TRAY_HEIGHT);
    expect(dockHeight(1, 'state')).toBe(MIN_CHAT_HEIGHT + DOCK_TRAY_HEIGHT);
  });

  it('reads storage defensively and migrates the old 390/480 defaults', () => {
    const storageOf = (value: string | null) => ({
      getItem: (key: string) => (key === 'hivemind.chat-height' ? value : null),
    });
    expect(readChatHeight(null)).toBe(DEFAULT_CHAT_HEIGHT);
    expect(readChatHeight(storageOf(null))).toBe(DEFAULT_CHAT_HEIGHT);
    expect(readChatHeight(storageOf('480'))).toBe(DEFAULT_CHAT_HEIGHT);
    expect(readChatHeight(storageOf('390'))).toBe(DEFAULT_CHAT_HEIGHT);
    expect(readChatHeight(storageOf('640'))).toBe(640);
    expect(readChatHeight(storageOf('not-a-number'))).toBe(DEFAULT_CHAT_HEIGHT);
    expect(readChatHeight(storageOf('9999'))).toBe(MAX_CHAT_HEIGHT);
    expect(
      readChatHeight({
        getItem: () => {
          throw new Error('storage denied');
        },
      }),
    ).toBe(DEFAULT_CHAT_HEIGHT);
  });
});

describe('chat dock layout', () => {
  it('renders the 560px default flush panel with a readable transcript floor', () => {
    const markup = renderWith(createElement(ChatCore), {
      messages: [{ id: 'm-1', role: 'user', text: 'hello', runId: null, streaming: false }],
    });
    expect(markup).toContain('height:560px');
    expect(markup).toContain('min-h-[140px]');
    expect(markup).toContain('aria-label="Chat transcript"');
    // The composer zone is the sticky, non-shrinking block above the footer.
    expect(markup).toContain('sticky bottom-0');
    expect(markup).toContain('id="composer"');
  });

  it('keeps the composer before the footer and has exactly one Hide control', () => {
    expect(countOf(chatCoreSource, 'setChatVisible(false)')).toBe(1);
    const composerAt = chatCoreSource.indexOf('id="composer"');
    const footerAt = chatCoreSource.indexOf('aria-label="Clear the visible chat transcript"');
    expect(composerAt).toBeGreaterThan(0);
    expect(footerAt).toBeGreaterThan(composerAt);
    // One canonical Hide button in the dock header; no second footer HIDE.
    expect(countOf(chatCoreSource, 'aria-label={STRINGS.chat.closeAria}')).toBe(1);
    expect(chatCoreSource).not.toContain('aria-label="Hide the chat dock"');
    // CLEAR stays available as the transcript-only reset.
    expect(chatCoreSource).toContain('aria-label="Clear the visible chat transcript"');
    expect(chatCoreSource).toContain('CLEAR');
  });

  it('moves session, policy, archive, create and root-control into the STATE tray', () => {
    const transcriptAt = chatCoreSource.indexOf('aria-label="Chat transcript"');
    const trayAt = chatCoreSource.indexOf('id="chat-dock-tray"');
    const sessionAt = chatCoreSource.indexOf('SESSION &amp; PERMISSIONS');
    expect(transcriptAt).toBeGreaterThan(0);
    expect(trayAt).toBeGreaterThan(transcriptAt);
    expect(sessionAt).toBeGreaterThan(trayAt);
    // No copy of the session block remains between the transcript and composer.
    expect(countOf(chatCoreSource, 'value={namedSession}')).toBe(1);
    expect(countOf(chatCoreSource, 'value={permissionPolicy}')).toBe(1);
    expect(countOf(chatCoreSource, 'ROOT CONTROL')).toBe(1);
    // Collapsed by default and capped so it cannot swallow the transcript again.
    expect(chatCoreSource).toContain('<details className="max-h-[168px] overflow-y-auto');
  });
});

describe('chat dock error + approval states', () => {
  it('renders a compact run refusal with a named dismiss action', () => {
    const markup = renderWith(createElement(ChatCore), {
      run: { id: 'run-err', status: 'failed', errorCode: 'E_KEY_MISSING' },
    });
    expect(markup).toContain('RUN REFUSED');
    expect(markup).toContain('E_KEY_MISSING');
    expect(markup).toContain('>Dismiss error</button>');
    expect(countOf(markup, 'role="alert"')).toBe(1);
    // The full guidance remains available on the message tooltip.
    expect(markup).toContain('title="');
  });

  it('renders the approval decision inline in the transcript, never in the footer', () => {
    const markup = renderWith(createElement(ChatCore), {
      approvals: [approval()],
    });
    const transcriptAt = markup.indexOf('aria-label="Chat transcript"');
    const approvalAt = markup.indexOf('data-testid="chat-inline-approval"');
    const composerAt = markup.indexOf('id="composer"');
    expect(transcriptAt).toBeGreaterThan(-1);
    expect(approvalAt).toBeGreaterThan(transcriptAt);
    expect(approvalAt).toBeLessThan(composerAt);
    expect(countOf(markup, '>Approve</button>')).toBe(1);
    expect(countOf(markup, '>Deny</button>')).toBe(1);
    expect(markup).toContain(STRINGS.controls.approve);
  });

  it('keeps every queued approval reachable from the inline card', () => {
    const markup = renderWith(createElement(ChatCore), {
      approvals: [approval(), approval({ approvalId: 'approval-2', tool: 'shell_exec' })],
    });
    expect(markup).toContain('Approval 1 of 2');
    expect(markup).toContain(STRINGS.approval.queue.previous);
    expect(markup).toContain(STRINGS.approval.queue.next);
  });

  it('renders a working local dismiss for a client-expired approval with no resolve record', () => {
    const markup = renderWith(createElement(ChatCore), {
      approvals: [approval({ expiresAt: '2000-01-01T00:00:00.000Z' })],
    });
    expect(markup).toContain(STRINGS.approval.expiry.expired);
    expect(countOf(markup, `>${STRINGS.approval.dismiss}</button>`)).toBe(1);
  });

  it('explains a nonce-less approval instead of offering enabled decisions', () => {
    const markup = renderWith(createElement(ChatCore), {
      approvals: [approval({ nonce: null })],
    });
    expect(markup).toContain(STRINGS.approval.nonceMissing);
    expect(markup).toContain('id="chat-inline-approval"');
  });

  it('keeps the default one-line view clear of the session block', () => {
    const markup = renderWith(createElement(ChatCore));
    expect(markup).not.toContain('SESSION &amp; PERMISSIONS');
    expect(markup).not.toContain('ROOT CONTROL');
    expect(markup).not.toContain('id="chat-dock-tray"');
  });
});
