/**
 * Chat/graph flow pins for the operator-reported dock bugs:
 *   - a terminal run still showed "running"/"N active" after CLI wait completion,
 *   - a streamed reply stayed marked streaming when another run took over,
 *   - transcript auto-scroll yanked the operator away from earlier text,
 *   - the dismissed error card hid a second, different failure on the same run,
 *   - the approval queue cursor was index-based and could shift onto another
 *     approval after one resolved,
 *   - session/refresh and canvas filter/status controls were dead in replay.
 *
 * Pure helpers are tested directly; store lifecycle uses the local FakeUiApi
 * pattern from streamingRender.test.ts.
 */
import { createElement, type ReactElement } from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { describe, expect, it } from 'vitest';

import type { ConnectionStatus, SessionInfo, UiApi } from '../src/api/client';
import { SCHEMA_VERSION } from '../src/api/contract';
import type { DecodedEvent, Envelope, Refusal, ServerCommandName } from '../src/api/contract';
import {
  ChatCore,
  approvalIndexFor,
  runActivity,
  runErrorKey,
  shouldAutoScroll,
} from '../src/chat/ChatCore';
import { Header, headerRunLabel } from '../src/panels/Header';
import { UiStoreContext } from '../src/state/context';
import { createUiStore, type ApprovalRecord, type UiState } from '../src/state/store';
import { STRINGS } from '../src/state/strings';
import chatCoreSource from '../src/chat/ChatCore.tsx?raw';
import canvasSource from '../src/flow/Canvas.tsx?raw';

class FakeUiApi implements UiApi {
  readonly kind = 'ws' as const;
  private readonly events = new Set<(event: DecodedEvent, envelope: Envelope<unknown>) => void>();
  private readonly sends: Array<{ resolve: () => void }> = [];

  async open(): Promise<SessionInfo> {
    return { token: 'fake', schema: SCHEMA_VERSION, last_seq: 0, ws: '/ws' };
  }
  close(): void {
    return;
  }
  send<N extends ServerCommandName>(
    _name: N,
    _payload: unknown,
    _options?: { readonly correlationId?: string },
  ): Promise<void> {
    return new Promise<void>((resolve) => {
      this.sends.push({ resolve });
    });
  }
  subscribe(listener: (event: DecodedEvent, envelope: Envelope<unknown>) => void): () => void {
    this.events.add(listener);
    return () => this.events.delete(listener);
  }
  subscribeStatus(_listener: (status: ConnectionStatus, detail?: Refusal) => void): () => void {
    return () => undefined;
  }
  subscribeClientError(_listener: (refusal: Refusal) => void): () => void {
    return () => undefined;
  }
  emit(event: DecodedEvent): void {
    const envelope: Envelope<unknown> = {
      schema: SCHEMA_VERSION,
      type: 'EVENT',
      name: event.name,
      id: 'e',
      correlation_id: 'e',
      run_id: null,
      task_id: null,
      seq: 1,
      ts: new Date().toISOString(),
      payload: {},
    };
    for (const listener of this.events) {
      listener(event, envelope);
    }
  }
  resolveNext(): void {
    this.sends.shift()?.resolve();
  }
}

async function runningStore(runId: string): Promise<{
  api: FakeUiApi;
  store: ReturnType<typeof createUiStore>;
}> {
  const api = new FakeUiApi();
  const store = createUiStore(api);
  await store.getState().init();
  const submission = store.getState().submitPrompt('hello hive');
  await Promise.resolve();
  api.resolveNext();
  await submission;
  api.emit({ name: 'RUN_CREATED', payload: { run_id: runId, status: 'running' } });
  return { api, store };
}

const SESSION = { token: 'test-token', schema: 'hiverouter.v1', last_seq: 0, ws: '/ws' };

function staticApi(): UiApi {
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
  const store = createUiStore(staticApi());
  store.setState({ connection: 'open' as const, chat: { pinned: true, visible: true }, ...state });
  (store as unknown as { getInitialState: () => UiState }).getInitialState = () =>
    store.getState();
  return renderToStaticMarkup(createElement(UiStoreContext.Provider, { value: store }, node));
}

function openingButton(markup: string, ariaLabel: string): string {
  const index = markup.indexOf(`aria-label="${ariaLabel}"`);
  expect(index).toBeGreaterThan(-1);
  const start = markup.lastIndexOf('<button', index);
  const end = markup.indexOf('>', index);
  return markup.slice(start, end + 1);
}

const assistantRows = (store: ReturnType<typeof createUiStore>) =>
  store.getState().messages.filter((message) => message.role === 'assistant');

describe('run activity label (terminal status beats a stale stream)', () => {
  it('reports idle for a completed run even when streamingRunId is stale', () => {
    expect(
      runActivity({
        connection: 'open',
        runStatus: 'completed',
        streamingRunId: 'run-1',
        activeAgents: 3,
      }),
    ).toEqual({ label: 'idle', tone: 'idle', live: false });
  });

  it('names a failed terminal run as failed, never as active', () => {
    const activity = runActivity({
      connection: 'open',
      runStatus: 'failed',
      streamingRunId: null,
      activeAgents: 1,
    });
    expect(activity.tone).toBe('failed');
    expect(activity.live).toBe(false);
  });

  it('keeps live labels while the run can still emit events', () => {
    expect(
      runActivity({ connection: 'open', runStatus: 'running', streamingRunId: null, activeAgents: 2 }),
    ).toEqual({ label: '2 active', tone: 'live', live: true });
    expect(
      runActivity({ connection: 'open', runStatus: 'planning', streamingRunId: null, activeAgents: 0 })
        .label,
    ).toBe('planning');
    expect(
      runActivity({ connection: 'closed', runStatus: 'running', streamingRunId: null, activeAgents: 2 })
        .tone,
    ).toBe('offline');
  });
});

describe('transcript follow and run-error identity', () => {
  it('follows only near the bottom', () => {
    const base = { scrollHeight: 1000, clientHeight: 200 };
    expect(shouldAutoScroll({ ...base, scrollTop: 800 })).toBe(true);
    expect(shouldAutoScroll({ ...base, scrollTop: 760 })).toBe(true);
    expect(shouldAutoScroll({ ...base, scrollTop: 700 })).toBe(false);
    expect(shouldAutoScroll({ ...base, scrollTop: Number.NaN })).toBe(true);
  });

  it('reopens the error card for a new code on the same run', () => {
    expect(runErrorKey('run-1', 'E_KEY_MISSING')).toBe('run-1:E_KEY_MISSING');
    expect(runErrorKey('run-1', 'E_KEY_MISSING')).not.toBe(
      runErrorKey('run-1', 'E_SCHEMA_INVALID'),
    );
    expect(runErrorKey(null, 'E_KEY_MISSING')).toBeNull();
    expect(runErrorKey('run-1', null)).toBeNull();
  });

  it('resolves the approval cursor by identity, never by shifted index', () => {
    const approvals = [{ approvalId: 'a' }, { approvalId: 'b' }, { approvalId: 'c' }];
    expect(approvalIndexFor(approvals, 'b')).toBe(1);
    expect(approvalIndexFor(approvals, 'c')).toBe(2);
    expect(approvalIndexFor(approvals, null)).toBe(0);
    expect(approvalIndexFor(approvals, 'gone')).toBe(0);
    expect(approvalIndexFor([], 'a')).toBe(0);
  });
});

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

describe('dock status and controls', () => {
  it('renders idle (not running) after a completed run with a stale stream id', () => {
    const markup = renderWith(createElement(ChatCore), {
      run: { id: 'run-1', status: 'completed', errorCode: null },
      streamingRunId: 'run-1',
      messages: [
        { id: 'm-1', role: 'user', text: 'hello', runId: 'run-1', streaming: false },
        { id: 'm-2', role: 'assistant', text: 'done', runId: 'run-1', streaming: false },
      ],
    });
    expect(markup).toContain('data-run-activity="idle"');
    expect(markup).not.toContain(`>${STRINGS.agentStates.streaming}<`);
    expect(markup).not.toContain('data-run-activity="live"');
  });

  it('marks a running run live and leaves Stop enabled', () => {
    const markup = renderWith(createElement(ChatCore), {
      run: { id: 'run-1', status: 'running', errorCode: null },
    });
    expect(markup).toContain('data-run-activity="live"');
    expect(openingButton(markup, 'Stop the active run')).not.toContain('disabled=""');
  });

  it('disables Stop for a terminal run and for planning-free idle', () => {
    const completed = renderWith(createElement(ChatCore), {
      run: { id: 'run-1', status: 'completed', errorCode: null },
    });
    expect(openingButton(completed, 'Stop the active run')).toContain('disabled=""');
    const idle = renderWith(createElement(ChatCore));
    expect(openingButton(idle, 'Stop the active run')).toContain('disabled=""');
  });

  it('keeps the approval queue index tied to the displayed approval', () => {
    const markup = renderWith(createElement(ChatCore), {
      approvals: [approval(), approval({ approvalId: 'approval-2', tool: 'shell_exec' })],
    });
    expect(markup).toContain('Approval 1 of 2');
    // Source pin: navigation selects an id; a plain index could point at a
    // different approval after one resolves.
    expect(chatCoreSource).toContain('setApprovalCursorId');
    expect(chatCoreSource).not.toContain('setApprovalCursor(');
    expect(chatCoreSource).toContain('approvalIndexFor(approvals, approvalCursorId)');
  });

  it('wires transcript follow, jump-to-latest and session refresh', () => {
    expect(chatCoreSource).toContain('onScroll={onTranscriptScroll}');
    expect(chatCoreSource).toContain('data-testid="chat-jump-latest"');
    expect(chatCoreSource).toContain('shouldAutoScroll({');
    expect(chatCoreSource).toContain('title="Reload the session list from the server"');
    expect(chatCoreSource).toContain('aria-busy={sessionBusy}');
    expect(chatCoreSource).toContain('No named sessions yet');
  });
});

describe('header run status', () => {
  it('names every run lifecycle state and replay', () => {
    expect(headerRunLabel(false, null)).toBe('No run');
    expect(headerRunLabel(false, 'completed')).toBe(STRINGS.agentStates.completed);
    expect(headerRunLabel(true, 'running')).toBe(STRINGS.states.replay);
  });

  it('renders the run chip on the header for completed and missing runs', () => {
    const completed = renderWith(createElement(Header), {
      run: { id: 'run-1', status: 'completed', errorCode: null },
    });
    expect(completed).toContain('data-testid="header-run-status"');
    expect(completed).toContain(STRINGS.agentStates.completed);
    expect(completed).toContain('data-run-status="completed"');

    const none = renderWith(createElement(Header));
    expect(none).toContain('data-run-status="none"');
    expect(none).toContain('No run');
  });

  it('renders replay instead of a live status during time travel', () => {
    const replay = renderWith(createElement(Header), {
      run: { id: 'run-1', status: 'running', errorCode: null },
      history: {
        ...createUiStore(staticApi()).getState().history,
        replayGraph: { revision: 1, graph: {} },
      },
    });
    expect(replay).toContain('data-run-status="replay"');
    expect(replay).toContain(STRINGS.states.replay);
  });
});

describe('canvas status and filter honesty', () => {
  it('derives the zoom label from the live viewport', () => {
    expect(canvasSource).toContain('const zoomLabel = zoomPercent(viewport.zoom);');
    expect(canvasSource).not.toContain('setZoomLabel');
  });

  it('disables replay-dead filters and exposes the run state', () => {
    expect(canvasSource).toContain('disabled={replaying || Object.keys(tasks).length === 0}');
    expect(canvasSource).toContain('disabled={replaying || selectedTaskId === null}');
    expect(canvasSource).toContain('data-testid="canvas-run-status"');
    expect(canvasSource).toContain('RUN · {runLabel}');
    expect(canvasSource).toContain('if (selectedTaskId === null && focusBranch)');
  });
});

describe('store run lifecycle (CLI wait completion)', () => {
  it('closes a streamed row from a terminal RUN_STATUS even without REPLY_DONE', async () => {
    const { api, store } = await runningStore('run-wait');
    api.emit({ name: 'REPLY_DELTA', payload: { run_id: 'run-wait', text_delta: 'final text' } });
    expect(store.getState().streamingRunId).toBe('run-wait');

    api.emit({
      name: 'RUN_STATUS',
      payload: { run_id: 'run-wait', status: 'completed', error_code: null },
    });
    expect(store.getState().run?.status).toBe('completed');
    expect(store.getState().streamingRunId).toBeNull();
    expect(assistantRows(store)[0]?.streaming).toBe(false);
    expect(assistantRows(store)[0]?.text).toBe('final text');
  });

  it('finalizes the old run row after another run takes over', async () => {
    const { api, store } = await runningStore('run-old');
    api.emit({ name: 'REPLY_DELTA', payload: { run_id: 'run-old', text_delta: 'old reply' } });
    api.emit({ name: 'RUN_CREATED', payload: { run_id: 'run-new', status: 'running' } });
    expect(assistantRows(store)[0]?.streaming).toBe(true);

    api.emit({
      name: 'RUN_STATUS',
      payload: { run_id: 'run-old', status: 'completed', error_code: null },
    });
    expect(assistantRows(store)[0]?.streaming).toBe(false);
    expect(store.getState().run?.id).toBe('run-new');
    expect(store.getState().run?.status).toBe('running');
  });

  it('finalizes a done reply for a non-current run without clearing the live stream', async () => {
    const { api, store } = await runningStore('run-a');
    api.emit({ name: 'REPLY_DELTA', payload: { run_id: 'run-a', text_delta: 'a text' } });
    api.emit({ name: 'RUN_CREATED', payload: { run_id: 'run-b', status: 'running' } });
    api.emit({ name: 'REPLY_DELTA', payload: { run_id: 'run-b', text_delta: 'b text' } });
    expect(store.getState().streamingRunId).toBe('run-b');

    api.emit({
      name: 'REPLY_DONE',
      payload: {
        run_id: 'run-a',
        usage: {
          prompt_tokens: 1,
          completion_tokens: 1,
          total_tokens: 2,
          prompt_cache_hit_tokens: 0,
          prompt_cache_miss_tokens: 1,
          reasoning_tokens: null,
        },
      },
    });
    const rows = assistantRows(store);
    expect(rows.find((row) => row.runId === 'run-a')?.streaming).toBe(false);
    expect(rows.find((row) => row.runId === 'run-b')?.streaming).toBe(true);
    expect(store.getState().streamingRunId).toBe('run-b');
  });

  it('does not adopt a stale terminal run when no record exists', async () => {
    const api = new FakeUiApi();
    const store = createUiStore(api);
    await store.getState().init();
    api.emit({
      name: 'RUN_STATUS',
      payload: { run_id: 'run-past', status: 'completed', error_code: null },
    });
    expect(store.getState().run).toBeNull();
  });

  it('adopts an active run learned only from its status frame', async () => {
    const api = new FakeUiApi();
    const store = createUiStore(api);
    await store.getState().init();
    api.emit({
      name: 'RUN_STATUS',
      payload: { run_id: 'run-cli', status: 'running', error_code: null },
    });
    expect(store.getState().run).toMatchObject({ id: 'run-cli', status: 'running' });
  });
});
