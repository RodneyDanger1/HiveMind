/**
 * Browser harness for the checkpoint-restore UI (owned panels only).
 *
 * This page mounts the real `DiffReviewPanel` and `DataPanel` from `src/` and
 * stubs the HTTP transport at `window.fetch`, so a Playwright run can assert:
 *  - a completed run offers Restore -> Confirm restore;
 *  - the confirm dispatches the real `POST /api/runs/{run_id}/checkpoints/{seq}/restore`
 *    with the bearer token from `GET /api/session`;
 *  - the receipt and the refusal are rendered from the server body;
 *  - the Data panel asks for a post-restore FILE_TREE_LIST and FILE_READ.
 *
 * The transport is a test double (allowed); the components, the restore helper
 * and the rendered states are the production code.
 */
import { createElement } from 'react';
import { createRoot } from 'react-dom/client';

import type { UiApi } from '../../src/api/client';
import type { FileTreeEntry } from '../../src/api/contract';
import { DataPanel } from '../../src/panels/DataPanel';
import { DiffReviewPanel } from '../../src/panels/DiffReviewPanel';
import { UiStoreContext } from '../../src/state/context';
import { createUiStore } from '../../src/state/store';

interface RecordedCall {
  readonly url: string;
  readonly method: string;
  readonly authorization: string | null;
}

interface RecordedSend {
  readonly name: string;
  readonly payload: unknown;
}

declare global {
  interface Window {
    __calls: RecordedCall[];
    __sends: RecordedSend[];
    __receipts: string[];
  }
}

window.__calls = [];
window.__sends = [];
window.__receipts = [];

const PATH = 'live-content-writer-proof.md';
const CONTENT = 'CONTENT WRITER LIVE OK';

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'content-type': 'application/json' },
  });
}

/** Wire shape of `GET /api/runs/{run_id}/checkpoints` (snake_case FileCheckpoint). */
function checkpointBody(runId: string): unknown {
  return {
    run_id: runId,
    checkpoints: [
      {
        checkpoint_id: `cp-${runId}`,
        run_id: runId,
        seq: 1,
        path: PATH,
        before: { kind: 'absent' },
        after: { kind: 'present', sha256: 'c'.repeat(64), size: 22 },
        created_at: '2026-11-17T15:20:00Z',
        completed_at: '2026-11-17T15:20:01Z',
        restored_at: null,
      },
    ],
  };
}

function installFetch(): void {
  window.fetch = (async (input: string | URL | Request, init?: RequestInit) => {
    const url =
      typeof input === 'string' ? input : input instanceof URL ? input.toString() : input.url;
    const method = (init?.method ?? 'GET').toUpperCase();
    window.__calls.push({
      url,
      method,
      authorization: new Headers(init?.headers).get('authorization'),
    });

    if (url.endsWith('/api/session')) {
      return json({ token: 'harness-token' });
    }
    const restore = /\/api\/runs\/([^/]+)\/checkpoints\/1\/restore$/.exec(url);
    if (restore !== null && method === 'POST') {
      if (decodeURIComponent(restore[1]!) === 'run-drift') {
        return json(
          {
            code: 'E_CHECKPOINT_DRIFT',
            message: 'the file no longer matches the recorded after-state',
            how_to_fix: 'restore from a checkpoint whose after-state still matches',
            retryable: false,
          },
          409,
        );
      }
      return json({ path: PATH, undone_seqs: [1], restored: { kind: 'absent' } });
    }
    const list = /\/api\/runs\/([^/]+)\/checkpoints$/.exec(url);
    if (list !== null && method === 'GET') {
      return json(checkpointBody(decodeURIComponent(list[1]!)));
    }
    return json(
      {
        code: 'E_NOT_FOUND',
        message: `harness has no route for ${method} ${url}`,
        how_to_fix: 'add the route to the harness stub',
        retryable: false,
      },
      404,
    );
  }) as typeof fetch;
}

function fakeUiApi(): UiApi {
  const api: UiApi = {
    kind: 'ws',
    async open() {
      throw new Error('the harness never opens a socket');
    },
    close() {
      // No socket to close: the harness drives the panel through fetch only.
    },
    async send(name, payload) {
      window.__sends.push({ name, payload });
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
  };
  return api;
}

function mount(targetId: string, child: ReturnType<typeof createElement>): void {
  const target = document.getElementById(targetId);
  if (target === null) {
    throw new Error(`harness target #${targetId} is missing`);
  }
  createRoot(target).render(child);
}

installFetch();

mount(
  'success',
  createElement(DiffReviewPanel, {
    runId: 'run-live',
    runStatus: 'completed',
    onRestored: (receipt) => {
      window.__receipts.push(`restored:${receipt.path}`);
    },
  }),
);

mount(
  'error',
  createElement(DiffReviewPanel, {
    runId: 'run-drift',
    runStatus: 'failed',
    onRestored: (receipt) => {
      window.__receipts.push(`unexpected:${receipt.path}`);
    },
  }),
);

const ENTRIES: readonly FileTreeEntry[] = [
  { path: PATH, name: PATH, kind: 'file', size_bytes: 22 },
];

const store = createUiStore(fakeUiApi());
store.setState({
  connection: 'open',
  run: { id: 'run-refresh', status: 'completed', errorCode: null },
  fileTree: {
    status: 'ok',
    root: '/workspace',
    entries: ENTRIES,
    refusal: null,
    contentStatus: 'ok',
    content: { path: PATH, sha256: 'c'.repeat(64), bytes: 22, text: CONTENT },
    selectedPath: PATH,
  },
});

mount(
  'refresh',
  createElement(UiStoreContext.Provider, { value: store }, createElement(DataPanel)),
);
