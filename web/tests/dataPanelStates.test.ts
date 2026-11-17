import { createElement, type ReactElement } from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { describe, expect, it } from 'vitest';

import type { FileContentPayload, FileTreeEntry } from '../src/api/contract';
import type { UiApi } from '../src/api/client';
import { DataPanel, refreshAfterRestore } from '../src/panels/DataPanel';
import { UiStoreContext } from '../src/state/context';
import { createUiStore, type FileTreeState, type UiState } from '../src/state/store';
import dataPanelSource from '../src/panels/DataPanel.tsx?raw';

/**
 * Data surface state ownership: FileTree owns the tree states, Editor owns the
 * open-file states. These tests pin that DataPanel no longer paints a second
 * copy around them, and that a loading/failed read never shows stale file text.
 */

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
  store.setState({ connection: 'open' as const, ...state });
  if (Object.keys(state).length > 0) {
    store.setState(state);
  }
  (store as unknown as { getInitialState: () => UiState }).getInitialState = () =>
    store.getState();
  return renderToStaticMarkup(
    createElement(UiStoreContext.Provider, { value: store }, node),
  );
}

const countOf = (source: string, needle: string): number => source.split(needle).length - 1;

const ENTRIES: readonly FileTreeEntry[] = [
  { path: 'README.md', name: 'README.md', kind: 'file', size_bytes: 19 },
  { path: 'src/new.ts', name: 'new.ts', kind: 'file', size_bytes: 24 },
];

const STALE_CONTENT: FileContentPayload = {
  path: 'README.md',
  sha256: 'a'.repeat(64),
  bytes: 19,
  text: 'STALE BODY from the previously opened file',
};

function fileTree(overrides: Partial<FileTreeState> = {}): FileTreeState {
  return {
    status: 'ok',
    root: '/workspace',
    entries: ENTRIES,
    refusal: null,
    contentStatus: 'idle',
    content: null,
    selectedPath: null,
    ...overrides,
  };
}

describe('DataPanel state ownership', () => {
  it('does not render the previous file while the next read is loading', () => {
    const markup = renderWith(createElement(DataPanel), {
      fileTree: fileTree({
        contentStatus: 'loading',
        content: STALE_CONTENT,
        selectedPath: 'src/new.ts',
      }),
    });
    expect(markup).toContain('aria-label="File preview"');
    expect(markup).toContain('aria-busy="true"');
    expect(markup).toContain('Loading');
    expect(markup).not.toContain('STALE BODY');
    expect(markup).not.toContain('Editor: README.md');
  });

  it('renders one error state and retries the requested path, not the old content path', () => {
    const markup = renderWith(createElement(DataPanel), {
      fileTree: fileTree({
        contentStatus: 'error',
        content: STALE_CONTENT,
        selectedPath: 'src/new.ts',
      }),
    });
    // The tree is ready; only the preview error may render an alert.
    expect(countOf(markup, 'role="alert"')).toBe(1);
    expect(markup).toContain('Retry src/new.ts');
    expect(markup).not.toContain('STALE BODY');
    expect(markup).not.toContain('Editor: README.md');
    // Source pin: the retry target is the requested path, never content.path.
    expect(dataPanelSource).toContain('const retryPath = fileTree.selectedPath;');
    expect(dataPanelSource).not.toContain('fileTree.content?.path');
  });

  it('leaves the loading state to FileTree (one status, no duplicate panel copy)', () => {
    const markup = renderWith(createElement(DataPanel), {
      fileTree: fileTree({ status: 'loading' }),
    });
    expect(countOf(markup, 'role="status"')).toBe(1);
    expect(markup).toContain('Loading workspace files');
    expect(markup).not.toContain('Nothing here yet');
  });

  it('leaves the tree error to FileTree (one alert, no duplicate panel copy)', () => {
    const markup = renderWith(createElement(DataPanel), {
      fileTree: fileTree({ status: 'error' }),
    });
    expect(countOf(markup, 'role="alert"')).toBe(1);
    expect(countOf(markup, 'File tree unavailable')).toBe(1);
    expect(markup).not.toContain('Something went wrong');
    expect(markup).not.toContain('Nothing here yet');
  });

  it('leaves the empty tree to FileTree', () => {
    const markup = renderWith(createElement(DataPanel), {
      fileTree: fileTree({ entries: [] }),
    });
    expect(countOf(markup, 'No files in this folder.')).toBe(1);
    expect(markup).not.toContain('Nothing here yet');
  });

  it('renders the confirmed file only when the read completed', () => {
    const markup = renderWith(createElement(DataPanel), {
      fileTree: fileTree({
        contentStatus: 'ok',
        content: STALE_CONTENT,
        selectedPath: 'README.md',
      }),
    });
    expect(markup).toContain('Editor: README.md');
    expect(markup).toContain('STALE BODY');
    expect(markup).not.toContain('Retry README.md');
  });
});

describe('refresh after a checkpoint restore', () => {
  const receipt = {
    path: 'live-content-writer-proof.md',
    undoneSeqs: [1],
    restored: { kind: 'absent' as const },
  };

  it('refreshes the tree and re-reads the restored file when it is the open preview', () => {
    const calls: string[] = [];
    refreshAfterRestore(
      receipt,
      'live-content-writer-proof.md',
      async () => {
        calls.push('tree');
      },
      async (path) => {
        calls.push(`open:${path}`);
      },
    );
    expect(calls).toEqual(['tree', 'open:live-content-writer-proof.md']);
  });

  it('never re-opens an unrelated preview path', () => {
    const calls: string[] = [];
    refreshAfterRestore(
      receipt,
      'README.md',
      async () => {
        calls.push('tree');
      },
      async (path) => {
        calls.push(`open:${path}`);
      },
    );
    expect(calls).toEqual(['tree']);
  });

  it('still refreshes the tree with no file selected', () => {
    const calls: string[] = [];
    refreshAfterRestore(
      receipt,
      null,
      async () => {
        calls.push('tree');
      },
      async (path) => {
        calls.push(`open:${path}`);
      },
    );
    expect(calls).toEqual(['tree']);
  });

  it('wires the restored receipt into the Data panel refresh', () => {
    expect(dataPanelSource).toContain('onRestored={(receipt) => {');
    expect(dataPanelSource).toContain(
      'refreshAfterRestore(receipt, fileTree.selectedPath, requestFileTree, openFile)',
    );
  });
});
