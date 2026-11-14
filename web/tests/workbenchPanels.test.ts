import { describe, expect, it } from 'vitest';
import { createElement } from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { SCHEMA_VERSION, type DecodedEvent, type Envelope } from '../src/api/contract';
import { FakeUiApi } from './helpers/fakeUiApi';
import { EditorPanel } from '../src/panels/EditorPanel';
import { DiffReviewPanel, FileChangeTable } from '../src/panels/DiffReviewPanel';
import { TerminalPanel } from '../src/panels/TerminalPanel';
import { MonitorControls } from '../src/panels/MonitorControls';
import {
  ACK_MEANING,
  WORKBENCH_HOPS,
  cancelRun,
  formatFileState,
  isKillableTaskState,
  isTerminalRunStatus,
  killJob,
  killWorker,
  loadRunFileChanges,
  missingHopDetail,
  restoreCheckpoint,
  parseFileState,
  parseRunFileChanges,
  readWorkspaceFile,
  type RunFileChange,
  type WorkspaceApi,
} from '../src/panels/workbenchContracts';

function render(node: Parameters<typeof renderToStaticMarkup>[0]): string {
  return renderToStaticMarkup(node);
}

interface EmitCall {
  readonly event: DecodedEvent;
  readonly correlationId: string;
}

/** Minimal correlated UiApi double: records sends and emits envelopes by correlation id. */
function makeApi(): {
  readonly api: WorkspaceApi;
  readonly sends: Array<{ name: string; payload: unknown; correlationId?: string }>;
  readonly emit: (call: EmitCall) => void;
  readonly listenerCount: () => number;
} {
  const sends: Array<{ name: string; payload: unknown; correlationId?: string }> = [];
  const listeners = new Set<(event: DecodedEvent, envelope: Envelope<unknown>) => void>();
  const api: WorkspaceApi = {
    send: ((name: string, payload: unknown, options?: { correlationId?: string }) => {
      sends.push({ name, payload, ...(options?.correlationId ? { correlationId: options.correlationId } : {}) });
      return Promise.resolve();
    }) as WorkspaceApi['send'],
    subscribe: (listener) => {
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },
  };
  return {
    api,
    sends,
    emit: ({ event, correlationId }) => {
      const envelope: Envelope<unknown> = {
        schema: SCHEMA_VERSION,
        type: 'EVENT',
        name: event.name,
        id: correlationId,
        correlation_id: correlationId,
        run_id: null,
        task_id: null,
        seq: 1,
        ts: '2026-11-13T00:00:00Z',
        payload: {},
      };
      for (const listener of listeners) {
        listener(event, envelope);
      }
    },
    listenerCount: () => listeners.size,
  };
}

const FILE_CONTENT: DecodedEvent = {
  name: 'FILE_CONTENT',
  payload: { path: 'src/a.ts', sha256: 'a'.repeat(64), bytes: 11, text: 'const a = 1;' },
};

describe('workbench capability map', () => {
  it('marks only routes that exist in the repository as wired', () => {
    expect(WORKBENCH_HOPS['editor.read'].status).toBe('wired');
    expect(WORKBENCH_HOPS['editor.read'].contract).toContain('FILE_READ');
    expect(WORKBENCH_HOPS['diff.listRunChanges'].contract).toBe('GET /api/runs/{run_id}/checkpoints');
    expect(WORKBENCH_HOPS['monitor.cancelRun'].contract).toBe('WS CANCEL_RUN');
    expect(WORKBENCH_HOPS['monitor.killWorker'].contract).toBe('WS KILL_WORKER');
    expect(WORKBENCH_HOPS['monitor.killJob'].contract).toBe('POST /api/jobs/{job_id}/kill');
    expect(WORKBENCH_HOPS['monitor.runDetail'].status).toBe('wired');
  });

  it('names the exact missing route for every unwired hop', () => {
    const save = WORKBENCH_HOPS['editor.save'];
    expect(save.status).toBe('missing');
    expect(save.missing).toContain('POST /api/workspace/file');
    expect(save.missing).toContain('FILE_WRITE');

    const content = WORKBENCH_HOPS['diff.content'];
    expect(content.status).toBe('missing');
    expect(content.missing).toContain('DIFF_PROPOSE');
    expect(content.missing).toContain('pre-image');

    const apply = WORKBENCH_HOPS['diff.apply'];
    expect(apply.status).toBe('missing');
    expect(apply.missing).toContain('DIFF_APPLY');

    const terminal = WORKBENCH_HOPS['terminal.open'];
    expect(terminal.status).toBe('missing');
    expect(terminal.missing).toContain('/api/terminal');
    expect(terminal.missing).toContain('SHELL_EXEC');

    const instruct = WORKBENCH_HOPS['monitor.instructTask'];
    expect(instruct.status).toBe('missing');
    expect(instruct.missing).toContain('POST /api/runs/{run_id}/tasks/{task_id}/instruct');
    expect(instruct.missing).toContain('TASK_INSTRUCT');

    const retry = WORKBENCH_HOPS['monitor.retryTask'];
    expect(retry.status).toBe('missing');
    expect(retry.missing).toContain('POST /api/runs/{run_id}/tasks/{task_id}/retry');
    expect(retry.missing).toContain('TASK_RETRY');
  });

  it('cites repository evidence and reports missing detail only for missing hops', () => {
    for (const hop of Object.values(WORKBENCH_HOPS)) {
      expect(hop.evidence).toMatch(/\.(rs|ts):\d/);
      if (hop.status === 'wired') {
        expect(missingHopDetail(hop.id)).toBeNull();
      } else {
        expect(missingHopDetail(hop.id)).toContain('No ');
      }
    }
    expect(ACK_MEANING).toContain('ACK');
    expect(ACK_MEANING).toContain('not');
  });
});

describe('monitor command payloads', () => {
  it('sends CANCEL_RUN with the frozen run_id payload', async () => {
    const fake = new FakeUiApi();
    await cancelRun(fake, 'run-1');
    expect(fake.sends).toEqual([{ name: 'CANCEL_RUN', payload: { run_id: 'run-1' } }]);
  });

  it('sends KILL_WORKER with the frozen task_id payload', async () => {
    const fake = new FakeUiApi();
    await killWorker(fake, 'task-1');
    expect(fake.sends).toEqual([{ name: 'KILL_WORKER', payload: { task_id: 'task-1' } }]);
  });

  it('refuses an empty id before sending anything', async () => {
    const fake = new FakeUiApi();
    await expect(cancelRun(fake, '  ')).rejects.toMatchObject({ refusal: { code: 'E_RUN_NOT_FOUND' } });
    await expect(killWorker(fake, '')).rejects.toMatchObject({ refusal: { code: 'E_TASK_NOT_FOUND' } });
    expect(fake.sends).toHaveLength(0);
  });

  it('posts the real job-kill route and maps a refusal body', async () => {
    const calls: Array<{ url: string; method: string | undefined }> = [];
    const okFetch = async (url: string, init?: RequestInit): Promise<Response> => {
      calls.push({ url, method: init?.method });
      return new Response(JSON.stringify({ killed: true }), { status: 200 });
    };
    await killJob('job-1', { fetchImpl: okFetch });
    expect(calls).toEqual([{ url: '/api/jobs/job-1/kill', method: 'POST' }]);

    const refuseFetch = async (): Promise<Response> =>
      new Response(
        JSON.stringify({ code: 'E_JOB_NOT_FOUND', message: 'no such job', how_to_fix: 'list jobs', retryable: false }),
        { status: 404 },
      );
    await expect(killJob('job-2', { fetchImpl: refuseFetch })).rejects.toMatchObject({
      refusal: { code: 'E_JOB_NOT_FOUND', how_to_fix: 'list jobs' },
    });
  });
});

describe('FILE_READ hop', () => {
  it('resolves only on a correlated FILE_CONTENT and unsubscribes', async () => {
    const { api, sends, emit, listenerCount } = makeApi();
    const pending = readWorkspaceFile(api, 'src/a.ts', { correlationId: 'c1', timeoutMs: 500 });
    expect(sends).toEqual([{ name: 'FILE_READ', payload: { path: 'src/a.ts' }, correlationId: 'c1' }]);
    emit({ event: FILE_CONTENT, correlationId: 'c1' });
    await expect(pending).resolves.toEqual({
      path: 'src/a.ts',
      sha256: 'a'.repeat(64),
      bytes: 11,
      text: 'const a = 1;',
    });
    expect(listenerCount()).toBe(0);
  });

  it('rejects with the correlated server refusal', async () => {
    const { api, emit } = makeApi();
    const pending = readWorkspaceFile(api, 'missing.ts', { correlationId: 'c2', timeoutMs: 500 });
    emit({
      event: {
        name: 'ERROR',
        payload: { code: 'E_PATH_NOT_FOUND', message: 'gone', how_to_fix: 'pick another', retryable: false },
      },
      correlationId: 'c2',
    });
    await expect(pending).rejects.toMatchObject({ refusal: { code: 'E_PATH_NOT_FOUND' } });
  });

  it('refuses jail escapes locally and path mismatches from the server', async () => {
    const { api, sends, emit } = makeApi();
    await expect(readWorkspaceFile(api, '../secret.txt')).rejects.toMatchObject({
      refusal: { code: 'E_PATH_OUTSIDE_JAIL' },
    });
    expect(sends).toHaveLength(0);

    const mismatch = readWorkspaceFile(api, 'src/a.ts', { correlationId: 'c3', timeoutMs: 500 });
    const otherFile: DecodedEvent = {
      name: 'FILE_CONTENT',
      payload: { path: 'src/other.ts', sha256: 'a'.repeat(64), bytes: 11, text: 'x' },
    };
    emit({ event: otherFile, correlationId: 'c3' });
    await expect(mismatch).rejects.toMatchObject({ refusal: { code: 'E_SCHEMA_INVALID' } });
  });

  it('rejects instead of hanging when no event arrives', async () => {
    const { api } = makeApi();
    await expect(
      readWorkspaceFile(api, 'src/a.ts', { correlationId: 'c4', timeoutMs: 5 }),
    ).rejects.toMatchObject({ refusal: { code: 'E_WS_DISCONNECTED' } });
  });
});

describe('run checkpoint hop', () => {
  const wire = {
    run_id: 'run-1',
    checkpoints: [
      {
        checkpoint_id: 'cp-2',
        run_id: 'run-1',
        seq: 2,
        path: 'src/b.ts',
        before: { kind: 'absent' },
        after: { kind: 'present', sha256: 'b'.repeat(64), size: 12 },
        created_at: '2026-11-13T00:00:02Z',
        completed_at: '2026-11-13T00:00:03Z',
        restored_at: null,
      },
      {
        checkpoint_id: 'cp-1',
        run_id: 'run-1',
        seq: 1,
        path: 'src/a.ts',
        before: { kind: 'present', sha256: 'a'.repeat(64), size: 10 },
        after: null,
        created_at: '2026-11-13T00:00:01Z',
        completed_at: null,
        restored_at: null,
      },
    ],
  };

  it('parses the real FileCheckpoint shape and orders by journal sequence', () => {
    const parsed = parseRunFileChanges(wire);
    expect(parsed.runId).toBe('run-1');
    expect(parsed.changes.map((change) => change.seq)).toEqual([1, 2]);
    expect(parsed.changes[0]?.before).toEqual({ kind: 'present', sha256: 'a'.repeat(64), size: 10 });
    expect(parsed.changes[0]?.after).toBeNull();
    expect(parsed.changes[1]?.before).toEqual({ kind: 'absent' });
    expect(formatFileState(parsed.changes[0]?.before ?? null)).toContain('10 B');
  });

  it('refuses malformed checkpoint rows instead of dropping them', () => {
    expect(parseFileState({ kind: 'present', sha256: '', size: 1 })).toBeNull();
    expect(parseFileState({ kind: 'present', sha256: 'a', size: -1 })).toBeNull();
    expect(parseFileState({ kind: 'unknown' })).toBeNull();
    expect(() => parseRunFileChanges({ run_id: 'r', checkpoints: {} })).toThrow(/run_id or checkpoints/);
    expect(() =>
      parseRunFileChanges({ run_id: 'r', checkpoints: [{ ...wire.checkpoints[0], seq: 0 }] }),
    ).toThrow(/FileCheckpoint/);
    expect(() =>
      parseRunFileChanges({ run_id: 'r', checkpoints: [{ ...wire.checkpoints[0], after: { kind: 'present' } }] }),
    ).toThrow(/FileCheckpoint/);
  });

  it('loads the real route and surfaces a server refusal', async () => {
    const urls: string[] = [];
    const okFetch = async (url: string): Promise<Response> => {
      urls.push(url);
      return new Response(JSON.stringify(wire), { status: 200 });
    };
    const loaded = await loadRunFileChanges('run-1', { baseUrl: 'http://127.0.0.1:8080', fetchImpl: okFetch });
    expect(urls).toEqual(['http://127.0.0.1:8080/api/runs/run-1/checkpoints']);
    expect(loaded.changes).toHaveLength(2);

    const missingFetch = async (): Promise<Response> =>
      new Response(JSON.stringify({ code: 'E_RUN_NOT_FOUND', message: 'no run', how_to_fix: 'list runs', retryable: false }), {
        status: 404,
      });
    await expect(
      loadRunFileChanges('run-x', { fetchImpl: missingFetch }),
    ).rejects.toMatchObject({ refusal: { code: 'E_RUN_NOT_FOUND' } });

    await expect(loadRunFileChanges('  ')).rejects.toMatchObject({
      refusal: { code: 'E_RUN_NOT_FOUND' },
    });
  });
});

describe('panel rendering', () => {
  it('renders the editor read-only and names the missing write route', () => {
    const markup = render(createElement(EditorPanel, { api: null, path: null }));
    expect(markup).toContain('No file open');
    expect(markup).toContain('This editor is read-only.');
    expect(markup).toContain('POST /api/workspace/file');
    expect(markup).toContain('FILE_WRITE');
  });

  it('renders the diff panel idle and names both missing diff hops', () => {
    const markup = render(createElement(DiffReviewPanel, { runId: null }));
    expect(markup).toContain('Select a run to review its file changes.');
    expect(markup).toContain('Line diff and merge apply/reject are unavailable.');
    expect(markup).toContain('DIFF_PROPOSE');
    expect(markup).toContain('DIFF_APPLY');
    expect(markup).toMatch(/disabled=""[^>]*>Refresh run changes/);
  });

  it('renders journalled file changes without inventing diff text', () => {
    const changes: readonly RunFileChange[] = [
      {
        checkpointId: 'cp-1',
        runId: 'run-1',
        seq: 1,
        path: 'src/a.ts',
        before: { kind: 'present', sha256: 'a'.repeat(64), size: 10 },
        after: { kind: 'present', sha256: 'b'.repeat(64), size: 12 },
        createdAt: '2026-11-13T00:00:01Z',
        completedAt: '2026-11-13T00:00:02Z',
      },
      {
        checkpointId: 'cp-2',
        runId: 'run-1',
        seq: 2,
        path: 'src/new.ts',
        before: { kind: 'absent' },
        after: null,
        createdAt: '2026-11-13T00:00:03Z',
        completedAt: null,
      },
    ];
    const markup = render(createElement(FileChangeTable, { changes }));
    expect(markup).toContain('src/a.ts');
    expect(markup).toContain('absent');
    expect(markup).toContain('10 B');
    expect(markup).toContain('12 B');
    expect(markup).toContain('mutation not recorded as complete');
    expect(markup).toContain('2026-11-13T00:00:03Z');
  });

  it('renders the terminal as an honest unavailable surface with no dead input', () => {
    const markup = render(createElement(TerminalPanel, {}));
    expect(markup).toContain('Terminal unavailable');
    expect(markup).toContain('No PTY route');
    expect(markup).toContain('/api/terminal');
    expect(markup).toContain('SHELL_EXEC');
    expect(markup).not.toContain('<input');
    expect(markup).not.toContain('<textarea');
    expect(markup).not.toContain('<button');
  });

  it('disables every monitor command without a connected API and names the gaps', () => {
    const markup = render(
      createElement(MonitorControls, {
        api: null,
        runId: 'run-1',
        tasks: [{ taskId: 't1', role: 'planner', state: 'running' }],
      }),
    );
    expect(markup).toContain('Cancel run');
    expect(markup).toContain('Kill worker t1');
    expect(markup).toContain('No runtime API is connected');
    expect(markup).toContain('Instruct task — unavailable.');
    expect(markup).toContain('POST /api/runs/{run_id}/tasks/{task_id}/instruct');
    expect(markup).toContain('TASK_INSTRUCT');
    expect(markup).toContain('Retry task — unavailable.');
    expect(markup).toContain('POST /api/runs/{run_id}/tasks/{task_id}/retry');
    expect(markup).toContain('TASK_RETRY');
    expect(markup).toMatch(/aria-label="Cancel run"[^>]*disabled=""/);
    expect(markup).toMatch(/aria-label="Kill worker t1"[^>]*disabled=""/);
    // refresh + cancel + kill: every rendered button is disabled, none is dead.
    expect(markup.split('<button').length - 1).toBe(3);
    expect(markup.split('disabled=""').length - 1).toBe(3);
  });

  it('explains a terminal run instead of offering cancel', () => {
    const markup = render(
      createElement(MonitorControls, {
        api: { send: (() => Promise.resolve()) as never },
        runId: 'run-1',
        runStatus: 'completed',
      }),
    );
    expect(markup).toContain('is terminal (completed)');
    expect(markup).toMatch(/aria-label="Cancel run"[^>]*disabled=""/);
  });
});

describe('monitor state predicates', () => {
  it('treats only terminal states as terminal and only live tasks as killable', () => {
    for (const state of ['completed', 'failed', 'cancelled', 'interrupted', 'killed']) {
      expect(isTerminalRunStatus(state)).toBe(true);
      expect(isKillableTaskState(state)).toBe(false);
    }
    expect(isTerminalRunStatus('running')).toBe(false);
    expect(isTerminalRunStatus(null)).toBe(false);
    expect(isKillableTaskState('running')).toBe(true);
    expect(isKillableTaskState('awaiting_approval')).toBe(true);
    expect(isKillableTaskState(undefined)).toBe(false);
  });
});

describe('checkpoint restore hop', () => {
  it('posts the checkpoint restore route and parses the server receipt', async () => {
    const calls: Array<{ url: string; method: string; authorization: string | null }> = [];
    const fetchImpl = async (input: string, init?: RequestInit): Promise<Response> => {
      const headers = new Headers(init?.headers);
      calls.push({
        url: input,
        method: init?.method ?? 'GET',
        authorization: headers.get('authorization'),
      });
      return new Response(
        JSON.stringify({
          path: 'src/a.ts',
          undone_seqs: [2, 1],
          restored: { kind: 'absent' },
        }),
        { status: 200, headers: { 'content-type': 'application/json' } },
      );
    };
    const receipt = await restoreCheckpoint('run-1', 2, {
      fetchImpl,
      sessionToken: 'test-token',
    });
    expect(calls).toEqual([
      {
        url: '/api/runs/run-1/checkpoints/2/restore',
        method: 'POST',
        authorization: 'Bearer test-token',
      },
    ]);
    expect(receipt).toEqual({
      path: 'src/a.ts',
      undoneSeqs: [2, 1],
      restored: { kind: 'absent' },
    });
  });
});