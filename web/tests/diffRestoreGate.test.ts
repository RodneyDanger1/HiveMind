import { createElement } from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { describe, expect, it } from 'vitest';

import { DiffReviewPanel, FileChangeTable, restorePolicy } from '../src/panels/DiffReviewPanel';
import type { RunFileChange } from '../src/panels/workbenchContracts';

/**
 * Regression pin for the live P2 defect: the Data panel listed
 * `live-content-writer-proof.md` (seq 1) for a completed run but rendered no
 * Restore control, because the panel gated restore on the run being
 * non-terminal. The HTTP restore route works after completion, so the run
 * lifecycle must never decide whether its journal may be rewound.
 */

const RUN_ID = 'run-01a1eb7a-completed';
const TERMINAL_STATUSES = ['completed', 'failed', 'cancelled', 'interrupted', 'killed'] as const;

const CHANGES: readonly RunFileChange[] = [
  {
    checkpointId: 'cp-1',
    runId: RUN_ID,
    seq: 1,
    path: 'live-content-writer-proof.md',
    before: { kind: 'absent' },
    after: { kind: 'present', sha256: 'c'.repeat(64), size: 22 },
    createdAt: '2026-11-17T15:20:00Z',
    completedAt: '2026-11-17T15:20:01Z',
  },
];

const countOf = (source: string, needle: string): number => source.split(needle).length - 1;

describe('checkpoint restore policy', () => {
  it('keeps a completed run restorable (the old terminal gate is gone)', () => {
    const policy = restorePolicy(RUN_ID, 'completed');
    expect(policy.canRestore).toBe(true);
    expect(policy.notice).toContain('Run status: completed');
    expect(policy.notice).toContain('remains restorable');
    expect(policy.notice).not.toContain('read-only history');
  });

  it('never gates restore on any terminal run status', () => {
    for (const status of TERMINAL_STATUSES) {
      expect(restorePolicy(RUN_ID, status).canRestore, `${status} must stay restorable`).toBe(true);
    }
  });

  it('still offers restore for active and unknown runs', () => {
    expect(restorePolicy(RUN_ID, 'running').canRestore).toBe(true);
    expect(restorePolicy(RUN_ID, null).canRestore).toBe(true);
    expect(restorePolicy(RUN_ID, undefined).canRestore).toBe(true);
    expect(restorePolicy(RUN_ID, null).notice).toContain('Run status: unknown');
  });

  it('only disables restore when no run is selected', () => {
    expect(restorePolicy(null, 'completed').canRestore).toBe(false);
    expect(restorePolicy(null, null).canRestore).toBe(false);
  });
});

describe('completed-run checkpoint table', () => {
  const noopRestore = (): void => undefined;

  it('renders the Restore control for a completed run', () => {
    const policy = restorePolicy(RUN_ID, 'completed');
    const markup = renderToStaticMarkup(
      createElement(FileChangeTable, {
        changes: CHANGES,
        canRestore: policy.canRestore,
        onRestore: noopRestore,
      }),
    );
    expect(markup).toContain('live-content-writer-proof.md');
    expect(markup).toContain('Restore');
    expect(markup).not.toContain('not recorded');
  });

  it('renders no action column when no run is selected', () => {
    const markup = renderToStaticMarkup(
      createElement(FileChangeTable, {
        changes: CHANGES,
        canRestore: restorePolicy(null, 'completed').canRestore,
        onRestore: noopRestore,
      }),
    );
    expect(markup).not.toContain('Restore');
    expect(markup).not.toContain('<th scope="col">Action</th>');
  });

  it('names an incomplete checkpoint instead of offering an unrestorable button', () => {
    const incomplete: readonly RunFileChange[] = [
      { ...CHANGES[0]!, checkpointId: 'cp-2', seq: 2, path: 'src/half-written.ts', after: null },
    ];
    const markup = renderToStaticMarkup(
      createElement(FileChangeTable, {
        changes: incomplete,
        canRestore: restorePolicy(RUN_ID, 'completed').canRestore,
        onRestore: noopRestore,
      }),
    );
    expect(markup).toContain('mutation not recorded as complete');
    expect(markup).toContain('not recorded');
    expect(markup).not.toContain('>Restore<');
  });

  it('never claims a recorded after-state is NOT recorded', () => {
    const markup = renderToStaticMarkup(
      createElement(FileChangeTable, {
        changes: CHANGES,
        canRestore: restorePolicy(RUN_ID, 'completed').canRestore,
        // No handler: the table must not invent a button, and must not lie
        // about the recorded digest either.
      }),
    );
    expect(markup).toContain('restore unavailable');
    expect(markup).not.toContain('not recorded');
  });
});

describe('diff panel copy for a completed run', () => {
  it('does not call a completed run read-only history', () => {
    const markup = renderToStaticMarkup(
      createElement(DiffReviewPanel, { runId: RUN_ID, runStatus: 'completed' }),
    );
    expect(markup).toContain('Run status: completed');
    expect(markup).toContain('remains restorable');
    expect(markup).not.toContain('read-only history');
    // Exactly one action column is impossible to prove while the panel is idle;
    // the table pin above proves the gate renders it once loaded.
    expect(countOf(markup, 'role="note"')).toBe(1);
  });
});
