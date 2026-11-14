/**
 * Diff review panel — run file changes from the real checkpoint journal.
 *
 * Hops (measured, see `workbenchContracts.ts`):
 *  - list changed files: `GET /api/runs/{run_id}/checkpoints` (WIRED)
 *  - line-level content: no route returns journalled pre-image bytes (MISSING)
 *  - apply/reject: `DIFF_APPLY` / `DIFF_REJECT` are decision-gated and never
 *    dispatched (MISSING)
 *
 * The panel renders the digests the server actually reports and names the two
 * absent hops. It never renders an Accept/Reject control that could not reach a
 * route, and never synthesises diff text from digests.
 */
import { useCallback, useEffect, useState } from 'react';
import { RefusalError } from '../api/refusals';
import type { Refusal } from '../api/contract';
import {
  formatFileState,
  isTerminalRunStatus,
  loadRunFileChanges,
  missingHopDetail,
  restoreCheckpoint,
  type LoadRunFileChangesOptions,
  type RestoredCheckpoint,
  type RunFileChange,
  type RunFileChanges,
} from './workbenchContracts';

export interface DiffReviewPanelProps {
  readonly runId: string | null;
  readonly runStatus?: string | null;
  /** Injection seam for tests and for callers that already hold the snapshot. */
  readonly loadChanges?: (runId: string) => Promise<RunFileChanges>;
  readonly loadOptions?: LoadRunFileChangesOptions;
  /** Injection seam for tests; defaults to the real restore route. */
  readonly restore?: (runId: string, seq: number) => Promise<RestoredCheckpoint>;
  readonly className?: string;
}

type PanelStatus = 'idle' | 'loading' | 'ready' | 'error';

const CONTENT_MISSING = missingHopDetail('diff.content') ?? 'Diff content is unavailable.';
const APPLY_MISSING = missingHopDetail('diff.apply') ?? 'Applying a change is unavailable.';

export function DiffReviewPanel({
  runId,
  runStatus,
  loadChanges,
  loadOptions,
  restore,
  className,
}: DiffReviewPanelProps) {
  const [status, setStatus] = useState<PanelStatus>('idle');
  const [data, setData] = useState<RunFileChanges | null>(null);
  const [error, setError] = useState<Refusal | null>(null);
  const [reloadKey, setReloadKey] = useState(0);
  const [restoreBusySeq, setRestoreBusySeq] = useState<number | null>(null);
  const [restored, setRestored] = useState<RestoredCheckpoint | null>(null);
  const [restoreError, setRestoreError] = useState<Refusal | null>(null);

  const doRestore = useCallback(
    async (change: RunFileChange) => {
      if (runId === null || restoreBusySeq !== null) return;
      setRestoreBusySeq(change.seq);
      setRestoreError(null);
      setRestored(null);
      try {
        const receipt = restore
          ? await restore(runId, change.seq)
          : await restoreCheckpoint(runId, change.seq, loadOptions);
        setRestored(receipt);
        setReloadKey((key) => key + 1);
      } catch (failure: unknown) {
        setRestoreError(
          failure instanceof RefusalError
            ? failure.refusal
            : {
                code: 'E_INTERNAL_ERROR',
                message: 'Restoring the checkpoint failed.',
                how_to_fix: 'Retry; the server refusal carries the cause.',
                retryable: true,
              },
        );
      } finally {
        setRestoreBusySeq(null);
      }
    },
    [runId, restore, loadOptions, restoreBusySeq],
  );

  const load = useCallback(
    (targetRunId: string) =>
      loadChanges ? loadChanges(targetRunId) : loadRunFileChanges(targetRunId, loadOptions),
    [loadChanges, loadOptions],
  );

  useEffect(() => {
    if (runId === null) {
      setStatus('idle');
      setData(null);
      setError(null);
      return;
    }
    let cancelled = false;
    setStatus('loading');
    setError(null);
    load(runId)
      .then((changes) => {
        if (cancelled) {
          return;
        }
        setData(changes);
        setStatus('ready');
      })
      .catch((failure: unknown) => {
        if (cancelled) {
          return;
        }
        setData(null);
        setStatus('error');
        setError(
          failure instanceof RefusalError
            ? failure.refusal
            : {
                code: 'E_INTERNAL_ERROR',
                message: 'Reading the run checkpoints failed.',
                how_to_fix: 'Retry; the server refusal carries the cause.',
                retryable: true,
              },
        );
      });
    return () => {
      cancelled = true;
    };
  }, [runId, load, reloadKey]);

  const changes = data?.changes ?? [];
  const terminal = isTerminalRunStatus(runStatus);

  return (
    <section aria-label="Run file changes" className={className}>
      <header className="flex flex-wrap items-center justify-between gap-2">
        <h3 className="text-sm font-semibold">Run file changes</h3>
        <button
          type="button"
          className="rounded-[var(--radius-sm)] border border-hairline px-2 py-1 text-xs"
          disabled={runId === null || status === 'loading'}
          onClick={() => setReloadKey((key) => key + 1)}
        >
          Refresh run changes
        </button>
      </header>

      <div
        role="note"
        className="mt-2 rounded-[var(--radius-sm)] border border-hairline px-3 py-2 text-xs"
      >
        <p className="font-medium">Line diff and merge apply/reject are unavailable.</p>
        <p className="mt-1 opacity-80">{CONTENT_MISSING}</p>
        <p className="mt-1 opacity-80">{APPLY_MISSING}</p>
        <p className="mt-1 opacity-80">
          Full-file checkpoint restore is available below: the server verifies the current
          file and the journalled before-image before writing, and refuses drift.
        </p>
        {terminal ? (
          <p className="mt-1 opacity-80">
            This run is terminal ({runStatus}); its journal is read-only history.
          </p>
        ) : null}
      </div>

      {status === 'idle' ? <p className="mt-3 text-sm">Select a run to review its file changes.</p> : null}

      {status === 'loading' ? (
        <p className="mt-3 text-sm" role="status">
          Loading run file changes…
        </p>
      ) : null}

      {status === 'error' && error !== null ? (
        <div role="alert" className="mt-3 text-sm">
          <p className="font-medium">E_CHECKPOINTS_UNAVAILABLE · {error.code}</p>
          <p className="opacity-80">{error.message}</p>
          <p className="opacity-80">{error.how_to_fix}</p>
          <button
            type="button"
            className="mt-2 rounded-[var(--radius-sm)] border border-hairline px-2 py-1 text-xs"
            onClick={() => setReloadKey((key) => key + 1)}
          >
            Retry loading run changes
          </button>
        </div>
      ) : null}

      {status === 'ready' ? (
        changes.length === 0 ? (
          <p className="mt-3 text-sm">
            No file changes were journalled for this run; the checkpoint journal is empty.
          </p>
        ) : (
          <FileChangeTable
            changes={changes}
            canRestore={!terminal}
            onRestore={(change) => {
              void doRestore(change);
            }}
            busySeq={restoreBusySeq}
          />
        )
      ) : null}

      {restored !== null ? (
        <p role="status" className="mt-3 text-xs">
          Restored {restored.path} to {formatFileState(restored.restored)}; undone sequence(s){' '}
          {restored.undoneSeqs.join(', ')}.
        </p>
      ) : null}
      {restoreError !== null ? (
        <div role="alert" className="mt-3 text-xs">
          <p className="font-medium">Restore refused · {restoreError.code}</p>
          <p className="opacity-80">{restoreError.message}</p>
          <p className="opacity-80">{restoreError.how_to_fix}</p>
        </div>
      ) : null}
    </section>
  );
}

/** Pure view of the journalled changes; exported so it can be rendered alone. */
export function FileChangeTable({
  changes,
  canRestore = false,
  onRestore,
  busySeq = null,
}: {
  readonly changes: readonly RunFileChange[];
  readonly canRestore?: boolean;
  readonly onRestore?: (change: RunFileChange) => void;
  readonly busySeq?: number | null;
}) {
  return (
    <table className="mt-3 w-full text-left text-xs">
      <caption className="sr-only">File changes journalled for this run</caption>
      <thead>
        <tr>
          <th scope="col">Path</th>
          <th scope="col">Before</th>
          <th scope="col">After</th>
          <th scope="col">Seq</th>
          <th scope="col">Journalled at</th>
          {canRestore ? <th scope="col">Action</th> : null}
        </tr>
      </thead>
      <tbody>
        {changes.map((change) => (
          <FileChangeRow
            key={`${change.runId}-${change.seq}`}
            change={change}
            canRestore={canRestore}
            onRestore={onRestore}
            busy={busySeq === change.seq}
            anyBusy={busySeq !== null}
          />
        ))}
      </tbody>
    </table>
  );
}

function FileChangeRow({
  change,
  canRestore,
  onRestore,
  busy,
  anyBusy,
}: {
  readonly change: RunFileChange;
  readonly canRestore: boolean;
  readonly onRestore?: (change: RunFileChange) => void;
  readonly busy: boolean;
  readonly anyBusy: boolean;
}) {
  const [confirm, setConfirm] = useState(false);
  const actionable = canRestore && change.after !== null && onRestore !== undefined;
  return (
    <tr className="border-t border-hairline">
      <td className="py-1 font-mono">{change.path}</td>
      <td className="py-1">{formatFileState(change.before)}</td>
      <td className="py-1">
        {formatFileState(change.after)}
        {change.after === null ? ' (mutation not recorded as complete)' : ''}
      </td>
      <td className="py-1">{change.seq}</td>
      <td className="py-1">{change.createdAt}</td>
      {canRestore ? (
        <td className="py-1">
          {actionable ? (
            <button
              type="button"
              disabled={anyBusy}
              className="rounded-[var(--radius-sm)] border border-hairline px-2 py-1 disabled:opacity-50"
              onClick={() => {
                if (!confirm) {
                  setConfirm(true);
                  return;
                }
                setConfirm(false);
                onRestore(change);
              }}
            >
              {busy ? 'Restoring…' : confirm ? 'Confirm restore' : 'Restore'}
            </button>
          ) : (
            <span className="opacity-60">not recorded</span>
          )}
        </td>
      ) : null}
    </tr>
  );
}
