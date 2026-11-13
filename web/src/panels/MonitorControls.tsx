/**
 * Monitor controls — cancel a run, kill a worker task, and name the task
 * controls that have no backend.
 *
 * Hops (measured, see `workbenchContracts.ts`):
 *  - cancel run: `CANCEL_RUN` (WIRED, WS)
 *  - kill worker: `KILL_WORKER` (WIRED, WS)
 *  - instruct task: no `TASK_INSTRUCT` command and no
 *    `POST /api/runs/{run_id}/tasks/{task_id}/instruct` route (MISSING)
 *  - retry task: no `TASK_RETRY` command and no
 *    `POST /api/runs/{run_id}/tasks/{task_id}/retry` route (MISSING)
 *
 * An ACK proves acceptance, not completion; the panel says so and waits for
 * `RUN_STATUS` / `NODE_STATE` events instead of claiming success.
 */
import { useEffect, useRef, useState } from 'react';
import { RefusalError } from '../api/refusals';
import type { Refusal } from '../api/contract';
import {
  ACK_MEANING,
  cancelRun,
  isKillableTaskState,
  isTerminalRunStatus,
  killWorker,
  missingHopDetail,
  type MonitorApi,
} from './workbenchContracts';

export interface MonitorTask {
  readonly taskId: string;
  readonly role: string;
  readonly state: string;
  readonly depth?: number | null;
}

export interface MonitorControlsProps {
  /** Connected runtime API; `null` disables every command and says why. */
  readonly api: MonitorApi | null;
  readonly runId: string | null;
  readonly runStatus?: string | null;
  readonly tasks?: readonly MonitorTask[];
  readonly onRefresh?: () => void;
  readonly className?: string;
}

type PendingAction =
  | { readonly kind: 'cancel' }
  | { readonly kind: 'kill'; readonly taskId: string }
  | null;

const INSTRUCT_MISSING = missingHopDetail('monitor.instructTask') ?? 'Instructing a task is unavailable.';
const RETRY_MISSING = missingHopDetail('monitor.retryTask') ?? 'Retrying a task is unavailable.';

export function MonitorControls({
  api,
  runId,
  runStatus,
  tasks = [],
  onRefresh,
  className,
}: MonitorControlsProps) {
  const [pending, setPending] = useState<PendingAction>(null);
  const [ack, setAck] = useState<string | null>(null);
  const [error, setError] = useState<Refusal | null>(null);
  const mounted = useRef(true);

  useEffect(() => {
    mounted.current = true;
    return () => {
      mounted.current = false;
    };
  }, []);

  const terminal = isTerminalRunStatus(runStatus);
  const busy = pending !== null;
  const canCancel = api !== null && runId !== null && !terminal && !busy;

  async function act(action: Exclude<PendingAction, null>, run: () => Promise<void>): Promise<void> {
    setPending(action);
    setError(null);
    setAck(null);
    try {
      await run();
      if (!mounted.current) {
        return;
      }
      setAck(action.kind === 'cancel' ? 'Cancel run' : `Kill worker ${action.taskId}`);
    } catch (failure: unknown) {
      if (!mounted.current) {
        return;
      }
      setError(
        failure instanceof RefusalError
          ? failure.refusal
          : {
              code: 'E_INTERNAL_ERROR',
              message: 'The command failed before the server answered.',
              how_to_fix: 'Reconnect the session and retry; no acknowledgement was received.',
              retryable: true,
            },
      );
    } finally {
      if (mounted.current) {
        setPending(null);
      }
    }
  }

  return (
    <section aria-label="Run monitor controls" className={className}>
      <header className="flex flex-wrap items-center justify-between gap-2">
        <h3 className="text-sm font-semibold">Monitor controls</h3>
        <button
          type="button"
          className="rounded-[var(--radius-sm)] border border-hairline px-2 py-1 text-xs"
          disabled={onRefresh === undefined}
          onClick={onRefresh}
        >
          Refresh run state
        </button>
      </header>

      <div className="mt-2 flex flex-wrap items-center gap-2">
        <button
          type="button"
          className="rounded-[var(--radius-sm)] border border-hairline px-2 py-1 text-xs"
          aria-label="Cancel run"
          aria-describedby="monitor-cancel-hint"
          disabled={!canCancel}
          onClick={() => {
            if (runId === null || api === null) {
              return;
            }
            void act({ kind: 'cancel' }, () => cancelRun(api, runId));
          }}
        >
          {pending?.kind === 'cancel' ? 'Cancelling…' : 'Cancel run'}
        </button>
        <span id="monitor-cancel-hint" className="text-xs opacity-70">
          {runtimeHint(api, runId, terminal, runStatus)}
        </span>
      </div>

      {ack !== null ? (
        <p className="mt-2 text-xs" role="status">
          {ack} accepted. {ACK_MEANING}
        </p>
      ) : null}

      {error !== null ? (
        <div role="alert" className="mt-2 text-xs">
          <p className="font-medium">{error.code}</p>
          <p className="opacity-80">{error.message}</p>
          <p className="opacity-80">{error.how_to_fix}</p>
        </div>
      ) : null}

      <div className="mt-3">
        <h4 className="text-xs font-medium">Tasks in this run</h4>
        {tasks.length === 0 ? (
          <p className="mt-1 text-xs opacity-70">
            No tasks reported for this run yet; worker kill stays disabled until a task appears.
          </p>
        ) : (
          <ul className="mt-1 space-y-1 text-xs">
            {tasks.map((task) => (
              <li key={task.taskId} className="flex items-center justify-between gap-2">
                <span className="font-mono">
                  {task.role} · {task.taskId} · {task.state}
                </span>
                <button
                  type="button"
                  className="rounded-[var(--radius-sm)] border border-hairline px-2 py-0.5"
                  aria-label={`Kill worker ${task.taskId}`}
                  disabled={api === null || busy || !isKillableTaskState(task.state)}
                  onClick={() => {
                    if (api === null) {
                      return;
                    }
                    const taskId = task.taskId;
                    void act({ kind: 'kill', taskId }, () => killWorker(api, taskId));
                  }}
                >
                  {pending?.kind === 'kill' && pending.taskId === task.taskId ? 'Killing…' : 'Kill'}
                </button>
              </li>
            ))}
          </ul>
        )}
      </div>

      <div role="group" aria-label="Unavailable task controls" className="mt-3 text-xs">
        <p className="font-medium">Instruct task — unavailable.</p>
        <p className="opacity-80" style={{ whiteSpace: 'pre-wrap' }}>
          {INSTRUCT_MISSING}
        </p>
        <p className="mt-2 font-medium">Retry task — unavailable.</p>
        <p className="opacity-80" style={{ whiteSpace: 'pre-wrap' }}>
          {RETRY_MISSING}
        </p>
      </div>
    </section>
  );
}

function runtimeHint(
  api: MonitorApi | null,
  runId: string | null,
  terminal: boolean,
  runStatus: string | null | undefined,
): string {
  if (api === null) {
    return 'No runtime API is connected, so no command can be sent.';
  }
  if (runId === null) {
    return 'Select a run before cancelling.';
  }
  if (terminal) {
    return `Run ${runId} is terminal (${runStatus ?? 'unknown'}), so cancel is not offered.`;
  }
  return `CANCEL_RUN is sent over the open WebSocket for run ${runId}.`;
}
