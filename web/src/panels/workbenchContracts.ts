/**
 * Workbench contract layer: the typed, testable seam between the four
 * workbench panels and the backend that actually exists.
 *
 * Every entry in {@link WORKBENCH_HOPS} was read from the repository, not
 * assumed:
 *  - frozen WebSocket vocabulary: `backend/crates/hiverouter-contract/src/command.rs`
 *  - command dispatch: `backend/crates/hiverouter-core/src/ws.rs`
 *  - HTTP route table: `backend/crates/hiverouter-core/src/http.rs`
 *  - run file-change journal: `backend/crates/hiverouter-core/src/checkpoint.rs`
 *
 * A hop is either `wired` (a real route/command exists and is implemented here)
 * or `missing` (no route exists). A missing hop carries the exact absent route
 * or command so the UI can name it instead of rendering a dead control. No
 * helper in this module fabricates a success: WebSocket helpers resolve only on
 * a correlated server event, and the HTTP helper rejects on a non-2xx refusal.
 */
import type { UiApi } from '../api/client';
import { validateWorkspacePath, type FileContentPayload, type Refusal } from '../api/contract';
import { uuidv7 } from '../api/ids';
import { RefusalError, clientRefusal } from '../api/refusals';

/* ------------------------------------------------------------------ capability */

export type WorkbenchHopId =
  | 'editor.read'
  | 'editor.save'
  | 'diff.listRunChanges'
  | 'diff.content'
  | 'diff.apply'
  | 'terminal.open'
  | 'monitor.runDetail'
  | 'monitor.cancelRun'
  | 'monitor.killWorker'
  | 'monitor.killJob'
  | 'monitor.instructTask'
  | 'monitor.retryTask';

export interface WorkbenchHop {
  /** Stable id used by tests and by the panels' copy. */
  readonly id: WorkbenchHopId;
  /** Short human label. */
  readonly label: string;
  /** `wired`: implemented against a real route. `missing`: no route exists. */
  readonly status: 'wired' | 'missing';
  /** The exact command name or HTTP route this hop speaks. */
  readonly contract: string;
  /** Repository evidence (`path:line`) for the contract claim. */
  readonly evidence: string;
  /** Present only for `missing`: the exact absent route/command, named. */
  readonly missing?: string;
}

/**
 * The measured workbench capability map (2026-11-13).
 *
 * `missing` strings are the acceptance text: the panels render them verbatim so
 * an operator can read the precise gap rather than a generic "unavailable".
 */
export const WORKBENCH_HOPS = {
  'editor.read': {
    id: 'editor.read',
    label: 'Open workspace file',
    status: 'wired',
    contract: 'WS FILE_READ -> FILE_CONTENT',
    evidence: 'web/src/api/contract.ts:97-101,350-355; backend/crates/hiverouter-core/src/ws.rs:1087-1096',
  },
  'editor.save': {
    id: 'editor.save',
    label: 'Save workspace file',
    status: 'missing',
    contract: 'none',
    evidence:
      'backend/crates/hiverouter-contract/src/command.rs:56-82,506-523; backend/crates/hiverouter-core/src/http.rs:687-829',
    missing:
      'No workspace write route: POST /api/workspace/file is not registered in backend/crates/hiverouter-core/src/http.rs (route table 687-829), and the frozen command vocabulary (backend/crates/hiverouter-contract/src/command.rs:56-82) defines no FILE_WRITE command. The editor therefore opens files read-only.',
  },
  'diff.listRunChanges': {
    id: 'diff.listRunChanges',
    label: 'List run file changes',
    status: 'wired',
    contract: 'GET /api/runs/{run_id}/checkpoints',
    evidence:
      'backend/crates/hiverouter-core/src/http.rs:806-810,1375-1397; backend/crates/hiverouter-core/src/checkpoint.rs:192-214',
  },
  'diff.content': {
    id: 'diff.content',
    label: 'Line diff of a changed file',
    status: 'missing',
    contract: 'none',
    evidence:
      'backend/crates/hiverouter-contract/src/command.rs:78-82; backend/crates/hiverouter-core/src/http.rs:806-818',
    missing:
      'No checkpoint content route: GET /api/runs/{run_id}/checkpoints returns before/after SHA-256 digests and sizes only, and no route returns the journalled pre-image bytes. DIFF_PROPOSE is decision-gated and unimplemented (backend/crates/hiverouter-contract/src/command.rs:78-82), so line-level diff content is unavailable.',
  },
  'diff.apply': {
    id: 'diff.apply',
    label: 'Apply / reject a run file change',
    status: 'missing',
    contract: 'none',
    evidence:
      'backend/crates/hiverouter-contract/src/command.rs:78-82,141-170; backend/crates/hiverouter-core/src/http.rs:806-818',
    missing:
      'No apply/reject route: DIFF_APPLY and DIFF_REJECT are decision-gated and not frozen (backend/crates/hiverouter-contract/src/command.rs:78-82) and are never dispatched (backend/crates/hiverouter-core/src/ws.rs). The only file-restoring route is POST /api/runs/{run_id}/checkpoints/{seq}/restore (a rewind, not a merge decision).',
  },
  'terminal.open': {
    id: 'terminal.open',
    label: 'Interactive terminal / PTY',
    status: 'missing',
    contract: 'none',
    evidence:
      'backend/crates/hiverouter-core/src/http.rs:687-829; backend/crates/hiverouter-contract/src/command.rs:506-523',
    missing:
      'No PTY route: backend/crates/hiverouter-core/src/http.rs registers no /api/terminal, /api/pty or /ws/terminal endpoint, and the frozen command vocabulary has no SHELL_EXEC or TERMINAL_* command. The shell tool (backend/crates/hiverouter-tools/src/shell.rs) is an approval-gated agent tool with no UI channel.',
  },
  'monitor.runDetail': {
    id: 'monitor.runDetail',
    label: 'Run task detail',
    status: 'wired',
    contract: 'GET /api/runs/{run_id}/detail',
    evidence: 'backend/crates/hiverouter-core/src/http.rs:805,4355-4430',
  },
  'monitor.cancelRun': {
    id: 'monitor.cancelRun',
    label: 'Cancel run',
    status: 'wired',
    contract: 'WS CANCEL_RUN',
    evidence:
      'backend/crates/hiverouter-contract/src/command.rs:202-207; backend/crates/hiverouter-core/src/ws.rs:985-991',
  },
  'monitor.killWorker': {
    id: 'monitor.killWorker',
    label: 'Kill worker task',
    status: 'wired',
    contract: 'WS KILL_WORKER',
    evidence:
      'backend/crates/hiverouter-contract/src/command.rs:209-214; backend/crates/hiverouter-core/src/ws.rs:992-998',
  },
  'monitor.killJob': {
    id: 'monitor.killJob',
    label: 'Kill background job',
    status: 'wired',
    contract: 'POST /api/jobs/{job_id}/kill',
    evidence: 'backend/crates/hiverouter-core/src/http.rs:819-821,1594-1640',
  },
  'monitor.instructTask': {
    id: 'monitor.instructTask',
    label: 'Instruct a task',
    status: 'missing',
    contract: 'none',
    evidence:
      'backend/crates/hiverouter-core/src/http.rs:687-829; backend/crates/hiverouter-contract/src/command.rs:506-523',
    missing:
      'No task-instruct route: POST /api/runs/{run_id}/tasks/{task_id}/instruct is not registered in backend/crates/hiverouter-core/src/http.rs (route table 687-829), and no TASK_INSTRUCT command exists in the frozen vocabulary. A2A messaging is read-only to the UI (GET /api/a2a/messages, /api/a2a/pending).',
  },
  'monitor.retryTask': {
    id: 'monitor.retryTask',
    label: 'Retry a task',
    status: 'missing',
    contract: 'none',
    evidence:
      'backend/crates/hiverouter-core/src/http.rs:687-829; backend/crates/hiverouter-contract/src/command.rs:506-523',
    missing:
      'No task-retry route: POST /api/runs/{run_id}/tasks/{task_id}/retry is not registered in backend/crates/hiverouter-core/src/http.rs (route table 687-829), and no TASK_RETRY command exists in the frozen vocabulary.',
  },
} as const satisfies Readonly<Record<WorkbenchHopId, WorkbenchHop>>;

export function workbenchHop(id: WorkbenchHopId): WorkbenchHop {
  return WORKBENCH_HOPS[id];
}

export function isWorkbenchHopWired(id: WorkbenchHopId): boolean {
  return WORKBENCH_HOPS[id].status === 'wired';
}

/** The verbatim missing-route sentence, or `null` for a wired hop. */
export function missingHopDetail(id: WorkbenchHopId): string | null {
  const hop: WorkbenchHop = WORKBENCH_HOPS[id];
  return hop.status === 'missing' ? (hop.missing ?? `${hop.label}: no route`) : null;
}

/** What an ACK proves: the server accepted the command, not that it finished. */
export const ACK_MEANING =
  'The server acknowledged the command (ACK): the request was accepted, not completed. Completion arrives later as RUN_STATUS / NODE_STATE events.';

/* ------------------------------------------------------- run file-change journal */

/** Journalled state of one workspace path (`checkpoint.rs` `FileState`). */
export type FileState = { readonly kind: 'absent' } | {
  readonly kind: 'present';
  readonly sha256: string;
  readonly size: number;
};

/** One `FileCheckpoint` row from `GET /api/runs/{run_id}/checkpoints`. */
export interface RunFileChange {
  readonly checkpointId: string;
  readonly runId: string;
  readonly seq: number;
  readonly path: string;
  readonly before: FileState;
  readonly after: FileState | null;
  readonly createdAt: string;
  readonly completedAt: string | null;
}

export interface RunFileChanges {
  readonly runId: string;
  readonly changes: readonly RunFileChange[];
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function readNonEmptyString(source: Record<string, unknown>, key: string): string | null {
  const value = source[key];
  return typeof value === 'string' && value.length > 0 ? value : null;
}

/** Parse one `FileState`; returns `null` for any shape the server never sends. */
export function parseFileState(value: unknown): FileState | null {
  if (!isRecord(value)) {
    return null;
  }
  if (value.kind === 'absent') {
    return { kind: 'absent' };
  }
  if (value.kind === 'present') {
    const sha256 = readNonEmptyString(value, 'sha256');
    const size = value.size;
    if (!sha256 || typeof size !== 'number' || !Number.isInteger(size) || size < 0) {
      return null;
    }
    return { kind: 'present', sha256, size };
  }
  return null;
}

/**
 * Strict parser for the checkpoint-list response. A malformed row refuses the
 * whole response rather than silently dropping a changed file.
 */
export function parseRunFileChanges(body: unknown): RunFileChanges {
  if (!isRecord(body)) {
    throw new RefusalError(
      clientRefusal('E_SCHEMA_INVALID', { message: 'Checkpoint response was not an object.' }),
    );
  }
  const runId = readNonEmptyString(body, 'run_id');
  const rows = body.checkpoints;
  if (!runId || !Array.isArray(rows)) {
    throw new RefusalError(
      clientRefusal('E_SCHEMA_INVALID', {
        message: 'Checkpoint response was missing run_id or checkpoints[].',
      }),
    );
  }
  const changes: RunFileChange[] = [];
  for (const row of rows) {
    if (!isRecord(row)) {
      throw new RefusalError(
        clientRefusal('E_SCHEMA_INVALID', { message: 'Checkpoint entry was not an object.' }),
      );
    }
    const checkpointId = readNonEmptyString(row, 'checkpoint_id');
    const rowRunId = readNonEmptyString(row, 'run_id');
    const seq = row.seq;
    const path = readNonEmptyString(row, 'path');
    const createdAt = readNonEmptyString(row, 'created_at');
    const before = parseFileState(row.before);
    const afterRaw = row.after;
    const after = afterRaw === null || afterRaw === undefined ? null : parseFileState(afterRaw);
    if (
      !checkpointId ||
      !rowRunId ||
      typeof seq !== 'number' ||
      !Number.isInteger(seq) ||
      seq < 1 ||
      !path ||
      !createdAt ||
      before === null ||
      (afterRaw !== null && afterRaw !== undefined && after === null)
    ) {
      throw new RefusalError(
        clientRefusal('E_SCHEMA_INVALID', { message: 'Checkpoint entry did not match FileCheckpoint.' }),
      );
    }
    changes.push({
      checkpointId,
      runId: rowRunId,
      seq,
      path,
      before,
      after,
      createdAt,
      completedAt: typeof row.completed_at === 'string' ? row.completed_at : null,
    });
  }
  changes.sort((left, right) => left.seq - right.seq);
  return { runId, changes };
}

export type FetchLike = (input: string, init?: RequestInit) => Promise<Response>;

export interface LoadRunFileChangesOptions {
  /** Base URL of the Rust core; empty means same-origin. */
  readonly baseUrl?: string;
  readonly fetchImpl?: FetchLike;
  readonly signal?: AbortSignal;
}

function refusalFromBody(body: unknown): Refusal | null {
  if (!isRecord(body)) {
    return null;
  }
  const nested = isRecord(body.error) ? body.error : body;
  const code = readNonEmptyString(nested, 'code');
  if (!code) {
    return null;
  }
  return {
    code,
    message: typeof nested.message === 'string' ? nested.message : clientRefusal(code).message,
    how_to_fix:
      typeof nested.how_to_fix === 'string' ? nested.how_to_fix : clientRefusal(code).how_to_fix,
    retryable: typeof nested.retryable === 'boolean' ? nested.retryable : clientRefusal(code).retryable,
  };
}

/**
 * Real hop for the diff panel: `GET /api/runs/{run_id}/checkpoints`.
 * Rejects with the server's refusal on a non-2xx status; never returns an
 * empty list as a stand-in for a failed read.
 */
export async function loadRunFileChanges(
  runId: string,
  options: LoadRunFileChangesOptions = {},
): Promise<RunFileChanges> {
  if (runId.trim().length === 0) {
    throw new RefusalError(clientRefusal('E_RUN_NOT_FOUND'));
  }
  const baseUrl = (options.baseUrl ?? '').replace(/\/+$/, '');
  const fetchImpl = options.fetchImpl ?? ((input, init) => fetch(input, init));
  const url = `${baseUrl}/api/runs/${encodeURIComponent(runId)}/checkpoints`;
  let response: Response;
  try {
    response = await fetchImpl(url, {
      method: 'GET',
      headers: { accept: 'application/json' },
      ...(options.signal ? { signal: options.signal } : {}),
    });
  } catch {
    throw new RefusalError(
      clientRefusal('E_WS_DISCONNECTED', { message: 'GET /api/runs/{run_id}/checkpoints failed.' }),
    );
  }
  let body: unknown = null;
  try {
    body = await response.json();
  } catch {
    body = null;
  }
  if (!response.ok) {
    throw new RefusalError(
      refusalFromBody(body) ??
        clientRefusal('E_INTERNAL_ERROR', {
          message: `GET /api/runs/{run_id}/checkpoints returned ${response.status}.`,
        }),
    );
  }
  return parseRunFileChanges(body);
}

/** Receipt returned by `POST /api/runs/{run_id}/checkpoints/{seq}/restore`. */
export interface RestoredCheckpoint {
  readonly path: string;
  readonly undoneSeqs: readonly number[];
  readonly restored: FileState;
}

export interface RestoreCheckpointOptions extends LoadRunFileChangesOptions {
  /** Bearer token; when omitted the helper reads `/api/session` once. */
  readonly sessionToken?: string;
}

async function readSessionToken(
  baseUrl: string,
  fetchImpl: FetchLike,
  signal?: AbortSignal,
): Promise<string> {
  let launchToken: string | null = null;
  try {
    if (typeof window !== 'undefined') {
      launchToken = new URL(window.location.href).searchParams.get('launch');
    }
  } catch {
    launchToken = null;
  }
  const response = await fetchImpl(`${baseUrl}/api/session`, {
    method: 'GET',
    headers: {
      accept: 'application/json',
      ...(launchToken === null ? {} : { 'x-hivemind-launch': launchToken }),
    },
    ...(signal ? { signal } : {}),
  });
  const body = (await response.json().catch(() => null)) as unknown;
  if (!response.ok || !isRecord(body) || typeof body.token !== 'string' || body.token.length === 0) {
    throw new RefusalError(
      refusalFromBody(body) ??
        clientRefusal('E_TOKEN_INVALID', { message: 'GET /api/session did not return a token.' }),
    );
  }
  return body.token;
}

/**
 * Real mutation hop: restore one checkpoint. The server verifies the current
 * and before-image digests before writing; a refusal is surfaced as-is.
 */
export async function restoreCheckpoint(
  runId: string,
  seq: number,
  options: RestoreCheckpointOptions = {},
): Promise<RestoredCheckpoint> {
  if (runId.trim().length === 0) {
    throw new RefusalError(clientRefusal('E_RUN_NOT_FOUND'));
  }
  if (!Number.isInteger(seq) || seq < 1) {
    throw new RefusalError(clientRefusal('E_SCHEMA_INVALID', { message: 'seq must be >= 1' }));
  }
  const baseUrl = (options.baseUrl ?? '').replace(/\/+$/, '');
  const fetchImpl = options.fetchImpl ?? ((input, init) => fetch(input, init));
  const token =
    options.sessionToken ?? (await readSessionToken(baseUrl, fetchImpl, options.signal));
  let response: Response;
  try {
    response = await fetchImpl(
      `${baseUrl}/api/runs/${encodeURIComponent(runId)}/checkpoints/${seq}/restore`,
      {
        method: 'POST',
        headers: {
          accept: 'application/json',
          authorization: `Bearer ${token}`,
        },
        ...(options.signal ? { signal: options.signal } : {}),
      },
    );
  } catch {
    throw new RefusalError(
      clientRefusal('E_WS_DISCONNECTED', { message: 'POST checkpoint restore failed.' }),
    );
  }
  const body = (await response.json().catch(() => null)) as unknown;
  if (!response.ok) {
    throw new RefusalError(
      refusalFromBody(body) ??
        clientRefusal('E_INTERNAL_ERROR', {
          message: `POST checkpoint restore returned ${response.status}.`,
        }),
    );
  }
  if (!isRecord(body)) {
    throw new RefusalError(
      clientRefusal('E_SCHEMA_INVALID', { message: 'Restore response was not an object.' }),
    );
  }
  const path = readNonEmptyString(body, 'path');
  const restored = parseFileState(body.restored);
  const undone = Array.isArray(body.undone_seqs)
    ? body.undone_seqs.filter((value): value is number => Number.isInteger(value) && value > 0)
    : null;
  if (!path || restored === null || undone === null) {
    throw new RefusalError(
      clientRefusal('E_SCHEMA_INVALID', { message: 'Restore response did not match RestoredFile.' }),
    );
  }
  return { path, undoneSeqs: undone, restored };
}

/* -------------------------------------------------------------- file read hop */

export type WorkspaceFile = FileContentPayload;

export interface WorkspaceApi {
  send: UiApi['send'];
  subscribe: UiApi['subscribe'];
}

export interface ReadWorkspaceFileOptions {
  readonly correlationId?: string;
  readonly timeoutMs?: number;
  readonly newId?: () => string;
}

/**
 * Real hop for the editor: `FILE_READ` -> correlated `FILE_CONTENT`.
 *
 * Resolves only on a `FILE_CONTENT` event carrying the same correlation id;
 * a correlated `ERROR` or an unanswered request rejects. The workspace jail is
 * pre-checked client-side (`validateWorkspacePath`), and the server repeats it.
 */
export function readWorkspaceFile(
  api: WorkspaceApi,
  path: string,
  options: ReadWorkspaceFileOptions = {},
): Promise<WorkspaceFile> {
  const pathRefusal = validateWorkspacePath(path);
  if (pathRefusal) {
    return Promise.reject(new RefusalError(clientRefusal(pathRefusal)));
  }
  const correlationId = options.correlationId ?? (options.newId ?? uuidv7)();
  const timeoutMs = options.timeoutMs ?? 15_000;
  return new Promise<WorkspaceFile>((resolve, reject) => {
    let settled = false;
    let timer: ReturnType<typeof setTimeout> | null = null;
    let unsubscribe: (() => void) | null = null;
    const finish = (settle: () => void): void => {
      if (settled) {
        return;
      }
      settled = true;
      if (timer !== null) {
        clearTimeout(timer);
      }
      unsubscribe?.();
      settle();
    };
    unsubscribe = api.subscribe((event, envelope) => {
      if (envelope.correlation_id !== correlationId) {
        return;
      }
      if (event.name === 'ERROR') {
        finish(() => reject(new RefusalError(event.payload)));
        return;
      }
      if (event.name !== 'FILE_CONTENT') {
        return;
      }
      const payload = event.payload;
      if (payload.path !== path) {
        finish(() =>
          reject(
            new RefusalError(
              clientRefusal('E_SCHEMA_INVALID', {
                message: `FILE_CONTENT named ${payload.path} for a ${path} read.`,
              }),
            ),
          ),
        );
        return;
      }
      finish(() =>
        resolve({
          path: payload.path,
          sha256: payload.sha256,
          bytes: payload.bytes,
          text: payload.text,
        }),
      );
    });
    timer = setTimeout(() => {
      finish(() =>
        reject(
          new RefusalError(
            clientRefusal('E_WS_DISCONNECTED', {
              message: `No FILE_CONTENT for FILE_READ ${path} within ${timeoutMs} ms.`,
            }),
          ),
        ),
      );
    }, timeoutMs);
    api.send('FILE_READ', { path }, { correlationId }).catch((error: unknown) => {
      finish(() => reject(error instanceof Error ? error : new Error(String(error))));
    });
  });
}

/* ------------------------------------------------------------ monitoring hops */

export type MonitorApi = Pick<UiApi, 'send'>;

/** `CANCEL_RUN` — request cancellation of an active run. */
export async function cancelRun(api: MonitorApi, runId: string): Promise<void> {
  if (runId.trim().length === 0) {
    throw new RefusalError(clientRefusal('E_RUN_NOT_FOUND'));
  }
  await api.send('CANCEL_RUN', { run_id: runId });
}

/** `KILL_WORKER` — kill one running worker task. */
export async function killWorker(api: MonitorApi, taskId: string): Promise<void> {
  if (taskId.trim().length === 0) {
    throw new RefusalError(clientRefusal('E_TASK_NOT_FOUND'));
  }
  await api.send('KILL_WORKER', { task_id: taskId });
}

/** `POST /api/jobs/{job_id}/kill` — stop one background job. */
export async function killJob(
  jobId: string,
  options: LoadRunFileChangesOptions = {},
): Promise<void> {
  if (jobId.trim().length === 0) {
    throw new RefusalError(clientRefusal('E_JOB_NOT_FOUND'));
  }
  const baseUrl = (options.baseUrl ?? '').replace(/\/+$/, '');
  const fetchImpl = options.fetchImpl ?? ((input, init) => fetch(input, init));
  let response: Response;
  try {
    response = await fetchImpl(`${baseUrl}/api/jobs/${encodeURIComponent(jobId)}/kill`, {
      method: 'POST',
      headers: { accept: 'application/json' },
      ...(options.signal ? { signal: options.signal } : {}),
    });
  } catch {
    throw new RefusalError(
      clientRefusal('E_WS_DISCONNECTED', { message: 'POST /api/jobs/{job_id}/kill failed.' }),
    );
  }
  if (!response.ok) {
    let body: unknown = null;
    try {
      body = await response.json();
    } catch {
      body = null;
    }
    throw new RefusalError(
      refusalFromBody(body) ??
        clientRefusal('E_INTERNAL_ERROR', {
          message: `POST /api/jobs/{job_id}/kill returned ${response.status}.`,
        }),
    );
  }
}

/** Format a journalled file state for display without inventing content. */
export function formatFileState(state: FileState | null): string {
  if (state === null) {
    return 'not recorded';
  }
  if (state.kind === 'absent') {
    return 'absent';
  }
  return `${state.size} B · ${state.sha256.slice(0, 12)}…`;
}

/** A run is terminal once no further state transitions can occur. */
export const TERMINAL_RUN_STATES = [
  'completed',
  'failed',
  'cancelled',
  'interrupted',
  'killed',
] as const;

export function isTerminalRunStatus(status: string | null | undefined): boolean {
  return typeof status === 'string' && (TERMINAL_RUN_STATES as readonly string[]).includes(status);
}

/** Task states a worker kill can still affect. */
export function isKillableTaskState(state: string | null | undefined): boolean {
  if (typeof state !== 'string') {
    return false;
  }
  return !['completed', 'failed', 'killed', 'cancelled', 'interrupted'].includes(state);
}
