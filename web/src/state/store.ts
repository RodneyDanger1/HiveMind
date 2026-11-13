/**
 * The one UI store: run/task graph, chat stream, approvals/history/settings/
 * file state, connection surface, refusals and pending commands. Mutating UI
 * state is applied only from server events (ACK-before-mutation); every
 * failure is a named refusal toast, never a silent drop or a fake success.
 */
import { createStore, type StoreApi } from 'zustand/vanilla';
import type {
  CommandPayloads,
  Envelope,
  FileContentPayload,
  FileTreeEntry,
  NodeEventPayload,
  Refusal,
  RunMetricsPayload,
  ServerCommandName,
  ToolOutcome,
} from '../api/contract';
import {
  NODE_KIND_LABELS,
  isAgentState,
  isRunActive,
  isTerminal,
  type AgentState,
  type NodeKindLabel,
} from './agentState';
import {
  isNodeKind,
  isToolOutcome,
  validatePromptText,
  validateWorkspacePath,
} from '../api/contract';
import { shortId, uuidv7 } from '../api/ids';
import { RefusalError, clientRefusal } from '../api/refusals';
import { apiBaseUrl, type ConnectionStatus, type SessionInfo, type UiApi } from '../api/client';
import { fetchHealth } from '../api/health';
import { DEFAULT_HTTP_TIMEOUT_MS, fetchWithBoundTimeout } from '../api/http';
import type { DecodedEvent } from '../api/contract';
import { deriveHud } from './metrics';
import { spawnGate } from './spawnMatrix';
import { formatStateOf, STRINGS } from './strings';
import { readReducedMotionOverride, writeReducedMotionOverride } from './preferences';
import {
  APPROVAL_ACK_SETTLE_CODE,
  APPROVAL_ACK_SETTLE_MS,
  APPROVAL_REPLAY_SETTLE_MS,
  approvalDisplayStatus,
  approvalStatusAfterRequest,
  beginReplayWindow,
  canRetryApproval,
  classifyApprovalRefusal,
  initialApprovalReplayState,
  isApprovalExpired,
  initialApprovalResolveState,
  isReplayWindowOpen,
  markApprovalsStale,
  settleReplay,
  settleReplayStatus,
  stampReplayEpoch,
  type ApprovalReplayState,
  type ApprovalResolveOutcome,
  type ApprovalResolveState,
} from './approval';

export type NavSection = 'workspace' | 'agents' | 'data' | 'models' | 'routing' | 'remote' | 'settings' | 'help';
export type TabId = 'workspace' | 'swarm' | 'routing' | 'history';
export type ToastRefusal = { readonly id: string; readonly refusal: Refusal; readonly at: number };
export type PendingCommand = { readonly name: ServerCommandName; readonly startedAt: number };

export interface TaskRecord {
  readonly id: string;
  readonly parentId: string | null;
  readonly kind: NodeKindLabel;
  readonly label: string;
  /** Server task role when the snapshot carries it; absent for legacy nodes. */
  readonly role?: string | null;
  readonly state: AgentState;
  readonly progress: number | null;
  readonly errorCode: string | null;
  /**
   * The node's own server CAS revision (`graph.rs TaskNode.rev`).
   *
   * `null` means UNKNOWN/unconfirmed: the node has never been seen in a
   * committed history snapshot, or a live NODE_* frame changed it after the
   * last confirmation. `REWIRE_TASK.expected_rev` is sent from this value —
   * never from the client-local `graphRev` counter, which is not a server CAS
   * revision and can never win `GraphAuthority::rewire` (`graph.rs:152`).
   */
  readonly rev: number | null;
}

export interface ChatMessage {
  readonly id: string;
  readonly role: 'user' | 'assistant';
  readonly text: string;
  readonly runId: string | null;
  readonly streaming: boolean;
}

export interface ApprovalRecord {
  readonly approvalId: string;
  readonly tool: string;
  readonly nonce: string | null;
  readonly decision: string | null;
  readonly requestedAt: number;
  /* 1.2 additive fields: present as null when the server omits them. */
  readonly runId: string | null;
  readonly taskId: string | null;
  readonly toolCallId: string | null;
  readonly argsSha256: string | null;
  readonly argsPreview?: string | null;
  readonly expiresAt: string | null;
}

/** One model-issued tool call: identity + args hash + exactly one terminal outcome. */
export interface ToolActivityRecord {
  readonly toolCallId: string;
  readonly tool: string;
  readonly argsSha256: string;
  readonly turn: number;
  readonly outcome: 'pending' | ToolOutcome;
  /** Wire event timestamp; absent on legacy frames and never guessed. */
  readonly at?: string;
  readonly code?: string;
  readonly text?: string;
  /**
   * Owning task from the wire `task_id` (TOOL_CALL/TOOL_RESULT). Optional so a
   * legacy/partial frame renders an honest "task unknown" instead of a guess.
   */
  readonly taskId?: string | null;
  /** Result byte size when the TOOL_RESULT frame reports it (never estimated). */
  readonly bytes?: number | null;
  /** Result digest when the TOOL_RESULT frame reports it (never invented). */
  readonly sha256?: string | null;
}

/**
 * Deterministic policy constant for out-of-order TOOL_RESULT frames: at most this many
 * results can wait for their TOOL_CALL (FIFO overflow drops the oldest, never rendered).
 */
export const MAX_BUFFERED_TOOL_RESULTS = 16;

/** Buffered terminal outcome waiting for its TOOL_CALL (never part of the rendered rows). */
interface BufferedToolResult {
  readonly toolCallId: string;
  readonly runId: string;
  readonly outcome: ToolOutcome;
  readonly code?: string;
  readonly bytes?: number | null;
  readonly sha256?: string | null;
}

export interface RunRecord {
  readonly id: string;
  readonly status: AgentState;
  readonly errorCode: string | null;
}

export interface ReplayGraph {
  readonly revision: number;
  readonly graph: unknown;
}

/** Committed-revision cursor for time travel (server history is the authority). */
export interface HistoryState {
  /** Known committed runs, newest first (server order). */
  readonly runs: readonly HistoryRunSummary[];
  /** Latest committed revision number; `-1` when the run has none. */
  readonly latest: number;
  /** Number of committed revisions (clamp target, not `latest + 1`). */
  readonly count: number;
  /** Exact committed revision numbers, ascending (may be non-contiguous). */
  readonly revisions: readonly number[];
  /** Revision currently shown; the latest revision when the live graph is shown. */
  readonly selected: number;
  /** Non-null while replaying a committed revision; `null` renders the live graph. */
  readonly replayGraph: ReplayGraph | null;
  readonly refreshStatus: 'idle' | 'loading' | 'ok' | 'error';
  readonly refreshRefusal: Refusal | null;
}

/** Why the last run/tasks/history rehydration was attempted. */
export type SnapshotSource =
  | 'boot'
  | 'reconnect'
  | 'resync'
  | 'refusal'
  | 'rewire'
  | 'manual';

/** What an otherwise successful rehydrate could not restore from the server. */
export type SnapshotIncomplete =
  | 'no-run'
  | 'no-committed-revision'
  | 'messages-unavailable'
  | 'run-changed';

/**
 * Server-authoritative rehydration of run/tasks/history.
 *
 * The graph snapshot from `GET /api/history/runs/{run_id}/state` is the
 * authority; no event is synthesized and nothing is invented when a resource
 * is missing (that is reported in `incomplete` and `refusal` instead).
 */
export interface SnapshotState {
  readonly status: 'idle' | 'loading' | 'ready' | 'degraded' | 'failed';
  readonly source: SnapshotSource | null;
  readonly runId: string | null;
  /** Committed revision the tasks were projected from; `null` when none exists. */
  readonly revision: number | null;
  readonly revisionCount: number;
  readonly incomplete: readonly SnapshotIncomplete[];
  readonly refusal: Refusal | null;
  readonly attempts: number;
  readonly updatedAt: number | null;
}

/**
 * An unresolved event-range gap announced by `RESYNC_REQUIRED` (or a command
 * refusal with `E_RESYNC_REQUIRED`). It is cleared ONLY by a settled snapshot
 * rehydrate — a following `SERVER_HELLO` must not present the gap as Connected.
 */
export type ResyncGap =
  | { readonly cause: 'event'; readonly expectedSeq: number; readonly lastSeq: number }
  | { readonly cause: 'refusal'; readonly code: string };

/** Injectable seams for the store's bounded history reads (tests and hosts). */
export interface UiStoreOptions {
  readonly fetchImpl?: typeof fetch;
  readonly snapshotTimeoutMs?: number;
}

export interface SettingsState {
  readonly status: 'unknown' | 'pending' | 'ok' | 'error';
  readonly provider: string | null;
  readonly model: string | null;
  readonly keyPresent: boolean | null;
  readonly thinking: string | null;
  readonly reasoningEffort: string | null;
  readonly refusal: Refusal | null;
  readonly health: 'unknown' | 'ok' | 'degraded' | 'failing';
  readonly healthVersion: string | null;
  readonly checks: readonly string[];
  readonly lastProbe: number | null;
}

export interface FileTreeState {
  readonly status: 'idle' | 'loading' | 'ok' | 'error';
  readonly root: string | null;
  readonly entries: readonly FileTreeEntry[];
  readonly refusal: Refusal | null;
  readonly contentStatus: 'idle' | 'loading' | 'ok' | 'error';
  readonly content: FileContentPayload | null;
  readonly selectedPath: string | null;
}

export interface ViewportState {
  readonly x: number;
  readonly y: number;
  readonly zoom: number;
}

export interface UiState {
  readonly initialized: boolean;
  readonly apiKind: 'ws' | 'mock';
  readonly connection: ConnectionStatus;
  readonly connectionRefusal: Refusal | null;
  readonly session: SessionInfo | null;
  readonly lastSeq: number;
  readonly run: RunRecord | null;
  readonly tasks: Readonly<Record<string, TaskRecord>>;
  readonly graphRev: number;
  readonly selectedTaskId: string | null;
  readonly messages: readonly ChatMessage[];
  readonly streamingRunId: string | null;
  readonly agentMessageVersion: number;
  readonly metrics: RunMetricsPayload | null;
  readonly history: HistoryState;
  /** Last server-authoritative rehydration (boot/reload/reconnect/resync). */
  readonly snapshot: SnapshotState;
  /** Unresolved RESYNC_REQUIRED gap; non-null until a snapshot rehydrate settles. */
  readonly resyncGap: ResyncGap | null;
  readonly approvals: readonly ApprovalRecord[];
  /** Approval ids with an in-flight APPROVAL_RESOLVE command (double-submit guard). */
  readonly resolvingApprovals: readonly string[];
  /** Per-approval resolve/retry machine; absent until the first gesture. */
  readonly approvalResolve: Readonly<Record<string, ApprovalResolveState>>;
  /** Replay-window state for pending approvals after reconnect. */
  readonly approvalReplay: ApprovalReplayState;
  /** True when the current approval list has not been confirmed since reconnect. */
  readonly approvalsStale: boolean;
  /** Replay epoch in which each approval id was last confirmed by a replayed request. */
  readonly approvalSeenEpoch: Readonly<Record<string, number>>;
  /** Server-reported tool calls in arrival order; outcomes transition pending -> terminal once. */
  readonly toolActivity: readonly ToolActivityRecord[];
  readonly refusals: readonly ToastRefusal[];
  readonly pendingCommands: Readonly<Record<string, PendingCommand>>;
  readonly view: { readonly nav: NavSection; readonly tab: TabId };
  readonly viewport: ViewportState;
  readonly chat: { readonly pinned: boolean; readonly visible: boolean };
  readonly settings: SettingsState;
  readonly fileTree: FileTreeState;
  readonly project: { readonly id: string; readonly name: string; readonly owner: string };
  /** Named session selected in the composer; used by the jobs console. */
  readonly activeSessionId: string | null;
  readonly reducedMotionOverride: boolean;
  readonly diffOpen: boolean;

  init(): Promise<void>;
  dispose(): void;
  submitPrompt(
    text: string,
    options?: {
      readonly sessionId?: string;
      readonly policy?: 'classic' | 'plan' | 'standard' | 'trusted';
    },
  ): Promise<void>;
  /**
   * Send SPAWN_SUB_HIVE for the explicit parent when given, otherwise the
   * selected task, otherwise the run's HyperAgent root. The class matrix is
   * enforced here as well as on the server: an impossible parent raises
   * E_TOOL_NOT_ALLOWED and sends nothing.
   */
  spawnSubHive(parentTaskId?: string): Promise<void>;
  rewireSelected(newParentId: string): Promise<void>;
  cancelRun(): Promise<void>;
  killWorker(): Promise<void>;
  resolveApproval(
    approvalId: string,
    decision: 'approve' | 'deny',
    reason?: string,
  ): Promise<ApprovalResolveOutcome>;
  retryApproval(approvalId: string): Promise<ApprovalResolveOutcome>;
  dismissApproval(approvalId: string): void;
  pinChat(pinned: boolean): Promise<void>;
  insertTemplate(templateId: string): Promise<void>;
  setHistoryRevision(revision: number): Promise<void>;
  /** Switch the client's viewed run to one of the committed history runs. */
  selectHistoryRun(runId: string): Promise<void>;
  /** Leave replay and render the live graph again (explicit exit, not just a disabled control). */
  exitHistoryReplay(): void;
  /** Rehydrate run/tasks/history from the committed server snapshot. */
  refreshRunSnapshot(source?: SnapshotSource): Promise<void>;
  setNav(nav: NavSection): void;
  setTab(tab: TabId): void;
  /**
   * Select a Workspace tab AND move the visible nav section to Workspace.
   * The Library rail is visible on every nav section, so its tabs must not
   * change hidden state while the user stays on Settings/Agents/etc.
   */
  setWorkspaceTab(tab: TabId): void;
  setViewport(viewport: ViewportState): void;
  selectTask(taskId: string | null): void;
  setChatVisible(visible: boolean): void;
  setActiveSessionId(sessionId: string | null): void;
  openDiffModal(): void;
  closeDiffModal(): void;
  dismissRefusal(id: string): void;
  setReducedMotionOverride(value: boolean): void;
  probeSettings(): Promise<void>;
  probeHealth(): Promise<void>;
  requestFileTree(path?: string): Promise<void>;
  openFile(path: string): Promise<void>;
  formatSelectedRevision(): string;
}

const HEALTHY_GREEN: readonly AgentState[] = ['healthy', 'completed'];

/** The event producer does not send a display label. UUIDv7 prefixes are the
 * same timestamp bucket across a run, so use the random tail and do not assert a
 * kind when the frame omitted `kind`. */
function fallbackTaskLabel(kind: NodeKindLabel | null, id: string): string {
  const suffix = id.length > 8 ? id.slice(-8) : id;
  return kind === null ? `Agent ${suffix}` : `${NODE_KIND_LABELS[kind]} ${suffix}`;
}

/**
 * Clears the streaming flag for one run. Returns the SAME array when no row
 * changed so duplicate REPLY_DONE / terminal RUN_STATUS frames do not allocate
 * (or notify subscribers with) a fresh copy of the whole transcript.
 */
function finalizeStreamingMessages(
  messages: readonly ChatMessage[],
  runId: string,
): readonly ChatMessage[] {
  let changed = false;
  const next = messages.map((message) => {
    if (message.role === 'assistant' && message.runId === runId && message.streaming) {
      changed = true;
      return { ...message, streaming: false };
    }
    return message;
  });
  return changed ? next : messages;
}

/**
 * Reconcile live node records with a committed snapshot. The snapshot is
 * authoritative unless the live record carries a strictly newer node revision
 * (an event that landed after the snapshot). This prevents a resync from
 * downgrading a node's revision while still repairing missed state changes.
 */
export function mergeSnapshotTasks(
  snapshot: Readonly<Record<string, TaskRecord>>,
  live: Readonly<Record<string, TaskRecord>>,
): Record<string, TaskRecord> {
  const merged: Record<string, TaskRecord> = { ...snapshot };
  for (const [id, liveTask] of Object.entries(live)) {
    const snapshotTask = merged[id];
    if (snapshotTask === undefined) {
      merged[id] = liveTask;
      continue;
    }
    const snapshotRev = snapshotTask.rev ?? 0;
    const liveRev = liveTask.rev ?? 0;
    if (liveRev > snapshotRev) {
      merged[id] = liveTask;
    }
  }
  return merged;
}

function baseProject(): { id: string; name: string; owner: string } {
  const key = 'hivemind.project-id';
  let id: string | null = null;
  try {
    if (typeof window !== 'undefined') {
      const stored = window.localStorage.getItem(key);
      if (stored !== null && stored.trim().length > 0) {
        id = stored;
      }
    }
  } catch {
    // Storage can be unavailable in a locked-down embedding context; a fresh
    // id is still a real project key, never a fabricated display value.
  }
  if (id === null) {
    id = uuidv7();
    try {
      if (typeof window !== 'undefined') {
        window.localStorage.setItem(key, id);
      }
    } catch {
      // Same fallback: the id lives for this page only.
    }
  }
  return {
    id,
    name: STRINGS.library.projectFallback,
    owner: STRINGS.chat.userLabel,
  };
}

function initialSettings(): SettingsState {
  return {
    status: 'unknown',
    provider: null,
    model: null,
    keyPresent: null,
    thinking: null,
    reasoningEffort: null,
    refusal: null,
    health: 'unknown',
    healthVersion: null,
    checks: [],
    lastProbe: null,
  };
}

function initialFileTree(): FileTreeState {
  return {
    status: 'idle',
    root: null,
    entries: [],
    refusal: null,
    contentStatus: 'idle',
    content: null,
    selectedPath: null,
  };
}

function initialChatVisible(): boolean {
  try {
    const raw = window.localStorage.getItem('hivemind.chat.visible.v1');
    return raw === null ? true : raw !== 'false';
  } catch {
    return true;
  }
}

function initialHistory(): HistoryState {
  return {
    runs: [],
    latest: -1,
    count: 0,
    revisions: [],
    selected: 0,
    replayGraph: null,
    refreshStatus: 'idle',
    refreshRefusal: null,
  };
}

function initialSnapshot(): SnapshotState {
  return {
    status: 'idle',
    source: null,
    runId: null,
    revision: null,
    revisionCount: 0,
    incomplete: [],
    refusal: null,
    attempts: 0,
    updatedAt: null,
  };
}

/* --------------------------------------------------------- history rehydration */

/**
 * Existing localhost HTTP read surface (`backend/crates/hiverouter-core/src/http.rs:356-358`):
 *   GET /api/history/runs
 *   GET /api/history/runs/{run_id}/revisions
 *   GET /api/history/runs/{run_id}/state?revision=N
 *   GET /api/history/runs/{run_id}/messages
 *
 * The messages route rehydrates the persisted transcript; a failed read is
 * reported as `messages-unavailable`, never fabricated.
 */
const HISTORY_RUNS_PATH = '/api/history/runs';

function historyRevisionsPath(runId: string): string {
  return `/api/history/runs/${encodeURIComponent(runId)}/revisions`;
}

function historyStatePath(runId: string, revision: number): string {
  return `/api/history/runs/${encodeURIComponent(runId)}/state?revision=${revision}`;
}

function historyMessagesPath(runId: string): string {
  return `/api/history/runs/${encodeURIComponent(runId)}/messages`;
}

function asRecord(value: unknown): Record<string, unknown> | null {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    return null;
  }
  return value as Record<string, unknown>;
}

function schemaRefusal(message: string): RefusalError {
  return new RefusalError(clientRefusal('E_SCHEMA_INVALID', { message }));
}

function toRefusal(error: unknown): Refusal {
  return error instanceof RefusalError ? error.refusal : clientRefusal('E_WS_DISCONNECTED');
}

export interface HistoryRunSummary {
  readonly runId: string;
  readonly status: AgentState;
  readonly errorCode: string | null;
}

function decodeHistoryRuns(body: unknown): readonly HistoryRunSummary[] {
  const record = asRecord(body);
  const runs = record?.runs;
  if (!Array.isArray(runs)) {
    throw schemaRefusal('GET /api/history/runs did not return a runs array.');
  }
  const decoded: HistoryRunSummary[] = [];
  for (const entry of runs) {
    const item = asRecord(entry);
    const runId = item?.run_id;
    const status = item?.status;
    if (typeof runId !== 'string' || !isAgentState(status)) {
      throw schemaRefusal('GET /api/history/runs returned a run row without a run_id/status.');
    }
    decoded.push({
      runId,
      status,
      errorCode: typeof item?.error_code === 'string' ? item.error_code : null,
    });
  }
  return decoded;
}

interface HistoryRevisionList {
  readonly revisions: readonly number[];
  readonly latest: number | null;
}

function decodeHistoryMessages(body: unknown): readonly ChatMessage[] {
  const record = asRecord(body);
  const messages = record?.messages;
  if (!Array.isArray(messages)) {
    throw schemaRefusal('GET /api/history/runs/{run_id}/messages did not return a messages array.');
  }
  const decoded: ChatMessage[] = [];
  for (const entry of messages) {
    const item = asRecord(entry);
    const id = item?.id;
    const role = item?.role;
    const text = item?.text;
    if (typeof id !== 'string' || (role !== 'user' && role !== 'assistant') || typeof text !== 'string') {
      throw schemaRefusal('A history message row has no id/user-or-assistant role/text.');
    }
    decoded.push({
      id,
      role,
      text,
      runId: typeof item?.run_id === 'string' ? item.run_id : null,
      streaming: false,
    });
  }
  return decoded;
}

function decodeHistoryRevisions(body: unknown): HistoryRevisionList {
  const record = asRecord(body);
  const rawRevisions = record?.revisions;
  if (!Array.isArray(rawRevisions)) {
    throw schemaRefusal('GET /api/history/runs/{run_id}/revisions did not return a revisions array.');
  }
  const revisions: number[] = [];
  for (const entry of rawRevisions) {
    const revision = asRecord(entry)?.revision;
    if (typeof revision !== 'number' || !Number.isInteger(revision) || revision < 0) {
      throw schemaRefusal('A history revision row has no non-negative integer revision.');
    }
    if (!revisions.includes(revision)) {
      revisions.push(revision);
    }
  }
  revisions.sort((left, right) => left - right);
  const latest = revisions.length > 0 ? revisions[revisions.length - 1] : null;
  const declared = record?.latest_revision;
  if (declared !== null && declared !== undefined && declared !== latest) {
    throw schemaRefusal('The revisions payload latest_revision disagrees with its revision list.');
  }
  return { revisions, latest };
}

interface HistoryStateProjection {
  readonly revision: number;
  readonly runStatus: AgentState | null;
  readonly tasks: Readonly<Record<string, TaskRecord>>;
}

function decodeHistoryState(body: unknown): HistoryStateProjection {
  const record = asRecord(body);
  const revision = record?.revision;
  if (typeof revision !== 'number' || !Number.isInteger(revision) || revision < 0) {
    throw schemaRefusal('GET /api/history/runs/{run_id}/state returned no non-negative revision.');
  }
  const runStatusRaw = record?.run_status;
  return {
    revision,
    runStatus: isAgentState(runStatusRaw) ? runStatusRaw : null,
    tasks: recordsFromSnapshotGraph(record?.graph),
  };
}

/**
 * Projects a committed history graph snapshot (`GraphSnapshot` in
 * `graph.rs:31-40`) into task records. Every node must carry `task_id`,
 * `state` and the server per-node `rev`; a malformed snapshot fails closed
 * (named refusal) rather than rendering an invented revision.
 */
function recordsFromSnapshotGraph(graph: unknown): Readonly<Record<string, TaskRecord>> {
  const record = asRecord(graph);
  const rawNodes = record?.nodes;
  if (!Array.isArray(rawNodes)) {
    throw schemaRefusal('A history snapshot graph has no nodes array.');
  }
  const parsed: Array<{
    id: string;
    parentId: string | null;
    kind: NodeKindLabel;
    role: string | null;
    label: string | null;
    state: AgentState;
    rev: number;
  }> = [];
  const ids = new Set<string>();
  for (const entry of rawNodes) {
    const node = asRecord(entry);
    const id = node?.task_id;
    const state = node?.state;
    const rev = node?.rev;
    if (
      typeof id !== 'string' ||
      !isAgentState(state) ||
      typeof rev !== 'number' ||
      !Number.isInteger(rev) ||
      rev < 0
    ) {
      throw schemaRefusal('A history snapshot node is not a {task_id, state, rev} record.');
    }
    if (ids.has(id)) {
      throw schemaRefusal(`A history snapshot contains task ${id} twice.`);
    }
    ids.add(id);
    const rawKind = node?.kind;
    const role = typeof node?.role === 'string' && node.role.length > 0 ? node.role : null;
    parsed.push({
      id,
      parentId: typeof node?.parent_task_id === 'string' ? node.parent_task_id : null,
      kind: isNodeKind(rawKind) ? rawKind : 'worker',
      role,
      label: typeof node?.label === 'string' && node.label.trim().length > 0 ? node.label : null,
      state,
      rev,
    });
  }
  const tasks: Record<string, TaskRecord> = {};
  for (const node of parsed) {
    tasks[node.id] = {
      id: node.id,
      // The snapshot is the graph authority; a parent outside this snapshot
      // cannot be drawn as an edge, so it is re-rooted rather than dangling.
      parentId: node.parentId !== null && ids.has(node.parentId) ? node.parentId : null,
      kind: node.kind,
      label: node.label ?? fallbackTaskLabel(node.kind, node.id),
      role: node.role,
      state: node.state,
      progress: null,
      errorCode: null,
      rev: node.rev,
    };
  }
  return tasks;
}

/**
 * Reads the additive topology `rev` from a NODE_* payload.
 *
 * `rev` is the same per-node revision `GraphAuthority::rewire` compares against
 * (`graph.rs:152`) — never the client-local event counter. It is optional on the
 * wire (a pre-1.3 producer omits it), so an absent/non-integer value stays
 * `null` (unconfirmed) instead of being guessed.
 */
function readNodeRev(payload: NodeEventPayload): number | null {
  const candidate = payload.rev;
  return typeof candidate === 'number' && Number.isInteger(candidate) && candidate >= 0
    ? candidate
    : null;
}

function chooseHistoryRun(
  runs: readonly HistoryRunSummary[],
  current: RunRecord | null,
): HistoryRunSummary | null {
  if (current !== null) {
    const same = runs.find((candidate) => candidate.runId === current.id);
    if (same !== undefined) {
      return same;
    }
  }
  const active = runs.find((candidate) => isRunActive(candidate.status));
  return active ?? runs[0] ?? null;
}

function insertRevision(revisions: readonly number[], revision: number): readonly number[] {
  if (revisions.includes(revision)) {
    return revisions;
  }
  return [...revisions, revision].sort((left, right) => left - right);
}

/** Honest preconditions for the Re-route control (shared by store and panels). */
export type RewireBlocker =
  | 'no-run'
  | 'run-terminal'
  | 'no-selection'
  | 'target-not-in-graph'
  | 'unknown-server-rev';

export interface RewireBlockerArgs {
  readonly run: RunRecord | null;
  readonly tasks: Readonly<Record<string, TaskRecord>>;
  readonly selectedTaskId: string | null;
  readonly newParentId: string | null;
}

/**
 * `unknown-server-rev` is the disabled/unknown state M-005 asks for: without a
 * confirmed node revision the command cannot win `GraphAuthority::rewire`
 * (`graph.rs:152`), so the store does not send a guessed `expected_rev` and
 * does not enter a false E_RESYNC_REQUIRED loop.
 */
export function rewireBlocker(args: RewireBlockerArgs): RewireBlocker | null {
  if (args.run === null) {
    return 'no-run';
  }
  if (!isRunActive(args.run.status)) {
    return 'run-terminal';
  }
  if (args.selectedTaskId === null || args.tasks[args.selectedTaskId] === undefined) {
    return 'no-selection';
  }
  if (args.newParentId === null || args.tasks[args.newParentId] === undefined) {
    return 'target-not-in-graph';
  }
  if (args.tasks[args.selectedTaskId].rev === null) {
    return 'unknown-server-rev';
  }
  return null;
}

function rewireRefusal(
  blocker: RewireBlocker,
  taskId: string,
  parentId: string | null,
): Refusal {
  switch (blocker) {
    case 'run-terminal':
      return clientRefusal('E_RUN_TERMINAL');
    case 'no-run':
      return clientRefusal('E_RUN_NOT_FOUND');
    case 'no-selection':
      return clientRefusal('E_TASK_NOT_FOUND', {
        message: 'Select a task before re-routing.',
      });
    case 'target-not-in-graph':
      return clientRefusal('E_TASK_NOT_FOUND', {
        message: `The re-route target ${parentId === null ? '(none)' : shortId(parentId)} is not in the current graph.`,
      });
    case 'unknown-server-rev':
      return clientRefusal('E_RESYNC_REQUIRED', {
        message: `No committed server revision is known for task ${shortId(taskId)}; re-route is disabled until a snapshot confirms it.`,
        retryable: true,
      });
  }
}

/** True while the UI is missing a confirmed server snapshot. */
export function isSnapshotDegraded(snapshot: SnapshotState): boolean {
  return (
    snapshot.status === 'loading' ||
    snapshot.status === 'degraded' ||
    snapshot.status === 'failed'
  );
}

export function createUiStore(api: UiApi, options: UiStoreOptions = {}): StoreApi<UiState> {
  const unsubscribes: Array<() => void> = [];
  // Out-of-order TOOL_RESULTs wait here until their TOOL_CALL arrives. The buffer is
  // intentionally closure-local: it is never rendered and never leaves the store.
  const bufferedToolResults: BufferedToolResult[] = [];

  const bufferToolResult = (payload: {
    readonly tool_call_id: string;
    readonly run_id: string;
    readonly outcome: ToolOutcome;
    readonly code?: string | null;
    readonly bytes?: number | null;
    readonly sha256?: string | null;
  }): void => {
    if (bufferedToolResults.some((entry) => entry.toolCallId === payload.tool_call_id)) {
      return; // dedupe: the first result per tool-call id wins
    }
    const code = payload.code ?? undefined;
    bufferedToolResults.push({
      toolCallId: payload.tool_call_id,
      runId: payload.run_id,
      outcome: payload.outcome,
      ...(code === undefined ? {} : { code }),
      ...(payload.bytes === undefined ? {} : { bytes: payload.bytes }),
      ...(payload.sha256 === undefined ? {} : { sha256: payload.sha256 }),
    });
    if (bufferedToolResults.length > MAX_BUFFERED_TOOL_RESULTS) {
      bufferedToolResults.shift(); // bounded: oldest orphan is dropped, never rendered
    }
  };

  const takeBufferedToolResult = (toolCallId: string): BufferedToolResult | null => {
    const index = bufferedToolResults.findIndex((entry) => entry.toolCallId === toolCallId);
    if (index === -1) {
      return null;
    }
    const [entry] = bufferedToolResults.splice(index, 1);
    return entry ?? null;
  };

  const dropBufferedToolResultsForRun = (runId: string): void => {
    for (let index = bufferedToolResults.length - 1; index >= 0; index -= 1) {
      if (bufferedToolResults[index]?.runId === runId) {
        bufferedToolResults.splice(index, 1);
      }
    }
  };

  return createStore<UiState>()((set, get) => {
    const pushRefusal = (refusal: Refusal): void => {
      const current = get().refusals;
      const dedupe = current.filter(
        (entry) => entry.refusal.code !== refusal.code || Date.now() - entry.at > 500,
      );
      set({
        refusals: [
          { id: uuidv7(), refusal, at: Date.now() },
          ...dedupe,
        ].slice(0, 4),
      });
    };

    const approvalTimers = new Map<string, ReturnType<typeof setTimeout>>();
    const clearApprovalTimer = (approvalId: string): void => {
      const timer = approvalTimers.get(approvalId);
      if (timer !== undefined) {
        clearTimeout(timer);
        approvalTimers.delete(approvalId);
      }
    };
    const armApprovalAckTimer = (approvalId: string): void => {
      clearApprovalTimer(approvalId);
      const timer = setTimeout(() => {
        approvalTimers.delete(approvalId);
        const current = get().approvalResolve[approvalId];
        if (current?.status !== 'awaiting_server') {
          return;
        }
        set({
          approvalResolve: {
            ...get().approvalResolve,
            [approvalId]: {
              ...current,
              status: 'retryable',
              refusal: clientRefusal(APPROVAL_ACK_SETTLE_CODE),
            },
          },
        });
      }, APPROVAL_ACK_SETTLE_MS);
      approvalTimers.set(approvalId, timer);
    };
    let approvalReplayTimer: ReturnType<typeof setTimeout> | null = null;
    const clearApprovalReplayTimer = (): void => {
      if (approvalReplayTimer !== null) {
        clearTimeout(approvalReplayTimer);
        approvalReplayTimer = null;
      }
    };
    const settleApprovalReplayWindow = (): void => {
      clearApprovalReplayTimer();
      const replay = get().approvalReplay;
      if (!replay.window) {
        return;
      }
      const nowMs = Date.now();
      const approvalResolve = { ...get().approvalResolve };
      const approvalSeenEpoch = get().approvalSeenEpoch;
      for (const approval of get().approvals) {
        const resolve = approvalResolve[approval.approvalId] ?? initialApprovalResolveState();
        const display = approvalDisplayStatus(approval, resolve, nowMs);
        const settled = settleReplayStatus(
          display,
          approvalSeenEpoch[approval.approvalId],
          replay.epoch,
        );
        if (settled !== display) {
          approvalResolve[approval.approvalId] = { ...resolve, status: settled };
        }
      }
      // Window settled: only replayed/confirmed ids stay actionable; unreconfirmed
      // actionable records became read-only `unconfirmed`, never auto-sent.
      set({
        approvalReplay: settleReplay(replay),
        approvalResolve,
        approvalsStale: false,
        approvalSeenEpoch: {},
      });
    };
    const armApprovalReplayTimer = (): void => {
      clearApprovalReplayTimer();
      approvalReplayTimer = setTimeout(() => {
        approvalReplayTimer = null;
        settleApprovalReplayWindow();
      }, APPROVAL_REPLAY_SETTLE_MS);
    };
    const markApprovalReplayStale = (): void => {
      clearApprovalReplayTimer();
      const replay = get().approvalReplay;
      set({
        approvalReplay: replay.stale ? replay : markApprovalsStale(replay),
        approvalsStale: true,
      });
    };

    /* ------------------------------------------- server snapshot / resync wiring */

    let disposed = false;
    let initGeneration = 0;
    let socketOpen = false;
    let reconnectPending = false;
    let resyncSerial = 0;
    let resyncFollowUp = false;
    let snapshotInFlight: Promise<void> | null = null;
    let snapshotGeneration = -1;
    const inFlightAborts = new Set<AbortController>();
    let pendingRewire: {
      readonly taskId: string;
      readonly newParentId: string;
    } | null = null;

    const historyResponseRefusal = async (
      response: Response,
      label: string,
    ): Promise<RefusalError> => {
      let body: unknown = null;
      try {
        body = await response.json();
      } catch {
        // An HTTP error may carry no body; the status below still names the failure.
        body = null;
      }
      const record = asRecord(body);
      if (record !== null && typeof record.code === 'string') {
        const base = clientRefusal(record.code);
        return new RefusalError({
          code: record.code,
          message:
            typeof record.message === 'string'
              ? record.message
              : `${label} refused with ${record.code}.`,
          how_to_fix:
            typeof record.how_to_fix === 'string' ? record.how_to_fix : base.how_to_fix,
          retryable: typeof record.retryable === 'boolean' ? record.retryable : base.retryable,
        });
      }
      return new RefusalError(
        clientRefusal('E_WS_DISCONNECTED', {
          message: `${label} returned HTTP ${response.status} without a refusal body.`,
          retryable: true,
        }),
      );
    };

    const historyFetch = async (path: string, label: string): Promise<unknown> => {
      const controller = new AbortController();
      inFlightAborts.add(controller);
      try {
        const response = await fetchWithBoundTimeout(`${apiBaseUrl()}${path}`, {
          label,
          timeoutMs: options.snapshotTimeoutMs ?? DEFAULT_HTTP_TIMEOUT_MS,
          ...(options.fetchImpl === undefined ? {} : { fetchImpl: options.fetchImpl }),
          signal: controller.signal,
          init: {
            credentials: 'same-origin',
            headers: { accept: 'application/json' },
          },
        });
        if (!response.ok) {
          throw await historyResponseRefusal(response, label);
        }
        try {
          return (await response.json()) as unknown;
        } catch {
          // An unreadable 2xx body is not state: fail closed with a named refusal.
          throw schemaRefusal(`${label} returned a body that is not JSON.`);
        }
      } finally {
        inFlightAborts.delete(controller);
      }
    };

    /** Replaces the committed-revision cursor without ever inventing a revision. */
    const applyHistoryRevisionList = (list: HistoryRevisionList): void => {
      const history = get().history;
      const latest = list.latest ?? -1;
      let selected = Math.max(latest, 0);
      if (history.replayGraph !== null) {
        selected = list.revisions.includes(history.selected)
          ? history.selected
          : Math.max(latest, 0);
      }
      set({
        history: {
          ...history,
          latest,
          count: list.revisions.length,
          revisions: list.revisions,
          selected,
          replayGraph:
            history.replayGraph !== null && list.revisions.includes(history.replayGraph.revision)
              ? history.replayGraph
              : null,
          refreshStatus: 'ok',
          refreshRefusal: null,
        },
      });
    };

    /** Quiet revision-cursor refresh (no toast): recorded in `history.refreshStatus`. */
    const refreshCommittedRevisions = async (runId: string): Promise<void> => {
      if (disposed || api.kind === 'mock') {
        return;
      }
      set({
        history: { ...get().history, refreshStatus: 'loading', refreshRefusal: null },
      });
      try {
        const body = await historyFetch(
          historyRevisionsPath(runId),
          `GET /api/history/runs/${shortId(runId)}/revisions`,
        );
        if (disposed) {
          return;
        }
        applyHistoryRevisionList(decodeHistoryRevisions(body));
      } catch (error) {
        if (disposed) {
          return;
        }
        set({
          history: {
            ...get().history,
            refreshStatus: 'error',
            refreshRefusal: toRefusal(error),
          },
        });
      }
    };

    /**
     * Rehydrates run/tasks/history from the existing committed history API.
     *
     * The server snapshot is the authority: a run with no committed revision is
     * reported as `degraded`/`no-committed-revision` instead of rendering an
     * invented graph, and the persisted transcript is rehydrated from the
     * `/messages` route (a failed read is reported as `messages-unavailable`).
     */
    const performSnapshotRefresh = (source: SnapshotSource): Promise<void> => {
      const generation = initGeneration;
      if (disposed || api.kind === 'mock') {
        return Promise.resolve();
      }
      // A refresh belongs to the generation that started it. After a StrictMode
      // dispose/re-init the old read is aborted and must neither be reused nor
      // allowed to write its result into the new generation's state.
      if (snapshotInFlight !== null && snapshotGeneration === generation) {
        return snapshotInFlight;
      }
      const stale = (): boolean => disposed || generation !== initGeneration;
      const attempt = (async (): Promise<void> => {
        const capturedSerial = resyncSerial;
        const priorSnapshot = get().snapshot;
        set({
          snapshot: {
            ...priorSnapshot,
            status: 'loading',
            source,
            refusal: null,
            attempts: priorSnapshot.attempts + 1,
            updatedAt: Date.now(),
          },
        });
        try {
          const runs = decodeHistoryRuns(
            await historyFetch(HISTORY_RUNS_PATH, 'GET /api/history/runs'),
          );
          const current = get().run;
          const chosen = chooseHistoryRun(runs, current);
          let list: HistoryRevisionList = { revisions: [], latest: null };
          let projection: HistoryStateProjection | null = null;
          let historyMessages: readonly ChatMessage[] | null = null;
          let messagesUnavailable = false;
          if (chosen !== null) {
            list = decodeHistoryRevisions(
              await historyFetch(
                historyRevisionsPath(chosen.runId),
                `GET /api/history/runs/${shortId(chosen.runId)}/revisions`,
              ),
            );
            try {
              historyMessages = decodeHistoryMessages(
                await historyFetch(
                  historyMessagesPath(chosen.runId),
                  `GET /api/history/runs/${shortId(chosen.runId)}/messages`,
                ),
              );
            } catch {
              // A broken read path is reported as incomplete, not as an invented
              // transcript and not as a failure of the whole graph refresh.
              messagesUnavailable = true;
            }
            if (list.latest !== null) {
              projection = decodeHistoryState(
                await historyFetch(
                  historyStatePath(chosen.runId, list.latest),
                  `GET /api/history/runs/${shortId(chosen.runId)}/state`,
                ),
              );
              if (projection.revision !== list.latest) {
                throw schemaRefusal(
                  'The run state revision disagrees with the latest committed revision.',
                );
              }
            }
          }
          if (stale()) {
            return;
          }

          const liveRun = get().run;
          const history = get().history;
          if (
            chosen !== null &&
            liveRun !== null &&
            liveRun.id !== chosen.runId &&
            isRunActive(liveRun.status)
          ) {
            // A newer live run appeared while this read was in flight: never replace it.
            set({
              snapshot: {
                status: 'degraded',
                source,
                runId: liveRun.id,
                revision: null,
                revisionCount: history.count,
                incomplete: ['run-changed', 'messages-unavailable'],
                refusal: null,
                attempts: get().snapshot.attempts,
                updatedAt: Date.now(),
              },
            });
            return;
          }
          if (chosen === null && liveRun !== null && isRunActive(liveRun.status)) {
            set({
              snapshot: {
                status: 'degraded',
                source,
                runId: liveRun.id,
                revision: null,
                revisionCount: history.count,
                incomplete: ['run-changed', 'messages-unavailable'],
                refusal: null,
                attempts: get().snapshot.attempts,
                updatedAt: Date.now(),
              },
            });
            return;
          }

          const incomplete: SnapshotIncomplete[] = [];
          if (chosen === null) {
            incomplete.push('no-run');
          } else {
            if (messagesUnavailable) {
              incomplete.push('messages-unavailable');
            }
            if (projection === null) {
              incomplete.push('no-committed-revision');
            }
          }
          const status: SnapshotState['status'] =
            chosen === null ? 'ready' : projection !== null ? 'ready' : 'degraded';
          const latest = list.latest ?? -1;
          const runStatus =
            chosen !== null &&
            liveRun !== null &&
            liveRun.id === chosen.runId &&
            isTerminal(liveRun.status) &&
            !isTerminal(chosen.status)
              ? liveRun.status
              : chosen?.status ?? null;
          const resolvedGap = resyncSerial === capturedSerial;
          const liveStreaming =
            get().streamingRunId !== null &&
            get().run !== null &&
            chosen !== null &&
            get().run?.id === chosen.runId;
          const liveRunCurrent = get().run;
          const preserveLiveTasks =
            chosen !== null &&
            liveRunCurrent !== null &&
            liveRunCurrent.id === chosen.runId &&
            isRunActive(liveRunCurrent.status);
          const liveTasks = get().tasks;
          set({
            // A committed snapshot can lag the live stream. Replacing the
            // transcript mid-stream would erase the partial assistant reply.
            ...(chosen !== null && historyMessages !== null && !liveStreaming
              ? { messages: historyMessages }
              : {}),
            ...(chosen === null
              ? { tasks: {} }
              : projection !== null
                ? {
                    tasks: preserveLiveTasks
                      ? mergeSnapshotTasks(projection.tasks, liveTasks)
                      : projection.tasks,
                  }
                : {}),
            run:
              chosen === null
                ? null
                : { id: chosen.runId, status: runStatus ?? chosen.status, errorCode: chosen.errorCode },
            graphRev: get().graphRev + 1,
            history: {
              ...history,
              runs,
              latest,
              count: list.revisions.length,
              revisions: list.revisions,
              selected: Math.max(latest, 0),
              replayGraph: null,
              refreshStatus: 'ok',
              refreshRefusal: null,
            },
            snapshot: {
              status,
              source,
              runId: chosen?.runId ?? null,
              revision: projection?.revision ?? null,
              revisionCount: list.revisions.length,
              incomplete,
              refusal: null,
              attempts: get().snapshot.attempts,
              updatedAt: Date.now(),
            },
            resyncGap: resolvedGap ? null : get().resyncGap,
            connection: resolvedGap && socketOpen ? 'open' : get().connection,
          });
          if (!resolvedGap) {
            // A newer gap arrived while this snapshot was read; it cannot clear it.
            scheduleResyncFollowUp();
          }
        } catch (error) {
          if (stale()) {
            return;
          }
          const failure = toRefusal(error);
          set({
            snapshot: {
              ...get().snapshot,
              status: 'failed',
              source,
              refusal: failure,
              updatedAt: Date.now(),
            },
            ...(source === 'resync' || source === 'refusal'
              ? { connection: 'reconnecting' as const }
              : {}),
          });
          if (source === 'resync' || source === 'refusal' || source === 'manual') {
            pushRefusal(failure);
          }
        }
      })();
      const tracked = attempt.finally(() => {
        if (snapshotInFlight === tracked) {
          snapshotInFlight = null;
          snapshotGeneration = -1;
        }
      });
      snapshotInFlight = tracked;
      snapshotGeneration = generation;
      return tracked;
    };

    const scheduleResyncFollowUp = (): void => {
      if (disposed || resyncFollowUp) {
        return;
      }
      resyncFollowUp = true;
      setTimeout(() => {
        resyncFollowUp = false;
        if (!disposed) {
          void performSnapshotRefresh('resync');
        }
      }, 0);
    };

    const raiseResyncGap = (gap: ResyncGap): void => {
      resyncSerial += 1;
      set({ resyncGap: gap, connection: 'reconnecting' });
      if (gap.cause === 'event') {
        // The server does not send an ERROR frame for a lost range: surface the
        // named refusal here so the gap is visible, not just an internal flag.
        pushRefusal(
          clientRefusal('E_RESYNC_REQUIRED', {
            message: `Missed events: expected sequence ${gap.expectedSeq} but the client last applied ${gap.lastSeq}. Rehydrating from the committed snapshot.`,
          }),
        );
      }
      void performSnapshotRefresh(gap.cause === 'event' ? 'resync' : 'refusal');
    };

    /**
     * Fallback parent for our own rewire when the producer omits the additive
     * `NODE_REWIRE.parent_task_id` (pre-1.3): the parent that the server ACKed
     * for the command is the new edge. Matched by task id (one rewire in flight
     * per task) and consumed once; the payload's parent always wins when present.
     */
    const takePendingRewire = (taskId: string): { readonly newParentId: string } | null => {
      const intent = pendingRewire;
      if (intent === null || intent.taskId !== taskId) {
        return null;
      }
      pendingRewire = null;
      return { newParentId: intent.newParentId };
    };

    /** Reads one node's committed server revision on demand (never guesses one). */
    const fetchNodeServerRev = async (runId: string, taskId: string): Promise<number | null> => {
      const list = decodeHistoryRevisions(
        await historyFetch(
          historyRevisionsPath(runId),
          `GET /api/history/runs/${shortId(runId)}/revisions`,
        ),
      );
      if (list.latest === null) {
        applyHistoryRevisionList(list);
        return null;
      }
      const projection = decodeHistoryState(
        await historyFetch(
          historyStatePath(runId, list.latest),
          `GET /api/history/runs/${shortId(runId)}/state`,
        ),
      );
      applyHistoryRevisionList(list);
      const record = projection.tasks[taskId];
      if (record === undefined) {
        return null;
      }
      const existing = get().tasks[taskId];
      if (existing !== undefined) {
        set({ tasks: { ...get().tasks, [taskId]: { ...existing, rev: record.rev } } });
      }
      return record.rev;
    };

    const applyEvent = (event: DecodedEvent, envelope: Envelope<unknown>): void => {
      if (typeof envelope.seq === 'number' && envelope.seq > get().lastSeq) {
        // The resolved store sequence follows the transport sequence; the gap check
        // below uses the RESYNC_REQUIRED payload, never this cursor.
        set({ lastSeq: envelope.seq });
      }
      const activeRunId = get().run?.id ?? null;
      const eventRunId = envelope.run_id ?? null;
      if (
        activeRunId !== null &&
        eventRunId !== null &&
        eventRunId !== activeRunId &&
        (event.name.startsWith('NODE_') || event.name === 'HISTORY_SNAPSHOT')
      ) {
        // A delayed replay from another run must never mutate this run's graph.
        return;
      }
      switch (event.name) {
        case 'ACK':
          return;
        case 'ERROR': {
          pushRefusal(event.payload);
          if (event.payload.code === 'E_KEY_MISSING') {
            set({ settings: { ...get().settings, keyPresent: false } });
          }
          if (event.payload.code === 'E_RESYNC_REQUIRED') {
            // A command-refused resync (e.g. stale rewire CAS) is not a transport
            // gap, but it still requires a real resnapshot — never a retry loop.
            raiseResyncGap({ cause: 'refusal', code: event.payload.code });
          }
          return;
        }
        case 'SERVER_HELLO': {
          socketOpen = true;
          set({
            lastSeq: event.payload.last_seq ?? get().lastSeq,
            // The hello that follows RESYNC_REQUIRED must NOT present the lost range
            // as Connected; the gap clears only when a snapshot rehydrate settles.
            connection: get().resyncGap === null ? 'open' : 'reconnecting',
            // Every hello is a reconnect/replay boundary: keep the current records
            // but mark them stale until the bounded replay window settles.
            approvalReplay: beginReplayWindow(
              get().approvalReplay.stale
                ? get().approvalReplay
                : markApprovalsStale(get().approvalReplay),
              'hello',
              Date.now(),
            ),
            approvalsStale: true,
          });
          armApprovalReplayTimer();
          const reconnected = reconnectPending;
          reconnectPending = false;
          if (
            reconnected ||
            get().snapshot.status === 'idle' ||
            get().snapshot.status === 'degraded' ||
            get().snapshot.status === 'failed'
          ) {
            // Reconnect/reload with no confirmed server snapshot yet (e.g. boot while
            // the server was down): rehydrate quietly. A server restart also resets
            // the bus cursor, so a transport reconnect must re-read the committed
            // state even when the pre-restart snapshot was 'ready'.
            void performSnapshotRefresh('reconnect');
          }
          return;
        }
        case 'RESYNC_REQUIRED': {
          markApprovalReplayStale();
          set({ connection: 'reconnecting' });
          raiseResyncGap({
            cause: 'event',
            expectedSeq: event.payload.expected_seq,
            lastSeq: event.payload.last_seq,
          });
          return;
        }
        case 'RUN_CREATED': {
          const previous = get().run;
          const freshRun = previous === null || previous.id !== event.payload.run_id;
          set({
            run: { id: event.payload.run_id, status: event.payload.status, errorCode: null },
            // A new run owns a new graph and a new revision line: keeping the previous
            // run's tasks/revisions would mix two runs' state (the old global cursor
            // bug). The chat transcript is the operator's conversation and is kept.
            ...(freshRun
              ? {
                  tasks: {},
                  selectedTaskId: null,
                  toolActivity: [],
                  metrics: null,
                  streamingRunId: null,
                  history: initialHistory(),
                  snapshot: initialSnapshot(),
                }
              : {}),
            graphRev: get().graphRev + 1,
          });
          return;
        }
        case 'RUN_STATUS': {
          // Deterministic cleanup: once a run is terminal, an orphan TOOL_RESULT for it
          // can never be replayed by a later TOOL_CALL, so drop the buffered entry.
          if (isTerminal(event.payload.status)) {
            dropBufferedToolResultsForRun(event.payload.run_id);
          }
          const run = get().run;
          const streamingThisRun = get().streamingRunId === event.payload.run_id;
          if (run === null) {
            // A late client (or a CLI-owned run) can learn the run only from a
            // status frame. Adopt an active or still-streaming run; never adopt
            // a stale terminal frame from an older run.
            if (!isTerminal(event.payload.status) || streamingThisRun) {
              set({
                run: {
                  id: event.payload.run_id,
                  status: event.payload.status,
                  errorCode: event.payload.error_code ?? null,
                },
                ...(isTerminal(event.payload.status)
                  ? {
                      streamingRunId: null,
                      messages: finalizeStreamingMessages(get().messages, event.payload.run_id),
                    }
                  : {}),
                graphRev: get().graphRev + 1,
              });
            }
            return;
          }
          if (run.id !== event.payload.run_id) {
            // A terminal frame for another run must still close that run's
            // streaming row: a CLI wait/completion or a fast run switch would
            // otherwise leave the transcript marked as streaming forever.
            const finalized = isTerminal(event.payload.status)
              ? finalizeStreamingMessages(get().messages, event.payload.run_id)
              : get().messages;
            if (finalized !== get().messages || streamingThisRun) {
              set({
                ...(streamingThisRun ? { streamingRunId: null } : {}),
                messages: finalized,
              });
            }
            return;
          }
          set({
            run: {
              id: run.id,
              status: event.payload.status,
              errorCode: event.payload.error_code ?? null,
            },
            ...(event.payload.error_code === 'E_KEY_MISSING'
              ? { settings: { ...get().settings, keyPresent: false } }
              : {}),
            streamingRunId:
              isTerminal(event.payload.status) && streamingThisRun
                ? null
                : get().streamingRunId,
            messages: isTerminal(event.payload.status)
              ? finalizeStreamingMessages(get().messages, event.payload.run_id)
              : get().messages,
            graphRev: get().graphRev + 1,
          });
          if (event.payload.error_code === 'E_KEY_MISSING') {
            pushRefusal(clientRefusal('E_KEY_MISSING'));
          } else if (event.payload.error_code !== null && event.payload.error_code !== undefined) {
            pushRefusal(clientRefusal(event.payload.error_code));
          }
          if (isTerminal(event.payload.status)) {
            // A terminal run commits its final snapshot: refresh the revision cursor so
            // time travel becomes reachable without waiting for a reload. Quiet: any
            // failure is recorded in `history.refreshStatus`, never faked.
            void refreshCommittedRevisions(event.payload.run_id);
          }
          return;
        }
        case 'REPLY_DELTA': {
          const runId = event.payload.run_id;
          if (get().run !== null && get().run?.id !== runId) {
            // A delayed/foreign stream must not append to this run's chat.
            return;
          }
          const delta = event.payload.text_delta;
          if (delta.length === 0) {
            // No-op deltas would otherwise copy the transcript and (on the first
            // frame) mount an empty assistant row. Text/stream state is unchanged.
            return;
          }
          // Immutable state requires a new array reference for a streaming text
          // update; the copy is bounded by the transcript, and finalized rows keep
          // their identity so React only re-renders the tail (see ChatCore).
          const messages = [...get().messages];
          const lastIndex = messages.length - 1;
          const last = messages[lastIndex];
          if (last && last.role === 'assistant' && last.runId === runId && last.streaming) {
            messages[lastIndex] = { ...last, text: last.text + delta };
          } else {
            messages.push({
              id: uuidv7(),
              role: 'assistant',
              text: delta,
              runId,
              streaming: true,
            });
          }
          set({ messages, streamingRunId: runId });
          return;
        }
        case 'REPLY_DONE': {
          const doneRunId = event.payload.run_id;
          const streamingThisRun = get().streamingRunId === doneRunId;
          // Finalize this run's row even if a newer run already owns `run`:
          // the reply that streamed into this transcript is finished.
          const finalized = finalizeStreamingMessages(get().messages, doneRunId);
          if (finalized === get().messages && !streamingThisRun) {
            return;
          }
          set({
            messages: finalized,
            streamingRunId: streamingThisRun ? null : get().streamingRunId,
          });
          return;
        }
        case 'NODE_SPAWN':
        case 'NODE_DRAG':
        case 'NODE_REWIRE':
        case 'NODE_STATE': {
          const payload = event.payload;
          const existing = get().tasks[payload.task_id];
          const incomingRev = readNodeRev(payload) ?? null;
          // A replayed NODE_SPAWN must not regress a live/completed node. The
          // server replay window can re-deliver the spawn after reconnect; the
          // spawn is a creation fact, not a state transition.
          if (
            event.name === 'NODE_SPAWN' &&
            existing !== undefined &&
            (incomingRev === null || existing.rev === null || incomingRev <= existing.rev)
          ) {
            return;
          }
          if (
            existing !== undefined &&
            incomingRev !== null &&
            existing.rev !== null &&
            incomingRev <= existing.rev
          ) {
            return;
          }
          const kind =
            payload.kind !== null && payload.kind !== undefined
              ? payload.kind
              : (existing?.kind ?? 'worker');
          // NODE_REWIRE: the additive `parent_task_id` (1.3) is the server's new
          // edge. For a pre-1.3 producer that omits it, the parent is the one the
          // server ACKed for our own REWIRE_TASK (consumed once per task).
          const intended =
            event.name === 'NODE_REWIRE' ? takePendingRewire(payload.task_id) : null;
          const payloadParent =
            payload.parent_task_id !== null && payload.parent_task_id !== undefined
              ? payload.parent_task_id
              : null;
          const record: TaskRecord = {
            id: payload.task_id,
            parentId: payloadParent ?? intended?.newParentId ?? existing?.parentId ?? null,
            kind,
            label: payload.label ?? existing?.label ?? fallbackTaskLabel(payload.kind ?? null, payload.task_id),
            // A spawn is a creation fact, never a state transition: replaying it
            // after a terminal NODE_STATE must not reset a completed/killed node.
            state:
              event.name === 'NODE_SPAWN' && existing !== undefined
                ? existing.state
                : payload.state,
            progress:
              payload.progress !== null && payload.progress !== undefined
                ? payload.progress
                : (existing?.progress ?? null),
            errorCode: payload.error_code ?? existing?.errorCode ?? null,
            // `rev` is the server's per-node revision (same value the rewire CAS
            // compares); an absent field stays unconfirmed (null) and is never
            // guessed. NODE_DRAG is not a graph mutation, so the last
            // confirmed revision survives it.
            rev:
              readNodeRev(payload) ??
              (event.name === 'NODE_DRAG' ? (existing?.rev ?? null) : null),
          };
          set({
            tasks: { ...get().tasks, [payload.task_id]: record },
            graphRev: get().graphRev + 1,
          });
          if (
            event.name === 'NODE_REWIRE' &&
            intended === null &&
            payloadParent === null
          ) {
            // Another client rewired and the frozen event cannot carry the new
            // parent, so the edge can only be learned from a committed snapshot.
            void performSnapshotRefresh('rewire');
          }
          return;
        }
        case 'AGENT_MESSAGE': {
          set({ agentMessageVersion: get().agentMessageVersion + 1 });
          return;
        }
        case 'RUN_METRICS': {
          set({ metrics: event.payload });
          return;
        }
        case 'TOOL_CALL': {
          const payload = event.payload;
          // Dedupe by provider tool-call id: the first identity wins and arrival order
          // is preserved; a replay must not duplicate the activity row.
          const existing = get().toolActivity.find(
            (record) => record.toolCallId === payload.tool_call_id,
          );
          if (existing) {
            return;
          }
          // A terminal result may have arrived before this call (fan-out ordering):
          // create the row directly in its terminal state and consume the buffer entry.
          const buffered = takeBufferedToolResult(payload.tool_call_id);
          // Attribution is server data only: `task_id` comes from the call, and a
          // buffered result may already carry the result size/digest.
          const record: ToolActivityRecord = buffered
            ? {
                toolCallId: payload.tool_call_id,
                tool: payload.tool,
                argsSha256: payload.args_sha256,
                turn: payload.turn,
                outcome: buffered.outcome,
                at: envelope.ts,
                taskId: payload.task_id,
                ...(buffered.code === undefined ? {} : { code: buffered.code }),
                ...(buffered.bytes === undefined ? {} : { bytes: buffered.bytes }),
                ...(buffered.sha256 === undefined ? {} : { sha256: buffered.sha256 }),
              }
            : {
                toolCallId: payload.tool_call_id,
                tool: payload.tool,
                argsSha256: payload.args_sha256,
                turn: payload.turn,
                outcome: 'pending',
                at: envelope.ts,
                taskId: payload.task_id,
              };
          set({ toolActivity: [...get().toolActivity, record] });
          return;
        }
        case 'TOOL_RESULT': {
          const payload = event.payload;
          // A malformed/unknown outcome never transitions activity (no fabricated state).
          if (!isToolOutcome(payload.outcome)) {
            return;
          }
          const existing = get().toolActivity.find(
            (record) => record.toolCallId === payload.tool_call_id,
          );
          if (!existing) {
            // Stale late result: the current run already reached a terminal state, so a
            // later TOOL_CALL cannot belong to a live execution. Drop it instead of
            // buffering a terminal outcome that must never be replayed.
            const currentRun = get().run;
            if (
              currentRun !== null &&
              currentRun.id === payload.run_id &&
              isTerminal(currentRun.status)
            ) {
              return;
            }
            // Out-of-order result: buffer it (bounded/deduped) and replay on the late
            // TOOL_CALL. Never fabricate a row from a result alone.
            bufferToolResult(payload);
            return;
          }
          // Terminal outcome is immutable: the first result wins, later results are ignored.
          if (existing.outcome !== 'pending') {
            return;
          }
          const code = payload.code ?? undefined;
          set({
            toolActivity: get().toolActivity.map((record) =>
              record.toolCallId === payload.tool_call_id
                ? {
                    ...record,
                    outcome: payload.outcome,
                    ...(code === undefined ? {} : { code }),
                    ...(payload.bytes === undefined ? {} : { bytes: payload.bytes }),
                    ...(payload.sha256 === undefined ? {} : { sha256: payload.sha256 }),
                  }
                : record,
            ),
          });
          return;
        }
        case 'APPROVAL_REQUEST': {
          const replay = get().approvalReplay;
          const replayWindowOpen = isReplayWindowOpen(replay);
          const existingResolve = get().approvalResolve[event.payload.approval_id];
          const approval: ApprovalRecord = {
            approvalId: event.payload.approval_id,
            tool: event.payload.tool,
            nonce: event.payload.nonce ?? null,
            decision: event.payload.decision ?? null,
            requestedAt: Date.now(),
            runId: event.payload.run_id ?? null,
            taskId: event.payload.task_id ?? null,
            toolCallId: event.payload.tool_call_id ?? null,
            argsSha256: event.payload.args_sha256 ?? null,
            argsPreview: event.payload.args_preview ?? null,
            expiresAt: event.payload.expires_at ?? null,
          };
          // Replay dedupe is id-based: replace-by-id keeps exactly one record; the
          // replay window only stamps confirmation and never sends a command.
          const approvalResolve = {
            ...get().approvalResolve,
            [approval.approvalId]: {
              ...(existingResolve ?? initialApprovalResolveState()),
              status: approvalStatusAfterRequest(
                existingResolve?.status,
                event.payload,
                Date.now(),
              ),
            },
          };
          const approvalSeenEpoch = replayWindowOpen
            ? stampReplayEpoch(get().approvalSeenEpoch, approval.approvalId, replay.epoch)
            : get().approvalSeenEpoch;
          set({
            approvals: [
              ...get().approvals.filter((a) => a.approvalId !== approval.approvalId),
              approval,
            ],
            approvalResolve,
            approvalSeenEpoch,
          });
          return;
        }
        case 'APPROVAL_RESOLVED': {
          clearApprovalTimer(event.payload.approval_id);
          const approvalResolve = { ...get().approvalResolve };
          delete approvalResolve[event.payload.approval_id];
          const approvalSeenEpoch = { ...get().approvalSeenEpoch };
          delete approvalSeenEpoch[event.payload.approval_id];
          set({
            approvals: get().approvals.filter(
              (approval) => approval.approvalId !== event.payload.approval_id,
            ),
            approvalResolve,
            approvalSeenEpoch,
          });
          return;
        }
        case 'SETTINGS_STATUS_RESULT': {
          set({
            settings: {
              ...get().settings,
              status: 'ok',
              provider: event.payload.provider,
              model: event.payload.model,
              keyPresent: event.payload.key_present,
              thinking: event.payload.thinking ?? null,
              reasoningEffort: event.payload.reasoning_effort ?? null,
              refusal: null,
              lastProbe: Date.now(),
            },
          });
          return;
        }
        case 'FILE_TREE_RESULT': {
          set({
            fileTree: {
              ...get().fileTree,
              status: 'ok',
              root: event.payload.root,
              entries: event.payload.entries,
              refusal: null,
            },
          });
          return;
        }
        case 'FILE_CONTENT': {
          set({
            fileTree: {
              ...get().fileTree,
              contentStatus: 'ok',
              content: event.payload,
              selectedPath: event.payload.path,
            },
          });
          return;
        }
        case 'HISTORY_SNAPSHOT': {
          const activeRunId = get().run?.id ?? null;
          if (activeRunId !== null && event.payload.run_id !== activeRunId) {
            // A delayed replay from another tab/run must never overwrite this
            // client's revision list or enter read-only mode.
            return;
          }
          const revision = event.payload.revision;
          const history = get().history;
          const revisions = insertRevision(history.revisions, revision);
          set({
            history: {
              ...history,
              latest: Math.max(history.latest, revision),
              count: revisions.length,
              revisions,
              selected: revision,
              replayGraph: { revision, graph: event.payload.graph },
            },
          });
          return;
        }
        case 'PING':
        case 'PONG':
          return;
      }
    };

    const run = async <N extends ServerCommandName>(
      name: N,
      payload: CommandPayloads[N],
      onRefusal?: (refusal: Refusal) => void,
    ): Promise<boolean> => {
      const correlationId = uuidv7();
      set({
        pendingCommands: {
          ...get().pendingCommands,
          [correlationId]: { name, startedAt: Date.now() },
        },
      });
      try {
        await api.send(name, payload, { correlationId });
        return true;
      } catch (error) {
        const refusal =
          error instanceof RefusalError ? error.refusal : clientRefusal('E_WS_DISCONNECTED');
        onRefusal?.(refusal);
        // Server ERROR frames are already toasted by applyEvent; only local
        // pre-send/transport refusals need a toast here.
        if (refusal.code === 'E_WS_DISCONNECTED' || refusal.code === 'E_PATH_OUTSIDE_JAIL' || refusal.code === 'E_FILE_TOO_LARGE') {
          pushRefusal(refusal);
        }
        if (refusal.code === 'E_KEY_MISSING') {
          set({ settings: { ...get().settings, keyPresent: false } });
        }
        return false;
      } finally {
        const pending = { ...get().pendingCommands };
        delete pending[correlationId];
        set({ pendingCommands: pending });
      }
    };

    const guardActiveRun = (): RunRecord | null => {
      const current = get().run;
      if (!current) {
        pushRefusal(clientRefusal('E_RUN_NOT_FOUND'));
        return null;
      }
      if (!isRunActive(current.status)) {
        // The run exists but is over: E_RUN_TERMINAL is the server's own code
        // for "no child can be spawned into a terminal run".
        pushRefusal(clientRefusal('E_RUN_TERMINAL'));
        return null;
      }
      return current;
    };

    return {
      initialized: false,
      apiKind: api.kind,
      connection: 'idle',
      connectionRefusal: null,
      session: null,
      lastSeq: 0,
      run: null,
      tasks: {},
      graphRev: 0,
      selectedTaskId: null,
      messages: [],
      streamingRunId: null,
      agentMessageVersion: 0,
      metrics: null,
      history: initialHistory(),
      snapshot: initialSnapshot(),
      resyncGap: null,
      approvals: [],
      resolvingApprovals: [],
      approvalResolve: {},
      approvalReplay: initialApprovalReplayState(),
      approvalsStale: false,
      approvalSeenEpoch: {},
      toolActivity: [],
      refusals: [],
      pendingCommands: {},
      view: { nav: 'workspace', tab: 'workspace' },
      viewport: { x: 0, y: 0, zoom: 1 },
      chat: { pinned: true, visible: initialChatVisible() },
      settings: initialSettings(),
      fileTree: initialFileTree(),
      project: baseProject(),
      activeSessionId: null,
      reducedMotionOverride: readReducedMotionOverride(),
      diffOpen: false,

      async init(): Promise<void> {
        if (get().initialized) {
          return;
        }
        disposed = false;
        const generation = ++initGeneration;
        set({ initialized: true, connection: 'connecting' });
        unsubscribes.push(
          api.subscribe((event, envelope) => applyEvent(event, envelope)),
          api.subscribeStatus((status, detail) => {
            socketOpen = status === 'open';
            if (status === 'reconnecting') {
              reconnectPending = true;
            }
            set({
              // An unresolved RESYNC gap outranks a transport "open": the events in the
              // gap are still missing until a snapshot rehydrate settles.
              connection: status === 'open' && get().resyncGap !== null ? 'reconnecting' : status,
              connectionRefusal: detail ?? get().connectionRefusal,
            });
          }),
          api.subscribeClientError((refusal) => pushRefusal(refusal)),
        );
        // Hydrate before the socket opens: the server does not replay old state on a
        // fresh hello (`Replay::Current`), so the committed history read is the only
        // way a reload can show the active run and make CANCEL_RUN available.
        await performSnapshotRefresh('boot');
        if (disposed || generation !== initGeneration) {
          return;
        }
        try {
          const session = await api.open();
          if (disposed || generation !== initGeneration) {
            // The shared API now belongs to the newer generation; closing it here
            // would kill that live socket. dispose() already closed the old one.
            return;
          }
          socketOpen = true;
          set({
            session,
            connection: get().resyncGap === null ? 'open' : 'reconnecting',
            lastSeq: session.last_seq,
          });
        } catch (error) {
          if (disposed || generation !== initGeneration) {
            return;
          }
          const refusal =
            error instanceof RefusalError ? error.refusal : clientRefusal('E_WS_DISCONNECTED');
          set({ connection: 'error', connectionRefusal: refusal });
          pushRefusal(refusal);
        }
      },

      dispose(): void {
        disposed = true;
        initGeneration += 1;
        // A disposed store has no socket and no owned snapshot read. Dropping the
        // in-flight handle here lets a StrictMode re-init hydrate immediately
        // instead of awaiting the previous generation's aborted boot.
        socketOpen = false;
        reconnectPending = false;
        snapshotInFlight = null;
        snapshotGeneration = -1;
        set({ initialized: false });
        for (const controller of inFlightAborts) {
          controller.abort();
        }
        inFlightAborts.clear();
        clearApprovalReplayTimer();
        for (const timer of approvalTimers.values()) {
          clearTimeout(timer);
        }
        approvalTimers.clear();
        for (const unsubscribe of unsubscribes) {
          unsubscribe();
        }
        unsubscribes.length = 0;
        api.close();
      },

      async submitPrompt(
        text: string,
        options?: {
          readonly sessionId?: string;
          readonly policy?: 'classic' | 'plan' | 'standard' | 'trusted';
        },
      ): Promise<void> {
        const refusalCode = validatePromptText(text);
        if (refusalCode) {
          pushRefusal(clientRefusal(refusalCode));
          return;
        }
        set({
          messages: [
            ...get().messages,
            { id: uuidv7(), role: 'user', text, runId: null, streaming: false },
          ],
        });
        const payload: {
          text: string;
          project_id: string;
          session_id?: string;
          permission_policy?: 'classic' | 'plan' | 'standard' | 'trusted';
        } = { text, project_id: get().project.id };
        if (options?.sessionId) payload.session_id = options.sessionId;
        if (options?.policy) payload.permission_policy = options.policy;
        await run('SUBMIT_PROMPT', payload);
      },

      async spawnSubHive(parentTaskId?: string): Promise<void> {
        const current = guardActiveRun();
        if (!current) {
          return;
        }
        const tasks = get().tasks;
        const selected = get().selectedTaskId;
        // An explicit card click wins; otherwise the selected card; otherwise
        // the run's HyperAgent root. Never fall back to an arbitrary task.
        const candidateId =
          parentTaskId ??
          selected ??
          Object.values(tasks).find((task) => task.kind === 'hyperagent')?.id ??
          null;
        const parentTask = candidateId === null ? null : (tasks[candidateId] ?? null);
        const gate = spawnGate({
          parent:
            parentTask === null
              ? null
              : { id: parentTask.id, kind: parentTask.kind },
          runStatus: current.status,
          connection: get().connection,
          replaying: get().history.replayGraph !== null,
          pending: Object.keys(get().pendingCommands).length > 0,
        });
        if (gate.plan === null) {
          const refusal =
            parentTaskId !== undefined && parentTask === null
              ? clientRefusal('E_TASK_NOT_FOUND', {
                  message: `Task ${shortId(parentTaskId)} is not in the current projection; no spawn was sent.`,
                  how_to_fix: 'Refresh the run projection, then select the parent card again.',
                })
              : clientRefusal('E_TOOL_NOT_ALLOWED', {
                  message: `${gate.reason ?? 'This parent cannot spawn.'} No SPAWN_SUB_HIVE was sent.`,
                  how_to_fix:
                    'Send SPAWN_SUB_HIVE from a HyperAgent, a SuperAgent, or an Agent (worker) parent only.',
                });
          pushRefusal(refusal);
          return;
        }
        // The exact frozen payload: selected parent + the matrix's wire role.
        await run('SPAWN_SUB_HIVE', {
          parent_task_id: gate.plan.parentId,
          role: gate.plan.role,
        });
      },

      async rewireSelected(newParentId: string): Promise<void> {
        const current = get().run;
        if (current === null) {
          pushRefusal(clientRefusal('E_RUN_NOT_FOUND'));
          return;
        }
        const taskId = get().selectedTaskId;
        const before = rewireBlocker({
          run: current,
          tasks: get().tasks,
          selectedTaskId: taskId,
          newParentId,
        });
        if (before === 'unknown-server-rev' && taskId !== null) {
          // The committed snapshot is the only authority for a node revision. One
          // bounded read here; a stale one is then refused by the server CAS and
          // surfaced (never retried with the same guessed value).
          try {
            await fetchNodeServerRev(current.id, taskId);
          } catch (error) {
            const failure = toRefusal(error);
            pushRefusal({
              ...failure,
              message: `${failure.message} Re-route needs task ${shortId(taskId)}'s committed server revision.`,
            });
            return;
          }
        }
        const blocker = rewireBlocker({
          run: get().run,
          tasks: get().tasks,
          selectedTaskId: taskId,
          newParentId,
        });
        if (blocker !== null) {
          pushRefusal(rewireRefusal(blocker, taskId ?? '', newParentId));
          return;
        }
        const rev = taskId === null ? null : (get().tasks[taskId]?.rev ?? null);
        if (taskId === null || rev === null) {
          pushRefusal(rewireRefusal('unknown-server-rev', taskId ?? '', newParentId));
          return;
        }
        // ACK-before-mutation: the parent lands on NODE_REWIRE (matched by task id),
        // because the frozen event payload carries no parent of its own.
        pendingRewire = { taskId, newParentId };
        const ok = await run('REWIRE_TASK', {
          task_id: taskId,
          new_parent_id: newParentId,
          expected_rev: rev,
        });
        if (!ok) {
          pendingRewire = null;
        }
      },

      async cancelRun(): Promise<void> {
        const current = get().run;
        if (!current || !isRunActive(current.status)) {
          pushRefusal(clientRefusal('E_RUN_TERMINAL'));
          return;
        }
        await run('CANCEL_RUN', { run_id: current.id });
      },

      async killWorker(): Promise<void> {
        const taskId = get().selectedTaskId;
        if (!taskId) {
          pushRefusal(clientRefusal('E_TASK_NOT_FOUND'));
          return;
        }
        const task = get().tasks[taskId];
        if (!task || (task.kind !== 'worker' && task.kind !== 'tool')) {
          pushRefusal(clientRefusal('E_KILL_DENIED'));
          return;
        }
        await run('KILL_WORKER', { task_id: taskId });
      },

      async resolveApproval(
        approvalId: string,
        decision: 'approve' | 'deny',
        reason?: string,
      ): Promise<ApprovalResolveOutcome> {
        if (get().resolvingApprovals.includes(approvalId)) {
          return {
            ok: false,
            kind: 'ignored',
            refusal: clientRefusal('E_APPROVAL_REPLAY', {
              message: 'an APPROVAL_RESOLVE is already in flight for this approval',
            }),
          };
        }
        const approval = get().approvals.find((candidate) => candidate.approvalId === approvalId);
        if (!approval) {
          const refusal = clientRefusal('E_APPROVAL_NOT_FOUND');
          pushRefusal(refusal);
          return { ok: false, kind: 'stale', refusal };
        }
        if (!approval.nonce) {
          const refusal = clientRefusal('E_SCHEMA_INVALID', {
            message: 'APPROVAL_REQUEST carried no nonce (contract gap); cannot resolve.',
          });
          pushRefusal(refusal);
          return { ok: false, kind: 'operator', refusal };
        }

        const previous = get().approvalResolve[approvalId] ?? initialApprovalResolveState();
        const attempts = previous.attempts + 1;
        const lastAttemptAt = Date.now();
        set({
          resolvingApprovals: [...get().resolvingApprovals, approvalId],
          approvalResolve: {
            ...get().approvalResolve,
            [approvalId]: {
              ...previous,
              status: 'resolving',
              attempts,
              lastDecision: decision,
              refusal: null,
              lastAttemptAt,
            },
          },
        });

        let refusal: Refusal | null = null;
        const ok = await run(
          'APPROVAL_RESOLVE',
          {
            approval_id: approvalId,
            nonce: approval.nonce,
            decision,
            ...(reason === undefined ? {} : { reason }),
          },
          (failure) => {
            refusal = failure;
          },
        );
        set({
          resolvingApprovals: get().resolvingApprovals.filter((id) => id !== approvalId),
        });

        if (ok) {
          set({
            approvalResolve: {
              ...get().approvalResolve,
              [approvalId]: {
                ...(get().approvalResolve[approvalId] ?? initialApprovalResolveState()),
                status: 'awaiting_server',
                attempts,
                lastDecision: decision,
                refusal: null,
                lastAttemptAt,
              },
            },
          });
          armApprovalAckTimer(approvalId);
          return { ok: true };
        }

        const failure = refusal ?? clientRefusal('E_WS_DISCONNECTED');
        const classified = classifyApprovalRefusal(failure);
        set({
          approvalResolve: {
            ...get().approvalResolve,
            [approvalId]: {
              ...(get().approvalResolve[approvalId] ?? initialApprovalResolveState()),
              status: classified.status,
              attempts,
              lastDecision: decision,
              refusal: failure,
              lastAttemptAt,
            },
          },
        });
        return { ok: false, kind: classified.kind, refusal: failure };
      },

      async retryApproval(approvalId: string): Promise<ApprovalResolveOutcome> {
        const approval = get().approvals.find((candidate) => candidate.approvalId === approvalId);
        if (!approval) {
          const refusal = clientRefusal('E_APPROVAL_NOT_FOUND');
          pushRefusal(refusal);
          return { ok: false, kind: 'stale', refusal };
        }
        const resolve = get().approvalResolve[approvalId];
        const canRetry = canRetryApproval({
          record: approval,
          resolve,
          nowMs: Date.now(),
          connection: get().connection,
        });
        if (!canRetry) {
          const refusal = clientRefusal('E_APPROVAL_REQUIRED', {
            message: 'this approval is not currently retryable',
          });
          pushRefusal(refusal);
          return { ok: false, kind: 'operator', refusal };
        }
        return get().resolveApproval(
          approvalId,
          resolve?.lastDecision ?? 'approve',
          'operator retry',
        );
      },

      dismissApproval(approvalId: string): void {
        const resolve = get().approvalResolve[approvalId];
        const approval = get().approvals.find((candidate) => candidate.approvalId === approvalId);
        const locallyExpired =
          approval !== undefined && isApprovalExpired(approval.expiresAt, Date.now());
        if (resolve === undefined) {
          // A client-clock-expired request may have no server resolve record
          // (the server replays expired rows as absent). It must still be
          // dismissable locally, otherwise the card is a dead end.
          if (!locallyExpired) {
            return;
          }
        } else if (
          resolve.status !== 'expired' &&
          resolve.status !== 'stale' &&
          resolve.status !== 'unconfirmed'
        ) {
          return;
        }
        clearApprovalTimer(approvalId);
        const approvalResolve = { ...get().approvalResolve };
        delete approvalResolve[approvalId];
        const approvalSeenEpoch = { ...get().approvalSeenEpoch };
        delete approvalSeenEpoch[approvalId];
        set({
          approvals: get().approvals.filter((candidate) => candidate.approvalId !== approvalId),
          approvalResolve,
          approvalSeenEpoch,
        });
      },

      async pinChat(pinned: boolean): Promise<void> {
        const ok = await run('CHAT_PIN', { pinned });
        if (ok) {
          set({ chat: { ...get().chat, pinned } });
        }
      },

      async insertTemplate(templateId: string): Promise<void> {
        await run('TEMPLATE_INSERT', {
          template_id: templateId,
          project_id: get().project.id,
        });
      },

      async selectHistoryRun(runId: string): Promise<void> {
        if (get().run?.id === runId) {
          return;
        }
        if (get().run !== null && isRunActive(get().run?.status ?? 'queued')) {
          pushRefusal(clientRefusal('E_RUN_ACTIVE', {
            message: 'An active run owns the command surface; finish or cancel it before viewing another run.',
          }));
          return;
        }
        const summary = get().history.runs.find((candidate) => candidate.runId === runId);
        if (summary === undefined) {
          pushRefusal(clientRefusal('E_RUN_NOT_FOUND'));
          return;
        }
        try {
          const list = decodeHistoryRevisions(
            await historyFetch(
              historyRevisionsPath(runId),
              `GET /api/history/runs/${shortId(runId)}/revisions`,
            ),
          );
          let projection: HistoryStateProjection | null = null;
          if (list.latest !== null) {
            projection = decodeHistoryState(
              await historyFetch(
                historyStatePath(runId, list.latest),
                `GET /api/history/runs/${shortId(runId)}/state`,
              ),
            );
            if (projection.revision !== list.latest) {
              throw schemaRefusal(
                'The selected run state revision disagrees with the latest committed revision.',
              );
            }
          }
          let transcript: readonly ChatMessage[] = [];
          let messagesUnavailable = false;
          try {
            transcript = decodeHistoryMessages(
              await historyFetch(
                historyMessagesPath(runId),
                `GET /api/history/runs/${shortId(runId)}/messages`,
              ),
            );
          } catch {
            transcript = [];
            messagesUnavailable = true;
          }
          const incomplete: SnapshotIncomplete[] = [];
          if (projection === null) incomplete.push('no-committed-revision');
          if (messagesUnavailable) incomplete.push('messages-unavailable');
          set({
            run: { id: runId, status: summary.status, errorCode: summary.errorCode },
            tasks: projection?.tasks ?? {},
            messages: transcript,
            selectedTaskId: null,
            streamingRunId: null,
            metrics: null,
            toolActivity: [],
            approvals: [],
            approvalsStale: false,
            approvalResolve: {},
            resolvingApprovals: [],
            approvalReplay: initialApprovalReplayState(),
            approvalSeenEpoch: {},
            resyncGap: null,
            history: {
              ...get().history,
              latest: list.latest ?? -1,
              count: list.revisions.length,
              revisions: list.revisions,
              selected: Math.max(list.latest ?? 0, 0),
              replayGraph: null,
              refreshStatus: 'ok',
              refreshRefusal: null,
            },
            snapshot: {
              status: projection === null || messagesUnavailable ? 'degraded' : 'ready',
              source: 'manual',
              runId,
              revision: projection?.revision ?? null,
              revisionCount: list.revisions.length,
              incomplete,
              refusal: null,
              attempts: get().snapshot.attempts,
              updatedAt: Date.now(),
            },
            graphRev: get().graphRev + 1,
          });
        } catch (error) {
          pushRefusal(toRefusal(error));
        }
      },

      async setHistoryRevision(revision: number): Promise<void> {
        const current = get().run;
        if (!current) {
          pushRefusal(clientRefusal('E_RUN_NOT_FOUND'));
          return;
        }
        if (get().history.revisions.length === 0) {
          // Time travel is only legal against committed revisions: seed the cursor
          // from the server first (revision 0 is one committed state, not "empty").
          await get().refreshRunSnapshot();
        }
        const history = get().history;
        if (!history.revisions.includes(revision)) {
          pushRefusal(
            clientRefusal('E_HISTORY_GAP', {
              message: `Revision ${revision} is not one of the ${history.count} committed revision(s) for run ${shortId(current.id)}.`,
            }),
          );
          return;
        }
        await run('HISTORY_REVISION_SET', { run_id: current.id, revision });
      },

      exitHistoryReplay(): void {
        const history = get().history;
        if (history.replayGraph === null) {
          return;
        }
        // Explicit exit: clear the replay graph so the live graph renders again, and
        // put the cursor at the live edge. No command is sent (nothing to ask for).
        set({
          history: {
            ...history,
            selected: Math.max(history.latest, 0),
            replayGraph: null,
          },
        });
      },

      async refreshRunSnapshot(source: SnapshotSource = 'manual'): Promise<void> {
        await performSnapshotRefresh(source);
      },

      setNav(nav: NavSection): void {
        set({ view: { ...get().view, nav } });
        if (nav === 'data' && get().fileTree.status === 'idle') {
          void get().requestFileTree();
        }
      },

      setTab(tab: TabId): void {
        set({ view: { ...get().view, tab } });
      },

      setWorkspaceTab(tab: TabId): void {
        set({ view: { nav: 'workspace', tab } });
      },

      setViewport(viewport: ViewportState): void {
        set({ viewport });
      },

      selectTask(taskId: string | null): void {
        set({ selectedTaskId: taskId });
      },

      setActiveSessionId(sessionId: string | null): void {
        set({ activeSessionId: sessionId });
      },

      setChatVisible(visible: boolean): void {
        try {
          window.localStorage.setItem('hivemind.chat.visible.v1', visible ? 'true' : 'false');
        } catch {
          // A storage refusal must not make the chat impossible to restore in this session.
        }
        set({ chat: { ...get().chat, visible } });
      },

      openDiffModal(): void {
        set({ diffOpen: true });
      },

      closeDiffModal(): void {
        set({ diffOpen: false });
      },

      dismissRefusal(id: string): void {
        set({ refusals: get().refusals.filter((entry) => entry.id !== id) });
      },

      setReducedMotionOverride(value: boolean): void {
        // Persisted: an operator who needs reduced motion must not re-set it on
        // every launch.
        writeReducedMotionOverride(value);
        set({ reducedMotionOverride: value });
      },

      async probeSettings(): Promise<void> {
        // Exactly one SETTINGS_STATUS per Settings open: the first call flips
        // status to 'pending' synchronously, so a second call (React StrictMode
        // double-effect or a rapid refresh) is a no-op until the response lands.
        if (get().settings.status === 'pending') {
          return;
        }
        set({ settings: { ...get().settings, status: 'pending', refusal: null } });
        const priorRefusalIds = new Set(get().refusals.map((entry) => entry.id));
        const ok = await run('SETTINGS_STATUS', {});
        if (!ok) {
          const refusal =
            get().refusals.find((entry) => !priorRefusalIds.has(entry.id))?.refusal ?? null;
          set({
            settings: {
              ...get().settings,
              status: 'error',
              refusal,
              lastProbe: Date.now(),
            },
          });
        }
      },

      async probeHealth(): Promise<void> {
        try {
          const snapshot = await fetchHealth();
          set({
            settings: {
              ...get().settings,
              health: snapshot.status,
              healthVersion: snapshot.version,
              checks: snapshot.checks,
              lastProbe: Date.now(),
            },
          });
        } catch (error) {
          const refusal =
            error instanceof RefusalError ? error.refusal : clientRefusal('E_WS_DISCONNECTED');
          set({
            settings: { ...get().settings, health: 'unknown', checks: [], lastProbe: Date.now() },
          });
          pushRefusal(refusal);
        }
      },

      async requestFileTree(path?: string): Promise<void> {
        set({ fileTree: { ...get().fileTree, status: 'loading', refusal: null } });
        const ok = await run('FILE_TREE_LIST', path ? { path } : {});
        if (!ok) {
          set({ fileTree: { ...get().fileTree, status: 'error' } });
        }
      },

      async openFile(path: string): Promise<void> {
        const jailRefusal = validateWorkspacePath(path);
        if (jailRefusal) {
          pushRefusal(clientRefusal(jailRefusal));
          return;
        }
        set({
          fileTree: {
            ...get().fileTree,
            contentStatus: 'loading',
            selectedPath: path,
            refusal: null,
          },
        });
        const ok = await run('FILE_READ', { path });
        if (!ok) {
          set({ fileTree: { ...get().fileTree, contentStatus: 'error' } });
        }
      },

      formatSelectedRevision(): string {
        const history = get().history;
        if (history.count === 0) {
          return STRINGS.scrubber.empty;
        }
        const index = history.revisions.indexOf(history.selected);
        // Clamp against the committed revision count, not a max revision index: a
        // sparse revision list (gaps) must not read "state 7 of 4".
        return formatStateOf(index >= 0 ? index : history.count - 1, history.count);
      },
    };
  });
}

export type UiStoreApi = StoreApi<UiState>;
export { deriveHud, isNodeKind, HEALTHY_GREEN };
