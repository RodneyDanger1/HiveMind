/**
 * Node cards (S15-S19), matching the HiveRouter spatial style system:
 * SuperAgent 420x150 r14 with a honey header, Worker 320x96 r12 with a state
 * dot, Tool 420x240 r14 with a dark result box, Broken route 320x110 crimson.
 * Metrics the wire contract does not deliver render "—", never a fabricated 0.
 */
import { Handle, Position } from '@xyflow/react';
import type { CSSProperties, ReactNode } from 'react';
import { STRINGS, formatCount, formatCtx, formatPercent } from '../state/strings';
import { NODE_KIND_LABELS, visualFor, type AgentState } from '../state/agentState';
import type { CanvasNodeData } from './graph';

type HiveNodeProps = { readonly data: CanvasNodeData };

function tokenColor(token: string): string {
  return token.startsWith('honey-') || token.startsWith('ink-') || token === 'verify' || token === 'signal' || token === 'amber' || token === 'hairline'
    ? `var(--color-${token})`
    : 'var(--color-ink-500)';
}

function stateTone(state: AgentState): string {
  switch (state) {
    case 'healthy':
      return 'var(--color-honey-400)';
    case 'running':
      return 'var(--color-honey-600)';
    case 'failed':
    case 'killed':
    case 'interrupted':
      return 'var(--color-signal)';
    case 'queued':
      return 'var(--color-hairline)';
    case 'completed':
      return 'var(--color-verify)';
    default:
      return 'var(--color-ink-500)';
  }
}

/** A state marker with no text label, for 40px chips and card headers. */
export function StateDot({ state, size = 8 }: { state: AgentState; size?: number }) {
  const active = state === 'running' || state === 'thinking' || state === 'streaming';
  return (
    <svg
      width={size}
      height={size}
      viewBox="0 0 12 12"
      aria-hidden="true"
      className={active ? 'state-pulse' : undefined}
      style={{ color: stateTone(state) }}
    >
      <circle cx="6" cy="6" r="4" fill="currentColor" />
    </svg>
  );
}

export function StateGlyph({ state, size = 10 }: { state: AgentState; size?: number }) {
  const visual = visualFor(state);
  const color = visual.dot === null ? tokenColor(visual.toneToken) : stateTone(state);
  const shape = visual.dot ?? visual.shape;
  return (
    <span className="inline-flex items-center gap-1" title={visual.label}>
      <svg width={size} height={size} viewBox="0 0 12 12" aria-hidden="true" style={{ color }}>
        {shape === 'dot' || shape === 'ring' ? (
          <circle cx="6" cy="6" r="4" fill={visual.dot ? 'currentColor' : 'none'} stroke="currentColor" strokeWidth={visual.dot ? 0 : 1.75} />
        ) : null}
        {shape === 'diamond' ? <polygon points="6,1 11,6 6,11 1,6" fill="currentColor" /> : null}
        {shape === 'square' ? <rect x="2" y="2" width="8" height="8" fill="currentColor" /> : null}
        {shape === 'triangle' ? <polygon points="6,1 11,10.5 1,10.5" fill="currentColor" /> : null}
        {shape === 'hex' ? <polygon points="6,1 10.5,3.5 10.5,8.5 6,11 1.5,8.5 1.5,3.5" fill="currentColor" /> : null}
      </svg>
      <span className="type-mono-meta" style={{ color }}>{visual.label}</span>
    </span>
  );
}

function Frame(props: {
  readonly data: CanvasNodeData;
  readonly children: ReactNode;
  readonly error?: boolean;
  readonly gold?: boolean;
  readonly radius?: number;
}) {
  const selected = props.data.selected;
  const live =
    props.data.task.state === 'running' ||
    props.data.task.state === 'thinking' ||
    props.data.task.state === 'streaming';
  // On a left branch the child sits left of its parent: attach the target to
  // the right edge and the source to the left edge so the parent->child edge
  // stays outside the cards instead of crossing the parent.
  const leftBranch = props.data.branchSide === 'left';
  const targetPosition = leftBranch ? Position.Right : Position.Left;
  const sourcePosition = leftBranch ? Position.Left : Position.Right;
  const style: CSSProperties = {
    borderColor: selected
      ? 'var(--color-ink-700)'
      : props.error
        ? 'var(--color-signal)'
        : props.gold
          ? 'var(--color-border-gold)'
          : 'var(--color-hairline)',
    boxShadow: selected ? 'var(--shadow-e2)' : 'var(--shadow-e1)',
    borderRadius: props.radius ?? 12,
  };
  return (
    <div
      className={
        'hive-node relative flex h-full w-full flex-col overflow-hidden border-[1.5px] text-left ' +
        (props.error ? 'bg-[var(--color-error-surface)]' : 'bg-panel') +
        (live ? ' is-live' : '')
      }
      style={style}
      data-node-kind={props.data.task.kind}
      data-live={live ? 'true' : undefined}
    >
      <Handle type="target" position={targetPosition} className="!h-2 !w-2 !border-0 !bg-honey-500" />
      <span
        className={'hive-node-seam' + (props.error ? ' hive-node-seam-error' : '')}
        aria-hidden="true"
      />
      <span className="hive-node-socket" data-side={leftBranch ? 'right' : 'left'} aria-hidden="true" />
      {props.children}
      {live ? <span className="hive-node-live-rail" aria-hidden="true" /> : null}
      <Handle type="source" position={sourcePosition} className="!h-2 !w-2 !border-0 !bg-honey-500" />
    </div>
  );
}

/** Server task role, when the snapshot carries one; never invented. */
function roleOf(data: CanvasNodeData): string | null {
  const role = data.task.role?.trim();
  return role !== undefined && role.length > 0 ? role : null;
}

function RoleChip({ role }: { role: string }) {
  return (
    <span className="hive-role-chip" title={`Role: ${role}`}>
      {role}
    </span>
  );
}

function Progress({
  value,
  height = 5,
  tone = 'super',
}: {
  value: number | null;
  height?: number;
  tone?: 'super' | 'worker';
}) {
  return (
    <div
      className="w-full overflow-hidden rounded-[var(--radius-pill)] bg-hairline"
      style={{ height }}
    >
      {value === null ? null : (
        <div
          className={
            'motion-progress-sheen h-full rounded-[var(--radius-pill)] ' +
            (tone === 'worker'
              ? 'bg-honey-400'
              : 'bg-gradient-to-r from-honey-500 to-honey-300')
          }
          style={{ width: `${Math.max(0, Math.min(100, value))}%` }}
        />
      )}
    </div>
  );
}

export function SuperAgentNode({ data }: HiveNodeProps) {
  const role = roleOf(data);
  return (
    <Frame data={data} radius={14} gold>
      <div className="flex h-10 items-center justify-between gap-2 bg-honey-100 pl-8 pr-4">
        <button
          type="button"
          className="nodrag min-w-0 truncate text-left text-[14.5px] font-semibold leading-[18.5px] text-ink-900"
          onClick={() => data.onSelect(data.task.id)}
          title={data.task.label}
          aria-label={`${data.task.label} — ${NODE_KIND_LABELS.superagent}, ${visualFor(data.task.state).label}`}
          aria-pressed={data.selected}
        >
          {data.task.label}
        </button>
        <StateDot state={data.task.state} size={7} />
        <span className="flex h-5 w-[68px] shrink-0 items-center justify-center rounded-[var(--radius-pill)] bg-honey-300">
          <span className="type-mono-meta text-[11px] text-ink-800">{visualFor(data.task.state).label}</span>
        </span>
        <button
          type="button"
          className="nodrag grid h-[20px] w-[20px] shrink-0 place-items-center rounded-full border border-honey-500 bg-panel text-[14px] leading-none text-ink-800 hover:bg-honey-100"
          title="Spawn an Agent under this SuperAgent"
          aria-label={`Spawn an Agent under ${data.task.label}`}
          onClick={(event) => {
            event.stopPropagation();
            data.onSpawn(data.task.id);
          }}
        >
          +
        </button>
      </div>
      <button type="button" className="nodrag flex min-h-0 flex-1 flex-col px-4 py-2 text-left" onClick={() => data.onSelect(data.task.id)}>
        <span className="flex min-w-0 items-center gap-1.5">
          <span className="type-mono-meta shrink-0 tracking-[0.12em] text-meta">{NODE_KIND_LABELS.superagent}</span>
          {role === null ? null : <RoleChip role={role} />}
        </span>
        <span className="mt-0.5 line-clamp-1 text-[11.5px] leading-[16px] text-ink-700">
          {STRINGS.node.roleCaption}
        </span>
        <div className="mt-auto">
          <Progress value={data.task.progress} tone="super" />
          <div className="type-mono-meta mt-1 flex justify-between text-meta">
            <span>{STRINGS.node.workersMetric}</span>
            <span>{data.task.progress === null ? STRINGS.node.noMetrics : formatPercent(data.task.progress, 0)}</span>
          </div>
        </div>
      </button>
    </Frame>
  );
}

export function WorkerAgentNode({ data }: HiveNodeProps) {
  const role = roleOf(data);
  return (
    <Frame data={data} radius={12}>
      <button type="button" className="nodrag flex min-h-0 flex-1 flex-col px-4 py-3 text-left" onClick={() => data.onSelect(data.task.id)}>
        <div className="flex items-center gap-2">
          <StateDot state={data.task.state} size={9} />
          <span className="min-w-0 flex-1 truncate text-[13px] font-medium leading-[18px] text-ink-900" title={data.task.label}>{data.task.label}</span>
          <span className="type-mono-meta text-meta">
            {data.task.progress === null ? STRINGS.node.noMetrics : formatPercent(data.task.progress, 0)}
          </span>
          <button
            type="button"
            className="nodrag grid h-5 w-5 shrink-0 place-items-center rounded-full border border-hairline bg-panel text-[12px] leading-none text-ink-800 hover:bg-honey-100"
            title="Spawn a temporary Subagent under this Agent"
            aria-label={`Spawn a temporary Subagent under ${data.task.label}`}
            onClick={(event) => {
              event.stopPropagation();
              data.onSpawn(data.task.id);
            }}
          >
            +
          </button>
        </div>
        <span className="mt-1 flex min-w-0 items-center justify-between gap-2">
          <span className="type-mono-meta truncate text-meta">{STRINGS.node.workerLabel}</span>
          {role === null ? null : <RoleChip role={role} />}
        </span>
        <div className="mt-auto">
          <Progress value={data.task.progress} height={3} tone="worker" />
          <span className="type-mono-meta mt-1 block text-meta">{formatCtx(null)}</span>
        </div>
      </button>
    </Frame>
  );
}

export function SubagentNode({ data }: HiveNodeProps) {
  const role = roleOf(data);
  return (
    <Frame data={data} radius={12}>
      <button
        type="button"
        className="nodrag flex min-h-0 flex-1 flex-col px-4 py-3 text-left"
        onClick={() => data.onSelect(data.task.id)}
        title={data.task.label}
        aria-label={`${data.task.label} — Subagent, ${visualFor(data.task.state).label}`}
        aria-pressed={data.selected}
      >
        <div className="flex items-center gap-2">
          <StateDot state={data.task.state} size={9} />
          <span className="min-w-0 flex-1 truncate text-[12.5px] font-medium leading-[17px] text-ink-900">
            {data.task.label}
          </span>
        </div>
        <span className="mt-1 flex min-w-0 items-center justify-between gap-2">
          <span className="type-mono-meta truncate text-meta">SUBAGENT · bounded worker</span>
          {role === null ? null : <RoleChip role={role} />}
        </span>
        <div className="mt-auto">
          <Progress value={data.task.progress} height={3} tone="worker" />
        </div>
      </button>
    </Frame>
  );
}

export function ToolAgentNode({ data }: HiveNodeProps) {
  const live =
    data.task.state === 'running' ||
    data.task.state === 'thinking' ||
    data.task.state === 'streaming';
  return (
    <Frame data={data} radius={14}>
      <button type="button" className="nodrag flex min-h-0 flex-1 flex-col px-4 py-3 text-left" onClick={() => data.onSelect(data.task.id)}>
        <div className="flex items-center justify-between gap-2">
          <span className="flex min-w-0 items-center gap-2">
            <StateDot state={data.task.state} size={8} />
            <span className="truncate text-[13.5px] font-semibold text-ink-900" title={data.task.label}>{data.task.label}</span>
          </span>
          <span className="type-mono-meta rounded-[var(--radius-pill)] bg-recessed px-2 py-0.5 text-ink-700">
            {NODE_KIND_LABELS.tool}
          </span>
        </div>
        <div className="hive-tool-terminal mt-2 h-[132px] shrink-0 overflow-hidden rounded-[8px] bg-ink-800 px-3 py-2">
          <div className="flex gap-1.5" aria-hidden="true">
            <span className="h-2 w-2 rounded-full bg-signal" />
            <span className="h-2 w-2 rounded-full bg-honey-400" />
            <span className="h-2 w-2 rounded-full bg-verify" />
          </div>
          <span className="type-mono-meta mt-3 block text-honey-100/80">{STRINGS.node.noToolOutput}</span>
          {live ? <span className="hive-tool-scan" aria-hidden="true" /> : null}
        </div>
        <div className="mt-2">
          <Progress value={data.task.progress} height={6} />
          <div className="type-mono-meta mt-1 flex justify-between text-meta">
            <span>{STRINGS.node.score}</span>
            <span>{STRINGS.node.noMetrics}</span>
          </div>
        </div>
      </button>
    </Frame>
  );
}

export function BrokenRouteNode({ data }: HiveNodeProps) {
  return (
    <Frame data={data} error radius={12}>
      <button type="button" className="nodrag flex min-h-0 flex-1 flex-col px-4 py-3 text-left" onClick={() => data.onSelect(data.task.id)}>
        <div className="flex items-center gap-2 text-signal">
          <span aria-hidden="true">▲</span>
          <span className="truncate text-[13px] font-semibold" title={data.task.label}>{data.task.label}</span>
        </div>
        <span className="type-mono-meta mt-1 text-meta">{data.task.errorCode ?? STRINGS.node.errorState}</span>
        <span className="type-mono-meta mt-2 text-ink-700">{STRINGS.node.brokenRoute}</span>
        <div className="mt-auto type-mono-meta flex justify-between text-meta">
          <span>{STRINGS.node.attempt}</span>
          <span>{formatCount(null)}</span>
        </div>
      </button>
    </Frame>
  );
}

export function HyperAgentNode({ data }: HiveNodeProps) {
  const live =
    data.task.state === 'running' ||
    data.task.state === 'thinking' ||
    data.task.state === 'streaming';
  return (
    <div
      className={'relative flex h-full w-full items-center justify-center' + (live ? ' is-live' : '')}
      data-node-kind="hyperagent"
    >
      <Handle type="target" position={Position.Left} className="!h-2 !w-2 !border-0 !bg-honey-500" />
      <div
        aria-hidden="true"
        className="absolute inset-3 border-[1.5px] border-honey-400/70 bg-honey-100/20"
        style={{ clipPath: 'polygon(25% 6.7%, 75% 6.7%, 100% 50%, 75% 93.3%, 25% 93.3%, 0 50%)' }}
      />
      <button
        type="button"
        className="nodrag relative z-10 rounded-[var(--radius-pill)] border border-honey-500 bg-panel/90 px-3 py-1 text-center"
        onClick={() => data.onSelect(data.task.id)}
        title={data.task.label}
        aria-label={`${data.task.label} — ${NODE_KIND_LABELS.hyperagent}, ${visualFor(data.task.state).label}`}
        aria-pressed={data.selected}
      >
        <span className="type-mono-meta block text-meta">{NODE_KIND_LABELS.hyperagent}</span>
        <span className="block text-[14.5px] font-semibold leading-[18.5px] text-ink-900">{data.task.label}</span>
      </button>
      <Handle type="source" position={Position.Right} className="!h-2 !w-2 !border-0 !bg-honey-500" />
    </div>
  );
}

export function HiveAnchorNode() {
  return (
    <div className="relative h-px w-px" aria-hidden="true">
      <Handle type="source" position={Position.Bottom} className="!h-px !w-px !border-0 !bg-transparent" />
    </div>
  );
}

export const NODE_RENDERERS = {
  hyperagent: HyperAgentNode,
  superagent: SuperAgentNode,
  subagent: SubagentNode,
  worker: WorkerAgentNode,
  tool: ToolAgentNode,
  error: BrokenRouteNode,
  hiveanchor: HiveAnchorNode,
} as const;
