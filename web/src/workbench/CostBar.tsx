/**
 * CAP-4 workbench — token usage and cost bar.
 *
 * Cost is rendered only when the host supplies a server-reported `costUsd`.
 * This component never multiplies tokens by a price table: an unstated price is
 * shown as unavailable, not invented. A total token count may be derived from
 * prompt + completion only when the provider omitted `totalTokens`, and that
 * derivation is labelled `derived`.
 */
import type { ReactNode } from 'react';

export type CostStatus = 'idle' | 'loading' | 'ready' | 'error' | 'unavailable' | 'unpriced';
export type UsageSource = 'server' | 'derived' | 'unknown';

export interface Usage {
  readonly promptTokens?: number | null;
  readonly completionTokens?: number | null;
  readonly totalTokens?: number | null;
  readonly cacheHitTokens?: number | null;
  readonly cacheMissTokens?: number | null;
  readonly reasoningTokens?: number | null;
}

export interface CostBarProps {
  readonly usage?: Usage | null;
  readonly costUsd?: number | null;
  readonly budgetUsd?: number | null;
  readonly status?: CostStatus;
  readonly errorMessage?: string | null;
  readonly label?: string;
  readonly onRetry?: () => void;
  readonly className?: string;
}

export const COST_COPY = {
  section: 'Usage & cost',
  loading: 'Loading usage…',
  error: 'Usage unavailable',
  retry: 'Retry',
  empty: 'No usage reported yet.',
  unavailable: 'Usage is incomplete; cost is not claimed.',
  unpriced: 'Tokens measured; cost unpriced (external points endpoint).',
  costNotReported: 'Cost not reported',
  tokensNotReported: 'Tokens not reported',
  total: 'total',
  prompt: 'prompt',
  completion: 'completion',
  cacheHit: 'cache hit',
  cacheMiss: 'cache miss',
  reasoning: 'reasoning',
  budget: 'budget',
  overBudget: 'over budget',
  derived: 'derived',
  server: 'server',
  unknown: 'unknown',
  source: 'source',
} as const;

function isCount(value: number | null | undefined): value is number {
  return typeof value === 'number' && Number.isFinite(value) && value >= 0;
}

/** Locale-independent grouping keeps renders deterministic across test hosts. */
export function formatTokenCount(value: number | null | undefined): string {
  if (!isCount(value)) {
    return '—';
  }
  return Math.trunc(value)
    .toString()
    .replace(/\B(?=(\d{3})+(?!\d))/g, ',');
}

export function formatUsd(value: number | null | undefined): string {
  if (typeof value !== 'number' || !Number.isFinite(value) || value < 0) {
    return '—';
  }
  const digits = value > 0 && value < 0.01 ? 4 : 2;
  return `$${value.toFixed(digits)}`;
}

export interface EffectiveTotal {
  readonly value: number | null;
  readonly source: UsageSource;
}

/**
 * Prefer the provider's total. Only when it is absent do we derive from a
 * complete prompt+completion pair; partial usage stays unknown.
 */
export function effectiveTotalTokens(usage: Usage | null | undefined): EffectiveTotal {
  if (usage === null || usage === undefined) {
    return { value: null, source: 'unknown' };
  }
  if (isCount(usage.totalTokens)) {
    return { value: Math.trunc(usage.totalTokens), source: 'server' };
  }
  if (isCount(usage.promptTokens) && isCount(usage.completionTokens)) {
    return {
      value: Math.trunc(usage.promptTokens + usage.completionTokens),
      source: 'derived',
    };
  }
  return { value: null, source: 'unknown' };
}

export interface BudgetProgress {
  /** Raw ratio, not clamped; values above 1 mean over budget. */
  readonly ratio: number;
  readonly percent: number;
  readonly over: boolean;
}

export function budgetProgress(
  costUsd: number | null | undefined,
  budgetUsd: number | null | undefined,
): BudgetProgress | null {
  if (
    typeof costUsd !== 'number' ||
    !Number.isFinite(costUsd) ||
    costUsd < 0 ||
    typeof budgetUsd !== 'number' ||
    !Number.isFinite(budgetUsd) ||
    budgetUsd <= 0
  ) {
    return null;
  }
  const ratio = costUsd / budgetUsd;
  return { ratio, percent: Math.round(ratio * 100), over: ratio > 1 };
}

export function costStatusLabel(status: CostStatus): string {
  switch (status) {
    case 'idle':
      return COST_COPY.empty;
    case 'loading':
      return COST_COPY.loading;
    case 'error':
      return COST_COPY.error;
    case 'unavailable':
      return COST_COPY.unavailable;
    case 'unpriced':
      return COST_COPY.unpriced;
    case 'ready':
      return COST_COPY.section;
  }
}

export function usageSourceLabel(source: UsageSource | null | undefined): string {
  switch (source) {
    case 'server':
      return COST_COPY.server;
    case 'derived':
      return COST_COPY.derived;
    default:
      return COST_COPY.unknown;
  }
}

function TokenStat({ label, value }: { label: string; value: string }): ReactNode {
  return (
    <span className="inline-flex items-baseline gap-1">
      <span className="type-mono-meta text-meta">{label}</span>
      <span className="type-mono-code text-ink-900">{value}</span>
    </span>
  );
}

export function CostBar({
  usage = null,
  costUsd = null,
  budgetUsd = null,
  status = 'ready',
  errorMessage = null,
  label = COST_COPY.section,
  onRetry,
  className = '',
}: CostBarProps) {
  const total = effectiveTotalTokens(usage);
  const budget = budgetProgress(costUsd, budgetUsd);
  const hasUsage =
    usage !== null &&
    usage !== undefined &&
    (isCount(usage.promptTokens) ||
      isCount(usage.completionTokens) ||
      isCount(usage.totalTokens) ||
      isCount(usage.cacheHitTokens) ||
      isCount(usage.cacheMissTokens) ||
      isCount(usage.reasoningTokens));

  return (
    <section
      aria-label={label}
      aria-busy={status === 'loading'}
      className={`flex flex-wrap items-center gap-x-3 gap-y-1 rounded-[var(--radius-card)] border border-hairline bg-panel px-3 py-1.5 shadow-[var(--shadow-e1)] ${className}`}
    >
      <h2 className="type-label shrink-0 text-ink-900">{label}</h2>

      {status === 'loading' ? (
        <p role="status" className="type-mono-meta text-meta">
          {COST_COPY.loading}
        </p>
      ) : null}

      {status === 'error' ? (
        <div role="alert" className="flex flex-wrap items-center gap-2">
          <span className="type-mono-meta text-signal">
            {errorMessage ?? COST_COPY.error}
          </span>
          {onRetry === undefined ? null : (
            <button
              type="button"
              className="type-mono-meta rounded-[var(--radius-sm)] border border-signal px-2 py-0.5 text-signal"
              onClick={() => onRetry()}
            >
              {COST_COPY.retry}
            </button>
          )}
        </div>
      ) : null}

      {status === 'unavailable' ? (
        <p role="status" className="type-mono-meta text-amber">
          {COST_COPY.unavailable}
        </p>
      ) : null}

      {status === 'unpriced' ? (
        <p role="status" className="type-mono-meta text-amber">
          {COST_COPY.unpriced}
        </p>
      ) : null}

      {status === 'idle' ? (
        <p role="status" className="type-mono-meta text-meta">
          {COST_COPY.empty}
        </p>
      ) : null}

      {status === 'ready' && !hasUsage ? (
        <p role="status" className="type-mono-meta text-meta">
          {COST_COPY.tokensNotReported}
        </p>
      ) : null}

      {status === 'ready' && hasUsage ? (
        <div className="flex flex-wrap items-center gap-x-3 gap-y-1">
          <TokenStat label={COST_COPY.total} value={formatTokenCount(total.value)} />
          <TokenStat label={COST_COPY.prompt} value={formatTokenCount(usage?.promptTokens)} />
          <TokenStat label={COST_COPY.completion} value={formatTokenCount(usage?.completionTokens)} />
          {isCount(usage?.cacheHitTokens) ? (
            <TokenStat label={COST_COPY.cacheHit} value={formatTokenCount(usage?.cacheHitTokens)} />
          ) : null}
          {isCount(usage?.cacheMissTokens) ? (
            <TokenStat label={COST_COPY.cacheMiss} value={formatTokenCount(usage?.cacheMissTokens)} />
          ) : null}
          {isCount(usage?.reasoningTokens) ? (
            <TokenStat label={COST_COPY.reasoning} value={formatTokenCount(usage?.reasoningTokens)} />
          ) : null}
          {total.source === 'derived' ? (
            <span className="type-mono-meta rounded-[var(--radius-pill)] border border-amber bg-panel-alt px-1.5 py-0.5 text-ink-800">
              {COST_COPY.derived}
            </span>
          ) : null}
        </div>
      ) : null}

      {status === 'ready' ? (
        <>
          <span className="hidden h-4 w-px bg-hairline sm:block" aria-hidden="true" />
          <div className="flex flex-wrap items-center gap-2">
            <span className="type-mono-code text-ink-900">
              {costUsd === null || costUsd === undefined
                ? COST_COPY.costNotReported
                : formatUsd(costUsd)}
            </span>
            {costUsd === null || costUsd === undefined ? null : (
              <span className="type-mono-meta rounded-[var(--radius-pill)] border border-hairline px-1.5 py-0.5 text-meta">
                {COST_COPY.server}
              </span>
            )}
            {budget === null ? null : (
              <>
                <div
                  role="progressbar"
                  aria-label={`${COST_COPY.budget} ${formatUsd(costUsd)} / ${formatUsd(budgetUsd)}`}
                  aria-valuemin={0}
                  aria-valuemax={100}
                  aria-valuenow={Math.min(100, Math.max(0, budget.percent))}
                  aria-valuetext={`${budget.percent}%`}
                  className="h-1.5 w-24 overflow-hidden rounded-[var(--radius-pill)] bg-recessed"
                >
                  <div
                    className={`h-full rounded-[var(--radius-pill)] ${budget.over ? 'bg-signal' : 'bg-honey-500'}`}
                    style={{ width: `${Math.min(100, Math.max(0, budget.percent))}%` }}
                  />
                </div>
                <span className={`type-mono-meta ${budget.over ? 'text-signal' : 'text-meta'}`}>
                  {formatUsd(costUsd)} / {formatUsd(budgetUsd)}
                  {budget.over ? ` · ${COST_COPY.overBudget}` : ''}
                </span>
              </>
            )}
          </div>
        </>
      ) : null}
    </section>
  );
}
