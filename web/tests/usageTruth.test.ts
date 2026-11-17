/**
 * Usage/cost display truth pins (TRUTHSEEKER seat).
 *
 * The live usage route `/api/usage/runs/{id}` reports provider token counters;
 * it reports no cost. These pins keep the display honest about that:
 *   - unknown token/cost values stay unknown (`—` / "Cost not reported"),
 *   - a measured zero stays distinguishable from an unknown,
 *   - a rendered money figure carries its provenance,
 *   - an unpriced (points) provider is not mislabelled "usage incomplete".
 *
 * `RED:` tests were the failing proofs when this audit wrote them. The routed
 * CostBar fix (distinct `unpriced` status/copy + a provenance badge on a numeric
 * cost) landed in `web/src/workbench/CostBar.tsx`, so the whole file is GREEN
 * now; the labels are kept for lineage. Re-run for the current state.
 */
import { describe, expect, it } from 'vitest';
import { createElement } from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import {
  COST_COPY,
  CostBar,
  budgetProgress,
  costStatusLabel,
  effectiveTotalTokens,
  formatTokenCount,
  formatUsd,
  type CostStatus,
} from '../src/workbench';

const usage = { promptTokens: 100, completionTokens: 20, totalTokens: 120 } as const;

describe('usage/cost truth pins', () => {
  it('keeps an unknown token count distinct from a measured zero', () => {
    expect(formatTokenCount(null)).toBe('—');
    expect(formatTokenCount(0)).toBe('0');
    // Partial usage never derives a total: one missing side must stay unknown.
    expect(
      effectiveTotalTokens({ promptTokens: 10, completionTokens: null, totalTokens: null }),
    ).toEqual({ value: null, source: 'unknown' });
    expect(effectiveTotalTokens({ totalTokens: 0 })).toEqual({ value: 0, source: 'server' });
  });

  it('renders an unreported cost as unknown, never as $0.00', () => {
    const markup = renderToStaticMarkup(createElement(CostBar, { usage, costUsd: null }));
    expect(markup).toContain(COST_COPY.costNotReported);
    expect(markup).not.toContain('$0.00');
    // Formatting a measured zero is fine; an absent value is not a zero.
    expect(formatUsd(0)).toBe('$0.00');
    expect(formatUsd(null)).toBe('—');
  });

  it('does not show a stale numeric cost when usage is unavailable', () => {
    const markup = renderToStaticMarkup(
      createElement(CostBar, { usage: null, costUsd: 0, status: 'unavailable' }),
    );
    expect(markup).toContain(COST_COPY.unavailable);
    expect(markup).not.toContain('$0.00');
    expect(budgetProgress(null, 10)).toBeNull();
  });

  it('RED: an unpriced provider has a distinct status and copy', () => {
    // An external OpenAI-compatible endpoint measures tokens but bills in
    // points: usage is COMPLETE, cost is UNPRICED. Collapsing that into
    // "Usage is incomplete" (the only non-ready copy today) mislabels measured
    // usage as missing.
    const copy = COST_COPY as unknown as Record<string, string>;
    expect(copy.unpriced, 'COST_COPY must carry an `unpriced` label').toBeTruthy();
    expect(
      costStatusLabel('unpriced' as unknown as CostStatus),
      "costStatusLabel('unpriced') must return a label, not undefined",
    ).toBeTruthy();
  });

  it('RED: a rendered money figure carries its provenance', () => {
    // V5: every stored/reported number carries provenance. Tokens get a
    // `derived` badge; the cost number gets none, so a client cannot tell a
    // server-measured USD amount from a derived/assumed one.
    const markup = renderToStaticMarkup(createElement(CostBar, { usage, costUsd: 0.02 }));
    expect(markup).toContain('$0.02');
    expect(
      markup,
      'the cost figure must be labelled with its provenance (measured/derived/unpriced)',
    ).toMatch(/measured|derived|unpriced|server/i);
  });
});
