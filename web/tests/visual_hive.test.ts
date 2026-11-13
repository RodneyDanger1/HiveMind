/**
 * Visual finish pins for the hive field, bee routes, node cards and the
 * Help/Guide surfaces. These assert structure (not pixels) so a refactor cannot
 * silently drop the comb texture, the live rail or the node glyphs.
 */
import { describe, expect, it } from 'vitest';
import hiveFieldSource from '../src/flow/HiveField.tsx?raw';
import beeEdgeSource from '../src/flow/BeeEdge.tsx?raw';
import nodesSource from '../src/flow/nodes.tsx?raw';
import helpSource from '../src/panels/HelpPanel.tsx?raw';
import guideSource from '../src/panels/GuideOverlay.tsx?raw';

describe('visual finish: hive body', () => {
  it('stamps a wax-cell texture and a paper vignette over the comb field', () => {
    expect(hiveFieldSource).toContain('id="hivefield-comb-texture"');
    expect(hiveFieldSource).toContain('url(#hivefield-comb-texture)');
    expect(hiveFieldSource).toContain('id="hivefield-vignette"');
    expect(hiveFieldSource).toContain('url(#hivefield-vignette)');
  });

  it('builds the entrance from a lit tunnel, a lip, a board and three wax cells', () => {
    expect(hiveFieldSource).toContain('hivefield-tunnel');
    expect(hiveFieldSource).toContain('hivefield-board');
    expect(hiveFieldSource).toContain('hive-entrance');
    const cells = hiveFieldSource.match(/<polygon points="(?:292|310|328),609/g) ?? [];
    expect(cells).toHaveLength(3);
    // The entrance glow stays the single animated element inside the symbol.
    expect(hiveFieldSource).toContain('hive-entrance-glow');
  });
});

describe('visual finish: bee routes', () => {
  it('rides a lit trail and marks the receiving end with a pollen ring', () => {
    expect(beeEdgeSource).toContain('bee-edge-glow');
    expect(beeEdgeSource).toContain('bee-edge-landing');
    expect(beeEdgeSource).toContain('props.targetX');
  });

  it('draws a striped bee with fluttering wing groups', () => {
    expect(beeEdgeSource).toContain('bee-wings');
    expect(beeEdgeSource).toContain('bee-wing-far');
    expect(beeEdgeSource).toMatch(/stroke="var\(--color-ink-800\)"/);
    // Wings sit behind the body and rotate with the route, not against it.
    expect(beeEdgeSource).toMatch(/<g className="bee-wings">[\s\S]*strokeWidth="0\.55"[\s\S]*<\/g>/);
  });
});

describe('visual finish: node cards', () => {
  it('adds a honey seam, a parent-side socket and a live rail to the frame', () => {
    expect(nodesSource).toContain('hive-node-seam');
    expect(nodesSource).toContain('hive-node-socket');
    expect(nodesSource).toContain("data-side={leftBranch ? 'right' : 'left'}");
    expect(nodesSource).toContain('hive-node-live-rail');
    expect(nodesSource).toContain('data-live={live ? ');
  });

  it('only marks running states live and shows a server role when present', () => {
    expect(nodesSource).toMatch(/state === 'running'/);
    expect(nodesSource).toMatch(/state === 'thinking'/);
    expect(nodesSource).toMatch(/state === 'streaming'/);
    expect(nodesSource).toContain('hive-role-chip');
    expect(nodesSource).toContain('roleOf(');
    // Truncated labels keep their full text reachable.
    expect(nodesSource.match(/title=\{data\.task\.label\}/g)?.length ?? 0).toBeGreaterThanOrEqual(4);
  });

  it('adds a scan pass only to a live tool terminal', () => {
    expect(nodesSource).toContain('hive-tool-terminal');
    expect(nodesSource).toContain('hive-tool-scan');
  });
});

describe('visual finish: help and guide', () => {
  it('numbers the help cards and rules them with a comb divider', () => {
    expect(helpSource).toContain('help-card-index');
    expect(helpSource).toContain('help-feature-index');
    expect(helpSource).toContain('help-divider');
    expect(helpSource).toContain('padStart(2, \'0\')');
  });

  it('adds a per-kind shape glyph and a comb rule to the guide', () => {
    expect(guideSource).toContain('GuideGlyph');
    expect(guideSource).toContain('guide-swatch-glyph');
    expect(guideSource).toContain('guide-divider');
    for (const kind of ['HYPER', 'SUPER', 'WORKER', 'SUB', 'TOOL', 'BEE ROUTE']) {
      expect(guideSource).toContain(`name === '${kind}'`);
    }
  });
});

describe('visual finish: route and card hooks', () => {
  it('keeps the new tab-strip and stillness hooks out of inline styles', () => {
    // The tab seam and the stillness overrides live in the stylesheet, so no
    // component may re-introduce a hard-coded animation for them.
    expect(nodesSource).not.toMatch(/animation:\s*['"`]/);
    expect(beeEdgeSource).toMatch(/className="motion-bee-ride"/);
    expect(beeEdgeSource).toMatch(/className="bee-bob"/);
  });
});
