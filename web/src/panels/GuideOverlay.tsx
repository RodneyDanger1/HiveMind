/**
 * Plain-language guide for the spatial canvas. Opened from the header `?`
 * control. It explains the nodes, the policies, root control, remote/daemon
 * truth, and the first-run path without adding another nav destination.
 */
import { useCallback, useEffect, useRef, useState } from 'react';
import { prefersReducedMotion } from '../a11y/reducedMotion';

const NODES = [
  { name: 'HYPER', tone: 'bg-honey-300', text: 'The planner. It creates the plan, opens two bounded control turns, reviews results, and writes the final answer.' },
  { name: 'SUPER', tone: 'bg-honey-200', text: 'A branch lead. It splits a large goal across workers and combines their results.' },
  { name: 'WORKER', tone: 'bg-panel-alt', text: 'An executor. It edits files, runs approved commands, runs tests, and reports back.' },
  { name: 'SUB', tone: 'bg-panel', text: 'A temporary helper under one worker. Read-only, no spawning, no shell, no writes.' },
  { name: 'TOOL', tone: 'bg-panel-alt', text: 'A tool-call record: what the agent tried, its outcome, and any refusal code.' },
  { name: 'BEE ROUTE', tone: 'bg-honey-100', text: 'A delegation edge. Click a node to inspect it; drag a worker onto another agent to reparent it while a run is active.' },
];

const POLICIES = [
  ['classic', 'Ask before writes and shell. Good default.'],
  ['plan', 'Read and plan only. No workspace changes.'],
  ['standard', 'Workspace writes are allowed; risky actions still ask.'],
  ['trusted', 'Full local automation. Only use on a workspace you trust.'],
];

const STEPS = [
  'Click New session name, pick a policy, and press Create — or use the Default session.',
  'Type a prompt in the composer and press Enter. The Hive AI Assistant sends it to the local server.',
  'Watch the graph appear: Hyper plans, then Super, Worker, and Sub nodes execute.',
  'Approvals appear as a modal. Read the preview, then approve or deny.',
  'Open Agents for todos, jobs, memory, and the roster; Data for runs; History to replay.',
];

/** Small shape grammar per node kind, drawn inside the honeycomb swatch. */
function GuideGlyph({ name }: { readonly name: string }) {
  const stroke = { fill: 'none', stroke: 'currentColor', strokeWidth: 1.3, strokeLinecap: 'round' as const, strokeLinejoin: 'round' as const };
  return (
    <svg className="guide-swatch-glyph" width="10" height="10" viewBox="0 0 12 12" aria-hidden="true" focusable="false">
      {name === 'HYPER' ? (
        <>
          <polygon points="6,1 10.5,3.5 10.5,8.5 6,11 1.5,8.5 1.5,3.5" {...stroke} />
          <polygon points="6,4 8,5.1 8,7.4 6,8.5 4,7.4 4,5.1" {...stroke} strokeWidth={1} />
        </>
      ) : null}
      {name === 'SUPER' ? (
        <>
          <polygon points="6,1.2 10.2,3.6 10.2,8.4 6,10.8 1.8,8.4 1.8,3.6" {...stroke} />
          <path d="M6 5.4v3" {...stroke} strokeWidth={1.2} />
        </>
      ) : null}
      {name === 'WORKER' ? <circle cx="6" cy="6" r="2.6" fill="currentColor" /> : null}
      {name === 'SUB' ? <circle cx="6" cy="6" r="3.4" {...stroke} /> : null}
      {name === 'TOOL' ? <path d="M3.2 3.4 6.6 6l-3.4 2.6M7.4 9h2.4" {...stroke} /> : null}
      {name === 'BEE ROUTE' ? (
        <>
          <ellipse cx="6" cy="6.6" rx="3.2" ry="2" fill="currentColor" />
          <path d="M6.6 4.8v3.6M4.6 4.9v3.4" stroke="var(--color-honey-200)" strokeWidth="0.9" />
          <path d="M4.4 4.4C3.4 3 2.4 2.8 1.7 3.3M7.2 4.3c1-1.3 2-1.5 2.7-1" {...stroke} strokeWidth={0.9} />
        </>
      ) : null}
    </svg>
  );
}

/** True when either the OS preference or the in-app switch asks for stillness. */
function motionOff(): boolean {
  return (
    prefersReducedMotion() ||
    (typeof document !== 'undefined' && document.documentElement.dataset.reducedMotion === 'true')
  );
}

export function GuideOverlay({ open, onClose }: { open: boolean; onClose: () => void }) {
  const [closing, setClosing] = useState(false);
  const closeTimer = useRef<number | null>(null);

  const requestClose = useCallback(() => {
    if (closing) return;
    if (motionOff() || typeof window === 'undefined') {
      onClose();
      return;
    }
    setClosing(true);
    closeTimer.current = window.setTimeout(() => {
      closeTimer.current = null;
      onClose();
    }, 150);
  }, [closing, onClose]);

  useEffect(() => {
    if (!open) return;
    const onKey = (event: KeyboardEvent): void => {
      if (event.key === 'Escape') requestClose();
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [open, requestClose]);

  useEffect(() => {
    if (!open) return;
    setClosing(false);
    if (closeTimer.current !== null) {
      window.clearTimeout(closeTimer.current);
      closeTimer.current = null;
    }
  }, [open]);

  useEffect(() => () => {
    if (closeTimer.current !== null) {
      window.clearTimeout(closeTimer.current);
    }
  }, []);

  if (!open) return null;

  return (
    <div
      className={`guide-veil fixed inset-0 z-[70] flex items-center justify-center p-4${closing ? ' is-closing' : ''}`}
      role="presentation"
      onMouseDown={requestClose}
    >
      <div className="guide-ambient" aria-hidden="true">
        <svg className="guide-comb" width="100%" height="100%" focusable="false">
          <defs>
            <pattern id="guide-comb-pattern" width="46" height="80" patternUnits="userSpaceOnUse">
              <path
                d="M11.5 0h23L46 20 34.5 40h-23L0 20Z"
                fill="none"
                stroke="currentColor"
                strokeWidth="1"
              />
              <path
                d="M-11.5 40h23L23 60 11.5 80h-23L-23 60Z"
                fill="none"
                stroke="currentColor"
                strokeWidth="1"
              />
            </pattern>
          </defs>
          <rect width="100%" height="100%" fill="url(#guide-comb-pattern)" />
        </svg>
      </div>

      <section
        role="dialog"
        aria-modal="true"
        aria-labelledby="hivemind-guide-title"
        className="surface-panel guide-panel max-h-[min(760px,90vh)] w-[min(760px,96vw)] overflow-y-auto p-5"
        onMouseDown={(event) => event.stopPropagation()}
      >
        <div className="flex items-start justify-between gap-4">
          <div className="min-w-0">
            <div className="hm-eyebrow flex text-meta">
              <span className="guide-eyebrow-hex" aria-hidden="true" />
              QUICK GUIDE
            </div>
            <h2 id="hivemind-guide-title" className="hm-display-title mt-1">
              What am I looking at?
            </h2>
            <p className="mt-2 max-w-[58ch] text-[13px] leading-5 text-ink-500">
              HiveMind is a local multi-agent workspace. A HyperAgent plans; SuperAgents split the
              work; Workers execute; Subagents help read-only. Every side effect is approved or
              refused by name, and every run is recorded in the local database.
            </p>
          </div>
          <button
            type="button"
            onClick={requestClose}
            aria-label="Close guide"
            className="guide-close flex h-9 w-9 shrink-0 items-center justify-center rounded-[var(--radius-pill)] border border-hairline bg-panel text-ink-700"
          >
            <svg width="14" height="14" viewBox="0 0 16 16" aria-hidden="true">
              <path
                d="m4 4 8 8M12 4l-8 8"
                fill="none"
                stroke="currentColor"
                strokeWidth="1.6"
                strokeLinecap="round"
              />
            </svg>
          </button>
        </div>

        <section className="guide-section">
          <h3 className="hm-section-title">The nodes</h3>
          <div className="mt-2.5 grid gap-2 sm:grid-cols-2">
            {NODES.map((node) => (
              <div key={node.name} className="guide-node">
                <div className="flex items-center gap-2">
                  <span className="guide-swatch" aria-hidden="true">
                    <span className={`guide-swatch-fill ${node.tone}`} />
                    <GuideGlyph name={node.name} />
                  </span>
                  <span className="hm-eyebrow text-ink-800">{node.name}</span>
                </div>
                <p className="mt-1.5 text-[12px] leading-[17px] text-ink-500">{node.text}</p>
              </div>
            ))}
          </div>
        </section>

        <div className="guide-divider" aria-hidden="true">
          {[0, 1, 2, 3].map((cell) => (
            <span key={cell} />
          ))}
        </div>

        <section className="guide-section">
          <h3 className="hm-section-title">Permissions</h3>
          <table className="mt-2 w-full border-collapse text-left text-[12px]">
            <tbody>
              {POLICIES.map(([name, text]) => (
                <tr key={name} className="guide-policy-row border-t border-hairline">
                  <th scope="row" className="w-[92px] py-1.5 pr-2 align-top">
                    <span className="guide-policy-name">{name}</span>
                  </th>
                  <td className="py-1.5 align-top text-ink-500">{text}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </section>

        <section className="guide-section">
          <h3 className="hm-section-title">What the agents can actually see</h3>
          <p className="mt-2 text-[12px] leading-[18px] text-ink-500">
            The agents are text-and-files only. They cannot see your screen, windows, browser, or
            desktop and have no vision or computer-use tool. An empty workspace honestly returns empty
            file lists. They can read logs/diagnostics once a workspace is populated; screenshots must
            be supplied as readable data.
          </p>
        </section>

        <section className="guide-section">
          <h3 className="hm-section-title">Root control, remotes, daemons</h3>
          <p className="mt-2 text-[12px] leading-[18px] text-ink-500">
            Root-control buttons only fill a prompt. The server decides whether the HyperAgent may
            actually spawn a remote or daemon. Remotes are in-process principals today, not remote
            machines; daemons are enrolled records, not background Windows services. Zero remotes and
            zero daemons is normal.
          </p>
        </section>

        <section className="guide-section">
          <h3 className="hm-section-title">First run</h3>
          <ol className="hm-steps mt-2.5">
            {STEPS.map((step) => <li key={step} className="hm-step">{step}</li>)}
          </ol>
        </section>

        <div className="mt-5 flex flex-wrap items-center justify-between gap-2 border-t border-hairline pt-3">
          <span className="hm-eyebrow flex flex-wrap items-center gap-1.5 text-meta">
            <kbd className="hm-keycap">ESC</kbd>
            <span>CLOSES</span>
            <span aria-hidden="true">·</span>
            <kbd className="hm-keycap">CTRL</kbd>
            <span aria-hidden="true">+</span>
            <kbd className="hm-keycap">O</kbd>
            <span>RECENTRES THE GRAPH</span>
          </span>
          <button
            type="button"
            onClick={requestClose}
            className="type-label hm-action hm-action-primary"
          >
            Got it
          </button>
        </div>
      </section>
    </div>
  );
}
