/** In-app tutorials and plain-English explanations. No dead controls. */
import { useUiStore } from '../state/context';
import { STRINGS } from '../state/strings';
import { InfoTip } from './InfoTip';

const TUTORIALS = [
  {
    title: 'First run',
    steps: [
      'In the bottom dock, open SESSION & PERMISSIONS and choose a policy.',
      'Type a small task in the composer; press Enter.',
      'Watch the graph: HYPER plans, SUPER splits, WORKER executes.',
      'Approve or deny any side effect in the inline card.',
    ],
  },
  {
    title: 'The dock',
    steps: [
      'Drag the top grip up or down to resize the chat.',
      'CONTROLS opens spawn / re-route / cancel / kill.',
      'STATE & SETTINGS shows provider, model, key, reasoning, and version.',
      'HIDE removes the dock; the header Chat button brings it back.',
    ],
  },
  {
    title: 'Safety',
    steps: [
      'classic asks before side effects; plan refuses writes.',
      'standard allows workspace writes; trusted removes prompts.',
      'Never use trusted on a workspace you do not trust.',
      'Every refusal has a name; the run ledger keeps it.',
    ],
  },
];

const FEATURES = [
  ['Workspace', 'The live agent graph and the resizable chat dock.'],
  ['Agents', 'Roster, jobs, todos, memory, lessons, and proposals.'],
  ['Data', 'Runs, revisions, evidence, and snapshots.'],
  ['Models', 'Model catalogue, usage, cache counters, and balance.'],
  ['Routing', 'Which model or strategy handles a task.'],
  ['Remote', 'Remote principals and daemons today are local records, not network machines.'],
  ['Settings', 'Provider profile, key status, diagnostics, and FigCraft gallery.'],
  ['FigCraft', 'Design boards and the design gallery; the live UI is coded from tokens.'],
  ['Screen access', 'Agents cannot see your desktop, browser, or screen. They can read workspace files and report diagnostics; a screenshot must be supplied as readable text/data.'],
  ['Workspace', 'The agent sees only the configured workspace root. If it is empty, file tools honestly return empty results.'],
];

export function HelpPanel() {
  const setNav = useUiStore((state) => state.setNav);
  return (
    <section className="help-surface" aria-labelledby="help-title">
      <div className="help-ambient" aria-hidden="true">
        <svg className="help-comb" width="100%" height="100%" focusable="false">
          <defs>
            <pattern id="help-comb-pattern" width="46" height="80" patternUnits="userSpaceOnUse">
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
          <rect width="100%" height="100%" fill="url(#help-comb-pattern)" />
        </svg>
      </div>

      <div className="help-scroll">
        <div className="mx-auto max-w-[980px]">
          <header className="help-hero">
            <div className="flex items-center gap-4 sm:gap-5">
              <div className="help-mark" aria-hidden="true">
                <svg width="44" height="44" viewBox="0 0 48 48" focusable="false">
                  <g
                    fill="none"
                    stroke="var(--color-ink-800)"
                    strokeWidth="1.5"
                    strokeLinecap="round"
                    strokeLinejoin="round"
                  >
                    <path d="M20 8.2 33 15.7v14.9L20 38 7 30.6V15.7Z" />
                    <path d="M20 16.6 25.8 20v6.8L20 30.2l-5.8-3.4V20Z" />
                    <path d="m10 20.2 4.2-2.4M30 29.6l-4.2 2.4" opacity=".55" />
                  </g>
                  <circle
                    cx="38.2"
                    cy="12.2"
                    r="7.4"
                    fill="var(--color-honey-200)"
                    stroke="var(--color-ink-800)"
                    strokeWidth="1.3"
                  />
                  <path
                    d="M35.5 10.7a2.5 2.5 0 1 1 3.3 2.4c-.6.2-.9.7-.9 1.3"
                    fill="none"
                    stroke="var(--color-ink-800)"
                    strokeWidth="1.5"
                    strokeLinecap="round"
                  />
                  <circle cx="37.9" cy="16.4" r=".9" fill="var(--color-ink-800)" />
                </svg>
              </div>
              <div className="min-w-0">
                <div className="hm-eyebrow flex text-meta">
                  <span className="help-eyebrow-hex" aria-hidden="true" />
                  HELP &amp; TUTORIALS
                </div>
                <h1 id="help-title" className="hm-display-title mt-1">
                  What do you want to do?
                </h1>
                <p className="mt-2 max-w-[64ch] text-[13px] leading-5 text-ink-500">
                  HiveMind is a local multi-agent workspace. The HyperAgent plans; SuperAgents split the
                  work; Workers execute; Subagents help read-only. Every risky action is approved or refused
                  by name, and every run is recorded locally.
                </p>
              </div>
            </div>
          </header>

          <div className="mt-5 flex flex-wrap items-end justify-between gap-2">
            <div className="flex items-center gap-2">
              <h2 className="hm-section-title">Start here</h2>
              <InfoTip label="How the tutorials work">
                Each card is a short checklist. Nothing here changes the runtime; the steps only point at
                the surfaces where the action happens.
              </InfoTip>
            </div>
            <span className="hm-eyebrow text-meta">{TUTORIALS.length} TUTORIALS</span>
          </div>

          <div className="mt-3 grid gap-3 lg:grid-cols-3">
            {TUTORIALS.map((tutorial, index) => (
              <section
                key={tutorial.title}
                className="help-card"
                aria-labelledby={`help-tutorial-${index}`}
              >
                <div className="flex items-start justify-between gap-2">
                  <h3 id={`help-tutorial-${index}`} className="hm-card-title">
                    {tutorial.title}
                  </h3>
                  <span className="help-card-hex" aria-hidden="true">
                    <span className="help-card-index">{String(index + 1).padStart(2, '0')}</span>
                  </span>
                </div>
                <ol className="hm-steps mt-3">
                  {tutorial.steps.map((step) => (
                    <li key={step} className="hm-step">{step}</li>
                  ))}
                </ol>
              </section>
            ))}
          </div>

          <div className="help-divider" aria-hidden="true">
            {[0, 1, 2, 3, 4].map((cell) => (
              <span key={cell} />
            ))}
          </div>

          <div className="mt-6 flex flex-wrap items-end justify-between gap-2">
            <h2 className="hm-section-title">What lives where</h2>
            <span className="hm-eyebrow text-meta">{FEATURES.length} SURFACES</span>
          </div>

          <div className="mt-3 grid gap-2 sm:grid-cols-2">
            {FEATURES.map(([name, text], index) => (
              <div key={`${name}-${index}`} className="help-feature">
                <div className="flex items-baseline gap-2">
                  <span className="help-feature-index" aria-hidden="true">
                    {String(index + 1).padStart(2, '0')}
                  </span>
                  <div className="hm-eyebrow min-w-0 text-ink-800">{name}</div>
                </div>
                <p className="mt-1 text-[12px] leading-[17px] text-ink-500">{text}</p>
              </div>
            ))}
          </div>

          <div className="mt-5 flex flex-wrap items-center gap-2">
            <button
              type="button"
              className="type-label hm-action hm-action-primary"
              onClick={() => setNav('workspace')}
            >
              Go to Workspace
              <svg width="12" height="12" viewBox="0 0 16 16" aria-hidden="true">
                <path
                  d="M3 8h9M8.5 4.5 12 8l-3.5 3.5"
                  fill="none"
                  stroke="currentColor"
                  strokeWidth="1.5"
                  strokeLinecap="round"
                  strokeLinejoin="round"
                />
              </svg>
            </button>
            <button
              type="button"
              className="type-label hm-action hm-action-secondary"
              onClick={() => setNav('settings')}
            >
              Provider &amp; key settings
            </button>
            <button
              type="button"
              className="type-label hm-action hm-action-secondary"
              onClick={() => setNav('agents')}
            >
              Agent roster &amp; jobs
            </button>
          </div>

          <p className="mt-5 flex flex-wrap items-center gap-1.5 hm-eyebrow text-meta">
            <span>{STRINGS.brand.wordmark}</span>
            <span aria-hidden="true">·</span>
            <span>Press</span>
            <kbd className="hm-keycap">?</kbd>
            <span>in the header for the quick canvas guide.</span>
          </p>
        </div>
      </div>
    </section>
  );
}
