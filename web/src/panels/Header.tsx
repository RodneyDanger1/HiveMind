/** Global top bar (S08/S09/S28): brand, quick agent search, connection and operator. */
import { useState } from 'react';
import { GuideOverlay } from './GuideOverlay';
import { useUiStore } from '../state/context';
import { STRINGS } from '../state/strings';
import { isRunActive } from '../state/agentState';

/** Visible run state for every screen; a null run is named, never guessed. */
export function headerRunLabel(
  replaying: boolean,
  runStatus: keyof typeof STRINGS.agentStates | null,
): string {
  if (replaying) {
    return STRINGS.states.replay;
  }
  return runStatus === null ? 'No run' : STRINGS.agentStates[runStatus];
}

export type ChatToggleIntent = 'navigate-show' | 'show' | 'hide';

/**
 * The chat renders only in Workspace/workspace. Off that surface the header
 * control navigates there and opens the chat; it never changes hidden state.
 */
export function chatToggleIntent(onWorkspace: boolean, chatVisible: boolean): ChatToggleIntent {
  if (!onWorkspace) {
    return 'navigate-show';
  }
  return chatVisible ? 'hide' : 'show';
}

export function Header() {
  const connection = useUiStore((state) => state.connection);
  const project = useUiStore((state) => state.project);
  const tasks = useUiStore((state) => state.tasks);
  const selectTask = useUiStore((state) => state.selectTask);
  const chatVisible = useUiStore((state) => state.chat.visible);
  const setChatVisible = useUiStore((state) => state.setChatVisible);
  const nav = useUiStore((state) => state.view.nav);
  const tab = useUiStore((state) => state.view.tab);
  const setWorkspaceTab = useUiStore((state) => state.setWorkspaceTab);
  const replaying = useUiStore((state) => state.history.replayGraph !== null);
  const exitHistoryReplay = useUiStore((state) => state.exitHistoryReplay);
  const runStatus = useUiStore((state) => state.run?.status ?? null);
  const runLabel = headerRunLabel(replaying, runStatus);
  const runActive = !replaying && runStatus !== null && isRunActive(runStatus);
  const [searchOpen, setSearchOpen] = useState(false);
  const [guideOpen, setGuideOpen] = useState(false);
  const [query, setQuery] = useState('');

  const onWorkspace = nav === 'workspace' && tab === 'workspace';
  const chatIntent = chatToggleIntent(onWorkspace, chatVisible);
  const toggleChat = (): void => {
    if (chatIntent === 'navigate-show') {
      setWorkspaceTab('workspace');
      setChatVisible(true);
      return;
    }
    setChatVisible(chatIntent === 'show');
  };

  const results = Object.values(tasks)
    .filter((task) => task.label.toLowerCase().includes(query.trim().toLowerCase()))
    .slice(0, 6);

  return (
    <header className="relative flex h-[52px] shrink-0 items-center gap-1.5 border-b border-hairline bg-panel px-3 sm:gap-3">
      <div className="flex min-w-0 items-center gap-2">
        <svg width="28" height="28" viewBox="0 0 28 28" aria-hidden="true" className="shrink-0">
          <path
            d="M14 2.2 24 8v12L14 25.8 4 20V8Z"
            fill="var(--color-ink-900)"
            stroke="var(--color-honey-500)"
            strokeWidth="1"
          />
          <path
            d="M14 8.4 19 11.3v5.4L14 19.6 9 16.7v-5.4Z"
            fill="var(--color-honey-400)"
          />
        </svg>
        <span className="hidden truncate text-[17px] font-semibold leading-none text-ink-900 sm:inline">
          HiveMind
        </span>
      </div>

      <div className="ml-auto flex min-w-0 shrink-0 items-center gap-3 max-[640px]:gap-1">
        {replaying ? (
          <button
            type="button"
            className="type-mono-meta rounded-[var(--radius-pill)] border border-honey-500 bg-honey-100 px-2 py-1 text-ink-900 hover:bg-honey-200"
            onClick={exitHistoryReplay}
          >
            Return live
          </button>
        ) : null}
        <button
          type="button"
          aria-label={
            chatIntent === 'navigate-show'
              ? STRINGS.chat.toggleWorkspaceAria
              : chatVisible
                ? STRINGS.chat.toggleHideAria
                : STRINGS.chat.toggleShowAria
          }
          title={
            chatIntent === 'navigate-show'
              ? STRINGS.chat.toggleWorkspaceHint
              : chatVisible
                ? 'Hide the bottom chat dock.'
                : 'Show the bottom chat dock.'
          }
          aria-pressed={chatVisible}
          className="type-mono-meta flex h-8 items-center gap-1 rounded-[var(--radius-pill)] border border-hairline bg-panel px-2 text-ink-700 hover:bg-panel-alt hover:text-ink-900"
          onClick={toggleChat}
        >
          <svg width="13" height="13" viewBox="0 0 18 18" aria-hidden="true">
            <path
              d="M3 3.5h12v8.5H9l-3.2 2.4v-2.4H3Z"
              fill="none"
              stroke="currentColor"
              strokeWidth="1.4"
              strokeLinejoin="round"
            />
          </svg>
          <span>{chatIntent === 'hide' ? STRINGS.chat.close : STRINGS.chat.reopen}</span>
        </button>
        <button
          type="button"
          aria-label="Open HiveMind guide"
          title="What am I looking at?"
          aria-expanded={guideOpen}
          className="flex h-8 w-8 items-center justify-center rounded-full border border-hairline bg-panel text-[13px] font-semibold text-ink-700 hover:bg-panel-alt hover:text-ink-900"
          onClick={() => setGuideOpen(true)}
        >
          ?
        </button>
        <button
          type="button"
          aria-label="Search agents"
          title="Find an agent in the current run."
          aria-expanded={searchOpen}
          className="flex h-8 w-8 items-center justify-center rounded-full text-ink-500 hover:bg-panel-alt hover:text-ink-900"
          onClick={() => setSearchOpen((open) => !open)}
        >
          <svg width="15" height="15" viewBox="0 0 20 20" aria-hidden="true">
            <circle cx="8.5" cy="8.5" r="5.2" fill="none" stroke="currentColor" strokeWidth="1.5" />
            <path d="m12.5 12.5 4 4" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
          </svg>
        </button>
        <span
          data-testid="header-run-status"
          data-run-status={replaying ? 'replay' : (runStatus ?? 'none')}
          title={
            replaying
              ? 'Read-only history replay is open; return live to control the current run'
              : runStatus === null
                ? 'No run yet — submit a prompt in the chat dock to start one'
                : `Run status: ${runLabel}`
          }
          aria-label={`Run status: ${runLabel}`}
          className={
            'type-mono-meta hidden items-center gap-1 rounded-[var(--radius-pill)] border px-2 py-1 sm:flex ' +
            (replaying
              ? 'border-ink-500 bg-recessed text-ink-800'
              : runStatus === null
                ? 'border-hairline bg-panel text-meta'
                : runStatus === 'failed'
                  ? 'border-signal text-signal'
                  : runActive
                    ? 'border-honey-500 bg-honey-100 text-ink-900'
                    : 'border-verify text-verify')
          }
        >
          <span
            aria-hidden="true"
            className={
              'h-1.5 w-1.5 rounded-full ' +
              (replaying
                ? 'bg-ink-500'
                : runStatus === null
                  ? 'bg-ink-500/40'
                  : runStatus === 'failed'
                    ? 'bg-signal'
                    : runActive
                      ? 'motion-hive-live-dot bg-honey-600'
                      : 'bg-verify')
            }
          />
          {runLabel}
        </span>
        <div
          title={`Server connection: ${STRINGS.connection[connection]}`}
          className={
            'type-mono-meta rounded-[var(--radius-pill)] border px-2 py-1 max-[640px]:max-w-[110px] truncate ' +
            (connection === 'open'
              ? 'border-verify text-verify'
              : connection === 'reconnecting' || connection === 'connecting'
                ? 'border-amber text-ink-800'
                : 'border-signal text-signal')
          }
          aria-live="polite"
        >
          {STRINGS.connection[connection]}
        </div>
        <div className="flex items-center gap-2 border-l border-hairline pl-3">
          <span className="flex h-8 w-8 items-center justify-center rounded-full bg-honey-200 text-[11px] font-semibold text-ink-800">
            {project.owner === 'You' ? 'HM' : project.owner.slice(0, 2).toUpperCase()}
          </span>
          <span className="type-mono-meta hidden text-ink-800 sm:block">
            {project.owner === 'You' ? 'Operator' : project.owner}
          </span>
        </div>
      </div>
      {searchOpen ? (
        <div className="surface-panel absolute right-3 top-[48px] z-50 w-[320px] rounded-[var(--radius-md)] border border-hairline p-2 shadow-[var(--shadow-e2)]">
          <label className="sr-only" htmlFor="agent-search">
            Search agents
          </label>
          <input
            id="agent-search"
            autoFocus
            value={query}
            onChange={(event) => setQuery(event.target.value)}
            onKeyDown={(event) => {
              if (event.key === 'Escape') {
                setSearchOpen(false);
              }
            }}
            placeholder="Find an agent…"
            className="w-full rounded-[var(--radius-sm)] border border-hairline bg-panel-alt px-2 py-1.5 text-[12px] text-ink-900 outline-none focus-visible:ring-2 focus-visible:ring-[var(--color-ink-700)]"
          />
          <ul className="mt-1 max-h-[240px] overflow-y-auto">
            {results.length === 0 ? (
              <li className="type-mono-meta px-2 py-2 text-meta">No agents in this run.</li>
            ) : (
              results.map((task) => (
                <li key={task.id}>
                  <button
                    type="button"
                    className="flex w-full items-center justify-between gap-2 rounded-[var(--radius-sm)] px-2 py-1.5 text-left hover:bg-honey-100"
                    onClick={() => {
                      selectTask(task.id);
                      setSearchOpen(false);
                    }}
                  >
                    <span className="truncate text-[12px] text-ink-900">{task.label}</span>
                    <span className="type-mono-meta shrink-0 text-meta">{task.state}</span>
                  </button>
                </li>
              ))
            )}
          </ul>
        </div>
      ) : null}
      <GuideOverlay open={guideOpen} onClose={() => setGuideOpen(false)} />
    </header>
  );
}
