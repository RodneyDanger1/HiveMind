/**
 * Terminal panel — honest unavailable state.
 *
 * There is no interactive shell surface in this build. The measured facts
 * (`workbenchContracts.ts` `terminal.open`):
 *  - the HTTP route table registers no `/api/terminal`, `/api/pty` or
 *    `/ws/terminal` endpoint;
 *  - the frozen WebSocket vocabulary has no `SHELL_EXEC` / `TERMINAL_*` command;
 *  - the shell tool is an approval-gated agent tool with no UI channel.
 *
 * Rendering a command box here would be a dead control, so this panel renders
 * the exact missing route and points at the surfaces that do have backends.
 */
import { WORKBENCH_HOPS } from './workbenchContracts';

export interface TerminalPanelProps {
  readonly className?: string;
}

const TERMINAL_HOP = WORKBENCH_HOPS['terminal.open'];

export function TerminalPanel({ className }: TerminalPanelProps) {
  return (
    <section aria-label="Terminal" className={className}>
      <h3 className="text-sm font-semibold">Terminal unavailable</h3>

      <div role="note" className="mt-2 rounded-[var(--radius-sm)] border border-hairline px-3 py-2 text-sm">
        <p>
          This build has no interactive shell channel, so no terminal is opened and no command
          input is rendered: there is no route that would receive it.
        </p>
        <p className="mt-2 font-medium">Missing backend hop</p>
        <p className="mt-1 text-xs opacity-80">{TERMINAL_HOP.missing}</p>
        <p className="mt-1 text-xs opacity-70">
          Checked contract: {TERMINAL_HOP.contract} · evidence {TERMINAL_HOP.evidence}
        </p>
      </div>

      <div className="mt-3 text-xs">
        <p className="font-medium">What does exist instead</p>
        <ul className="mt-1 list-disc pl-5">
          <li>
            Background jobs: <code>GET /api/jobs</code> lists them and{' '}
            <code>POST /api/jobs/&#123;job_id&#125;/kill</code> stops one (see Monitor controls).
          </li>
          <li>
            Agent shell calls run as approval-gated tool invocations; their requests appear in the
            approval ledger, not in this panel.
          </li>
        </ul>
      </div>
    </section>
  );
}
