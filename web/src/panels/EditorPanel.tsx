/**
 * Editor panel — the workbench's file surface, wired to the real read path.
 *
 * Hops (measured, see `workbenchContracts.ts`):
 *  - open: `FILE_READ` -> correlated `FILE_CONTENT` (WIRED)
 *  - save: no workspace write route exists (MISSING). The editor therefore
 *    renders read-only and names the absent route instead of offering a Save
 *    button that cannot succeed.
 */
import { useEffect, useState } from 'react';
import { Editor } from '../workbench/Editor';
import { RefusalError } from '../api/refusals';
import type { Refusal } from '../api/contract';
import {
  missingHopDetail,
  readWorkspaceFile,
  type WorkspaceApi,
  type WorkspaceFile,
} from './workbenchContracts';

export interface EditorPanelProps {
  /** Connected runtime API; `null` when the socket is not open. */
  readonly api: WorkspaceApi | null;
  /** Workspace-relative path, or `null` when no file is selected. */
  readonly path: string | null;
  readonly onLoaded?: (file: WorkspaceFile) => void;
  readonly className?: string;
}

type PanelStatus = 'idle' | 'loading' | 'ready' | 'error';

const SAVE_MISSING = missingHopDetail('editor.save') ?? 'Saving is unavailable.';

export function EditorPanel({ api, path, onLoaded, className }: EditorPanelProps) {
  const [status, setStatus] = useState<PanelStatus>('idle');
  const [file, setFile] = useState<WorkspaceFile | null>(null);
  const [error, setError] = useState<Refusal | null>(null);
  const [reloadKey, setReloadKey] = useState(0);

  useEffect(() => {
    if (path === null) {
      setStatus('idle');
      setFile(null);
      setError(null);
      return;
    }
    if (api === null) {
      setStatus('error');
      setFile(null);
      setError({
        code: 'E_WS_DISCONNECTED',
        message: 'The runtime session is not open, so the file cannot be read.',
        how_to_fix: 'Reconnect the local session, then retry.',
        retryable: true,
      });
      return;
    }
    let cancelled = false;
    setStatus('loading');
    setError(null);
    readWorkspaceFile(api, path)
      .then((loaded) => {
        if (cancelled) {
          return;
        }
        setFile(loaded);
        setStatus('ready');
        onLoaded?.(loaded);
      })
      .catch((failure: unknown) => {
        if (cancelled) {
          return;
        }
        setFile(null);
        setStatus('error');
        setError(
          failure instanceof RefusalError
            ? failure.refusal
            : {
                code: 'E_INTERNAL_ERROR',
                message: 'Reading the file failed.',
                how_to_fix: 'Retry the read; the server refusal carries the cause.',
                retryable: true,
              },
        );
      });
    return () => {
      cancelled = true;
    };
  }, [api, path, onLoaded, reloadKey]);

  const errorMessage = error === null ? null : `${error.code}: ${error.message}`;

  return (
    <section aria-label="File editor" className={className}>
      <div
        role="note"
        className="mb-2 rounded-[var(--radius-sm)] border border-hairline bg-hairline px-3 py-2 text-sm"
      >
        <p className="font-medium">This editor is read-only.</p>
        <p className="mt-1 opacity-80">{SAVE_MISSING}</p>
      </div>

      <Editor
        path={path ?? ''}
        value={file?.text ?? ''}
        status={status}
        errorMessage={errorMessage}
        readOnly
        readOnlyReason="No workspace write route exists; saves are disabled."
        dirty={false}
        onRetry={() => setReloadKey((key) => key + 1)}
      />

      {file !== null && status === 'ready' ? (
        <p className="mt-2 text-xs opacity-70">
          {file.path} · {file.bytes} bytes · sha256 {file.sha256.slice(0, 12)}…
        </p>
      ) : null}
    </section>
  );
}
