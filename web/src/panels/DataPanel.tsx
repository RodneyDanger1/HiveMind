/**
 * S22 Data: FILE_TREE_LIST / FILE_READ against the patched contract, jailed client-side too.
 *
 * FILE_TREE_LIST accepts an optional path. Directory rows expand in place; only
 * regular files dispatch FILE_READ.
 *
 * Status ownership: FileTree renders its own idle/loading/error/empty states from
 * the `status` prop, and Editor renders the open file. This panel must not add a
 * second copy of those states around them, and it must not keep the previously
 * opened file visible while the next FILE_READ is in flight.
 */
import { useEffect } from 'react';
import { useUiStore } from '../state/context';
import { STRINGS } from '../state/strings';
import { Editor, FileTree, FILE_TREE_COPY, buildFileTreeNodes } from '../workbench';
import type { FileTreeStatus } from '../workbench';
import { PanelTitle, EmptyState, LoadingState, ErrorState } from './StatePack';
import { DiffReviewPanel } from './DiffReviewPanel';

const FILE_TREE_STATUS: Record<string, FileTreeStatus> = {
  idle: 'idle',
  loading: 'loading',
  ok: 'ready',
  error: 'error',
};

export function DataPanel() {
  const fileTree = useUiStore((state) => state.fileTree);
  const requestFileTree = useUiStore((state) => state.requestFileTree);
  const openFile = useUiStore((state) => state.openFile);
  const run = useUiStore((state) => state.run);

  useEffect(() => {
    if (fileTree.status === 'idle') {
      void requestFileTree();
    }
  }, [fileTree.status, requestFileTree]);

  // The path a retry must re-open is the one that was requested, not the last
  // file that happened to read successfully (content keeps the old payload).
  const retryPath = fileTree.selectedPath;

  return (
    // Mobile stacks the two panes at their natural height and scrolls the panel;
    // the previous fixed 40/60 rows clipped the Editor's header/footer at 375px.
    // The desktop two-column layout keeps the single fixed row and its own scroll.
    <div className="grid h-full min-h-0 grid-cols-1 gap-3 overflow-y-auto p-4 lg:grid-cols-[280px_1fr] lg:grid-rows-1 lg:overflow-hidden">
      {/*
        No section wrapper here: FileTree already renders the single named
        "Workspace files" landmark. Nesting it in a second identically-labelled
        section made screen readers announce the same workspace twice.
      */}
      <div className="min-h-0 max-h-[40vh] overflow-y-auto lg:max-h-none">
        <PanelTitle>{STRINGS.panels.dataTitle}</PanelTitle>
        <FileTree
          nodes={buildFileTreeNodes(fileTree.entries)}
          selectedPath={fileTree.selectedPath}
          root={FILE_TREE_COPY.root}
          status={FILE_TREE_STATUS[fileTree.status] ?? 'idle'}
          errorMessage={fileTree.status === 'error' ? STRINGS.panels.dataError : null}
          onSelectFile={(path) => {
            void openFile(path);
          }}
          onToggleDir={(path) => {
            void requestFileTree(path);
          }}
          onRefresh={() => {
            void requestFileTree();
          }}
          expandOnToggle={false}
        />
      </div>
      <section
        aria-label="File preview"
        aria-busy={fileTree.contentStatus === 'loading'}
        className="min-h-fit overflow-auto lg:min-h-0"
      >
        {fileTree.contentStatus === 'loading' ? <LoadingState /> : null}
        {fileTree.contentStatus === 'error' ? (
          <div className="space-y-2">
            <ErrorState message={STRINGS.states.unavailable} />
            <button
              type="button"
              className="type-mono-meta rounded-[var(--radius-sm)] border border-hairline px-2 py-1 text-ink-800 hover:bg-honey-100 disabled:cursor-not-allowed disabled:opacity-50"
              aria-label={
                retryPath === null
                  ? STRINGS.controls.retry
                  : `${STRINGS.controls.retry} ${retryPath}`
              }
              disabled={retryPath === null}
              onClick={() => {
                if (retryPath !== null) {
                  void openFile(retryPath);
                }
              }}
            >
              {STRINGS.controls.retry}
            </button>
          </div>
        ) : null}
        {fileTree.contentStatus === 'idle' ? (
          <EmptyState message={STRINGS.panels.dataPreviewHint} />
        ) : null}
        {/*
          Only a confirmed FILE_CONTENT may render. While a read is loading or
          after it failed, the previous file's text is stale and must not be
          presented as the newly selected path.
        */}
        {fileTree.contentStatus === 'ok' && fileTree.content !== null ? (
          <Editor
            path={fileTree.content.path}
            value={fileTree.content.text}
            readOnly
            readOnlyReason="Workspace reads are preview-only here; approved mutations run through the agent tool path."
          />
        ) : null}
        <div className="mt-4 border-t border-hairline pt-3">
          <DiffReviewPanel runId={run?.id ?? null} runStatus={run?.status ?? null} />
        </div>
      </section>
    </div>
  );
}
