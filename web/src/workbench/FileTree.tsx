/**
 * CAP-4 workbench — workspace file tree.
 *
 * Renders only the nodes the caller supplies (no sample rows, no inferred
 * directories). Directory expansion is either controlled (`expandedPaths`) or
 * local, and every read/open/refresh action is a caller callback. The tree is a
 * roving-tabindex ARIA tree: arrows navigate/expand/collapse, Enter/Space opens.
 *
 * Integration note: the existing FILE_TREE_RESULT contract is a flat listing
 * (`path`, `name`, `kind`, `size_bytes`). A host can build `FileTreeNode[]` from
 * that listing, or call `buildFileTreeNodes(entries)` exported here.
 */
import { useEffect, useMemo, useRef, useState } from 'react';
import type { KeyboardEvent } from 'react';

export type FileTreeStatus = 'idle' | 'loading' | 'ready' | 'error';
export type FileNodeKind = 'file' | 'dir';

export interface FileTreeNode {
  /** Workspace-relative path; also the React key and keyboard identity. */
  readonly path: string;
  readonly name: string;
  readonly kind: FileNodeKind;
  /** Unknown sizes stay null rather than becoming an invented 0. */
  readonly sizeBytes?: number | null;
  /** Only directories may carry children; a file's children are ignored. */
  readonly children?: readonly FileTreeNode[];
}

/** Minimal structural shape of the frozen `FILE_TREE_RESULT` entry. */
export interface FlatFileTreeEntry {
  readonly path: string;
  readonly name: string;
  readonly kind: FileNodeKind;
  readonly size_bytes?: number | null;
}

export interface FileTreeRow {
  readonly node: FileTreeNode;
  readonly depth: number;
  readonly expanded: boolean;
  readonly parentPath: string | null;
}

export interface FileTreeProps {
  readonly nodes: readonly FileTreeNode[];
  readonly selectedPath?: string | null;
  /** Controlled expansion. Omit to let the tree manage its own expansion. */
  readonly expandedPaths?: readonly string[];
  readonly defaultExpandedPaths?: readonly string[];
  readonly root?: string | null;
  readonly status?: FileTreeStatus;
  readonly errorMessage?: string | null;
  readonly onSelectFile?: (path: string) => void;
  readonly onToggleDir?: (path: string) => void;
  readonly onRefresh?: () => void;
  /**
   * When false, a directory click is only a navigation callback and the row
   * does not expand locally. Use this with a flat FILE_TREE_RESULT listing.
   */
  readonly expandOnToggle?: boolean;
  readonly className?: string;
}

export const FILE_TREE_COPY = {
  section: 'Workspace files',
  refresh: 'Refresh file tree',
  root: 'Workspace',
  empty: 'No files in this folder.',
  idle: 'File tree has not loaded yet.',
  loading: 'Loading workspace files…',
  retry: 'Retry',
  error: 'File tree unavailable',
  file: 'file',
  folder: 'folder',
} as const;

function toExpandedSet(
  expandedPaths: ReadonlySet<string> | readonly string[] | undefined,
): ReadonlySet<string> {
  if (expandedPaths === undefined) {
    return new Set<string>();
  }
  if (typeof (expandedPaths as ReadonlySet<string>).has === 'function') {
    return expandedPaths as ReadonlySet<string>;
  }
  return new Set(expandedPaths as readonly string[]);
}

/**
 * Converts the frozen flat FILE_TREE_RESULT listing into a one-level node list.
 * It never invents a parent directory the server did not send.
 */
export function buildFileTreeNodes(entries: readonly FlatFileTreeEntry[]): readonly FileTreeNode[] {
  return entries.map((entry) => ({
    path: entry.path,
    name: entry.name,
    kind: entry.kind,
    sizeBytes: typeof entry.size_bytes === 'number' && entry.size_bytes >= 0 ? entry.size_bytes : null,
  }));
}

/** Depth-first flattening of only the currently visible rows. */
export function flattenFileTree(
  nodes: readonly FileTreeNode[],
  expandedPaths: ReadonlySet<string> | readonly string[] = [],
): readonly FileTreeRow[] {
  const expanded = toExpandedSet(expandedPaths);
  const rows: FileTreeRow[] = [];
  const walk = (list: readonly FileTreeNode[], depth: number, parentPath: string | null): void => {
    for (const node of list) {
      const isDirectory = node.kind === 'dir';
      const isExpanded = isDirectory && expanded.has(node.path);
      rows.push({ node, depth, expanded: isExpanded, parentPath });
      if (isDirectory && isExpanded) {
        walk(node.children ?? [], depth + 1, node.path);
      }
    }
  };
  walk(nodes, 0, null);
  return rows;
}

export interface TreeNavigationTarget {
  /** New roving row; omitted when the key only toggles/activates the current row. */
  readonly index?: number;
  readonly toggle?: boolean;
  readonly activate?: boolean;
}

function parentRowIndex(rows: readonly FileTreeRow[], row: FileTreeRow): number {
  if (row.parentPath === null) {
    return -1;
  }
  return rows.findIndex((candidate) => candidate.node.path === row.parentPath);
}

/**
 * Pure keyboard mapping for the ARIA tree. Returns null for keys the tree does
 * not own, so the caller does not swallow unrelated browser shortcuts.
 */
export function treeNavigationTarget(
  rows: readonly FileTreeRow[],
  activeIndex: number,
  key: string,
): TreeNavigationTarget | null {
  if (rows.length === 0) {
    return null;
  }
  const current = Math.min(Math.max(activeIndex, 0), rows.length - 1);
  const row = rows[current];
  if (row === undefined) {
    return null;
  }
  switch (key) {
    case 'ArrowDown':
      return { index: Math.min(current + 1, rows.length - 1) };
    case 'ArrowUp':
      return { index: Math.max(current - 1, 0) };
    case 'Home':
      return { index: 0 };
    case 'End':
      return { index: rows.length - 1 };
    case 'ArrowRight':
      if (row.node.kind !== 'dir') {
        return null;
      }
      if (row.expanded) {
        const next = rows[current + 1];
        return next !== undefined && next.depth === row.depth + 1 ? { index: current + 1 } : null;
      }
      return { toggle: true };
    case 'ArrowLeft': {
      if (row.node.kind === 'dir' && row.expanded) {
        return { toggle: true };
      }
      const parent = parentRowIndex(rows, row);
      return parent >= 0 ? { index: parent } : null;
    }
    case 'Enter':
    case ' ':
      return { activate: true };
    default:
      return null;
  }
}

export function formatFileSize(sizeBytes: number | null | undefined): string {
  if (typeof sizeBytes !== 'number' || !Number.isFinite(sizeBytes) || sizeBytes < 0) {
    return '—';
  }
  if (sizeBytes < 1024) {
    return `${sizeBytes} B`;
  }
  if (sizeBytes < 1024 * 1024) {
    return `${(sizeBytes / 1024).toFixed(1)} KB`;
  }
  return `${(sizeBytes / (1024 * 1024)).toFixed(1)} MB`;
}

/** Total files in the supplied tree, including collapsed descendants. */
export function countFileNodes(nodes: readonly FileTreeNode[]): number {
  let count = 0;
  for (const node of nodes) {
    if (node.kind === 'file') {
      count += 1;
    } else {
      count += countFileNodes(node.children ?? []);
    }
  }
  return count;
}

function FileGlyph({ kind, expanded }: { kind: FileNodeKind; expanded: boolean }) {
  if (kind === 'dir') {
    return (
      <svg
        aria-hidden="true"
        width="13"
        height="13"
        viewBox="0 0 16 16"
        className="shrink-0 text-honey-700"
      >
        <path
          d="M1.8 4.2h4l1.2 1.4h7.2v6.6a1 1 0 0 1-1 1H2.8a1 1 0 0 1-1-1Z"
          fill="none"
          stroke="currentColor"
          strokeWidth="1.2"
        />
        {expanded ? (
          <path d="M3 13.2 5 8h9l-2 5.2Z" fill="none" stroke="currentColor" strokeWidth="1.1" />
        ) : null}
      </svg>
    );
  }
  return (
    <svg
      aria-hidden="true"
      width="13"
      height="13"
      viewBox="0 0 16 16"
      className="shrink-0 text-ink-500"
    >
      <path d="M3.2 1.8h6.2l3.4 3.4v9H3.2Z" fill="none" stroke="currentColor" strokeWidth="1.2" />
      <path d="M9.4 1.8v3.5h3.4" fill="none" stroke="currentColor" strokeWidth="1.1" />
    </svg>
  );
}

export function FileTree({
  nodes,
  selectedPath = null,
  expandedPaths,
  defaultExpandedPaths = [],
  root = null,
  status = 'ready',
  errorMessage = null,
  onSelectFile,
  onToggleDir,
  onRefresh,
  expandOnToggle = true,
  className = '',
}: FileTreeProps) {
  const [internalExpanded, setInternalExpanded] = useState<ReadonlySet<string>>(
    () => new Set(defaultExpandedPaths),
  );
  const [activePath, setActivePath] = useState<string | null>(selectedPath);
  const rowRefs = useRef<Map<string, HTMLButtonElement>>(new Map());
  const focusRequest = useRef<string | null>(null);
  const controlledExpansion = expandedPaths !== undefined;
  const expanded = controlledExpansion ? toExpandedSet(expandedPaths) : internalExpanded;
  const rows = useMemo(() => flattenFileTree(nodes, expanded), [nodes, expanded]);

  const activeIndex = (() => {
    const found = activePath === null ? -1 : rows.findIndex((row) => row.node.path === activePath);
    if (found >= 0) {
      return found;
    }
    const selected = selectedPath === null ? -1 : rows.findIndex((row) => row.node.path === selectedPath);
    return selected >= 0 ? selected : rows.length > 0 ? 0 : -1;
  })();
  const activeRow = activeIndex >= 0 ? rows[activeIndex] : undefined;

  useEffect(() => {
    const path = focusRequest.current;
    if (path === null || typeof document === 'undefined') {
      return;
    }
    focusRequest.current = null;
    rowRefs.current.get(path)?.focus();
  }, [activePath, rows]);

  const setExpanded = (path: string, next: boolean): void => {
    if (!controlledExpansion) {
      setInternalExpanded((current) => {
        const copy = new Set(current);
        if (next) {
          copy.add(path);
        } else {
          copy.delete(path);
        }
        return copy;
      });
    }
    onToggleDir?.(path);
  };

  const toggleDirectory = (node: FileTreeNode): void => {
    if (!expandOnToggle) {
      onToggleDir?.(node.path);
      return;
    }
    const row = rows.find((candidate) => candidate.node.path === node.path);
    setExpanded(node.path, row?.expanded !== true);
  };

  const activate = (node: FileTreeNode): void => {
    setActivePath(node.path);
    if (node.kind === 'dir') {
      toggleDirectory(node);
      return;
    }
    onSelectFile?.(node.path);
  };

  const onTreeKeyDown = (event: KeyboardEvent<HTMLUListElement>): void => {
    const target = treeNavigationTarget(rows, activeIndex, event.key);
    if (target === null || activeRow === undefined) {
      return;
    }
    event.preventDefault();
    if (target.toggle === true) {
      toggleDirectory(activeRow.node);
    }
    if (target.activate === true) {
      activate(activeRow.node);
    }
    if (target.index !== undefined && target.index !== activeIndex) {
      const next = rows[target.index];
      if (next !== undefined) {
        setActivePath(next.node.path);
        focusRequest.current = next.node.path;
      }
    }
  };

  const fileCount = countFileNodes(nodes);

  return (
    <section
      aria-label={FILE_TREE_COPY.section}
      aria-busy={status === 'loading'}
      className={`flex min-h-0 flex-col overflow-hidden rounded-[var(--radius-card)] border border-hairline bg-panel shadow-[var(--shadow-e1)] ${className}`}
    >
      <header className="flex flex-wrap items-center gap-2 border-b border-hairline bg-panel-alt px-3 py-2">
        <div className="flex min-w-0 flex-1 items-baseline gap-2">
          <h2 className="type-label shrink-0 whitespace-nowrap text-ink-900">
            {FILE_TREE_COPY.section}
          </h2>
          <span className="type-mono-meta truncate text-meta" title={root ?? undefined}>
            {root ?? FILE_TREE_COPY.root}
          </span>
        </div>
        <span className="type-mono-meta shrink-0 text-meta">
          {fileCount} {FILE_TREE_COPY.file}
          {fileCount === 1 ? '' : 's'}
        </span>
        <button
          type="button"
          className="type-mono-meta shrink-0 whitespace-nowrap rounded-[var(--radius-sm)] border border-hairline bg-panel px-2 py-1 text-ink-800 hover:bg-honey-100 disabled:cursor-not-allowed disabled:opacity-50"
          aria-label={FILE_TREE_COPY.refresh}
          disabled={onRefresh === undefined || status === 'loading'}
          onClick={() => onRefresh?.()}
        >
          {FILE_TREE_COPY.refresh}
        </button>
      </header>

      {status === 'idle' ? (
        <div className="p-3">
          {/*
            The header already exposes the refresh control; a second button with
            the same accessible name here made two "Refresh file tree" actions
            appear in the same landmark.
          */}
          <p className="type-body-l text-ink-500">{FILE_TREE_COPY.idle}</p>
        </div>
      ) : null}

      {status === 'loading' ? (
        <p role="status" className="type-mono-meta p-3 text-meta">
          {FILE_TREE_COPY.loading}
        </p>
      ) : null}

      {status === 'error' ? (
        <div role="alert" className="m-3 rounded-[var(--radius-md)] border border-signal bg-error-surface p-3">
          <p className="type-label text-signal">{FILE_TREE_COPY.error}</p>
          <p className="type-body-l mt-1 text-ink-900">
            {errorMessage ?? 'The workspace file tree could not be loaded.'}
          </p>
          {onRefresh === undefined ? null : (
            <button
              type="button"
              className="type-label mt-2 rounded-[var(--radius-button)] border border-signal px-3 py-1.5 text-signal"
              onClick={() => onRefresh()}
            >
              {FILE_TREE_COPY.retry}
            </button>
          )}
        </div>
      ) : null}

      {status === 'ready' && rows.length === 0 ? (
        <div className="p-3">
          <p className="type-body-l text-ink-500">{FILE_TREE_COPY.empty}</p>
        </div>
      ) : null}

      {status === 'ready' && rows.length > 0 ? (
        <ul
          role="tree"
          aria-label={FILE_TREE_COPY.section}
          className="m-0 min-h-0 flex-1 list-none overflow-y-auto p-1"
          onKeyDown={onTreeKeyDown}
        >
          {rows.map((row) => {
            const isSelected = selectedPath === row.node.path;
            const isActive = activeRow?.node.path === row.node.path;
            const size = formatFileSize(row.node.sizeBytes);
            return (
              <li key={row.node.path} role="none" className="m-0 p-0">
                <button
                  ref={(element) => {
                    if (element === null) {
                      rowRefs.current.delete(row.node.path);
                    } else {
                      rowRefs.current.set(row.node.path, element);
                    }
                  }}
                  type="button"
                  role="treeitem"
                  aria-level={row.depth + 1}
                  aria-selected={isSelected}
                  aria-expanded={row.node.kind === 'dir' ? row.expanded : undefined}
                  aria-label={`${row.node.name}, ${row.node.kind === 'dir' ? FILE_TREE_COPY.folder : FILE_TREE_COPY.file}, ${row.node.kind === 'dir' ? (row.expanded ? 'expanded' : 'collapsed') : size}`}
                  tabIndex={isActive ? 0 : -1}
                  title={row.node.path}
                  className={`flex w-full items-center gap-1.5 rounded-[var(--radius-sm)] border px-2 py-1 text-left ${
                    isSelected
                      ? 'border-honey-400 bg-honey-100 text-ink-900'
                      : 'border-transparent text-ink-800 hover:bg-panel-alt'
                  }`}
                  style={{ paddingLeft: `${Math.min(row.depth, 12) * 14 + 8}px` }}
                  onClick={() => {
                    setActivePath(row.node.path);
                    if (row.node.kind === 'dir') {
                      toggleDirectory(row.node);
                    } else {
                      onSelectFile?.(row.node.path);
                    }
                  }}
                >
                  <FileGlyph kind={row.node.kind} expanded={row.expanded} />
                  <span className="type-mono-code min-w-0 flex-1 truncate">{row.node.name}</span>
                  {row.node.kind === 'file' ? (
                    <span className="type-mono-meta shrink-0 text-meta">{size}</span>
                  ) : (
                    <span className="type-mono-meta shrink-0 text-meta" aria-hidden="true">
                      {row.expanded ? '−' : '+'}
                    </span>
                  )}
                </button>
              </li>
            );
          })}
        </ul>
      ) : null}
    </section>
  );
}
