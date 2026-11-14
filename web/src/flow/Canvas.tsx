/**
 * S01 canvas + S13 minimap + S20 zoom controls. @xyflow/react 12.12.1 with
 * explicit zoom bounds 0.1-3.0 (library defaults 0.5/2 overridden, R-U7),
 * Ctrl+0 recentre (Windows; Meta+0 alias), honeycomb vector field, bee edges.
 * Replay mode renders the read-only HISTORY_SNAPSHOT graph via the same
 * components but never mutates live state.
 */
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import {
  MiniMap,
  ReactFlow,
  ReactFlowProvider,
  useReactFlow,
  useViewport,
  ViewportPortal,
  type Edge,
  type Node,
  type NodeTypes,
  type EdgeTypes,
} from '@xyflow/react';
import '@xyflow/react/dist/style.css';
import {
  buildEdges,
  buildNodes,
  buildReplayTasks,
  filterVisibleTasks,
  HIVE_ANCHOR_ID,
  HIVE_ANCHOR_POSITION,
  type CanvasNode,
} from './graph';
import { NODE_RENDERERS } from './nodes';
import { BeeEdge } from './BeeEdge';
import { handleCanvasShortcut } from '../a11y/keyboard';
import { useUiStore } from '../state/context';
import { HudBadge } from '../panels/HudBadge';
import { HiveField } from './HiveField';
import { RemoteLane } from './RemoteLane';
import { LAYOUT } from '../tokens/tokens';
import { isRunActive } from '../state/agentState';
import type { TaskRecord } from '../state/store';
import { useAppReducedMotion } from '../state/useAppReducedMotion';
import { STRINGS } from '../state/strings';

function BeeMark() {
  return (
    <svg width="14" height="14" viewBox="0 0 18 18" aria-hidden="true">
      <ellipse cx="9" cy="9" rx="5" ry="3.2" fill="currentColor" />
      <path d="M3 9h12M5.5 6.5h7M5.5 11.5h7" stroke="var(--color-honey-300)" strokeWidth="1.1" />
      <path d="M7 5.5C5.5 3.7 3.6 3.4 2.5 4.2c1.4.4 2.2 1 3.8 1.9ZM11 5.5c1.5-1.8 3.4-2.1 4.5-1.3-1.4.4-2.2 1-3.8 1.9Z" fill="var(--color-honey-100)" />
    </svg>
  );
}

const nodeTypes = NODE_RENDERERS as unknown as NodeTypes;
const edgeTypes: EdgeTypes = { bee: BeeEdge };
const POSITION_STORAGE_PREFIX = 'hivemind.canvas.positions.v1';

type Position = { x: number; y: number };
type PositionMap = Readonly<Record<string, Position>>;

function loadPositions(runId: string | null): PositionMap {
  if (runId === null) {
    return {};
  }
  try {
    const raw = window.localStorage.getItem(`${POSITION_STORAGE_PREFIX}:${runId}`);
    if (raw === null) {
      return {};
    }
    const parsed: unknown = JSON.parse(raw);
    if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) {
      return {};
    }
    const positions: Record<string, Position> = {};
    for (const [taskId, point] of Object.entries(parsed)) {
      if (typeof point !== 'object' || point === null) {
        continue;
      }
      const candidate = point as { x?: unknown; y?: unknown };
      if (typeof candidate.x === 'number' && typeof candidate.y === 'number') {
        positions[taskId] = { x: candidate.x, y: candidate.y };
      }
    }
    return positions;
  } catch {
    return {};
  }
}

function savePositions(runId: string | null, positions: PositionMap): void {
  if (runId === null) {
    return;
  }
  try {
    window.localStorage.setItem(
      `${POSITION_STORAGE_PREFIX}:${runId}`,
      JSON.stringify(positions),
    );
  } catch {
    // Storage may be unavailable/quota-full; the canvas remains usable.
  }
}

function zoomPercent(zoom: number): string {
  return `${Math.round(zoom * 100)}%`;
}

function CanvasInner() {
  const tasks = useUiStore((state) => state.tasks);
  const selectedTaskId = useUiStore((state) => state.selectedTaskId);
  const selectTask = useUiStore((state) => state.selectTask);
  const spawnSubHive = useUiStore((state) => state.spawnSubHive);
  const rewireSelected = useUiStore((state) => state.rewireSelected);
  const runId = useUiStore((state) => state.run?.id ?? null);
  const setViewport = useUiStore((state) => state.setViewport);
  const connection = useUiStore((state) => state.connection);
  const history = useUiStore((state) => state.history);
  const reduced = useAppReducedMotion();
  const { setCenter, getZoom, zoomIn, zoomOut, fitView, getIntersectingNodes, getNode } = useReactFlow();
  const viewport = useViewport();
  // Derived from the live viewport: the old local label started at "100%"
  // and only changed on the first onMoveEnd, so it went stale after a fit.
  const zoomLabel = zoomPercent(viewport.zoom);
  const [positionOverrides, setPositionOverrides] = useState<PositionMap>({});
  const [hideFinished, setHideFinished] = useState(false);
  const [focusBranch, setFocusBranch] = useState(false);
  const [dragSourceId, setDragSourceId] = useState<string | null>(null);
  const [dropTargetId, setDropTargetId] = useState<string | null>(null);
  const [pendingReparent, setPendingReparent] = useState<{
    readonly taskId: string;
    readonly targetId: string;
  } | null>(null);
  const canvasRef = useRef<HTMLDivElement | null>(null);
  const chatVisible = useUiStore((state) => state.chat.visible);
  const runStatus = useUiStore((state) => state.run?.status ?? null);
  const interactiveGraph = runStatus !== null && isRunActive(runStatus);
  const replaying = history.replayGraph !== null;
  const runLabel = replaying
    ? STRINGS.states.replay
    : runStatus === null
      ? 'No run'
      : STRINGS.agentStates[runStatus];

  useEffect(() => {
    // Focus-branch has no meaning without a selection; leaving it pressed made
    // the next clicked card silently filter the graph.
    if (selectedTaskId === null && focusBranch) {
      setFocusBranch(false);
    }
  }, [selectedTaskId, focusBranch]);

  useEffect(() => {
    setPositionOverrides(loadPositions(runId));
  }, [runId]);

  const handleSelect = useCallback(
    (taskId: string) => {
      selectTask(taskId);
    },
    [selectTask],
  );

  const handleSpawn = useCallback(
    (taskId: string) => {
      void spawnSubHive(taskId);
    },
    [spawnSubHive],
  );

  const findDropTarget = useCallback(
    (node: Node): TaskRecord | null => {
      const source = tasks[node.id];
      if (source === undefined) {
        return null;
      }
      const target = getIntersectingNodes(node).find((candidate) => {
        const candidateTask = tasks[candidate.id];
        return (
          candidate.id !== node.id &&
          candidateTask !== undefined &&
          (candidateTask.kind === 'superagent' ||
            candidateTask.kind === 'hyperagent' ||
            candidateTask.kind === 'worker') &&
          candidate.id !== source.parentId
        );
      });
      return target === undefined ? null : tasks[target.id] ?? null;
    },
    [getIntersectingNodes, tasks],
  );

  const visibleTasks = useMemo<Readonly<Record<string, TaskRecord>>>(
    () => filterVisibleTasks(tasks, { hideFinished, focusBranch, selectedTaskId }),
    [tasks, hideFinished, focusBranch, selectedTaskId],
  );

  // The base layout is recomputed only when the task projection changes. Drag
  // overlays are applied in a second, cheap pass so a drag never re-runs the
  // tree layout (and so a mid-drag re-render cannot snap the card back).
  const baseNodes = useMemo<CanvasNode[]>(() => {
    if (history.replayGraph) {
      return [];
    }
    // A graph that is visible is arrangeable: dragging a card after a run
    // finishes still persists its position. Server mutations stay gated by
    // `interactiveGraph` in the drag/drop handlers.
    return buildNodes(visibleTasks, selectedTaskId, handleSelect, runStatus !== null, handleSpawn);
  }, [visibleTasks, selectedTaskId, handleSelect, handleSpawn, history.replayGraph, runStatus]);

  const nodes = useMemo<CanvasNode[]>(() => {
    if (history.replayGraph) {
      return [];
    }
    return baseNodes.map((node) => {
      // The HyperAgent centre is a fixed anchor: its position can never be
      // overwritten by a stale drag overlay.
      const override =
        node.id === HIVE_ANCHOR_ID || node.data.task.kind === 'hyperagent'
          ? undefined
          : positionOverrides[node.id];
      const highlighted =
        node.id === dropTargetId
          ? {
              ...node,
              style: {
                ...node.style,
                boxShadow: '0 0 0 2px var(--color-verify), 0 0 28px rgba(24, 156, 112, 0.45)',
              },
            }
          : node;
      return override ? { ...highlighted, position: override } : highlighted;
    });
  }, [baseNodes, history.replayGraph, positionOverrides, dropTargetId]);

  const hasTasks = Object.keys(visibleTasks).length > 0;
  const liveNodes = useMemo<CanvasNode[]>(() => {
    if (history.replayGraph) {
      return [];
    }
    // Keep the invisible anchor while a hive exists even if the hide-finished
    // filter removes the HyperAgent node itself. Otherwise hyperagent child
    // edges point at a source handle that is not mounted and React Flow drops
    // the hierarchy edge.
    const hasHiveParent = Object.values(tasks).some((task) => task.kind === 'hyperagent');
    if (!hasHiveParent) {
      return nodes;
    }
    return [
      ...nodes,
      {
        id: HIVE_ANCHOR_ID,
        type: 'hiveanchor',
        position: HIVE_ANCHOR_POSITION,
        style: { width: 1, height: 1, opacity: 0, pointerEvents: 'none' },
        data: {},
        draggable: false,
        selectable: false,
        connectable: false,
        focusable: false,
      } as CanvasNode,
    ];
  }, [nodes, tasks, history.replayGraph]);

  const handleNodeDrag = useCallback(
    (_: unknown, node: Node) => {
      if (history.replayGraph) {
        return;
      }
      const source = tasks[node.id];
      if (source === undefined || source.kind === 'hyperagent' || node.id === HIVE_ANCHOR_ID) {
        return;
      }
      // Persist the live drag position into the overlay. Without this, any
      // re-render triggered mid-drag (drop-target highlight, selection) would
      // re-apply the layout position and snap the card back under the cursor.
      setPositionOverrides((previous) => ({
        ...previous,
        [node.id]: { x: node.position.x, y: node.position.y },
      }));
      const target = findDropTarget(node);
      setDragSourceId(node.id);
      setDropTargetId(target?.id ?? null);
    },
    [findDropTarget, history.replayGraph, tasks],
  );

  const handleNodeDragStop = useCallback(
    (_: unknown, node: Node) => {
      setDragSourceId(null);
      setDropTargetId(null);
      if (history.replayGraph) {
        return;
      }
      const source = tasks[node.id];
      if (source === undefined || source.kind === 'hyperagent' || node.id === HIVE_ANCHOR_ID) {
        return;
      }
      // Reparenting is a server mutation; only offer it while the run is
      // active. A finished graph can still be arranged (positions are local).
      if (!interactiveGraph) {
        return;
      }
      const target = findDropTarget(node);
      if (target !== null) {
        setPendingReparent({ taskId: node.id, targetId: target.id });
      }
    },
    [findDropTarget, history.replayGraph, interactiveGraph, tasks],
  );

  const confirmReparent = useCallback(async (): Promise<void> => {
    const pending = pendingReparent;
    if (pending === null) {
      return;
    }
    setPendingReparent(null);
    selectTask(pending.taskId);
    await rewireSelected(pending.targetId);
  }, [pendingReparent, rewireSelected, selectTask]);

  const dragLine = useMemo(() => {
    if (dragSourceId === null || dropTargetId === null) {
      return null;
    }
    const source = getNode(dragSourceId);
    const target = getNode(dropTargetId);
    if (source === undefined || target === undefined) {
      return null;
    }
    const sourceWidth = source.measured?.width ?? 1;
    const sourceHeight = source.measured?.height ?? 1;
    const targetWidth = target.measured?.width ?? 1;
    const targetHeight = target.measured?.height ?? 1;
    const sx = (source.position.x + sourceWidth / 2) * viewport.zoom + viewport.x;
    const sy = (source.position.y + sourceHeight / 2) * viewport.zoom + viewport.y;
    const tx = (target.position.x + targetWidth / 2) * viewport.zoom + viewport.x;
    const ty = (target.position.y + targetHeight / 2) * viewport.zoom + viewport.y;
    return { sx, sy, tx, ty };
  }, [dragSourceId, dropTargetId, getNode, viewport]);

  const fittedRunRef = useRef<string | null>(null);

  useEffect(() => {
    // Fit once per run. Re-fitting on every NODE_SPAWN discarded the operator's
    // pan/zoom while a run was still growing. Ctrl+0 remains the explicit fit.
    if (history.replayGraph || !hasTasks || fittedRunRef.current === runId) {
      return;
    }
    let timer: number | null = null;
    const id = requestAnimationFrame(() => {
      timer = window.setTimeout(() => {
        fittedRunRef.current = runId;
        void fitView({ padding: chatVisible ? 0.08 : 0.18, duration: 0, minZoom: 0.1, maxZoom: 1 });
      }, 420);
    });
    return () => {
      cancelAnimationFrame(id);
      if (timer !== null) {
        window.clearTimeout(timer);
      }
    };
  }, [runId, hasTasks, history.replayGraph, fitView, chatVisible]);

  const edges = useMemo<Edge[]>(() => buildEdges(visibleTasks), [visibleTasks]);

  useEffect(() => {
    if (history.replayGraph === null) {
      return;
    }
    const frame = requestAnimationFrame(() => {
      void fitView({ padding: 0.08, duration: 0, maxZoom: 1.1 });
    });
    return () => cancelAnimationFrame(frame);
  }, [history.replayGraph, fitView]);

  const duration = reduced ? 0 : 250;
  // Ctrl+0/Meta+0 recentres INSTANTLY at document capture level so browser
  // shortcuts, focused controls or React Flow's own key handling cannot swallow
  // it; a rAF re-apply guarantees the transform matrix has changed after the key.
  useEffect(() => {
    const recentre = (): void => {
      setCenter(0, 0, { zoom: getZoom(), duration: 0 });
      requestAnimationFrame(() => {
        setCenter(0, 0, { zoom: getZoom(), duration: 0 });
      });
    };
    const onKeyDown = (event: KeyboardEvent): void => {
      const handled = handleCanvasShortcut(event, {
        recenter: recentre,
        zoomIn: () => {
          void zoomIn({ duration });
        },
        zoomOut: () => {
          void zoomOut({ duration });
        },
        fitView: () => {
          void fitView({ duration, padding: 0.2 });
        },
      });
      if (handled) {
        event.preventDefault();
        event.stopPropagation();
      }
    };
    document.addEventListener('keydown', onKeyDown, true);
    return () => document.removeEventListener('keydown', onKeyDown, true);
  }, [duration, fitView, getZoom, setCenter, zoomIn, zoomOut]);

  // S13: accessible name on the React Flow minimap element (role img).
  useEffect(() => {
    const label = STRINGS.minimap.aria;
    const apply = (): void => {
      for (const selector of ['.react-flow__minimap', '.rf__minimap']) {
        const element = document.querySelector(selector);
        if (element) {
          element.setAttribute('aria-label', label);
          element.setAttribute('role', 'img');
        }
      }
    };
    apply();
    const raf = requestAnimationFrame(apply);
    return () => cancelAnimationFrame(raf);
  }, []);

  const selected = selectedTaskId ? tasks[selectedTaskId] : null;
  const replayTasks = useMemo(
    () => buildReplayTasks(history.replayGraph?.graph),
    [history.replayGraph],
  );
  const noopSelect = useCallback((_taskId: string) => {}, []);
  const replayNodes = useMemo(() => {
    const nodes = buildNodes(replayTasks, null, noopSelect, false);
    const hasHiveParent = Object.values(replayTasks).some((task) => task.kind === 'hyperagent');
    if (!hasHiveParent) {
      return nodes;
    }
    return [
      ...nodes,
      {
        id: HIVE_ANCHOR_ID,
        type: 'hiveanchor',
        position: HIVE_ANCHOR_POSITION,
        style: { width: 1, height: 1, opacity: 0, pointerEvents: 'none' },
        data: {},
        draggable: false,
        selectable: false,
        connectable: false,
        focusable: false,
      } as CanvasNode,
    ];
  }, [replayTasks, noopSelect]);
  const replayEdges = useMemo(() => buildEdges(replayTasks), [replayTasks]);
  const displayedVisibleTasks = history.replayGraph ? replayTasks : visibleTasks;
  const displayedTasks = history.replayGraph ? replayTasks : tasks;

  return (
    <div
      id="hive-canvas"
      ref={canvasRef}
      className="relative h-full min-h-0 w-full"
      style={{
        paddingTop: '56px',
        paddingBottom: chatVisible ? 'var(--chat-dock-height, 0px)' : undefined,
        transition: 'padding-bottom 180ms ease',
      }}
      role="application"
      aria-label="HiveMind spatial canvas"
      tabIndex={-1}
    >
      <svg aria-hidden="true" width="0" height="0" className="absolute">
        <defs>
          <marker id="bee-arrow" markerWidth="7" markerHeight="7" refX="6" refY="3.5" orient="auto">
            <path d="M0 0 7 3.5 0 7Z" fill="var(--color-honey-400)" />
          </marker>
          <marker id="bee-arrow-broken" markerWidth="7" markerHeight="7" refX="6" refY="3.5" orient="auto">
            <path d="M0 0 7 3.5 0 7Z" fill="var(--color-signal)" />
          </marker>
        </defs>
      </svg>
      <div className="hive-scan" aria-hidden="true" />
      <div className="pointer-events-none absolute left-1/2 top-1/2 z-0 h-[660px] w-[620px] -translate-x-1/2 -translate-y-1/2">
        <span className="ambient-bee ambient-bee-a" aria-hidden="true">
          <BeeMark />
        </span>
        <span className="ambient-bee ambient-bee-b" aria-hidden="true">
          <BeeMark />
        </span>
        <span className="ambient-bee ambient-bee-c" aria-hidden="true">
          <BeeMark />
        </span>
      </div>
      <HudBadge />
      {dragLine === null ? null : (
        <svg
          aria-hidden="true"
          className="pointer-events-none absolute inset-0 z-20 h-full w-full"
        >
          <line
            x1={dragLine.sx}
            y1={dragLine.sy}
            x2={dragLine.tx}
            y2={dragLine.ty}
            stroke="var(--color-verify)"
            strokeWidth="2"
            strokeDasharray="5 4"
            strokeLinecap="round"
          />
          <circle cx={dragLine.tx} cy={dragLine.ty} r="5" fill="var(--color-verify)" />
        </svg>
      )}
      {pendingReparent === null ? null : (
        <div className="absolute bottom-24 left-1/2 z-30 w-[min(430px,92%)] -translate-x-1/2 rounded-[var(--radius-card)] border border-verify bg-panel p-3 shadow-[var(--shadow-e2)]">
          <p className="type-label text-ink-900">
            Connect {tasks[pendingReparent.taskId]?.label ?? 'agent'} under{' '}
            {tasks[pendingReparent.targetId]?.label ?? 'target'}?
          </p>
          <p className="type-mono-meta mt-1 text-meta">
            The card moves with this connection. Memory, tool and message scope changes are not
            applied yet; the server rewire is the first step.
          </p>
          <div className="mt-2 flex justify-end gap-2">
            <button
              type="button"
              className="type-label rounded-[var(--radius-button)] border border-hairline px-3 py-1 text-ink-700"
              onClick={() => setPendingReparent(null)}
            >
              Cancel
            </button>
            <button
              type="button"
              className="type-label rounded-[var(--radius-button)] bg-honey-300 px-3 py-1 text-ink-900"
              onClick={() => {
                void confirmReparent();
              }}
            >
              Connect
            </button>
          </div>
        </div>
      )}
      <div className="absolute left-3 top-16 z-20 flex max-w-[calc(100%-1.5rem)] flex-wrap items-center gap-1">
        <button
          type="button"
          aria-pressed={hideFinished}
          disabled={replaying || Object.keys(tasks).length === 0}
          title={
            replaying
              ? 'Filters are disabled while viewing a read-only replay'
              : 'Hide completed and failed cards that have no visible descendants'
          }
          className={
            'type-mono-meta rounded-[var(--radius-pill)] border px-2 py-1 disabled:cursor-not-allowed disabled:opacity-50 ' +
            (hideFinished
              ? 'border-honey-500 bg-honey-100 text-ink-900'
              : 'border-hairline bg-panel text-ink-500 hover:bg-honey-100')
          }
          onClick={() => setHideFinished((previous) => !previous)}
        >
          Hide finished
        </button>
        <button
          type="button"
          aria-pressed={focusBranch}
          disabled={replaying || selectedTaskId === null}
          title={
            replaying
              ? 'Filters are disabled while viewing a read-only replay'
              : selectedTaskId === null
                ? 'Select a node to focus its branch'
                : 'Show only the selected branch'
          }
          className={
            'type-mono-meta rounded-[var(--radius-pill)] border px-2 py-1 disabled:cursor-not-allowed disabled:opacity-50 ' +
            (focusBranch
              ? 'border-honey-500 bg-honey-100 text-ink-900'
              : 'border-hairline bg-panel text-ink-500 hover:bg-honey-100')
          }
          onClick={() => setFocusBranch((previous) => !previous)}
        >
          Focus branch
        </button>
        <span
          className="type-mono-meta rounded-[var(--radius-pill)] border border-hairline bg-panel px-2 py-1 text-meta"
          title="Cards visible after the active filters / cards in this graph"
        >
          {Object.keys(displayedVisibleTasks).length}/{Object.keys(displayedTasks).length}
        </span>
        <span
          data-testid="canvas-run-status"
          data-run-status={replaying ? 'replay' : (runStatus ?? 'none')}
          title={
            replaying
              ? 'Read-only replay of a committed revision; drag and filters are off'
              : runStatus === null
                ? 'No run has been created in this session yet'
                : `Active run status: ${STRINGS.agentStates[runStatus]}`
          }
          className={
            'type-mono-meta rounded-[var(--radius-pill)] border px-2 py-1 ' +
            (replaying
              ? 'border-ink-500 bg-recessed text-ink-800'
              : runStatus === null
                ? 'border-hairline bg-panel text-meta'
                : runStatus === 'failed'
                  ? 'border-signal bg-signal/10 text-signal'
                  : runStatus !== null && isRunActive(runStatus)
                    ? 'border-honey-500 bg-honey-100 text-ink-900'
                    : 'border-verify bg-verify/10 text-verify')
          }
        >
          RUN · {runLabel}
        </span>
      </div>
      <ReactFlow
        nodes={history.replayGraph ? replayNodes : liveNodes}
        edges={history.replayGraph ? replayEdges : edges}
        nodeTypes={nodeTypes}
        edgeTypes={edgeTypes}
        minZoom={0.1}
        maxZoom={3}
        defaultViewport={{ x: 0, y: 0, zoom: 1 }}
        onInit={(instance) => {
          // World origin is the hive. React Flow's default viewport puts the
          // origin at the pane's top-left; centre it before the first paint.
          void instance.setCenter(0, 0, { zoom: 1, duration: 0 });
        }}
        nodesFocusable
        edgesFocusable
        deleteKeyCode={null}
        multiSelectionKeyCode={null}
        onNodeClick={(_, node) => {
          if (!history.replayGraph) {
            handleSelect(node.id);
          }
        }}
        onNodeDrag={handleNodeDrag}
        onNodeDragStop={(_, node: Node) => {
          if (history.replayGraph) {
            handleNodeDragStop(_, node);
            return;
          }
          setPositionOverrides((previous) => {
            const next = {
              ...previous,
              [node.id]: { x: node.position.x, y: node.position.y },
            };
            savePositions(runId, next);
            return next;
          });
          handleNodeDragStop(_, node);
        }}
        onPaneClick={() => {
          if (!history.replayGraph) {
            selectTask(null);
          }
        }}
        onMoveEnd={(_, viewport) => {
          setViewport(viewport);
        }}
        fitView={history.replayGraph !== null}
        fitViewOptions={{ padding: 0.08, minZoom: 0.75, maxZoom: 0.75 }}
        className="h-full w-full bg-transparent"
      >
        <ViewportPortal>
          <div
            aria-hidden="true"
            className="pointer-events-none absolute"
            style={{ left: -310, top: -330, width: 620, height: 660 }}
          >
            <HiveField center={false} />
          </div>
        </ViewportPortal>
        <MiniMap
          pannable
          zoomable
          className={Object.keys(displayedVisibleTasks).length === 0 ? 'hive-minimap-empty' : undefined}
          ariaLabel={STRINGS.minimap.aria}
          nodeColor={(node: Node) => {
            const kind = (node.data as { task?: { kind?: string } } | undefined)?.task?.kind;
            switch (kind) {
              case 'superagent':
                return 'var(--color-honey-400)';
              case 'subagent':
                return 'var(--color-verify)';
              case 'worker':
                return 'var(--color-honey-600)';
              case 'tool':
                return 'var(--color-ink-500)';
              case 'error':
                return 'var(--color-signal)';
              default:
                return 'var(--color-honey-300)';
            }
          }}
          style={{
            width: LAYOUT.minimap.width,
            height: LAYOUT.minimap.height,
            backgroundColor: 'var(--color-panel)',
            border: '1px solid var(--color-hairline)',
            borderRadius: 'var(--radius-md)',
          }}
        />
      </ReactFlow>

      {history.replayGraph ? (
        <div className="absolute left-1/2 top-4 -translate-x-1/2">
          <div className="type-label rounded-[var(--radius-pill)] border border-hairline bg-recessed px-3 py-1 text-ink-800">
            {STRINGS.states.replay} · {STRINGS.scrubber.label} {history.selected}
          </div>
        </div>
      ) : null}

      <div
        role="group"
        className="pointer-events-none absolute bottom-32 left-1/2 z-20 -translate-x-1/2 rounded-[var(--radius-pill)] border border-hairline bg-panel/95 px-3 py-1.5 shadow-[var(--shadow-e1)]"
        aria-label="Hierarchy legend"
      >
        <div className="flex flex-wrap items-center justify-center gap-x-3 gap-y-1 type-mono-meta text-[11px] text-meta">
          <span title="HYPER: planner and root controller" className="flex items-center gap-1"><i className="h-2 w-2 rounded-full bg-honey-500" />HYPER</span>
          <span title="SUPER: branch lead that splits and aggregates work" className="flex items-center gap-1"><i className="h-2 w-2 rounded-full bg-honey-400" />SUPER</span>
          <span title="SUB: temporary read-only helper under a worker" className="flex items-center gap-1"><i className="h-2 w-2 rounded-full bg-verify" />SUB</span>
          <span title="WORKER: executes files, tests, and approved commands" className="flex items-center gap-1"><i className="h-2 w-2 rounded-full bg-honey-600" />WORKER</span>
          <span title="TOOL: a recorded tool call and its outcome" className="flex items-center gap-1"><i className="h-2 w-2 rounded-full bg-ink-500" />TOOL</span>
          <span className="flex items-center gap-1 text-honey-700">bee route</span>
          <span className="flex items-center gap-1 text-signal">broken</span>
        </div>
      </div>
      <div
        role="group"
        className="absolute bottom-32 left-3 flex flex-col gap-1"
        aria-label="Zoom controls"
      >

        <button
          type="button"
          className="surface-panel flex h-8 w-8 items-center justify-center rounded-[var(--radius-sm)] text-ink-800 hover:bg-honey-100"
          onClick={() => {
            void zoomIn({ duration });
          }}
          aria-label={STRINGS.controls.zoomIn}
        >
          <svg width="12" height="12" viewBox="0 0 14 14" aria-hidden="true">
            <path d="M2 7h10M7 2v10" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
          </svg>
        </button>
        <span className="type-mono-meta text-center text-meta">{zoomLabel}</span>
        <button
          type="button"
          className="surface-panel flex h-8 w-8 items-center justify-center rounded-[var(--radius-sm)] text-ink-800 hover:bg-honey-100"
          onClick={() => {
            void zoomOut({ duration });
          }}
          aria-label={STRINGS.controls.zoomOut}
        >
          <svg width="12" height="12" viewBox="0 0 14 14" aria-hidden="true">
            <path d="M2 7h10" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
          </svg>
        </button>
        <button
          type="button"
          className="surface-panel flex h-8 w-8 items-center justify-center rounded-[var(--radius-sm)] text-ink-800 hover:bg-honey-100"
          onClick={() => {
            setCenter(0, 0, { zoom: getZoom(), duration: 0 });
          }}
          aria-label={STRINGS.controls.recenter}
          title={STRINGS.controls.recenter}
        >
          <svg width="13" height="13" viewBox="0 0 16 16" aria-hidden="true">
            <circle cx="8" cy="8" r="3" fill="none" stroke="currentColor" strokeWidth="1.4" />
            <path d="M8 1.5v2M8 12.5v2M1.5 8h2M12.5 8h2" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" />
          </svg>
        </button>
        <button
          type="button"
          className="surface-panel flex h-8 w-8 items-center justify-center rounded-[var(--radius-sm)] text-ink-800 hover:bg-honey-100"
          onClick={() => {
            void fitView({ duration, padding: 0.2 });
          }}
          aria-label={STRINGS.controls.zoomFit}
        >
          <svg width="13" height="13" viewBox="0 0 16 16" aria-hidden="true">
            <path d="M2.5 6V2.5H6M10 2.5h3.5V6M13.5 10v3.5H10M6 13.5H2.5V10" fill="none" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" strokeLinejoin="round" />
          </svg>
        </button>
      </div>

      {nodes.length === 0 && !history.replayGraph ? (
        <div className="pointer-events-none absolute inset-x-0 top-24 flex justify-center">
          <p className="type-mono-meta rounded-[var(--radius-pill)] border border-hairline bg-panel px-3 py-1.5 text-meta">
            {Object.keys(tasks).length > 0 ? 'No agents match the current filters.' : STRINGS.panels.agentsEmpty}
          </p>
        </div>
      ) : null}

      <RemoteLane />

      <div aria-live="polite" className="sr-only">
        {selected ? `${selected.label}: ${selected.state}` : ''}
        {connection === 'reconnecting' ? STRINGS.connection.reconnectBanner : ''}
      </div>
    </div>
  );
}

export function Canvas() {
  return (
    <ReactFlowProvider>
      <CanvasInner />
    </ReactFlowProvider>
  );
}
