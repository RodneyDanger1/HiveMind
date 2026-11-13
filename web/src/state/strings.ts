/**
 * The single string table (R-U13): every user-facing string resolves from
 * here so no copy is hardcoded in components. Static copy is derived from the
 * FigCraft PDF text (`PDF:<line>` = evidence/pdf/pages.txt) or the frozen
 * contract; strings the PDF does not provide are PROPOSED-ADDITION.
 */

export const STRINGS = {
  app: {
    title: 'HiveMind',
    subtitle: 'SPATIAL EXECUTION ENVIRONMENT',
    documentTitle: 'HiveMind — Spatial Execution Environment',
    skipToCanvas: 'Skip to canvas',
  },
  splash: {
    skip: 'Skip intro',
    skipAria: 'Skip the startup intro and show the workspace',
    skipHint: 'Press Escape to skip the intro.',
  },
  crash: {
    title: 'The interface stopped rendering',
    body: 'A panel threw while React was rendering, so this window stopped before it could draw the rest of the app. The local server and the committed runs are unaffected.',
    detailLabel: 'Reported error',
    reload: 'Reload the interface',
    hint: 'If this screen comes back after a reload, close the window and reopen HiveMind from its shortcut so the launcher restarts the local server.',
  },
  brand: {
    wordmark: 'HiveMind',
    lockupSubtitle: 'SPATIAL EXECUTION ENVIRONMENT',
    /** PROPOSED-ADDITION: inline fallback glyph until FigCraft assets are allowlisted. */
    markAria: 'HiveMind hexagon with routing bee',
  },
  nav: {
    aria: 'Primary navigation',
    workspace: 'Workspace',
    agents: 'Agents',
    data: 'Data',
    models: 'Models',
    routing: 'Routing',
    remote: 'Remote',
    settings: 'Settings',
    help: 'Help',
  },
  tabs: {
    aria: 'Workspace tabs',
    workspace: 'Workspace',
    swarm: 'Swarm',
    routing: 'Routing',
    history: 'History',
  },
  hud: {
    aria: 'Swarm throughput',
    superagents: 'SUPERAGENTS',
    workers: 'WORKERS',
    green: 'GREEN',
    noMetrics: 'NO METRICS',
    deltaLabel: 'since previous metrics frame',
    sourceServer: 'live',
    sourceDerived: 'derived',
    sourceNone: 'no metrics',
  },
  connection: {
    idle: 'Idle',
    connecting: 'Connecting to local server',
    open: 'Connected',
    reconnecting: 'Reconnecting',
    closed: 'Disconnected',
    error: 'Connection failed',
    mock: 'DEV MOCK · simulated data',
    offlineBanner: 'The local server is unreachable — reopen HiveMind from its shortcut so the launcher restarts it, then retry.',
    reconnectBanner: 'Reconnecting to the local server; mutations are blocked until the socket is open.',
  },
  assets: {
    pending: 'FigCraft assets pending UI-watchdog allowlist; PDF-derived tokens only.',
    accepted:
      'FigCraft A+B exports accepted by UI watchdog (PASS-WITH-NOTES, FAIL 0). Values come from the token module, never sampled pixels; pixel-layout fidelity remains UNVERIFIED.',
    gateLabel: 'FIGCRAFT GATE',
  },
  models: {
    none: 'The server has not reported provider/model yet (run SETTINGS_STATUS in Settings).',
    providerLabel: 'Provider',
    modelLabel: 'Model',
    contractNote:
      'Provider, model, and account values are exactly what the server reports; the catalogue never implies wire support for an external endpoint.',
    notReported: 'Not reported',
  },
  gallery: {
    title: 'Node & state pack (DEV ONLY)',
    note: 'Dev-only renderer gallery; never reachable in a production build.',
  },
  chat: {
    aria: 'HyperAgent hive chat core',
    title: 'Hive AI Assistant',
    pinned: 'Pinned to hive centre',
    pin: 'Pin chat',
    unpin: 'Unpin chat',
    close: 'Hide chat',
    reopen: 'Show chat',
    empty: 'Type a prompt to create the first run. The HyperAgent maps the work before any worker spawns.',
    assistantName: 'Hive AI Assistant',
    userLabel: 'You',
    closeAria: 'Hide the chat core (the hive stays)',
    untitledRun: 'Untitled run',
    /** Header toggle: chat only renders inside Workspace/workspace. */
    toggleShowAria: 'Show Hive chat',
    toggleHideAria: 'Hide Hive chat',
    toggleWorkspaceAria: 'Open Hive chat in Workspace',
    toggleWorkspaceHint: 'Chat lives in Workspace; this opens it there.',
  },
  composer: {
    placeholder: 'Type your prompt...',
    submit: 'Submit prompt',
    hint: 'Enter submits · Shift+Enter newline · Ctrl+0 recentres',
    emptyRefusal: 'Prompt is empty',
  },
  controls: {
    spawn: 'Spawn sub-hive',
    reroute: 'Re-route',
    cancel: 'Cancel',
    kill: 'Kill worker',
    approve: 'Approve',
    deny: 'Deny',
    retry: 'Retry',
    dismiss: 'Dismiss',
    close: 'Close',
    refresh: 'Refresh',
    recenter: 'Recentre hive (Ctrl+0)',
    zoomIn: 'Zoom in',
    zoomOut: 'Zoom out',
    zoomFit: 'Fit swarm',
    openSettings: 'Open settings',
    disabledNoRun: 'No active run',
    disabledNoSelection: 'Select a running worker first',
  },
  library: {
    aria: 'Library panel',
    title: 'Library',
    projectLabel: 'Project',
    projectFallback: 'HiveMind AI',
    activeAgents: 'AGENTS IN RUN',
    templates: 'TEMPLATES',
    tip: 'Drag a worker onto another super-agent to re-route its context stream live.',
    emptyAgents: 'No active agents',
    templateLicence: 'licence',
  },
  templates: {
    contentWriter: 'Content Writer',
    dataAnalyst: 'Data Analyst',
    codeReviewer: 'Code Reviewer',
    chatbot: 'Chatbot',
  },
  scrubber: {
    aria: 'Time-travel scrubber',
    label: 'Routing state',
    of: 'of',
    live: 'live',
    empty: 'No run history yet',
  },
  minimap: {
    aria: 'Mini-map',
  },
  toast: {
    regionAria: 'Refusals and errors',
    prefix: 'REFUSED BY NAME',
    suffix: 'NOT SILENT',
    dismiss: 'Dismiss refusal',
  },
  settings: {
    aria: 'Settings',
    title: 'Settings',
    keyTitle: 'Provider key status',
    keyUnknown: 'Not probed by the server',
    keyPresent: 'Key configured',
    keyMissing: 'Key missing',
    keyUnprobedHint: 'Submit a prompt to surface E_KEY_MISSING if no key is configured.',
    thinkingLabel: 'Thinking mode',
    effortLabel: 'Reasoning effort',
    notReported: 'Not reported',
    modelLabel: 'Model',
    providerLabel: 'Provider',
    healthLabel: 'Server health',
    healthUnknown: 'Not checked',
    checksLabel: 'Environment checks',
    refresh: 'Re-check health',
  },
  /** One label per agent state (R-U4: same vocabulary as the enum). */
  agentStates: {
    healthy: 'Healthy',
    running: 'Running',
    queued: 'Queued',
    failed: 'Failed',
    completed: 'Completed',
    cancelled: 'Cancelled',
    killed: 'Killed',
    planning: 'Planning',
    thinking: 'Thinking',
    awaiting_approval: 'Awaiting approval',
    streaming: 'Streaming',
    reconnecting: 'Reconnecting',
    replay: 'Replay',
    empty: 'Empty',
    loading: 'Loading',
    interrupted: 'Interrupted',
  },
  states: {
    loading: 'Loading',
    empty: 'Nothing here yet',
    error: 'Something went wrong',
    offline: 'Server unreachable',
    missingKey: 'Provider key missing',
    replay: 'Replay (read-only)',
    notMeasured: 'Not measured',
    unavailable: 'Unavailable',
  },
  panels: {
    agentsTitle: 'Agents',
    agentsEmpty: 'No agents yet — submit a prompt to create the first run.',
    dataTitle: 'Data',
    dataUnavailable:
      'Workspace files are served by the frozen FILE_TREE_LIST and FILE_READ commands. Load the tree, then select a file to preview it here.',
    dataError:
      'The workspace tree could not be loaded; the server returned a named refusal (see the toast) — retry with Refresh.',
    dataPreviewHint: 'Select a file from the tree to preview it here.',
    modelsTitle: 'Models',
    modelsEmpty: 'Server reports no models.',
    routingTitle: 'Routing',
    routingEmpty: 'No edges in the current graph.',
    historyTitle: 'History',
    historyEmpty: 'No revisions yet.',
    remoteTitle: 'Remote',
  },
  /**
   * W12 tool activity (S15/S16). Consumed by `ToolActivityPanel` (heading, empty,
   * list/section labels, outcome labels) and by the agents-surface region wrapper.
   * Copy is PROPOSED-ADDITION (PDF carries no tool-activity heading).
   */
  toolActivity: {
    heading: 'Tool activity',
    sectionAria: 'Tool activity',
    regionAria: 'Tool activity and approvals for the current run',
    listAria: 'Tool calls',
    empty: 'No tool activity yet — tool calls appear here when the model invokes a tool.',
    /** One label per declared `ToolOutcome` plus the neutral fallback. */
    outcomes: {
      pending: 'Pending',
      ok: 'OK',
      refused: 'Refused',
      denied: 'Denied',
      timeout: 'Timed out',
      cancelled: 'Cancelled',
      unknown: 'Unknown',
    },
    labels: {
      argsSha256: 'args_sha256',
      turn: 'turn',
      toolCallId: 'tool_call_id',
      code: 'code',
      taskId: 'task_id',
      bytes: 'bytes',
      sha256: 'sha256',
    },
    /** Honest grouping copy: the wire may omit the owning task on legacy rows. */
    unattributedTask: 'task unknown',
    calls: 'calls',
    unknownValue: '—',
  },
  /**
   * Server-authoritative rehydration strip. `snapshot.status`/`incomplete` were
   * computed by the store but never rendered; the banner is the only consumer.
   */
  snapshot: {
    aria: 'Live state status',
    confirming: 'Confirming live state with the server',
    degraded: 'Live state is unconfirmed',
    failed: 'Live state refresh failed',
    incomplete: 'missing',
    attempts: 'attempts',
    updated: 'updated',
    resync: 'Re-sync',
    resyncing: 'Re-syncing…',
  },
  diff: {
    title: 'Code-diff review',
    decisionRequired:
      'Decision required: the code-diff surface is proposal-gated (no PDF artboard, no frozen DIFF command). It is not built in v1.',
  },
  approval: {
    title: 'Tool approval requested',
    toolLabel: 'Tool',
    resolving: 'Resolving approval…',
    resolveFailed: 'Approval resolution failed',
    retry: 'Retry',
    dismiss: 'Dismiss',
    staleBanner:
      'Reconnected: pending approvals are being replayed. Keep this panel open until the server settles; do not approve an unconfirmed card.',
    resolveHint: 'APPROVAL_RESOLVE',
    /** Evidence labels keep the frozen wire field names (args_sha256, run_id, …). */
    fields: {
      argsSha256: 'args_sha256',
      argsPreview: 'args_preview',
      runId: 'run_id',
      taskId: 'task_id',
      toolCallId: 'tool_call_id',
      approvalId: 'approval_id',
      expires: 'expires',
    },
    /** Honest TTL display: unknown stays unknown, past stays expired. */
    expiry: {
      unknown: 'unknown',
      expired: 'expired',
      expiresIn: 'expires in',
    },
    unknownValue: '—',
    /** Pending-approval queue navigation (one dialog, N reachable records). */
    queue: {
      label: 'Approval',
      of: 'of',
      previous: 'Previous approval',
      next: 'Next approval',
      previousShort: 'Previous',
      nextShort: 'Next',
    },
    nonceMissing:
      'This APPROVAL_REQUEST did not carry the nonce required by the frozen APPROVAL_RESOLVE command, so it cannot be resolved from the UI. Report the missing nonce; do not invent one.',
    networkTargetMissing:
      'This network approval replayed without its target URL preview. Approve is disabled; re-request the approval rather than approving an unknown destination.',
  },
  node: {
    hyperAgent: 'HyperAgent',
    superAgent: 'SuperAgent',
    workerAgent: 'WorkerAgent',
    toolAgent: 'ToolAgent',
    errorState: 'Error state',
    role: 'role',
    roleCaption: 'decomposes intent, owns the sub-hive',
    workersMetric: 'workers',
    noToolOutput: 'tool output not measured',
    brokenRoute: 'broken route · error state',
    workers: 'workers',
    ctx: 'ctx',
    progress: 'progress',
    score: 'score',
    attempt: 'attempt',
    failed: 'Compile failed',
    toolLabel: 'TOOL',
    workerLabel: 'WORKER',
    running: 'RUNNING',
    noMetrics: '—',
  },
  seed: {
    superAgentRoot: 'Hive chat core',
    superAgentBackend: 'Backend Architecture',
    superAgentFrontend: 'Frontend Architecture',
    workerTailwind: 'tailwind-builder',
    workerSchema: 'db-schema',
    workerTest: 'agent.spec.ts',
    toolPrisma: 'Prisma Generator',
    errorPrismaMessage: 'Prisma client type mismatch on Run.sessionId',
    toolCode: 'schema.prisma',
  },
  /** Client-side refusal copy for codes that can be raised without a server frame. */
  clientRefusalUnknown: {
    message: 'Unrecognised server refusal',
    howToFix: 'Record the frame and report a contract gap; do not retry blindly.',
  },
  clientRefusals: {
    E_KEY_MISSING: {
      message: 'No provider key is configured on the local server.',
      howToFix:
        'Set the configured provider key in the server process environment (DEEPSEEK_API_KEY for DeepSeek), then restart the server.',
      retryable: true,
    },
    E_PROMPT_EMPTY: {
      message: 'The prompt is empty.',
      howToFix: 'Type a prompt before submitting (1-8000 characters).',
      retryable: true,
    },
    E_PROMPT_TOO_LONG: {
      message: 'The prompt exceeds 8000 characters.',
      howToFix: 'Shorten the prompt to 8000 characters or fewer.',
      retryable: true,
    },
    E_WS_DISCONNECTED: {
      message: 'The local server is unreachable.',
      howToFix: 'Start the HiveMind server (cargo run --release) and reload the UI.',
      retryable: true,
    },
    E_DB_SCHEMA_NEWER: {
      message: 'This database was written by a newer HiveMind build.',
      howToFix: 'Upgrade HiveMind or restore a backup made by this build; do not delete the database.',
      retryable: false,
    },
    E_PROVIDER_BALANCE: {
      message: 'The configured provider API account has insufficient balance.',
      howToFix: 'Top up the configured provider account or use a funded key; this call is never retried.',
      retryable: false,
    },
    E_SCHEMA_INVALID: {
      message: 'A frame did not match the frozen contract.',
      howToFix: 'Reload the UI; if it repeats, record the frame and restart the server.',
      retryable: true,
    },
    E_CALL_LIMIT: {
      message: 'The run reached its bounded provider/tool call budget.',
      howToFix:
        'The work was stopped honestly. Start a smaller run or raise the documented call budget; do not retry blindly.',
      retryable: false,
    },
    E_TOKEN_BUDGET_EXCEEDED: {
      message: 'The run reached its combined token budget before the next model call.',
      howToFix:
        'The work was stopped honestly with no partial reply. Start a smaller run, or raise the documented run token budget; the cap is a hard safety limit, not a model error.',
      retryable: false,
    },
    E_BUDGET_EXCEEDED: {
      message: 'The run reached its cost budget.',
      howToFix:
        'The work was stopped honestly. Start a smaller run or raise the documented run cost cap; do not retry blindly.',
      retryable: false,
    },
    E_APPROVAL_TIMEOUT: {
      message: 'The approval request expired before a decision was recorded.',
      howToFix:
        'No side effect was performed. Start the request again and approve or deny within the approval window.',
      retryable: true,
    },
    E_APPROVAL_NOT_FOUND: {
      message: 'That approval request no longer exists.',
      howToFix: 'Refresh the approval list; the request may have expired or been consumed.',
      retryable: false,
    },
    E_TOOL_NOT_ALLOWED: {
      message: 'That tool is not available to this agent tier.',
      howToFix: 'Use a tool from the tier capability list, or let the parent agent perform the call.',
      retryable: false,
    },
    E_TOOL_SCHEMA: {
      message: 'The tool arguments did not match the tool schema.',
      howToFix: 'Call the tool again with the documented required fields and no unknown fields.',
      retryable: true,
    },
    E_SPAWN_DEPTH: {
      message: 'That spawn would exceed the maximum hierarchy depth.',
      howToFix: 'Let the current agent do the work directly, or spawn from a shallower parent.',
      retryable: false,
    },
    E_FANOUT_LIMIT: {
      message: 'That parent already has the maximum number of children.',
      howToFix: 'Reuse an existing child or reduce the plan fan-out.',
      retryable: false,
    },
    E_AGENT_LIMIT: {
      message: 'The run reached its total agent cap.',
      howToFix: 'Reduce the plan size or continue in a new run.',
      retryable: false,
    },
    E_LLM_RATE_LIMIT: {
      message: 'The provider rate limit was hit (HTTP 429).',
      howToFix:
        'Wait for the provider window to reset, then retry. The server already retried a bounded number of times; reduce parallel tasks for a low-rate endpoint.',
      retryable: true,
    },
    E_PROVIDER_INVALID_RESPONSE: {
      message: 'The provider returned a response the documented contract cannot accept.',
      howToFix:
        'Check the configured model/base URL and the server log for the provider detail; change nothing until the response shape is confirmed.',
      retryable: false,
    },
    E_USAGE_INCOMPLETE: {
      message: 'The provider did not report enough usage to settle cost.',
      howToFix:
        'The run is incomplete and its cost is not claimed. Check the provider usage fields; do not treat the run as settled.',
      retryable: false,
    },
    E_COMMAND_UNKNOWN: {
      message: 'The server does not recognise this command.',
      howToFix: 'This UI build is out of date with the server contract; redeploy the matching bundle.',
      retryable: false,
    },
    E_ORIGIN_REJECTED: {
      message: 'The browser origin is not allowlisted for this server.',
      howToFix: 'Open the UI from http://127.0.0.1:8080 (or add the origin to HIVEROUTER_ALLOWED_ORIGINS).',
      retryable: false,
    },
    E_TOKEN_INVALID: {
      message: 'The per-session WebSocket token is missing or stale.',
      howToFix: 'Reload the UI so it fetches a fresh token from /api/session.',
      retryable: true,
    },
    E_ZOOM_RANGE: {
      message: 'Zoom is outside the supported 0.1x-3.0x range.',
      howToFix: 'Use the zoom controls; values are clamped to 0.1-3.0.',
      retryable: true,
    },
    E_RESYNC_REQUIRED: {
      message: 'The client missed events and must resnapshot.',
      howToFix: 'Wait for the reconnect/resnapshot to finish; do not retry mutations until synced.',
      retryable: true,
    },
    E_TASK_NOT_FOUND: {
      message: 'The selected task no longer exists on the server.',
      howToFix: 'Select a task that still exists; the graph may have changed.',
      retryable: false,
    },
    E_RUN_NOT_FOUND: {
      message: 'There is no active run to act on.',
      howToFix: 'Submit a prompt to create a run first.',
      retryable: true,
    },
    E_SESSION_NOT_FOUND: {
      message: 'That session no longer exists.',
      howToFix: 'Select an existing session or create a new one.',
      retryable: false,
    },
    E_JOB_NOT_FOUND: {
      message: 'That background job no longer exists.',
      howToFix: 'Refresh the job list and select an existing job id.',
      retryable: false,
    },
    E_RUN_TERMINAL: {
      message: 'The run is already terminal.',
      howToFix: 'Submit a new prompt; completed/cancelled/killed runs cannot be mutated.',
      retryable: false,
    },
    E_RUN_ACTIVE: {
      message: 'A run is already active in this session.',
      howToFix: 'Wait for the active run to finish or cancel it first.',
      retryable: true,
    },
    E_KILL_DENIED: {
      message: 'The worker cannot be killed from its current state.',
      howToFix: 'Only running workers can be killed; refresh the worker state.',
      retryable: false,
    },
    E_REMOTE_UNKNOWN: {
      message: 'The remote principal does not exist or has been retired.',
      howToFix: 'Refresh the Remote lane and choose an enrolled fixture remote.',
      retryable: false,
    },
    E_REMOTE_OFFLINE: {
      message: 'The remote is not online; the action was not dispatched.',
      howToFix: 'Wait for a heartbeat, then retry the action.',
      retryable: true,
    },
    E_REMOTE_CAPABILITY: {
      message: 'No grant covers this remote, device, capability and action.',
      howToFix: 'Grant the exact action to this remote before actuating.',
      retryable: false,
    },
    E_DEVICE_UNKNOWN: {
      message: 'The device is not registered to this remote.',
      howToFix: 'Refresh the remote device inventory.',
      retryable: false,
    },
    E_DEVICE_STATE_STALE: {
      message: 'The device state is stale or unknown.',
      howToFix: 'Refresh the device state before acting; stale actions are refused.',
      retryable: true,
    },
    E_DEVICE_ACTION_IN_FLIGHT: {
      message: 'Another action for this device is already in flight.',
      howToFix: 'Wait for the current result; devices are single-flight.',
      retryable: true,
    },
    E_DEVICE_APPROVAL_REQUIRED: {
      message: 'This critical device action requires operator approval.',
      howToFix: 'Resolve the pending approval with its nonce.',
      retryable: false,
    },
    E_DEVICE_APPROVAL_INVALID: {
      message: 'The device approval is expired, replayed or mismatched.',
      howToFix: 'Refresh the pending approval and resolve the current nonce once.',
      retryable: false,
    },
    E_REMOTE_TRANSPORT: {
      message: 'That remote transport is unavailable in this build.',
      howToFix: 'Use the in-process fixture adapter; network adapters are not enabled.',
      retryable: false,
    },
    E_DETACHED_CONFLICT: {
      message: 'That detached display name or request id is already in use.',
      howToFix: 'Choose a different display name or payload, or retry with the original request id.',
      retryable: false,
    },
    E_DETACHED_CAP: {
      message: 'The lower-level detached-principal cap is reached.',
      howToFix: 'Retire an existing detached principal or use the HyperAgent root-control path.',
      retryable: false,
    },
  },
} as const;

export type StringTable = typeof STRINGS;

/** Locale-stable number formatting; counts in the UI are derived, never typed. */
export function formatCount(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return STRINGS.node.noMetrics;
  }
  return new Intl.NumberFormat('en-US').format(value);
}

export function formatPercent(value: number | null | undefined, digits = 1): string {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return STRINGS.node.noMetrics;
  }
  return `${value.toFixed(digits)}%`;
}

export function formatDelta(value: number | null | undefined, digits = 1): string {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return STRINGS.node.noMetrics;
  }
  const sign = value >= 0 ? '+' : '';
  return `${sign}${value.toFixed(digits)}%`;
}

export function formatCtx(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return STRINGS.node.noMetrics;
  }
  return `${(value / 1000).toFixed(1)}k ${STRINGS.node.ctx}`;
}

export function formatStateOf(index: number, total: number): string {
  return `${STRINGS.scrubber.label} ${index + 1} ${STRINGS.scrubber.of} ${total}`;
}

export function refusalCopy(code: string): {
  message: string;
  howToFix: string;
  retryable: boolean;
} | null {
  const table: Record<string, { message: string; howToFix: string; retryable: boolean }> =
    STRINGS.clientRefusals;
  return table[code] ?? null;
}
