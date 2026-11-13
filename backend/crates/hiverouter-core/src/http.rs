//! Localhost HTTP surface: `GET /health`, `GET /api/session`, `GET /ws`, and an
//! honest 404 until the embedded web build exists.

use std::collections::BTreeMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Instant;

use axum::extract::{ConnectInfo, Path, Query, Request, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use hiverouter_contract::Refusal;

#[path = "health.rs"]
pub mod health;

/// Optional per-launch secret; when set, `/api/session` requires the matching
/// `x-hivemind-launch` header supplied by the launcher URL.
const LAUNCH_TOKEN_ENV: &str = "HIVEROUTER_LAUNCH_TOKEN";

fn configured_launch_token() -> Option<String> {
    std::env::var(LAUNCH_TOKEN_ENV)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn supplied_launch_token(headers: &HeaderMap) -> Option<&str> {
    headers
        .get("x-hivemind-launch")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
}

fn launch_token_matches(expected: Option<&str>, supplied: Option<&str>) -> bool {
    match expected {
        None => true,
        Some(expected) => supplied == Some(expected),
    }
}
use hiverouter_contract::{PermissionPolicy, RunId};
use hiverouter_provider::{ClientConfig, DeepSeekClient};
use hiverouter_tools::jail::WorkspaceJail;
use serde::Serialize;
use tokio::net::TcpListener;
use uuid::Uuid;

use crate::bus::EventBus;
use crate::checkpoint::CheckpointJournal;
use crate::checks::Check;
use crate::config::Config;
use crate::db::Database;
use crate::execution::{
    task_node_kind, ProviderExecutor, RunEngine, UnconfiguredExecutor,
};
use crate::graph::GraphAuthority;
use crate::history::HistoryReadModel;
use crate::jobs::{JobPolicy, JobRegistry, JobRequest};
use crate::prompts::{load_personas_embedded, Persona, TemplateRegistry};
use crate::provider_executor::ProviderExecutorAdapter;
use crate::remote::{
    Device, DeviceAction, NewDevice, NewGrant, NewRemote, RemoteAgent, RemoteAudit, RemoteGrant,
};
use crate::snapshot::SnapshotStore;
use crate::tools_bridge::ToolsBridge;
use crate::ws::{refusal_response, SessionAuth, ws_handler};

/// Shared server state.
#[derive(Debug)]
pub struct AppState {
    /// SQLite database (cache/metrics/usage/run rows).
    pub db: Arc<Database>,
    /// Environment checks ran at boot.
    pub checks: Vec<Check>,
    /// Process start time.
    pub started_at: Instant,
    /// Per-process identity issued at construction (never reused across boots).
    pub boot_id: String,
    /// Resolved configuration.
    pub config: Arc<Config>,
    /// Server event sequencer + fan-out.
    pub bus: Arc<EventBus>,
    /// The single authoritative graph owner.
    pub graph: Arc<GraphAuthority>,
    /// WebSocket session authentication.
    pub session: Arc<SessionAuth>,
    /// Run lifecycle engine.
    pub engine: Arc<RunEngine>,
    /// Remote control plane (tree-free principals, devices, actions).
    pub remote: Arc<crate::remote::RemoteStore>,
    /// Daemon/watchdog records (also tree-free; never task rows).
    pub daemons: Arc<crate::daemon::DaemonStore>,
    /// History / time-travel read model.
    pub history: Arc<HistoryReadModel>,
    /// Wired tools capabilities (settings, jailed files, approvals, diagnose).
    pub tools: Arc<ToolsBridge>,
    /// Background jobs owned by this server process, when the workspace opened.
    pub jobs: Option<Arc<JobRegistry>>,
    /// Named refusal when the embedded prompt assets failed to load.
    ///
    /// `None` means the planner template registry loaded from the binary's
    /// embedded assets. `Some(refusal)` means the boot path is serving with a
    /// required-and-failed `prompt_assets` check: `/health` reports 503 and the
    /// error code, and planner renders refuse by name â€” never a fake success.
    pub prompt_assets_error: Option<Refusal>,
}

/// Loads the shipped prompt templates embedded in the binary.
///
/// # Errors
/// Returns the named refusal (`E_TEMPLATE_INVALID`, `E_TEMPLATE_UNKNOWN`,
/// `E_TEMPLATE_LICENSE_MISSING`, `E_TEMPLATE_REF_FORBIDDEN`) when the embedded
/// manifest or a template body is missing/malformed.
pub fn load_prompt_assets() -> Result<Arc<TemplateRegistry>, Refusal> {
    TemplateRegistry::load_embedded().map(Arc::new)
}

/// Production prompt-asset load with an explicit failed boot check on error.
///
/// A missing/invalid asset must never degrade into an empty registry that
/// silently answers later with `E_TEMPLATE_UNKNOWN`: the refusal is recorded as
/// a required failed check (so `/health` is 503), logged, and kept on the state.
fn prompt_assets_for(checks: &mut Vec<Check>) -> (Arc<TemplateRegistry>, Option<Refusal>) {
    match load_prompt_assets() {
        Ok(templates) => (templates, None),
        Err(refusal) => {
            tracing::error!(
                code = refusal.code(),
                message = refusal.message(),
                "embedded prompt assets refused at boot; planner renders will refuse by name"
            );
            checks.push(Check {
                name: "prompt_assets",
                required: true,
                ok: false,
                detail: format!(
                    "embedded template assets refused: {} ({})",
                    refusal.message(),
                    refusal.code()
                ),
                code: Some(refusal.code()),
            });
            // The engine still needs a registry object; it is empty, but the
            // failure is explicit (required failed check + health code + log),
            // never silent. The render path refuses with E_TEMPLATE_UNKNOWN.
            (Arc::new(TemplateRegistry::default()), Some(refusal))
        }
    }
}

fn persona_assets_for(
    templates: &TemplateRegistry,
    checks: &mut Vec<Check>,
) -> Vec<Persona> {
    match load_personas_embedded(templates) {
        Ok(personas) => personas,
        Err(refusal) => {
            tracing::error!(
                code = refusal.code(),
                message = refusal.message(),
                "embedded persona assets refused at boot; scoped allowlists stay empty"
            );
            checks.push(Check {
                name: "persona_assets",
                required: true,
                ok: false,
                detail: format!(
                    "embedded persona assets refused: {} ({})",
                    refusal.message(),
                    refusal.code()
                ),
                code: Some(refusal.code()),
            });
            Vec::new()
        }
    }
}

fn merge_persisted_personas(
    db: &Arc<Database>,
    templates: &TemplateRegistry,
    mut personas: Vec<Persona>,
) -> Vec<Persona> {
    let store = crate::store::RunStore::new(db.clone());
    let persisted = match store.agent_profiles() {
        Ok(profiles) => profiles,
        Err(error) => {
            tracing::warn!(
                code = error.code(),
                "persisted agent profiles refused; embedded profiles only"
            );
            return personas;
        }
    };
    for (id, value, source) in persisted {
        let persona: Persona = match serde_json::from_value(value) {
            Ok(persona) => persona,
            Err(_) => {
                tracing::warn!(profile_id = %id, %source, "persisted profile is invalid JSON");
                continue;
            }
        };
        if persona.id != id {
            tracing::warn!(
                profile_id = %id,
                profile_inner_id = %persona.id,
                "persisted profile id does not match its row id"
            );
            continue;
        }
        if let Err(refusal) = persona.validate(templates) {
            tracing::warn!(
                profile_id = %id,
                code = refusal.code(),
                "persisted profile failed validation; embedded profile retained"
            );
            continue;
        }
        if let Some(existing) = personas.iter_mut().find(|existing| existing.id == id) {
            *existing = persona;
        } else {
            personas.push(persona);
        }
    }
    personas
}

fn tools_migration_check(db: &Database, checks: &mut Vec<Check>) {
    if let Err(error) = ToolsBridge::migrate(db) {
        tracing::error!(
            code = error.code(),
            "tools approval-table migration failed; required health check fails"
        );
        checks.push(Check {
            name: "tools_migration",
            required: true,
            ok: false,
            detail: format!(
                "tools approval-table migration failed: {} ({})",
                error.message(),
                error.code()
            ),
            code: Some(error.code()),
        });
    }
}

fn build_tools(db: &Arc<Database>, config: &Config) -> Arc<ToolsBridge> {
    let now_unix_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0);
    match ToolsBridge::expire_pending_approvals(db, now_unix_secs) {
        Ok(0) => {}
        Ok(expired) => tracing::warn!(
            expired,
            "consumed restart-orphaned pending approvals at boot; no waiter survives restart"
        ),
        Err(error) => tracing::error!(
            code = error.code(),
            "approval boot reaper failed; restart-orphaned rows may remain pending (no in-process waiter survives)"
        ),
    }
    match db.with_connection(|connection| crate::jobs::reconcile_running_jobs(connection)) {
        Ok(0) => {}
        Ok(reconciled) => tracing::warn!(
            reconciled,
            "marked restart-orphaned background jobs failed; no old child handle survives"
        ),
        Err(error) => tracing::error!(
            "job boot reconciliation failed; persisted jobs may claim running without an owner: {error}"
        ),
    }
    let bridge =
        ToolsBridge::new_with_data_dir(&config.workspace_root, &config.data_dir, db.clone())
            .with_side_effects_enabled(config.side_effects_enabled);
    let bridge = match CheckpointJournal::open(db.clone(), &config.workspace_root) {
        Ok(journal) => bridge.with_checkpoint_journal(journal),
        Err(error) => {
            tracing::warn!(
                code = error.code(),
                "checkpoint journal unavailable; approved file mutations will not be revertible"
            );
            bridge
        }
    };
    Arc::new(bridge)
}

/// Builds the production executor. Missing key or disabled provider falls back
/// to the honest `E_KEY_MISSING` executor; it never fabricates replies.
fn build_executor(
    config: &Config,
    personas: &[Persona],
    templates: &TemplateRegistry,
) -> Arc<dyn ProviderExecutor> {
    if !config.provider.enabled {
        tracing::info!("provider disabled by configuration; live prompts refuse E_KEY_MISSING");
        return Arc::new(UnconfiguredExecutor);
    }
    let prompts = persona_system_prompts(personas, templates);
    let allowlists = personas
        .iter()
        .map(|persona| (persona.id.clone(), persona.allowed_tool_ids.clone()))
        .collect::<std::collections::HashMap<_, _>>();
    match ProviderExecutorAdapter::from_env(config.provider.clone()).map(|adapter| {
        adapter
            .with_persona_prompts(prompts)
            .with_persona_tool_allowlists(allowlists)
    }) {
        Ok(executor) => Arc::new(executor),
        Err(error) => {
            tracing::warn!(
                code = error.code(),
                "provider key/client unavailable; live prompts refuse by name"
            );
            Arc::new(UnconfiguredExecutor)
        }
    }
}

/// Renders each persona's template and keeps its system prompt, so leaf and
/// tool-turn calls use the persona's actual identity instead of one generic
/// prompt for every role. A malformed/missing template simply leaves that role
/// on the honest generic fallback; the planner already validates templates.
fn persona_system_prompts(
    personas: &[Persona],
    templates: &TemplateRegistry,
) -> std::collections::HashMap<String, String> {
    let inputs = BTreeMap::new();
    let mut prompts = std::collections::HashMap::new();
    for persona in personas {
        if let Ok(messages) = templates.render(&persona.template_id, &inputs) {
            if let Some(system) = messages
                .into_iter()
                .find(|message| message.role == crate::prompts::MessageRole::System)
            {
                prompts.insert(persona.id.clone(), system.content);
            }
        }
    }
    prompts
}


/// Builds the shared engine, wiring the production workspace jail when the
/// configured root is usable. A missing/invalid root leaves the jail absent so
/// read-only tool calls refuse E_FILE_TREE_UNAVAILABLE instead of touching disk.
fn build_engine(
    db: Arc<Database>,
    bus: Arc<EventBus>,
    graph: Arc<GraphAuthority>,
    config: &Config,
    templates: Arc<TemplateRegistry>,
    personas: Vec<Persona>,
    snapshots: Arc<SnapshotStore>,
    tools: Arc<ToolsBridge>,
    wire_workspace_jail: bool,
) -> Arc<RunEngine> {
    let remote = Arc::new(crate::remote::RemoteStore::new(db.clone()));
    let daemons = Arc::new(crate::daemon::DaemonStore::new(db.clone()));
    let repo_context = db
        .with_connection_refusal(|connection| {
            crate::repo_index::context_block(connection, &config.workspace_root).map_err(|error| {
                tracing::warn!(
                    workspace_root = %config.workspace_root.display(),
                    %error,
                    "repository index unavailable; planning prompts will not include workspace context"
                );
                hiverouter_contract::Refusal::InternalError
            })
        })
        .ok();
    let skills_context = match crate::skills::load_skills(&config.data_dir.join("skills")) {
        Ok(skills) => {
            let block = crate::skills::prompt_block(&skills);
            if block.trim().is_empty() {
                None
            } else {
                tracing::info!(skills = skills.len(), "loaded local skills into prompt data");
                Some(block)
            }
        }
        Err(error) => {
            tracing::warn!(%error, "local skills unavailable; prompts continue without them");
            None
        }
    };
    let db_path = db.path().to_path_buf();
    let mut engine = RunEngine::new(db, bus, graph, build_executor(config, &personas, &templates), templates)
        .with_personas(personas)
        .with_snapshot_store(snapshots)
        .with_tools(tools)
        .with_remote_store(remote)
        .with_daemon_store(daemons)
        .with_root_orchestrator()
        .with_repo_context(repo_context)
        .with_skills_context(skills_context)
        .with_authorized_lessons(crate::execution::load_authorized_lessons(
            &config.data_dir.join("evolution").join("LESSONS.jsonl"),
        ));
    if wire_workspace_jail {
        match WorkspaceJail::open(&config.workspace_root) {
            Ok(jail) => {
                let mut jail = jail;
                for denied in ToolsBridge::data_denials(
                    &config.workspace_root,
                    &config.data_dir,
                    &db_path,
                ) {
                    jail = jail.deny_path(denied);
                }
                engine = engine.with_tool_workspace(jail);
            }
            Err(error) => {
                tracing::warn!(
                    workspace_root = %config.workspace_root.display(),
                    code = error.code(),
                    "workspace jail unavailable; read-only model tool calls will refuse E_FILE_TREE_UNAVAILABLE"
                );
            }
        }
        // Production bounded pool for independently-ready tasks. Test services
        // keep the historical serial path so scripted event sequences remain
        // deterministic. A local env override lets a low-rate provider be tested
        // without changing the compiled default.
        let parallel = std::env::var("HIVEROUTER_PARALLEL_TASKS")
            .ok()
            .and_then(|value| value.trim().parse::<usize>().ok())
            .unwrap_or(32)
            .clamp(1, 64);
        // External OpenAI-compatible endpoints commonly publish a low per-key
        // rate limit (the live Token Lab endpoint allows 20 req/min). Do not
        // let a DeepSeek-sized local pool turn one plan into a 429 storm; the
        // operator can still lower this with the env override.
        let parallel = if config.provider.is_external_endpoint() {
            parallel.min(2)
        } else {
            parallel
        };
        engine = engine.with_parallel_dispatch(parallel);
    }
    Arc::new(engine)
}

/// Publishes the boot-recovery settlement frames once the bus exists.
///
/// `main.rs` performs `recover_on_boot` before `AppState` is built (the bus does
/// not exist yet), so the settlement `NODE_STATE` frames are published here as a
/// catch-up from the persisted recovery outcome. It mutates nothing and is a
/// no-op on a clean boot (no interrupted run, no recovered task).
fn publish_recovery_catch_up(db: &Arc<Database>, graph: &GraphAuthority, bus: &EventBus) {
    match crate::recovery::publish_recovered_node_states(db.clone(), graph, bus) {
        Ok(_) => {}
        Err(error) => tracing::warn!(
            code = error.code(),
            "boot catch-up could not publish recovered node states"
        ),
    }
}

impl AppState {
    /// Builds state for a running server with a fresh event bus, graph and
    /// session token derived from the configuration.
    ///
    /// Loads the embedded prompt templates (production path). A load failure is
    /// recorded as a required failed `prompt_assets` check and the named refusal
    /// on the state; it never degrades silently.
    #[must_use]
    pub fn new(db: Arc<Database>, mut checks: Vec<Check>, config: Arc<Config>) -> Self {
        let session = Arc::new(SessionAuth::new(
            Uuid::now_v7().to_string(),
            config.allowed_origins.clone(),
        ));
        let bus = Arc::new(EventBus::new());
        let graph = Arc::new(GraphAuthority::new());
        let snapshots = Arc::new(SnapshotStore::new(&config.data_dir, db.clone()));
        let (templates, prompt_assets_error) = prompt_assets_for(&mut checks);
        let personas = merge_persisted_personas(
            &db,
            &templates,
            persona_assets_for(&templates, &mut checks),
        );
        tools_migration_check(&db, &mut checks);
        let tools = build_tools(&db, &config);
        let engine = build_engine(db.clone(), bus.clone(), graph.clone(), &config, templates, personas, snapshots.clone(), tools.clone(), true);
        let history = Arc::new(HistoryReadModel::new(db.clone(), (*snapshots).clone()));
        let remote = Arc::new(crate::remote::RemoteStore::new(db.clone()));
        let daemons = Arc::new(crate::daemon::DaemonStore::new(db.clone()));
        let jobs = JobRegistry::open(&config.workspace_root, JobPolicy::conservative_default())
            .ok()
            .map(|jobs| Arc::new(jobs.with_persistence(db.clone())));
        publish_recovery_catch_up(&db, &graph, &bus);
        Self {
            db,
            checks,
            started_at: Instant::now(),
            boot_id: Uuid::now_v7().to_string(),
            config,
            bus,
            graph,
            session,
            engine,
            remote,
            daemons,
            history,
            tools,
            jobs,
            prompt_assets_error,
        }
    }

    /// Builds state with an explicitly recovered graph (boot path).
    ///
    /// This is the binary's construction path: it loads the embedded prompt
    /// templates and records a named refusal (`prompt_assets_error` + required
    /// failed check) if they are missing/invalid.
    #[must_use]
    pub fn with_graph(
        db: Arc<Database>,
        mut checks: Vec<Check>,
        config: Arc<Config>,
        graph: Arc<GraphAuthority>,
    ) -> Self {
        let session = Arc::new(SessionAuth::new(
            Uuid::now_v7().to_string(),
            config.allowed_origins.clone(),
        ));
        let bus = Arc::new(EventBus::new());
        let snapshots = Arc::new(SnapshotStore::new(&config.data_dir, db.clone()));
        let (templates, prompt_assets_error) = prompt_assets_for(&mut checks);
        let personas = merge_persisted_personas(
            &db,
            &templates,
            persona_assets_for(&templates, &mut checks),
        );
        tools_migration_check(&db, &mut checks);
        let tools = build_tools(&db, &config);
        let engine = build_engine(db.clone(), bus.clone(), graph.clone(), &config, templates, personas, snapshots.clone(), tools.clone(), true);
        let history = Arc::new(HistoryReadModel::new(db.clone(), (*snapshots).clone()));
        let remote = Arc::new(crate::remote::RemoteStore::new(db.clone()));
        let daemons = Arc::new(crate::daemon::DaemonStore::new(db.clone()));
        let jobs = JobRegistry::open(&config.workspace_root, JobPolicy::conservative_default())
            .ok()
            .map(|jobs| Arc::new(jobs.with_persistence(db.clone())));
        publish_recovery_catch_up(&db, &graph, &bus);
        Self {
            db,
            checks,
            started_at: Instant::now(),
            boot_id: Uuid::now_v7().to_string(),
            config,
            bus,
            graph,
            session,
            engine,
            remote,
            daemons,
            history,
            tools,
            jobs,
            prompt_assets_error,
        }
    }

    /// Builds state with explicitly supplied services (integration tests).
    ///
    /// Test-only seam: it keeps the documented empty `TemplateRegistry::default()`
    /// so unit/integration tests can build an engine without assets. Production
    /// uses [`AppState::new`] / [`AppState::with_graph`], which load the embedded
    /// planner template and record a named refusal on failure.
    #[must_use]
    pub fn with_services(
        db: Arc<Database>,
        mut checks: Vec<Check>,
        config: Arc<Config>,
        bus: Arc<EventBus>,
        graph: Arc<GraphAuthority>,
        session: Arc<SessionAuth>,
    ) -> Self {
        let snapshots = Arc::new(SnapshotStore::new(&config.data_dir, db.clone()));
        tools_migration_check(&db, &mut checks);
        let tools = build_tools(&db, &config);
        let engine = build_engine(db.clone(), bus.clone(), graph.clone(), &config, Arc::new(TemplateRegistry::default()), Vec::new(), snapshots.clone(), tools.clone(), false);
        let history = Arc::new(HistoryReadModel::new(db.clone(), (*snapshots).clone()));
        let remote = Arc::new(crate::remote::RemoteStore::new(db.clone()));
        let daemons = Arc::new(crate::daemon::DaemonStore::new(db.clone()));
        let jobs = JobRegistry::open(&config.workspace_root, JobPolicy::conservative_default())
            .ok()
            .map(|jobs| Arc::new(jobs.with_persistence(db.clone())));
        Self {
            db,
            checks,
            started_at: Instant::now(),
            boot_id: Uuid::now_v7().to_string(),
            config,
            bus,
            graph,
            session,
            engine,
            remote,
            daemons,
            history,
            tools,
            jobs,
            prompt_assets_error: None,
        }
    }

    /// Whether the production prompt templates loaded successfully.
    #[must_use]
    pub fn prompt_assets_loaded(&self) -> bool {
        self.prompt_assets_error.is_none()
    }
}

/// Health payload.
#[derive(Debug, Serialize)]
pub struct HealthResponse {
    /// `ok` (all checks passed), `degraded` (optional check failed) or
    /// `failing` (required check failed).
    pub status: &'static str,
    /// Crate version.
    pub version: &'static str,
    /// Contract schema id.
    pub schema: &'static str,
    /// Process uptime in seconds.
    pub uptime_secs: u64,
    /// OS process id (diagnostic identity; not a stable machine identifier).
    pub pid: u32,
    /// Per-process identity issued at boot.
    pub boot_id: String,
    /// Effective data directory.
    pub data_dir: String,
    /// Effective file-tool workspace root.
    pub workspace_root: String,
    /// Environment checks.
    pub checks: Vec<Check>,
    /// Database health.
    pub database: DatabaseHealth,
    /// Transport counters.
    pub transport: TransportHealth,
    /// Embedded prompt-asset load status (planner template registry).
    pub prompt_assets: PromptAssetsHealth,
}

/// Database portion of the health payload.
#[derive(Debug, Serialize)]
pub struct DatabaseHealth {
    /// Database file path (`:memory:` for in-memory databases).
    pub path: String,
    /// Applied schema version, when readable.
    pub schema_version: Option<i64>,
    /// Named refusal code when the database is not readable.
    pub error_code: Option<&'static str>,
}

/// Transport portion of the health payload.
#[derive(Debug, Serialize)]
pub struct TransportHealth {
    /// Current server event sequence.
    pub last_seq: u64,
    /// Graph nodes in memory.
    pub graph_nodes: Option<usize>,
    /// Origin allowlist size (the values themselves stay out of the payload).
    pub allowed_origins: usize,
}

/// Prompt-asset portion of the health payload (production boot proof).
#[derive(Debug, Serialize)]
pub struct PromptAssetsHealth {
    /// True when the embedded planner template registry loaded at boot.
    pub loaded: bool,
    /// Named refusal code when the embedded assets were missing or invalid.
    pub error_code: Option<&'static str>,
}

/// Builds the server router.
#[must_use]
pub fn build_router(state: Arc<AppState>) -> Router {
    let guard_state = Arc::clone(&state);
    let auth_state = Arc::clone(&state);
    Router::new()
        .route("/health", get(health))
        .route("/diagnose", get(diagnose))
        .route("/api/runtime", get(runtime_status))
        .route("/api/logs", get(logs))
        .route("/api/session", get(api_session))
        .route("/api/skills", get(skills_list))
        .route("/api/skills/{skill_id}", get(skill_detail))
        .route("/api/sessions", get(sessions_list).post(session_create))
        .route("/api/sessions/{session_id}", get(session_detail))
        .route(
            "/api/sessions/{session_id}/resume",
            get(session_resume),
        )
        .route(
            "/api/sessions/{session_id}/archive",
            post(session_archive),
        )
        .route(
            "/api/sessions/{session_id}/unarchive",
            post(session_unarchive),
        )
        .route(
            "/api/sessions/{session_id}/todos",
            get(session_todos).post(session_todo_add),
        )
        .route(
            "/api/sessions/{session_id}/todos/{todo_id}/state",
            post(session_todo_state),
        )
        .route("/api/sessions/{session_id}/policy", post(session_policy))
        .route("/api/history/runs", get(history_runs))
        .route("/api/history/runs/{run_id}/revisions", get(history_revisions))
        .route("/api/history/runs/{run_id}/state", get(history_state))
        .route("/api/history/runs/{run_id}/messages", get(history_messages))
        .route("/api/agents/runs/{run_id}/messages", get(agent_messages))
        .route(
            "/api/agents/runs/{run_id}/messages/{message_id}/answer",
            post(answer_agent_message),
        )
        .route("/api/agents/runs/{run_id}/roster", get(agent_roster))
        .route("/api/agents/runs/{run_id}/memory", get(agent_memory_for_run))
        .route("/api/memory/banks", get(memory_banks))
        .route("/api/memory/summary", get(memory_summary))
        .route(
            "/api/memory/banks/{bank_id}/entries",
            get(memory_bank_entries),
        )
        .route("/api/memory/clear", post(clear_run_memory))
        .route("/api/memory/prune", post(prune_run_memory))
        .route("/api/memory/trust", post(promote_memory))
        .route("/api/memory/journal", get(memory_journal))
        .route("/api/memory/journal/verify", get(verify_memory_journal))
        .route(
            "/api/agents/runs/{run_id}/proposals",
            get(run_evolution_proposals),
        )
        .route(
            "/api/agents/runs/{run_id}/proposals/{proposal_id}/authorize",
            post(authorize_evolution_proposal),
        )
        .route(
            "/api/agents/runs/{run_id}/proposals/{proposal_id}/apply",
            post(apply_evolution_lesson),
        )
        .route(
            "/api/agents/runs/{run_id}/proposals/{proposal_id}/rollback",
            post(rollback_evolution_lesson),
        )
        .route("/api/evolution/lessons", get(evolution_lessons))
        .route(
            "/api/learning/candidates",
            get(learning_candidates).post(learning_candidate_create),
        )
        .route(
            "/api/learning/candidates/{candidate_id}/activate",
            post(learning_activate),
        )
        .route(
            "/api/learning/candidates/{candidate_id}/rollback",
            post(learning_rollback),
        )
        .route("/api/a2a/pending", get(a2a_pending))
        .route("/api/a2a/messages", get(a2a_messages))
        .route("/api/remotes", get(remote_list).post(remote_create))
        .route("/api/remotes/{remote_id}", get(remote_detail))
        .route("/api/remotes/{remote_id}/revoke", post(remote_revoke))
        .route(
            "/api/remotes/{remote_id}/heartbeat",
            post(remote_heartbeat),
        )
        .route(
            "/api/remotes/{remote_id}/devices",
            post(remote_device_create),
        )
        .route("/api/remotes/{remote_id}/grants", post(remote_grant_create))
        .route("/api/remotes/{remote_id}/audit", get(remote_audit_list))
        .route("/api/daemons", get(daemon_list).post(daemon_create))
        .route("/api/daemons/{daemon_id}", get(daemon_detail))
        .route("/api/daemons/{daemon_id}/switch", post(daemon_switch))
        .route("/api/daemons/{daemon_id}/retire", post(daemon_retire))
        .route("/api/daemons/{daemon_id}/reports", get(daemon_reports))
        .route("/api/supervisor", get(supervisor_status))
        .route("/api/devices/{device_id}", get(remote_device_get))
        .route("/api/devices/{device_id}/actions", post(remote_action_create))
        .route(
            "/api/device-actions/{action_id}/resolve",
            post(remote_action_resolve),
        )
        .route("/api/approvals", get(pending_approvals))
        .route("/api/templates", get(templates))
        .route("/api/workspace", get(workspace))
        .route("/api/runs/active", get(active_run))
        .route("/api/agents/profiles", get(agent_profiles))
        .route("/api/agents/profiles/import", post(import_agent_profile))
        .route("/api/agents/profiles/{profile_id}", delete(delete_agent_profile))
        .route("/api/agents/memory", get(agent_memory_list))
        .route("/api/agents/memory/{agent_key}", delete(clear_agent_memory))
        .route("/api/usage/runs/{run_id}", get(run_usage))
        .route("/api/runs/{run_id}/detail", get(run_detail))
        .route("/api/runs/{run_id}/checkpoints", get(run_checkpoints))
        .route(
            "/api/runs/{run_id}/checkpoints/{seq}",
            get(run_checkpoint),
        )
        .route(
            "/api/runs/{run_id}/checkpoints/{seq}/restore",
            post(restore_run_checkpoint),
        )
        .route(
            "/api/runs/{run_id}/checkpoints/revert",
            post(revert_run_checkpoints),
        )
        .route("/api/jobs", get(jobs_list).post(job_start))
        .route("/api/jobs/{job_id}", get(job_detail))
        .route("/api/jobs/{job_id}/kill", post(job_kill))
        .route("/api/eval/runs", get(eval_runs_list))
        .route("/api/eval/run", post(eval_run))
        .route(
            "/api/eval/suites",
            get(eval_suites_list).post(eval_suite_create),
        )
        .route("/api/eval/suites/{suite_id}/run", post(eval_suite_run))
        .route("/api/eval/metrics", get(eval_metrics))
        .route("/api/memory/context/{task_id}", get(task_memory_context))
        .route("/api/projects", get(projects))
        .route("/api/provider/models", get(provider_models))
        .route("/api/provider/balance", get(provider_balance))
        .route(
            "/api/provider/profile",
            get(provider_profile_get).post(provider_profile_set),
        )
        .route("/ws", get(ws_handler))
        .fallback(crate::static_ui::static_ui)
        .layer(middleware::from_fn_with_state(guard_state, host_guard))
        .layer(middleware::from_fn_with_state(auth_state, mutation_auth))
        .with_state(state)
}

/// Rejects a request whose `Host` header is not this loopback server.
/// Origin checks do not stop DNS rebinding: a rebinding page can issue a
/// same-origin request with a hostile `Host`. The Host header is therefore the
/// server's own identity check, applied before any route runs.
async fn host_guard(
    State(state): State<Arc<AppState>>,
    request: Request,
    next: Next,
) -> Response {
    let host = request
        .headers()
        .get(header::HOST)
        .and_then(|value| value.to_str().ok());
    if !host_allowed(&state.config.bind, host) {
        return (
            StatusCode::MISDIRECTED_REQUEST,
            Json(serde_json::json!({
                "code": Refusal::OriginRejected.code(),
                "message": "the Host header is not this loopback HiveMind server",
                "how_to_fix": "open HiveMind through its launcher on 127.0.0.1/localhost",
            })),
        )
            .into_response();
    }
    next.run(request).await
}

/// Requires the per-boot session token on every mutating `/api/*` request.
///
/// Read routes stay loopback/Host-gated for now; mutations can change profiles,
/// memory, approvals, or run state, so they must not be driveable by an
/// unauthenticated local page or process.
async fn mutation_auth(
    State(state): State<Arc<AppState>>,
    request: Request,
    next: Next,
) -> Response {
    let launch_gate = configured_launch_token();
    if let Some(expected) = launch_gate {
        if request.method() == axum::http::Method::GET
            && request.uri().path().starts_with("/api/")
            && request.uri().path() != "/api/session"
        {
            let supplied = supplied_launch_token(request.headers());
            if !launch_token_matches(Some(expected.as_str()), supplied) {
                return (
                    StatusCode::FORBIDDEN,
                    [(header::CACHE_CONTROL, "no-store")],
                    Json(serde_json::json!({
                        "code": Refusal::OriginRejected.code(),
                        "message": "this read route requires the launcher token",
                        "how_to_fix": "open HiveMind through HiveMind.exe or start-hivemind.ps1",
                    })),
                )
                    .into_response();
            }
        }
    }
    if matches!(request.method().as_str(), "GET" | "HEAD" | "OPTIONS") {
        return next.run(request).await;
    }
    let supplied = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(str::trim)
        .or_else(|| {
            request
                .headers()
                .get("x-hivemind-token")
                .and_then(|value| value.to_str().ok())
                .map(str::trim)
        });
    if supplied == Some(state.session.token()) {
        return next.run(request).await;
    }
    (
        StatusCode::UNAUTHORIZED,
        [(header::CACHE_CONTROL, "no-store")],
        Json(serde_json::json!({
            "code": Refusal::TokenInvalid.code(),
            "message": "this HTTP mutation requires the per-boot HiveMind session token",
            "how_to_fix": "send Authorization: Bearer <session token> or x-hivemind-token",
        })),
    )
        .into_response()
}

/// Whether a `Host` header names this bound loopback server.
///
/// Production binds a fixed port and requires the exact loopback authority.
/// Test harnesses bind port 0 and receive the real port only after binding, so
/// they accept any loopback authority; they never accept a non-loopback host.
fn host_allowed(bind: &SocketAddr, host: Option<&str>) -> bool {
    let Some(host) = host.map(str::trim).filter(|host| !host.is_empty()) else {
        return false;
    };
    if bind.port() == 0 {
        return loopback_host(host);
    }
    let expected = [
        format!("127.0.0.1:{}", bind.port()),
        format!("localhost:{}", bind.port()),
        format!("[::1]:{}", bind.port()),
    ];
    expected.iter().any(|allowed| host.eq_ignore_ascii_case(allowed))
}

fn loopback_host(host: &str) -> bool {
    let lower = host.to_ascii_lowercase();
    lower == "127.0.0.1"
        || lower == "localhost"
        || lower == "[::1]"
        || lower.starts_with("127.0.0.1:")
        || lower.starts_with("localhost:")
        || lower.starts_with("[::1]:")
}

/// `GET /diagnose` â€” tools-owned derived diagnostics (no secrets).
async fn diagnose(State(state): State<Arc<AppState>>) -> Response {
    match state.tools.diagnostics() {
        Ok(mut value) => {
            if let Some(object) = value.as_object_mut() {
                object.insert(
                    "side_effects_enabled".to_owned(),
                    serde_json::Value::Bool(state.config.side_effects_enabled),
                );
                object.insert(
                    "uptime_secs".to_owned(),
                    serde_json::json!(state.started_at.elapsed().as_secs()),
                );
                object.insert("pid".to_owned(), serde_json::json!(std::process::id()));
                object.insert("boot_id".to_owned(), serde_json::json!(state.boot_id));
            }
            (StatusCode::OK, Json(value)).into_response()
        }
        Err(refusal) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "code": refusal.code(),
                "message": refusal.message(),
                "how_to_fix": refusal.how_to_fix(),
                "retryable": refusal.retryable(),
            })),
        )
            .into_response(),
    }
}

/// `GET /api/logs?source=launcher|stdout|stderr|audit&tail=N`.
///
/// Origin/peer gate and launcher-token gate (the latter runs in
/// [`mutation_auth`] for every `/api/*` GET) precede any file access. The
/// handler accepts only the four whitelisted sources; the path itself is never
/// caller-controlled. Bodies are capped at 64 KiB and redacted by
/// [`health::read_log_tail_with_secrets`]. `Cache-Control: no-store` is set on
/// every outcome so log material is never cached by a browser or proxy.
async fn logs(
    State(state): State<Arc<AppState>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Query(params): Query<BTreeMap<String, String>>,
) -> Response {
    let origin = headers
        .get(header::ORIGIN)
        .and_then(|value| value.to_str().ok());
    let origin_allowed = match origin {
        Some(origin) => state.session.is_origin_allowed(Some(origin)),
        // Same-origin fetches normally carry no Origin header; only a loopback
        // peer may use that path.
        None => peer.ip().is_loopback(),
    };
    if !origin_allowed {
        return log_route_error(
            StatusCode::FORBIDDEN,
            Refusal::OriginRejected,
            "the request Origin is not the local HiveMind UI",
        );
    }
    if let Some(unknown) = params
        .keys()
        .find(|key| !matches!(key.as_str(), "source" | "tail"))
    {
        return log_route_error(
            StatusCode::BAD_REQUEST,
            Refusal::SchemaInvalid,
            &format!("unknown query parameter {unknown:?}; accepted: source, tail"),
        );
    }
    let Some(source_value) = params.get("source") else {
        return log_route_error(
            StatusCode::BAD_REQUEST,
            Refusal::SchemaInvalid,
            "source is required: launcher, stdout, stderr or audit",
        );
    };
    let Some(source) = health::LogSource::parse(source_value.trim()) else {
        return log_route_error(
            StatusCode::BAD_REQUEST,
            Refusal::SchemaInvalid,
            "source must be exactly one of: launcher, stdout, stderr, audit",
        );
    };
    let tail = match params.get("tail") {
        None => health::DEFAULT_TAIL_LINES,
        Some(raw) => match raw.trim().parse::<usize>() {
            Ok(value) if value > 0 => value,
            _ => {
                return log_route_error(
                    StatusCode::BAD_REQUEST,
                    Refusal::SchemaInvalid,
                    "tail must be a positive integer line count",
                );
            }
        },
    };
    let extra_secrets = [state.session.token().to_owned()];
    match health::read_log_tail_with_secrets(&state.config.data_dir, source, tail, &extra_secrets) {
        Ok(body) => (
            StatusCode::OK,
            [(header::CACHE_CONTROL, "no-store")],
            Json(body),
        )
            .into_response(),
        Err(error) => {
            let status = match error.refusal() {
                Refusal::PathNotFound => StatusCode::NOT_FOUND,
                Refusal::PathOutsideJail => StatusCode::FORBIDDEN,
                Refusal::InternalError => StatusCode::INTERNAL_SERVER_ERROR,
                _ => StatusCode::BAD_REQUEST,
            };
            log_route_error(status, error.refusal(), error.detail())
        }
    }
}

/// JSON refusal body for the log route, always `no-store`.
fn log_route_error(status: StatusCode, refusal: Refusal, detail: &str) -> Response {
    (
        status,
        [(header::CACHE_CONTROL, "no-store")],
        Json(serde_json::json!({
            "code": refusal.code(),
            "message": refusal.message(),
            "how_to_fix": refusal.how_to_fix(),
            "retryable": refusal.retryable(),
            "detail": detail,
        })),
    )
        .into_response()
}

async fn skills_list(State(state): State<Arc<AppState>>) -> Response {
    match crate::skills::load_skills(&state.config.data_dir.join("skills")) {
        Ok(skills) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "skills": skills
                    .iter()
                    .map(|skill| serde_json::json!({
                        "id": skill.id,
                        "name": skill.name,
                        "description": skill.description,
                    }))
                    .collect::<Vec<_>>()
            })),
        )
            .into_response(),
        Err(_) => refusal_response(StatusCode::SERVICE_UNAVAILABLE, &Refusal::InternalError),
    }
}

async fn skill_detail(
    State(state): State<Arc<AppState>>,
    Path(skill_id): Path<String>,
) -> Response {
    match crate::skills::load_skills(&state.config.data_dir.join("skills")) {
        Ok(skills) => match skills.into_iter().find(|skill| skill.id == skill_id) {
            Some(skill) => (StatusCode::OK, Json(skill)).into_response(),
            None => refusal_response(StatusCode::NOT_FOUND, &Refusal::PathNotFound),
        },
        Err(_) => refusal_response(StatusCode::SERVICE_UNAVAILABLE, &Refusal::InternalError),
    }
}

async fn sessions_list(
    State(state): State<Arc<AppState>>,
    Query(params): Query<std::collections::BTreeMap<String, String>>,
) -> Response {
    let include_archived = params
        .get("include_archived")
        .is_some_and(|value| value == "true" || value == "1");
    let store = crate::store::RunStore::new(state.db.clone());
    match store.sessions_with_archived(include_archived) {
        Ok(sessions) => (StatusCode::OK, Json(serde_json::json!({ "sessions": sessions }))).into_response(),
        Err(refusal) => refusal_response(StatusCode::SERVICE_UNAVAILABLE, &refusal),
    }
}

async fn session_resume(
    State(state): State<Arc<AppState>>,
    Path(session_id): Path<String>,
) -> Response {
    let store = crate::store::RunStore::new(state.db.clone());
    let session = match store.session(&session_id) {
        Ok(session) => session,
        Err(refusal) => return refusal_response(StatusCode::NOT_FOUND, &refusal),
    };
    if session.archived_at.is_some() {
        return refusal_response(StatusCode::CONFLICT, &Refusal::DecisionRequired);
    }
    let runs = match store.runs_for_session(&session_id) {
        Ok(runs) => runs,
        Err(refusal) => return refusal_response(StatusCode::SERVICE_UNAVAILABLE, &refusal),
    };
    let latest = runs.first().map(|run| {
        serde_json::json!({
            "run_id": run.id.to_string(),
            "status": run.status.as_str(),
            "permission_policy": run.permission_policy.as_str(),
            "created_at": run.created_at.as_str(),
            "updated_at": run.updated_at.as_str(),
        })
    });
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "session": session,
            "latest_run": latest,
            "resume": {
                "mode": "new_run_same_session",
                "note": "The engine has no mid-run resume. Resume means submit a new run with this session_id; the session's policy and todos carry over."
            }
        })),
    )
        .into_response()
}

async fn session_archive(
    State(state): State<Arc<AppState>>,
    Path(session_id): Path<String>,
) -> Response {
    let store = crate::store::RunStore::new(state.db.clone());
    match store.set_session_archived(&session_id, true) {
        Ok(session) => (StatusCode::OK, Json(session)).into_response(),
        Err(Refusal::SessionNotFound) => refusal_response(StatusCode::NOT_FOUND, &Refusal::SessionNotFound),
        Err(refusal) => refusal_response(StatusCode::SERVICE_UNAVAILABLE, &refusal),
    }
}

async fn session_unarchive(
    State(state): State<Arc<AppState>>,
    Path(session_id): Path<String>,
) -> Response {
    let store = crate::store::RunStore::new(state.db.clone());
    match store.set_session_archived(&session_id, false) {
        Ok(session) => (StatusCode::OK, Json(session)).into_response(),
        Err(Refusal::SessionNotFound) => refusal_response(StatusCode::NOT_FOUND, &Refusal::SessionNotFound),
        Err(refusal) => refusal_response(StatusCode::SERVICE_UNAVAILABLE, &refusal),
    }
}

async fn session_create(
    State(state): State<Arc<AppState>>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let Some(name) = body.get("name").and_then(serde_json::Value::as_str) else {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
    };
    let Some(project_id) = body.get("project_id").and_then(serde_json::Value::as_str) else {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
    };
    let Some(workspace_root) = body
        .get("workspace_root")
        .and_then(serde_json::Value::as_str)
    else {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
    };
    if name.trim().is_empty() || project_id.trim().is_empty() || workspace_root.trim().is_empty() {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
    }
    let policy = match body.get("permission_policy") {
        None => PermissionPolicy::Classic,
        Some(value) => match value
            .as_str()
            .and_then(PermissionPolicy::from_str_strict)
        {
            Some(policy) => policy,
            None => {
                return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
            }
        },
    };
    let store = crate::store::RunStore::new(state.db.clone());
    match store.create_session(project_id, name, workspace_root, policy) {
        Ok(session) => (StatusCode::CREATED, Json(session)).into_response(),
        Err(refusal) => refusal_response(StatusCode::SERVICE_UNAVAILABLE, &refusal),
    }
}

async fn session_detail(
    State(state): State<Arc<AppState>>,
    Path(session_id): Path<String>,
) -> Response {
    let store = crate::store::RunStore::new(state.db.clone());
    let session = match store.session(&session_id) {
        Ok(session) => session,
        Err(refusal) => return refusal_response(StatusCode::NOT_FOUND, &refusal),
    };
    let runs = match store.runs_for_session(&session.id) {
        Ok(runs) => runs
            .into_iter()
            .map(|run| {
                serde_json::json!({
                    "run_id": run.id.to_string(),
                    "project_id": run.project_id,
                    "status": run.status.as_str(),
                    "permission_policy": run.permission_policy.as_str(),
                    "created_at": run.created_at.as_str(),
                })
            })
            .collect::<Vec<_>>(),
        Err(refusal) => return refusal_response(StatusCode::SERVICE_UNAVAILABLE, &refusal),
    };
    (
        StatusCode::OK,
        Json(serde_json::json!({ "session": session, "runs": runs })),
    )
        .into_response()
}

async fn session_todos(
    State(state): State<Arc<AppState>>,
    Path(session_id): Path<String>,
) -> Response {
    let store = crate::store::RunStore::new(state.db.clone());
    if let Err(refusal) = store.session(&session_id) {
        return refusal_response(StatusCode::NOT_FOUND, &refusal);
    }
    match store.todos(&session_id) {
        Ok(todos) => (StatusCode::OK, Json(serde_json::json!({ "todos": todos }))).into_response(),
        Err(refusal) => refusal_response(StatusCode::SERVICE_UNAVAILABLE, &refusal),
    }
}

async fn session_todo_add(
    State(state): State<Arc<AppState>>,
    Path(session_id): Path<String>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let Some(text) = body.get("text").and_then(serde_json::Value::as_str) else {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
    };
    let text = text.trim();
    if text.is_empty() || text.chars().count() > 4_000 {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
    }
    let store = crate::store::RunStore::new(state.db.clone());
    match store.add_todo(&session_id, text) {
        Ok(todo) => (StatusCode::CREATED, Json(todo)).into_response(),
        Err(Refusal::DecisionRequired) => {
            refusal_response(StatusCode::CONFLICT, &Refusal::DecisionRequired)
        }
        Err(Refusal::SessionNotFound) => {
            refusal_response(StatusCode::NOT_FOUND, &Refusal::SessionNotFound)
        }
        Err(refusal) => refusal_response(StatusCode::BAD_REQUEST, &refusal),
    }
}

async fn session_todo_state(
    State(state): State<Arc<AppState>>,
    Path((session_id, todo_id)): Path<(String, String)>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let Some(state_name) = body.get("state").and_then(serde_json::Value::as_str) else {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
    };
    let store = crate::store::RunStore::new(state.db.clone());
    match store.set_todo_state(&session_id, &todo_id, state_name) {
        Ok(todo) => (StatusCode::OK, Json(todo)).into_response(),
        Err(Refusal::DecisionRequired) => {
            refusal_response(StatusCode::NOT_FOUND, &Refusal::DecisionRequired)
        }
        Err(Refusal::SchemaInvalid) => {
            refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid)
        }
        Err(refusal) => refusal_response(StatusCode::SERVICE_UNAVAILABLE, &refusal),
    }
}

async fn session_policy(
    State(state): State<Arc<AppState>>,
    Path(session_id): Path<String>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let Some(policy) = body
        .get("permission_policy")
        .and_then(serde_json::Value::as_str)
        .and_then(PermissionPolicy::from_str_strict)
    else {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
    };
    let store = crate::store::RunStore::new(state.db.clone());
    match store.set_session_policy(&session_id, policy) {
        Ok(()) => match store.session(&session_id) {
            Ok(session) => (StatusCode::OK, Json(session)).into_response(),
            Err(refusal) => refusal_response(StatusCode::NOT_FOUND, &refusal),
        },
        Err(Refusal::DecisionRequired) => {
            refusal_response(StatusCode::CONFLICT, &Refusal::DecisionRequired)
        }
        Err(refusal) => refusal_response(StatusCode::NOT_FOUND, &refusal),
    }
}

fn checkpoint_journal(state: &AppState) -> Result<CheckpointJournal, Response> {
    CheckpointJournal::open(state.db.clone(), &state.config.workspace_root)
        .map_err(|error| refusal_response(StatusCode::SERVICE_UNAVAILABLE, &error.refusal()))
}

fn require_run(state: &AppState, run_id: RunId) -> Result<(), Response> {
    let store = crate::store::RunStore::new(state.db.clone());
    store
        .get_run(run_id)
        .map(|_| ())
        .map_err(|error| refusal_response(StatusCode::NOT_FOUND, &error))
}

async fn run_checkpoints(
    State(state): State<Arc<AppState>>,
    Path(run_raw): Path<String>,
) -> Response {
    let Ok(run_id) = run_raw.parse::<RunId>() else {
        return refusal_response(StatusCode::NOT_FOUND, &Refusal::RunNotFound);
    };
    if let Err(response) = require_run(&state, run_id) {
        return response;
    }
    let journal = match checkpoint_journal(&state) {
        Ok(journal) => journal,
        Err(response) => return response,
    };
    match journal.entries_for_run(run_id) {
        Ok(entries) => (
            StatusCode::OK,
            Json(serde_json::json!({ "run_id": run_id.to_string(), "checkpoints": entries })),
        )
            .into_response(),
        Err(error) => refusal_response(StatusCode::NOT_FOUND, &error.refusal()),
    }
}

async fn run_checkpoint(
    State(state): State<Arc<AppState>>,
    Path((run_raw, seq_raw)): Path<(String, u64)>,
) -> Response {
    let Ok(run_id) = run_raw.parse::<RunId>() else {
        return refusal_response(StatusCode::NOT_FOUND, &Refusal::RunNotFound);
    };
    if let Err(response) = require_run(&state, run_id) {
        return response;
    }
    let journal = match checkpoint_journal(&state) {
        Ok(journal) => journal,
        Err(response) => return response,
    };
    match journal.get(run_id, seq_raw) {
        Ok(Some(entry)) => (StatusCode::OK, Json(entry)).into_response(),
        Ok(None) => refusal_response(StatusCode::NOT_FOUND, &Refusal::RunNotFound),
        Err(error) => refusal_response(StatusCode::NOT_FOUND, &error.refusal()),
    }
}

async fn restore_run_checkpoint(
    State(state): State<Arc<AppState>>,
    Path((run_raw, seq)): Path<(String, u64)>,
) -> Response {
    let Ok(run_id) = run_raw.parse::<RunId>() else {
        return refusal_response(StatusCode::NOT_FOUND, &Refusal::RunNotFound);
    };
    if let Err(response) = require_run(&state, run_id) {
        return response;
    }
    let journal = match checkpoint_journal(&state) {
        Ok(journal) => journal,
        Err(response) => return response,
    };
    match journal.restore_entry(run_id, seq) {
        Ok(restored) => (StatusCode::OK, Json(restored)).into_response(),
        Err(error) => refusal_response(StatusCode::CONFLICT, &error.refusal()),
    }
}

async fn revert_run_checkpoints(
    State(state): State<Arc<AppState>>,
    Path(run_raw): Path<String>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let Ok(run_id) = run_raw.parse::<RunId>() else {
        return refusal_response(StatusCode::NOT_FOUND, &Refusal::RunNotFound);
    };
    let Some(sequence) = body.get("sequence").and_then(serde_json::Value::as_u64) else {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
    };
    if let Err(response) = require_run(&state, run_id) {
        return response;
    }
    let journal = match checkpoint_journal(&state) {
        Ok(journal) => journal,
        Err(response) => return response,
    };
    match journal.restore_to(run_id, sequence) {
        Ok(outcome) => (StatusCode::OK, Json(outcome)).into_response(),
        Err(error) => refusal_response(StatusCode::CONFLICT, &error.refusal()),
    }
}

fn persisted_jobs(
    state: &Arc<AppState>,
    limit: u32,
) -> Result<Vec<crate::jobs::JobSnapshot>, Refusal> {
    state.db.with_connection_refusal(|connection| {
        crate::jobs::list_persisted_jobs(connection, limit)
            .map_err(|error| hiverouter_tools::ToolError::from(error).refusal())
    })
}

fn persist_job_snapshot(
    state: &Arc<AppState>,
    snapshot: &crate::jobs::JobSnapshot,
) -> Result<(), Refusal> {
    state.db.with_connection_refusal(|connection| {
        crate::jobs::persist_job(connection, snapshot)
            .map_err(|error| hiverouter_tools::ToolError::from(error).refusal())
    })
}

async fn jobs_list(State(state): State<Arc<AppState>>) -> Response {
    let Some(jobs) = state.jobs.as_ref() else {
        return refusal_response(StatusCode::SERVICE_UNAVAILABLE, &Refusal::FileTreeUnavailable);
    };
    let mut all = jobs.list_all();
    let known: std::collections::HashSet<String> =
        all.iter().map(|job| job.id.clone()).collect();
    match persisted_jobs(&state, 200) {
        Ok(persisted) => {
            all.extend(persisted.into_iter().filter(|job| !known.contains(&job.id)));
        }
        Err(error) => {
            tracing::warn!(code = error.code(), "persisted job history unavailable");
        }
    }
    all.sort_by(|left, right| {
        right
            .started_at
            .cmp(&left.started_at)
            .then_with(|| right.id.cmp(&left.id))
    });
    let truncated = all.len() > 200;
    all.truncate(200);
    (
        StatusCode::OK,
        Json(serde_json::json!({ "jobs": all, "truncated": truncated })),
    )
        .into_response()
}

async fn job_start(
    State(state): State<Arc<AppState>>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let Some(jobs) = state.jobs.as_ref() else {
        return refusal_response(StatusCode::SERVICE_UNAVAILABLE, &Refusal::FileTreeUnavailable);
    };
    let Some(program) = body.get("program").and_then(serde_json::Value::as_str) else {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
    };
    let Some(args) = body.get("args").and_then(serde_json::Value::as_array) else {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
    };
    let Some(args) = args
        .iter()
        .map(|value| value.as_str().map(ToOwned::to_owned))
        .collect::<Option<Vec<_>>>()
    else {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
    };
    let Some(session_id) = body
        .get("session_id")
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|session_id| !session_id.is_empty() && session_id.len() <= 160)
    else {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
    };
    // HTTP jobs are operator-initiated but still bound to a real, active
    // session; an arbitrary string must not create an unattributable job.
    let store = crate::store::RunStore::new(state.db.clone());
    match store.session(session_id) {
        Ok(session) if session.archived_at.is_some() => {
            return refusal_response(StatusCode::CONFLICT, &Refusal::DecisionRequired);
        }
        Ok(_) => {}
        Err(Refusal::SessionNotFound) => {
            return refusal_response(StatusCode::NOT_FOUND, &Refusal::SessionNotFound);
        }
        Err(refusal) => {
            return refusal_response(StatusCode::SERVICE_UNAVAILABLE, &refusal);
        }
    }
    let mut request = JobRequest::new(session_id, program, args);
    if let Some(cwd) = body.get("cwd").and_then(serde_json::Value::as_str) {
        request = request.with_cwd(cwd);
    }
    match jobs.start(request).await {
        Ok(snapshot) => {
            if let Err(error) = persist_job_snapshot(&state, &snapshot) {
                tracing::warn!(code = error.code(), job_id = %snapshot.id, "job started but could not be persisted");
            }
            (StatusCode::CREATED, Json(snapshot)).into_response()
        }
        Err(error) => refusal_response(StatusCode::CONFLICT, &error.refusal()),
    }
}

async fn job_detail(
    State(state): State<Arc<AppState>>,
    Path(job_id): Path<String>,
) -> Response {
    let Some(jobs) = state.jobs.as_ref() else {
        return refusal_response(StatusCode::SERVICE_UNAVAILABLE, &Refusal::FileTreeUnavailable);
    };
    match jobs.get(&job_id) {
        Ok(snapshot) => (StatusCode::OK, Json(snapshot)).into_response(),
        Err(error) => match persisted_jobs(&state, 1_000) {
            Ok(jobs) => match jobs.into_iter().find(|job| job.id == job_id) {
                Some(snapshot) => (StatusCode::OK, Json(snapshot)).into_response(),
                None => refusal_response(StatusCode::NOT_FOUND, &error.refusal()),
            },
            // A store read failure is not evidence that the job is absent.
            Err(read_error) => {
                refusal_response(StatusCode::SERVICE_UNAVAILABLE, &read_error)
            }
        },
    }
}

async fn job_kill(
    State(state): State<Arc<AppState>>,
    Path(job_id): Path<String>,
) -> Response {
    let Some(jobs) = state.jobs.as_ref() else {
        return refusal_response(StatusCode::SERVICE_UNAVAILABLE, &Refusal::FileTreeUnavailable);
    };
    match jobs.kill(&job_id).await {
        Ok(snapshot) => {
            if let Err(error) = persist_job_snapshot(&state, &snapshot) {
                tracing::warn!(code = error.code(), job_id = %snapshot.id, "killed job could not be persisted");
            }
            (StatusCode::OK, Json(snapshot)).into_response()
        }
        Err(crate::jobs::JobError::UnknownJob(_)) => {
            refusal_response(StatusCode::NOT_FOUND, &Refusal::JobNotFound)
        }
        Err(error) => refusal_response(StatusCode::CONFLICT, &error.refusal()),
    }
}

async fn eval_suites_list(State(state): State<Arc<AppState>>) -> Response {
    match state.db.with_connection(|connection| crate::eval::list_suites(connection, 100)) {
        Ok(suites) => (StatusCode::OK, Json(serde_json::json!({ "suites": suites }))).into_response(),
        Err(error) => {
            tracing::warn!("eval suite list failed: {error}");
            refusal_response(StatusCode::SERVICE_UNAVAILABLE, &Refusal::InternalError)
        }
    }
}

async fn eval_suite_create(
    State(state): State<Arc<AppState>>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let Some(name) = body.get("name").and_then(serde_json::Value::as_str).map(str::trim) else {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
    };
    let Some(cases) = body.get("cases").and_then(serde_json::Value::as_array) else {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
    };
    if name.is_empty() || name.chars().count() > 120 || cases.is_empty() || cases.len() > 32 {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
    }
    let mut parsed = Vec::with_capacity(cases.len());
    for case in cases {
        let Some(case_id) = case.get("case_id").and_then(serde_json::Value::as_str).map(str::trim) else {
            return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
        };
        let Some(program) = case.get("program").and_then(serde_json::Value::as_str).map(str::trim) else {
            return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
        };
        let Some(args) = case.get("args").and_then(serde_json::Value::as_array) else {
            return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
        };
        let Some(args) = args
            .iter()
            .map(|value| value.as_str().map(ToOwned::to_owned))
            .collect::<Option<Vec<_>>>()
        else {
            return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
        };
        let expected_exit = match case.get("expected_exit_code") {
            None => 0,
            Some(value) => match value.as_i64().and_then(|value| i32::try_from(value).ok()) {
                Some(value) if (-1..=255).contains(&value) => value,
                _ => return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid),
            },
        };
        if case_id.is_empty()
            || case_id.chars().count() > 120
            || program.is_empty()
            || program.len() > 120
        {
            return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
        }
        parsed.push(crate::eval::EvalCase {
            case_id: case_id.to_owned(),
            program: program.to_owned(),
            args,
            expected_exit,
        });
    }
    let suite = crate::eval::EvalSuite {
        id: uuid::Uuid::now_v7().to_string(),
        name: name.to_owned(),
        description: body
            .get("description")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .chars()
            .take(400)
            .collect(),
        cases: parsed,
        created_at: hiverouter_contract::Timestamp::now().as_str().to_owned(),
    };
    match state.db.with_connection(|connection| crate::eval::create_suite(connection, &suite)) {
        Ok(()) => (StatusCode::CREATED, Json(suite)).into_response(),
        Err(error) => {
            tracing::warn!("eval suite create refused: {error}");
            refusal_response(StatusCode::CONFLICT, &Refusal::InternalError)
        }
    }
}

async fn eval_suite_run(
    State(state): State<Arc<AppState>>,
    Path(suite_id): Path<String>,
) -> Response {
    let Some(jobs) = state.jobs.as_ref() else {
        return refusal_response(StatusCode::SERVICE_UNAVAILABLE, &Refusal::FileTreeUnavailable);
    };
    let suite = match state.db.with_connection(|connection| crate::eval::get_suite(connection, &suite_id)) {
        Ok(Some(suite)) => suite,
        Ok(None) => return refusal_response(StatusCode::NOT_FOUND, &Refusal::PathNotFound),
        Err(error) => {
            tracing::warn!("eval suite read failed: {error}");
            return refusal_response(StatusCode::SERVICE_UNAVAILABLE, &Refusal::InternalError);
        }
    };
    let mut receipts = Vec::with_capacity(suite.cases.len());
    for case in &suite.cases {
        let started = match jobs
            .start(crate::jobs::JobRequest::new("eval", case.program.clone(), case.args.clone()))
            .await
        {
            Ok(snapshot) => snapshot,
            Err(error) => return refusal_response(StatusCode::CONFLICT, &error.refusal()),
        };
        let terminal = match jobs.wait(&started.id).await {
            Ok(snapshot) => snapshot,
            Err(error) => return refusal_response(StatusCode::CONFLICT, &error.refusal()),
        };
        let sha = hiverouter_tools::sha256::sha256_hex(terminal.output.tail.as_bytes());
        let receipt = crate::eval::receipt_from_observation(
            &suite.id,
            &case.case_id,
            &case.program,
            &case.args,
            case.expected_exit,
            &terminal.id,
            terminal.state.as_str(),
            terminal.exit_code,
            &sha,
            terminal.output.bytes,
        );
        if let Err(error) = state.db.with_connection(|connection| {
            crate::eval::record_eval(connection, &receipt)
        }) {
            tracing::error!("eval suite receipt could not be stored: {error}");
            return refusal_response(StatusCode::INTERNAL_SERVER_ERROR, &Refusal::InternalError);
        }
        receipts.push(receipt);
    }
    let metrics = match state.db.with_connection(|connection| {
        crate::eval::suite_metrics(connection, &suite_id)
    }) {
        Ok(metrics) => metrics,
        Err(_) => return refusal_response(StatusCode::SERVICE_UNAVAILABLE, &Refusal::InternalError),
    };
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "suite_id": suite_id,
            "receipts": receipts,
            "metrics": metrics,
        })),
    )
        .into_response()
}

async fn eval_metrics(
    State(state): State<Arc<AppState>>,
    Query(params): Query<std::collections::BTreeMap<String, String>>,
) -> Response {
    let Some(suite_id) = params.get("suite_id").map(String::as_str).filter(|value| !value.is_empty()) else {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
    };
    let exists = match state.db.with_connection(|connection| {
        crate::eval::get_suite(connection, suite_id)
    }) {
        Ok(Some(_)) => true,
        Ok(None) => false,
        Err(_) => return refusal_response(StatusCode::SERVICE_UNAVAILABLE, &Refusal::InternalError),
    };
    if !exists {
        return refusal_response(StatusCode::NOT_FOUND, &Refusal::PathNotFound);
    }
    match state.db.with_connection(|connection| {
        crate::eval::suite_metrics(connection, suite_id)
    }) {
        Ok(metrics) => (StatusCode::OK, Json(metrics)).into_response(),
        Err(_) => refusal_response(StatusCode::SERVICE_UNAVAILABLE, &Refusal::InternalError),
    }
}

async fn learning_candidates(State(state): State<Arc<AppState>>) -> Response {
    match state.db.with_connection(|connection| {
        crate::learning::list_candidates(connection, 100)
    }) {
        Ok(candidates) => (
            StatusCode::OK,
            Json(serde_json::json!({ "candidates": candidates })),
        )
            .into_response(),
        Err(error) => {
            tracing::warn!("lesson candidate list failed: {error}");
            refusal_response(StatusCode::SERVICE_UNAVAILABLE, &Refusal::InternalError)
        }
    }
}

async fn learning_candidate_create(
    State(state): State<Arc<AppState>>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let field = |name: &str, max: usize| {
        body.get(name)
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty() && value.chars().count() <= max)
            .map(ToOwned::to_owned)
    };
    let Some(candidate_key) = field("candidate_key", 120) else {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
    };
    let Some(title) = field("title", 400) else {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
    };
    let Some(body_text) = field("body", 8_000) else {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
    };
    let Some(baseline_eval_id) = field("baseline_eval_id", 80) else {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
    };
    let Some(candidate_eval_id) = field("candidate_eval_id", 80) else {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
    };
    if baseline_eval_id == candidate_eval_id {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
    }
    let candidate = crate::learning::LessonCandidate {
        id: uuid::Uuid::now_v7().to_string(),
        candidate_key,
        title,
        body: body_text,
        baseline_eval_id,
        candidate_eval_id,
        status: "pending".to_owned(),
        created_at: hiverouter_contract::Timestamp::now().as_str().to_owned(),
        activated_at: None,
        rolled_back_at: None,
    };
    match state.db.with_connection(|connection| {
        crate::learning::create_candidate(connection, &candidate)
    }) {
        Ok(()) => (StatusCode::CREATED, Json(candidate)).into_response(),
        Err(error) => {
            tracing::warn!("lesson candidate refused: {error}");
            refusal_response(StatusCode::CONFLICT, &Refusal::InternalError)
        }
    }
}

async fn learning_activate(
    State(state): State<Arc<AppState>>,
    Path(candidate_id): Path<String>,
) -> Response {
    let path = evolution_lesson_path(&state);
    let outcome = state.db.with_connection(|connection| {
        let candidate = crate::learning::get_candidate(connection, &candidate_id)?;
        let Some(candidate) = candidate else {
            return Ok(None);
        };
        if candidate.status != "pending"
            || !crate::learning::evals_passed(
                connection,
                &candidate.baseline_eval_id,
                &candidate.candidate_eval_id,
            )?
        {
            return Ok(None);
        }
        Ok(Some(candidate))
    });
    let candidate = match outcome {
        Ok(Some(candidate)) => candidate,
        Ok(None) => return refusal_response(StatusCode::CONFLICT, &Refusal::DecisionRequired),
        Err(error) => {
            tracing::warn!("lesson activation read failed: {error}");
            return refusal_response(StatusCode::SERVICE_UNAVAILABLE, &Refusal::InternalError);
        }
    };
    let receipt_sha256 = match state.db.with_connection(|connection| {
        connection.query_row(
            "SELECT output_sha256 FROM eval_runs WHERE id = ?1",
            rusqlite::params![candidate.candidate_eval_id],
            |row| row.get::<_, String>(0),
        )
    }) {
        Ok(hash) => hash,
        Err(_) => return refusal_response(StatusCode::SERVICE_UNAVAILABLE, &Refusal::InternalError),
    };
    // DB activation first; the ledger append is the outbox step. If the append
    // fails, compensate the DB so a later boot cannot admit a line for a
    // candidate that never became active.
    let active = match state.db.with_connection(|connection| {
        crate::learning::activate_candidate(connection, &candidate_id)
    }) {
        Ok(Some(active)) => active,
        Ok(None) => return refusal_response(StatusCode::CONFLICT, &Refusal::DecisionRequired),
        Err(error) => {
            tracing::warn!("lesson activation write failed: {error}");
            return refusal_response(StatusCode::SERVICE_UNAVAILABLE, &Refusal::InternalError);
        }
    };
    let line = crate::learning::applied_lesson_line(&active, &receipt_sha256);
    if let Err(refusal) = append_lesson_line(&path, &line) {
        let _ = state.db.with_connection(|connection| {
            crate::learning::reset_activation(connection, &candidate_id)
        });
        return refusal_response(StatusCode::SERVICE_UNAVAILABLE, &refusal);
    }
    state.engine.reload_authorized_lessons(&path);
    (StatusCode::OK, Json(active)).into_response()
}

async fn learning_rollback(
    State(state): State<Arc<AppState>>,
    Path(candidate_id): Path<String>,
) -> Response {
    let path = evolution_lesson_path(&state);
    let candidate = match state.db.with_connection(|connection| {
        crate::learning::get_candidate(connection, &candidate_id)
    }) {
        Ok(Some(candidate)) if candidate.status == "active" => candidate,
        Ok(_) => return refusal_response(StatusCode::CONFLICT, &Refusal::DecisionRequired),
        Err(error) => {
            tracing::warn!("lesson rollback read failed: {error}");
            return refusal_response(StatusCode::SERVICE_UNAVAILABLE, &Refusal::InternalError);
        }
    };
    // Tombstone first: if the ledger cannot be written, do not claim rollback.
    if let Err(refusal) = append_lesson_line(&path, &crate::learning::rolled_back_line(&candidate)) {
        return refusal_response(StatusCode::SERVICE_UNAVAILABLE, &refusal);
    }
    match state.db.with_connection(|connection| {
        crate::learning::rollback_candidate(connection, &candidate_id)
    }) {
        Ok(Some(candidate)) => {
            state.engine.reload_authorized_lessons(&path);
            (StatusCode::OK, Json(candidate)).into_response()
        }
        Ok(None) => refusal_response(StatusCode::CONFLICT, &Refusal::DecisionRequired),
        Err(error) => {
            tracing::warn!("lesson rollback failed: {error}");
            refusal_response(StatusCode::SERVICE_UNAVAILABLE, &Refusal::InternalError)
        }
    }
}

fn append_lesson_line(path: &std::path::Path, line: &str) -> Result<(), Refusal> {
    use std::io::Write as _;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|_| Refusal::InternalError)?;
    }
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|_| Refusal::InternalError)?;
    writeln!(file, "{line}").map_err(|_| Refusal::InternalError)
}

async fn eval_runs_list(State(state): State<Arc<AppState>>) -> Response {
    let receipts = match state.db.with_connection(|connection| {
        crate::eval::list_eval(connection, 100)
    }) {
        Ok(receipts) => receipts,
        Err(error) => {
            tracing::warn!("eval receipt list failed: {error}");
            return refusal_response(StatusCode::SERVICE_UNAVAILABLE, &Refusal::InternalError);
        }
    };
    (StatusCode::OK, Json(serde_json::json!({ "eval_runs": receipts }))).into_response()
}

async fn eval_run(
    State(state): State<Arc<AppState>>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let Some(jobs) = state.jobs.as_ref() else {
        return refusal_response(StatusCode::SERVICE_UNAVAILABLE, &Refusal::FileTreeUnavailable);
    };
    let Some(suite_id) = body.get("suite_id").and_then(serde_json::Value::as_str).map(str::trim) else {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
    };
    let Some(case_id) = body.get("case_id").and_then(serde_json::Value::as_str).map(str::trim) else {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
    };
    let Some(program) = body.get("program").and_then(serde_json::Value::as_str) else {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
    };
    let Some(args) = body.get("args").and_then(serde_json::Value::as_array) else {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
    };
    let Some(args) = args
        .iter()
        .map(|value| value.as_str().map(ToOwned::to_owned))
        .collect::<Option<Vec<_>>>()
    else {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
    };
    let expected_exit = match body.get("expected_exit_code") {
        None => 0,
        Some(value) => match value.as_i64().and_then(|value| i32::try_from(value).ok()) {
            Some(value) if (-1..=255).contains(&value) => value,
            _ => return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid),
        },
    };
    if suite_id.is_empty()
        || case_id.is_empty()
        || suite_id.len() > 120
        || case_id.len() > 120
        || program.trim().is_empty()
    {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
    }
    let request = crate::jobs::JobRequest::new("eval", program, args.clone());
    let started = match jobs.start(request).await {
        Ok(snapshot) => snapshot,
        Err(error) => return refusal_response(StatusCode::CONFLICT, &error.refusal()),
    };
    let terminal = match jobs.wait(&started.id).await {
        Ok(snapshot) => snapshot,
        Err(error) => return refusal_response(StatusCode::CONFLICT, &error.refusal()),
    };
    let output_sha256 = hiverouter_tools::sha256::sha256_hex(terminal.output.tail.as_bytes());
    let state_label = terminal.state.as_str();
    let receipt = crate::eval::receipt_from_observation(
        suite_id,
        case_id,
        program,
        &args,
        expected_exit,
        &terminal.id,
        state_label,
        terminal.exit_code,
        &output_sha256,
        terminal.output.bytes,
    );
    let persisted = state.db.with_connection(|connection| {
        crate::eval::record_eval(connection, &receipt)
    });
    if let Err(error) = persisted {
        tracing::error!("eval receipt could not be stored: {error}");
        return refusal_response(StatusCode::INTERNAL_SERVER_ERROR, &Refusal::InternalError);
    }
    (StatusCode::CREATED, Json(receipt)).into_response()
}

async fn history_runs(State(state): State<Arc<AppState>>) -> Response {
    match state.history.runs(200) {
        Ok(runs) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "runs": runs.iter().map(|run| serde_json::json!({
                    "run_id": run.id.to_string(),
                    "project_id": run.project_id,
                    "status": run.status.as_str(),
                    "error_code": run.error_code,
                    "created_at": run.created_at.as_str(),
                })).collect::<Vec<_>>(),
            })),
        )
            .into_response(),
        Err(refusal) => refusal_response(StatusCode::SERVICE_UNAVAILABLE, &refusal),
    }
}

async fn history_revisions(
    State(state): State<Arc<AppState>>,
    Path(run_id): Path<String>,
) -> Response {
    let Ok(run_id) = run_id.parse::<hiverouter_contract::RunId>() else {
        return refusal_response(StatusCode::NOT_FOUND, &hiverouter_contract::Refusal::RunNotFound);
    };
    match state.history.time_travel(run_id) {
        Ok(view) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "run_id": view.run_id.to_string(),
                "latest_revision": view.latest_revision,
                "revision_count": view.revision_count(),
                "revisions": view.revisions.iter().map(|meta| serde_json::json!({
                    "revision": meta.revision,
                    "created_at": meta.created_at.as_str(),
                    "run_status": meta.run_status.map(|state| state.as_str()),
                })).collect::<Vec<_>>(),
            })),
        )
            .into_response(),
        Err(refusal) => refusal_response(StatusCode::NOT_FOUND, &refusal),
    }
}

async fn history_state(
    State(state): State<Arc<AppState>>,
    Path(run_id): Path<String>,
    Query(query): Query<std::collections::HashMap<String, String>>,
) -> Response {
    let Ok(run_id) = run_id.parse::<hiverouter_contract::RunId>() else {
        return refusal_response(StatusCode::NOT_FOUND, &hiverouter_contract::Refusal::RunNotFound);
    };
    let Some(revision) = query.get("revision").and_then(|value| value.parse::<u64>().ok()) else {
        return refusal_response(StatusCode::BAD_REQUEST, &hiverouter_contract::Refusal::SchemaInvalid);
    };
    match state.history.state_at(run_id, revision) {
        Ok(snapshot) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "run_id": snapshot.run_id.to_string(),
                "revision": snapshot.revision,
                "schema_version": snapshot.schema_version,
                "run_status": snapshot.run_status.map(|state| state.as_str()),
                "graph": history_graph_value(&state, run_id, snapshot.graph),
            })),
        )
            .into_response(),
        Err(refusal) if refusal == hiverouter_contract::Refusal::HistoryGap => {
            refusal_response(StatusCode::NOT_FOUND, &refusal)
        }
        Err(refusal) => refusal_response(StatusCode::CONFLICT, &refusal),
    }
}

/// The one run-scoped, identity-enriched graph projection used by both the
/// HTTP history read and the WebSocket replay event.
///
/// The stored snapshot is topology-only and may predate run-scoped capture.
/// Filter by the run's task rows, then add role/kind/label from the authority.
pub(crate) fn history_graph_value(
    state: &AppState,
    run_id: hiverouter_contract::RunId,
    mut graph: crate::graph::GraphSnapshot,
) -> serde_json::Value {
    let run_tasks = state.engine.store.tasks_for_run(run_id).unwrap_or_default();
    let allowed: std::collections::HashSet<_> = run_tasks.iter().map(|task| task.id).collect();
    // Fail closed: an unavailable/empty task index must never return the
    // unfiltered process graph. The filtered graph can legitimately be empty.
    graph.nodes.retain(|node| allowed.contains(&node.task_id));
    graph
        .edges
        .retain(|(parent, child)| allowed.contains(parent) && allowed.contains(child));
    graph.toposort.retain(|task_id| allowed.contains(task_id));
    let nodes: Vec<serde_json::Value> = graph
        .nodes
        .iter()
        .map(|node| {
            let task = run_tasks.iter().find(|task| task.id == node.task_id);
            let role = task.map_or("", |task| task.role.as_str());
            let kind = task.map_or_else(
                || node.kind.as_deref().unwrap_or("worker"),
                |task| task_node_kind(task),
            );
            serde_json::json!({
                "task_id": node.task_id.to_string(),
                "parent_task_id": node.parent_task_id.map(|id| id.to_string()),
                "state": node.state.as_str(),
                "rev": node.rev,
                "role": role,
                "kind": kind,
                "label": node_label_for(role, kind, &node.task_id.to_string()),
            })
        })
        .collect();
    serde_json::json!({
        "revision": graph.revision,
        "nodes": nodes,
        "edges": graph.edges.iter().map(|(parent, child)| {
            [parent.to_string(), child.to_string()]
        }).collect::<Vec<_>>(),
        "toposort": graph.toposort.iter().map(ToString::to_string).collect::<Vec<_>>(),
    })
}

fn node_label_for(role: &str, kind: &str, task_id: &str) -> String {
    let configured = role.trim();
    let suffix = task_id
        .chars()
        .rev()
        .take(8)
        .collect::<String>()
        .chars()
        .rev()
        .collect::<String>();
    match kind {
        "hyperagent" => format!("HyperAgent {suffix}"),
        "superagent" => format!("SuperAgent {suffix}"),
        "tool" => format!("ToolAgent {suffix}"),
        _ if !configured.is_empty() => format!("{configured} {suffix}"),
        _ => format!("Agent {suffix}"),
    }
}

async fn history_messages(
    State(state): State<Arc<AppState>>,
    Path(run_id): Path<String>,
) -> Response {
    let Ok(run_id) = run_id.parse::<hiverouter_contract::RunId>() else {
        return refusal_response(StatusCode::NOT_FOUND, &hiverouter_contract::Refusal::RunNotFound);
    };
    if let Err(refusal) = state.engine.store.get_run(run_id) {
        return refusal_response(StatusCode::NOT_FOUND, &refusal);
    }
    const MAX_MESSAGES: usize = 1_000;
    match state.engine.store.messages(run_id) {
        Ok(messages) => {
            let total = messages.len();
            let truncated = total > MAX_MESSAGES;
            // Keep the newest window. Dropping the oldest is the only behavior
            // that preserves the current conversation and its just-streamed tail.
            let visible = messages
                .into_iter()
                .skip(total.saturating_sub(MAX_MESSAGES));
            let rows: Vec<serde_json::Value> = visible
                .map(|message| {
                    serde_json::json!({
                        "id": message.id.to_string(),
                        "run_id": message.run_id.to_string(),
                        "role": message.role.as_str(),
                        "text": message.text,
                        "created_at": message.created_at.as_str(),
                    })
                })
                .collect();
            (
                StatusCode::OK,
                Json(serde_json::json!({
                    "run_id": run_id.to_string(),
                    "messages": rows,
                    "truncated": truncated,
                })),
            )
                .into_response()
        }
        Err(refusal) => refusal_response(StatusCode::SERVICE_UNAVAILABLE, &refusal),
    }
}

/// `POST /api/agents/runs/{run_id}/messages/{message_id}/answer`
///
/// Operator answer to one open A2A question. It is a compare-and-set state
/// transition; it never grants tool authority or fabricates a reply.
async fn answer_agent_message(
    State(state): State<Arc<AppState>>,
    Path((run_raw, message_raw)): Path<(String, String)>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let Ok(run_id) = run_raw.parse::<hiverouter_contract::RunId>() else {
        return refusal_response(StatusCode::NOT_FOUND, &Refusal::RunNotFound);
    };
    let Ok(message_id) = message_raw.parse::<Uuid>() else {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
    };
    if let Err(refusal) = state.engine.store.get_run(run_id) {
        return refusal_response(StatusCode::NOT_FOUND, &refusal);
    }
    let Some(answer) = body.get("answer").and_then(|value| value.as_str()) else {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
    };
    let answer = answer.trim();
    if answer.is_empty() || answer.chars().count() > 2_000 {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
    }
    if let Some(actor) = body.get("actor").and_then(|value| value.as_str()) {
        if actor.trim() != "operator" {
            // The HTTP mailbox is the operator surface. An agent answer uses the
            // engine's parent path, not this route; accepting a caller-supplied
            // actor would let an agent impersonate the operator.
            return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
        }
    }
    let actor = "operator";
    match state
        .engine
        .store
        .answer_agent_message(run_id, message_id, actor, answer)
    {
        Ok(()) => {
            state.engine.wake_a2a_waiter(message_id).await;
            (
                [(header::CACHE_CONTROL, "no-store")],
                Json(serde_json::json!({
                    "run_id": run_id.to_string(),
                    "message_id": message_id.to_string(),
                    "status": "answered",
                })),
            )
                .into_response()
        }
        Err(Refusal::TaskNotFound) => {
            refusal_response(StatusCode::NOT_FOUND, &Refusal::TaskNotFound)
        }
        Err(Refusal::TaskTerminal) => {
            refusal_response(StatusCode::CONFLICT, &Refusal::TaskTerminal)
        }
        Err(refusal) => refusal_response(StatusCode::INTERNAL_SERVER_ERROR, &refusal),
    }
}

async fn agent_messages(
    State(state): State<Arc<AppState>>,
    Path(run_id): Path<String>,
    Query(query): Query<std::collections::HashMap<String, String>>,
) -> Response {
    let Ok(run_id) = run_id.parse::<hiverouter_contract::RunId>() else {
        return refusal_response(StatusCode::NOT_FOUND, &hiverouter_contract::Refusal::RunNotFound);
    };
    if let Err(refusal) = state.engine.store.get_run(run_id) {
        return refusal_response(StatusCode::NOT_FOUND, &refusal);
    }
    let task_id = match query.get("task_id") {
        Some(raw) => match raw.parse::<hiverouter_contract::TaskId>() {
            Ok(task_id) => Some(task_id),
            Err(_) => {
                return refusal_response(StatusCode::BAD_REQUEST, &hiverouter_contract::Refusal::SchemaInvalid);
            }
        },
        None => None,
    };
    if let Some(task_id) = task_id {
        match state.engine.store.get_task(task_id) {
            Ok(task) if task.run_id == run_id => {}
            Ok(_) => {
                return refusal_response(StatusCode::NOT_FOUND, &hiverouter_contract::Refusal::TaskNotFound);
            }
            Err(refusal) => return refusal_response(StatusCode::NOT_FOUND, &refusal),
        }
    }
    let from_task_id = match query.get("from_task_id") {
        Some(raw) => match raw.parse::<hiverouter_contract::TaskId>() {
            Ok(task_id) => Some(task_id),
            Err(_) => {
                return refusal_response(StatusCode::BAD_REQUEST, &hiverouter_contract::Refusal::SchemaInvalid);
            }
        },
        None => None,
    };
    if let Some(from_task_id) = from_task_id {
        match state.engine.store.get_task(from_task_id) {
            Ok(task) if task.run_id == run_id => {}
            Ok(_) => {
                return refusal_response(StatusCode::NOT_FOUND, &hiverouter_contract::Refusal::TaskNotFound);
            }
            Err(refusal) => return refusal_response(StatusCode::NOT_FOUND, &refusal),
        }
    }
    let kind = query.get("kind").map(String::as_str);
    if kind.is_some_and(|value| value != "ask_parent" && value != "send_to_sibling") {
        return refusal_response(StatusCode::BAD_REQUEST, &hiverouter_contract::Refusal::SchemaInvalid);
    }
    let result = match task_id {
        Some(task_id) => state.engine.store.agent_messages_for_task(run_id, task_id, 1_000),
        None => state.engine.store.agent_messages_for_run(run_id),
    };
    match result {
        Ok(messages) => {
            let messages: Vec<_> = messages
                .into_iter()
                .filter(|message| from_task_id.is_none_or(|from| message.from_task_id == from.to_string()))
                .filter(|message| kind.is_none_or(|kind| message.kind.as_str() == kind))
                .collect();
            (
                StatusCode::OK,
                [(header::CACHE_CONTROL, "no-store")],
                Json(serde_json::json!({
                    "run_id": run_id.to_string(),
                    "task_id": task_id.map(|id| id.to_string()),
                    "from_task_id": from_task_id.map(|id| id.to_string()),
                    "kind": kind,
                    "messages": messages.into_iter().map(|message| serde_json::json!({
                        "id": message.id,
                        "from_task_id": message.from_task_id,
                        "to_task_id": message.to_task_id,
                        "kind": message.kind,
                        "text": message.text,
                        "status": message.status,
                        "deadline_at": message.deadline_at,
                        "answered_at": message.answered_at,
                        "answer_text": message.answer_text,
                        "answer_actor": message.answer_actor,
                        "in_reply_to": message.in_reply_to,
                        "attempts": message.attempts,
                        "last_attempt_at": message.last_attempt_at,
                        "created_at": message.created_at,
                    })).collect::<Vec<_>>(),
                })),
            )
                .into_response()
        }
        Err(refusal) => refusal_response(StatusCode::SERVICE_UNAVAILABLE, &refusal),
    }
}

async fn import_agent_profile(
    State(state): State<Arc<AppState>>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let profile_value = body.get("profile").cloned().unwrap_or(body);
    let persona: Persona = match serde_json::from_value(profile_value.clone()) {
        Ok(persona) => persona,
        Err(_) => {
            return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
        }
    };
    if let Err(refusal) = persona.validate(&state.engine.templates) {
        return refusal_response(StatusCode::BAD_REQUEST, &refusal);
    }
    if let Err(refusal) = state
        .engine
        .store
        .upsert_agent_profile(&persona.id, &profile_value, "api")
    {
        return refusal_response(StatusCode::SERVICE_UNAVAILABLE, &refusal);
    }
    state.engine.upsert_persona(persona.clone());
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "profile": {
                "id": persona.id,
                "display_name": persona.display_name,
                "tier": persona.tier,
                "template_id": persona.template_id,
                "allowed_tool_ids": persona.allowed_tool_ids,
                "output_schema_id": persona.output_schema_id,
                "refusal_policy": persona.refusal_policy,
                "max_children": persona.max_children,
                "max_depth": persona.max_depth,
            },
            "source": "api",
        })),
    )
        .into_response()
}

async fn delete_agent_profile(
    State(state): State<Arc<AppState>>,
    Path(profile_id): Path<String>,
) -> Response {
    let id = profile_id.trim();
    if id.is_empty() || id.chars().count() > 128 {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
    }
    let removed = match state.engine.store.delete_agent_profile(id) {
        Ok(removed) => removed,
        Err(refusal) => return refusal_response(StatusCode::SERVICE_UNAVAILABLE, &refusal),
    };
    if !removed {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "code": "E_PROFILE_NOT_FOUND",
                "message": "no imported profile with that id",
            })),
        )
            .into_response();
    }
    state.engine.remove_persona(id);
    // Deleting an override restores the embedded profile without a restart.
    if let Ok(builtins) = load_personas_embedded(&state.engine.templates) {
        if let Some(builtin) = builtins.into_iter().find(|persona| persona.id == id) {
            state.engine.upsert_persona(builtin);
        }
    }
    (
        StatusCode::OK,
        Json(serde_json::json!({ "id": id, "removed": true })),
    )
        .into_response()
}

async fn agent_profiles(State(state): State<Arc<AppState>>) -> Response {
    let profiles: Vec<serde_json::Value> = state
        .engine
        .personas()
        .into_iter()
        .map(|persona| {
            serde_json::json!({
                "id": persona.id,
                "display_name": persona.display_name,
                "tier": persona.tier,
                "template_id": persona.template_id,
                "allowed_tool_ids": persona.allowed_tool_ids,
                "output_schema_id": persona.output_schema_id,
                "refusal_policy": persona.refusal_policy,
                "max_children": persona.max_children,
                "max_depth": persona.max_depth,
            })
        })
        .collect();
    (StatusCode::OK, Json(serde_json::json!({ "profiles": profiles }))).into_response()
}

/// A2A discovery: the authoritative agent roster for one run.
///
/// IDs here are the exact `TaskId`s that `send_to_sibling` / `ask_parent` need,
/// so a model can discover peers instead of inventing an id.
async fn agent_roster(
    State(state): State<Arc<AppState>>,
    Path(run_id): Path<String>,
) -> Response {
    let Ok(run_id) = run_id.parse::<hiverouter_contract::RunId>() else {
        return refusal_response(StatusCode::NOT_FOUND, &hiverouter_contract::Refusal::RunNotFound);
    };
    if state.engine.store.get_run(run_id).is_err() {
        return refusal_response(StatusCode::NOT_FOUND, &hiverouter_contract::Refusal::RunNotFound);
    }
    match state.engine.store.tasks_for_run(run_id) {
        Ok(tasks) => {
            let agents: Vec<serde_json::Value> = tasks
                .iter()
                .map(|task| {
                    let kind = task_node_kind(task);
                    let identity = state.engine.store.task_identity(task.id).ok().flatten();
                    let ownership = state.engine.store.task_ownership(task.id).ok().flatten();
                    serde_json::json!({
                        "task_id": task.id.to_string(),
                        "parent_task_id": task.parent_task_id.map(|id| id.to_string()),
                        "role": task.role,
                        "class": identity.as_ref().map(|identity| identity.class.as_str()),
                        "rank": identity.as_ref().map(|identity| identity.rank),
                        "profile_id": identity.as_ref().map(|identity| identity.profile_id.as_str()),
                        "ownership_mode": ownership.as_ref().map(|ownership| ownership.mode.as_str()),
                        "ownership_scope": ownership.as_ref().and_then(|ownership| ownership.scope.as_deref()),
                        "owner_task_id": ownership.as_ref().and_then(|ownership| ownership.owner_task_id.as_deref()),
                        "kind": kind,
                        "label": node_label_for(&task.role, kind, &task.id.to_string()),
                        "state": task.state.as_str(),
                        "depth": task.depth,
                        "rev": task.rev,
                    })
                })
                .collect();
            (
                StatusCode::OK,
                Json(serde_json::json!({
                    "run_id": run_id.to_string(),
                    "count": agents.len(),
                    "agents": agents,
                })),
            )
                .into_response()
        }
        Err(refusal) => refusal_response(StatusCode::SERVICE_UNAVAILABLE, &refusal),
    }
}

/// Read-only pending approvals. This is a query projection, never a resolve:
/// nonces are not consumed and no execution is triggered.
async fn pending_approvals(
    State(state): State<Arc<AppState>>,
    Query(query): Query<std::collections::HashMap<String, String>>,
) -> Response {
    let run_id = match query.get("run_id") {
        Some(raw) => match raw.parse::<hiverouter_contract::RunId>() {
            Ok(run_id) => Some(run_id),
            Err(_) => {
                return refusal_response(StatusCode::BAD_REQUEST, &hiverouter_contract::Refusal::SchemaInvalid);
            }
        },
        None => None,
    };
    if let Some(run_id) = run_id {
        if let Err(refusal) = state.engine.store.get_run(run_id) {
            return refusal_response(StatusCode::NOT_FOUND, &refusal);
        }
    }
    let result = match run_id {
        Some(run_id) => state.tools.pending_approval_events(run_id),
        None => state.tools.pending_approval_events_all(20),
    };
    match result {
        Ok(mut events) => {
            // A pending row can outlive a run that failed between the request
            // and its boot-time reaper. Never show an actionable approval for
            // a terminal run.
            events.retain(|event| {
                event.run_id.map_or(true, |id| {
                    state
                        .engine
                        .store
                        .get_run(id)
                        .map_or(false, |run| !run.status.is_terminal())
                })
            });
            (
                StatusCode::OK,
                [(header::CACHE_CONTROL, "no-store")],
                Json(serde_json::json!({
                    "run_id": run_id.map(|id| id.to_string()),
                    "count": events.len(),
                    "approvals": events,
                })),
            )
                .into_response()
        }
        Err(refusal) => refusal_response(StatusCode::SERVICE_UNAVAILABLE, &refusal),
    }
}

/// Read-only template catalog (ids, names, declared inputs, licence/source).
async fn templates(State(state): State<Arc<AppState>>) -> Response {
    let templates: Vec<serde_json::Value> = state
        .engine
        .templates
        .template_ids()
        .into_iter()
        .filter_map(|id| state.engine.templates.get(&id).map(|template| (id, template)))
        .map(|(id, template)| {
            let insertable = matches!(
                id.as_str(),
                "content-writer" | "data-analyst" | "code-reviewer" | "chatbot"
            );
            serde_json::json!({
                "template_id": id,
                "name": template.name,
                "inputs": template.inputs,
                "insertable": insertable,
                "spdx": template.manifest.spdx,
                "source_url": template.manifest.source_url,
                "fetched_at": template.manifest.fetched_at,
            })
        })
        .collect();
    (StatusCode::OK, Json(serde_json::json!({ "templates": templates }))).into_response()
}

/// Read-only workspace identity for agents/operators. The path itself is not a
/// secret; no file contents are exposed here.
async fn workspace(State(state): State<Arc<AppState>>) -> Response {
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "data_dir": state.config.data_dir.display().to_string(),
            "workspace_root": state.config.workspace_root.display().to_string(),
            "db_path": state.config.db_file().display().to_string(),
        })),
    )
        .into_response()
}

/// Read-only active-run projection: the live engine slot plus the latest
/// non-terminal persisted run, with no fabricated run when both are empty.
async fn active_run(State(state): State<Arc<AppState>>) -> Response {
    let live = state.engine.active_run().map(|id| id.to_string());
    match state.engine.store.latest_active_run() {
        Ok(Some(run)) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "active_run_id": live,
                "latest_active_run": {
                    "run_id": run.id.to_string(),
                    "project_id": run.project_id,
                    "status": run.status.as_str(),
                    "error_code": run.error_code,
                    "created_at": run.created_at.as_str(),
                },
            })),
        )
            .into_response(),
        Ok(None) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "active_run_id": live,
                "latest_active_run": null,
            })),
        )
            .into_response(),
        Err(refusal) => refusal_response(StatusCode::SERVICE_UNAVAILABLE, &refusal),
    }
}

/// `POST /api/memory/clear` — run-scoped memory clear with a receipt.
async fn clear_run_memory(
    State(state): State<Arc<AppState>>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let Some(run_raw) = body.get("run_id").and_then(|value| value.as_str()) else {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
    };
    let Ok(run_id) = run_raw.parse::<hiverouter_contract::RunId>() else {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
    };
    let confirmed = body
        .get("confirm")
        .and_then(|value| value.as_bool())
        .unwrap_or(false);
    let reason = body
        .get("reason")
        .and_then(|value| value.as_str())
        .map(str::trim)
        .unwrap_or_default();
    if !confirmed || reason.is_empty() || reason.chars().count() > 500 {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
    }
    let run = match state.engine.store.get_run(run_id) {
        Ok(run) => run,
        Err(refusal) => return refusal_response(StatusCode::NOT_FOUND, &refusal),
    };
    if !run.status.is_terminal() {
        return refusal_response(StatusCode::CONFLICT, &Refusal::RunActive);
    }
    // Governance records are the only rollback key. Refuse to clear them; a
    // global applied lesson must never be orphaned by a run-memory wipe.
    match state.engine.store.memory_hyper_summary(run_id, 256) {
        Ok(entries) => {
            if entries.iter().any(|entry| {
                entry.key.starts_with("evolution_authorization:")
                    || entry.key.starts_with("evolution_apply:")
            }) {
                return refusal_response(StatusCode::CONFLICT, &Refusal::ApprovalRequired);
            }
        }
        Err(refusal) => return refusal_response(StatusCode::SERVICE_UNAVAILABLE, &refusal),
    }
    match state.engine.store.clear_run_memory(run_id, "http", reason) {
        Ok(receipt) => (
            [(header::CACHE_CONTROL, "no-store")],
            Json(serde_json::json!({
                "run_id": run_id.to_string(),
                "flat_rows": receipt.flat_rows,
                "bank_entries": receipt.bank_entries,
                "banks_closed": receipt.banks_closed,
                "reason": reason,
            })),
        )
            .into_response(),
        Err(refusal) => refusal_response(StatusCode::SERVICE_UNAVAILABLE, &refusal),
    }
}

/// `POST /api/memory/prune` — operator retention of inactive run entries.
async fn prune_run_memory(
    State(state): State<Arc<AppState>>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let Some(run_raw) = body.get("run_id").and_then(|value| value.as_str()) else {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
    };
    let Ok(run_id) = run_raw.parse::<hiverouter_contract::RunId>() else {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
    };
    let confirmed = body
        .get("confirm")
        .and_then(|value| value.as_bool())
        .unwrap_or(false);
    let dry_run = body
        .get("dry_run")
        .and_then(|value| value.as_bool())
        .unwrap_or(true);
    if !confirmed {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
    }
    let run = match state.engine.store.get_run(run_id) {
        Ok(run) => run,
        Err(refusal) => return refusal_response(StatusCode::NOT_FOUND, &refusal),
    };
    if !run.status.is_terminal() {
        return refusal_response(StatusCode::CONFLICT, &Refusal::RunActive);
    }
    match state
        .engine
        .store
        .prune_inactive_memory(run_id, dry_run)
    {
        Ok(removed) => (
            [(header::CACHE_CONTROL, "no-store")],
            Json(serde_json::json!({
                "run_id": run_id.to_string(),
                "dry_run": dry_run,
                "removed": removed,
            })),
        )
            .into_response(),
        Err(refusal) => refusal_response(StatusCode::SERVICE_UNAVAILABLE, &refusal),
    }
}

/// `POST /api/memory/trust` — operator-only trust promotion with journal entry.
async fn promote_memory(
    State(state): State<Arc<AppState>>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let text = |key: &str| body.get(key).and_then(|value| value.as_str()).map(str::trim);
    let (Some(run_raw), Some(bank_id), Some(entry_id), Some(trust)) =
        (text("run_id"), text("bank_id"), text("entry_id"), text("trust"))
    else {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
    };
    let Ok(run_id) = run_raw.parse::<hiverouter_contract::RunId>() else {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
    };
    if bank_id.is_empty() || entry_id.is_empty() {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
    }
    match state
        .engine
        .store
        .promote_memory_entry(run_id, bank_id, entry_id, trust, "operator")
    {
        Ok(true) => (
            [(header::CACHE_CONTROL, "no-store")],
            Json(serde_json::json!({ "updated": true, "trust": trust })),
        )
            .into_response(),
        Ok(false) => refusal_response(StatusCode::NOT_FOUND, &Refusal::TaskNotFound),
        Err(refusal) => refusal_response(StatusCode::BAD_REQUEST, &refusal),
    }
}

/// `GET /api/memory/journal?run_id=` — recent content-free memory actions.
async fn memory_journal(
    State(state): State<Arc<AppState>>,
    Query(query): Query<std::collections::HashMap<String, String>>,
) -> Response {
    let Some(raw) = query.get("run_id") else {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
    };
    let Ok(run_id) = raw.parse::<hiverouter_contract::RunId>() else {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
    };
    let limit = query
        .get("limit")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(50)
        .clamp(1, 200);
    match state.engine.store.memory_journal_for_run(run_id, limit) {
        Ok(rows) => (
            [(header::CACHE_CONTROL, "no-store")],
            Json(serde_json::json!({
                "run_id": run_id.to_string(),
                "actions": rows.into_iter().map(|(action, bank_id, entry_id, created_at)| {
                    serde_json::json!({
                        "action": action,
                        "bank_id": bank_id,
                        "entry_id": entry_id,
                        "created_at": created_at,
                    })
                }).collect::<Vec<_>>(),
            })),
        )
            .into_response(),
        Err(refusal) => refusal_response(StatusCode::SERVICE_UNAVAILABLE, &refusal),
    }
}

/// `GET /api/memory/journal/verify?run_id=` — recomputes the journal hash chain.
async fn verify_memory_journal(
    State(state): State<Arc<AppState>>,
    Query(query): Query<std::collections::HashMap<String, String>>,
) -> Response {
    let Some(raw) = query.get("run_id") else {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
    };
    let Ok(run_id) = raw.parse::<hiverouter_contract::RunId>() else {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
    };
    match state.engine.store.verify_memory_journal(run_id) {
        Ok(verification) => (
            [(header::CACHE_CONTROL, "no-store")],
            Json(serde_json::json!({
                "run_id": run_id.to_string(),
                "valid": verification.valid,
                "checked": verification.checked,
                "unchained": verification.unchained,
            })),
        )
            .into_response(),
        Err(refusal) => refusal_response(StatusCode::SERVICE_UNAVAILABLE, &refusal),
    }
}

fn remote_json(remote: &RemoteAgent) -> serde_json::Value {
    serde_json::json!({
        "remote_id": remote.remote_id,
        "display_name": remote.display_name,
        "owner_kind": remote.owner_kind,
        "owner_id": remote.owner_id,
        "created_by": remote.created_by,
        "transport_kind": remote.transport_kind,
        "adapter_kind": remote.adapter_kind,
        "state": remote.state,
        "rev": remote.rev,
        "last_seen_at": remote.last_seen_at,
        "lease_expires_at": remote.lease_expires_at,
        "revoked_at": remote.revoked_at,
        "created_at": remote.created_at,
    })
}

fn device_json(device: &Device) -> serde_json::Value {
    let state: serde_json::Value =
        serde_json::from_str(&device.state_json).unwrap_or(serde_json::Value::Null);
    serde_json::json!({
        "device_id": device.device_id,
        "remote_id": device.remote_id,
        "class": device.class,
        "name": device.name,
        "room": device.room,
        "capabilities": serde_json::from_str::<serde_json::Value>(&device.capabilities_json)
            .unwrap_or(serde_json::Value::Null),
        "state": state,
        "state_version": device.state_version,
        "last_seen_at": device.last_seen_at,
        "safety_class": device.safety_class,
        "revoked_at": device.revoked_at,
    })
}

fn grant_json(grant: &RemoteGrant) -> serde_json::Value {
    serde_json::json!({
        "grant_id": grant.grant_id,
        "remote_id": grant.remote_id,
        "device_id": grant.device_id,
        "capability_id": grant.capability_id,
        "action": grant.action,
        "granted_at": grant.granted_at,
        "expires_at": grant.expires_at,
        "revoked_at": grant.revoked_at,
    })
}

fn action_json(action: &DeviceAction) -> serde_json::Value {
    serde_json::json!({
        "action_id": action.action_id,
        "remote_id": action.remote_id,
        "device_id": action.device_id,
        "capability_id": action.capability_id,
        "action": action.action,
        "status": action.status,
        "requested_by_kind": action.requested_by_kind,
        "approval_nonce": action.approval_nonce,
        "approval_expires_at": action.approval_expires_at,
        "error_code": action.error_code,
        "created_at": action.created_at,
        "started_at": action.started_at,
        "ended_at": action.ended_at,
    })
}

fn audit_json(audit: &RemoteAudit) -> serde_json::Value {
    serde_json::json!({
        "at": audit.at,
        "actor_kind": audit.actor_kind,
        "actor_id": audit.actor_id,
        "remote_id": audit.remote_id,
        "device_id": audit.device_id,
        "action": audit.action,
        "request_id": audit.request_id,
        "outcome": audit.outcome,
        "refusal_code": audit.refusal_code,
    })
}

fn remote_refusal_response(refusal: &Refusal) -> Response {
    let status = match refusal {
        Refusal::RemoteUnknown | Refusal::DeviceUnknown => StatusCode::NOT_FOUND,
        Refusal::RemoteCapability => StatusCode::FORBIDDEN,
        Refusal::RemoteOffline
        | Refusal::DeviceStateStale
        | Refusal::DeviceActionInFlight
        | Refusal::DeviceApprovalRequired
        | Refusal::DeviceApprovalInvalid => StatusCode::CONFLICT,
        Refusal::SchemaInvalid | Refusal::RemoteTransport => StatusCode::BAD_REQUEST,
        _ => StatusCode::SERVICE_UNAVAILABLE,
    };
    refusal_response(status, refusal)
}

/// `GET /api/remotes` — remote roster with device counts and latest health.
async fn remote_list(State(state): State<Arc<AppState>>) -> Response {
    let remotes = match state.remote.list_remotes() {
        Ok(remotes) => remotes,
        Err(refusal) => return remote_refusal_response(&refusal),
    };
    let mut cards = Vec::with_capacity(remotes.len());
    for remote in remotes {
        let devices = state.remote.list_devices(&remote.remote_id).unwrap_or_default();
        let grants = state.remote.list_grants(&remote.remote_id).unwrap_or_default();
        let audit = state.remote.list_audit(Some(&remote.remote_id), 1).unwrap_or_default();
        cards.push(serde_json::json!({
            "remote": remote_json(&remote),
            "device_count": devices.len(),
            "grant_count": grants.len(),
            "last_audit": audit.first().map(audit_json),
        }));
    }
    (
        [(header::CACHE_CONTROL, "no-store")],
        Json(serde_json::json!({ "remotes": cards, "count": cards.len() })),
    )
        .into_response()
}

/// `POST /api/remotes` — create a tree-free fixture principal.
async fn remote_create(
    State(state): State<Arc<AppState>>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let Some(display_name) = body.get("display_name").and_then(|value| value.as_str()) else {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
    };
    let Some(display_name) = (!display_name.trim().is_empty()).then_some(display_name) else {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
    };
    let owner_kind = body
        .get("owner_kind")
        .and_then(|value| value.as_str())
        .unwrap_or("operator");
    let owner_id = body
        .get("owner_id")
        .and_then(|value| value.as_str())
        .unwrap_or("operator");
    let transport_kind = body
        .get("transport_kind")
        .and_then(|value| value.as_str())
        .unwrap_or("in_process");
    let adapter_kind = body
        .get("adapter_kind")
        .and_then(|value| value.as_str())
        .unwrap_or("fixture");
    let endpoint_ref = body
        .get("endpoint_ref")
        .and_then(|value| value.as_str())
        .unwrap_or("fixture");
    match state.remote.create_remote(NewRemote {
        display_name: display_name.to_owned(),
        owner_kind: owner_kind.to_owned(),
        owner_id: owner_id.to_owned(),
        transport_kind: transport_kind.to_owned(),
        adapter_kind: adapter_kind.to_owned(),
        endpoint_ref: endpoint_ref.to_owned(),
        credential_ref: None,
        capabilities_json: "[]".to_owned(),
        created_by: "operator".to_owned(),
                request_id: None,
                request_fingerprint: None,
    }) {
        Ok(remote) => (StatusCode::CREATED, Json(remote_json(&remote))).into_response(),
        Err(refusal) => remote_refusal_response(&refusal),
    }
}

/// `POST /api/remotes/{remote_id}/revoke` — revoke a remote principal.
async fn remote_revoke(
    State(state): State<Arc<AppState>>,
    Path(remote_id): Path<String>,
) -> Response {
    match state.remote.revoke_remote(&remote_id) {
        Ok(()) => (
            [(header::CACHE_CONTROL, "no-store")],
            Json(serde_json::json!({ "remote_id": remote_id, "state": "revoked" })),
        )
            .into_response(),
        Err(refusal) => remote_refusal_response(&refusal),
    }
}

/// `POST /api/remotes/{remote_id}/heartbeat` — record a real fixture heartbeat.
async fn remote_heartbeat(
    State(state): State<Arc<AppState>>,
    Path(remote_id): Path<String>,
) -> Response {
    match state.remote.heartbeat(&remote_id) {
        Ok(remote) => (
            [(header::CACHE_CONTROL, "no-store")],
            Json(remote_json(&remote)),
        )
            .into_response(),
        Err(refusal) => remote_refusal_response(&refusal),
    }
}

/// `GET /api/remotes/{remote_id}` — remote, devices, grants and audit.
async fn remote_detail(
    State(state): State<Arc<AppState>>,
    Path(remote_id): Path<String>,
) -> Response {
    let remote = match state.remote.get_remote(&remote_id) {
        Ok(Some(remote)) => remote,
        Ok(None) => return refusal_response(StatusCode::NOT_FOUND, &Refusal::RemoteUnknown),
        Err(refusal) => return remote_refusal_response(&refusal),
    };
    let devices = state.remote.list_devices(&remote_id).unwrap_or_default();
    let grants = state.remote.list_grants(&remote_id).unwrap_or_default();
    let audit = state.remote.list_audit(Some(&remote_id), 50).unwrap_or_default();
    (
        [(header::CACHE_CONTROL, "no-store")],
        Json(serde_json::json!({
            "remote": remote_json(&remote),
            "devices": devices.iter().map(device_json).collect::<Vec<_>>(),
            "grants": grants.iter().map(grant_json).collect::<Vec<_>>(),
            "audit": audit.iter().map(audit_json).collect::<Vec<_>>(),
        })),
    )
        .into_response()
}

/// `POST /api/remotes/{remote_id}/devices` — register a fixture device.
async fn remote_device_create(
    State(state): State<Arc<AppState>>,
    Path(remote_id): Path<String>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let Some(class) = body.get("class").and_then(|value| value.as_str()) else {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
    };
    let Some(name) = body.get("name").and_then(|value| value.as_str()) else {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
    };
    let adapter_ref = body
        .get("adapter_ref")
        .and_then(|value| value.as_str())
        .unwrap_or(name);
    let capabilities = body
        .get("capabilities")
        .and_then(|value| value.as_str())
        .unwrap_or(
            r#"[{"capability_id":"power","actions":["on","off","toggle"]}]"#,
        );
    let safety_class = body
        .get("safety_class")
        .and_then(|value| value.as_str())
        .unwrap_or_else(|| {
            if class == "light" || class == "switch" {
                "reversible"
            } else {
                "critical"
            }
        });
    match state.remote.create_device(NewDevice {
        remote_id,
        class: class.to_owned(),
        name: name.to_owned(),
        room: body
            .get("room")
            .and_then(|value| value.as_str())
            .map(str::to_owned),
        adapter_ref: adapter_ref.to_owned(),
        capabilities_json: capabilities.to_owned(),
        safety_class: safety_class.to_owned(),
    }) {
        Ok(device) => (StatusCode::CREATED, Json(device_json(&device))).into_response(),
        Err(refusal) => remote_refusal_response(&refusal),
    }
}

/// `POST /api/remotes/{remote_id}/grants` — explicit operator grant.
async fn remote_grant_create(
    State(state): State<Arc<AppState>>,
    Path(remote_id): Path<String>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let Some(device_id) = body.get("device_id").and_then(|value| value.as_str()) else {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
    };
    let Some(capability_id) = body.get("capability_id").and_then(|value| value.as_str()) else {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
    };
    let Some(action) = body.get("action").and_then(|value| value.as_str()) else {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
    };
    match state.remote.create_grant(NewGrant {
        remote_id,
        device_id: device_id.to_owned(),
        capability_id: capability_id.to_owned(),
        action: action.to_owned(),
        constraints_json: "{}".to_owned(),
        expires_at: body
            .get("expires_at")
            .and_then(|value| value.as_str())
            .map(str::to_owned),
    }) {
        Ok(grant) => (StatusCode::CREATED, Json(grant_json(&grant))).into_response(),
        Err(refusal) => remote_refusal_response(&refusal),
    }
}

/// `GET /api/devices/{device_id}` — current normalized device state.
async fn remote_device_get(
    State(state): State<Arc<AppState>>,
    Path(device_id): Path<String>,
) -> Response {
    match state.remote.get_device(&device_id) {
        Ok(Some(device)) => (
            [(header::CACHE_CONTROL, "no-store")],
            Json(device_json(&device)),
        )
            .into_response(),
        Ok(None) => refusal_response(StatusCode::NOT_FOUND, &Refusal::DeviceUnknown),
        Err(refusal) => remote_refusal_response(&refusal),
    }
}

/// `POST /api/devices/{device_id}/actions` — request one fixture action.
async fn remote_action_create(
    State(state): State<Arc<AppState>>,
    Path(device_id): Path<String>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let capability_id = body
        .get("capability_id")
        .and_then(|value| value.as_str())
        .unwrap_or("power");
    let Some(action) = body.get("action").and_then(|value| value.as_str()) else {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
    };
    let idempotency_key = body
        .get("idempotency_key")
        .and_then(|value| value.as_str())
        .filter(|value| !value.is_empty())
        .map_or_else(|| format!("action:{}", Uuid::now_v7()), str::to_owned);
    match state.remote.request_action(
        &device_id,
        capability_id,
        action,
        body.get("params").unwrap_or(&serde_json::Value::Null),
        body.get("expected"),
        &idempotency_key,
        "operator",
        "operator",
    ) {
        Ok(action) => (StatusCode::OK, Json(action_json(&action))).into_response(),
        Err(refusal) => remote_refusal_response(&refusal),
    }
}

/// `POST /api/device-actions/{action_id}/resolve` — approve/deny a critical action.
async fn remote_action_resolve(
    State(state): State<Arc<AppState>>,
    Path(action_id): Path<String>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let Some(nonce) = body.get("nonce").and_then(|value| value.as_str()) else {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::DeviceApprovalInvalid);
    };
    let Some(decision) = body.get("decision").and_then(|value| value.as_str()) else {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::DeviceApprovalInvalid);
    };
    if decision != "approve" && decision != "deny" {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::DeviceApprovalInvalid);
    }
    match state
        .remote
        .resolve_action(&action_id, nonce, decision == "approve", "operator")
    {
        Ok(action) => (StatusCode::OK, Json(action_json(&action))).into_response(),
        Err(refusal) => remote_refusal_response(&refusal),
    }
}

/// `GET /api/remotes/{remote_id}/audit` — redacted remote audit.
async fn remote_audit_list(
    State(state): State<Arc<AppState>>,
    Path(remote_id): Path<String>,
    Query(query): Query<std::collections::HashMap<String, String>>,
) -> Response {
    let limit = query
        .get("limit")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(100)
        .clamp(1, 200);
    match state.remote.list_audit(Some(&remote_id), limit) {
        Ok(audit) => (
            [(header::CACHE_CONTROL, "no-store")],
            Json(serde_json::json!({
                "remote_id": remote_id,
                "audit": audit.iter().map(audit_json).collect::<Vec<_>>(),
            })),
        )
            .into_response(),
        Err(refusal) => remote_refusal_response(&refusal),
    }
}

fn daemon_json(daemon: &crate::daemon::Daemon) -> serde_json::Value {
    serde_json::json!({
        "daemon_id": daemon.daemon_id,
        "display_name": daemon.display_name,
        "owner_kind": daemon.owner_kind,
        "owner_id": daemon.owner_id,
        "kind": daemon.kind,
        "job": serde_json::from_str::<serde_json::Value>(&daemon.job_json)
            .unwrap_or(serde_json::Value::Null),
        "state": daemon.state,
        "interval_secs": daemon.interval_secs,
        "next_tick_at": daemon.next_tick_at,
        "last_tick_at": daemon.last_tick_at,
        "last_report_at": daemon.last_report_at,
        "restart_attempts": daemon.restart_attempts,
        "last_error_code": daemon.last_error_code,
        "created_by": daemon.created_by,
        "created_at": daemon.created_at,
        "retired_at": daemon.retired_at,
    })
}

async fn daemon_list(State(state): State<Arc<AppState>>) -> Response {
    match state.daemons.list() {
        Ok(daemons) => (
            [(header::CACHE_CONTROL, "no-store")],
            Json(serde_json::json!({
                "daemons": daemons.iter().map(daemon_json).collect::<Vec<_>>(),
                "count": daemons.len(),
            })),
        )
            .into_response(),
        Err(refusal) => refusal_response(StatusCode::SERVICE_UNAVAILABLE, &refusal),
    }
}

async fn daemon_create(
    State(state): State<Arc<AppState>>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let Some(display_name) = body.get("display_name").and_then(|value| value.as_str()) else {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
    };
    let owner_kind = body
        .get("owner_kind")
        .and_then(|value| value.as_str())
        .unwrap_or("operator");
    let owner_id = body
        .get("owner_id")
        .and_then(|value| value.as_str())
        .unwrap_or("operator");
    let kind = body
        .get("kind")
        .and_then(|value| value.as_str())
        .unwrap_or("watchdog");
    let interval_secs = body
        .get("interval_secs")
        .and_then(serde_json::Value::as_i64)
        .unwrap_or(60);
    match state.daemons.create(crate::daemon::NewDaemon {
        display_name: display_name.to_owned(),
        owner_kind: owner_kind.to_owned(),
        owner_id: owner_id.to_owned(),
        kind: kind.to_owned(),
        job_json: body
            .get("job")
            .cloned()
            .unwrap_or_else(|| serde_json::json!({}))
            .to_string(),
        interval_secs,
        created_by: "operator".to_owned(),
    }) {
        Ok(daemon) => (StatusCode::CREATED, Json(daemon_json(&daemon))).into_response(),
        Err(refusal) => refusal_response(StatusCode::BAD_REQUEST, &refusal),
    }
}

async fn daemon_detail(
    State(state): State<Arc<AppState>>,
    Path(daemon_id): Path<String>,
) -> Response {
    match state.daemons.get(&daemon_id) {
        Ok(Some(daemon)) => (
            [(header::CACHE_CONTROL, "no-store")],
            Json(daemon_json(&daemon)),
        )
            .into_response(),
        Ok(None) => refusal_response(StatusCode::NOT_FOUND, &Refusal::TaskNotFound),
        Err(refusal) => refusal_response(StatusCode::SERVICE_UNAVAILABLE, &refusal),
    }
}

async fn daemon_switch(
    State(state): State<Arc<AppState>>,
    Path(daemon_id): Path<String>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let Some(enabled) = body.get("enabled").and_then(serde_json::Value::as_bool) else {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
    };
    match state.daemons.set_enabled(&daemon_id, enabled) {
        Ok(daemon) => (StatusCode::OK, Json(daemon_json(&daemon))).into_response(),
        Err(refusal) => refusal_response(StatusCode::NOT_FOUND, &refusal),
    }
}

async fn daemon_retire(
    State(state): State<Arc<AppState>>,
    Path(daemon_id): Path<String>,
) -> Response {
    match state.daemons.retire(&daemon_id) {
        Ok(()) => (
            [(header::CACHE_CONTROL, "no-store")],
            Json(serde_json::json!({ "daemon_id": daemon_id, "state": "retired" })),
        )
            .into_response(),
        Err(refusal) => refusal_response(StatusCode::NOT_FOUND, &refusal),
    }
}

async fn daemon_reports(
    State(state): State<Arc<AppState>>,
    Path(daemon_id): Path<String>,
) -> Response {
    match state.daemons.reports(&daemon_id, 50) {
        Ok(reports) => (
            [(header::CACHE_CONTROL, "no-store")],
            Json(serde_json::json!({
                "daemon_id": daemon_id,
                "count": reports.len(),
                "reports": reports.into_iter().map(|report| serde_json::json!({
                    "report_id": report.report_id,
                    "outcome": report.outcome,
                    "detail": serde_json::from_str::<serde_json::Value>(&report.detail_json)
                        .unwrap_or(serde_json::Value::Null),
                    "created_at": report.created_at,
                })).collect::<Vec<_>>(),
            })),
        )
            .into_response(),
        Err(refusal) => refusal_response(StatusCode::SERVICE_UNAVAILABLE, &refusal),
    }
}

async fn supervisor_status(State(state): State<Arc<AppState>>) -> Response {
    match crate::remote_supervisor::read_state(&state.db) {
        Ok(supervisor) => (
            [(header::CACHE_CONTROL, "no-store")],
            Json(serde_json::json!({
                "lane_started_at": supervisor.lane_started_at,
                "last_tick_at": supervisor.last_tick_at,
                "next_due_at": supervisor.next_due_at,
                "backoff_ms": supervisor.backoff_ms,
                "last_error_code": supervisor.last_error_code,
            })),
        )
            .into_response(),
        Err(refusal) => refusal_response(StatusCode::SERVICE_UNAVAILABLE, &refusal),
    }
}

/// `GET /api/a2a/pending` — open A2A questions for the operator mailbox.
async fn a2a_pending(State(state): State<Arc<AppState>>) -> Response {
    match state.engine.store.pending_agent_messages(100) {
        Ok(messages) => (
            [(header::CACHE_CONTROL, "no-store")],
            Json(serde_json::json!({
                "messages": messages.into_iter().map(|message| serde_json::json!({
                    "run_id": message.run_id,
                    "id": message.id,
                    "from_task_id": message.from_task_id,
                    "to_task_id": message.to_task_id,
                    "kind": message.kind,
                    "text": message.text,
                    "deadline_at": message.deadline_at,
                    "attempts": message.attempts,
                    "last_attempt_at": message.last_attempt_at,
                    "created_at": message.created_at,
                })).collect::<Vec<_>>(),
            })),
        )
            .into_response(),
        Err(refusal) => refusal_response(StatusCode::SERVICE_UNAVAILABLE, &refusal),
    }
}

async fn a2a_messages(
    State(state): State<Arc<AppState>>,
    Query(query): Query<std::collections::HashMap<String, String>>,
) -> Response {
    let Some(raw) = query.get("run_id") else {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
    };
    let Ok(run_id) = raw.parse::<hiverouter_contract::RunId>() else {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
    };
    if state.engine.store.get_run(run_id).is_err() {
        return refusal_response(StatusCode::NOT_FOUND, &Refusal::RunNotFound);
    }
    match state.engine.store.agent_messages_for_run(run_id) {
        Ok(messages) => (
            [(header::CACHE_CONTROL, "no-store")],
            Json(serde_json::json!({
                "run_id": run_id.to_string(),
                "messages": messages.into_iter().map(|message| serde_json::json!({
                    "id": message.id,
                    "from_task_id": message.from_task_id,
                    "to_task_id": message.to_task_id,
                    "kind": message.kind,
                    "text": message.text,
                    "status": message.status,
                    "deadline_at": message.deadline_at,
                    "answered_at": message.answered_at,
                    "answer_text": message.answer_text,
                    "answer_actor": message.answer_actor,
                    "in_reply_to": message.in_reply_to,
                    "created_at": message.created_at,
                })).collect::<Vec<_>>(),
            })),
        )
            .into_response(),
        Err(refusal) => refusal_response(StatusCode::SERVICE_UNAVAILABLE, &refusal),
    }
}

/// `GET /api/memory/banks?run_id=` — durable bank list for one run.
async fn memory_banks(
    State(state): State<Arc<AppState>>,
    Query(query): Query<std::collections::HashMap<String, String>>,
) -> Response {
    let Some(raw) = query.get("run_id") else {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
    };
    let Ok(run_id) = raw.parse::<hiverouter_contract::RunId>() else {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
    };
    if state.engine.store.get_run(run_id).is_err() {
        return refusal_response(StatusCode::NOT_FOUND, &Refusal::RunNotFound);
    }
    match state.engine.store.memory_banks_for_run(run_id) {
        Ok(banks) => (
            [(header::CACHE_CONTROL, "no-store")],
            Json(serde_json::json!({
                "run_id": run_id.to_string(),
                "banks": banks.into_iter().map(|bank| serde_json::json!({
                    "id": bank.id,
                    "kind": bank.kind,
                    "scope_id": bank.scope_id,
                    "run_id": bank.run_id,
                    "owner_task_id": bank.owner_task_id,
                    "parent_bank_id": bank.parent_bank_id,
                    "display_name": bank.display_name,
                    "rev": bank.rev,
                    "created_at": bank.created_at,
                    "closed_at": bank.closed_at,
                })).collect::<Vec<_>>(),
            })),
        )
            .into_response(),
        Err(refusal) => refusal_response(StatusCode::SERVICE_UNAVAILABLE, &refusal),
    }
}

async fn memory_summary(
    State(state): State<Arc<AppState>>,
    Query(query): Query<std::collections::HashMap<String, String>>,
) -> Response {
    let Some(raw) = query.get("run_id") else {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
    };
    let Ok(run_id) = raw.parse::<hiverouter_contract::RunId>() else {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
    };
    if state.engine.store.get_run(run_id).is_err() {
        return refusal_response(StatusCode::NOT_FOUND, &Refusal::RunNotFound);
    }
    match state.engine.store.memory_hyper_summary(run_id, 8) {
        Ok(entries) => (
            [(header::CACHE_CONTROL, "no-store")],
            Json(serde_json::json!({
                "run_id": run_id.to_string(),
                "raw_access": false,
                "bounded_values": true,
                "value_truncated_at": 512,
                "count": entries.len(),
                "summaries": entries.into_iter().map(|entry| serde_json::json!({
                    "entry_id": entry.entry_id,
                    "bank_id": entry.bank_id,
                    "key": entry.key,
                    "value": entry.value.chars().take(512).collect::<String>(),
                })).collect::<Vec<_>>(),
            })),
        )
            .into_response(),
        Err(refusal) => refusal_response(StatusCode::SERVICE_UNAVAILABLE, &refusal),
    }
}

/// `GET /api/agents/runs/{run_id}/proposals` — recorded self-evolution
/// proposals and their explicit governance state. This is a read projection;
/// it never votes, applies, or mutates memory.
async fn run_evolution_proposals(
    State(state): State<Arc<AppState>>,
    Path(run_id): Path<String>,
) -> Response {
    let Ok(run_id) = run_id.parse::<hiverouter_contract::RunId>() else {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
    };
    if state.engine.store.get_run(run_id).is_err() {
        return refusal_response(StatusCode::NOT_FOUND, &Refusal::RunNotFound);
    }
    let entries = match state.engine.store.memory_hyper_summary(run_id, 256) {
        Ok(entries) => entries,
        Err(refusal) => return refusal_response(StatusCode::SERVICE_UNAVAILABLE, &refusal),
    };
    let authorizations: std::collections::HashSet<String> = entries
        .iter()
        .filter_map(|entry| entry.key.strip_prefix("evolution_authorization:").map(str::to_owned))
        .collect();
    let applied_lessons: std::collections::HashMap<String, serde_json::Value> = entries
        .iter()
        .filter_map(|entry| {
            let proposal_id = entry.key.strip_prefix("evolution_apply:")?.to_owned();
            let value = serde_json::from_str::<serde_json::Value>(&entry.value).ok()?;
            Some((proposal_id, value))
        })
        .collect();
    let rolled_back: std::collections::HashSet<String> = entries
        .iter()
        .filter_map(|entry| entry.key.strip_prefix("evolution_rollback:").map(str::to_owned))
        .collect();
    let votes: Vec<(String, serde_json::Value)> = entries
        .iter()
        .filter(|entry| entry.key.starts_with("evolution_vote:"))
        .filter_map(|entry| {
            let proposal_id = entry.key.split(':').nth(1)?.to_owned();
            let value = serde_json::from_str(&entry.value).ok()?;
            Some((proposal_id, value))
        })
        .collect();
    let valid_voters: std::collections::HashSet<String> = match state.engine.store.tasks_for_run(run_id) {
        Ok(tasks) => tasks
            .into_iter()
            .filter(|task| {
                task.run_id == run_id
                    && crate::execution::task_agent_tier(task)
                        == hiverouter_tools::tool::AgentTier::SuperAgent
                    && task.state == hiverouter_contract::AgentState::Completed
            })
            .map(|task| task.id.to_string())
            .collect(),
        Err(refusal) => return refusal_response(StatusCode::SERVICE_UNAVAILABLE, &refusal),
    };
    let proposals: Vec<serde_json::Value> = entries
        .into_iter()
        .filter(|entry| entry.key.starts_with("improvement:"))
        .filter_map(|entry| {
            let value: serde_json::Value = serde_json::from_str(&entry.value).ok()?;
            let proposal_id = value
                .get("proposal_id")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_owned();
            let proposal_votes: Vec<&serde_json::Value> = votes
                .iter()
                .filter(|(id, _)| id == &proposal_id)
                .map(|(_, vote)| vote)
                .collect();
            let is_valid_vote = |vote: &&serde_json::Value| {
                vote.get("voter_task_id")
                    .and_then(serde_json::Value::as_str)
                    .map(|id| valid_voters.contains(id))
                    .unwrap_or(false)
            };
            let approvals = proposal_votes
                .iter()
                .filter(|vote| {
                    is_valid_vote(vote)
                        && vote.get("vote").and_then(serde_json::Value::as_str) == Some("approve")
                })
                .count();
            let rejections = proposal_votes
                .iter()
                .filter(|vote| {
                    is_valid_vote(vote)
                        && vote.get("vote").and_then(serde_json::Value::as_str) == Some("reject")
                })
                .count();
            let authorized = authorizations.contains(&proposal_id);
            let lesson = applied_lessons.get(&proposal_id);
            let lesson_applied = lesson
                .and_then(|value| value.get("applied"))
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false)
                && !rolled_back.contains(&proposal_id);
            let lesson_sha256 = lesson
                .and_then(|value| value.get("sha256"))
                .cloned()
                .unwrap_or(serde_json::Value::Null);
            let lesson_state = if rolled_back.contains(&proposal_id) {
                "rolled_back"
            } else if lesson_applied {
                "applied_lesson"
            } else {
                "not_applied"
            };
            let vote_state = if authorized {
                "authorized_not_applied"
            } else if rejections > 0 {
                "rejected_by_superagent"
            } else if approvals >= 4 {
                "awaiting_operator"
            } else {
                "awaiting_votes"
            };
            Some(serde_json::json!({
                "proposal_key": entry.key,
                "proposal_id": proposal_id,
                "area": value.get("area").cloned().unwrap_or(serde_json::Value::Null),
                "problem": value.get("problem").cloned().unwrap_or(serde_json::Value::Null),
                "proposal": value.get("proposal").cloned().unwrap_or(serde_json::Value::Null),
                "evidence": value.get("evidence").cloned().unwrap_or(serde_json::Value::Null),
                "applied": value.get("applied").cloned().unwrap_or(serde_json::json!(false)),
                "governance": value.get("governance").cloned().unwrap_or(serde_json::Value::Null),
                "truthful_scope": value.get("truthful_scope").cloned().unwrap_or(serde_json::Value::Null),
                "votes": proposal_votes,
                "current_superagent_approvals": approvals,
                "current_superagent_rejections": rejections,
                "vote_state": vote_state,
                "authorized": authorized,
                "lesson_applied": lesson_applied,
                "lesson_state": lesson_state,
                "lesson_sha256": lesson_sha256,
                "can_apply": false,
            }))
        })
        .collect();
    (
        [(header::CACHE_CONTROL, "no-store")],
        Json(serde_json::json!({
            "schema": "hivemind.proposals.v1",
            "run_id": run_id.to_string(),
            "count": proposals.len(),
            "applied": false,
            "vote_path": "superagent_votes; operator_authorize; lesson_apply; lesson_rollback; code_apply_not_implemented",
            "proposals": proposals,
        })),
    )
        .into_response()
}

/// `POST /api/agents/runs/{run_id}/proposals/{proposal_id}/authorize` — the
/// operator's fifth seat. Requires exactly four distinct SuperAgent approvals,
/// zero rejections, and records an authorization only; it never applies code.
async fn authorize_evolution_proposal(
    State(state): State<Arc<AppState>>,
    Path((run_id, proposal_id)): Path<(String, String)>,
) -> Response {
    let Ok(run_id) = run_id.parse::<hiverouter_contract::RunId>() else {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
    };
    let Ok(proposal_uuid) = proposal_id.parse::<uuid::Uuid>() else {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
    };
    if state.engine.store.get_run(run_id).is_err() {
        return refusal_response(StatusCode::NOT_FOUND, &Refusal::RunNotFound);
    }
    let entries = match state.engine.store.memory_hyper_summary(run_id, 256) {
        Ok(entries) => entries,
        Err(refusal) => return refusal_response(StatusCode::SERVICE_UNAVAILABLE, &refusal),
    };
    let proposal_key = format!("improvement:{proposal_uuid}");
    let Some(proposal_value) = entries
        .iter()
        .find(|entry| entry.key == proposal_key)
        .and_then(|entry| serde_json::from_str::<serde_json::Value>(&entry.value).ok())
    else {
        return refusal_response(StatusCode::NOT_FOUND, &Refusal::ToolNotFound);
    };
    if proposal_value.get("applied").and_then(serde_json::Value::as_bool) == Some(true) {
        return refusal_response(StatusCode::CONFLICT, &Refusal::ApprovalReplay);
    }
    let prefix = format!("evolution_vote:{proposal_uuid}:");
    let votes: Vec<serde_json::Value> = entries
        .iter()
        .filter(|entry| entry.key.starts_with(&prefix))
        .filter_map(|entry| serde_json::from_str(&entry.value).ok())
        .collect();
    let tasks = match state.engine.store.tasks_for_run(run_id) {
        Ok(tasks) => tasks,
        Err(refusal) => return refusal_response(StatusCode::SERVICE_UNAVAILABLE, &refusal),
    };
    let root_task_id = tasks
        .iter()
        .find(|task| task.parent_task_id.is_none())
        .map(|task| task.id);
    let Some(root_task_id) = root_task_id else {
        return refusal_response(StatusCode::CONFLICT, &Refusal::InternalError);
    };
    let by_id: std::collections::HashMap<_, _> =
        tasks.into_iter().map(|task| (task.id, task)).collect();
    let mut voters = std::collections::HashSet::new();
    let mut approvals = 0usize;
    let mut rejections = 0usize;
    for vote in &votes {
        let Some(voter) = vote.get("voter_task_id").and_then(serde_json::Value::as_str) else {
            return refusal_response(StatusCode::CONFLICT, &Refusal::SchemaInvalid);
        };
        let Ok(voter_id) = voter.parse::<hiverouter_contract::TaskId>() else {
            return refusal_response(StatusCode::CONFLICT, &Refusal::SchemaInvalid);
        };
        let Some(voter_task) = by_id.get(&voter_id) else {
            return refusal_response(StatusCode::CONFLICT, &Refusal::TaskNotFound);
        };
        if voter_task.run_id != run_id
            || crate::execution::task_agent_tier(voter_task)
                != hiverouter_tools::tool::AgentTier::SuperAgent
            || voter_task.state != hiverouter_contract::AgentState::Completed
        {
            return refusal_response(StatusCode::CONFLICT, &Refusal::ToolNotAllowed);
        }
        if !voters.insert(voter.to_owned()) {
            return refusal_response(StatusCode::CONFLICT, &Refusal::ApprovalReplay);
        }
        match vote.get("vote").and_then(serde_json::Value::as_str) {
            Some("approve") => approvals += 1,
            Some("reject") => rejections += 1,
            _ => return refusal_response(StatusCode::CONFLICT, &Refusal::SchemaInvalid),
        }
    }
    if voters.len() < 4 || approvals < 4 || rejections != 0 {
        return refusal_response(StatusCode::CONFLICT, &Refusal::ApprovalRequired);
    }
    let authorization_key = format!("evolution_authorization:{proposal_uuid}");
    if entries.iter().any(|entry| entry.key == authorization_key) {
        return (StatusCode::OK, Json(serde_json::json!({
            "already_authorized": true,
            "proposal_id": proposal_uuid.to_string(),
            "state": "authorized_not_applied",
            "applied": false,
        })))
        .into_response();
    }
    let proposal_sha256 = evolution_proposal_sha256(&proposal_value);
    let payload = serde_json::json!({
        "proposal_id": proposal_uuid.to_string(),
        "proposal_sha256": proposal_sha256,
        "operator_approved": true,
        "superagent_approvals": 4,
        "superagent_rejections": 0,
        "unanimous": true,
        "state": "authorized_not_applied",
        "applied": false,
    });
    if let Err(refusal) = state.engine.store.record_agent_memory_scoped(
        "hyper",
        &authorization_key,
        &payload.to_string(),
        run_id,
        root_task_id,
        "run",
    ) {
        return refusal_response(StatusCode::SERVICE_UNAVAILABLE, &refusal);
    }
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "authorized": true,
            "proposal_id": proposal_uuid.to_string(),
            "state": "authorized_not_applied",
            "applied": false,
            "operator_approved": true,
            "superagent_approvals": 4,
        })),
    )
        .into_response()
}

/// `GET /api/evolution/lessons` — read-only projection of the authorized
/// lesson ledger. It never applies, removes, or mutates a lesson.
async fn evolution_lessons(State(state): State<Arc<AppState>>) -> Response {
    let path = evolution_lesson_path(&state);
    let bytes = match read_lesson_bytes(&path) {
        Ok(bytes) => bytes,
        Err(refusal) => return refusal_response(StatusCode::SERVICE_UNAVAILABLE, &refusal),
    };
    let lessons: Vec<serde_json::Value> = String::from_utf8_lossy(&bytes)
        .lines()
        .filter(|line| !line.trim().is_empty())
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .take(200)
        .collect();
    (
        [(header::CACHE_CONTROL, "no-store")],
        Json(serde_json::json!({
            "schema": "hivemind.evolution-lessons.v1",
            "count": lessons.len(),
            "artifact": "evolution/LESSONS.jsonl",
            "lessons": lessons,
        })),
    )
        .into_response()
}

/// Stable hash of the proposal fields an authorization binds. Field values are
/// concatenated explicitly so JSON map ordering cannot change the digest.
fn evolution_proposal_sha256(proposal: &serde_json::Value) -> String {
    let field = |name: &str| {
        proposal
            .get(name)
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
    };
    let canonical = format!(
        "area={}\nproblem={}\nproposal={}\nevidence={}\n",
        field("area"),
        field("problem"),
        field("proposal"),
        field("evidence")
    );
    hiverouter_tools::sha256_hex(canonical.as_bytes())
}

/// Fixed, non-agent-writable lesson path used by the authorized evolution apply.
fn evolution_lesson_path(state: &AppState) -> std::path::PathBuf {
    state.config.data_dir.join("evolution").join("LESSONS.jsonl")
}

fn read_lesson_bytes(path: &std::path::Path) -> Result<Vec<u8>, Refusal> {
    match std::fs::read(path) {
        Ok(bytes) => Ok(bytes),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(_) => Err(Refusal::InternalError),
    }
}

fn write_lesson_atomic(path: &std::path::Path, bytes: &[u8]) -> Result<(), Refusal> {
    let Some(parent) = path.parent() else {
        return Err(Refusal::InternalError);
    };
    std::fs::create_dir_all(parent).map_err(|_| Refusal::InternalError)?;
    let temp = parent.join(format!(".LESSONS.{}.tmp", uuid::Uuid::now_v7()));
    std::fs::write(&temp, bytes).map_err(|_| Refusal::InternalError)?;
    std::fs::rename(&temp, path).map_err(|_| {
        let _ = std::fs::remove_file(&temp);
        Refusal::InternalError
    })
}

/// `POST .../proposals/{proposal_id}/apply` — applies only a durable lesson
/// artifact after the five-seat authorization. It never mutates code, prompts,
/// tool grants, personas, or gates.
async fn apply_evolution_lesson(
    State(state): State<Arc<AppState>>,
    Path((run_id, proposal_id)): Path<(String, String)>,
) -> Response {
    let Ok(run_id) = run_id.parse::<hiverouter_contract::RunId>() else {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
    };
    let Ok(proposal_uuid) = proposal_id.parse::<uuid::Uuid>() else {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
    };
    if state.engine.store.get_run(run_id).is_err() {
        return refusal_response(StatusCode::NOT_FOUND, &Refusal::RunNotFound);
    }
    let entries = match state.engine.store.memory_hyper_summary(run_id, 256) {
        Ok(entries) => entries,
        Err(refusal) => return refusal_response(StatusCode::SERVICE_UNAVAILABLE, &refusal),
    };
    let authorization_key = format!("evolution_authorization:{proposal_uuid}");
    let Some(authorization) = entries
        .iter()
        .find(|entry| entry.key == authorization_key)
        .and_then(|entry| serde_json::from_str::<serde_json::Value>(&entry.value).ok())
    else {
        return refusal_response(StatusCode::CONFLICT, &Refusal::ApprovalRequired);
    };
    let apply_key = format!("evolution_apply:{proposal_uuid}");
    if entries.iter().any(|entry| entry.key == apply_key) {
        return refusal_response(StatusCode::CONFLICT, &Refusal::ApprovalReplay);
    }
    let proposal_key = format!("improvement:{proposal_uuid}");
    let Some(proposal) = entries
        .iter()
        .find(|entry| entry.key == proposal_key)
        .and_then(|entry| serde_json::from_str::<serde_json::Value>(&entry.value).ok())
    else {
        return refusal_response(StatusCode::NOT_FOUND, &Refusal::ToolNotFound);
    };
    let Some(authorized_sha256) = authorization
        .get("proposal_sha256")
        .and_then(serde_json::Value::as_str)
    else {
        return refusal_response(StatusCode::CONFLICT, &Refusal::SchemaInvalid);
    };
    if evolution_proposal_sha256(&proposal) != authorized_sha256 {
        return refusal_response(StatusCode::CONFLICT, &Refusal::ApprovalReplay);
    }
    let receipt = match crate::evolution_gate::verify_evaluation_receipt(
        &state.config.data_dir,
        &proposal_uuid.to_string(),
    ) {
        Ok(receipt) => receipt,
        Err(refusal) => {
            let status = if refusal == Refusal::ApprovalRequired {
                StatusCode::CONFLICT
            } else if refusal == Refusal::SchemaInvalid {
                StatusCode::UNPROCESSABLE_ENTITY
            } else {
                StatusCode::SERVICE_UNAVAILABLE
            };
            return refusal_response(status, &refusal);
        }
    };
    let root_task_id = match state.engine.store.tasks_for_run(run_id) {
        Ok(tasks) => tasks
            .into_iter()
            .find(|task| task.parent_task_id.is_none())
            .map(|task| task.id),
        Err(refusal) => return refusal_response(StatusCode::SERVICE_UNAVAILABLE, &refusal),
    };
    let Some(root_task_id) = root_task_id else {
        return refusal_response(StatusCode::CONFLICT, &Refusal::InternalError);
    };
    let path = evolution_lesson_path(&state);
    let existing = match read_lesson_bytes(&path) {
        Ok(bytes) => bytes,
        Err(refusal) => return refusal_response(StatusCode::SERVICE_UNAVAILABLE, &refusal),
    };
    let previous_sha256 = hiverouter_tools::sha256_hex(&existing);
    let mut lines: Vec<String> = String::from_utf8_lossy(&existing)
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(str::to_owned)
        .collect();
    if lines.iter().any(|line| {
        serde_json::from_str::<serde_json::Value>(line)
            .ok()
            .and_then(|value| value.get("proposal_id").and_then(serde_json::Value::as_str).map(str::to_owned))
            .as_deref()
            == Some(proposal_uuid.to_string().as_str())
    }) {
        return refusal_response(StatusCode::CONFLICT, &Refusal::ApprovalReplay);
    }
    for field in ["area", "problem", "proposal", "evidence"] {
        if let Some(text) = proposal.get(field).and_then(serde_json::Value::as_str) {
            if !crate::execution::memory_text_is_safe(text)
                || text.chars().any(char::is_control)
            {
                return refusal_response(StatusCode::UNPROCESSABLE_ENTITY, &Refusal::SchemaInvalid);
            }
        }
    }
    let lesson = serde_json::json!({
        "proposal_id": proposal_uuid.to_string(),
        "area": proposal.get("area").cloned().unwrap_or(serde_json::Value::Null),
        "problem": proposal.get("problem").cloned().unwrap_or(serde_json::Value::Null),
        "proposal": proposal.get("proposal").cloned().unwrap_or(serde_json::Value::Null),
        "evidence": proposal.get("evidence").cloned().unwrap_or(serde_json::Value::Null),
        "expected_effect": proposal.get("expected_effect").cloned().unwrap_or(serde_json::Value::Null),
        "verification_plan": proposal.get("verification_plan").cloned().unwrap_or(serde_json::Value::Null),
        "rollback": proposal.get("rollback").cloned().unwrap_or(serde_json::Value::Null),
        "receipt_sha256": receipt.sha256.clone(),
        "held_out_task_set": {
            "id": receipt.task_set_id.clone(),
            "sha256": receipt.task_set_sha256.clone(),
            "n": receipt.task_count,
        },
        "baseline_pass_rate": receipt.baseline_pass_rate.clone(),
        "candidate_pass_rate": receipt.candidate_pass_rate.clone(),
        "state": "applied_lesson",
    });
    let lesson_line = lesson.to_string();
    let line_sha256 = hiverouter_tools::sha256_hex(lesson_line.as_bytes());
    lines.push(lesson_line);
    let mut next = lines.join("\n");
    next.push('\n');
    if let Err(refusal) = write_lesson_atomic(&path, next.as_bytes()) {
        return refusal_response(StatusCode::SERVICE_UNAVAILABLE, &refusal);
    }
    let sha256 = hiverouter_tools::sha256_hex(next.as_bytes());
    let memory_value = serde_json::json!({
        "proposal_id": proposal_uuid.to_string(),
        "state": "applied_lesson",
        "applied": true,
        "artifact": "evolution/LESSONS.jsonl",
        "sha256": sha256,
        "line_sha256": line_sha256,
        "previous_sha256": previous_sha256,
        "receipt_sha256": receipt.sha256.clone(),
    });
    if let Err(refusal) = state.engine.store.record_agent_memory_scoped(
        "hyper",
        &apply_key,
        &memory_value.to_string(),
        run_id,
        root_task_id,
        "run",
    ) {
        let _ = write_lesson_atomic(&path, &existing);
        return refusal_response(StatusCode::SERVICE_UNAVAILABLE, &refusal);
    }
    state.engine.reload_authorized_lessons(&path);
    (StatusCode::OK, Json(serde_json::json!({
        "applied": true,
        "scope": "lesson_artifact_only",
        "proposal_id": proposal_uuid.to_string(),
        "artifact": "evolution/LESSONS.jsonl",
        "sha256": sha256,
        "line_sha256": line_sha256,
        "receipt_sha256": receipt.sha256,
        "state": "applied_lesson",
    }))).into_response()
}

/// `POST .../proposals/{proposal_id}/rollback` — removes exactly the applied
/// lesson line after verifying the current file hash matches the apply record.
async fn rollback_evolution_lesson(
    State(state): State<Arc<AppState>>,
    Path((run_id, proposal_id)): Path<(String, String)>,
) -> Response {
    let Ok(run_id) = run_id.parse::<hiverouter_contract::RunId>() else {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
    };
    let Ok(proposal_uuid) = proposal_id.parse::<uuid::Uuid>() else {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
    };
    let entries = match state.engine.store.memory_hyper_summary(run_id, 256) {
        Ok(entries) => entries,
        Err(refusal) => return refusal_response(StatusCode::SERVICE_UNAVAILABLE, &refusal),
    };
    let apply_key = format!("evolution_apply:{proposal_uuid}");
    let Some(apply_value) = entries
        .iter()
        .find(|entry| entry.key == apply_key)
        .and_then(|entry| serde_json::from_str::<serde_json::Value>(&entry.value).ok())
    else {
        return refusal_response(StatusCode::CONFLICT, &Refusal::ApprovalRequired);
    };
    let Some(expected_sha) = apply_value.get("sha256").and_then(serde_json::Value::as_str) else {
        return refusal_response(StatusCode::CONFLICT, &Refusal::InternalError);
    };
    let line_sha = apply_value
        .get("line_sha256")
        .and_then(serde_json::Value::as_str);
    let path = evolution_lesson_path(&state);
    let existing = match read_lesson_bytes(&path) {
        Ok(bytes) => bytes,
        Err(refusal) => return refusal_response(StatusCode::SERVICE_UNAVAILABLE, &refusal),
    };
    let mut lines: Vec<String> = String::from_utf8_lossy(&existing)
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(str::to_owned)
        .collect();
    let removed = if let Some(line_sha) = line_sha {
        let mut removed = None;
        lines.retain(|line| {
            if hiverouter_tools::sha256_hex(line.as_bytes()) == line_sha {
                removed = Some(line.clone());
                false
            } else {
                true
            }
        });
        let Some(line) = removed else {
            return refusal_response(StatusCode::CONFLICT, &Refusal::ApprovalReplay);
        };
        let matches_proposal = serde_json::from_str::<serde_json::Value>(&line)
            .ok()
            .and_then(|value| {
                value
                    .get("proposal_id")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned)
            })
            .as_deref()
            == Some(proposal_uuid.to_string().as_str());
        if !matches_proposal {
            return refusal_response(StatusCode::CONFLICT, &Refusal::ApprovalReplay);
        }
        line
    } else {
        if hiverouter_tools::sha256_hex(&existing) != expected_sha {
            return refusal_response(StatusCode::CONFLICT, &Refusal::ApprovalReplay);
        }
        let before = lines.len();
        lines.retain(|line| {
            serde_json::from_str::<serde_json::Value>(line)
                .ok()
                .and_then(|value| {
                    value
                        .get("proposal_id")
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_owned)
                })
                .as_deref()
                != Some(proposal_uuid.to_string().as_str())
        });
        if lines.len() == before {
            return refusal_response(StatusCode::CONFLICT, &Refusal::ToolNotFound);
        }
        String::new()
    };
    let _ = removed;
    let mut next = lines.join("\n");
    if !next.is_empty() {
        next.push('\n');
    }
    if let Err(refusal) = write_lesson_atomic(&path, next.as_bytes()) {
        return refusal_response(StatusCode::SERVICE_UNAVAILABLE, &refusal);
    }
    let sha256 = hiverouter_tools::sha256_hex(next.as_bytes());
    let rollback_key = format!("evolution_rollback:{proposal_uuid}");
    let root_task_id = match state.engine.store.tasks_for_run(run_id) {
        Ok(tasks) => tasks
            .into_iter()
            .find(|task| task.parent_task_id.is_none())
            .map(|task| task.id),
        Err(refusal) => return refusal_response(StatusCode::SERVICE_UNAVAILABLE, &refusal),
    };
    let Some(root_task_id) = root_task_id else {
        return refusal_response(StatusCode::CONFLICT, &Refusal::InternalError);
    };
    let value = serde_json::json!({
        "proposal_id": proposal_uuid.to_string(),
        "state": "rolled_back",
        "applied": false,
        "sha256": sha256,
    });
    if let Err(refusal) = state.engine.store.record_agent_memory_scoped(
        "hyper",
        &rollback_key,
        &value.to_string(),
        run_id,
        root_task_id,
        "run",
    ) {
        let _ = write_lesson_atomic(&path, &existing);
        return refusal_response(StatusCode::SERVICE_UNAVAILABLE, &refusal);
    }
    state.engine.reload_authorized_lessons(&path);
    (StatusCode::OK, Json(serde_json::json!({
        "rolled_back": true,
        "proposal_id": proposal_uuid.to_string(),
        "artifact": "evolution/LESSONS.jsonl",
        "sha256": sha256,
        "state": "rolled_back",
    }))).into_response()
}

/// `GET /api/memory/banks/{bank_id}/entries?limit=` — active bank entries.
async fn memory_bank_entries(
    State(state): State<Arc<AppState>>,
    Path(bank_id): Path<String>,
    Query(query): Query<std::collections::HashMap<String, String>>,
) -> Response {
    let Some(raw_run) = query.get("run_id") else {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
    };
    let Ok(run_id) = raw_run.parse::<hiverouter_contract::RunId>() else {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
    };
    if state.engine.store.get_run(run_id).is_err() {
        return refusal_response(StatusCode::NOT_FOUND, &Refusal::RunNotFound);
    }
    let limit = query
        .get("limit")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(50)
        .clamp(1, 200);
    match state
        .engine
        .store
        .memory_entries_for_bank_scoped(run_id, &bank_id, limit)
    {
        Ok(entries) => (
            [(header::CACHE_CONTROL, "no-store")],
            Json(serde_json::json!({
                "bank_id": bank_id,
                "entries": entries.into_iter().map(|entry| serde_json::json!({
                    "id": entry.id,
                    "bank_id": entry.bank_id,
                    "key": entry.key,
                    "kind": entry.kind,
                    "value": entry.value,
                    "source_kind": entry.source_kind,
                    "trust": entry.trust,
                    "status": entry.status,
                    "created_at": entry.created_at,
                })).collect::<Vec<_>>(),
            })),
        )
            .into_response(),
        Err(refusal) => refusal_response(StatusCode::SERVICE_UNAVAILABLE, &refusal),
    }
}

async fn agent_memory_for_run(
    State(state): State<Arc<AppState>>,
    Path(run_id): Path<String>,
    Query(query): Query<std::collections::HashMap<String, String>>,
) -> Response {
    let Ok(run_id) = run_id.parse::<hiverouter_contract::RunId>() else {
        return refusal_response(StatusCode::NOT_FOUND, &hiverouter_contract::Refusal::RunNotFound);
    };
    if let Err(refusal) = state.engine.store.get_run(run_id) {
        return refusal_response(StatusCode::NOT_FOUND, &refusal);
    }
    let scope = query.get("scope").map(String::as_str);
    if scope.is_some_and(|value| value.is_empty() || value.chars().count() > 128) {
        return refusal_response(StatusCode::BAD_REQUEST, &hiverouter_contract::Refusal::SchemaInvalid);
    }
    let result = match scope {
        Some(scope) => state.engine.store.agent_memory_entries_for_scope(run_id, scope, 200),
        None => state.engine.store.agent_memory_entries_for_run(run_id, 200),
    };
    match result {
        Ok(entries) => {
            let rows: Vec<serde_json::Value> = entries
                .into_iter()
                .map(|entry| {
                    serde_json::json!({
                        "run_id": entry.run_id,
                        "scope_id": entry.scope_id,
                        "agent_key": entry.agent_key,
                        "key": entry.key,
                        "value": entry.value,
                        "task_id": entry.task_id,
                        "created_at": entry.created_at,
                    })
                })
                .collect();
            (
                StatusCode::OK,
                [(header::CACHE_CONTROL, "no-store")],
                Json(serde_json::json!({
                    "run_id": run_id.to_string(),
                    "scope": scope,
                    "count": rows.len(),
                    "memories": rows,
                })),
            )
                .into_response()
        }
        Err(refusal) => refusal_response(StatusCode::SERVICE_UNAVAILABLE, &refusal),
    }
}

async fn agent_memory_list(State(state): State<Arc<AppState>>) -> Response {
    let mut profiles: Vec<serde_json::Value> = Vec::new();
    for persona in state.engine.personas() {
        match state.engine.store.agent_memory(&persona.id, 8) {
            Ok(memories) => profiles.push(serde_json::json!({
                "id": persona.id,
                "display_name": persona.display_name,
                "memories": memories.into_iter().map(|(key, value)| serde_json::json!({
                    "key": key,
                    "value": value,
                })).collect::<Vec<_>>(),
            })),
            Err(refusal) => {
                return refusal_response(StatusCode::SERVICE_UNAVAILABLE, &refusal);
            }
        }
    }
    (StatusCode::OK, Json(serde_json::json!({ "profiles": profiles }))).into_response()
}

async fn clear_agent_memory(
    State(state): State<Arc<AppState>>,
    Path(agent_key): Path<String>,
) -> Response {
    let key = agent_key.trim();
    if key.is_empty() || key.chars().count() > 128 {
        return refusal_response(StatusCode::BAD_REQUEST, &hiverouter_contract::Refusal::SchemaInvalid);
    }
    match state.engine.store.clear_agent_memory(key) {
        Ok(removed) => (
            StatusCode::OK,
            Json(serde_json::json!({ "agent_key": key, "removed": removed })),
        )
            .into_response(),
        Err(refusal) => refusal_response(StatusCode::SERVICE_UNAVAILABLE, &refusal),
    }
}

/// `GET /api/runs/{run_id}/detail` — run summary, task classes/states, usage.
async fn run_detail(
    State(state): State<Arc<AppState>>,
    Path(run_id): Path<String>,
) -> Response {
    let Ok(run_id) = run_id.parse::<hiverouter_contract::RunId>() else {
        return refusal_response(StatusCode::NOT_FOUND, &Refusal::RunNotFound);
    };
    let run = match state.engine.store.get_run(run_id) {
        Ok(run) => run,
        Err(refusal) => return refusal_response(StatusCode::NOT_FOUND, &refusal),
    };
    let tasks = match state.engine.store.tasks_for_run(run_id) {
        Ok(tasks) => tasks,
        Err(refusal) => return refusal_response(StatusCode::SERVICE_UNAVAILABLE, &refusal),
    };
    let mut counts: std::collections::BTreeMap<&str, u64> = std::collections::BTreeMap::new();
    let mut rows = Vec::with_capacity(tasks.len());
    for task in &tasks {
        *counts.entry(task.state.as_str()).or_insert(0) += 1;
        let identity = state.engine.store.task_identity(task.id).ok().flatten();
        rows.push(serde_json::json!({
            "task_id": task.id.to_string(),
            "parent_task_id": task.parent_task_id.map(|id| id.to_string()),
            "role": task.role,
            "class": identity.as_ref().map(|identity| identity.class.as_str()),
            "profile_id": identity.as_ref().map(|identity| identity.profile_id.as_str()),
            "state": task.state.as_str(),
            "depth": task.depth,
            "rev": task.rev,
        }));
    }
    let usage = state.engine.store.run_usage(run_id).ok().flatten();
    let banks = state
        .engine
        .store
        .memory_banks_for_run(run_id)
        .map(|banks| banks.len())
        .unwrap_or(0);
    (
        [(header::CACHE_CONTROL, "no-store")],
        Json(serde_json::json!({
            "run_id": run.id.to_string(),
            "project_id": run.project_id,
            "status": run.status.as_str(),
            "error_code": run.error_code,
            "created_at": run.created_at.as_str(),
            "updated_at": run.updated_at.as_str(),
            "task_count": tasks.len(),
            "state_counts": counts,
            "tasks": rows,
            "usage": usage.map(|usage| serde_json::json!({
                "prompt_tokens": usage.prompt_tokens,
                "completion_tokens": usage.completion_tokens,
                "total_tokens": usage.total_tokens,
            })),
            "memory_banks": banks,
        })),
    )
        .into_response()
}

/// `GET /api/memory/context/{task_id}` — latest snapshot metadata for one task.
async fn task_memory_context(
    State(state): State<Arc<AppState>>,
    Path(task_id): Path<String>,
) -> Response {
    let Ok(task_id) = task_id.parse::<hiverouter_contract::TaskId>() else {
        return refusal_response(StatusCode::NOT_FOUND, &Refusal::TaskNotFound);
    };
    match state.engine.store.memory_context_snapshot(task_id) {
        Ok(Some((banks, entry_ids, rendered_chars, truncated, prompt_sha256, created_at))) => {
            let banks: Vec<String> = match serde_json::from_str(&banks) {
                Ok(banks) => banks,
                Err(_) => {
                    return refusal_response(
                        StatusCode::SERVICE_UNAVAILABLE,
                        &Refusal::InternalError,
                    );
                }
            };
            let entry_ids: Vec<String> = match serde_json::from_str(&entry_ids) {
                Ok(entry_ids) => entry_ids,
                Err(_) => {
                    return refusal_response(
                        StatusCode::SERVICE_UNAVAILABLE,
                        &Refusal::InternalError,
                    );
                }
            };
            (
                [(header::CACHE_CONTROL, "no-store")],
                Json(serde_json::json!({
                    "task_id": task_id.to_string(),
                    "banks": banks,
                    "entry_ids": entry_ids,
                    "rendered_chars": rendered_chars,
                    "truncated": truncated,
                    "prompt_sha256": prompt_sha256,
                    "created_at": created_at,
                })),
            )
                .into_response()
        }
        Ok(None) => refusal_response(StatusCode::NOT_FOUND, &Refusal::TaskNotFound),
        Err(refusal) => refusal_response(StatusCode::SERVICE_UNAVAILABLE, &refusal),
    }
}

async fn run_usage(
    State(state): State<Arc<AppState>>,
    Path(run_id): Path<String>,
) -> Response {
    let Ok(run_id) = run_id.parse::<hiverouter_contract::RunId>() else {
        return refusal_response(StatusCode::NOT_FOUND, &hiverouter_contract::Refusal::RunNotFound);
    };
    if let Err(refusal) = state.engine.store.get_run(run_id) {
        return refusal_response(StatusCode::NOT_FOUND, &refusal);
    }
    match state.engine.store.run_usage(run_id) {
        Ok(Some(usage)) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "run_id": run_id.to_string(),
                "usage": {
                    "prompt_tokens": usage.prompt_tokens,
                    "completion_tokens": usage.completion_tokens,
                    "total_tokens": usage.total_tokens,
                    "prompt_cache_hit_tokens": usage.prompt_cache_hit_tokens,
                    "prompt_cache_miss_tokens": usage.prompt_cache_miss_tokens,
                    "reasoning_tokens": usage.reasoning_tokens,
                },
            })),
        )
            .into_response(),
        Ok(None) => (
            StatusCode::OK,
            Json(serde_json::json!({ "run_id": run_id.to_string(), "usage": null })),
        )
            .into_response(),
        Err(refusal) => refusal_response(StatusCode::SERVICE_UNAVAILABLE, &refusal),
    }
}

/// `GET /api/provider/models` — documented DeepSeek model catalogue proxy.
///
/// The API key never reaches the browser: the local server reads
/// `DEEPSEEK_API_KEY`, calls the provider, and returns only public metadata.
async fn provider_models(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    if !provider_origin_allowed(&state, &headers) {
        return refusal_response(StatusCode::FORBIDDEN, &Refusal::OriginRejected);
    }
    let client = match DeepSeekClient::from_env_with_key_env(
        ClientConfig::default().with_base_url(state.config.provider.base_url.clone()),
        state.config.provider.key_env_name(),
    ) {
        Ok(client) => client,
        Err(error) => return provider_error_response(&error),
    };
    match client.list_models().await {
        Ok(models) => (
            StatusCode::OK,
            [(header::CACHE_CONTROL, "no-store")],
            Json(serde_json::json!({
                "object": models.object,
                "models": models.data,
                "configured_model": state.config.provider.wire_model_id(),
            })),
        )
            .into_response(),
        Err(error) => provider_error_response(&error),
    }
}

/// `GET /api/provider/balance` — documented DeepSeek account balance proxy.
async fn provider_balance(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    if !provider_origin_allowed(&state, &headers) {
        return refusal_response(StatusCode::FORBIDDEN, &Refusal::OriginRejected);
    }
    if state.config.provider.is_external_endpoint() {
        return (
            StatusCode::OK,
            [(header::CACHE_CONTROL, "no-store")],
            Json(serde_json::json!({
                "supported": false,
                "is_available": null,
                "balances": [],
                "reason": "the configured OpenAI-compatible endpoint does not document an account-balance route",
            })),
        )
            .into_response();
    }
    let client = match DeepSeekClient::from_env_with_key_env(
        ClientConfig::default().with_base_url(state.config.provider.base_url.clone()),
        state.config.provider.key_env_name(),
    ) {
        Ok(client) => client,
        Err(error) => return provider_error_response(&error),
    };
    match client.balance().await {
        Ok(balance) => (
            StatusCode::OK,
            [(header::CACHE_CONTROL, "no-store")],
            Json(serde_json::json!({
                "supported": true,
                "is_available": balance.is_available,
                "balances": balance.balance_infos,
            })),
        )
            .into_response(),
        Err(error) => provider_error_response(&error),
    }
}

/// `GET /api/provider/profile` — saved non-secret provider profile.
async fn provider_profile_get(State(state): State<Arc<AppState>>) -> Response {
    let path = crate::config::provider_profile_path();
    let current = path
        .as_deref()
        .and_then(|path| std::fs::read_to_string(path).ok())
        .and_then(|text| {
            serde_json::from_str::<serde_json::Value>(text.trim_start_matches('\u{feff}')).ok()
        })
        .map(|value| {
            // Never echo unknown fields from the file: the profile is defined as
            // a non-secret endpoint record, not a general JSON store.
            serde_json::json!({
                "name": value.get("name").and_then(|item| item.as_str()),
                "api_type": value.get("api_type").and_then(|item| item.as_str()),
                "base_url": value.get("base_url").and_then(|item| item.as_str()),
                "model": value.get("model").and_then(|item| item.as_str()),
                "wire_model": value.get("wire_model").and_then(|item| item.as_str()),
            })
        });
    (
        StatusCode::OK,
        [(header::CACHE_CONTROL, "no-store")],
        Json(serde_json::json!({
            "profile": current,
            "path": path.as_ref().map(|path| path.display().to_string()),
            "active": {
                "api_type": state.config.provider.api_type,
                "base_url": state.config.provider.base_url,
                "wire_model": state.config.provider.wire_model_id(),
                "model": state.config.provider.model.id(),
                "key_env": state.config.provider.key_env_name(),
                "endpoint": if state.config.provider.is_external_endpoint() { "external" } else { "deepseek" },
            },
            "restart_required_after_save": true,
        })),
    )
        .into_response()
}

/// `POST /api/provider/profile` — persist a non-secret provider profile.
async fn provider_profile_set(
    State(state): State<Arc<AppState>>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    if !state.config.side_effects_enabled {
        return refusal_response(StatusCode::FORBIDDEN, &Refusal::ToolNotAllowed);
    }
    let Some(base_url) = body.get("base_url").and_then(|value| value.as_str()) else {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
    };
    let base_url = base_url.trim().trim_end_matches('/');
    if base_url.is_empty()
        || !base_url.starts_with("http://") && !base_url.starts_with("https://")
    {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
    }
    let model = body
        .get("model")
        .and_then(|value| value.as_str())
        .unwrap_or("deepseek-flash");
    if hiverouter_provider::Model::from_id(model).is_none() {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
    }
    let api_type = match body.get("api_type").and_then(|value| value.as_str()) {
        Some(value) => match crate::config::parse_api_type(value) {
            Ok(value) => value,
            Err(_) => return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid),
        },
        None if base_url != hiverouter_provider::DEFAULT_BASE_URL => "tokenlabs".to_owned(),
        None => "deepseek".to_owned(),
    };
    let wire_model = body
        .get("wire_model")
        .and_then(|value| value.as_str())
        .map(str::trim)
        .filter(|value| !value.is_empty());
    if (api_type == "tokenlabs" || base_url != hiverouter_provider::DEFAULT_BASE_URL)
        && wire_model.is_none()
    {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
    }
    if api_type == "tokenlabs" && base_url == hiverouter_provider::DEFAULT_BASE_URL {
        return refusal_response(StatusCode::BAD_REQUEST, &Refusal::SchemaInvalid);
    }
    let profile = serde_json::json!({
        "name": body.get("name").and_then(|value| value.as_str()).unwrap_or("custom"),
        "api_type": api_type,
        "base_url": base_url,
        "model": model,
        "wire_model": wire_model,
    });
    match crate::config::write_provider_profile(&profile) {
        Ok(()) => (
            [(header::CACHE_CONTROL, "no-store")],
            Json(serde_json::json!({
                "saved": true,
                "restart_required": true,
                "profile": profile,
            })),
        )
            .into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "code": Refusal::InternalError.code(),
                "message": error.to_string(),
                "how_to_fix": "Fix the provider profile path/permissions, then save again.",
            })),
        )
            .into_response(),
    }
}

fn provider_origin_allowed(state: &AppState, headers: &HeaderMap) -> bool {    match headers.get(header::ORIGIN).and_then(|value| value.to_str().ok()) {
        None => true,
        Some(origin) => state
            .config
            .allowed_origins
            .iter()
            .any(|allowed| allowed.eq_ignore_ascii_case(origin)),
    }
}

fn provider_error_response(error: &hiverouter_provider::ProviderError) -> Response {
    let status = match error.code() {
        "E_KEY_MISSING" => StatusCode::SERVICE_UNAVAILABLE,
        "E_PROVIDER_AUTH" => StatusCode::UNAUTHORIZED,
        "E_PROVIDER_RATE_LIMIT" => StatusCode::TOO_MANY_REQUESTS,
        _ => StatusCode::BAD_GATEWAY,
    };
    refusal_response(status, &error.refusal())
}

async fn projects(State(state): State<Arc<AppState>>) -> Response {
    match state.engine.store.projects() {
        Ok(projects) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "projects": projects.into_iter().map(|(id, name, created_at)| serde_json::json!({
                    "id": id,
                    "name": name,
                    "created_at": created_at,
                })).collect::<Vec<_>>(),
            })),
        )
            .into_response(),
        Err(refusal) => refusal_response(StatusCode::SERVICE_UNAVAILABLE, &refusal),
    }
}

/// Read-only runtime capacity/provider status. No secrets; key presence is a
/// boolean and runtime caps are the effective values enforced by the engine.
async fn runtime_status(State(state): State<Arc<AppState>>) -> Response {
    let active_run = state.engine.active_run();
    let latest_run = match state.engine.store.latest_active_run() {
        Ok(run) => run,
        Err(refusal) => return refusal_response(StatusCode::SERVICE_UNAVAILABLE, &refusal),
    };
    let latest_run_id = latest_run.as_ref().map(|run| run.id);
    let task_count = match active_run.or(latest_run_id) {
        Some(run_id) => match state.engine.store.task_count(run_id) {
            Ok(count) => Some(count),
            Err(refusal) => return refusal_response(StatusCode::SERVICE_UNAVAILABLE, &refusal),
        },
        None => None,
    };
    let key_present = state
        .checks
        .iter()
        .find(|check| check.name == "deepseek_api_key")
        .map(|check| check.ok);
    let caps = state.engine.caps;
    (
        StatusCode::OK,
        [(header::CACHE_CONTROL, "no-store")],
        Json(serde_json::json!({
            "status": if active_run.is_some() { "busy" } else { "idle" },
            "active_run_id": active_run.map(|id| id.to_string()),
            "latest_active_run_id": latest_run_id.map(|id| id.to_string()),
            "active_task_count": task_count,
            "caps": {
                "max_depth": caps.max_depth,
                "max_fanout": caps.max_fanout,
                "max_agents": caps.max_agents,
                "max_calls": caps.max_calls,
                "parallel_dispatch": state.engine.parallel_dispatch(),
            },
            "provider": {
                "enabled": state.config.provider.enabled,
                "api_type": state.config.provider.api_type,
                "model": state.config.provider.model.id(),
                "wire_model": state.config.provider.wire_model_id(),
                "key_env": state.config.provider.key_env_name(),
                "endpoint": if state.config.provider.is_external_endpoint() { "external" } else { "deepseek" },
                "billing": if state.config.provider.is_external_endpoint() { "external_unverified" } else { "deepseek_usd" },
                "key_present": key_present,
                "planner_thinking": format!("{:?}", state.config.provider.planner_thinking.kind).to_lowercase(),
                "leaf_thinking": format!("{:?}", state.config.provider.leaf_thinking.kind).to_lowercase(),
                "reasoning_effort": format!("{:?}", state.config.provider.planner_reasoning_effort).to_lowercase(),
                "planner_max_tokens": state.config.provider.planner_max_tokens,
                "leaf_max_tokens": state.config.provider.leaf_max_tokens,
            },
            "side_effects_enabled": state.config.side_effects_enabled,
            "workspace_root": state.config.workspace_root.display().to_string(),
            "last_seq": state.bus.current_seq(),
            "graph_nodes": state.graph.node_count().ok(),
        })),
    )
        .into_response()
}

async fn health(State(state): State<Arc<AppState>>) -> Response {
    let version = state.db.schema_version();
    let database = match &version {
        Ok(schema_version) => DatabaseHealth {
            path: state.db.path().display().to_string(),
            schema_version: Some(*schema_version),
            error_code: None,
        },
        Err(error) => DatabaseHealth {
            path: state.db.path().display().to_string(),
            schema_version: None,
            error_code: Some(error.refusal().code()),
        },
    };
    let required_ok = state
        .checks
        .iter()
        .filter(|check| check.required)
        .all(|check| check.ok)
        && version.is_ok();
    let all_ok = state.checks.iter().all(|check| check.ok) && version.is_ok();
    let status = if required_ok && all_ok {
        "ok"
    } else if required_ok {
        "degraded"
    } else {
        "failing"
    };
    let payload = HealthResponse {
        status,
        version: env!("CARGO_PKG_VERSION"),
        schema: hiverouter_contract::SCHEMA_VERSION,
        uptime_secs: state.started_at.elapsed().as_secs(),
        pid: std::process::id(),
        boot_id: state.boot_id.clone(),
        data_dir: state.config.data_dir.display().to_string(),
        workspace_root: state.config.workspace_root.display().to_string(),
        checks: state.checks.clone(),
        database,
        transport: TransportHealth {
            last_seq: state.bus.current_seq(),
            graph_nodes: state.graph.node_count().ok(),
            allowed_origins: state.session.allowed_origins().len(),
        },
        prompt_assets: PromptAssetsHealth {
            loaded: state.prompt_assets_loaded(),
            error_code: state.prompt_assets_error.map(|refusal| refusal.code()),
        },
    };
    let status_code = if required_ok {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (status_code, Json(payload)).into_response()
}

async fn api_session(
    State(state): State<Arc<AppState>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Response {
    let origin = headers
        .get(header::ORIGIN)
        .and_then(|value| value.to_str().ok());
    let allowed = match origin {
        Some(origin) => state.session.is_origin_allowed(Some(origin)),
        // Same-origin fetches normally carry no Origin header; only a loopback
        // peer may use that path.
        None => peer.ip().is_loopback(),
    };
    if !allowed {
        return refusal_response(StatusCode::FORBIDDEN, &hiverouter_contract::Refusal::OriginRejected);
    }
    if let Some(expected) = configured_launch_token() {
        let supplied = supplied_launch_token(&headers);
        if !launch_token_matches(Some(expected.as_str()), supplied) {
            return (
                StatusCode::FORBIDDEN,
                Json(serde_json::json!({
                    "code": hiverouter_contract::Refusal::OriginRejected.code(),
                    "message": "the launcher token is required to read the session",
                    "how_to_fix": "open HiveMind through HiveMind.exe or start-hivemind.ps1",
                })),
            )
                .into_response();
        }
    }
    (
        StatusCode::OK,
        [(header::CACHE_CONTROL, "no-store")],
        Json(serde_json::json!({
            "token": state.session.token(),
            "schema": hiverouter_contract::SCHEMA_VERSION,
            "last_seq": state.bus.current_seq(),
            "ws": "/ws",
        })),
    )
        .into_response()
}

/// Serves the router on an already-bound listener with graceful shutdown.
///
/// # Errors
/// Returns the listener error when `axum::serve` fails.
pub async fn serve(
    listener: TcpListener,
    state: Arc<AppState>,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> std::io::Result<()> {
    axum::serve(
        listener,
        build_router(state).into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown)
    .await
}

/// Best-effort loopback check used by handlers that receive a peer address.
#[must_use]
pub fn is_loopback(ip: IpAddr) -> bool {
    ip.is_loopback()
}

#[cfg(test)]
mod tests {
    use super::*;
    use hiverouter_contract::{AgentState, TaskId};

    use crate::bus::Replay;
    use crate::entities::TaskRecord;

    #[test]
    fn boot_state_publishes_the_recovered_node_states() {
        let temp = tempfile::tempdir().expect("temp dir");
        let config = Arc::new(
            Config::resolve("127.0.0.1:0", temp.path().to_str().expect("utf8"), None)
                .expect("config"),
        );
        let db = Arc::new(Database::open(config.db_file()).expect("database"));
        db.migrate().expect("migrations");
        let store = crate::store::RunStore::new(db.clone());
        let run = store
            .create_run("project-1", "crashed while waiting", AgentState::Interrupted)
            .expect("run");
        let task = TaskRecord {
            id: TaskId::new(),
            run_id: run.id,
            parent_task_id: None,
            role: "worker".to_owned(),
            depth: 1,
            spawn_seq: 1,
            state: AgentState::Interrupted,
            rev: 0,
        };
        store.insert_task(&task).expect("task");

        // The production boot path: recovery ran first (main.rs), then the state
        // is built; the catch-up must publish the persisted recovery outcome.
        let state = AppState::new(db, crate::checks::run_checks(), config);
        let events = match state.bus.replay_after(0) {
            Replay::Events(events) => events,
            other => panic!("expected one boot catch-up frame, got {other:?}"),
        };
        assert_eq!(events.len(), 1, "one frame for the one recovered task");
        assert_eq!(events[0].envelope.name, "NODE_STATE");
        assert_eq!(events[0].envelope.payload["task_id"], task.id.to_string());
        assert_eq!(events[0].envelope.payload["state"], "interrupted");
        assert_eq!(events[0].envelope.run_id, Some(run.id));
    }

    #[test]
    fn launch_token_gate_matches_only_the_configured_secret() {
        assert!(launch_token_matches(None, None));
        assert!(launch_token_matches(Some("abc"), Some("abc")));
        assert!(!launch_token_matches(Some("abc"), None));
        assert!(!launch_token_matches(Some("abc"), Some("wrong")));
    }
}