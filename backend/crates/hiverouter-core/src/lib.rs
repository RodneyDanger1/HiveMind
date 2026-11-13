//! HiveMind v1 backend core: localhost axum server, environment checks, SQLite
//! bootstrap. Phase 1 surface only; later phases add transport, graph and stores
//! without duplicating these owners.

#![forbid(unsafe_code)]
#![warn(missing_debug_implementations)]

pub mod approval_wait;
pub mod bus;
pub mod caps;
pub mod chat_bus;
pub mod checkpoint;
pub mod delegation_protocol;
pub mod hivewire;
pub mod jobs;
pub mod semantic_recall;
pub mod session_model;
pub mod repo_index;
pub mod checks;
pub mod config;
pub mod context;
pub mod daemon;
pub mod db;
pub mod entities;
pub mod eval;
pub mod evolution_gate;
pub mod execution;
pub mod graph;
pub mod handshake;
pub mod history;
pub mod http;
pub mod learning;
pub mod planner;
pub mod prompts;
pub mod provider_executor;
pub mod recovery;
pub mod remote;
pub mod remote_supervisor;
pub mod run_state;
pub mod skills;
pub mod snapshot;
pub mod static_ui;
pub mod store;
pub mod tool_loop;
pub mod tools_bridge;
pub mod ws;

pub use approval_wait::{ApprovalWaiters, ApprovalWake};
pub use bus::{EventBus, Replay, SequencedEvent};
pub use caps::Caps;
pub use checks::{run_checks, Check};
pub use config::{Config, ConfigError};
pub use context::{ContextStore, ScopedContext};
pub use daemon::{Daemon, DaemonReport, DaemonStore, NewDaemon};
pub use db::{Database, DbError, Migration, ToolTurnRecord, MIGRATIONS};
pub use entities::{ChatMessage, ChatRole, PermissionPolicy, Run, Session, TaskRecord, Todo};
pub use evolution_gate::{verify_evaluation_receipt, VerifiedReceipt};
pub use execution::{
    DeltaSink, ProviderExecutor, RunEngine, RunRegistry, StreamedReply, ToolTurnTranscript,
};
pub use graph::{GraphAuthority, GraphSnapshot, TaskNode};
pub use handshake::{HandshakeFrame, HandshakeLog, HandshakeOutcome};
pub use history::{HistoryReadModel, TimeTravel};
pub use http::{build_router, serve, AppState, HealthResponse};
pub use planner::{Plan, PlanNode};
pub use prompts::{Persona, TemplateRegistry};
pub use provider_executor::ProviderExecutorAdapter;
pub use recovery::{recover_on_boot, RecoveryReport};
pub use remote::{
    Device, DeviceAction, RemoteAgent, RemoteAudit, RemoteGrant, RemoteStore,
};
pub use remote_supervisor::{RemoteSupervisor, SupervisorState};
pub use run_state::{
    apply_run, apply_task, can_apply_run, can_apply_task, Effect, RunStateError, RunTransition,
    Scope, StateEvent, TaskTransition,
};
pub use skills::{load_skills, prompt_block as skills_prompt_block, Skill, SkillError};
pub use snapshot::{SnapshotEnvelope, SnapshotStore};
pub use store::RunStore;
pub use tool_loop::{
    bounded_tool_text, call_payload, check_call_budget, next_turn, permission_for, plan_call,
    requires_approval, tool_specs_for_tier, validate_plan, ToolCallPlan, ToolExecution,
    MAX_CALLS_PER_RUN, MAX_TOOL_RESULT_BYTES, MAX_TOOL_TURNS,
};
pub use tools_bridge::ToolsBridge;
pub use ws::SessionAuth;

