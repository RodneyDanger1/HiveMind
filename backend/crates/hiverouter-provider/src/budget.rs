//! Pre-send budget core and the B1–B12 invariants (blueprint §2a, C17).
//!
//! Policy defaults are PROPOSED-POLICY values from the blueprint; they are not
//! measured facts and are only changed with a decision + ledger row.
//!
//! Enforcement order in [`RunBudget::preflight`] happens **before** any HTTP
//! request is built: an exceeded cap refuses with zero provider calls.
//! Retries are bounded per call (≤3 attempts, jittered, Retry-After capped at
//! 60 s, no retry on 401/402 — the client honours this policy).

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use hiverouter_contract::{Refusal, Usage};
use rust_decimal::Decimal;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;

use crate::accounting::{CallCost, CostMode, CostReport, CostStatus, RunAccounting};
use crate::dto::Model;
use crate::error::ProviderError;
use crate::pricing::PricingTable;

/// B1: run cost cap, USD per run (PROPOSED-POLICY).
pub const RUN_COST_CAP_USD_CENTS: i64 = 100;

/// B2: combined prompt+completion token budget per run (PROPOSED-POLICY).
pub const TOKEN_BUDGET_COMBINED: u64 = 2_000_000;

/// B3: provider call cap per run, including retries (PROPOSED-POLICY).
/// Maximum provider calls per run. The accepted plan cap is 40 nodes; the
/// planner, root control and final synthesis mean a 40-node run needs more
/// than 40 sends, so the call cap cannot be lower than the node cap.
pub const CALL_CAP: u32 = 128;

/// B4: maximum spawn depth (Hyper → Super → Worker).
pub const MAX_SPAWN_DEPTH: u8 = 3;

/// B5: maximum children per task.
pub const FANOUT_LIMIT: u8 = 8;

/// B6: maximum agents per run.
pub const AGENT_LIMIT: u16 = 64;

/// B7: local in-flight semaphore size (provider ceiling: 2500 flash / 500 pro).
pub const IN_FLIGHT_LIMIT: usize = 8;

/// B8: maximum attempts per provider call (first try + retries).
pub const MAX_ATTEMPTS: u32 = 3;

/// B9: Retry-After is capped at 60 s (FACT F1 + §2a).
pub const RETRY_AFTER_CAP: Duration = Duration::from_secs(60);

/// B10: exactly one account key; multi-key rotation is forbidden.
pub const SUPPORTED_KEY_COUNT: usize = 1;

/// The documented run cost cap as a Decimal.
#[must_use]
pub fn run_cost_cap_usd() -> Decimal {
    Decimal::new(RUN_COST_CAP_USD_CENTS, 2)
}

/// Budget limits for one run (defaults are the blueprint PROPOSED-POLICY values).
#[derive(Debug, Clone, PartialEq)]
pub struct BudgetLimits {
    /// B1: run cost cap in USD.
    pub run_cost_cap_usd: Decimal,
    /// B2: combined token budget.
    pub token_budget_combined: u64,
    /// B3: call cap including retries.
    pub call_cap: u32,
    /// B8: attempts per call.
    pub max_attempts: u32,
    /// How usage becomes money: DeepSeek USD, or measured-but-unpriced for an
    /// external OpenAI-compatible endpoint.
    pub cost_mode: CostMode,
}

impl Default for BudgetLimits {
    fn default() -> Self {
        Self {
            run_cost_cap_usd: run_cost_cap_usd(),
            token_budget_combined: TOKEN_BUDGET_COMBINED,
            call_cap: CALL_CAP,
            max_attempts: MAX_ATTEMPTS,
            cost_mode: CostMode::DeepSeekUsd,
        }
    }
}

impl BudgetLimits {
    /// Limits for an explicitly configured OpenAI-compatible endpoint.
    ///
    /// Such an endpoint may bill in points rather than DeepSeek USD, so the
    /// DeepSeek dollar cap is not a truthful stop condition. Token and call
    /// caps still apply, and [`CostMode::ExternalUnpriced`] guarantees the run
    /// never reports a settled DeepSeek USD amount.
    #[must_use]
    pub fn external_endpoint() -> Self {
        Self {
            run_cost_cap_usd: Decimal::from(u64::MAX),
            cost_mode: CostMode::ExternalUnpriced,
            ..Self::default()
        }
    }
}

/// A pre-send estimate. Values are `Assumed` provenance and never settled as
/// cost; they may only gate a send against the documented budgets.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CallEstimate {
    /// Estimated prompt tokens (assumed, not measured).
    pub prompt_tokens: Option<u64>,
    /// Maximum output tokens the call may generate (from the request's cap).
    pub max_output_tokens: Option<u64>,
}

impl CallEstimate {
    /// Total estimate when both sides are known.
    #[must_use]
    pub fn total_tokens(&self) -> Option<u64> {
        match (self.prompt_tokens, self.max_output_tokens) {
            (Some(prompt), Some(output)) => Some(prompt.saturating_add(output)),
            _ => None,
        }
    }
}

/// Proof that a send passed the pre-send budget checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BudgetPermit {
    /// Zero-based call index this permit allows.
    pub call_index: u32,
}

/// Per-run budget and cost accounting (single money owner: [`RunAccounting`]).
#[derive(Debug, Clone)]
pub struct RunBudget {
    limits: BudgetLimits,
    accounting: RunAccounting,
    attempts: u32,
    retries: u32,
}

impl RunBudget {
    /// New budget for one run using the documented pricing table.
    #[must_use]
    pub fn new(limits: BudgetLimits) -> Self {
        Self::with_pricing(limits, PricingTable::documented())
    }

    /// New budget over an explicit pricing table (tests / future price updates).
    ///
    /// The cost mode follows `limits.cost_mode`, so
    /// `RunBudget::new(BudgetLimits::external_endpoint())` never settles USD.
    #[must_use]
    pub fn with_pricing(limits: BudgetLimits, table: PricingTable) -> Self {
        let mode = limits.cost_mode;
        Self {
            limits,
            accounting: RunAccounting::with_mode(table, mode),
            attempts: 0,
            retries: 0,
        }
    }

    /// New budget for an external endpoint (measured usage, no USD settlement).
    #[must_use]
    pub fn external(limits: BudgetLimits) -> Self {
        let mut limits = limits;
        limits.cost_mode = CostMode::ExternalUnpriced;
        Self::new(limits)
    }

    /// Cost mode of this run.
    #[must_use]
    pub fn cost_mode(&self) -> CostMode {
        self.accounting.mode()
    }

    /// Configured limits.
    #[must_use]
    pub fn limits(&self) -> &BudgetLimits {
        &self.limits
    }

    /// Recorded calls (each settled usage record).
    #[must_use]
    pub fn calls(&self) -> u32 {
        self.accounting.calls()
    }

    /// Provider send attempts including retries (B3 call cap counts these).
    #[must_use]
    pub fn attempts(&self) -> u32 {
        self.attempts
    }

    /// Records one send attempt (called immediately before each provider send,
    /// including retries, so the B3 call cap includes retries).
    pub fn record_attempt(&mut self) {
        self.attempts = self.attempts.saturating_add(1);
    }

    /// Retries recorded for diagnostics (bounded per call by the client policy).
    #[must_use]
    pub fn retries(&self) -> u32 {
        self.retries
    }

    /// Spent cost in USD (Decimal only).
    #[must_use]
    pub fn spent_usd(&self) -> Decimal {
        self.accounting.settled_usd().unwrap_or(Decimal::ZERO)
    }

    /// Remaining run cost budget (never negative).
    #[must_use]
    pub fn remaining_usd(&self) -> Decimal {
        match self.limits.run_cost_cap_usd.checked_sub(self.spent_usd()) {
            Some(remaining) if remaining > Decimal::ZERO => remaining,
            _ => Decimal::ZERO,
        }
    }

    /// Combined prompt+completion tokens accumulated so far, `None` when any
    /// side is unknown (never fabricated as zero).
    #[must_use]
    pub fn combined_tokens(&self) -> Option<u64> {
        let usage = self.accounting.usage();
        let prompt = usage.prompt_tokens.or_else(|| {
            match (
                usage.prompt_cache_hit_tokens,
                usage.prompt_cache_miss_tokens,
            ) {
                (Some(hit), Some(miss)) => Some(hit.saturating_add(miss)),
                _ => None,
            }
        })?;
        let completion = usage.completion_tokens?;
        Some(prompt.saturating_add(completion))
    }

    /// Run cost status.
    #[must_use]
    pub fn status(&self) -> CostStatus {
        self.accounting.status()
    }

    /// Accounting report (null stays null; INCOMPLETE exports no cost).
    #[must_use]
    pub fn report(&self) -> CostReport {
        self.accounting.report()
    }

    /// Pre-send budget check (B1/B2/B3/B7 and the V1–V6 integrity rule).
    ///
    /// Must be called **before** the HTTP request is built; a refusal means the
    /// caller sends nothing.
    ///
    /// # Errors
    /// - [`Refusal::UsageIncomplete`] when a previous call left the run
    ///   incomplete because its usage counters were missing or inconsistent
    ///   (the token/cost totals cannot be verified). A transport failure is
    ///   recorded separately via [`Self::record_transport_failure`] and does
    ///   not poison later sends.
    /// - [`Refusal::CallLimit`] when the call cap is reached.
    /// - [`Refusal::BudgetExceeded`] when the run cost cap is reached.
    /// - [`Refusal::TokenBudgetExceeded`] when known spend + the estimate
    ///   exceeds the combined token budget.
    pub fn preflight(&self, estimate: &CallEstimate) -> Result<BudgetPermit, ProviderError> {
        if self.accounting.incomplete_reason() == Some(Refusal::UsageIncomplete) {
            return Err(ProviderError::new(
                Refusal::UsageIncomplete,
                "a previous call left the run INCOMPLETE; refusing another send because the \
                 token and cost totals cannot be verified",
            ));
        }
        if self.attempts >= self.limits.call_cap {
            return Err(ProviderError::new(
                Refusal::CallLimit,
                format!(
                    "call cap reached ({} of {} attempts, including retries)",
                    self.attempts, self.limits.call_cap
                ),
            ));
        }
        let spent = self.spent_usd();
        // An external endpoint has no USD price; the dollar cap is not a
        // truthful stop condition there, so only the token/call caps gate it.
        if self.cost_mode().prices_usd() && spent >= self.limits.run_cost_cap_usd {
            return Err(ProviderError::new(
                Refusal::BudgetExceeded,
                format!(
                    "run cost cap reached (spent {spent} of {})",
                    self.limits.run_cost_cap_usd
                ),
            ));
        }
        let used_tokens = if self.accounting.calls() == 0 {
            // A fresh run has provably used zero tokens (no call recorded), so
            // the pre-send estimate can be checked against the full budget.
            Some(0)
        } else {
            self.combined_tokens()
        };
        if let (Some(used), Some(estimated)) = (used_tokens, estimate.total_tokens()) {
            let projected = used.saturating_add(estimated);
            if projected > self.limits.token_budget_combined {
                return Err(ProviderError::new(
                    Refusal::TokenBudgetExceeded,
                    format!(
                        "combined token budget would be exceeded ({projected} > {})",
                        self.limits.token_budget_combined
                    ),
                ));
            }
        }
        Ok(BudgetPermit {
            call_index: self.calls(),
        })
    }

    /// Records the outcome of an allowed call (usage may be absent).
    ///
    /// # Errors
    /// [`Refusal::UsageIncomplete`] when usage is absent or too partial to
    /// settle a cost; the run is marked INCOMPLETE and no cost is exported.
    pub fn record_call(
        &mut self,
        model: Model,
        usage: Option<&Usage>,
        at_unix_secs: u64,
    ) -> Result<CallCost, ProviderError> {
        self.accounting.record(model, usage, at_unix_secs)
    }

    /// Records a stream attempt that failed in transport after it may have been
    /// billed.
    ///
    /// The run report stays INCOMPLETE (spend unknown, `settled_usd` null) but
    /// later sends are **not** poisoned: a transient transport failure must not
    /// fail every sibling task sharing the run budget. The caller must surface
    /// the original transport refusal to its own task and must not substitute
    /// this for [`Self::record_call`] on a stream that completed.
    pub fn record_transport_failure(&mut self, refusal: Refusal) {
        self.accounting.record_transport_failure(refusal);
    }

    /// The transport failure recorded for this run, if any.
    #[must_use]
    pub fn transport_failure(&self) -> Option<Refusal> {
        self.accounting.transport_failure()
    }

    /// Records one retry attempt (diagnostics; the per-call cap is enforced by
    /// the client's [`RetryPolicy`]).
    pub fn on_retry(&mut self) {
        self.retries = self.retries.saturating_add(1);
    }
}

/// Backoff policy for provider retries (B8/B9).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetryPolicy {
    /// Attempts including the first try.
    pub max_attempts: u32,
    /// Base exponential backoff.
    pub base_delay: Duration,
    /// Cap for the jittered exponential delay.
    pub max_delay: Duration,
    /// Maximum jitter added to an exponential delay.
    pub jitter_max: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: MAX_ATTEMPTS,
            base_delay: Duration::from_millis(250),
            max_delay: Duration::from_secs(8),
            jitter_max: Duration::from_millis(250),
        }
    }
}

impl RetryPolicy {
    /// Delay before attempt `attempt` (1-based; attempt 1 is the first retry).
    ///
    /// A provider `Retry-After` wins (documented server instruction) and is
    /// capped at [`RETRY_AFTER_CAP`]; otherwise jittered exponential backoff is
    /// used. No retry happens on 401/402 (enforced by the client).
    #[must_use]
    pub fn delay_for(&self, attempt: u32, retry_after: Option<Duration>) -> Duration {
        if let Some(retry_after) = retry_after {
            return retry_after.min(RETRY_AFTER_CAP);
        }
        let exponent = attempt.saturating_sub(1).min(16);
        let exponential = self.base_delay.saturating_mul(1u32 << exponent);
        let jitter = jitter_within(self.jitter_max);
        exponential
            .saturating_add(jitter)
            .min(self.max_delay.max(exponential.min(self.max_delay)))
    }
}

/// Deterministic-bounded jitter drawn from a uuid-v7 (no extra RNG crate).
fn jitter_within(max: Duration) -> Duration {
    let budget_nanos = u64::try_from(max.as_nanos()).unwrap_or(u64::MAX);
    if budget_nanos == 0 {
        return Duration::ZERO;
    }
    let seed = Uuid::now_v7().as_u128() as u64;
    Duration::from_nanos(seed % budget_nanos)
}

/// Parses a `Retry-After` header value (integer seconds) and caps it at 60 s.
///
/// HTTP-date values are not documented by the provider and are ignored (`None`),
/// which falls back to jittered exponential backoff rather than guessing.
#[must_use]
pub fn retry_after_from_header_value(value: Option<&str>) -> Option<Duration> {
    let seconds: u64 = value?.trim().parse().ok()?;
    Some(Duration::from_secs(seconds).min(RETRY_AFTER_CAP))
}

/// Default DeepSeek request-rate policy: no local request-rate governor exists
/// for the documented account endpoint (its documented limiter is
/// concurrency, enforced by [`InFlightLimiter`]). Keeping 0 here means the
/// default endpoint behaves exactly as before.
pub const DEEPSEEK_REQUESTS_PER_MINUTE: u32 = 0;

/// Documented request-rate limit of the external OpenAI-compatible endpoint
/// (AMD Token Lab: 20 requests/minute).
pub const EXTERNAL_ENDPOINT_REQUESTS_PER_MINUTE: u32 = 20;

/// Default window for request-rate accounting.
pub const RATE_WINDOW: Duration = Duration::from_secs(60);

/// Default ceiling on how long one HTTP attempt may wait for a rate permit
/// before the call is refused by name instead of queueing indefinitely.
pub const RATE_MAX_WAIT: Duration = Duration::from_secs(10);

/// Request-rate policy for one endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RateLimitPolicy {
    /// Permits per `window`; `0` disables the governor (DeepSeek default).
    pub requests_per_minute: u32,
    /// Sliding window length (normally 60 s; short only in tests).
    pub window: Duration,
    /// Longest wait an attempt will accept before refusing.
    pub max_wait: Duration,
}

impl RateLimitPolicy {
    /// No request-rate governor (documented DeepSeek default behavior).
    #[must_use]
    pub fn unlimited() -> Self {
        Self {
            requests_per_minute: DEEPSEEK_REQUESTS_PER_MINUTE,
            window: RATE_WINDOW,
            max_wait: Duration::ZERO,
        }
    }

    /// `requests_per_minute` permits per 60 s window.
    #[must_use]
    pub fn per_minute(requests_per_minute: u32) -> Self {
        Self {
            requests_per_minute,
            window: RATE_WINDOW,
            max_wait: RATE_MAX_WAIT,
        }
    }

    /// Documented external endpoint policy (20 requests/minute).
    #[must_use]
    pub fn external_endpoint() -> Self {
        Self::per_minute(EXTERNAL_ENDPOINT_REQUESTS_PER_MINUTE)
    }

    /// Same policy over a different window (tests use short windows).
    #[must_use]
    pub fn with_window(mut self, window: Duration) -> Self {
        self.window = window;
        self
    }

    /// Same policy with a different maximum wait.
    #[must_use]
    pub fn with_max_wait(mut self, max_wait: Duration) -> Self {
        self.max_wait = max_wait;
        self
    }

    /// Whether the governor is disabled.
    #[must_use]
    pub fn is_unlimited(&self) -> bool {
        self.requests_per_minute == 0
    }
}

impl Default for RateLimitPolicy {
    fn default() -> Self {
        Self::unlimited()
    }
}

/// Proof that one HTTP attempt holds a request-rate permit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RatePermit {
    /// Grant instant measured from the governor's start.
    pub granted_at: Duration,
}

/// The wait until the next permit would become available.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RateWait {
    /// Time until the oldest in-window grant leaves the window.
    pub wait: Duration,
}

/// Shared sliding-window request-rate governor for provider HTTP attempts.
///
/// Every attempt (including retries) must take a permit before it is sent.
/// The governor keeps at most `requests_per_minute` grant instants; a caller
/// that would have to wait longer than `max_wait` is refused with
/// [`Refusal::LlmRateLimit`] instead of queueing without a bound. It never
/// busy-loops: [`Self::acquire`] sleeps exactly until the next permit.
///
/// Determinism: [`Self::try_acquire_at`] takes the current instant as an
/// argument, so tests can inject a clock and assert spacing/permit accounting
/// without sleeping for a real minute.
#[derive(Debug)]
pub struct RequestRateGovernor {
    policy: RateLimitPolicy,
    epoch: std::time::Instant,
    state: Mutex<VecDeque<Duration>>,
    granted_total: std::sync::atomic::AtomicU64,
}

impl RequestRateGovernor {
    /// Builds a governor from a policy.
    #[must_use]
    pub fn new(policy: RateLimitPolicy) -> Self {
        Self {
            policy,
            epoch: std::time::Instant::now(),
            state: Mutex::new(VecDeque::new()),
            granted_total: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Configured policy.
    #[must_use]
    pub fn policy(&self) -> &RateLimitPolicy {
        &self.policy
    }

    /// Total permits granted since construction (diagnostics and tests).
    #[must_use]
    pub fn granted_total(&self) -> u64 {
        self.granted_total
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    fn lock_state(&self) -> std::sync::MutexGuard<'_, VecDeque<Duration>> {
        // The critical section never panics; recover rather than poison.
        match self.state.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// Tries to take a permit at an explicitly supplied instant.
    ///
    /// `at` must be monotonic. Returns the wait until the window frees a slot
    /// when no permit is available; it never sleeps.
    ///
    /// # Errors
    /// Never errors; a too-long wait is reported as [`RateWait`] so the caller
    /// can name the refusal.
    pub fn try_acquire_at(&self, at: Duration) -> Result<RatePermit, RateWait> {
        if self.policy.is_unlimited() {
            self.granted_total
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return Ok(RatePermit { granted_at: at });
        }
        let mut grants = self.lock_state();
        while let Some(oldest) = grants.front() {
            if at.saturating_sub(*oldest) < self.policy.window {
                break;
            }
            grants.pop_front();
        }
        let limit = usize::try_from(self.policy.requests_per_minute).unwrap_or(usize::MAX);
        if grants.len() < limit {
            grants.push_back(at);
            drop(grants);
            self.granted_total
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return Ok(RatePermit { granted_at: at });
        }
        let oldest = grants.front().copied().unwrap_or(at);
        let wait = oldest.saturating_add(self.policy.window).saturating_sub(at);
        Err(RateWait { wait })
    }

    /// Waits for (or refuses) a permit for one HTTP attempt.
    ///
    /// # Errors
    /// [`Refusal::LlmRateLimit`] when the next permit is further away than
    /// `policy.max_wait`; the wait is bounded by `max_wait`, and the retry loop
    /// is bounded to two sleeps, so this can never spin.
    pub async fn acquire(&self) -> Result<RatePermit, ProviderError> {
        if self.policy.is_unlimited() {
            return self
                .try_acquire_at(Duration::ZERO)
                .map_err(|wait| self.too_far(wait.wait));
        }
        let mut sleeps = 0u32;
        loop {
            match self.try_acquire_at(self.epoch.elapsed()) {
                Ok(permit) => return Ok(permit),
                Err(wait) if wait.wait > self.policy.max_wait => {
                    return Err(self.too_far(wait.wait));
                }
                Err(wait) => {
                    sleeps = sleeps.saturating_add(1);
                    if sleeps > 2 {
                        return Err(self.too_far(wait.wait));
                    }
                    // Sleep exactly until the window frees the slot (plus 1 ms
                    // so the boundary is inclusive on the next check).
                    tokio::time::sleep(wait.wait.saturating_add(Duration::from_millis(1))).await;
                }
            }
        }
    }

    fn too_far(&self, wait: Duration) -> ProviderError {
        ProviderError::new(
            Refusal::LlmRateLimit,
            format!(
                "request-rate governor: next request permit is {} ms away, beyond the {} ms \
                 maximum wait ({} requests per {}); refusing instead of queueing",
                u64::try_from(wait.as_millis()).unwrap_or(u64::MAX),
                u64::try_from(self.policy.max_wait.as_millis()).unwrap_or(u64::MAX),
                self.policy.requests_per_minute,
                u64::try_from(self.policy.window.as_secs()).unwrap_or(u64::MAX),
            ),
        )
        .with_status(429)
    }
}

/// Local in-flight concurrency limiter (B7; provider concurrency is
/// account-level per FACT F1 — this local semaphore is the only limiter).
#[derive(Debug, Clone)]
pub struct InFlightLimiter {
    semaphore: Arc<Semaphore>,
    limit: usize,
}

impl InFlightLimiter {
    /// Creates a limiter with the documented local in-flight size (8).
    #[must_use]
    pub fn new(limit: usize) -> Self {
        assert!(limit > 0, "in-flight limit must be positive");
        Self {
            semaphore: Arc::new(Semaphore::new(limit)),
            limit,
        }
    }

    /// Documented local limit.
    #[must_use]
    pub fn limit(&self) -> usize {
        self.limit
    }

    /// Permits currently available.
    #[must_use]
    pub fn available_permits(&self) -> usize {
        self.semaphore.available_permits()
    }

    /// Waits for a permit (queued work waits; `E_SEMAPHORE_FULL` is returned by
    /// [`Self::try_acquire`] for non-blocking callers).
    ///
    /// # Errors
    /// [`Refusal::InternalError`] only if the semaphore is closed (it never is
    /// in this crate).
    pub async fn acquire(&self) -> Result<OwnedSemaphorePermit, ProviderError> {
        Arc::clone(&self.semaphore)
            .acquire_owned()
            .await
            .map_err(|_| {
                ProviderError::new(Refusal::InternalError, "in-flight semaphore was closed")
            })
    }

    /// Non-blocking acquire.
    ///
    /// # Errors
    /// [`Refusal::SemaphoreFull`] when every permit is in flight.
    pub fn try_acquire(&self) -> Result<OwnedSemaphorePermit, ProviderError> {
        Arc::clone(&self.semaphore)
            .try_acquire_owned()
            .map_err(|error| match error {
                tokio::sync::TryAcquireError::NoPermits => ProviderError::new(
                    Refusal::SemaphoreFull,
                    format!("in-flight semaphore full (limit {})", self.limit),
                ),
                tokio::sync::TryAcquireError::Closed => {
                    ProviderError::new(Refusal::InternalError, "in-flight semaphore was closed")
                }
            })
    }
}

/// Spawn caps for one run (B4/B5/B6), checked before any spawn is committed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RunCaps {
    /// Maximum depth.
    pub max_depth: u8,
    /// Maximum children per task.
    pub max_fanout: u8,
    /// Maximum total agents per run.
    pub max_agents: u16,
}

impl Default for RunCaps {
    fn default() -> Self {
        Self {
            max_depth: MAX_SPAWN_DEPTH,
            max_fanout: FANOUT_LIMIT,
            max_agents: AGENT_LIMIT,
        }
    }
}

impl RunCaps {
    /// Checks a proposed spawn against the caps.
    ///
    /// # Errors
    /// [`Refusal::SpawnDepth`], [`Refusal::FanoutLimit`] or
    /// [`Refusal::AgentLimit`] — refused by name before the spawn is recorded.
    pub fn check_spawn(
        &self,
        child_depth: u8,
        current_children: u8,
        current_agents: u16,
    ) -> Result<(), ProviderError> {
        if child_depth > self.max_depth {
            return Err(ProviderError::new(
                Refusal::SpawnDepth,
                format!(
                    "spawn depth {child_depth} exceeds the maximum {}",
                    self.max_depth
                ),
            ));
        }
        if current_children >= self.max_fanout {
            return Err(ProviderError::new(
                Refusal::FanoutLimit,
                format!(
                    "fan-out {current_children} already at the limit {}",
                    self.max_fanout
                ),
            ));
        }
        if current_agents.saturating_add(1) > self.max_agents {
            return Err(ProviderError::new(
                Refusal::AgentLimit,
                format!("spawn would exceed the {} agent cap", self.max_agents),
            ));
        }
        Ok(())
    }
}

/// Enforces the one-key rule (D2/B10). Multi-key rotation is forbidden.
///
/// # Errors
/// [`Refusal::KeyMissing`] for zero keys, [`Refusal::MultikeyForbidden`] for
/// more than one.
pub fn enforce_single_key(key_count: usize) -> Result<(), ProviderError> {
    match key_count {
        0 => Err(ProviderError::new(
            Refusal::KeyMissing,
            "no DeepSeek API key configured (exactly one key is required)",
        )),
        SUPPORTED_KEY_COUNT => Ok(()),
        other => Err(ProviderError::new(
            Refusal::MultikeyForbidden,
            format!("{other} provider keys configured; exactly one account key is supported"),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hiverouter_contract::Timestamp;
    use std::str::FromStr;

    fn unix(rfc3339: &str) -> u64 {
        Timestamp::parse(rfc3339)
            .expect("valid")
            .unix_secs()
            .expect("representable")
    }

    fn settled_usage() -> Usage {
        Usage {
            prompt_tokens: Some(1_000),
            completion_tokens: Some(500),
            total_tokens: Some(1_500),
            prompt_cache_hit_tokens: Some(400),
            prompt_cache_miss_tokens: Some(600),
            reasoning_tokens: None,
        }
    }

    #[test]
    fn defaults_match_blueprint_policy_values() {
        let limits = BudgetLimits::default();
        assert_eq!(limits.run_cost_cap_usd, Decimal::from_str("1.00").unwrap());
        assert_eq!(limits.token_budget_combined, 2_000_000);
        assert_eq!(limits.call_cap, 128);
        assert_eq!(limits.max_attempts, 3);
        assert_eq!(MAX_SPAWN_DEPTH, 3);
        assert_eq!(FANOUT_LIMIT, 8);
        assert_eq!(AGENT_LIMIT, 64);
        assert_eq!(IN_FLIGHT_LIMIT, 8);
        assert_eq!(RETRY_AFTER_CAP, Duration::from_secs(60));
    }

    #[test]
    fn budget_preflight_refuses_before_any_send() {
        // Cost cap reached by a real measured call; the next preflight refuses
        // without any provider interaction.
        let mut budget = RunBudget::new(BudgetLimits::default());
        let expensive = Usage {
            prompt_tokens: Some(1_000_000),
            completion_tokens: Some(1_000_000),
            total_tokens: Some(2_000_000),
            prompt_cache_hit_tokens: Some(500_000),
            prompt_cache_miss_tokens: Some(500_000),
            reasoning_tokens: None,
        };
        budget
            .record_call(Model::Pro, Some(&expensive), unix("2026-11-06T01:00:00Z"))
            .expect("measured call settles");
        assert!(budget.spent_usd() > budget.limits().run_cost_cap_usd);
        let error = budget
            .preflight(&CallEstimate::default())
            .expect_err("cap reached");
        assert_eq!(error.code(), "E_BUDGET_EXCEEDED");
        assert_eq!(budget.calls(), 1, "no call was attempted by preflight");
    }

    #[test]
    fn call_cap_and_token_budget_refuse_by_name() {
        // The cap counts send attempts including retries.
        let mut limits = BudgetLimits::default();
        limits.call_cap = 1;
        let mut budget = RunBudget::new(limits);
        budget.record_attempt();
        assert_eq!(
            budget
                .preflight(&CallEstimate::default())
                .expect_err("cap")
                .code(),
            "E_CALL_LIMIT"
        );

        let mut limits = BudgetLimits::default();
        limits.call_cap = 2;
        let mut budget = RunBudget::new(limits);
        budget
            .record_call(Model::Flash, Some(&settled_usage()), 0)
            .expect("settles");
        budget
            .record_call(Model::Flash, Some(&settled_usage()), 0)
            .expect("settles");
        budget.record_attempt();
        budget.record_attempt();
        assert_eq!(
            budget
                .preflight(&CallEstimate::default())
                .expect_err("cap")
                .code(),
            "E_CALL_LIMIT"
        );

        let mut limits = BudgetLimits::default();
        limits.token_budget_combined = 1_000;
        let budget = RunBudget::new(limits);
        let estimate = CallEstimate {
            prompt_tokens: Some(900),
            max_output_tokens: Some(200),
        };
        assert_eq!(
            budget.preflight(&estimate).expect_err("tokens").code(),
            "E_TOKEN_BUDGET_EXCEEDED"
        );
    }

    #[test]
    fn incomplete_usage_blocks_further_sends() {
        let mut budget = RunBudget::new(BudgetLimits::default());
        budget
            .record_call(Model::Flash, None, 0)
            .expect_err("null usage is incomplete");
        assert_eq!(
            budget
                .preflight(&CallEstimate::default())
                .expect_err("incomplete")
                .code(),
            "E_USAGE_INCOMPLETE"
        );
    }

    #[test]
    fn retry_after_is_capped_and_parsed() {
        assert_eq!(
            retry_after_from_header_value(Some("120")),
            Some(Duration::from_secs(60))
        );
        assert_eq!(
            retry_after_from_header_value(Some("7")),
            Some(Duration::from_secs(7))
        );
        assert_eq!(
            retry_after_from_header_value(Some("Wed, 21 Oct 2026 07:28:00 GMT")),
            None
        );
        assert_eq!(retry_after_from_header_value(None), None);

        let policy = RetryPolicy::default();
        assert_eq!(
            policy.delay_for(1, Some(Duration::from_secs(120))),
            Duration::from_secs(60),
            "Retry-After wins and is capped"
        );
        for attempt in 1..=5 {
            let delay = policy.delay_for(attempt, None);
            assert!(delay >= policy.base_delay, "backoff has a floor");
            assert!(
                delay <= policy.max_delay.max(policy.base_delay),
                "backoff is bounded"
            );
        }
    }

    #[test]
    fn one_key_rule_refuses_by_name() {
        assert!(enforce_single_key(1).is_ok());
        assert_eq!(
            enforce_single_key(0).expect_err("none").code(),
            "E_KEY_MISSING"
        );
        assert_eq!(
            enforce_single_key(2).expect_err("multi").code(),
            "E_MULTIKEY_FORBIDDEN"
        );
    }

    #[test]
    fn spawn_caps_refuse_by_name() {
        let caps = RunCaps::default();
        assert!(caps.check_spawn(3, 7, 63).is_ok());
        assert_eq!(
            caps.check_spawn(4, 0, 0).expect_err("depth").code(),
            "E_SPAWN_DEPTH"
        );
        assert_eq!(
            caps.check_spawn(2, 8, 0).expect_err("fanout").code(),
            "E_FANOUT_LIMIT"
        );
        assert_eq!(
            caps.check_spawn(2, 0, 64).expect_err("agents").code(),
            "E_AGENT_LIMIT"
        );
    }

    #[tokio::test]
    async fn in_flight_limiter_bounds_concurrency() {
        let limiter = InFlightLimiter::new(2);
        let first = limiter.try_acquire().expect("permit 1");
        let second = limiter.try_acquire().expect("permit 2");
        assert_eq!(limiter.available_permits(), 0);
        assert_eq!(
            limiter.try_acquire().expect_err("full").code(),
            "E_SEMAPHORE_FULL"
        );
        drop(first);
        let _third = limiter.try_acquire().expect("permit released");
        drop(second);
        let _fourth = limiter.acquire().await.expect("queued acquire works");
    }

    #[test]
    fn report_totals_are_null_when_unknown() {
        let budget = RunBudget::new(BudgetLimits::default());
        let report = budget.report();
        assert_eq!(report.status, "COMPLETE");
        assert_eq!(report.prompt_tokens, None);
        assert_eq!(report.completion_tokens, None);
        assert_eq!(report.settled_usd.as_deref(), Some("0"));
    }
}
