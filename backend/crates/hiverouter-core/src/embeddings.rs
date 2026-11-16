//! Optional embedding support for memory recall: a provider-neutral [`Embedder`]
//! trait, one loopback HTTP adapter, exact f32 cosine scoring, and the
//! `memory_embeddings` persistence helpers.
//!
//! # Boundaries
//!
//! Embeddings are **optional and opt-in**. Nothing in this module makes a network
//! call unless the caller constructed a [`LoopbackHttpEmbedder`] from a
//! configuration whose `opt_in` flag is `true`. The adapter only accepts an
//! `http(s)` endpoint whose host is loopback (`localhost`, `127.0.0.0/8`, or
//! `::1`), rejects credentials in the URL, does not follow redirects, applies a
//! bounded timeout, and caps both request and response sizes.
//!
//! This module never changes trust. A vector is an index, not evidence: the
//! trust/verification labels on a memory row remain the only source of truth,
//! and [`crate::semantic_recall`] carries those labels through unchanged.
//!
//! # Determinism
//!
//! [`cosine_similarity_f32`] is an **exact f32** computation: the dot product and
//! both squared norms are accumulated in `f32` in input order and the result is
//! clamped to `-1.0..=1.0`. It performs no `f64` widening and no normalisation
//! step, so two callers with the same vectors get bit-identical input to the
//! same arithmetic sequence. There is no approximate-nearest-neighbour index and
//! no ANN dependency; callers are bounded to [`MAX_EMBEDDING_ENTRIES`].
//!
//! # Persistence
//!
//! [`MEMORY_EMBEDDINGS_SCHEMA`] / [`MEMORY_EMBEDDINGS_SCHEMA_DOWN`] are the
//! migration SQL for the `memory_embeddings` table. [`embeddings_migration`]
//! wraps them in the existing [`crate::db::Migration`] shape so the owning
//! database module can append the migration without duplicating SQL. The store
//! helpers ([`write_embedding`], [`read_embedding`],
//! [`read_embeddings_for_entries`], [`delete_embedding`]) operate on a bare
//! `&rusqlite::Connection` and are safe to call inside an existing transaction.
//!
//! # Test doubles
//!
//! Tests in `tests/embedding_*` implement [`Embedder`] with an in-memory
//! `TestEmbedder`. That double replaces the **external embedding service** — a
//! permitted external-boundary double — and never replaces recall logic or the
//! production adapter.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use async_trait::async_trait;
use hiverouter_contract::Timestamp;
use rusqlite::{params, params_from_iter, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};

use crate::semantic_recall::QueryEmbedder;

/// Hard ceiling on entries one embedding-backed recall may score.
pub const MAX_EMBEDDING_ENTRIES: usize = 4_096;
/// Hard ceiling on one embedding dimension.
pub const MAX_EMBEDDING_DIM: usize = 8_192;
/// Hard ceiling on texts in one `embed_texts` call.
pub const MAX_TEXTS_PER_CALL: usize = 64;
/// Hard ceiling on one input text, in bytes.
pub const MAX_TEXT_BYTES: usize = 16 * 1024;
/// Hard ceiling on the combined input, in bytes.
pub const MAX_TOTAL_INPUT_BYTES: usize = 256 * 1024;
/// Hard ceiling on one HTTP response body, in bytes.
pub const MAX_RESPONSE_BYTES: usize = 8 * 1024 * 1024;
/// Hard ceiling on one HTTP request body, in bytes.
pub const MAX_REQUEST_BYTES: usize = 2 * 1024 * 1024;
/// Entry ids per SQLite read; keeps the placeholder count well below any
/// build's `SQLITE_MAX_VARIABLE_NUMBER`.
pub const MAX_IDS_PER_QUERY: usize = 512;
/// Default request timeout.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);
/// Smallest accepted request timeout.
pub const MIN_TIMEOUT: Duration = Duration::from_millis(100);
/// Largest accepted request timeout.
pub const MAX_TIMEOUT: Duration = Duration::from_secs(120);
/// Maximum bytes of a model identifier.
pub const MAX_MODEL_ID_BYTES: usize = 128;
/// Maximum bytes of an entry id accepted by the persistence helpers.
pub const MAX_ENTRY_ID_BYTES: usize = 256;

/// `memory_embeddings` migration (forward SQL).
///
/// One row per `(entry_id, model_id)`. The vector is little-endian `f32` bytes;
/// `dim` is persisted so a reader can reject a truncated/corrupt blob instead of
/// guessing a shape. `ON DELETE CASCADE` keeps vectors from outliving the memory
/// entry they index.
pub const MEMORY_EMBEDDINGS_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS memory_embeddings (
    entry_id   TEXT NOT NULL,
    model_id   TEXT NOT NULL,
    dim        INTEGER NOT NULL CHECK (dim BETWEEN 1 AND 8192),
    vector     BLOB NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (entry_id, model_id),
    FOREIGN KEY (entry_id) REFERENCES memory_entries(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_memory_embeddings_model
    ON memory_embeddings(model_id, entry_id);
"#;

/// Inverse of [`MEMORY_EMBEDDINGS_SCHEMA`].
pub const MEMORY_EMBEDDINGS_SCHEMA_DOWN: &str =
    "DROP INDEX IF EXISTS idx_memory_embeddings_model; DROP TABLE IF EXISTS memory_embeddings;";

/// Builds the ordered migration descriptor for the supplied schema version.
///
/// The owning database module chooses the version; this module owns only the SQL
/// and the migration name (`memory_embeddings`).
#[must_use]
pub const fn embeddings_migration(version: i64) -> crate::db::Migration {
    crate::db::Migration {
        version,
        name: "memory_embeddings",
        up: MEMORY_EMBEDDINGS_SCHEMA,
        down: MEMORY_EMBEDDINGS_SCHEMA_DOWN,
    }
}

/// One model's identity and vector width.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EmbeddingModelMetadata {
    /// Operator-supplied model id sent on the wire and stored with the vector.
    pub model_id: String,
    /// Vector width; every vector for this model must have exactly this length.
    pub dim: usize,
}

impl EmbeddingModelMetadata {
    /// Validates and builds model metadata.
    ///
    /// # Errors
    /// [`EmbeddingError::InvalidConfig`] for a blank/oversized/control-character
    /// model id or a dimension outside `1..=MAX_EMBEDDING_DIM`.
    pub fn new(model_id: &str, dim: usize) -> Result<Self, EmbeddingError> {
        validate_model_id(model_id)?;
        validate_dim(dim)?;
        Ok(Self {
            model_id: model_id.trim().to_owned(),
            dim,
        })
    }
}

/// A provider-neutral embedding interface.
///
/// Implementations must return one vector per input text, in input order, with
/// exactly [`Embedder::dim`] finite `f32` components. The loopback HTTP adapter
/// is one implementation; tests replace the external service with a double.
#[async_trait]
pub trait Embedder: Send + Sync {
    /// Model id sent to the provider and persisted with vectors.
    fn model_id(&self) -> &str;

    /// Vector width this embedder produces.
    fn dim(&self) -> usize;

    /// Embeds each text, returning one vector per input in order.
    ///
    /// # Errors
    /// A named [`EmbeddingError`] for invalid input, a disabled/unreachable
    /// endpoint, a malformed or shape-invalid response, or a timeout.
    async fn embed_texts(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, EmbeddingError>;
}

/// Stable failure vocabulary for embedding configuration, transport, decoding,
/// and persistence.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EmbeddingError {
    /// The operator did not opt in; no request is made.
    Disabled,
    /// The endpoint/model/dimension/timeout configuration is invalid.
    InvalidConfig {
        /// Human-readable cause (never contains a credential).
        reason: String,
    },
    /// The endpoint is not a loopback `http(s)` URL.
    NotLoopback,
    /// One call had no texts, a blank text, or exceeded a bound.
    InvalidInput {
        /// Human-readable cause.
        reason: String,
    },
    /// Too many entries were supplied to a bounded operation.
    TooManyEntries {
        /// Supplied count.
        count: usize,
        /// Configured ceiling.
        max: usize,
    },
    /// The provider could not be reached.
    Transport(String),
    /// The request exceeded its configured deadline.
    Timeout,
    /// The provider returned a non-success status.
    HttpStatus {
        /// HTTP status code.
        status: u16,
    },
    /// The response body was larger than [`MAX_RESPONSE_BYTES`].
    ResponseTooLarge {
        /// Observed/predicted body size.
        bytes: usize,
        /// Configured ceiling.
        max: usize,
    },
    /// The response was not the documented embedding shape.
    MalformedResponse(String),
    /// The response held the wrong number of vectors.
    VectorCount {
        /// Vectors expected.
        expected: usize,
        /// Vectors returned.
        actual: usize,
    },
    /// A vector had the wrong width.
    DimensionMismatch {
        /// Required width.
        expected: usize,
        /// Observed width.
        actual: usize,
    },
    /// A vector component was NaN or infinite.
    NonFinite {
        /// Position of the offending vector in the batch.
        index: usize,
    },
    /// A vector had a zero norm and therefore no cosine.
    ZeroVector {
        /// Position of the offending vector in the batch.
        index: usize,
    },
    /// Cosine inputs had unequal (or empty) shapes.
    CosineShape {
        /// Left vector width.
        left: usize,
        /// Right vector width.
        right: usize,
    },
    /// A persistence read/write failed or a stored row was corrupt.
    Store(String),
}

impl EmbeddingError {
    /// Stable refusal/fallback code.
    #[must_use]
    pub fn code(&self) -> &'static str {
        match self {
            Self::Disabled => "E_EMBEDDING_DISABLED",
            Self::InvalidConfig { .. } => "E_EMBEDDING_CONFIG",
            Self::NotLoopback => "E_EMBEDDING_NOT_LOOPBACK",
            Self::InvalidInput { .. } => "E_EMBEDDING_INPUT",
            Self::TooManyEntries { .. } => "E_EMBEDDING_BOUND",
            Self::Transport(_) => "E_EMBEDDING_TRANSPORT",
            Self::Timeout => "E_EMBEDDING_TIMEOUT",
            Self::HttpStatus { .. } => "E_EMBEDDING_HTTP_STATUS",
            Self::ResponseTooLarge { .. } => "E_EMBEDDING_RESPONSE_TOO_LARGE",
            Self::MalformedResponse(_) => "E_EMBEDDING_RESPONSE",
            Self::VectorCount { .. } => "E_EMBEDDING_VECTOR_COUNT",
            Self::DimensionMismatch { .. } => "E_EMBEDDING_DIM",
            Self::NonFinite { .. } => "E_EMBEDDING_NON_FINITE",
            Self::ZeroVector { .. } => "E_EMBEDDING_ZERO_VECTOR",
            Self::CosineShape { .. } => "E_EMBEDDING_COSINE_SHAPE",
            Self::Store(_) => "E_EMBEDDING_STORE",
        }
    }

    /// Human-readable detail. Never includes an API key or request body.
    #[must_use]
    pub fn message(&self) -> String {
        match self {
            Self::Disabled => "embeddings are disabled; the operator did not opt in".to_owned(),
            Self::InvalidConfig { reason } => format!("invalid embedding configuration: {reason}"),
            Self::NotLoopback => "embedding endpoint must be a loopback http(s) URL".to_owned(),
            Self::InvalidInput { reason } => format!("invalid embedding input: {reason}"),
            Self::TooManyEntries { count, max } => {
                format!("embedding operation received {count} entries, above the {max} bound")
            }
            Self::Transport(detail) => format!("embedding transport failed: {detail}"),
            Self::Timeout => "embedding request timed out".to_owned(),
            Self::HttpStatus { status } => {
                format!("embedding endpoint returned HTTP {status}")
            }
            Self::ResponseTooLarge { bytes, max } => {
                format!("embedding response body {bytes} bytes exceeds {max} bytes")
            }
            Self::MalformedResponse(detail) => {
                format!("embedding response is not valid: {detail}")
            }
            Self::VectorCount { expected, actual } => {
                format!("embedding response returned {actual} vectors, expected {expected}")
            }
            Self::DimensionMismatch { expected, actual } => {
                format!("embedding vector has {actual} dimensions, expected {expected}")
            }
            Self::NonFinite { index } => {
                format!("embedding vector {index} contains a non-finite component")
            }
            Self::ZeroVector { index } => {
                format!("embedding vector {index} has a zero norm")
            }
            Self::CosineShape { left, right } => {
                format!("cosine requires equal non-empty vectors, got {left} and {right}")
            }
            Self::Store(detail) => format!("memory_embeddings store error: {detail}"),
        }
    }
}

impl std::fmt::Display for EmbeddingError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message())
    }
}

impl std::error::Error for EmbeddingError {}

impl From<rusqlite::Error> for EmbeddingError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Store(error.to_string())
    }
}

fn validate_model_id(model_id: &str) -> Result<(), EmbeddingError> {
    let trimmed = model_id.trim();
    if trimmed.is_empty() || trimmed.len() > MAX_MODEL_ID_BYTES {
        return Err(EmbeddingError::InvalidConfig {
            reason: format!("model id must be 1..={MAX_MODEL_ID_BYTES} bytes"),
        });
    }
    if trimmed.chars().any(char::is_control) {
        return Err(EmbeddingError::InvalidConfig {
            reason: "model id must not contain control characters".to_owned(),
        });
    }
    Ok(())
}

fn validate_dim(dim: usize) -> Result<(), EmbeddingError> {
    if dim == 0 || dim > MAX_EMBEDDING_DIM {
        return Err(EmbeddingError::InvalidConfig {
            reason: format!("dimension must be 1..={MAX_EMBEDDING_DIM}"),
        });
    }
    Ok(())
}

fn validate_timeout(timeout: Duration) -> Result<(), EmbeddingError> {
    if timeout < MIN_TIMEOUT || timeout > MAX_TIMEOUT {
        return Err(EmbeddingError::InvalidConfig {
            reason: format!(
                "timeout must be between {}ms and {}s",
                MIN_TIMEOUT.as_millis(),
                MAX_TIMEOUT.as_secs()
            ),
        });
    }
    Ok(())
}

/// Validated operator configuration for the loopback adapter.
#[derive(Clone, Debug)]
pub struct EmbeddingEndpointConfig {
    endpoint: reqwest::Url,
    model: EmbeddingModelMetadata,
    timeout: Duration,
    opt_in: bool,
}

impl EmbeddingEndpointConfig {
    /// Validates an operator endpoint, model id, dimension, timeout, and opt-in.
    ///
    /// `opt_in == false` is a valid configuration value (the adapter then
    /// refuses with [`EmbeddingError::Disabled`]); it is not a validation error.
    ///
    /// # Errors
    /// [`EmbeddingError::InvalidConfig`] for a malformed URL, blank model id,
    /// invalid dimension, or out-of-range timeout; [`EmbeddingError::NotLoopback`]
    /// when the URL does not name a loopback host.
    pub fn new(
        endpoint: &str,
        model_id: &str,
        dim: usize,
        timeout: Duration,
        opt_in: bool,
    ) -> Result<Self, EmbeddingError> {
        let endpoint = endpoint.trim();
        if endpoint.is_empty() {
            return Err(EmbeddingError::InvalidConfig {
                reason: "endpoint must not be blank".to_owned(),
            });
        }
        let parsed =
            reqwest::Url::parse(endpoint).map_err(|error| EmbeddingError::InvalidConfig {
                reason: format!("endpoint is not a valid URL: {error}"),
            })?;
        validate_loopback_endpoint(&parsed)?;
        validate_timeout(timeout)?;
        Ok(Self {
            endpoint: parsed,
            model: EmbeddingModelMetadata::new(model_id, dim)?,
            timeout,
            opt_in,
        })
    }

    /// Endpoint URL.
    #[must_use]
    pub fn endpoint(&self) -> &reqwest::Url {
        &self.endpoint
    }

    /// Model metadata.
    #[must_use]
    pub fn model(&self) -> &EmbeddingModelMetadata {
        &self.model
    }

    /// Configured request timeout.
    #[must_use]
    pub const fn timeout(&self) -> Duration {
        self.timeout
    }

    /// Whether the operator opted in.
    #[must_use]
    pub const fn is_opted_in(&self) -> bool {
        self.opt_in
    }
}

fn validate_loopback_endpoint(url: &reqwest::Url) -> Result<(), EmbeddingError> {
    if url.scheme() != "http" && url.scheme() != "https" {
        return Err(EmbeddingError::InvalidConfig {
            reason: "endpoint scheme must be http or https".to_owned(),
        });
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(EmbeddingError::InvalidConfig {
            reason: "endpoint must not contain credentials".to_owned(),
        });
    }
    let Some(host) = url.host_str() else {
        return Err(EmbeddingError::InvalidConfig {
            reason: "endpoint must have a host".to_owned(),
        });
    };
    if host.eq_ignore_ascii_case("localhost") {
        return Ok(());
    }
    match host.parse::<std::net::IpAddr>() {
        Ok(address) if address.is_loopback() => Ok(()),
        _ => Err(EmbeddingError::NotLoopback),
    }
}

/// One-loopback-only HTTP adapter for an operator-supplied embedding endpoint.
///
/// The request body is the documented OpenAI-compatible shape
/// `{"model": "...", "input": ["..."]}`. The response is `{"data":[{"embedding":
/// [...], "index": 0}]}`; `index` is optional and, when present, is used to
/// restore input order. Redirects are not followed and credentials are not
/// accepted.
#[derive(Clone, Debug)]
pub struct LoopbackHttpEmbedder {
    endpoint: reqwest::Url,
    model: EmbeddingModelMetadata,
    client: reqwest::Client,
}

impl LoopbackHttpEmbedder {
    /// Builds the adapter. Refuses without an explicit opt-in.
    ///
    /// # Errors
    /// [`EmbeddingError::Disabled`] when `config.is_opted_in()` is false;
    /// [`EmbeddingError::InvalidConfig`] when the HTTP client cannot be built.
    pub fn new(config: EmbeddingEndpointConfig) -> Result<Self, EmbeddingError> {
        if !config.opt_in {
            return Err(EmbeddingError::Disabled);
        }
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(config.timeout)
            .no_proxy()
            .build()
            .map_err(|error| EmbeddingError::InvalidConfig {
                reason: format!("could not build the embedding HTTP client: {error}"),
            })?;
        Ok(Self {
            endpoint: config.endpoint,
            model: config.model,
            client,
        })
    }

    /// Endpoint URL.
    #[must_use]
    pub fn endpoint(&self) -> &reqwest::Url {
        &self.endpoint
    }

    async fn post_embeddings(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, EmbeddingError> {
        let request = EmbeddingRequest {
            model: &self.model.model_id,
            input: texts,
        };
        let body = serde_json::to_vec(&request).map_err(|error| EmbeddingError::InvalidConfig {
            reason: format!("could not serialize the embedding request: {error}"),
        })?;
        if body.len() > MAX_REQUEST_BYTES {
            return Err(EmbeddingError::InvalidInput {
                reason: format!("embedding request body exceeds {MAX_REQUEST_BYTES} bytes"),
            });
        }
        let response = self
            .client
            .post(self.endpoint.clone())
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(body)
            .send()
            .await
            .map_err(|error| {
                if error.is_timeout() {
                    EmbeddingError::Timeout
                } else {
                    EmbeddingError::Transport(error.to_string())
                }
            })?;
        let status = response.status();
        if !status.is_success() {
            return Err(EmbeddingError::HttpStatus {
                status: status.as_u16(),
            });
        }
        if response
            .content_length()
            .is_some_and(|length| length > MAX_RESPONSE_BYTES as u64)
        {
            return Err(EmbeddingError::ResponseTooLarge {
                bytes: response.content_length().unwrap_or(0) as usize,
                max: MAX_RESPONSE_BYTES,
            });
        }
        let mut bytes = Vec::new();
        let mut response = response;
        loop {
            let chunk = response.chunk().await.map_err(|error| {
                if error.is_timeout() {
                    EmbeddingError::Timeout
                } else {
                    EmbeddingError::Transport(error.to_string())
                }
            })?;
            let Some(chunk) = chunk else { break };
            if bytes.len().saturating_add(chunk.len()) > MAX_RESPONSE_BYTES {
                return Err(EmbeddingError::ResponseTooLarge {
                    bytes: bytes.len().saturating_add(chunk.len()),
                    max: MAX_RESPONSE_BYTES,
                });
            }
            bytes.extend_from_slice(&chunk);
        }
        let decoded: EmbeddingResponse = serde_json::from_slice(&bytes).map_err(|error| {
            EmbeddingError::MalformedResponse(format!("response is not JSON: {error}"))
        })?;
        order_and_validate(decoded.data, texts.len(), self.model.dim)
    }
}

#[derive(Serialize)]
struct EmbeddingRequest<'a> {
    model: &'a str,
    input: &'a [&'a str],
}

#[derive(Deserialize)]
struct EmbeddingResponse {
    data: Vec<EmbeddingDatum>,
}

#[derive(Deserialize)]
struct EmbeddingDatum {
    embedding: Vec<f32>,
    #[serde(default)]
    index: Option<usize>,
}

fn order_and_validate(
    data: Vec<EmbeddingDatum>,
    expected: usize,
    dim: usize,
) -> Result<Vec<Vec<f32>>, EmbeddingError> {
    if data.len() != expected {
        return Err(EmbeddingError::VectorCount {
            expected,
            actual: data.len(),
        });
    }
    let has_index = data.iter().any(|datum| datum.index.is_some());
    let mut ordered: Vec<Option<Vec<f32>>> = vec![None; expected];
    if has_index {
        for datum in data {
            let Some(index) = datum.index else {
                return Err(EmbeddingError::MalformedResponse(
                    "response mixes indexed and unindexed vectors".to_owned(),
                ));
            };
            if index >= expected {
                return Err(EmbeddingError::MalformedResponse(format!(
                    "response vector index {index} is out of range for {expected} inputs"
                )));
            }
            if ordered[index].is_some() {
                return Err(EmbeddingError::MalformedResponse(format!(
                    "response repeats vector index {index}"
                )));
            }
            ordered[index] = Some(datum.embedding);
        }
        ordered
            .into_iter()
            .enumerate()
            .map(|(index, vector)| {
                vector
                    .ok_or_else(|| {
                        EmbeddingError::MalformedResponse(format!(
                            "response is missing vector index {index}"
                        ))
                    })
                    .and_then(|vector| validate_vector(&vector, dim, index))
            })
            .collect()
    } else {
        data.into_iter()
            .enumerate()
            .map(|(index, datum)| validate_vector(&datum.embedding, dim, index))
            .collect()
    }
}

fn validate_vector(
    vector: &[f32],
    expected_dim: usize,
    index: usize,
) -> Result<Vec<f32>, EmbeddingError> {
    if vector.len() != expected_dim {
        return Err(EmbeddingError::DimensionMismatch {
            expected: expected_dim,
            actual: vector.len(),
        });
    }
    let mut norm_squared = 0.0_f32;
    for component in vector {
        if !component.is_finite() {
            return Err(EmbeddingError::NonFinite { index });
        }
        norm_squared += component * component;
    }
    if norm_squared == 0.0 || !norm_squared.is_finite() {
        return Err(EmbeddingError::ZeroVector { index });
    }
    Ok(vector.to_vec())
}

#[async_trait]
impl Embedder for LoopbackHttpEmbedder {
    fn model_id(&self) -> &str {
        &self.model.model_id
    }

    fn dim(&self) -> usize {
        self.model.dim
    }

    async fn embed_texts(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, EmbeddingError> {
        validate_texts(texts)?;
        self.post_embeddings(texts).await
    }
}

#[async_trait]
impl QueryEmbedder for LoopbackHttpEmbedder {
    fn model_id(&self) -> &str {
        &self.model.model_id
    }

    fn dim(&self) -> usize {
        self.model.dim
    }

    async fn embed_texts(
        &self,
        texts: &[&str],
    ) -> Result<Vec<Vec<f32>>, crate::semantic_recall::QueryEmbedError> {
        <Self as Embedder>::embed_texts(self, texts)
            .await
            .map_err(|error| {
                crate::semantic_recall::QueryEmbedError::new(error.code(), error.message())
            })
    }

    fn cosine(
        &self,
        query: &[f32],
        candidate: &[f32],
    ) -> Result<f32, crate::semantic_recall::QueryEmbedError> {
        cosine_similarity_f32(query, candidate).map_err(|error| {
            crate::semantic_recall::QueryEmbedError::new(error.code(), error.message())
        })
    }
}

fn validate_texts(texts: &[&str]) -> Result<(), EmbeddingError> {
    if texts.is_empty() {
        return Err(EmbeddingError::InvalidInput {
            reason: "at least one text is required".to_owned(),
        });
    }
    if texts.len() > MAX_TEXTS_PER_CALL {
        return Err(EmbeddingError::InvalidInput {
            reason: format!("at most {MAX_TEXTS_PER_CALL} texts are accepted per call"),
        });
    }
    let mut total = 0usize;
    for (index, text) in texts.iter().enumerate() {
        if text.trim().is_empty() {
            return Err(EmbeddingError::InvalidInput {
                reason: format!("text {index} is blank"),
            });
        }
        if text.len() > MAX_TEXT_BYTES {
            return Err(EmbeddingError::InvalidInput {
                reason: format!("text {index} exceeds {MAX_TEXT_BYTES} bytes"),
            });
        }
        total = total.saturating_add(text.len());
    }
    if total > MAX_TOTAL_INPUT_BYTES {
        return Err(EmbeddingError::InvalidInput {
            reason: format!("combined input exceeds {MAX_TOTAL_INPUT_BYTES} bytes"),
        });
    }
    Ok(())
}

/// Exact f32 cosine similarity in `-1.0..=1.0`.
///
/// Both norms and the dot product are accumulated in `f32` in input order. Empty,
/// unequal, non-finite, or zero-norm inputs are refused by name; a cosine is
/// never guessed from a malformed vector.
///
/// # Errors
/// [`EmbeddingError::CosineShape`] for empty/unequal shapes,
/// [`EmbeddingError::NonFinite`] for a non-finite component, and
/// [`EmbeddingError::ZeroVector`] when either norm is zero.
pub fn cosine_similarity_f32(left: &[f32], right: &[f32]) -> Result<f32, EmbeddingError> {
    if left.is_empty() || left.len() != right.len() {
        return Err(EmbeddingError::CosineShape {
            left: left.len(),
            right: right.len(),
        });
    }
    let mut dot = 0.0_f32;
    let mut left_norm = 0.0_f32;
    let mut right_norm = 0.0_f32;
    for (index, (left_component, right_component)) in left.iter().zip(right.iter()).enumerate() {
        if !left_component.is_finite() || !right_component.is_finite() {
            return Err(EmbeddingError::NonFinite { index });
        }
        dot += left_component * right_component;
        left_norm += left_component * left_component;
        right_norm += right_component * right_component;
    }
    if left_norm == 0.0 || right_norm == 0.0 {
        return Err(EmbeddingError::ZeroVector { index: 0 });
    }
    let cosine = dot / (left_norm.sqrt() * right_norm.sqrt());
    if !cosine.is_finite() {
        return Err(EmbeddingError::NonFinite { index: 0 });
    }
    Ok(cosine.clamp(-1.0, 1.0))
}

/// Exact cosine mapped onto the recall score range `0.0..=1.0`.
///
/// Negative cosine becomes `0.0`; this is a score projection, not a trust or
/// provenance signal.
///
/// # Errors
/// Every [`cosine_similarity_f32`] refusal.
pub fn cosine_score_f32(left: &[f32], right: &[f32]) -> Result<f32, EmbeddingError> {
    Ok(cosine_similarity_f32(left, right)?.max(0.0))
}

/// One persisted vector row.
#[derive(Clone, Debug, PartialEq)]
pub struct StoredEmbedding {
    /// Owning memory entry.
    pub entry_id: String,
    /// Model that produced the vector.
    pub model_id: String,
    /// Vector width.
    pub dim: usize,
    /// Vector components.
    pub vector: Vec<f32>,
}

fn encode_vector(vector: &[f32]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(vector.len() * 4);
    for component in vector {
        bytes.extend_from_slice(&component.to_le_bytes());
    }
    bytes
}

fn decode_vector(
    entry_id: &str,
    model_id: &str,
    dim: i64,
    bytes: Vec<u8>,
) -> Result<StoredEmbedding, EmbeddingError> {
    let dim = usize::try_from(dim).map_err(|_| {
        EmbeddingError::Store(format!(
            "memory_embeddings row {entry_id} has a negative dimension"
        ))
    })?;
    if dim == 0 || dim > MAX_EMBEDDING_DIM {
        return Err(EmbeddingError::Store(format!(
            "memory_embeddings row {entry_id} has dimension {dim} outside 1..={MAX_EMBEDDING_DIM}"
        )));
    }
    let expected_bytes = dim.checked_mul(4).ok_or_else(|| {
        EmbeddingError::Store(format!(
            "memory_embeddings row {entry_id} has an oversized dimension"
        ))
    })?;
    if bytes.len() != expected_bytes {
        return Err(EmbeddingError::Store(format!(
            "memory_embeddings row {entry_id} stores {} bytes, expected {expected_bytes}",
            bytes.len()
        )));
    }
    let mut vector = Vec::with_capacity(dim);
    for chunk in bytes.chunks_exact(4) {
        vector.push(f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]));
    }
    let mut norm_squared = 0.0_f32;
    for (index, component) in vector.iter().enumerate() {
        if !component.is_finite() {
            return Err(EmbeddingError::Store(format!(
                "memory_embeddings row {entry_id} component {index} is not finite"
            )));
        }
        norm_squared += component * component;
    }
    if norm_squared == 0.0 || !norm_squared.is_finite() {
        return Err(EmbeddingError::Store(format!(
            "memory_embeddings row {entry_id} has a zero or non-finite norm"
        )));
    }
    Ok(StoredEmbedding {
        entry_id: entry_id.to_owned(),
        model_id: model_id.to_owned(),
        dim,
        vector,
    })
}

fn validate_entry_id(entry_id: &str) -> Result<(), EmbeddingError> {
    if entry_id.trim().is_empty() || entry_id.len() > MAX_ENTRY_ID_BYTES {
        return Err(EmbeddingError::InvalidInput {
            reason: format!("entry id must be 1..={MAX_ENTRY_ID_BYTES} bytes"),
        });
    }
    Ok(())
}

/// Inserts or replaces one vector for `(entry_id, model_id)`.
///
/// # Errors
/// [`EmbeddingError::InvalidInput`] for a blank/oversized entry id,
/// [`EmbeddingError::InvalidConfig`] for invalid model metadata/dimension,
/// [`EmbeddingError::NonFinite`]/[`EmbeddingError::ZeroVector`] for an unusable
/// vector, and [`EmbeddingError::Store`] for a SQLite/FK failure.
pub fn write_embedding(
    connection: &Connection,
    entry_id: &str,
    model_id: &str,
    vector: &[f32],
) -> Result<(), EmbeddingError> {
    validate_entry_id(entry_id)?;
    let model = EmbeddingModelMetadata::new(model_id, vector.len())?;
    validate_vector(vector, model.dim, 0)?;
    let encoded = encode_vector(vector);
    let now = Timestamp::now();
    connection
        .execute(
            "INSERT INTO memory_embeddings (entry_id, model_id, dim, vector, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(entry_id, model_id) DO UPDATE SET
                 dim = excluded.dim,
                 vector = excluded.vector,
                 created_at = excluded.created_at",
            params![
                entry_id,
                model.model_id,
                i64::try_from(model.dim).unwrap_or(i64::MAX),
                encoded,
                now.as_str()
            ],
        )
        .map_err(|error| {
            EmbeddingError::Store(format!(
                "could not write embedding for entry {entry_id}: {error}"
            ))
        })?;
    Ok(())
}

/// Reads one vector for `(entry_id, model_id)`.
///
/// # Errors
/// [`EmbeddingError::Store`] for a SQLite failure or a corrupt stored row.
pub fn read_embedding(
    connection: &Connection,
    entry_id: &str,
    model_id: &str,
) -> Result<Option<StoredEmbedding>, EmbeddingError> {
    let row: Option<(i64, Vec<u8>)> = connection
        .query_row(
            "SELECT dim, vector FROM memory_embeddings
             WHERE entry_id = ?1 AND model_id = ?2",
            params![entry_id, model_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(|error| {
            EmbeddingError::Store(format!(
                "could not read embedding for entry {entry_id}: {error}"
            ))
        })?;
    row.map(|(dim, bytes)| decode_vector(entry_id, model_id, dim, bytes))
        .transpose()
}

/// Reads every vector present for `entry_ids` under one model id.
///
/// This is the bounded batch read used by hybrid recall. Entries without a row
/// are simply absent from the map; completeness is decided by the caller.
///
/// # Errors
/// [`EmbeddingError::TooManyEntries`] above [`MAX_EMBEDDING_ENTRIES`],
/// [`EmbeddingError::InvalidInput`] for a blank id, and [`EmbeddingError::Store`]
/// for a SQLite failure or corrupt row.
pub fn read_embeddings_for_entries(
    connection: &Connection,
    model_id: &str,
    entry_ids: &[&str],
) -> Result<HashMap<String, Vec<f32>>, EmbeddingError> {
    validate_model_id(model_id)?;
    if entry_ids.len() > MAX_EMBEDDING_ENTRIES {
        return Err(EmbeddingError::TooManyEntries {
            count: entry_ids.len(),
            max: MAX_EMBEDDING_ENTRIES,
        });
    }
    let mut seen = HashSet::with_capacity(entry_ids.len());
    let mut unique: Vec<&str> = Vec::with_capacity(entry_ids.len());
    for entry_id in entry_ids {
        validate_entry_id(entry_id)?;
        if seen.insert(*entry_id) {
            unique.push(*entry_id);
        }
    }
    if unique.is_empty() {
        return Ok(HashMap::new());
    }
    let mut vectors = HashMap::with_capacity(unique.len());
    for chunk in unique.chunks(MAX_IDS_PER_QUERY) {
        let placeholders = (0..chunk.len())
            .map(|index| format!("?{}", index + 2))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "SELECT entry_id, dim, vector FROM memory_embeddings
             WHERE model_id = ?1 AND entry_id IN ({placeholders})"
        );
        let mut statement = connection.prepare(&sql).map_err(|error| {
            EmbeddingError::Store(format!(
                "could not prepare the embedding batch read: {error}"
            ))
        })?;
        let parameters = std::iter::once(model_id).chain(chunk.iter().copied());
        let rows = statement
            .query_map(params_from_iter(parameters), |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                ))
            })
            .map_err(|error| {
                EmbeddingError::Store(format!("could not read embeddings in batch: {error}"))
            })?;
        for row in rows {
            let (entry_id, dim, bytes) = row.map_err(|error| {
                EmbeddingError::Store(format!("could not decode an embedding row: {error}"))
            })?;
            let stored = decode_vector(&entry_id, model_id, dim, bytes)?;
            vectors.insert(entry_id, stored.vector);
        }
    }
    Ok(vectors)
}

/// Deletes one vector.
///
/// # Errors
/// [`EmbeddingError::Store`] for a SQLite failure.
pub fn delete_embedding(
    connection: &Connection,
    entry_id: &str,
    model_id: &str,
) -> Result<bool, EmbeddingError> {
    let changed = connection
        .execute(
            "DELETE FROM memory_embeddings WHERE entry_id = ?1 AND model_id = ?2",
            params![entry_id, model_id],
        )
        .map_err(|error| {
            EmbeddingError::Store(format!(
                "could not delete embedding for entry {entry_id}: {error}"
            ))
        })?;
    Ok(changed > 0)
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn exact_cosine_handles_orthogonal_opposite_and_identical_vectors() {
        let left = [1.0_f32, 0.0, 0.0];
        let right = [0.0_f32, 1.0, 0.0];
        assert_eq!(cosine_similarity_f32(&left, &left).expect("identical"), 1.0);
        assert_eq!(
            cosine_similarity_f32(&left, &right).expect("orthogonal"),
            0.0
        );
        let opposite = [-1.0_f32, 0.0, 0.0];
        assert_eq!(
            cosine_similarity_f32(&left, &opposite).expect("opposite"),
            -1.0
        );
        assert_eq!(cosine_score_f32(&left, &opposite).expect("projected"), 0.0);
    }

    #[test]
    fn cosine_refuses_zero_norm_and_shape_mismatch() {
        assert_eq!(
            cosine_similarity_f32(&[0.0_f32, 0.0], &[1.0_f32, 0.0]),
            Err(EmbeddingError::ZeroVector { index: 0 })
        );
        assert_eq!(
            cosine_similarity_f32(&[1.0_f32], &[1.0_f32, 0.0]),
            Err(EmbeddingError::CosineShape { left: 1, right: 2 })
        );
    }

    #[test]
    fn loopback_endpoint_validation_rejects_non_loopback_and_credentials() {
        let config = EmbeddingEndpointConfig::new(
            "http://127.0.0.1:9999/v1/embeddings",
            "test-embed",
            8,
            DEFAULT_TIMEOUT,
            true,
        );
        assert!(config.is_ok(), "{config:?}");
        assert_eq!(
            EmbeddingEndpointConfig::new(
                "http://10.0.0.5:9999/v1/embeddings",
                "test-embed",
                8,
                DEFAULT_TIMEOUT,
                true
            )
            .expect_err("public address must refuse"),
            EmbeddingError::NotLoopback
        );
        assert!(EmbeddingEndpointConfig::new(
            "http://user:pass@127.0.0.1:9999/v1/embeddings",
            "test-embed",
            8,
            DEFAULT_TIMEOUT,
            true
        )
        .is_err());
    }

    #[test]
    fn adapter_without_opt_in_refuses_before_any_request() {
        let config = EmbeddingEndpointConfig::new(
            "http://127.0.0.1:9999/v1/embeddings",
            "test-embed",
            8,
            DEFAULT_TIMEOUT,
            false,
        )
        .expect("valid config");
        assert_eq!(
            LoopbackHttpEmbedder::new(config).expect_err("must refuse"),
            EmbeddingError::Disabled
        );
        assert_eq!(EmbeddingError::Disabled.code(), "E_EMBEDDING_DISABLED");
    }

    #[test]
    fn vector_codec_roundtrips_exactly() {
        let vector = [0.25_f32, -1.5, 3.0, 0.0];
        let bytes = encode_vector(&vector);
        let decoded = decode_vector(
            "entry:test",
            "model",
            i64::try_from(vector.len()).expect("dim"),
            bytes,
        )
        .expect("decode");
        assert_eq!(decoded.vector, vector);
        assert_eq!(decoded.dim, vector.len());
    }

    #[test]
    fn truncated_blob_is_refused_not_guessed() {
        let error = decode_vector("entry:test", "model", 2, vec![0, 1, 2]).expect_err("truncated");
        assert_eq!(error.code(), "E_EMBEDDING_STORE");
        assert!(error.message().contains("stores 3 bytes"));
    }
}
