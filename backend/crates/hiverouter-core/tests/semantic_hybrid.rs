//! Hybrid recall tests: exact-vector paraphrase recall, explicit degraded
//! fallback, model refusal, and trust preservation.
//!
//! `TestEmbedder` below is an **external-boundary test double**: it stands in for
//! the embedding service (the network/model boundary), not for recall logic,
//! cosine scoring, provenance handling, or the persistence layer. Those run
//! unchanged from production code.

#![allow(dead_code)]

mod db {
    pub use hiverouter_core::db::*;
}

mod semantic_recall {
    pub use hiverouter_core::semantic_recall::*;
}

#[path = "../src/embeddings.rs"]
mod embeddings;

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use hiverouter_contract::{AgentState, TaskId};
use hiverouter_core::db::Database;
use hiverouter_core::entities::TaskRecord;
use hiverouter_core::store::{MemoryEntryRecord, RunStore};

use embeddings::{
    cosine_similarity_f32, embeddings_migration, read_embeddings_for_entries, write_embedding,
    Embedder, EmbeddingError, EmbeddingModelMetadata,
};
use semantic_recall::{
    recall_hybrid, QueryEmbedError, QueryEmbedder, RecallError, RecallQuery, Verification,
    VECTOR_ALGORITHM,
};

const MODEL: &str = "test-embed";
const DIM: usize = 4;

struct TestEmbedder {
    model: EmbeddingModelMetadata,
    query_vectors: HashMap<String, Vec<f32>>,
    failure: Option<QueryEmbedError>,
}

impl TestEmbedder {
    fn new(query_vectors: HashMap<String, Vec<f32>>) -> Self {
        Self {
            model: EmbeddingModelMetadata::new(MODEL, DIM).expect("model"),
            query_vectors,
            failure: None,
        }
    }

    fn failing(error: QueryEmbedError) -> Self {
        Self {
            model: EmbeddingModelMetadata::new(MODEL, DIM).expect("model"),
            query_vectors: HashMap::new(),
            failure: Some(error),
        }
    }
}

#[async_trait]
impl Embedder for TestEmbedder {
    fn model_id(&self) -> &str {
        &self.model.model_id
    }

    fn dim(&self) -> usize {
        self.model.dim
    }

    async fn embed_texts(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, EmbeddingError> {
        if let Some(error) = &self.failure {
            return Err(EmbeddingError::Transport(error.message.clone()));
        }
        texts
            .iter()
            .map(|text| {
                self.query_vectors
                    .get(*text)
                    .cloned()
                    .ok_or_else(|| EmbeddingError::InvalidInput {
                        reason: format!("test double has no vector for {text:?}"),
                    })
            })
            .collect()
    }
}

#[async_trait]
impl QueryEmbedder for TestEmbedder {
    fn model_id(&self) -> &str {
        &self.model.model_id
    }

    fn dim(&self) -> usize {
        self.model.dim
    }

    async fn embed_texts(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, QueryEmbedError> {
        if let Some(error) = &self.failure {
            return Err(error.clone());
        }
        texts
            .iter()
            .map(|text| {
                self.query_vectors.get(*text).cloned().ok_or_else(|| {
                    QueryEmbedError::new(
                        "E_EMBEDDING_INPUT",
                        format!("test double has no vector for {text:?}"),
                    )
                })
            })
            .collect()
    }

    fn cosine(&self, query: &[f32], candidate: &[f32]) -> Result<f32, QueryEmbedError> {
        cosine_similarity_f32(query, candidate)
            .map_err(|error| QueryEmbedError::new(error.code(), error.message()))
    }
}

struct Fixture {
    database: Arc<Database>,
    entry_a: MemoryEntryRecord,
    entry_b: MemoryEntryRecord,
}

fn fixture(label: &str) -> Fixture {
    let database = Arc::new(Database::open_in_memory().expect("in-memory database"));
    database.migrate().expect("migrations");
    let store = RunStore::new(Arc::clone(&database));
    let run = store
        .create_run("p", label, AgentState::Running)
        .expect("run");
    let task = TaskRecord {
        id: TaskId::new(),
        run_id: run.id,
        parent_task_id: None,
        role: "worker".to_owned(),
        depth: 1,
        spawn_seq: 1,
        state: AgentState::Running,
        rev: 0,
    };
    store.insert_task(&task).expect("task");
    let entry_a = store
        .record_memory_entry(
            run.id,
            task.id,
            "worker",
            "deploy-cache",
            "Cache entries must be invalidated whenever a deployment ships",
            "fact",
            "unverified",
        )
        .expect("entry a");
    let entry_b = store
        .record_memory_entry(
            run.id,
            task.id,
            "worker",
            "token-budget",
            "The token budget for a run is capped at 120000 tokens",
            "constraint",
            "unverified",
        )
        .expect("entry b");
    database
        .with_connection(|connection| {
            connection.execute_batch(embeddings_migration(33).up)?;
            Ok(())
        })
        .expect("embedding migration");

    let entries = store
        .memory_entries_for_bank_scoped(run.id, &format!("bank:branch:{}", task.id), 16)
        .expect("entries");
    assert_eq!(entries.len(), 2);
    let entry_a = entries
        .iter()
        .find(|entry| entry.id == entry_a)
        .cloned()
        .expect("stored entry a");
    let entry_b = entries
        .iter()
        .find(|entry| entry.id == entry_b)
        .cloned()
        .expect("stored entry b");

    database
        .with_connection(|connection| {
            write_embedding(connection, &entry_a.id, MODEL, &[1.0, 0.0, 0.0, 0.0])
                .map_err(|error| rusqlite::Error::InvalidParameterName(error.message()))?;
            write_embedding(connection, &entry_b.id, MODEL, &[0.0, 1.0, 0.0, 0.0])
                .map_err(|error| rusqlite::Error::InvalidParameterName(error.message()))?;
            Ok(())
        })
        .expect("vectors");

    Fixture {
        database,
        entry_a,
        entry_b,
    }
}

fn load_vectors(fixture: &Fixture) -> HashMap<String, Vec<f32>> {
    let ids = [fixture.entry_a.id.as_str(), fixture.entry_b.id.as_str()];
    fixture
        .database
        .with_connection(|connection| {
            read_embeddings_for_entries(connection, MODEL, &ids)
                .map_err(|error| rusqlite::Error::InvalidParameterName(error.message()))
        })
        .expect("read vectors")
}

#[tokio::test]
async fn paraphrased_query_uses_exact_vectors_not_lexical_overlap() {
    let fixture = fixture("hybrid paraphrase");
    let vectors = load_vectors(&fixture);
    let mut query_vectors = HashMap::new();
    // The query shares no content term with either stored value, so the lexical
    // path has no hit. The vector path still finds the cache entry: this is the
    // paraphrase guarantee.
    query_vectors.insert(
        "how do we evict stale copies after a release".to_owned(),
        vec![1.0, 0.0, 0.0, 0.0],
    );
    let embedder = TestEmbedder::new(query_vectors);

    let response = recall_hybrid(
        &[fixture.entry_a.clone(), fixture.entry_b.clone()],
        &vectors,
        &RecallQuery::new("how do we evict stale copies after a release"),
        &embedder,
        MODEL,
    )
    .await
    .expect("hybrid recall");

    assert!(!response.degraded, "{:?}", response.degraded_reason);
    assert_eq!(response.algorithm, VECTOR_ALGORITHM);
    assert_eq!(response.model.model_id, MODEL);
    assert_eq!(response.model.dim, DIM);
    assert_eq!(response.vector_entries, 2);
    assert_eq!(response.hits.len(), 1, "{:?}", response.hits);
    let first = &response.hits[0];
    assert_eq!(first.entry_id, fixture.entry_a.id);
    assert_eq!(first.key, "deploy-cache");
    assert!(
        (first.score - 1.0).abs() < f32::EPSILON as f64 + 1e-6,
        "identical query vector must score 1.0, got {}",
        first.score
    );
    assert_eq!(
        first.provenance.verification,
        Verification::Unverified,
        "cosine must never upgrade trust"
    );
    assert!(!first.provenance.verified);
    assert_eq!(first.provenance.trust.as_deref(), Some("unverified"));
}

#[tokio::test]
async fn incomplete_vectors_degrade_to_the_existing_lexical_hits() {
    let fixture = fixture("hybrid degraded");
    let mut vectors = load_vectors(&fixture);
    vectors.remove(&fixture.entry_b.id);
    let mut query_vectors = HashMap::new();
    query_vectors.insert("cache deployment".to_owned(), vec![1.0, 0.0, 0.0, 0.0]);
    let embedder = TestEmbedder::new(query_vectors);

    let response = recall_hybrid(
        &[fixture.entry_a.clone(), fixture.entry_b.clone()],
        &vectors,
        &RecallQuery::new("cache deployment"),
        &embedder,
        MODEL,
    )
    .await
    .expect("degraded recall");

    assert!(response.degraded, "missing coverage must be explicit");
    let reason = response.degraded_reason.expect("reason");
    assert!(
        reason.contains("E_EMBEDDING_INCOMPLETE"),
        "reason must name incomplete vectors: {reason}"
    );
    assert_eq!(response.algorithm, semantic_recall::ALGORITHM);
    assert_eq!(response.vector_entries, 0);
    assert_eq!(response.hits.len(), 1, "{:?}", response.hits);
    assert_eq!(response.hits[0].entry_id, fixture.entry_a.id);
    assert_eq!(
        response.hits[0].provenance.verification,
        Verification::Unverified
    );
}

#[tokio::test]
async fn query_embedding_failure_is_an_explicit_lexical_fallback() {
    let fixture = fixture("hybrid query failure");
    let vectors = load_vectors(&fixture);
    let embedder = TestEmbedder::failing(QueryEmbedError::new(
        "E_EMBEDDING_TIMEOUT",
        "the loopback endpoint timed out",
    ));

    let response = recall_hybrid(
        &[fixture.entry_a.clone(), fixture.entry_b.clone()],
        &vectors,
        &RecallQuery::new("cache deployment"),
        &embedder,
        MODEL,
    )
    .await
    .expect("fallback recall");

    assert!(response.degraded);
    let reason = response.degraded_reason.expect("reason");
    assert!(reason.contains("E_EMBEDDING_QUERY"), "{reason}");
    assert!(reason.contains("E_EMBEDDING_TIMEOUT"), "{reason}");
    assert_eq!(response.algorithm, semantic_recall::ALGORITHM);
    assert_eq!(response.hits[0].entry_id, fixture.entry_a.id);
}

#[tokio::test]
async fn unknown_model_is_refused_not_degraded() {
    let fixture = fixture("hybrid unknown model");
    let vectors = load_vectors(&fixture);
    let mut query_vectors = HashMap::new();
    query_vectors.insert("cache deployment".to_owned(), vec![1.0, 0.0, 0.0, 0.0]);
    let embedder = TestEmbedder::new(query_vectors);

    let error = recall_hybrid(
        &[fixture.entry_a.clone(), fixture.entry_b.clone()],
        &vectors,
        &RecallQuery::new("cache deployment"),
        &embedder,
        "other-model",
    )
    .await
    .expect_err("unknown model must refuse");
    assert!(
        matches!(&error, RecallError::UnknownModel { requested, available }
            if requested == "other-model" && available == MODEL),
        "{error:?}"
    );
    assert_eq!(error.code(), "E_RECALL_UNKNOWN_MODEL");
}

#[tokio::test]
async fn blank_model_is_refused_by_name() {
    let fixture = fixture("hybrid blank model");
    let vectors = load_vectors(&fixture);
    let embedder = TestEmbedder::new(HashMap::new());
    let error = recall_hybrid(
        &[fixture.entry_a.clone()],
        &vectors,
        &RecallQuery::new("cache"),
        &embedder,
        "   ",
    )
    .await
    .expect_err("blank model must refuse");
    assert_eq!(error.code(), "E_RECALL_UNKNOWN_MODEL");
}

#[test]
fn stored_vectors_are_read_through_the_migration_helper() {
    let fixture = fixture("hybrid store helper");
    let vectors = load_vectors(&fixture);
    assert_eq!(vectors.len(), 2);
    assert_eq!(
        vectors.get(&fixture.entry_a.id),
        Some(&vec![1.0, 0.0, 0.0, 0.0])
    );
    // The store helper is bounded and rejects another model by name, proving
    // hybrid recall cannot silently mix model spaces.
    let ids = [fixture.entry_a.id.as_str()];
    let other = fixture
        .database
        .with_connection(|connection| {
            read_embeddings_for_entries(connection, "other-model", &ids)
                .map_err(|error| rusqlite::Error::InvalidParameterName(error.message()))
        })
        .expect("other model read");
    assert!(other.is_empty());
}
