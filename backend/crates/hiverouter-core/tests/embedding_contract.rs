//! Embedding contract tests: migration SQL, `memory_embeddings` helpers, exact
//! f32 cosine, and the loopback HTTP adapter.
//!
//! `src/embeddings.rs` is compiled here through `#[path]` because this work
//! package owns module files only and must not edit `lib.rs`. The shims expose
//! the `crate::db` / `crate::semantic_recall` paths the module uses once the
//! crate root declares it, so the file under test is byte-identical to the
//! production module.
//!
//! `TestEmbedder` is deliberately **not** used here: the adapter test points a
//! real `LoopbackHttpEmbedder` at a local `wiremock` server, so the production
//! request/response path is exercised. `wiremock` replaces the external
//! embedding service only (a permitted external-boundary double).

#![allow(dead_code)]

mod db {
    pub use hiverouter_core::db::*;
}

mod semantic_recall {
    pub use hiverouter_core::semantic_recall::*;
}

#[path = "../src/embeddings.rs"]
mod embeddings;

use std::sync::Arc;
use std::time::Duration;

use hiverouter_contract::{AgentState, RunId, TaskId};
use hiverouter_core::db::Database;
use hiverouter_core::entities::TaskRecord;
use hiverouter_core::store::RunStore;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use embeddings::{
    cosine_score_f32, cosine_similarity_f32, delete_embedding, embeddings_migration,
    read_embedding, read_embeddings_for_entries, write_embedding, Embedder,
    EmbeddingEndpointConfig, EmbeddingError, LoopbackHttpEmbedder, DEFAULT_TIMEOUT,
};

const MODEL: &str = "test-embed";
const DIM: usize = 4;

fn store_with_entry(label: &str) -> (RunStore, Arc<Database>, RunId, TaskId, String) {
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
    let entry_id = store
        .record_memory_entry(
            run.id,
            task.id,
            "worker",
            "deploy-cache",
            "Cache entries must be invalidated whenever a deployment ships",
            "fact",
            "unverified",
        )
        .expect("entry");
    (store, database, run.id, task.id, entry_id)
}

fn apply_migration(database: &Database) {
    database
        .with_connection(|connection| {
            connection.execute_batch(embeddings_migration(33).up)?;
            Ok(())
        })
        .expect("embedding migration");
}

#[test]
fn migration_sql_helpers_and_vector_roundtrip_are_accepted() {
    let (_store, database, _run_id, _task_id, entry_id) = store_with_entry("embedding store");
    apply_migration(&database);

    database
        .with_connection(|connection| {
            write_embedding(connection, &entry_id, MODEL, &[1.0, 0.0, 0.0, 0.0])
                .map_err(|error| rusqlite::Error::InvalidParameterName(error.message()))?;
            let stored = read_embedding(connection, &entry_id, MODEL)
                .map_err(|error| rusqlite::Error::InvalidParameterName(error.message()))?
                .expect("stored vector");
            assert_eq!(stored.entry_id, entry_id);
            assert_eq!(stored.model_id, MODEL);
            assert_eq!(stored.dim, DIM);
            assert_eq!(stored.vector, vec![1.0, 0.0, 0.0, 0.0]);

            let ids = [entry_id.as_str()];
            let batch = read_embeddings_for_entries(connection, MODEL, &ids)
                .map_err(|error| rusqlite::Error::InvalidParameterName(error.message()))?;
            assert_eq!(batch.len(), 1);
            assert_eq!(batch.get(&entry_id), Some(&vec![1.0, 0.0, 0.0, 0.0]));

            let other_model = read_embeddings_for_entries(connection, "other-model", &ids)
                .map_err(|error| rusqlite::Error::InvalidParameterName(error.message()))?;
            assert!(
                other_model.is_empty(),
                "a model must not read another model's vectors"
            );

            assert!(delete_embedding(connection, &entry_id, MODEL)
                .map_err(|error| rusqlite::Error::InvalidParameterName(error.message()))?);
            assert!(read_embedding(connection, &entry_id, MODEL)
                .map_err(|error| rusqlite::Error::InvalidParameterName(error.message()))?
                .is_none());
            Ok(())
        })
        .expect("embedding helpers");

    // The inverse is exercised too: dropping and re-applying proves the helper
    // SQL is accepted by this SQLite build in both directions.
    database
        .with_connection(|connection| {
            connection.execute_batch(embeddings_migration(33).down)?;
            connection.execute_batch(embeddings_migration(33).up)?;
            Ok(())
        })
        .expect("migration inverse");
    let vectors: i64 = database
        .with_connection(|connection| {
            connection.query_row("SELECT COUNT(*) FROM memory_embeddings", [], |row| {
                row.get(0)
            })
        })
        .expect("count");
    assert_eq!(vectors, 0);
}

#[test]
fn cosine_is_exact_f32_and_projects_negative_scores_to_zero() {
    let left = [1.0_f32, 0.0, 0.0];
    assert_eq!(cosine_similarity_f32(&left, &left).expect("same"), 1.0);
    assert_eq!(
        cosine_similarity_f32(&left, &[-1.0_f32, 0.0, 0.0]).expect("opposite"),
        -1.0
    );
    assert_eq!(
        cosine_score_f32(&left, &[-1.0_f32, 0.0, 0.0]).expect("projected"),
        0.0
    );
    assert_eq!(
        cosine_similarity_f32(&[0.0_f32, 1.0], &[1.0_f32, 0.0]).expect("orthogonal"),
        0.0
    );
    assert_eq!(
        cosine_similarity_f32(&[0.0_f32, 0.0], &[1.0_f32, 0.0]).expect_err("zero norm"),
        EmbeddingError::ZeroVector { index: 0 }
    );
}

#[tokio::test]
async fn loopback_http_adapter_posts_model_and_input_and_restores_index_order() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/embeddings"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "data": [
                {"embedding": [0.0, 1.0, 0.0, 0.0], "index": 1},
                {"embedding": [1.0, 0.0, 0.0, 0.0], "index": 0}
            ]
        })))
        .mount(&server)
        .await;

    let config = EmbeddingEndpointConfig::new(
        &format!("{}/v1/embeddings", server.uri()),
        MODEL,
        DIM,
        DEFAULT_TIMEOUT,
        true,
    )
    .expect("loopback config");
    let embedder = LoopbackHttpEmbedder::new(config).expect("opted-in adapter");
    assert_eq!(embedder.model_id(), MODEL);
    assert_eq!(embedder.dim(), DIM);
    let vectors = embedder
        .embed_texts(&["cache invalidation", "token budget"])
        .await
        .expect("embedding response");
    assert_eq!(
        vectors,
        vec![vec![1.0, 0.0, 0.0, 0.0], vec![0.0, 1.0, 0.0, 0.0],],
        "index must restore input order even when the server returns it reversed"
    );

    let requests = server.received_requests().await.expect("recorded requests");
    assert_eq!(requests.len(), 1);
    let body: serde_json::Value = serde_json::from_slice(&requests[0].body).expect("request JSON");
    assert_eq!(body["model"], MODEL);
    assert_eq!(body["input"][0], "cache invalidation");
    assert_eq!(body["input"][1], "token budget");
}

#[tokio::test]
async fn adapter_refuses_without_opt_in_and_rejects_non_loopback_endpoints() {
    let disabled = EmbeddingEndpointConfig::new(
        "http://127.0.0.1:9/v1/embeddings",
        MODEL,
        DIM,
        DEFAULT_TIMEOUT,
        false,
    )
    .expect("valid config");
    assert_eq!(
        LoopbackHttpEmbedder::new(disabled).expect_err("disabled must refuse"),
        EmbeddingError::Disabled
    );
    assert_eq!(EmbeddingError::Disabled.code(), "E_EMBEDDING_DISABLED");

    assert_eq!(
        EmbeddingEndpointConfig::new(
            "http://10.0.0.5:8080/v1/embeddings",
            MODEL,
            DIM,
            DEFAULT_TIMEOUT,
            true,
        )
        .expect_err("public host must refuse"),
        EmbeddingError::NotLoopback
    );
    assert_eq!(
        EmbeddingEndpointConfig::new(
            "https://example.com/v1/embeddings",
            MODEL,
            DIM,
            DEFAULT_TIMEOUT,
            true,
        )
        .expect_err("external host must refuse"),
        EmbeddingError::NotLoopback
    );
    assert!(
        EmbeddingEndpointConfig::new(
            "http://user:secret@127.0.0.1:9/v1/embeddings",
            MODEL,
            DIM,
            DEFAULT_TIMEOUT,
            true,
        )
        .is_err(),
        "credentials in the endpoint must refuse"
    );
}

#[tokio::test]
async fn adapter_refuses_wrong_vector_count_and_http_errors() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/embeddings"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "data": [{"embedding": [1.0, 0.0, 0.0, 0.0]}]
        })))
        .mount(&server)
        .await;
    let config = EmbeddingEndpointConfig::new(
        &format!("{}/v1/embeddings", server.uri()),
        MODEL,
        DIM,
        DEFAULT_TIMEOUT,
        true,
    )
    .expect("config");
    let embedder = LoopbackHttpEmbedder::new(config).expect("adapter");
    let error = embedder
        .embed_texts(&["one", "two"])
        .await
        .expect_err("count mismatch");
    assert_eq!(
        error,
        EmbeddingError::VectorCount {
            expected: 2,
            actual: 1
        }
    );

    let failing = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/embeddings"))
        .respond_with(ResponseTemplate::new(503))
        .mount(&failing)
        .await;
    let config = EmbeddingEndpointConfig::new(
        &format!("{}/v1/embeddings", failing.uri()),
        MODEL,
        DIM,
        DEFAULT_TIMEOUT,
        true,
    )
    .expect("config");
    let embedder = LoopbackHttpEmbedder::new(config).expect("adapter");
    assert_eq!(
        embedder.embed_texts(&["one"]).await.expect_err("status"),
        EmbeddingError::HttpStatus { status: 503 }
    );
}

#[tokio::test]
async fn adapter_rejects_dimension_mismatch_and_non_finite_components() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/embeddings"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "data": [{"embedding": [1.0, 0.0, 0.0]}]
        })))
        .mount(&server)
        .await;
    let config = EmbeddingEndpointConfig::new(
        &format!("{}/v1/embeddings", server.uri()),
        MODEL,
        DIM,
        DEFAULT_TIMEOUT,
        true,
    )
    .expect("config");
    let embedder = LoopbackHttpEmbedder::new(config).expect("adapter");
    assert_eq!(
        embedder.embed_texts(&["one"]).await.expect_err("dim"),
        EmbeddingError::DimensionMismatch {
            expected: DIM,
            actual: 3
        }
    );
}

#[test]
fn vector_width_is_bounded() {
    let config = EmbeddingEndpointConfig::new(
        "http://127.0.0.1:9/v1/embeddings",
        MODEL,
        0,
        DEFAULT_TIMEOUT,
        true,
    );
    assert!(matches!(config, Err(EmbeddingError::InvalidConfig { .. })));
    let config = EmbeddingEndpointConfig::new(
        "http://127.0.0.1:9/v1/embeddings",
        MODEL,
        DIM,
        Duration::from_millis(1),
        true,
    );
    assert!(matches!(config, Err(EmbeddingError::InvalidConfig { .. })));
}
