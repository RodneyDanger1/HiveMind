//! Acceptance tests for the bounded repository symbol search.
//!
//! A real temporary repository is indexed into an in-memory SQLite database;
//! the tests exercise name/kind queries, deterministic ordering, the hard
//! result cap, signature bounds, multi-language extraction, comment/string
//! false-positive resistance, and re-index idempotence.

use std::fs;
use std::path::Path;

use hiverouter_core::repo_index::{
    self, RepoIndexError, Workspace, MAX_SYMBOL_QUERY_BYTES, MAX_SYMBOL_RESULTS,
    MAX_SYMBOL_SIGNATURE_BYTES,
};
use rusqlite::Connection;
use tempfile::TempDir;

fn database() -> Connection {
    Connection::open_in_memory().expect("in-memory SQLite")
}

fn write_file(root: &Path, rel_path: &str, contents: &str) {
    let path = root.join(rel_path);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("parent directory");
    }
    fs::write(path, contents).expect("write file");
}

fn fixture() -> (TempDir, Connection, Workspace) {
    let directory = tempfile::tempdir().expect("temporary workspace");
    let root = directory.path();
    write_file(
        root,
        "src/lib.rs",
        "pub fn alpha_function(value: u32) -> u32 {\n    value + 1\n}\n\npub struct BetaStruct {\n    pub field: u8,\n}\n\npub enum GammaEnum {\n    First,\n    Second,\n}\n",
    );
    write_file(
        root,
        "web/app.ts",
        "export class DeltaClass {\n  run(): void {}\n}\n",
    );
    write_file(
        root,
        "scripts/tool.py",
        "def epsilon_func(x):\n    return x\n",
    );
    let connection = database();
    repo_index::index_workspace(&connection, root).expect("index workspace");
    let workspace = Workspace::open(root).expect("workspace");
    (directory, connection, workspace)
}

fn symbol_count(connection: &Connection) -> i64 {
    connection
        .query_row("SELECT COUNT(*) FROM repo_symbols", [], |row| row.get(0))
        .expect("count symbols")
}

#[test]
fn indexes_and_finds_symbols_by_name_with_path_line_kind_and_signature() {
    let (_temp, connection, workspace) = fixture();
    assert_eq!(
        symbol_count(&connection),
        5,
        "alpha_function, BetaStruct, GammaEnum, DeltaClass, epsilon_func"
    );

    let hits = repo_index::search_symbols(&connection, workspace.id(), "alpha", None, 10)
        .expect("search alpha");
    assert_eq!(hits.len(), 1);
    let hit = &hits[0];
    assert_eq!(hit.name, "alpha_function");
    assert_eq!(hit.kind, "function");
    assert_eq!(hit.rel_path, "src/lib.rs");
    assert_eq!(hit.line, 1);
    assert_eq!(hit.signature, "pub fn alpha_function(value: u32) -> u32");

    let struct_hits = repo_index::search_symbols(&connection, workspace.id(), "beta", None, 10)
        .expect("search beta");
    assert_eq!(struct_hits.len(), 1);
    assert_eq!(struct_hits[0].name, "BetaStruct");
    assert_eq!(struct_hits[0].kind, "struct");
    assert_eq!(struct_hits[0].line, 5);
    assert_eq!(struct_hits[0].signature, "pub struct BetaStruct");

    // Case-insensitive name match.
    let gamma = repo_index::search_symbols(&connection, workspace.id(), "GAMMA", None, 10)
        .expect("search GAMMA");
    assert_eq!(gamma.len(), 1);
    assert_eq!(gamma[0].kind, "enum");

    // Other supported languages are indexed too.
    let class = repo_index::search_symbols(&connection, workspace.id(), "Delta", None, 10)
        .expect("search Delta");
    assert_eq!(class.len(), 1);
    assert_eq!(class[0].kind, "class");
    assert_eq!(class[0].rel_path, "web/app.ts");

    let python = repo_index::search_symbols(&connection, workspace.id(), "epsilon", None, 10)
        .expect("search epsilon");
    assert_eq!(python.len(), 1);
    assert_eq!(python[0].kind, "function");
    assert_eq!(python[0].rel_path, "scripts/tool.py");
}

#[test]
fn filters_by_kind_and_orders_exact_matches_first() {
    let (_temp, connection, workspace) = fixture();

    let functions = repo_index::search_symbols(
        &connection,
        workspace.id(),
        "",
        Some("function"),
        MAX_SYMBOL_RESULTS,
    )
    .expect("all functions");
    assert_eq!(
        functions
            .iter()
            .map(|hit| hit.name.as_str())
            .collect::<Vec<_>>(),
        vec!["alpha_function", "epsilon_func"],
        "an empty query with a kind filter lists every symbol of that kind in name order"
    );

    let no_match = repo_index::search_symbols(
        &connection,
        workspace.id(),
        "beta",
        Some("function"),
        MAX_SYMBOL_RESULTS,
    )
    .expect("kind mismatch");
    assert!(no_match.is_empty(), "kind is an exact filter");

    // Exact name beats a longer name that merely contains the query.
    write_file(
        workspace.root(),
        "src/extra.rs",
        "pub fn alpha_function_extra() {}\n",
    );
    repo_index::index_workspace(&connection, workspace.root()).expect("re-index");
    let ordered =
        repo_index::search_symbols(&connection, workspace.id(), "alpha_function", None, 10)
            .expect("ordered search");
    assert_eq!(ordered[0].name, "alpha_function");
    assert_eq!(ordered[1].name, "alpha_function_extra");
}

#[test]
fn result_count_and_query_bounds_are_enforced() {
    let (_temp, connection, workspace) = fixture();

    let capped = repo_index::search_symbols(&connection, workspace.id(), "a", None, 2)
        .expect("capped search");
    assert_eq!(capped.len(), 2, "the caller limit caps the result set");

    let over = repo_index::search_symbols(
        &connection,
        workspace.id(),
        "a",
        None,
        MAX_SYMBOL_RESULTS + 1,
    )
    .expect_err("limit above the hard cap must be refused");
    assert!(
        matches!(over, RepoIndexError::InvalidLimit { .. }),
        "expected invalid limit, got {over:?}"
    );

    let long_query = "x".repeat(MAX_SYMBOL_QUERY_BYTES + 1);
    let over_query = repo_index::search_symbols(&connection, workspace.id(), &long_query, None, 1)
        .expect_err("oversized query must be refused");
    assert!(
        matches!(over_query, RepoIndexError::InvalidQuery(_)),
        "expected invalid query, got {over_query:?}"
    );

    let unknown_kind = repo_index::search_symbols(
        &connection,
        workspace.id(),
        "alpha",
        Some("metafunction"),
        1,
    )
    .expect_err("unknown kind must be refused, not silently empty");
    assert!(
        matches!(unknown_kind, RepoIndexError::InvalidQuery(_)),
        "expected invalid query, got {unknown_kind:?}"
    );

    let empty = repo_index::search_symbols(&connection, workspace.id(), "", None, 1)
        .expect_err("empty query without a kind must be refused");
    assert!(matches!(empty, RepoIndexError::InvalidQuery(_)));
}

#[test]
fn comments_and_strings_do_not_create_symbols_and_signatures_are_bounded() {
    let directory = tempfile::tempdir().expect("temporary workspace");
    let root = directory.path();
    let long_signature = format!(
        "pub fn long_signature({}) -> u32 {{ 0 }}\n",
        (0..40)
            .map(|index| format!("argument_{index}: u32"))
            .collect::<Vec<_>>()
            .join(", ")
    );
    write_file(
        root,
        "src/noise.rs",
        &format!(
            "// fn hidden_in_comment() {{}}\n/* fn hidden_in_block() {{}} */\nconst TEXT: &str = \"fn hidden_in_string()\";\n{long_signature}"
        ),
    );
    let connection = database();
    let workspace = Workspace::open(root).expect("workspace");
    repo_index::index_workspace(&connection, root).expect("index workspace");

    for hidden in [
        "hidden_in_comment",
        "hidden_in_block",
        "hidden_in_string",
        "hidden",
    ] {
        let hits = repo_index::search_symbols(&connection, workspace.id(), hidden, None, 10)
            .expect("search hidden");
        assert!(
            hits.is_empty(),
            "comment/string declaration {hidden} must not be indexed: {hits:?}"
        );
    }

    let hits = repo_index::search_symbols(&connection, workspace.id(), "long_signature", None, 10)
        .expect("search long signature");
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].line, 4);
    assert!(
        hits[0].signature.len() <= MAX_SYMBOL_SIGNATURE_BYTES,
        "signature is {} bytes, above the {MAX_SYMBOL_SIGNATURE_BYTES}-byte bound",
        hits[0].signature.len()
    );
    assert!(
        hits[0].signature.ends_with("..."),
        "a truncated signature must be visibly marked: {:?}",
        hits[0].signature
    );
}

#[test]
fn reindexing_replaces_symbols_instead_of_duplicating_them() {
    let (_temp, connection, workspace) = fixture();
    assert_eq!(symbol_count(&connection), 5);

    let report =
        repo_index::index_workspace(&connection, workspace.root()).expect("re-index workspace");
    assert_eq!(
        report.symbols_indexed, 5,
        "the report counts declarations written by this walk"
    );
    assert_eq!(
        symbol_count(&connection),
        5,
        "re-indexing must replace rows, not append duplicates"
    );

    // A deleted declaration disappears on re-index.
    fs::remove_file(workspace.root().join("scripts/tool.py")).expect("remove file");
    repo_index::index_workspace(&connection, workspace.root()).expect("re-index after delete");
    let epsilon = repo_index::search_symbols(&connection, workspace.id(), "epsilon", None, 10)
        .expect("search epsilon");
    assert!(
        epsilon.is_empty(),
        "stale symbols must not survive re-index"
    );
}
