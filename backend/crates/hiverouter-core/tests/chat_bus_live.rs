//! Live persistence tests for the per-tree chat bus (`src/chat_bus.rs`).
//!
//! These tests use the library module (`hiverouter_core::chat_bus`), not a
//! path-included copy, and every durable case goes through the real JSONL file
//! journal. The in-memory journal is used only where the property under test is
//! the persistence port's failure behaviour.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use hiverouter_contract::{Refusal, RunId, TaskId};
use hiverouter_core::chat_bus::{
    ChatBus, ChatJournal, ChatJournalRecord, ChatKind, FileJournal, MemoryJournal, PostOutcome,
    PostRequest, Visibility, JOURNAL_FILE_NAME, MAX_BODY_CHARS, MAX_IDEMPOTENCY_KEY_CHARS,
};
use tempfile::TempDir;

fn post(
    bus: &ChatBus,
    run: RunId,
    tree: TaskId,
    from: TaskId,
    to: TaskId,
    kind: ChatKind,
    body: &str,
    key: &str,
) -> Result<PostOutcome, Refusal> {
    bus.post(PostRequest {
        run_id: run,
        tree_id: tree,
        from_task_id: from,
        to_task_id: to,
        kind,
        body,
        idempotency_key: key,
        visibility: Visibility::ParentChain,
    })
}

fn post_with_visibility(
    bus: &ChatBus,
    run: RunId,
    tree: TaskId,
    from: TaskId,
    to: TaskId,
    kind: ChatKind,
    body: &str,
    key: &str,
    visibility: Visibility,
) -> Result<PostOutcome, Refusal> {
    bus.post(PostRequest {
        run_id: run,
        tree_id: tree,
        from_task_id: from,
        to_task_id: to,
        kind,
        body,
        idempotency_key: key,
        visibility,
    })
}

fn journal_path(data_dir: &Path) -> PathBuf {
    data_dir.join(JOURNAL_FILE_NAME)
}

fn assert_send_sync<T: Send + Sync>() {}

#[test]
fn chat_bus_is_safe_to_share_from_a_run_engine() {
    assert_send_sync::<ChatBus>();
    assert_send_sync::<Arc<ChatBus>>();
}

fn line_count(path: &Path) -> usize {
    std::fs::read_to_string(path)
        .expect("journal file")
        .lines()
        .count()
}

/// root -> child -> grandchild, plus root -> sibling.
struct Tree {
    run: RunId,
    root: TaskId,
    child: TaskId,
    grandchild: TaskId,
    sibling: TaskId,
}

fn seed_tree(bus: &ChatBus) -> Tree {
    let run = RunId::new();
    let root = TaskId::new();
    let child = TaskId::new();
    let grandchild = TaskId::new();
    let sibling = TaskId::new();
    bus.open_tree(run, root).expect("open root tree");
    bus.join_tree(run, root, child, root).expect("join child");
    bus.join_tree(run, root, grandchild, child)
        .expect("join grandchild");
    bus.join_tree(run, root, sibling, root)
        .expect("join sibling");
    Tree {
        run,
        root,
        child,
        grandchild,
        sibling,
    }
}

#[test]
fn data_dir_constructor_writes_jsonl_and_reopen_continues_the_sequence() {
    let dir = TempDir::new().expect("tempdir");
    let path = journal_path(dir.path());
    let run = RunId::new();
    let root = TaskId::new();
    let child = TaskId::new();
    let first_message_id;

    {
        let bus = ChatBus::open_file(dir.path()).expect("open data dir");
        assert!(
            path.is_file(),
            "open_file(data_dir) must create the journal"
        );
        bus.open_tree(run, root).expect("tree");
        bus.join_tree(run, root, child, root).expect("join");

        let first = post(
            &bus,
            run,
            root,
            child,
            root,
            ChatKind::Question,
            "what is the release scope?",
            "q-1",
        )
        .expect("first post");
        assert!(first.appended());
        assert_eq!(first.message().seq, 1);
        first_message_id = first.message().message_id;

        // The same idempotency key is acknowledged once, not appended twice.
        let duplicate = post(
            &bus,
            run,
            root,
            child,
            root,
            ChatKind::Question,
            "what is the release scope?",
            "q-1",
        )
        .expect("duplicate acknowledged");
        assert!(!duplicate.appended());
        assert_eq!(duplicate.message().message_id, first.message().message_id);

        let second = post(
            &bus,
            run,
            root,
            child,
            root,
            ChatKind::Result,
            "scope confirmed",
            "r-1",
        )
        .expect("second post");
        assert_eq!(second.message().seq, 2);
    }

    // Exactly open_tree + join_tree + two posts reached the file.
    assert_eq!(line_count(&path), 4);
    let reopened = ChatBus::open_file(dir.path()).expect("reopen data dir");
    let summary = reopened.tree(run, root).expect("summary after reopen");
    assert_eq!(summary.member_count, 2);
    assert_eq!(summary.message_count, 2);
    assert_eq!(summary.last_seq, 2);

    // Replayed messages keep their identity and order.
    let observed = reopened
        .observe_tree(run, root, root, 8)
        .expect("root observes history");
    assert_eq!(observed.len(), 2);
    assert_eq!(observed[0].seq, 1);
    assert_eq!(observed[0].message_id, first_message_id);
    assert_eq!(observed[1].idempotency_key, "r-1");

    // The idempotency index survives restart: the duplicate is still a no-op.
    let duplicate = post(
        &reopened,
        run,
        root,
        child,
        root,
        ChatKind::Question,
        "what is the release scope?",
        "q-1",
    )
    .expect("duplicate after reopen");
    assert!(!duplicate.appended());
    assert_eq!(line_count(&path), 4, "duplicate appended no journal line");

    let next = post(
        &reopened,
        run,
        root,
        child,
        root,
        ChatKind::Status,
        "continuing after restart",
        "s-1",
    )
    .expect("post after reopen");
    assert_eq!(next.message().seq, 3, "sequence continues after reopen");
    assert_eq!(line_count(&path), 5);
}

#[test]
fn parent_chain_reaches_ancestors_and_root_observer_is_the_full_audit_view() {
    let dir = TempDir::new().expect("tempdir");
    let bus = ChatBus::open_file(dir.path()).expect("open data dir");
    let tree = seed_tree(&bus);

    // Parent-chain escalation: visible to sender, recipient and ancestors, but
    // never to the sibling branch.
    post_with_visibility(
        &bus,
        tree.run,
        tree.root,
        tree.grandchild,
        tree.child,
        ChatKind::Escalation,
        "blocked on the schema decision",
        "esc-1",
        Visibility::ParentChain,
    )
    .expect("parent-chain escalation");
    // Task-private answer: only the two named tasks.
    post_with_visibility(
        &bus,
        tree.run,
        tree.root,
        tree.child,
        tree.grandchild,
        ChatKind::Answer,
        "use schema v2",
        "ans-1",
        Visibility::Task,
    )
    .expect("task-private answer");
    // Root-observer status.
    post_with_visibility(
        &bus,
        tree.run,
        tree.root,
        tree.child,
        tree.root,
        ChatKind::Status,
        "schema chosen",
        "status-1",
        Visibility::RootObserver,
    )
    .expect("root observer status");

    let root_inbox = bus
        .inbox(tree.run, tree.root, tree.root, 8)
        .expect("root inbox");
    assert_eq!(
        root_inbox.len(),
        2,
        "private answer is not in the ancestor inbox"
    );
    assert_eq!(root_inbox[0].kind, ChatKind::Escalation);
    assert_eq!(root_inbox[1].kind, ChatKind::Status);

    let child_inbox = bus
        .inbox(tree.run, tree.root, tree.child, 8)
        .expect("child inbox");
    assert_eq!(
        child_inbox.len(),
        3,
        "child is sender/recipient of every message"
    );

    let sibling_inbox = bus
        .inbox(tree.run, tree.root, tree.sibling, 8)
        .expect("sibling inbox");
    assert!(sibling_inbox.is_empty(), "sibling branch sees nothing");

    // The tree root sees every message, including Task-visibility traffic.
    let audit = bus
        .observe_tree(tree.run, tree.root, tree.root, 8)
        .expect("tree root audit");
    assert_eq!(audit.len(), 3);
    assert_eq!(audit[1].visibility, Visibility::Task);

    // A second tree in the same run is observable by the run root (the first
    // tree's root) but not writable by a member of the first tree.
    let other_tree = TaskId::new();
    bus.open_tree(tree.run, other_tree).expect("second tree");
    post_with_visibility(
        &bus,
        tree.run,
        other_tree,
        other_tree,
        other_tree,
        ChatKind::Brief,
        "independent branch",
        "brief-1",
        Visibility::RootObserver,
    )
    .expect("second tree post");
    assert_eq!(
        bus.observe_tree(tree.run, other_tree, tree.root, 8)
            .expect("run root observes every tree")
            .len(),
        1
    );
    assert_eq!(
        post(
            &bus,
            tree.run,
            other_tree,
            tree.child,
            other_tree,
            ChatKind::Brief,
            "cross-tree",
            "cross-1",
        ),
        Err(Refusal::TaskNotFound)
    );

    // Non-root members and non-members are refused the audit view by name.
    assert_eq!(
        bus.observe_tree(tree.run, tree.root, tree.child, 8),
        Err(Refusal::ToolNotAllowed)
    );
    assert_eq!(
        bus.observe_tree(tree.run, tree.root, TaskId::new(), 8),
        Err(Refusal::ToolNotAllowed)
    );
    assert_eq!(
        bus.observe_tree(tree.run, TaskId::new(), tree.root, 8),
        Err(Refusal::TaskNotFound)
    );
}

#[test]
fn corrupt_and_torn_journals_are_refused_by_name() {
    let dir = TempDir::new().expect("tempdir");
    let path = journal_path(dir.path());

    std::fs::write(&path, "not-json\n").expect("write corrupt");
    assert_eq!(
        ChatBus::open_file(dir.path()).err(),
        Some(Refusal::SnapshotCorrupt)
    );

    // A blank line is damage, not a record separator this writer emits.
    let open = ChatJournalRecord::OpenTree {
        run_id: RunId::new(),
        tree_id: TaskId::new(),
    };
    let valid = serde_json::to_string(&open).expect("serialize");
    std::fs::write(&path, format!("{valid}\n\n{valid}\n")).expect("blank line");
    assert_eq!(
        ChatBus::open_file(dir.path()).err(),
        Some(Refusal::SnapshotCorrupt)
    );

    // A torn final line (process died mid-append) is refused, not ignored.
    std::fs::write(
        &path,
        format!("{valid}\n{{\"record\":\"post\",\"message\":"),
    )
    .expect("torn");
    assert_eq!(
        ChatBus::open_file(dir.path()).err(),
        Some(Refusal::SnapshotCorrupt)
    );

    // A syntactically valid journal with a sequence gap is also corrupt.
    let run = RunId::new();
    let tree = TaskId::new();
    let open = ChatJournalRecord::OpenTree {
        run_id: run,
        tree_id: tree,
    };
    let message = hiverouter_core::chat_bus::ChatMessage {
        run_id: run,
        tree_id: tree,
        seq: 2,
        message_id: uuid::Uuid::now_v7(),
        from_task_id: tree,
        to_task_id: tree,
        kind: ChatKind::Brief,
        body: "out of order".to_owned(),
        idempotency_key: "gap-1".to_owned(),
        visibility: Visibility::Task,
        created_at: hiverouter_contract::Timestamp::now(),
    };
    let gap = ChatJournalRecord::Post { message };
    std::fs::write(
        &path,
        format!(
            "{}\n{}\n",
            serde_json::to_string(&open).expect("open"),
            serde_json::to_string(&gap).expect("gap")
        ),
    )
    .expect("sequence gap journal");
    assert_eq!(
        ChatBus::open_file(dir.path()).err(),
        Some(Refusal::SnapshotCorrupt)
    );
}

#[test]
fn bodies_keys_and_idempotency_are_bounded_before_the_journal() {
    let dir = TempDir::new().expect("tempdir");
    let path = journal_path(dir.path());
    let bus = ChatBus::open_file(dir.path()).expect("open data dir");
    let tree = seed_tree(&bus);
    let before = line_count(&path);

    let at_cap = "x".repeat(MAX_BODY_CHARS);
    let accepted = post(
        &bus,
        tree.run,
        tree.root,
        tree.child,
        tree.root,
        ChatKind::Brief,
        &at_cap,
        "cap-ok",
    )
    .expect("body at cap");
    assert_eq!(accepted.message().seq, 1);

    let over_cap = "x".repeat(MAX_BODY_CHARS + 1);
    assert_eq!(
        post(
            &bus,
            tree.run,
            tree.root,
            tree.child,
            tree.root,
            ChatKind::Result,
            &over_cap,
            "cap-too-big",
        ),
        Err(Refusal::SchemaInvalid)
    );
    assert_eq!(
        post(
            &bus,
            tree.run,
            tree.root,
            tree.child,
            tree.root,
            ChatKind::Status,
            "   ",
            "blank-body",
        ),
        Err(Refusal::SchemaInvalid)
    );
    assert_eq!(
        post(
            &bus,
            tree.run,
            tree.root,
            tree.child,
            tree.root,
            ChatKind::Status,
            "ok",
            "",
        ),
        Err(Refusal::SchemaInvalid)
    );
    let long_key = "k".repeat(MAX_IDEMPOTENCY_KEY_CHARS + 1);
    assert_eq!(
        post(
            &bus,
            tree.run,
            tree.root,
            tree.child,
            tree.root,
            ChatKind::Status,
            "ok",
            &long_key,
        ),
        Err(Refusal::SchemaInvalid)
    );
    assert_eq!(
        line_count(&path),
        before + 1,
        "refusals append nothing; only the at-cap post is committed"
    );

    // Key reuse with a different payload is a caller bug, refused.
    assert_eq!(
        post(
            &bus,
            tree.run,
            tree.root,
            tree.child,
            tree.root,
            ChatKind::Brief,
            "different body",
            "cap-ok",
        ),
        Err(Refusal::SchemaInvalid)
    );
    let next = post(
        &bus,
        tree.run,
        tree.root,
        tree.child,
        tree.root,
        ChatKind::Status,
        "next",
        "next-key",
    )
    .expect("valid post after refusals");
    assert_eq!(next.message().seq, 2, "refusals never consume a sequence");
}

#[derive(Debug, Default)]
struct FlakyJournal {
    fail: Mutex<bool>,
    records: Mutex<Vec<ChatJournalRecord>>,
}

impl ChatJournal for FlakyJournal {
    fn append(&self, record: &ChatJournalRecord) -> Result<(), Refusal> {
        if *self.fail.lock().map_err(|_| Refusal::InternalError)? {
            return Err(Refusal::InternalError);
        }
        self.records
            .lock()
            .map_err(|_| Refusal::InternalError)?
            .push(record.clone());
        Ok(())
    }

    fn replay(&self) -> Result<Vec<ChatJournalRecord>, Refusal> {
        Ok(self
            .records
            .lock()
            .map_err(|_| Refusal::InternalError)?
            .clone())
    }
}

#[test]
fn persistence_port_failure_is_never_acknowledged_as_memory_success() {
    let journal = Arc::new(FlakyJournal::default());
    let bus = ChatBus::open(journal.clone()).expect("open port");
    let run = RunId::new();
    let root = TaskId::new();
    let child = TaskId::new();
    bus.open_tree(run, root).expect("tree");
    bus.join_tree(run, root, child, root).expect("join");

    *journal.fail.lock().expect("lock") = true;
    assert_eq!(
        post(
            &bus,
            run,
            root,
            child,
            root,
            ChatKind::Status,
            "must not commit",
            "flaky-1",
        ),
        Err(Refusal::InternalError)
    );
    let summary = bus.tree(run, root).expect("summary");
    assert_eq!(summary.message_count, 0, "no memory-only success");
    assert_eq!(summary.last_seq, 0, "failed append consumed no sequence");
    assert_eq!(journal.records.lock().expect("records").len(), 2);

    *journal.fail.lock().expect("lock") = false;
    let committed = post(
        &bus,
        run,
        root,
        child,
        root,
        ChatKind::Status,
        "commits now",
        "flaky-2",
    )
    .expect("post after recovery");
    assert_eq!(committed.message().seq, 1);
}

#[test]
fn concurrent_posts_are_serialized_into_one_contiguous_durable_sequence() {
    let dir = TempDir::new().expect("tempdir");
    let path = journal_path(dir.path());
    let bus = Arc::new(ChatBus::open_file(dir.path()).expect("open data dir"));
    let tree = seed_tree(&bus);
    let run = tree.run;
    let tree_id = tree.root;
    let child = tree.child;
    let root = tree.root;

    let mut sequences: Vec<u64> = std::thread::scope(|scope| {
        let mut handles = Vec::new();
        for worker in 0..8_u64 {
            let bus = Arc::clone(&bus);
            handles.push(scope.spawn(move || {
                let mut local = Vec::new();
                for index in 0..4_u64 {
                    let key = format!("w{worker}-m{index}");
                    let outcome = post(
                        &bus,
                        run,
                        tree_id,
                        child,
                        root,
                        ChatKind::Status,
                        &format!("worker {worker} message {index}"),
                        &key,
                    )
                    .expect("concurrent post");
                    local.push(outcome.message().seq);
                }
                local
            }));
        }
        handles
            .into_iter()
            .flat_map(|handle| handle.join().expect("worker"))
            .collect()
    });
    sequences.sort_unstable();
    assert_eq!(sequences, (1..=32).collect::<Vec<u64>>());

    let observed = bus
        .observe_tree(run, tree_id, root, 64)
        .expect("root observes all");
    assert_eq!(observed.len(), 32);
    assert!(observed.windows(2).all(|pair| pair[0].seq < pair[1].seq));

    // Membership records: open + three joins. Then exactly 32 posts.
    assert_eq!(line_count(&path), 4 + 32);
    drop(bus);
    let reopened = ChatBus::open_journal_file(&path).expect("reopen exact file");
    let summary = reopened.tree(run, tree_id).expect("summary");
    assert_eq!(summary.message_count, 32);
    assert_eq!(summary.last_seq, 32);
}

#[test]
fn file_journal_path_is_exposed_and_memory_port_is_explicit() {
    let dir = TempDir::new().expect("tempdir");
    let path = journal_path(dir.path());
    let file = FileJournal::open(&path).expect("open file journal");
    assert_eq!(file.path(), path.as_path());

    // The memory journal is a genuine persistence-port implementation, but it
    // is not the production path and does not survive a new bus.
    let journal = Arc::new(MemoryJournal::new());
    let bus = ChatBus::open(journal.clone()).expect("memory port");
    let run = RunId::new();
    let root = TaskId::new();
    bus.open_tree(run, root).expect("tree");
    assert_eq!(journal.len().expect("records"), 1);
    drop(bus);
    let fresh = ChatBus::open(journal).expect("same memory port reopens");
    assert_eq!(fresh.tree(run, root).expect("summary").member_count, 1);
    assert_eq!(
        ChatBus::open_journal_file(dir.path().join("missing.jsonl"))
            .expect("missing journal is empty")
            .tree(run, root)
            .err(),
        Some(Refusal::TaskNotFound)
    );
}
