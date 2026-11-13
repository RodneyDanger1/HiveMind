//! Contract tests for the per-tree chat bus (`src/chat_bus.rs`).
//!
//! The module is included by path because `lib.rs` is outside this change's
//! allowed write set; registering `pub mod chat_bus;` there makes these same
//! assertions run against the library module unchanged.

#[path = "../src/chat_bus.rs"]
mod chat_bus;

use std::sync::{Arc, Mutex};

use chat_bus::{
    ChatBus, ChatJournal, ChatJournalRecord, ChatKind, MemoryJournal, PostOutcome, PostRequest,
    Visibility, MAX_BODY_CHARS, MAX_IDEMPOTENCY_KEY_CHARS,
};
use hiverouter_contract::{Refusal, RunId, TaskId};

struct Fixture {
    bus: ChatBus,
    journal: Arc<MemoryJournal>,
}

fn fixture() -> Fixture {
    let journal = Arc::new(MemoryJournal::new());
    let bus = ChatBus::open(journal.clone()).expect("empty journal opens");
    Fixture { bus, journal }
}

/// root -> child -> grandchild, plus a sibling branch root -> sibling.
struct Tree {
    run: RunId,
    root: TaskId,
    child: TaskId,
    grandchild: TaskId,
    sibling: TaskId,
    tree: TaskId,
}

fn tree(fixture: &Fixture) -> Tree {
    let run = RunId::new();
    let root = TaskId::new();
    let child = TaskId::new();
    let grandchild = TaskId::new();
    let sibling = TaskId::new();
    fixture.bus.open_tree(run, root).expect("tree opens");
    fixture
        .bus
        .join_tree(run, root, child, root)
        .expect("child joins");
    fixture
        .bus
        .join_tree(run, root, grandchild, child)
        .expect("grandchild joins");
    fixture
        .bus
        .join_tree(run, root, sibling, root)
        .expect("sibling joins");
    Tree {
        run,
        root,
        child,
        grandchild,
        sibling,
        tree: root,
    }
}

fn post(
    fixture: &Fixture,
    run: RunId,
    tree: TaskId,
    from: TaskId,
    to: TaskId,
    kind: ChatKind,
    body: &str,
    key: &str,
) -> Result<PostOutcome, Refusal> {
    fixture.bus.post(PostRequest {
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

#[test]
fn trees_are_isolated_by_run_and_by_tree() {
    let fixture = fixture();
    let first = tree(&fixture);
    let second = tree(&fixture);
    assert_ne!(first.run, second.run);

    let first_message = post(
        &fixture,
        first.run,
        first.tree,
        first.grandchild,
        first.child,
        ChatKind::Question,
        "what next?",
        "q1",
    )
    .expect("posts inside its own tree");
    assert_eq!(first_message.message().seq, 1);
    assert!(first_message.appended());

    // A task of the second run cannot read the first run's tree.
    assert_eq!(
        fixture
            .bus
            .inbox(second.run, second.tree, second.root, 8)
            .expect("own inbox"),
        Vec::new()
    );
    // Cross-run reads are refused by name, not answered with an empty list.
    assert_eq!(
        fixture.bus.inbox(first.run, first.tree, second.root, 8),
        Err(Refusal::TaskNotFound)
    );
    // Sequence numbers are per tree: the second tree starts at 1 too.
    let second_message = post(
        &fixture,
        second.run,
        second.tree,
        second.child,
        second.child,
        ChatKind::Status,
        "independent sequence",
        "s1",
    )
    .expect("posts in the second tree");
    assert_eq!(second_message.message().seq, 1);

    let first_summary = fixture.bus.tree(first.run, first.tree).expect("summary");
    assert_eq!(first_summary.message_count, 1);
    assert_eq!(first_summary.last_seq, 1);
    assert_eq!(first_summary.member_count, 4);

    // Two trees in the same run are isolated as well, with the run root able to
    // observe both.
    let root_tree = Tree {
        run: first.run,
        root: first.root,
        child: first.child,
        grandchild: first.grandchild,
        sibling: first.sibling,
        tree: first.tree,
    };
    let other_tree_root = TaskId::new();
    fixture
        .bus
        .open_tree(first.run, other_tree_root)
        .expect("second tree opens in the same run");
    post(
        &fixture,
        first.run,
        other_tree_root,
        other_tree_root,
        other_tree_root,
        ChatKind::Brief,
        "separate tree",
        "t2",
    )
    .expect("root may post to itself");
    let other_summary = fixture
        .bus
        .tree(first.run, other_tree_root)
        .expect("second summary");
    assert_eq!(other_summary.member_count, 1);
    assert_eq!(other_summary.message_count, 1);
    assert_eq!(other_summary.run_root, root_tree.root);
    assert_eq!(
        fixture
            .bus
            .observe_tree(first.run, first.tree, root_tree.root, 8)
            .expect("run root observes the first tree")
            .len(),
        1
    );
    assert_eq!(
        fixture
            .bus
            .observe_tree(first.run, other_tree_root, root_tree.root, 8)
            .expect("run root observes every tree")
            .len(),
        1
    );
}

#[test]
fn parent_chain_visibility_reaches_ancestors_but_not_siblings() {
    let fixture = fixture();
    let tree = tree(&fixture);

    // Grandchild escalates to its parent with ParentChain visibility.
    post(
        &fixture,
        tree.run,
        tree.tree,
        tree.grandchild,
        tree.child,
        ChatKind::Escalation,
        "blocked on the schema decision",
        "esc-1",
    )
    .expect("grandchild posts to parent");

    let child_inbox = fixture
        .bus
        .inbox(tree.run, tree.tree, tree.child, 8)
        .expect("child inbox");
    assert_eq!(child_inbox.len(), 1);
    assert_eq!(child_inbox[0].from_task_id, tree.grandchild);

    // The root is an ancestor of the sender, so it reads the escalation.
    let root_inbox = fixture
        .bus
        .inbox(tree.run, tree.tree, tree.root, 8)
        .expect("root inbox");
    assert_eq!(root_inbox.len(), 1);
    assert_eq!(root_inbox[0].seq, 1);

    // The sibling branch is neither sender, recipient nor ancestor.
    assert_eq!(
        fixture
            .bus
            .inbox(tree.run, tree.tree, tree.sibling, 8)
            .expect("sibling inbox"),
        Vec::new()
    );

    // A Task-visibility message between the child and grandchild is invisible to
    // the grandchild's sibling branch and to the root's inbox, but the root's
    // audit view still sees it.
    fixture
        .bus
        .post(PostRequest {
            run_id: tree.run,
            tree_id: tree.tree,
            from_task_id: tree.child,
            to_task_id: tree.grandchild,
            kind: ChatKind::Answer,
            body: "use the v2 schema",
            idempotency_key: "private-1",
            visibility: Visibility::Task,
        })
        .expect("task-private answer");
    assert_eq!(
        fixture
            .bus
            .inbox(tree.run, tree.tree, tree.root, 8)
            .expect("root inbox")
            .len(),
        1,
        "task-private traffic is not in an ancestor inbox"
    );
    assert_eq!(
        fixture
            .bus
            .inbox(tree.run, tree.tree, tree.sibling, 8)
            .expect("sibling inbox")
            .len(),
        0
    );
    assert_eq!(
        fixture
            .bus
            .observe_tree(tree.run, tree.tree, tree.root, 8)
            .expect("root audit view")
            .len(),
        2
    );
}

#[test]
fn root_observes_every_message_and_tree_but_others_are_refused() {
    let fixture = fixture();
    let tree = tree(&fixture);
    post(
        &fixture,
        tree.run,
        tree.tree,
        tree.child,
        tree.grandchild,
        ChatKind::Brief,
        "starting the parser",
        "b-1",
    )
    .expect("brief");

    let observed = fixture
        .bus
        .observe_tree(tree.run, tree.tree, tree.root, 8)
        .expect("tree root observes");
    assert_eq!(observed.len(), 1);
    assert_eq!(observed[0].kind, ChatKind::Brief);

    // A non-root member is refused the audit view by name.
    assert_eq!(
        fixture.bus.observe_tree(tree.run, tree.tree, tree.child, 8),
        Err(Refusal::ToolNotAllowed)
    );
    // A task that is not even a member is refused too.
    assert_eq!(
        fixture
            .bus
            .observe_tree(tree.run, tree.tree, TaskId::new(), 8),
        Err(Refusal::ToolNotAllowed)
    );
    // Unknown tree is a named not-found, never an empty success.
    assert_eq!(
        fixture
            .bus
            .observe_tree(tree.run, TaskId::new(), tree.root, 8),
        Err(Refusal::TaskNotFound)
    );
}

#[test]
fn idempotency_key_appends_once_and_payload_change_is_refused() {
    let fixture = fixture();
    let tree = tree(&fixture);

    let first = post(
        &fixture,
        tree.run,
        tree.tree,
        tree.child,
        tree.root,
        ChatKind::Status,
        "halfway",
        "status-1",
    )
    .expect("first post");
    assert!(first.appended());

    let replay = post(
        &fixture,
        tree.run,
        tree.tree,
        tree.child,
        tree.root,
        ChatKind::Status,
        "halfway",
        "status-1",
    )
    .expect("replay is acknowledged, not failed");
    assert!(!replay.appended());
    assert_eq!(replay.message().message_id, first.message().message_id);
    assert_eq!(replay.message().seq, first.message().seq);

    assert_eq!(fixture.journal.len().expect("journal len"), 5);
    // open tree + 3 joins + 1 message = 5 records; the replay appended nothing.
    assert_eq!(
        fixture
            .bus
            .inbox(tree.run, tree.tree, tree.root, 8)
            .expect("inbox")
            .len(),
        1
    );

    // Reusing the key with a different payload is a caller bug, refused.
    assert_eq!(
        post(
            &fixture,
            tree.run,
            tree.tree,
            tree.child,
            tree.root,
            ChatKind::Status,
            "different body",
            "status-1",
        ),
        Err(Refusal::SchemaInvalid)
    );
    assert_eq!(
        fixture
            .bus
            .tree(tree.run, tree.tree)
            .expect("summary")
            .last_seq,
        1
    );
}

#[test]
fn body_cap_is_enforced_without_consuming_a_sequence() {
    let fixture = fixture();
    let tree = tree(&fixture);

    let at_cap = "x".repeat(MAX_BODY_CHARS);
    let accepted = post(
        &fixture,
        tree.run,
        tree.tree,
        tree.child,
        tree.root,
        ChatKind::Brief,
        &at_cap,
        "cap-ok",
    )
    .expect("body at the cap is accepted");
    assert_eq!(accepted.message().seq, 1);
    assert_eq!(accepted.message().body.chars().count(), MAX_BODY_CHARS);

    let over_cap = "x".repeat(MAX_BODY_CHARS + 1);
    let before = fixture.journal.len().expect("journal len");
    assert_eq!(
        post(
            &fixture,
            tree.run,
            tree.tree,
            tree.child,
            tree.root,
            ChatKind::Result,
            &over_cap,
            "cap-too-big",
        ),
        Err(Refusal::SchemaInvalid)
    );
    assert_eq!(fixture.journal.len().expect("journal len"), before);
    assert_eq!(
        fixture
            .bus
            .tree(tree.run, tree.tree)
            .expect("summary")
            .last_seq,
        1,
        "a refused body never consumes a sequence"
    );

    assert_eq!(
        post(
            &fixture,
            tree.run,
            tree.tree,
            tree.child,
            tree.root,
            ChatKind::Status,
            "   ",
            "blank",
        ),
        Err(Refusal::SchemaInvalid)
    );
    assert_eq!(
        post(
            &fixture,
            tree.run,
            tree.tree,
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
            &fixture,
            tree.run,
            tree.tree,
            tree.child,
            tree.root,
            ChatKind::Status,
            "ok",
            &long_key,
        ),
        Err(Refusal::SchemaInvalid)
    );

    let next = post(
        &fixture,
        tree.run,
        tree.tree,
        tree.child,
        tree.root,
        ChatKind::Status,
        "next",
        "cap-next",
    )
    .expect("a valid post follows the refusals");
    assert_eq!(next.message().seq, 2);
}

#[test]
fn illegal_cross_tree_post_is_refused_and_appends_nothing() {
    let fixture = fixture();
    let first = tree(&fixture);
    let second = tree(&fixture);
    let before = fixture.journal.len().expect("journal len");

    // Sender belongs to the other tree.
    assert_eq!(
        post(
            &fixture,
            second.run,
            second.tree,
            first.child,
            second.root,
            ChatKind::Question,
            "wrong tree",
            "cross-1",
        ),
        Err(Refusal::TaskNotFound)
    );
    // Recipient belongs to the other tree.
    assert_eq!(
        post(
            &fixture,
            second.run,
            second.tree,
            second.root,
            first.child,
            ChatKind::Question,
            "wrong recipient",
            "cross-2",
        ),
        Err(Refusal::TaskNotFound)
    );
    // Same run, different tree: still refused.
    let other_tree_root = TaskId::new();
    fixture
        .bus
        .open_tree(first.run, other_tree_root)
        .expect("sibling tree opens");
    let before_cross = fixture.journal.len().expect("journal len");
    assert_eq!(
        post(
            &fixture,
            first.run,
            other_tree_root,
            first.child,
            other_tree_root,
            ChatKind::Brief,
            "same run, other tree",
            "cross-3",
        ),
        Err(Refusal::TaskNotFound)
    );
    // Unknown tree.
    assert_eq!(
        post(
            &fixture,
            first.run,
            TaskId::new(),
            first.child,
            first.root,
            ChatKind::Brief,
            "no tree",
            "cross-4",
        ),
        Err(Refusal::TaskNotFound)
    );
    assert_eq!(
        fixture.journal.len().expect("journal len"),
        before_cross,
        "refused cross-tree posts never reach the journal"
    );
    assert!(fixture.journal.len().expect("journal len") > before);

    // A task cannot be joined into two trees of one run.
    assert_eq!(
        fixture
            .bus
            .join_tree(first.run, other_tree_root, first.child, other_tree_root),
        Err(Refusal::SchemaInvalid)
    );
    // Nor under a parent that is not a member.
    assert_eq!(
        fixture
            .bus
            .join_tree(first.run, first.tree, TaskId::new(), TaskId::new()),
        Err(Refusal::TaskNotFound)
    );
    // Rejoining with the same parent is idempotent.
    fixture
        .bus
        .join_tree(first.run, first.tree, first.child, first.root)
        .expect("idempotent rejoin");
}

#[test]
fn file_journal_reopens_with_history_and_continues_the_sequence() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("bus").join("chat_bus.jsonl");
    let run = RunId::new();
    let root = TaskId::new();
    let child = TaskId::new();

    {
        let bus = ChatBus::open_file(&path).expect("opens a fresh journal");
        bus.open_tree(run, root).expect("tree");
        bus.join_tree(run, root, child, root).expect("join");
        for (index, key) in ["p1", "p2"].iter().enumerate() {
            let outcome = bus
                .post(PostRequest {
                    run_id: run,
                    tree_id: root,
                    from_task_id: child,
                    to_task_id: root,
                    kind: ChatKind::Status,
                    body: "persisted",
                    idempotency_key: key,
                    visibility: Visibility::ParentChain,
                })
                .expect("post");
            assert_eq!(outcome.message().seq, index as u64 + 1);
        }
        // A duplicate does not append a second line.
        let duplicate = bus
            .post(PostRequest {
                run_id: run,
                tree_id: root,
                from_task_id: child,
                to_task_id: root,
                kind: ChatKind::Status,
                body: "persisted",
                idempotency_key: "p1",
                visibility: Visibility::ParentChain,
            })
            .expect("duplicate");
        assert!(!duplicate.appended());
    }

    let reopened = ChatBus::open_file(&path).expect("reopens the journal");
    let summary = reopened.tree(run, root).expect("summary after reopen");
    assert_eq!(summary.member_count, 2);
    assert_eq!(summary.message_count, 2);
    assert_eq!(summary.last_seq, 2);
    let observed = reopened
        .observe_tree(run, root, root, 8)
        .expect("history after reopen");
    assert_eq!(observed.len(), 2);
    assert_eq!(observed[0].idempotency_key, "p1");
    assert_eq!(observed[1].seq, 2);

    // The sequence continues after the restored high-water mark.
    let next = reopened
        .post(PostRequest {
            run_id: run,
            tree_id: root,
            from_task_id: child,
            to_task_id: root,
            kind: ChatKind::Result,
            body: "after restart",
            idempotency_key: "p3",
            visibility: Visibility::RootObserver,
        })
        .expect("post after reopen");
    assert_eq!(next.message().seq, 3);
    let lines = std::fs::read_to_string(&path).expect("journal file");
    assert_eq!(
        lines.lines().count(),
        5,
        "open + join + 3 posts; dup appended none"
    );
}

#[test]
fn corrupt_journal_is_refused_by_name() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("chat_bus.jsonl");
    std::fs::write(&path, "not-json\n").expect("write corrupt journal");
    assert_eq!(
        ChatBus::open_file(&path).err(),
        Some(Refusal::SnapshotCorrupt)
    );
}

#[test]
fn membership_helpers_agree_with_the_bus_state() {
    let bus = ChatBus::in_memory();
    let run = RunId::new();
    let root = TaskId::new();
    let child = TaskId::new();

    assert_eq!(bus.tree_key_for(run), Err(Refusal::TaskNotFound));
    bus.open_tree(run, root).expect("tree");
    assert_eq!(
        bus.tree_key_for(run).expect("main tree").tree_id,
        root,
        "the first tree opened for a run is its main tree"
    );
    bus.join_tree(run, root, child, root).expect("join");
    assert!(bus.is_member(run, root, child).expect("member read"));
    assert!(!bus
        .is_member(run, root, TaskId::new())
        .expect("member read"));
    assert!(!bus
        .is_member(run, TaskId::new(), root)
        .expect("member read"));

    let journal = MemoryJournal::new();
    assert!(journal.is_empty().expect("journal empty"));
    journal
        .append(&ChatJournalRecord::OpenTree {
            run_id: run,
            tree_id: root,
        })
        .expect("append");
    assert!(!journal.is_empty().expect("journal not empty"));
    assert_eq!(journal.records().expect("records").len(), 1);
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
fn journal_failure_leaves_no_message_and_no_consumed_sequence() {
    let journal = Arc::new(FlakyJournal::default());
    let bus = ChatBus::open(journal.clone()).expect("opens");
    let run = RunId::new();
    let root = TaskId::new();
    let child = TaskId::new();
    bus.open_tree(run, root).expect("tree");
    bus.join_tree(run, root, child, root).expect("join");

    *journal.fail.lock().expect("lock") = true;
    assert_eq!(
        bus.post(PostRequest {
            run_id: run,
            tree_id: root,
            from_task_id: child,
            to_task_id: root,
            kind: ChatKind::Status,
            body: "will not commit",
            idempotency_key: "flaky-1",
            visibility: Visibility::ParentChain,
        }),
        Err(Refusal::InternalError)
    );
    let summary = bus.tree(run, root).expect("summary");
    assert_eq!(summary.message_count, 0, "no memory-only write");
    assert_eq!(summary.last_seq, 0, "sequence not consumed");

    *journal.fail.lock().expect("lock") = false;
    let committed = bus
        .post(PostRequest {
            run_id: run,
            tree_id: root,
            from_task_id: child,
            to_task_id: root,
            kind: ChatKind::Status,
            body: "commits now",
            idempotency_key: "flaky-2",
            visibility: Visibility::ParentChain,
        })
        .expect("post after recovery");
    assert_eq!(committed.message().seq, 1);
}
