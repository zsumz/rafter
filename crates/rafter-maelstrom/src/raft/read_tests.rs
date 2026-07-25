//! The read gate: which applied index a granted barrier actually waits for,
//! and what re-examines a read that is still waiting.

use std::{
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use rafter::LogIndex;
use rafter_invariant_test::{oracle_assert, oracle_assert_eq};
use serde_json::json;

use crate::{protocol::Envelope, MaelstromNode, PendingRead};

static TEST_DIRECTORY_ID: AtomicU64 = AtomicU64::new(0);

/// The regression, at the kernel-direct gate.
///
/// A single-node cluster's first entry is its leader's `Noop` at index 1, and
/// the barrier grants there. No application entry has ever committed, so the
/// application floor below the read index is `LogIndex::ZERO` and a state
/// machine that has applied nothing already satisfies it. Gating on the read
/// index instead — which is what this node did — held the read until an
/// unrelated write committed, and on a read-only tail that never happens.
#[test]
fn a_read_granted_at_a_noop_index_flushes_without_a_later_apply() {
    let root = test_root("read-noop-index");
    let mut process = elected_single_node_process(&root);
    let node = process.initialized.as_mut().expect("node initializes");

    oracle_assert_eq!(
        node.node.commit_index(),
        LogIndex(1),
        "the only committed entry is the leadership noop"
    );
    oracle_assert_eq!(
        node.app.applied,
        LogIndex::ZERO,
        "a noop never reaches the application"
    );
    oracle_assert_eq!(
        node.node.committed_application_index_through(LogIndex(1)),
        LogIndex::ZERO,
        "there is no application entry at or below the read index"
    );

    node.handle_envelope(read_envelope("c1", 7, "counter"));

    oracle_assert!(
        node.completed_replies.contains(&("c1".to_owned(), 7)),
        "a read granted at a noop index must answer with no write in between"
    );
    oracle_assert!(
        node.pending_reads.is_empty(),
        "the answered read must leave no waiter behind"
    );
    remove_test_root(root);
}

/// The tick is what re-examines a read that stalled.
///
/// `flush_reads` otherwise runs only from the grant arm, from an apply, and
/// from a snapshot install, so a read whose floor becomes reachable through any
/// other path waits for unrelated traffic to arrive and trigger a pass. This
/// stalls a granted read, advances the applied state through a path that calls
/// no flush hook, and drives nothing but time.
#[test]
fn a_stalled_read_is_reexamined_by_a_tick_with_no_apply_to_trigger_it() {
    let root = test_root("read-tick-retry");
    let mut process = elected_single_node_process(&root);
    let node = process.initialized.as_mut().expect("node initializes");
    node.pending_reads.insert(
        1,
        PendingRead {
            origin: node.name.clone(),
            client: "c1".to_owned(),
            in_reply_to: 9,
            key: json!("counter"),
            application_floor: Some(LogIndex(1)),
        },
    );
    node.flush_reads();
    oracle_assert!(
        !node.completed_replies.contains(&("c1".to_owned(), 9)),
        "the read must stall while the applied state is below its floor"
    );

    // An applied-state advance that no flush hook observes.
    node.app.applied = LogIndex(1);
    process.tick();

    let node = process
        .initialized
        .as_mut()
        .expect("node stays initialized");
    oracle_assert!(
        node.completed_replies.contains(&("c1".to_owned(), 9)),
        "a tick alone must re-examine the stalled read"
    );
    oracle_assert!(node.pending_reads.is_empty());
    remove_test_root(root);
}

fn elected_single_node_process(root: &Path) -> MaelstromNode {
    let mut process = MaelstromNode::default();
    process
        .initialize_at_root(&init_envelope("n1", &["n1"]), root.to_path_buf())
        .expect("production Maelstrom initialization opens a fresh node");
    for _ in 0..20 {
        if process
            .initialized
            .as_ref()
            .expect("node initializes")
            .node
            .commit_index()
            > LogIndex::ZERO
        {
            break;
        }
        process.tick();
    }
    process
}

fn init_envelope(node_name: &str, node_names: &[&str]) -> Envelope {
    Envelope {
        src: "controller".to_owned(),
        dest: node_name.to_owned(),
        body: json!({
            "type": "init",
            "msg_id": 1,
            "node_id": node_name,
            "node_ids": node_names,
        }),
    }
}

fn read_envelope(client: &str, msg_id: u64, key: &str) -> Envelope {
    Envelope {
        src: client.to_owned(),
        dest: "n1".to_owned(),
        body: json!({ "type": "read", "msg_id": msg_id, "key": key }),
    }
}

fn test_root(name: &str) -> PathBuf {
    let id = TEST_DIRECTORY_ID.fetch_add(1, Ordering::Relaxed);
    let root = std::env::temp_dir().join(format!(
        "rafter-maelstrom-{name}-{}-{id}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&root);
    root
}

fn remove_test_root(root: PathBuf) {
    let _ = std::fs::remove_dir_all(root);
}
