//! The read gate: which applied index a granted barrier actually waits for,
//! what re-examines a read that is still waiting, and who the answer reaches.

use std::{
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use rafter::{Input, LogIndex, Message, NodeId, RequestVote, Role, Term};
use rafter_invariant_test::{oracle_assert, oracle_assert_eq};
use serde_json::{json, Value};

use crate::{
    protocol::{body_type, Envelope},
    InitializedNode, MaelstromNode, PendingRead,
};

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

/// A forwarded read granted before this node lost leadership is still answered
/// after it.
///
/// This is the interleaving the tick-driven flush made reachable. The grant
/// resolved an application floor the state machine had not passed yet, nothing
/// else on this node touched the waiter, and by the time a tick re-examined it
/// the node had stepped down. The reply gate used to require `role() == Leader`
/// before a forwarded origin could be answered, and `flush_reads` retired the
/// waiter before that gate ran — so the read was consumed and answered to
/// nobody. Answering here is correct, not merely convenient: the grant is a
/// finished quorum proof and the applied state is a committed prefix, so the
/// answer is the committed state at an instant inside the read's own interval.
/// The `client` module header carries the argument.
#[test]
fn a_forwarded_read_granted_before_demotion_is_answered_after_it() {
    let root = test_root("read-forwarded-after-demotion");
    let mut process = elected_single_node_process(&root);
    let node = process.initialized.as_mut().expect("node initializes");
    node.handle_envelope(write_envelope("c0", 3, "counter", 7));
    oracle_assert_eq!(
        node.app.applied,
        LogIndex(2),
        "the write applies above the leadership noop"
    );

    // A granted barrier this node cannot answer yet: its floor is above the
    // applied state, so the read stalls with its answer still owed.
    node.pending_reads.insert(
        1,
        PendingRead {
            origin: "n2".to_owned(),
            client: "c1".to_owned(),
            in_reply_to: 11,
            key: json!("counter"),
            application_floor: Some(LogIndex(3)),
        },
    );
    node.flush_reads();
    oracle_assert!(
        node.pending_reads.contains_key(&1),
        "the read stalls while the applied state is below its floor"
    );

    step_down_under_a_higher_term(node);
    oracle_assert!(
        node.node.role() != Role::Leader,
        "a higher term steps this node down"
    );

    // The applied state reaches the floor only now, with leadership already
    // gone and only the tick left to notice.
    node.app.applied = LogIndex(3);
    process.tick();

    let node = process
        .initialized
        .as_mut()
        .expect("node stays initialized");
    oracle_assert!(
        node.node.role() != Role::Leader,
        "the answer must be produced while this node is not the leader"
    );
    oracle_assert!(
        node.pending_reads.is_empty(),
        "the answered read leaves no waiter behind"
    );
    oracle_assert_eq!(
        forwarded_answer(node, "n2", 11),
        Some(json!({ "kind": "read_ok", "value": 7 })),
        "the forwarded read is answered to the node that forwarded it"
    );
    remove_test_root(root);
}

/// A flush retires a waiter only against an answer that actually left the node.
///
/// The consumption-ordering defect was structural rather than about any single
/// gate: `flush_reads` removed the waiter and only afterwards let
/// `deliver_result` decide whether it could send, so any arm that declined
/// dropped the read with nothing left to record that an answer was owed. This
/// pins the coupling itself — over one flush, on a node that is not the leader,
/// across both reply shapes: the local client, and a peer that forwarded.
#[test]
fn a_flush_answers_every_waiter_it_retires() {
    let root = test_root("read-flush-answers-all");
    let mut process = fresh_cluster_member(&root, "n1", &["n1", "n2", "n3"]);
    let node = process.initialized.as_mut().expect("node initializes");
    oracle_assert!(
        node.node.role() != Role::Leader,
        "a fresh member of a three-node cluster does not lead"
    );

    for (request_id, origin, client, in_reply_to) in [
        (1_u64, "n1", "c1", 21_u64),
        (2, "n2", "c2", 22),
        (3, "n3", "c3", 23),
    ] {
        node.pending_reads.insert(
            request_id,
            PendingRead {
                origin: origin.to_owned(),
                client: client.to_owned(),
                in_reply_to,
                key: json!("counter"),
                application_floor: Some(LogIndex::ZERO),
            },
        );
    }

    node.flush_reads();

    oracle_assert!(
        node.pending_reads.is_empty(),
        "every waiter at or above its floor is retired"
    );
    oracle_assert!(
        direct_answer(node, "c1", 21).is_some(),
        "the locally originated read is answered to its client"
    );
    oracle_assert!(
        forwarded_answer(node, "n2", 22).is_some(),
        "a forwarded read is answered to its origin though this node does not lead"
    );
    oracle_assert!(
        forwarded_answer(node, "n3", 23).is_some(),
        "and so is every other forwarded read the same flush retired"
    );
    remove_test_root(root);
}

/// A read answered once must not be answered twice, however many flush hooks
/// run. Delivering before retiring the waiter must not change that.
#[test]
fn a_flushed_read_is_not_answered_twice() {
    let root = test_root("read-double-flush");
    let mut process = elected_single_node_process(&root);
    let node = process.initialized.as_mut().expect("node initializes");
    node.handle_envelope(read_envelope("c1", 21, "counter"));
    oracle_assert!(node.completed_replies.contains(&("c1".to_owned(), 21)));
    oracle_assert!(node.pending_reads.is_empty());

    let emitted_before = node.emitted.len();
    node.flush_reads();
    process.tick();
    let node = process
        .initialized
        .as_mut()
        .expect("node stays initialized");
    node.flush_reads();

    oracle_assert_eq!(
        node.emitted.len(),
        emitted_before,
        "no flush hook re-answers a read that already left the table"
    );
    oracle_assert!(node.pending_reads.is_empty());
    remove_test_root(root);
}

/// Steps this node down the way production does: a higher term arrives.
fn step_down_under_a_higher_term(node: &mut InitializedNode) {
    let higher = Term(node.node.current_term().0 + 1);
    node.step(Input::Message {
        from: NodeId(2),
        message: Message::RequestVote(RequestVote {
            term: higher,
            candidate_id: NodeId(2),
            last_log_index: LogIndex(64),
            last_log_term: higher,
        }),
    });
}

/// The result this node handed back to `origin` for a request `origin`
/// forwarded to it.
fn forwarded_answer(node: &InitializedNode, origin: &str, in_reply_to: u64) -> Option<Value> {
    node.emitted
        .iter()
        .find(|envelope| {
            envelope.dest == origin
                && body_type(&envelope.body) == Some("client_result")
                && envelope.body.get("in_reply_to").and_then(Value::as_u64) == Some(in_reply_to)
        })
        .and_then(|envelope| envelope.body.get("result").cloned())
}

/// The reply this node sent straight to `client`.
fn direct_answer(node: &InitializedNode, client: &str, in_reply_to: u64) -> Option<Value> {
    node.emitted
        .iter()
        .find(|envelope| {
            envelope.dest == client
                && envelope.body.get("in_reply_to").and_then(Value::as_u64) == Some(in_reply_to)
        })
        .map(|envelope| envelope.body.clone())
}

fn fresh_cluster_member(root: &Path, node_name: &str, node_names: &[&str]) -> MaelstromNode {
    let mut process = MaelstromNode::default();
    process
        .initialize_at_root(&init_envelope(node_name, node_names), root.to_path_buf())
        .expect("production Maelstrom initialization opens a fresh node");
    process
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

fn write_envelope(client: &str, msg_id: u64, key: &str, value: u64) -> Envelope {
    Envelope {
        src: client.to_owned(),
        dest: "n1".to_owned(),
        body: json!({ "type": "write", "msg_id": msg_id, "key": key, "value": value }),
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
