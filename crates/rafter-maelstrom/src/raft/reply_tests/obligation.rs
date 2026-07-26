//! The obligation a client request leaves behind, and what discharges it.
//!
//! The parent module pins *who* answers a committed write. These pin that
//! somebody does — that every request this node accepts is answered, whatever
//! becomes of the entry behind it, and that it is answered once.
//!
//! Each test here began as a red-team probe against an enumeration the reply
//! path had argued in prose: a list of the ways an answer could fail to arrive,
//! proved in the easy direction and relied on in the hard one. They are kept as
//! regression tests because the argument that replaced those lists is
//! mechanical, and these are what fail when the mechanism is removed.

use std::collections::BTreeMap;

use rafter::{AppendEntries, LogIndex, Message, NodeId, Term};
use rafter_codec::encode_message;
use rafter_invariant_test::{oracle_assert, oracle_assert_eq};
use serde_json::{json, Value};

use crate::{
    app::{encode_snapshot_payload, persist_snapshot_application_state},
    protocol::{body_type, encode_hex, Envelope},
    InitializedNode,
};

use super::{
    direct_answer, elected_single_node_process, forwarded_write, fresh_cluster_member,
    remove_test_root, replicate, test_root,
};

// ---------------------------------------------------------------------------
// A forward travels one hop.
// ---------------------------------------------------------------------------

/// A forward is not bounced between two peers forever.
///
/// `known_leader` is set from any leader-bearing message and never cleared, so
/// two nodes each holding the other as their last-seen leader is an ordinary
/// residue of two elections. Relaying a request that was itself relayed puts
/// one `client_forward` on the wire per hop for as long as that lasts.
///
/// The cap is not a hop counter on the wire: a request that arrived from a peer
/// is never relayed at all, so the chain is one hop by construction and no
/// count can be miscarried or forged.
#[test]
fn a_forward_is_not_bounced_between_two_peers_forever() {
    let root_one = test_root("obligation-forward-pingpong-n1");
    let root_two = test_root("obligation-forward-pingpong-n2");
    let mut first = fresh_cluster_member(&root_one, "n1", &["n1", "n2", "n3"]);
    let mut second = fresh_cluster_member(&root_two, "n2", &["n1", "n2", "n3"]);

    // n1 last heard n2 lead; n2 last heard n1 lead. Neither leads now.
    first
        .initialized
        .as_mut()
        .expect("n1 initializes")
        .known_leader = Some(NodeId(2));
    second
        .initialized
        .as_mut()
        .expect("n2 initializes")
        .known_leader = Some(NodeId(1));

    let mut in_flight = Some(client_write("n1", "c1", 5, "counter", 7));
    for _ in 0..20 {
        let Some(envelope) = in_flight.take() else {
            break;
        };
        let target = if envelope.dest == "n1" {
            &mut first
        } else {
            &mut second
        };
        let node = target.initialized.as_mut().expect("node stays initialized");
        let before = node.emitted.len();
        node.handle_envelope(envelope);
        in_flight = node
            .emitted
            .iter()
            .skip(before)
            .find(|emitted| body_type(&emitted.body) == Some("client_forward"))
            .map(|emitted| Envelope {
                src: emitted.src.clone(),
                dest: emitted.dest.clone(),
                body: emitted.body.clone(),
            });
    }

    let hops = client_forwards(first.initialized.as_ref().expect("n1"))
        + client_forwards(second.initialized.as_ref().expect("n2"));
    oracle_assert!(
        hops <= 2,
        "one client request must not be forwarded without bound; hops = {hops}"
    );
    remove_test_root(root_one);
    remove_test_root(root_two);
}

/// A request a node cannot serve and may not relay is refused to the peer that
/// sent it, not dropped.
///
/// The other half of the cap. Declining to relay is only bounded rather than
/// lossy because the peer is told in the same breath, and told something
/// definite: this node appended nothing, so the client may reissue with no risk
/// that the first attempt is still in flight somewhere. A silent drop would
/// trade the storm for a stranded client, which is the worse trade — the
/// `client` module header rejects it for reads and it is no better here.
#[test]
fn a_forwarded_request_this_node_cannot_serve_is_refused_rather_than_relayed() {
    let root = test_root("obligation-forward-refused");
    let mut process = fresh_cluster_member(&root, "n2", &["n1", "n2", "n3"]);
    let node = process.initialized.as_mut().expect("node initializes");

    // n2 does not lead, and believes n3 does. Without the cap this is exactly
    // the state that relays a peer's forward onward.
    node.known_leader = Some(NodeId(3));
    node.handle_envelope(forward_envelope(
        "n1",
        "n2",
        "c1",
        5,
        &json!({ "type": "write", "key": "counter", "value": 7 }),
    ));

    oracle_assert_eq!(
        client_forwards(node),
        0,
        "a request that already travelled one hop is not relayed a second time; \
         emitted = {:#?}",
        node.emitted
    );
    oracle_assert!(
        forwarded_answer_body(node, "n1", 5).is_some(),
        "and the peer waiting on this node is told so; emitted = {:#?}",
        node.emitted
    );
    remove_test_root(root);
}

/// A leader announcement from a term this node has left does not become the
/// target of its next forward.
///
/// `observe_leader` runs before the step, so it can compare the announcement
/// against the term this node already holds. A strictly older term names a
/// leader the cluster has replaced; recording it spends this request's one hop
/// on a node that can only refuse it.
#[test]
fn a_stale_leader_announcement_does_not_become_the_next_forward_target() {
    let root = test_root("obligation-stale-leader");
    let mut process = fresh_cluster_member(&root, "n2", &["n1", "n2", "n3"]);
    let node = process.initialized.as_mut().expect("node initializes");

    // n1 leads in term 5, and n2 hears it.
    node.handle_envelope(heartbeat_envelope("n1", "n2", Term(5), NodeId(1)));
    oracle_assert_eq!(
        node.known_leader,
        Some(NodeId(1)),
        "the current leader's heartbeat is recorded"
    );

    // A heartbeat from term 2 arrives late, from a leader deposed three terms
    // ago. The kernel already ignores it; the forwarding memory must too.
    node.handle_envelope(heartbeat_envelope("n3", "n2", Term(2), NodeId(3)));
    oracle_assert_eq!(
        node.known_leader,
        Some(NodeId(1)),
        "a leader from an older term does not replace the current one"
    );
    remove_test_root(root);
}

// ---------------------------------------------------------------------------
// Every accepted request is answered.
// ---------------------------------------------------------------------------

/// The node that took the request from the client answers it when the node that
/// took the forward does not.
///
/// The reply path once licensed a volatile record, and a residue left behind by
/// a snapshot install, on an unconditional claim about a *different* process:
/// "the forwarding peer applies the same entry with its own name as `origin`
/// and answers its client directly". Nothing enforced it, and the very
/// mechanism the sentence invoked is what removes the forwarding peer's route
/// to its own client — `apply_command` returns on `AlreadyApplied` before it
/// ever reaches the claim, and an applied index jumped past the entry means no
/// apply arrives for it again at all.
///
/// The interleaving is ordinary: a follower relays a write, falls behind or is
/// partitioned, the leader's `client_result` is lost or the leader restarts,
/// and the follower rejoins behind a compacted log and takes an
/// `InstallSnapshot`. The write is in this node's own state machine and nobody
/// is left who will say so.
#[test]
fn a_relaying_peer_answers_its_client_when_the_accepting_node_does_not() {
    let root = test_root("obligation-forwarder-snapshot-jump");
    let mut process = fresh_cluster_member(&root, "n2", &["n1", "n2", "n3"]);
    let node = process.initialized.as_mut().expect("node initializes");

    // n1 leads and n2 has heard from it. That is all `forward_or_reply` needs.
    node.known_leader = Some(NodeId(1));

    // c1's write reaches n2, which relays it to n1 and records what it now owes.
    node.handle_envelope(client_write("n2", "c1", 5, "counter", 7));
    oracle_assert!(
        forwarded_request(node, "n1", "c1", 5).is_some(),
        "n2 relays the write to the leader it knows; emitted = {:#?}",
        node.emitted
    );
    oracle_assert_eq!(
        node.owed_answers.answer_to(&("c1".to_owned(), 5)),
        Some(node.name.as_str()),
        "and holds a record naming itself as the node that answers this client"
    );

    // The leader accepted the forward, proposed it with origin = "n2", and it
    // committed at index 1. Its `client_result` never reaches n2 — n2 was
    // partitioned, or the leader restarted and its volatile record went with
    // it. n2 rejoins behind the compacted log and takes an InstallSnapshot
    // covering index 1: precisely what `apply_snapshot` does to `app`.
    let mut kv = BTreeMap::new();
    kv.insert("\"counter\"".to_owned(), json!(7));
    persist_snapshot_application_state(
        &root,
        &mut node.app,
        LogIndex(1),
        &encode_snapshot_payload(&kv).expect("snapshot payload encodes"),
    )
    .expect("snapshot application state persists");
    node.flush_reads();

    // No `Output::Apply` for that index can ever reach `apply_command` again,
    // so no event other than the deadline is coming.
    for _ in 0..10 {
        process.tick();
    }
    let node = process
        .initialized
        .as_mut()
        .expect("node stays initialized");

    oracle_assert_eq!(
        node.app.kv.get("\"counter\""),
        Some(&json!(7)),
        "the write committed and this node's state machine carries it"
    );
    oracle_assert!(
        direct_answer(node, "c1", 5).is_some(),
        "the node that took the request from the client must answer it; \
         emitted = {:#?}",
        node.emitted
    );
    oracle_assert!(
        node.owed_answers.is_empty(),
        "and the record that said so is retired by the answer going out"
    );
    remove_test_root(root);
}

/// Control for the probe above: it isolates the snapshot jump as the cause
/// rather than the setup.
///
/// Same node, same relay, same lost `client_result`. The one difference is that
/// this time n2 applies the entry itself, so the fast path answers immediately
/// and the deadline is never reached. That is what the deadline is a backstop
/// *for*, and the control is what keeps it from being the only path in use.
#[test]
fn control_a_relaying_peer_that_applies_the_entry_answers_its_client_at_once() {
    let root = test_root("obligation-forwarder-applies");
    let mut process = fresh_cluster_member(&root, "n2", &["n1", "n2", "n3"]);
    let node = process.initialized.as_mut().expect("node initializes");
    node.known_leader = Some(NodeId(1));
    node.handle_envelope(client_write("n2", "c1", 5, "counter", 7));

    // No snapshot jump: the entry the leader proposed for n2 replicates here and
    // applies normally. No tick is driven, so nothing here can be the deadline.
    node.step(replicate(
        &[forwarded_write("n2", "c1", 5, "counter", 7)],
        1,
    ));

    oracle_assert!(
        direct_answer(node, "c1", 5).is_some(),
        "a relaying peer that applies the entry answers its client from the apply"
    );
    oracle_assert!(
        node.owed_answers.is_empty(),
        "and that answer retires the record just the same"
    );
    remove_test_root(root);
}

/// A proposal the kernel refuses is answered to the peer that relayed it.
///
/// `propose` recorded the obligation before stepping, and `Output::RejectProposal`
/// printed and returned. No entry is appended, so no apply ever runs, so nothing
/// pays or clears the record — and the peer that relayed the request, and the
/// client behind it, are told nothing. This is the shape the checkpoint-failure
/// fix called the bug it repaired, surviving one arm over in the same match.
#[test]
fn a_rejected_proposal_answers_the_peer_that_relayed_it() {
    let root = test_root("obligation-rejected-proposal-answer");
    let mut process = elected_single_node_process(&root);
    let node = process.initialized.as_mut().expect("node initializes");

    // A value larger than one AppendEntries may carry. `step_client_proposal`
    // answers `ProposalRejection::PayloadTooLarge` and appends nothing.
    let oversized = "x".repeat(600 * 1024);
    node.handle_envelope(forward_envelope(
        "n2",
        "n1",
        "c1",
        5,
        &json!({ "type": "write", "key": "counter", "value": oversized }),
    ));

    oracle_assert!(
        forwarded_answer_body(node, "n2", 5).is_some(),
        "a request this node accepted and then refused to log must still be \
         answered — nothing else in the cluster holds a record of it; \
         emitted = {:#?}",
        node.emitted
    );
    remove_test_root(root);
}

/// The same refusal leaves no obligation behind.
///
/// A record now exists exactly when the kernel appended an entry for the
/// request, because both are decided from the same step's outputs. Here the
/// entry does not exist on this node or any other, no snapshot is involved, and
/// nothing may survive to be swept later.
#[test]
fn a_rejected_proposal_leaves_no_obligation_behind() {
    let root = test_root("obligation-rejected-proposal-leak");
    let mut process = elected_single_node_process(&root);
    let node = process.initialized.as_mut().expect("node initializes");

    let oversized = "x".repeat(600 * 1024);
    node.handle_envelope(forward_envelope(
        "n2",
        "n1",
        "c1",
        5,
        &json!({ "type": "write", "key": "counter", "value": oversized }),
    ));

    oracle_assert!(
        node.owed_answers.is_empty(),
        "an obligation for an entry that will never exist must not survive"
    );
    remove_test_root(root);
}

/// An obligation is paid to the peer that lodged it, not to whichever origin a
/// committed entry happens to name.
///
/// The record was once a bare set of request keys with no origin in it, while
/// the answer's recipient was read out of `command.origin` — the payload, which
/// every replica sees identically. So the record could not say who it owed, and
/// the answer went to whichever origin the first matching commit carried. Here
/// a record lodged for n2's forward would pay n3, a node that relayed nothing,
/// while n2 gets silence.
#[test]
fn an_obligation_is_paid_to_the_peer_that_lodged_it() {
    let root = test_root("obligation-misdelivery");
    let mut process = fresh_cluster_member(&root, "n1", &["n1", "n2", "n3"]);
    let node = process.initialized.as_mut().expect("node initializes");

    // The state `propose` leaves behind for a forward n2 sent here.
    node.accept_answer_obligation("n2", "c1", 5);

    // The entry that commits for (c1, 5) carries a different origin.
    node.step(replicate(
        &[forwarded_write("n3", "c1", 5, "counter", 7)],
        1,
    ));

    oracle_assert_eq!(
        forwarded_answer_body(node, "n3", 5),
        None,
        "a node that relayed nothing here must not be handed an answer"
    );
    oracle_assert!(
        forwarded_answer_body(node, "n2", 5).is_some(),
        "and the peer whose forward this node actually accepted must be"
    );
    remove_test_root(root);
}

/// A request stranded with no local event to notice it is still answered.
///
/// The general statement of the property, driven without a snapshot, a
/// rejection or a relay: a leader accepts a forward, the entry never commits,
/// and nothing on this node will ever fire for it again. That covers the case
/// argued but not reproducible in-crate — a leader deposed before its entry
/// replicates, whose entry the next leader truncates — because the sweep does
/// not ask which of those happened.
///
/// The answer is indefinite, and that is the load-bearing part. The entry may
/// yet commit under another leader, so any code asserting the write did not
/// happen would be a statement this node cannot make.
#[test]
fn an_accepted_request_with_no_outcome_is_answered_indefinitely_at_its_deadline() {
    let root = test_root("obligation-deadline-sweep");
    let mut process = fresh_cluster_member(&root, "n1", &["n1", "n2", "n3"]);
    let node = process.initialized.as_mut().expect("node initializes");

    // The state `propose` leaves behind for a forward n2 sent here. No entry
    // for it ever commits on this node.
    node.accept_answer_obligation("n2", "c1", 5);
    oracle_assert_eq!(
        forwarded_answer_body(node, "n2", 5),
        None,
        "nothing is owed yet — the request may still commit"
    );

    for _ in 0..10 {
        process.tick();
    }
    let node = process
        .initialized
        .as_mut()
        .expect("node stays initialized");

    let answer = forwarded_answer_body(node, "n2", 5).expect("the deadline answers the peer");
    oracle_assert_eq!(
        answer.pointer("/result/code").and_then(Value::as_u64),
        Some(0),
        "and answers with Maelstrom's indefinite `timeout`, because this node \
         cannot say whether the write took effect; answer = {answer:#?}"
    );
    oracle_assert!(
        node.owed_answers.is_empty(),
        "the swept record is retired by the answer, not merely dropped"
    );
    remove_test_root(root);
}

// ---------------------------------------------------------------------------
// Helpers.
// ---------------------------------------------------------------------------

/// A client's `write` arriving straight at `dest`.
fn client_write(dest: &str, client: &str, msg_id: u64, key: &str, value: u64) -> Envelope {
    Envelope {
        src: client.to_owned(),
        dest: dest.to_owned(),
        body: json!({ "type": "write", "msg_id": msg_id, "key": key, "value": value }),
    }
}

/// One peer's `client_forward` of a client request, as `forward_or_reply` emits.
fn forward_envelope(
    from: &str,
    dest: &str,
    client: &str,
    in_reply_to: u64,
    request: &Value,
) -> Envelope {
    Envelope {
        src: from.to_owned(),
        dest: dest.to_owned(),
        body: json!({
            "type": "client_forward",
            "client": client,
            "in_reply_to": in_reply_to,
            "request": request,
        }),
    }
}

/// An empty `AppendEntries` from `leader` in `term`, framed the way the wire
/// carries it.
fn heartbeat_envelope(from: &str, dest: &str, term: Term, leader: NodeId) -> Envelope {
    let message = Message::AppendEntries(AppendEntries {
        term,
        leader_id: leader,
        prev_log_index: LogIndex::ZERO,
        prev_log_term: Term(0),
        sequence: 1,
        entries: Vec::new().into(),
        leader_commit: LogIndex::ZERO,
    });
    let frame = encode_message(&message).expect("message encodes");
    Envelope {
        src: from.to_owned(),
        dest: dest.to_owned(),
        body: json!({ "type": "raft", "frame": encode_hex(&frame) }),
    }
}

/// The `client_result` this node handed back to `origin` for one request.
fn forwarded_answer_body(node: &InitializedNode, origin: &str, in_reply_to: u64) -> Option<Value> {
    node.emitted
        .iter()
        .find(|envelope| {
            envelope.dest == origin
                && body_type(&envelope.body) == Some("client_result")
                && envelope.body.get("in_reply_to").and_then(Value::as_u64) == Some(in_reply_to)
        })
        .map(|envelope| envelope.body.clone())
}

/// The `client_forward` this node handed to `leader` for one client request.
fn forwarded_request(
    node: &InitializedNode,
    leader: &str,
    client: &str,
    in_reply_to: u64,
) -> Option<Value> {
    node.emitted
        .iter()
        .find(|envelope| {
            envelope.dest == leader
                && body_type(&envelope.body) == Some("client_forward")
                && envelope.body.get("client").and_then(Value::as_str) == Some(client)
                && envelope.body.get("in_reply_to").and_then(Value::as_u64) == Some(in_reply_to)
        })
        .map(|envelope| envelope.body.clone())
}

/// How many `client_forward` envelopes this node put on the wire.
fn client_forwards(node: &InitializedNode) -> usize {
    node.emitted
        .iter()
        .filter(|envelope| body_type(&envelope.body) == Some("client_forward"))
        .count()
}
