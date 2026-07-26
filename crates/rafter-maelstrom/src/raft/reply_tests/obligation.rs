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

use rafter::{AppendEntries, LogIndex, Message, NodeId, Term};
use rafter_codec::encode_message;
use rafter_invariant_test::{oracle_assert, oracle_assert_eq};
use serde_json::{json, Value};

use crate::{
    protocol::{body_type, encode_hex, Envelope},
    InitializedNode,
};

use super::{fresh_cluster_member, remove_test_root, test_root};

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

/// How many `client_forward` envelopes this node put on the wire.
fn client_forwards(node: &InitializedNode) -> usize {
    node.emitted
        .iter()
        .filter(|envelope| body_type(&envelope.body) == Some("client_forward"))
        .count()
}
