//! The envelopes the obligation scenarios send, and the readings they take.
//!
//! Two halves, matching how the scenarios use them: constructors that build one
//! wire message the way a client, a relaying peer, or a leader's heartbeat puts
//! it on the wire, and readers that count or fetch what this node mailed back.
//! Nothing here asserts an obligation; the scenarios own every claim.

use rafter::{AppendEntries, LogIndex, Message, NodeId, Term};
use rafter_codec::encode_message;
use serde_json::{json, Value};

use crate::{
    protocol::{body_type, encode_hex, Envelope},
    InitializedNode,
};

/// A client's `cas` arriving straight at `dest`.
pub(super) fn client_cas(
    dest: &str,
    client: &str,
    msg_id: u64,
    key: &str,
    from: u64,
    to: u64,
) -> Envelope {
    Envelope {
        src: client.to_owned(),
        dest: dest.to_owned(),
        body: json!({
            "type": "cas", "msg_id": msg_id, "key": key, "from": from, "to": to,
        }),
    }
}

/// One peer's `client_forward` of a client request, as `forward_or_reply` emits.
pub(super) fn forward_envelope(
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
pub(super) fn heartbeat_envelope(from: &str, dest: &str, term: Term, leader: NodeId) -> Envelope {
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
pub(super) fn forwarded_answer_body(
    node: &InitializedNode,
    origin: &str,
    in_reply_to: u64,
) -> Option<Value> {
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
pub(super) fn forwarded_request(
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

/// How many `client_result` envelopes this node mailed `origin` for one request.
pub(super) fn forwarded_answers(node: &InitializedNode, origin: &str, in_reply_to: u64) -> usize {
    node.emitted
        .iter()
        .filter(|envelope| {
            envelope.dest == origin
                && body_type(&envelope.body) == Some("client_result")
                && envelope.body.get("in_reply_to").and_then(Value::as_u64) == Some(in_reply_to)
        })
        .count()
}
