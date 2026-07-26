use std::{collections::BTreeMap, error::Error};

use rafter::NodeId;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct Envelope {
    pub(crate) src: String,
    pub(crate) dest: String,
    pub(crate) body: Value,
}

pub(crate) fn body_type(body: &Value) -> Option<&str> {
    body.get("type").and_then(Value::as_str)
}

/// The harness's own message types: the whole of the vocabulary this binary
/// speaks above Maelstrom's client protocol.
///
/// One list, in one place, read in both directions by the one match that routes
/// an envelope. There used to be no list at all — three string patterns in the
/// dispatch, each carrying its own sender rule or forgetting to — and "which
/// senders may say this?" was answered three times, correctly twice.
///
/// It is an enum rather than a `&str` predicate so the routing match is
/// exhaustive over it. A fourth harness message cannot be added without rustc
/// demanding a row for it beside the three below, and the only rows available
/// are rows for a sender this node's membership recognized: everything else is
/// already spoken for. That is what makes "every arm decides which senders it
/// hears from" a thing the compiler keeps rather than a sentence in a doc
/// comment.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum HarnessMessage {
    /// A framed Raft message between peers.
    Raft,
    /// A client request one peer could not serve and relayed here.
    ClientForward,
    /// What became of a request this node relayed to a peer.
    ClientResult,
}

impl HarnessMessage {
    /// Classifies one `body.type`, or reports that it is not one of ours.
    ///
    /// `None` is the client protocol: Maelstrom's own operations, and anything
    /// a build of this harness does not implement. Both are a client's to send
    /// and neither is a peer's, which is the other half of the same list.
    pub(crate) fn of(body_type: &str) -> Option<Self> {
        match body_type {
            "raft" => Some(Self::Raft),
            "client_forward" => Some(Self::ClientForward),
            "client_result" => Some(Self::ClientResult),
            _ => None,
        }
    }
}

/// One envelope's `src`, resolved against this node's membership: a sender this
/// cluster knows as a node.
///
/// Minted only by [`Self::recognized`], out of a field no other module can name
/// or fill, and that constructor is the membership lookup itself. So a value of
/// this type in hand is the same fact as `src` being in `name_to_id` — not a
/// claim that somebody checked, but the check — and no caller anywhere can
/// produce one for a name the map does not hold.
///
/// It exists to be *required*. Every handler for a [`HarnessMessage`] takes one,
/// so a harness message from a client, a Maelstrom service, or a name this
/// cluster has never heard of cannot reach a handler at all. That was three
/// separate lookups written into three handlers, two of which had one; the arm
/// that did not took `envelope.src` as the node a client's answer would be
/// addressed to, unchecked.
///
/// Both names for the sender ride along, because they are one lookup. The
/// kernel knows a peer by its [`NodeId`] and the wire knows it by its Maelstrom
/// name, and a handler that resolved one while the dispatch resolved the other
/// would be two reads of one map that can disagree. Every handler takes what it
/// needs from the token, so none of them reads `envelope.src` for an identity
/// again.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Peer {
    name: String,
    id: NodeId,
}

impl Peer {
    /// This sender as a peer, if this node's membership knows the name.
    ///
    /// The only constructor, deliberately: the gate is inside it, so calling it
    /// with a client's name yields `None` rather than a token, wherever it is
    /// called from.
    pub(crate) fn recognized(name_to_id: &BTreeMap<String, NodeId>, src: &str) -> Option<Self> {
        name_to_id.get(src).copied().map(|id| Self {
            name: src.to_owned(),
            id,
        })
    }

    /// This peer's Maelstrom name, as the membership map holds it.
    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    /// The kernel's identity for this peer.
    pub(crate) fn id(&self) -> NodeId {
        self.id
    }
}

pub(crate) fn required_str<'a>(body: &'a Value, field: &str) -> Result<&'a str, Box<dyn Error>> {
    body.get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("body.{field} must be a string").into())
}

pub(crate) fn required_array<'a>(
    body: &'a Value,
    field: &str,
) -> Result<&'a Vec<Value>, Box<dyn Error>> {
    body.get(field)
        .and_then(Value::as_array)
        .ok_or_else(|| format!("body.{field} must be an array").into())
}

pub(crate) fn required_u64(body: &Value, field: &str) -> Result<u64, Box<dyn Error>> {
    body.get(field)
        .and_then(Value::as_u64)
        .ok_or_else(|| format!("body.{field} must be an unsigned integer").into())
}

pub(crate) fn node_id_map(node_names: &[String]) -> BTreeMap<String, NodeId> {
    node_names
        .iter()
        .enumerate()
        .map(|(index, name)| {
            let protocol_id = u64::try_from(index + 1).expect("node count fits u64");
            (name.clone(), NodeId(protocol_id))
        })
        .collect()
}

pub(crate) fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

pub(crate) fn decode_hex(encoded: &str) -> Result<Vec<u8>, String> {
    let bytes = encoded.as_bytes();
    if !bytes.len().is_multiple_of(2) {
        return Err("hex string has odd length".to_string());
    }
    bytes
        .chunks_exact(2)
        .map(|pair| {
            let high = hex_value(pair[0])?;
            let low = hex_value(pair[1])?;
            Ok((high << 4) | low)
        })
        .collect()
}

fn hex_value(byte: u8) -> Result<u8, String> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(format!("invalid hex byte {byte}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use rafter::{LogIndex, Message};
    use rafter_codec::{decode_message, encode_message};

    #[test]
    fn raft_frames_round_trip_through_hex_for_maelstrom_body() {
        let message = Message::RequestVote(rafter::RequestVote {
            term: rafter::Term(3),
            candidate_id: NodeId(2),
            last_log_index: LogIndex(5),
            last_log_term: rafter::Term(2),
        });
        let frame = encode_message(&message).expect("message encodes");
        let decoded_frame = decode_hex(&encode_hex(&frame)).expect("hex decodes");
        let decoded = decode_message(&decoded_frame).expect("message decodes");
        assert_eq!(decoded, message);
    }

    #[test]
    fn node_ids_follow_maelstrom_init_order() {
        let map = node_id_map(&["n3".to_string(), "n1".to_string(), "n2".to_string()]);
        assert_eq!(map["n3"], NodeId(1));
        assert_eq!(map["n1"], NodeId(2));
        assert_eq!(map["n2"], NodeId(3));
    }
}
