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
