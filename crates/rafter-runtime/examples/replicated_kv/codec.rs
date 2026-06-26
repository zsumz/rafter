use std::{collections::BTreeMap, net::SocketAddr};

use rafter::{LogIndex, NodeId};

pub(crate) fn fields(line: &str) -> Vec<&str> {
    line.split_whitespace().collect()
}

pub(crate) fn parse_node_id(raw: &str) -> NodeId {
    NodeId(raw.parse().expect("node id parses"))
}

pub(crate) fn parse_log_index(raw: &str) -> LogIndex {
    LogIndex(raw.parse().expect("log index parses"))
}

pub(crate) fn parse_peer(raw: &str) -> (NodeId, SocketAddr) {
    let (node_id, addr) = raw.split_once('=').expect("peer is id=addr");
    (
        parse_node_id(node_id),
        addr.parse().expect("peer socket address parses"),
    )
}

pub(crate) fn encode_value(value: Option<&String>) -> &str {
    value.map_or("-", String::as_str)
}

pub(crate) fn decode_value(value: &str) -> Option<String> {
    (value != "-").then(|| value.to_string())
}

pub(crate) fn encode_set(key: &str, value: &str) -> Vec<u8> {
    format!("set\t{key}\t{value}").into_bytes()
}

pub(crate) fn apply_set(command: &str, kv: &mut BTreeMap<String, String>) {
    let mut fields = command.splitn(3, '\t');
    assert_eq!(fields.next(), Some("set"));
    let key = fields.next().expect("set key");
    let value = fields.next().expect("set value");
    kv.insert(key.to_string(), value.to_string());
}

pub(crate) fn apply_set_with_parts(
    command: &str,
    kv: &mut BTreeMap<String, String>,
) -> (String, String) {
    let mut fields = command.splitn(3, '\t');
    assert_eq!(fields.next(), Some("set"));
    let key = fields.next().expect("set key").to_string();
    let value = fields.next().expect("set value").to_string();
    kv.insert(key.clone(), value.clone());
    (key, value)
}

pub(crate) fn encode_snapshot(kv: &BTreeMap<String, String>) -> Vec<u8> {
    let mut payload = Vec::new();
    for (key, value) in kv {
        payload.extend_from_slice(key.as_bytes());
        payload.push(b'\t');
        payload.extend_from_slice(value.as_bytes());
        payload.push(b'\n');
    }
    payload
}

pub(crate) fn decode_snapshot(payload: &[u8]) -> BTreeMap<String, String> {
    let text = std::str::from_utf8(payload).expect("snapshot payload is UTF-8");
    let mut kv = BTreeMap::new();
    for line in text.lines() {
        if line.is_empty() {
            continue;
        }
        let (key, value) = line
            .split_once('\t')
            .expect("snapshot entry is tab-separated");
        kv.insert(key.to_string(), value.to_string());
    }
    kv
}

pub(crate) fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = 0xffff_ffff_u32;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            let mask = 0u32.wrapping_sub(crc & 1);
            crc = (crc >> 1) ^ (0xedb8_8320 & mask);
        }
    }
    !crc
}
