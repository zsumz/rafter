//! Maelstrom body encoding and the identities init order assigns.
//!
//! A Raft frame must survive the hex round trip a Maelstrom body carries it in,
//! and node identities must follow the order the init message declares, so the
//! harness and the kernel never disagree about who is who.

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
