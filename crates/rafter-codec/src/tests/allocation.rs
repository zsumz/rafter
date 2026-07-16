//! Reusable-buffer and shared decoded-payload allocation contracts.

use rafter::{AppendEntries, InstallSnapshot, LogEntry, LogIndex, Message, NodeId, Term};

use super::support::{append_entries, application_payload, test_snapshot};
use crate::{decode_message, encode_message, encode_message_into, EncodePeerMessageError};

#[test]
fn decoded_append_entries_payloads_share_the_frame_allocation() {
    let message = Message::AppendEntries(AppendEntries {
        sequence: 0,
        term: Term(8),
        leader_id: NodeId(1),
        prev_log_index: LogIndex(10),
        prev_log_term: Term(7),
        entries: vec![
            LogEntry::application(Term(8), b"first payload".to_vec()),
            LogEntry::application(Term(8), b"second payload".to_vec()),
        ]
        .into(),
        leader_commit: LogIndex(11),
    });
    let encoded = encode_message(&message).expect("message encodes");
    let decoded = decode_message(&encoded).expect("message decodes");
    let Message::AppendEntries(decoded) = decoded else {
        panic!("message remains append entries");
    };
    let first = application_payload(&decoded.entries[0]);
    let second = application_payload(&decoded.entries[1]);

    assert_eq!(first, b"first payload");
    assert_eq!(second, b"second payload");
    assert!(first.shares_allocation(second));
}

#[test]
fn encode_message_into_matches_owned_encoder_and_reuses_buffer() {
    let message = append_entries();
    let expected = encode_message(&message).expect("message encodes");
    let mut encoded = vec![0; expected.len() + 128];
    let original_ptr = encoded.as_ptr();

    encode_message_into(&mut encoded, &message).expect("message encodes into buffer");

    assert_eq!(encoded, expected);
    assert_eq!(encoded.as_ptr(), original_ptr);
}

#[test]
fn encode_message_into_clears_buffer_on_error() {
    let message = Message::InstallSnapshot(InstallSnapshot {
        term: Term(9),
        leader_id: NodeId(1),
        metadata: test_snapshot().metadata,
        application_payload: b"snapshot bytes".to_vec(),
    });
    let mut encoded = vec![1, 2, 3];

    assert!(matches!(
        encode_message_into(&mut encoded, &message),
        Err(EncodePeerMessageError::UnsupportedMessage {
            message: "InstallSnapshot",
            ..
        })
    ));
    assert!(encoded.is_empty());
}
