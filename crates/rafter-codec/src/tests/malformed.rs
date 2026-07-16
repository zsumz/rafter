//! Malformed-frame rejection and decoder failure-precedence contracts.

use rafter::NodeId;

use super::support::{
    append_entries, first_append_entry_kind_offset, rewrite_frame_checksum, snapshot_chunk,
    snapshot_group_bytes_offset, snapshot_metadata, snapshot_version_offset, vote_request,
};
use crate::{
    decode_message, encode_message,
    v1::{LogEntryTag, MembershipTag},
    DecodePeerMessageError, MAGIC, VERSION,
};

#[test]
fn decode_rejects_bad_magic_and_unsupported_version() {
    let mut bad_magic = encode_message(&vote_request()).expect("message encodes");
    bad_magic[0] = b'X';
    assert_eq!(
        decode_message(&bad_magic),
        Err(DecodePeerMessageError::InvalidMagic(*b"XFPM"))
    );

    let mut version = encode_message(&vote_request()).expect("message encodes");
    version[4] = VERSION + 1;
    assert_eq!(
        decode_message(&version),
        Err(DecodePeerMessageError::UnsupportedVersion(VERSION + 1))
    );
}

#[test]
fn decode_rejects_truncation_trailing_bytes_and_checksum_damage() {
    let encoded = encode_message(&append_entries()).expect("message encodes");
    assert_eq!(
        decode_message(&encoded[..encoded.len() - 1]),
        Err(DecodePeerMessageError::UnexpectedEof {
            needed: 4,
            remaining: 3,
        })
    );

    let mut trailing = encode_message(&vote_request()).expect("message encodes");
    trailing.push(0);
    assert_eq!(
        decode_message(&trailing),
        Err(DecodePeerMessageError::TrailingBytes(1))
    );

    let mut corrupt = encode_message(&vote_request()).expect("message encodes");
    corrupt[6] ^= 1;
    assert!(matches!(
        decode_message(&corrupt),
        Err(DecodePeerMessageError::FrameChecksumMismatch { .. })
    ));
}

#[test]
fn decode_rejects_unknown_message_entry_and_membership_tags() {
    for tag in [5, 99] {
        let mut encoded = encode_message(&vote_request()).expect("message encodes");
        encoded[5] = tag;
        assert_eq!(
            decode_message(&encoded),
            Err(DecodePeerMessageError::UnknownMessageType(tag))
        );
    }

    let mut entry = encode_message(&append_entries()).expect("message encodes");
    entry[first_append_entry_kind_offset()] = 99;
    assert_eq!(
        decode_message(&entry),
        Err(DecodePeerMessageError::UnknownLogEntryKind(99))
    );

    let mut membership = encode_message(&snapshot_chunk(snapshot_metadata(Some(
        rafter::MembershipConfig::stable(
            rafter::MembershipSet::new(vec![NodeId(1)], Vec::new()).expect("valid membership"),
        ),
    ))))
    .expect("message encodes");
    let membership_tag_offset = snapshot_version_offset(&membership) + 4;
    membership[membership_tag_offset] = 99;
    assert_eq!(
        decode_message(&membership),
        Err(DecodePeerMessageError::UnknownMembershipKind(99))
    );
}

#[test]
fn decode_rejects_invalid_boolean_and_utf8() {
    let mut boolean = encode_message(&rafter::Message::RequestVoteResponse(
        rafter::RequestVoteResponse {
            term: rafter::Term(7),
            voter_id: NodeId(3),
            vote_granted: true,
        },
    ))
    .expect("message encodes");
    boolean[22] = 2;
    assert_eq!(
        decode_message(&boolean),
        Err(DecodePeerMessageError::InvalidBoolean(2))
    );

    let mut utf8 =
        encode_message(&snapshot_chunk(snapshot_metadata(None))).expect("snapshot chunk encodes");
    utf8[snapshot_group_bytes_offset()] = 0xff;
    rewrite_frame_checksum(&mut utf8);
    assert_eq!(
        decode_message(&utf8),
        Err(DecodePeerMessageError::InvalidUtf8 {
            field: "snapshot_group_id",
        })
    );
}

#[test]
fn membership_tag_types_remain_owned_by_the_registry() {
    assert_eq!(u8::from(LogEntryTag::Application), 0);
    assert_eq!(u8::from(MembershipTag::Stable), 0);
    assert_eq!(&MAGIC, b"RFPM");
}
