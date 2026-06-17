use rafter::{CommittedConfiguration, ConfigurationId, LogIndex, NodeId, Term};

use crate::{
    crc32, decode_raft_hard_state, encode_raft_hard_state, DecodeRaftHardStateError, RaftHardState,
    RAFT_HARD_STATE_MAGIC, RAFT_HARD_STATE_VERSION,
};

fn hard_state() -> RaftHardState {
    RaftHardState {
        current_term: Term(7),
        voted_for: Some(NodeId(3)),
        commit_index: LogIndex(11),
        committed_configuration: Some(CommittedConfiguration {
            index: LogIndex(9),
            config_id: ConfigurationId(4),
        }),
    }
}

fn replace_checksum(encoded: &mut [u8]) {
    let checksum_position = encoded.len() - 4;
    let checksum = crc32(&encoded[..checksum_position]);
    encoded[checksum_position..].copy_from_slice(&checksum.to_be_bytes());
}

#[test]
fn hard_state_round_trips_through_envelope() {
    let state = hard_state();

    let encoded = encode_raft_hard_state(&state);

    assert_eq!(&encoded[..4], &RAFT_HARD_STATE_MAGIC);
    assert_eq!(encoded[4], RAFT_HARD_STATE_VERSION);
    assert_eq!(decode_raft_hard_state(&encoded), Ok(state));
}

#[test]
fn hard_state_without_vote_round_trips_through_envelope() {
    let state = RaftHardState {
        current_term: Term(11),
        voted_for: None,
        commit_index: LogIndex::ZERO,
        committed_configuration: None,
    };

    let encoded = encode_raft_hard_state(&state);

    assert_eq!(decode_raft_hard_state(&encoded), Ok(state));
}

#[test]
fn decode_rejects_corrupt_hard_state_checksum() {
    let mut encoded = encode_raft_hard_state(&hard_state());
    encoded[8] ^= 0xff;

    assert!(matches!(
        decode_raft_hard_state(&encoded),
        Err(DecodeRaftHardStateError::ChecksumMismatch { .. })
    ));
}

#[test]
fn decode_rejects_invalid_magic_after_checksum_passes() {
    let mut encoded = encode_raft_hard_state(&hard_state());
    encoded[0] = b'X';
    replace_checksum(&mut encoded);

    assert_eq!(
        decode_raft_hard_state(&encoded),
        Err(DecodeRaftHardStateError::InvalidMagic(*b"XFHS"))
    );
}

#[test]
fn decode_rejects_unsupported_version_after_checksum_passes() {
    let mut encoded = encode_raft_hard_state(&hard_state());
    encoded[4] = 99;
    replace_checksum(&mut encoded);

    assert_eq!(
        decode_raft_hard_state(&encoded),
        Err(DecodeRaftHardStateError::UnsupportedVersion(99))
    );
}

#[test]
fn decode_rejects_invalid_vote_flag_after_checksum_passes() {
    let mut encoded = encode_raft_hard_state(&hard_state());
    encoded[13] = 2;
    replace_checksum(&mut encoded);

    assert_eq!(
        decode_raft_hard_state(&encoded),
        Err(DecodeRaftHardStateError::InvalidVotedForFlag(2))
    );
}

#[test]
fn decode_rejects_invalid_committed_configuration_flag_after_checksum_passes() {
    let mut encoded = encode_raft_hard_state(&hard_state());
    encoded[30] = 2;
    replace_checksum(&mut encoded);

    assert_eq!(
        decode_raft_hard_state(&encoded),
        Err(DecodeRaftHardStateError::InvalidCommittedConfigurationFlag(
            2
        ))
    );
}

#[test]
fn decode_rejects_truncated_hard_state() {
    assert_eq!(
        decode_raft_hard_state(&[]),
        Err(DecodeRaftHardStateError::UnexpectedEof {
            needed: 4,
            remaining: 0,
        })
    );
}

#[test]
fn decode_rejects_trailing_bytes_after_checksum_passes() {
    let mut encoded = encode_raft_hard_state(&hard_state());
    encoded.insert(encoded.len() - 4, 0);
    replace_checksum(&mut encoded);

    assert_eq!(
        decode_raft_hard_state(&encoded),
        Err(DecodeRaftHardStateError::TrailingBytes(1))
    );
}
