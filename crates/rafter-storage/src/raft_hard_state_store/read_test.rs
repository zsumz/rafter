//! Streaming hard-state admission preserves the slice decoder's corruption verdicts.
use super::*;
use crate::{crc32, decode_raft_hard_state, encode_raft_hard_state};
use rafter::{CommittedConfiguration, ConfigurationId, LogIndex, NodeId, Term};
use std::io::Cursor;

fn with_signed_tail(tail: usize) -> Vec<u8> {
    let mut bytes = encode_raft_hard_state(&RaftHardState::default());
    bytes.truncate(bytes.len() - 4);
    bytes.resize(bytes.len() + tail, 0);
    let checksum = crc32(&bytes);
    bytes.extend_from_slice(&checksum.to_be_bytes());
    bytes
}

#[test]
fn oversized_hard_state_is_classified_without_unbounded_read_requests() {
    let bytes = with_signed_tail(256 * 1024);
    let mut reader = BudgetReader(Cursor::new(bytes));
    let error = read_hard_state(&mut reader).unwrap_err();
    assert!(matches!(
        error,
        ReadHardStateError::Decode(DecodeRaftHardStateError::TrailingBytes(262_144))
    ));
    assert_eq!(reader.0.position(), reader.0.get_ref().len() as u64);
}

struct BudgetReader(Cursor<Vec<u8>>);
impl Read for BudgetReader {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        assert!(output.len() <= 4096, "unbounded hard-state read request");
        self.0.read(output)
    }
}

fn assert_matches_slice(bytes: &[u8]) {
    let expected = decode_raft_hard_state(bytes);
    for chunk in [1, 3, 4, 7, 4096] {
        let reader = ChunkedReader {
            inner: Cursor::new(bytes),
            chunk,
            interrupt: false,
        };
        let actual = match read_hard_state(reader) {
            Ok(state) => Ok(state),
            Err(ReadHardStateError::Decode(error)) => Err(error),
            Err(ReadHardStateError::Io(error)) => panic!("unexpected I/O: {error}"),
        };
        assert_eq!(actual, expected, "length {}, chunk {chunk}", bytes.len());
    }
}

#[test]
fn short_interrupted_and_corrupt_hard_state_matches_the_slice_decoder() {
    for state in [
        RaftHardState::default(),
        RaftHardState {
            current_term: Term(17),
            voted_for: Some(NodeId(9)),
            commit_index: LogIndex(41),
            committed_configuration: Some(CommittedConfiguration {
                index: LogIndex(31),
                config_id: ConfigurationId(7),
            }),
        },
    ] {
        let valid = encode_raft_hard_state(&state);
        for end in 0..=valid.len() {
            assert_matches_slice(&valid[..end]);
        }
        for end in 0..valid.len() - 4 {
            let mut short_body = valid[..end].to_vec();
            let checksum = crc32(&short_body);
            short_body.extend_from_slice(&checksum.to_be_bytes());
            assert_matches_slice(&short_body);
        }
        for offset in 0..valid.len() {
            let mut corrupt = valid.clone();
            corrupt[offset] ^= 0x80;
            assert_matches_slice(&corrupt);
            // Re-sign to exercise field/schema errors after checksum admission.
            resign(&mut corrupt);
            assert_matches_slice(&corrupt);
        }
    }
}

#[test]
fn oversized_hard_state_preserves_checksum_schema_and_trailing_error_precedence() {
    for tail in [1, 2, 3, 4095, 4096, 4097] {
        let bytes = with_signed_tail(tail);
        assert_matches_slice(&bytes);
        for offset in [0, 4, 13, 14, 30, 31, 39] {
            let mut invalid = bytes.clone();
            invalid[offset] = 0xff;
            assert_matches_slice(&invalid); // Bad checksum must win over schema.
            resign(&mut invalid);
            assert_matches_slice(&invalid); // Schema must win over unused suffix.
        }
    }
}

#[test]
fn hard_state_stream_preserves_io_failures_even_after_a_complete_envelope() {
    let bytes = encode_raft_hard_state(&RaftHardState::default());
    for cut in 0..=bytes.len() {
        let reader = (&bytes[..cut]).chain(FailingRead);
        let error = read_hard_state(reader).unwrap_err();
        let ReadHardStateError::Io(error) = error else {
            panic!("I/O failure was hidden at {cut}: {error:?}");
        };
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
        assert_eq!(error.to_string(), "injected hard-state read failure");
    }
}

#[test]
fn oversized_hard_state_file_is_refused_without_mutation() {
    use crate::raft_hard_state_store::test_support::{remove_test_file, test_store_path};
    use crate::{FileRaftHardStateStore, OpenRaftHardStateStoreError};
    use std::fs;

    let path = test_store_path("streaming-oversized");
    let bytes = with_signed_tail(8 * 1024 * 1024);
    fs::write(&path, &bytes).unwrap();
    for _ in 0..2 {
        let error = FileRaftHardStateStore::open(&path).unwrap_err();
        assert!(matches!(
            error,
            OpenRaftHardStateStoreError::Decode(DecodeRaftHardStateError::TrailingBytes(8_388_608))
        ));
        assert_eq!(fs::read(&path).unwrap(), bytes);
    }
    remove_test_file(path);
}

fn resign(bytes: &mut [u8]) {
    let body_len = bytes.len() - 4;
    let crc = crc32(&bytes[..body_len]);
    bytes[body_len..].copy_from_slice(&crc.to_be_bytes());
}

struct ChunkedReader<'a> {
    inner: Cursor<&'a [u8]>,
    chunk: usize,
    interrupt: bool,
}
impl Read for ChunkedReader<'_> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        self.interrupt = !self.interrupt;
        if self.interrupt {
            return Err(io::ErrorKind::Interrupted.into());
        }
        let size = output.len().min(self.chunk);
        self.inner.read(&mut output[..size])
    }
}

struct FailingRead;
impl Read for FailingRead {
    fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
        Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "injected hard-state read failure",
        ))
    }
}
