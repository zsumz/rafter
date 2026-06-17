use std::{error::Error, fmt};

use rafter::{CommittedConfiguration, ConfigurationId, LogIndex, NodeId, Term};

use crate::checksum::crc32;

/// Magic prefix for the Raft hard-state envelope.
pub const RAFT_HARD_STATE_MAGIC: [u8; 4] = *b"RFHS";
/// Current Raft hard-state envelope version.
pub const RAFT_HARD_STATE_VERSION: u8 = 1;

/// Durable Raft hard state persisted outside the log.
///
/// It records the current term, durable vote, durable commit floor, and the
/// committed configuration identity used to validate recovery against the log.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RaftHardState {
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
    pub commit_index: LogIndex,
    pub committed_configuration: Option<CommittedConfiguration>,
}

/// Errors returned while decoding a hard-state envelope.
///
/// This enum is exhaustive because the envelope format is closed over these
/// corruption and format failures.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DecodeRaftHardStateError {
    UnexpectedEof { needed: usize, remaining: usize },
    InvalidMagic([u8; 4]),
    UnsupportedVersion(u8),
    InvalidVotedForFlag(u8),
    InvalidCommittedConfigurationFlag(u8),
    ChecksumMismatch { expected: u32, actual: u32 },
    TrailingBytes(usize),
}

impl fmt::Display for DecodeRaftHardStateError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnexpectedEof { needed, remaining } => write!(
                formatter,
                "Raft hard-state envelope needs {needed} bytes but only {remaining} remain"
            ),
            Self::InvalidMagic(magic) => write!(
                formatter,
                "Raft hard-state envelope magic {magic:02x?} is not RFHS"
            ),
            Self::UnsupportedVersion(version) => write!(
                formatter,
                "Raft hard-state envelope version {version} is not supported"
            ),
            Self::InvalidVotedForFlag(flag) => write!(
                formatter,
                "Raft hard-state voted-for flag {flag} is not 0 or 1"
            ),
            Self::InvalidCommittedConfigurationFlag(flag) => write!(
                formatter,
                "Raft hard-state committed-configuration flag {flag} is not 0 or 1"
            ),
            Self::ChecksumMismatch { expected, actual } => write!(
                formatter,
                "Raft hard-state envelope stored checksum {expected:#010x} does not match computed checksum {actual:#010x}"
            ),
            Self::TrailingBytes(remaining) => write!(
                formatter,
                "Raft hard-state envelope has {remaining} trailing bytes"
            ),
        }
    }
}

impl Error for DecodeRaftHardStateError {}

/// Encodes Raft hard state into a versioned, checksummed envelope.
///
/// Layout:
///
/// ```text
/// magic[4] | version[1] | current_term[u64] | voted_for_flag[u8] |
/// voted_for_node[u64] | commit_index[u64] |
/// committed_configuration_flag[u8] |
/// committed_configuration_index[u64] | committed_configuration_id[u64] |
/// crc32[u32]
/// ```
///
/// The checksum covers every byte before the checksum field. When
/// `voted_for_flag` is `0`, `voted_for_node` is encoded as `0` and ignored on
/// decode.
#[must_use]
pub fn encode_raft_hard_state(state: &RaftHardState) -> Vec<u8> {
    let mut writer = Writer::new();
    writer.bytes(&RAFT_HARD_STATE_MAGIC);
    writer.u8(RAFT_HARD_STATE_VERSION);
    writer.u64(state.current_term.0);
    if let Some(node_id) = state.voted_for {
        writer.u8(1);
        writer.u64(node_id.0);
    } else {
        writer.u8(0);
        writer.u64(0);
    }
    writer.u64(state.commit_index.0);
    if let Some(committed_configuration) = state.committed_configuration {
        writer.u8(1);
        writer.u64(committed_configuration.index.0);
        writer.u64(committed_configuration.config_id.0);
    } else {
        writer.u8(0);
        writer.u64(0);
        writer.u64(0);
    }

    let checksum = crc32(writer.as_slice());
    writer.u32(checksum);
    writer.finish()
}

/// Decodes and verifies one Raft hard-state envelope.
///
/// # Errors
///
/// Returns [`DecodeRaftHardStateError`] when the envelope is malformed, uses an
/// unsupported version, has trailing bytes, or fails checksum verification.
pub fn decode_raft_hard_state(envelope: &[u8]) -> Result<RaftHardState, DecodeRaftHardStateError> {
    let without_checksum_len =
        envelope
            .len()
            .checked_sub(4)
            .ok_or(DecodeRaftHardStateError::UnexpectedEof {
                needed: 4,
                remaining: envelope.len(),
            })?;
    let expected_checksum = {
        let checksum_bytes = &envelope[without_checksum_len..];
        u32::from_be_bytes([
            checksum_bytes[0],
            checksum_bytes[1],
            checksum_bytes[2],
            checksum_bytes[3],
        ])
    };
    let actual_checksum = crc32(&envelope[..without_checksum_len]);
    if expected_checksum != actual_checksum {
        return Err(DecodeRaftHardStateError::ChecksumMismatch {
            expected: expected_checksum,
            actual: actual_checksum,
        });
    }

    let mut reader = Reader::new(&envelope[..without_checksum_len]);
    let magic = reader.magic()?;
    if magic != RAFT_HARD_STATE_MAGIC {
        return Err(DecodeRaftHardStateError::InvalidMagic(magic));
    }

    let version = reader.u8()?;
    if version != RAFT_HARD_STATE_VERSION {
        return Err(DecodeRaftHardStateError::UnsupportedVersion(version));
    }

    let current_term = Term(reader.u64()?);
    let voted_for = match reader.u8()? {
        0 => {
            let _ignored = reader.u64()?;
            None
        }
        1 => Some(NodeId(reader.u64()?)),
        flag => return Err(DecodeRaftHardStateError::InvalidVotedForFlag(flag)),
    };
    let commit_index = LogIndex(reader.u64()?);
    let committed_configuration = match reader.u8()? {
        0 => {
            let _ignored_index = reader.u64()?;
            let _ignored_id = reader.u64()?;
            None
        }
        1 => Some(CommittedConfiguration {
            index: LogIndex(reader.u64()?),
            config_id: ConfigurationId(reader.u64()?),
        }),
        flag => {
            return Err(DecodeRaftHardStateError::InvalidCommittedConfigurationFlag(
                flag,
            ));
        }
    };
    reader.finish()?;

    Ok(RaftHardState {
        current_term,
        voted_for,
        commit_index,
        committed_configuration,
    })
}

struct Writer {
    bytes: Vec<u8>,
}

impl Writer {
    fn new() -> Self {
        Self { bytes: Vec::new() }
    }

    fn as_slice(&self) -> &[u8] {
        &self.bytes
    }

    fn finish(self) -> Vec<u8> {
        self.bytes
    }

    fn bytes(&mut self, bytes: &[u8]) {
        self.bytes.extend_from_slice(bytes);
    }

    fn u8(&mut self, value: u8) {
        self.bytes.push(value);
    }

    fn u32(&mut self, value: u32) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    fn u64(&mut self, value: u64) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }
}

struct Reader<'a> {
    envelope: &'a [u8],
    position: usize,
}

impl<'a> Reader<'a> {
    fn new(envelope: &'a [u8]) -> Self {
        Self {
            envelope,
            position: 0,
        }
    }

    fn finish(&self) -> Result<(), DecodeRaftHardStateError> {
        let remaining = self.envelope.len() - self.position;
        if remaining == 0 {
            Ok(())
        } else {
            Err(DecodeRaftHardStateError::TrailingBytes(remaining))
        }
    }

    fn magic(&mut self) -> Result<[u8; 4], DecodeRaftHardStateError> {
        let bytes = self.take(4)?;
        Ok([bytes[0], bytes[1], bytes[2], bytes[3]])
    }

    fn u8(&mut self) -> Result<u8, DecodeRaftHardStateError> {
        Ok(self.take(1)?[0])
    }

    fn u64(&mut self) -> Result<u64, DecodeRaftHardStateError> {
        let bytes = self.take(8)?;
        Ok(u64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], DecodeRaftHardStateError> {
        let remaining = self.envelope.len() - self.position;
        if remaining < len {
            return Err(DecodeRaftHardStateError::UnexpectedEof {
                needed: len,
                remaining,
            });
        }

        let start = self.position;
        self.position += len;
        Ok(&self.envelope[start..self.position])
    }
}
