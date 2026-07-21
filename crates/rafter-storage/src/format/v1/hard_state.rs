//! Version-1 hard-state envelope grammar and canonical decoding.
//!
//! This module owns RFHS framing, fields, reserved-zero rules, and checksum
//! mapping. File publication and recovery live in the hard-state store.

use std::{error::Error, fmt};

use rafter::{CommittedConfiguration, ConfigurationId, LogIndex, NodeId, Term};

use crate::format::{
    finish_checksummed, verify_checksum, ChecksumError, CursorError, Reader, Writer,
};

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
    UnexpectedEof {
        needed: usize,
        remaining: usize,
    },
    InvalidMagic([u8; 4]),
    UnsupportedVersion(u8),
    InvalidVotedForFlag(u8),
    /// An absent vote carried a non-zero reserved node-id field.
    NonCanonicalAbsentVotedFor(NodeId),
    InvalidCommittedConfigurationFlag(u8),
    /// An absent committed configuration carried non-zero reserved fields.
    NonCanonicalAbsentCommittedConfiguration {
        index: LogIndex,
        config_id: ConfigurationId,
    },
    ChecksumMismatch {
        expected: u32,
        actual: u32,
    },
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
            Self::NonCanonicalAbsentVotedFor(node_id) => write!(
                formatter,
                "Raft hard-state absent voted-for field must encode node id 0, found {}",
                node_id.0
            ),
            Self::InvalidCommittedConfigurationFlag(flag) => write!(
                formatter,
                "Raft hard-state committed-configuration flag {flag} is not 0 or 1"
            ),
            Self::NonCanonicalAbsentCommittedConfiguration { index, config_id } => write!(
                formatter,
                "Raft hard-state absent committed configuration must encode index and id 0, found index {} and id {}",
                index.0,
                config_id.0
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

impl From<CursorError> for DecodeRaftHardStateError {
    fn from(error: CursorError) -> Self {
        match error {
            CursorError::UnexpectedEof { needed, remaining } => {
                Self::UnexpectedEof { needed, remaining }
            }
            CursorError::TrailingBytes(remaining) => Self::TrailingBytes(remaining),
        }
    }
}

impl From<ChecksumError> for DecodeRaftHardStateError {
    fn from(error: ChecksumError) -> Self {
        match error {
            ChecksumError::UnexpectedEof { needed, remaining } => {
                Self::UnexpectedEof { needed, remaining }
            }
            ChecksumError::Mismatch { expected, actual } => {
                Self::ChecksumMismatch { expected, actual }
            }
        }
    }
}

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
/// The checksum covers every byte before the checksum field. When a presence
/// flag is `0`, every fixed-width field belonging to the absent value is encoded
/// as zero. The decoder rejects non-zero absent fields as noncanonical v1 bytes.
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

    finish_checksummed(writer)
}

/// Decodes and verifies one Raft hard-state envelope.
///
/// # Errors
///
/// Returns [`DecodeRaftHardStateError`] when the envelope is malformed, uses an
/// unsupported version, has trailing bytes, or fails checksum verification.
pub fn decode_raft_hard_state(envelope: &[u8]) -> Result<RaftHardState, DecodeRaftHardStateError> {
    let body = verify_checksum(envelope)?;
    let mut reader = Reader::new(body);
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
            let node_id = NodeId(reader.u64()?);
            if node_id.0 != 0 {
                return Err(DecodeRaftHardStateError::NonCanonicalAbsentVotedFor(
                    node_id,
                ));
            }
            None
        }
        1 => Some(NodeId(reader.u64()?)),
        flag => return Err(DecodeRaftHardStateError::InvalidVotedForFlag(flag)),
    };
    let commit_index = LogIndex(reader.u64()?);
    let committed_configuration = match reader.u8()? {
        0 => {
            let index = LogIndex(reader.u64()?);
            let config_id = ConfigurationId(reader.u64()?);
            if index != LogIndex::ZERO || config_id.0 != 0 {
                return Err(
                    DecodeRaftHardStateError::NonCanonicalAbsentCommittedConfiguration {
                        index,
                        config_id,
                    },
                );
            }
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
