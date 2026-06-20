use crate::{
    decode_raft_log_entry, encode_raft_log_entry, EncodeRaftLogEntryError, PersistedRaftLogEntry,
};

use super::RaftLogReplayError;

const RAFT_LOG_FRAME_HEADER_LEN: usize = 4;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ReplayedRaftLogFrame {
    pub offset: usize,
    pub entry: PersistedRaftLogEntry,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct RaftLogFrameScan {
    pub frames: Vec<ReplayedRaftLogFrame>,
    pub error: Option<RaftLogReplayError>,
}

pub(super) fn append_raft_log_frame(
    output: &mut Vec<u8>,
    entry: &PersistedRaftLogEntry,
) -> Result<(), EncodeRaftLogEntryError> {
    let encoded = encode_raft_log_entry(entry)?;
    let len = u32::try_from(encoded.len())
        .map_err(|_| EncodeRaftLogEntryError::PayloadTooLarge { len: encoded.len() })?;
    output.extend_from_slice(&len.to_be_bytes());
    output.extend_from_slice(&encoded);
    Ok(())
}

pub(super) fn append_raft_log_frames(
    output: &mut Vec<u8>,
    entries: &[PersistedRaftLogEntry],
) -> Result<(), EncodeRaftLogEntryError> {
    for entry in entries {
        append_raft_log_frame(output, entry)?;
    }
    Ok(())
}

pub(super) fn scan_raft_log_frames(input: &[u8]) -> RaftLogFrameScan {
    let mut position = 0;
    let mut frames = Vec::new();

    while position < input.len() {
        let remaining = input.len() - position;
        if remaining < RAFT_LOG_FRAME_HEADER_LEN {
            return RaftLogFrameScan {
                frames,
                error: Some(RaftLogReplayError::PartialFrameHeader {
                    offset: position,
                    remaining,
                }),
            };
        }

        let frame_offset = position;
        let len = u32::from_be_bytes([
            input[position],
            input[position + 1],
            input[position + 2],
            input[position + 3],
        ]) as usize;
        position += RAFT_LOG_FRAME_HEADER_LEN;

        let remaining = input.len() - position;
        if remaining < len {
            return RaftLogFrameScan {
                frames,
                error: Some(RaftLogReplayError::PartialEntry {
                    offset: frame_offset,
                    expected: len,
                    remaining,
                }),
            };
        }

        let entry = match decode_raft_log_entry(&input[position..position + len]) {
            Ok(entry) => entry,
            Err(source) => {
                return RaftLogFrameScan {
                    frames,
                    error: Some(RaftLogReplayError::CorruptEntry {
                        offset: frame_offset,
                        source,
                    }),
                };
            }
        };
        frames.push(ReplayedRaftLogFrame {
            offset: frame_offset,
            entry,
        });
        position += len;
    }
    RaftLogFrameScan {
        frames,
        error: None,
    }
}

impl RaftLogReplayError {
    pub(super) const fn offset(&self) -> usize {
        match self {
            Self::PartialFrameHeader { offset, .. }
            | Self::PartialEntry { offset, .. }
            | Self::CorruptEntry { offset, .. } => *offset,
        }
    }
}
