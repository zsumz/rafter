//! Length-framed retained-log encoding and corruption-aware streaming replay.
//!
//! This module owns the outer u32 frame boundary around RFLE entries. It reads
//! one frame at a time and writes replacement logs one frame at a time; logical
//! continuity, file publication, and repair policy live elsewhere.

use std::io::{self, Read, Write};

use crate::{
    decode_raft_log_entry, encode_borrowed_raft_log_entry, encode_raft_log_entry,
    BorrowedPersistedRaftLogEntry, EncodeRaftLogEntryError, PersistedRaftLogEntry,
};

use super::RaftLogReplayError;

const RAFT_LOG_FRAME_HEADER_LEN: usize = 4;
const RAFT_LOG_FRAME_HEADER_LEN_U64: u64 = 4;
const STREAM_READ_CHUNK_LEN: usize = 64 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ReplayedRaftLogFrame {
    pub offset: u64,
    pub entry: PersistedRaftLogEntry,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct RaftLogFrameScan {
    pub frames: Vec<ReplayedRaftLogFrame>,
    pub replay_error: Option<(u64, RaftLogReplayError)>,
}

#[derive(Debug)]
pub(super) enum RaftLogFrameReadError {
    Io(io::Error),
    Replay {
        offset: u64,
        source: RaftLogReplayError,
    },
}

#[derive(Debug)]
pub(super) enum WriteRaftLogFramesError {
    Encode(EncodeRaftLogEntryError),
    Io(io::Error),
}

struct RaftLogFrameReader<R> {
    reader: R,
    offset: u64,
    remaining: u64,
    finished: bool,
}

impl<R: Read> RaftLogFrameReader<R> {
    fn new(reader: R, file_len: u64) -> Self {
        Self {
            reader,
            offset: 0,
            remaining: file_len,
            finished: false,
        }
    }

    fn next_frame(&mut self) -> Result<Option<ReplayedRaftLogFrame>, RaftLogFrameReadError> {
        if self.finished || self.remaining == 0 {
            self.finished = true;
            return Ok(None);
        }

        let frame_offset = self.offset;
        if self.remaining < RAFT_LOG_FRAME_HEADER_LEN_U64 {
            return Err(self.replay_error(
                frame_offset,
                RaftLogReplayError::PartialFrameHeader {
                    offset: diagnostic_usize(frame_offset),
                    remaining: diagnostic_usize(self.remaining),
                },
            ));
        }

        let mut header = [0u8; RAFT_LOG_FRAME_HEADER_LEN];
        let header_bytes =
            read_to_fill(&mut self.reader, &mut header).map_err(RaftLogFrameReadError::Io)?;
        if header_bytes != header.len() {
            return Err(self.replay_error(
                frame_offset,
                RaftLogReplayError::PartialFrameHeader {
                    offset: diagnostic_usize(frame_offset),
                    remaining: header_bytes,
                },
            ));
        }
        self.offset += RAFT_LOG_FRAME_HEADER_LEN_U64;
        self.remaining -= RAFT_LOG_FRAME_HEADER_LEN_U64;

        let encoded_len_u64 = u64::from(u32::from_be_bytes(header));
        if encoded_len_u64 > self.remaining {
            return Err(self.replay_error(
                frame_offset,
                RaftLogReplayError::PartialEntry {
                    offset: diagnostic_usize(frame_offset),
                    expected: diagnostic_usize(encoded_len_u64),
                    remaining: diagnostic_usize(self.remaining),
                },
            ));
        }
        let Ok(encoded_len) = usize::try_from(encoded_len_u64) else {
            return Err(self.replay_error(
                frame_offset,
                RaftLogReplayError::PartialEntry {
                    offset: diagnostic_usize(frame_offset),
                    expected: usize::MAX,
                    remaining: diagnostic_usize(self.remaining),
                },
            ));
        };

        let encoded =
            read_frame_bytes(&mut self.reader, encoded_len).map_err(RaftLogFrameReadError::Io)?;
        if encoded.len() != encoded_len {
            return Err(self.replay_error(
                frame_offset,
                RaftLogReplayError::PartialEntry {
                    offset: diagnostic_usize(frame_offset),
                    expected: encoded_len,
                    remaining: encoded.len(),
                },
            ));
        }

        let entry = decode_raft_log_entry(&encoded).map_err(|source| {
            self.replay_error(
                frame_offset,
                RaftLogReplayError::CorruptEntry {
                    offset: diagnostic_usize(frame_offset),
                    source,
                },
            )
        })?;
        self.offset += encoded_len_u64;
        self.remaining -= encoded_len_u64;

        Ok(Some(ReplayedRaftLogFrame {
            offset: frame_offset,
            entry,
        }))
    }

    fn replay_error(&mut self, offset: u64, source: RaftLogReplayError) -> RaftLogFrameReadError {
        self.finished = true;
        RaftLogFrameReadError::Replay { offset, source }
    }
}

pub(super) fn read_raft_log_frames(
    reader: impl Read,
    file_len: u64,
) -> Result<RaftLogFrameScan, io::Error> {
    let mut reader = RaftLogFrameReader::new(reader, file_len);
    let mut frames = Vec::new();
    loop {
        match reader.next_frame() {
            Ok(Some(frame)) => frames.push(frame),
            Ok(None) => {
                return Ok(RaftLogFrameScan {
                    frames,
                    replay_error: None,
                });
            }
            Err(RaftLogFrameReadError::Io(error)) => return Err(error),
            Err(RaftLogFrameReadError::Replay { offset, source }) => {
                return Ok(RaftLogFrameScan {
                    frames,
                    replay_error: Some((offset, source)),
                });
            }
        }
    }
}

#[cfg(test)]
pub(super) fn append_raft_log_frame(
    output: &mut Vec<u8>,
    entry: &PersistedRaftLogEntry,
) -> Result<(), EncodeRaftLogEntryError> {
    let encoded = encode_raft_log_entry(entry)?;
    append_encoded_frame(output, &encoded)
}

pub(super) fn append_borrowed_raft_log_frame(
    output: &mut Vec<u8>,
    entry: BorrowedPersistedRaftLogEntry<'_>,
) -> Result<(), EncodeRaftLogEntryError> {
    let encoded = encode_borrowed_raft_log_entry(entry)?;
    append_encoded_frame(output, &encoded)
}

fn append_encoded_frame(
    output: &mut Vec<u8>,
    encoded: &[u8],
) -> Result<(), EncodeRaftLogEntryError> {
    let len = encoded_frame_len(encoded)?;
    output.extend_from_slice(&len.to_be_bytes());
    output.extend_from_slice(encoded);
    Ok(())
}

#[cfg(test)]
pub(super) fn append_raft_log_frames(
    output: &mut Vec<u8>,
    entries: &[PersistedRaftLogEntry],
) -> Result<(), EncodeRaftLogEntryError> {
    for entry in entries {
        append_raft_log_frame(output, entry)?;
    }
    Ok(())
}

pub(super) fn write_raft_log_frames(
    output: &mut impl Write,
    entries: &[PersistedRaftLogEntry],
) -> Result<(), WriteRaftLogFramesError> {
    for entry in entries {
        let encoded = encode_raft_log_entry(entry).map_err(WriteRaftLogFramesError::Encode)?;
        let len = encoded_frame_len(&encoded).map_err(WriteRaftLogFramesError::Encode)?;
        output
            .write_all(&len.to_be_bytes())
            .and_then(|()| output.write_all(&encoded))
            .map_err(WriteRaftLogFramesError::Io)?;
    }
    Ok(())
}

fn encoded_frame_len(encoded: &[u8]) -> Result<u32, EncodeRaftLogEntryError> {
    u32::try_from(encoded.len())
        .map_err(|_| EncodeRaftLogEntryError::PayloadTooLarge { len: encoded.len() })
}

fn read_frame_bytes(reader: &mut impl Read, len: usize) -> io::Result<Vec<u8>> {
    let mut encoded = vec![0; len];
    let mut filled = 0;
    while filled < len {
        let chunk_end = filled.saturating_add(STREAM_READ_CHUNK_LEN).min(len);
        let read = read_to_fill(reader, &mut encoded[filled..chunk_end])?;
        filled += read;
        if filled < chunk_end {
            encoded.truncate(filled);
            break;
        }
    }
    Ok(encoded)
}

fn read_to_fill(reader: &mut impl Read, output: &mut [u8]) -> io::Result<usize> {
    let mut filled = 0;
    while filled < output.len() {
        let read = read_retrying_interrupts(reader, &mut output[filled..])?;
        if read == 0 {
            break;
        }
        filled += read;
    }
    Ok(filled)
}

fn read_retrying_interrupts(reader: &mut impl Read, output: &mut [u8]) -> io::Result<usize> {
    loop {
        match reader.read(output) {
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            result => return result,
        }
    }
}

fn diagnostic_usize(value: u64) -> usize {
    usize::try_from(value).unwrap_or(usize::MAX)
}

#[cfg(test)]
#[path = "frames_test.rs"]
mod tests;
