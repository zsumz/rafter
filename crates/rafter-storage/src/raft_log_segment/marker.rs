//! Admit a compacted-prefix boundary before permitting log-tail repair.
use super::{compaction_marker_path, OpenRaftLogSegmentError};
use crate::{
    format::{
        read_checksummed_prefix,
        v1::log_compaction::{decode_compaction_body, COMPACTION_BODY_LEN},
        StreamChecksumError,
    },
    raft_log_compaction::DecodeRaftLogCompactionMarkerError,
};
use rafter::LogIndex;
use std::{
    fs::File,
    io::{self, Read},
    path::Path,
};

pub(super) fn read_compaction_marker(path: &Path) -> Result<LogIndex, OpenRaftLogSegmentError> {
    let marker_path = compaction_marker_path(path);
    let file = match File::open(&marker_path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(LogIndex::ZERO),
        Err(error) => {
            return Err(OpenRaftLogSegmentError::Io {
                operation: "open raft log compaction marker",
                path: marker_path,
                source: error.into(),
            });
        }
    };
    read_marker(file).map_err(|error| match error {
        ReadMarkerError::Io(error) => OpenRaftLogSegmentError::Io {
            operation: "read raft log compaction marker",
            path: marker_path,
            source: error.into(),
        },
        ReadMarkerError::Decode(error) => OpenRaftLogSegmentError::CompactionMarker(error),
    })
}

#[derive(Debug)]
enum ReadMarkerError {
    Io(io::Error),
    Decode(DecodeRaftLogCompactionMarkerError),
}

fn read_marker(reader: impl Read) -> Result<LogIndex, ReadMarkerError> {
    let envelope =
        read_checksummed_prefix(reader, COMPACTION_BODY_LEN).map_err(|error| match error {
            StreamChecksumError::Io(error) => ReadMarkerError::Io(error),
            StreamChecksumError::Checksum(error) => ReadMarkerError::Decode(error.into()),
        })?;
    let index = decode_compaction_body(&envelope.prefix).map_err(ReadMarkerError::Decode)?;
    if envelope.body_len > COMPACTION_BODY_LEN as u64 {
        return Err(ReadMarkerError::Decode(
            DecodeRaftLogCompactionMarkerError::TrailingBytes(
                usize::try_from(envelope.body_len - COMPACTION_BODY_LEN as u64)
                    .unwrap_or(usize::MAX),
            ),
        ));
    }
    Ok(index)
}

#[cfg(test)]
#[path = "marker_test.rs"]
mod tests;
