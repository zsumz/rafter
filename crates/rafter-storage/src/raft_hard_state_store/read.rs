//! Read and classify one hard-state envelope before permitting publication.
use crate::{
    format::{
        read_checksummed_prefix,
        v1::hard_state::{decode_hard_state_body, HARD_STATE_BODY_LEN},
        StreamChecksumError,
    },
    DecodeRaftHardStateError, RaftHardState,
};
use std::io::{self, Read};

#[derive(Debug)]
pub(super) enum ReadHardStateError {
    Io(io::Error),
    Decode(DecodeRaftHardStateError),
}

pub(super) fn read_hard_state(reader: impl Read) -> Result<RaftHardState, ReadHardStateError> {
    let envelope =
        read_checksummed_prefix(reader, HARD_STATE_BODY_LEN).map_err(|error| match error {
            StreamChecksumError::Io(error) => ReadHardStateError::Io(error),
            StreamChecksumError::Checksum(error) => ReadHardStateError::Decode(error.into()),
        })?;
    let state = decode_hard_state_body(&envelope.prefix).map_err(ReadHardStateError::Decode)?;
    if envelope.body_len > HARD_STATE_BODY_LEN as u64 {
        return Err(ReadHardStateError::Decode(
            DecodeRaftHardStateError::TrailingBytes(
                usize::try_from(envelope.body_len - HARD_STATE_BODY_LEN as u64)
                    .unwrap_or(usize::MAX),
            ),
        ));
    }
    Ok(state)
}

#[cfg(test)]
#[path = "read_test.rs"]
mod tests;
