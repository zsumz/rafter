//! Canonical fixed-width route for the fixture's one numeric group identity.

use std::{error::Error, fmt};

use rafter_transport_tls::GroupIdCodec;

use super::super::replica::LockGroupId;

/// Canonical big-endian encoding for [`LockGroupId`].
#[derive(Clone, Copy, Debug, Default)]
pub struct LockGroupCodec;

/// Invalid encoded lock group identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LockGroupCodecError {
    actual: usize,
}

impl fmt::Display for LockGroupCodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "lock group identity must contain exactly eight bytes; got {}",
            self.actual
        )
    }
}

impl Error for LockGroupCodecError {}

impl GroupIdCodec<LockGroupId> for LockGroupCodec {
    type Error = LockGroupCodecError;

    fn max_encoded_len(&self) -> usize {
        u64::BITS as usize / 8
    }

    fn max_decoded_heap_bytes(&self) -> usize {
        0
    }

    fn encode(&self, group_id: &LockGroupId, output: &mut Vec<u8>) -> Result<(), Self::Error> {
        output.clear();
        output.extend_from_slice(&group_id.0.to_be_bytes());
        Ok(())
    }

    fn decode(&self, input: &[u8]) -> Result<LockGroupId, Self::Error> {
        let encoded: [u8; 8] = input.try_into().map_err(|_| LockGroupCodecError {
            actual: input.len(),
        })?;
        Ok(LockGroupId(u64::from_be_bytes(encoded)))
    }
}
