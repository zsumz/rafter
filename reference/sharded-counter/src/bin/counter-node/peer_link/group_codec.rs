//! Canonical fixed-width route for group identity plus incarnation.

use std::{error::Error, fmt};

use rafter_reference_sharded_counter::{GroupId, GroupIncarnation};
use rafter_transport_tls::GroupIdCodec;

/// Transport identity for one live counter group incarnation.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct PeerGroupId {
    group_id: GroupId,
    incarnation: GroupIncarnation,
}

impl PeerGroupId {
    pub(super) const fn new(group_id: GroupId, incarnation: GroupIncarnation) -> Self {
        Self {
            group_id,
            incarnation,
        }
    }

    pub(super) const fn group_id(self) -> GroupId {
        self.group_id
    }

    pub(super) const fn incarnation(self) -> GroupIncarnation {
        self.incarnation
    }
}

/// Canonical big-endian `(group, incarnation)` codec.
#[derive(Clone, Copy, Debug, Default)]
pub(super) struct PeerGroupCodec;

/// Invalid encoded counter transport group identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum PeerGroupCodecError {
    InvalidLength { actual: usize },
    ZeroIncarnation,
}

impl fmt::Display for PeerGroupCodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidLength { actual } => write!(
                formatter,
                "counter transport group identity must contain exactly eight bytes; got {actual}"
            ),
            Self::ZeroIncarnation => {
                formatter.write_str("counter transport group incarnation must be nonzero")
            }
        }
    }
}

impl Error for PeerGroupCodecError {}

impl GroupIdCodec<PeerGroupId> for PeerGroupCodec {
    type Error = PeerGroupCodecError;

    fn max_encoded_len(&self) -> usize {
        8
    }

    fn max_decoded_heap_bytes(&self) -> usize {
        0
    }

    fn encode(&self, group_id: &PeerGroupId, output: &mut Vec<u8>) -> Result<(), Self::Error> {
        output.clear();
        output.extend_from_slice(&group_id.group_id.get().to_be_bytes());
        output.extend_from_slice(&group_id.incarnation.get().to_be_bytes());
        Ok(())
    }

    fn decode(&self, input: &[u8]) -> Result<PeerGroupId, Self::Error> {
        let encoded: [u8; 8] =
            input
                .try_into()
                .map_err(|_| PeerGroupCodecError::InvalidLength {
                    actual: input.len(),
                })?;
        let group_id = GroupId::new(u32::from_be_bytes([
            encoded[0], encoded[1], encoded[2], encoded[3],
        ]));
        let incarnation = GroupIncarnation::new(u32::from_be_bytes([
            encoded[4], encoded[5], encoded[6], encoded[7],
        ]))
        .ok_or(PeerGroupCodecError::ZeroIncarnation)?;
        Ok(PeerGroupId::new(group_id, incarnation))
    }
}
