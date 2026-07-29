//! Snapshot boundary metadata and payload descriptor vocabulary.

use std::fmt;

use super::super::{CommittedConfiguration, LogIndex, MembershipConfig, NodeId, Term};
use super::{
    snapshot_transfer_id_from_parts, ApplicationSnapshotKind, SnapshotGroupId,
    SnapshotMetadataError, SnapshotTransferId,
};

/// Application-defined snapshot format version.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ApplicationSnapshotVersion(u16);

impl ApplicationSnapshotVersion {
    /// Constructs a non-zero application snapshot version.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotMetadataError::ZeroApplicationSnapshotVersion`] when
    /// `value` is zero.
    pub fn new(value: u16) -> Result<Self, SnapshotMetadataError> {
        if value == 0 {
            Err(SnapshotMetadataError::ZeroApplicationSnapshotVersion)
        } else {
            Ok(Self(value))
        }
    }

    /// Returns the numeric snapshot format version.
    #[must_use]
    pub fn get(self) -> u16 {
        self.0
    }
}

impl fmt::Display for ApplicationSnapshotVersion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

/// Application-owned snapshot identity embedded in Raft snapshot metadata.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ApplicationSnapshotMetadata {
    /// Application-defined snapshot kind. Applications should use this to
    /// route decoding to the correct state-machine snapshot format, not as an
    /// integrity mechanism.
    pub kind: ApplicationSnapshotKind,
    /// Application-defined, non-zero snapshot format version.
    pub version: ApplicationSnapshotVersion,
}

impl ApplicationSnapshotMetadata {
    /// Builds application snapshot metadata.
    #[must_use]
    pub fn new(kind: ApplicationSnapshotKind, version: ApplicationSnapshotVersion) -> Self {
        Self { kind, version }
    }
}

/// Raft-owned snapshot metadata that defines the compacted log boundary.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct RaftSnapshotMetadata {
    /// Logical Raft group whose state is captured.
    pub group_id: SnapshotGroupId,
    /// Replica that authored the snapshot.
    pub writer_id: NodeId,
    /// Greatest log index covered by the snapshot.
    pub last_included_index: LogIndex,
    /// Term stored at `last_included_index`.
    pub last_included_term: Term,
    /// Greatest term visible to the writer's durable hard state.
    pub hard_state_term: Term,
    /// Application snapshot format identity and version.
    pub application: ApplicationSnapshotMetadata,
    /// Committed membership state captured at the boundary, when known.
    pub committed_configuration: Option<SnapshotCommittedConfiguration>,
}

/// Committed configuration state captured in a snapshot.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct SnapshotCommittedConfiguration {
    /// Committed configuration identity, when the log supplied one.
    pub configuration: Option<CommittedConfiguration>,
    /// Effective committed membership at the snapshot boundary.
    pub membership: MembershipConfig,
}

impl SnapshotCommittedConfiguration {
    /// Builds committed configuration metadata for a snapshot.
    #[must_use]
    pub const fn new(
        configuration: Option<CommittedConfiguration>,
        membership: MembershipConfig,
    ) -> Self {
        Self {
            configuration,
            membership,
        }
    }
}

impl RaftSnapshotMetadata {
    /// Constructs validated Raft-owned snapshot metadata.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotMetadataError`] when the snapshot boundary does not
    /// describe a non-empty committed prefix that could have existed in the
    /// visible hard-state term.
    pub fn new(
        group_id: SnapshotGroupId,
        writer_id: NodeId,
        last_included_index: LogIndex,
        last_included_term: Term,
        hard_state_term: Term,
        application: ApplicationSnapshotMetadata,
    ) -> Result<Self, SnapshotMetadataError> {
        if last_included_index == LogIndex::ZERO {
            return Err(SnapshotMetadataError::ZeroLastIncludedIndex);
        }
        if last_included_index.0 == u64::MAX {
            return Err(SnapshotMetadataError::LastIncludedIndexAtMaximum);
        }
        if last_included_term.is_zero() {
            return Err(SnapshotMetadataError::ZeroLastIncludedTerm {
                last_included_index,
            });
        }
        if last_included_term > hard_state_term {
            return Err(SnapshotMetadataError::SnapshotTermAheadOfHardState {
                last_included_index,
                last_included_term,
                hard_state_term,
            });
        }

        Ok(Self {
            group_id,
            writer_id,
            last_included_index,
            last_included_term,
            hard_state_term,
            application,
            committed_configuration: None,
        })
    }

    /// Attaches committed membership without a configuration identity.
    #[must_use]
    pub fn with_committed_membership(mut self, membership: MembershipConfig) -> Self {
        self.committed_configuration = Some(SnapshotCommittedConfiguration::new(None, membership));
        self
    }

    /// Attaches committed membership and optional committed configuration id.
    #[must_use]
    pub fn with_committed_configuration(
        mut self,
        configuration: SnapshotCommittedConfiguration,
    ) -> Self {
        self.committed_configuration = Some(configuration);
        self
    }

    /// Returns the committed membership captured by this snapshot.
    #[must_use]
    pub fn committed_membership(&self) -> Option<&MembershipConfig> {
        self.committed_configuration
            .as_ref()
            .map(|state| &state.membership)
    }

    /// Returns the committed configuration identity captured by this snapshot.
    #[must_use]
    pub fn committed_configuration_state(&self) -> Option<CommittedConfiguration> {
        self.committed_configuration
            .as_ref()
            .and_then(|state| state.configuration)
    }
}

/// The kernel's view of a snapshot: Raft-owned metadata plus the payload
/// length, never the payload itself.
///
/// Application snapshot content stays in the application's snapshot store.
/// A leader streams it by emitting
/// [`Output::SendSnapshotChunk`](crate::Output::SendSnapshotChunk)
/// directives that the transport resolves through a [`super::SnapshotChunkSource`];
/// a receiver hands each validated chunk to its store through
/// [`Output::StageSnapshotChunk`](crate::Output::StageSnapshotChunk). Kernel
/// memory therefore stays bounded regardless of payload size.
///
/// The `application_payload_crc32` field detects accidental payload
/// corruption and mismatched transfers in a non-Byzantine system. It is not a
/// cryptographic digest; applications that require adversarial integrity
/// should include a stronger digest in their own snapshot format and verify it
/// before making the snapshot visible.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct RaftSnapshot {
    /// Raft-visible snapshot descriptor.
    pub metadata: RaftSnapshotMetadata,
    /// Complete opaque application payload length.
    pub application_payload_len: u64,
    /// CRC32 of the complete opaque application payload.
    pub application_payload_crc32: u32,
}

impl RaftSnapshot {
    /// Constructs a snapshot descriptor from already-computed payload metadata.
    ///
    /// `application_payload_crc32` is a corruption check over the opaque
    /// application payload, not an adversarial integrity proof.
    #[must_use]
    pub fn new(
        metadata: RaftSnapshotMetadata,
        application_payload_len: u64,
        application_payload_crc32: u32,
    ) -> Self {
        Self {
            metadata,
            application_payload_len,
            application_payload_crc32,
        }
    }

    /// Constructs a snapshot descriptor and computes its payload CRC32.
    ///
    /// The computed CRC32 is for accidental corruption detection only.
    #[must_use]
    pub fn from_payload(metadata: RaftSnapshotMetadata, application_payload: &[u8]) -> Self {
        Self::new(
            metadata,
            application_payload.len() as u64,
            application_payload_crc32(application_payload),
        )
    }

    /// The deterministic identity of this snapshot's transfer, derived from
    /// the metadata, payload length, and application payload checksum. Every
    /// chunk of a transfer carries it, and stores key staged and served
    /// payloads by it.
    ///
    /// The value is a routing identity, not a cryptographic digest.
    #[must_use]
    pub fn transfer_id(&self) -> SnapshotTransferId {
        snapshot_transfer_id_from_parts(
            &self.metadata,
            self.application_payload_len,
            self.application_payload_crc32,
        )
    }
}

#[must_use]
pub(crate) fn application_payload_crc32(bytes: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFFu32;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            if crc & 1 == 1 {
                crc = (crc >> 1) ^ 0xEDB8_8320;
            } else {
                crc >>= 1;
            }
        }
    }
    !crc
}
