use std::{error::Error, fmt};

use super::{CommittedConfiguration, LogIndex, MembershipConfig, MembershipSet, NodeId, Term};

mod identity;
mod source;
mod transfer_id;

pub use identity::{ApplicationSnapshotKind, SnapshotGroupId, SnapshotIdError};
pub use source::{
    InMemorySnapshotChunkSource, InMemorySnapshotSourceError, SnapshotChunkRequest,
    SnapshotChunkSource,
};
pub(crate) use transfer_id::snapshot_transfer_id_from_parts;

/// Stable identifier for one snapshot transfer.
///
/// Values produced by [`RaftSnapshot::transfer_id`] are deterministic,
/// non-zero 64-bit routing identities derived from Raft snapshot metadata,
/// payload length, and the payload CRC32. They let receivers reject chunks
/// that do not belong to the advertised transfer, but they are not
/// collision-resistant digests or authentication tags.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SnapshotTransferId(pub u64);

impl fmt::Display for SnapshotTransferId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

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
    pub group_id: SnapshotGroupId,
    pub writer_id: NodeId,
    pub last_included_index: LogIndex,
    pub last_included_term: Term,
    pub hard_state_term: Term,
    pub application: ApplicationSnapshotMetadata,
    pub committed_configuration: Option<SnapshotCommittedConfiguration>,
}

/// Committed configuration state captured in a snapshot.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct SnapshotCommittedConfiguration {
    pub configuration: Option<CommittedConfiguration>,
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
/// directives that the transport resolves through a [`SnapshotChunkSource`];
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
    pub metadata: RaftSnapshotMetadata,
    pub application_payload_len: u64,
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

/// A leader-side directive to send one snapshot chunk to a follower.
///
/// Carries everything the wire message needs except the payload bytes; the
/// transport resolves those through a [`SnapshotChunkSource`] with
/// [`SnapshotChunkSend::resolve`]. A directive that cannot be resolved is
/// dropped exactly like a lost message — the transfer resumes from the
/// follower's acknowledged offset.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct SnapshotChunkSend {
    pub term: Term,
    pub leader_id: NodeId,
    pub transfer_id: SnapshotTransferId,
    pub metadata: RaftSnapshotMetadata,
    pub total_payload_len: u64,
    pub application_payload_crc32: u32,
    pub offset: u64,
    pub len: u32,
    pub done: bool,
}

/// A validated inbound snapshot chunk for the receiver's snapshot store.
///
/// Chunks arrive in offset order within a transfer; `offset` is always the
/// staging area's current length for `transfer_id` (a new transfer starts at
/// zero). `done` marks the final chunk: the staged payload is complete and
/// the [`Output::ApplySnapshot`](crate::Output::ApplySnapshot) emitted
/// alongside it refers to the staged content.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct StagedSnapshotChunk {
    pub leader_id: NodeId,
    pub transfer_id: SnapshotTransferId,
    pub metadata: RaftSnapshotMetadata,
    pub total_payload_len: u64,
    pub application_payload_crc32: u32,
    pub offset: u64,
    pub bytes: Vec<u8>,
    pub done: bool,
}

/// Receiver-side progress for a partially staged snapshot transfer.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct PendingSnapshotTransfer {
    pub leader_id: NodeId,
    pub transfer_id: SnapshotTransferId,
    pub metadata: RaftSnapshotMetadata,
    pub total_payload_len: u64,
    pub application_payload_crc32: u32,
    pub received_len: u64,
}

impl PendingSnapshotTransfer {
    /// Returns the number of payload bytes already received.
    #[must_use]
    pub fn received_bytes(&self) -> u64 {
        self.received_len
    }

    /// Returns whether the staged payload length has reached the descriptor.
    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.received_bytes() == self.total_payload_len
    }
}

/// Snapshot transfer observability for one node.
#[derive(Clone, Debug, Default, Eq, Hash, PartialEq)]
pub struct SnapshotTransferStatus {
    pub leader: Vec<LeaderSnapshotTransferStatus>,
    pub follower: Option<FollowerSnapshotTransferStatus>,
    pub rejected_chunks: SnapshotChunkRejectionCounters,
}

impl SnapshotTransferStatus {
    /// Returns whether no leader transfer, follower transfer, or rejection
    /// counter is active.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.leader.is_empty() && self.follower.is_none() && self.rejected_chunks.is_empty()
    }
}

/// Leader-side snapshot transfer progress for one follower.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct LeaderSnapshotTransferStatus {
    pub follower_id: NodeId,
    pub transfer_id: SnapshotTransferId,
    pub last_included_index: LogIndex,
    pub total_bytes: u64,
    pub next_offset: u64,
}

/// Follower-side snapshot transfer progress from one leader.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct FollowerSnapshotTransferStatus {
    pub leader_id: NodeId,
    pub transfer_id: SnapshotTransferId,
    pub last_included_index: LogIndex,
    pub total_bytes: u64,
    pub received_bytes: u64,
}

/// Counters for rejected inbound snapshot chunks.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct SnapshotChunkRejectionCounters {
    pub stale_term: u64,
    pub wrong_transfer: u64,
    pub metadata_mismatch: u64,
    pub out_of_order_offset: u64,
    pub invalid_bounds: u64,
    pub corrupt_persisted_pending_transfer: u64,
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

impl SnapshotChunkRejectionCounters {
    /// Returns whether all rejection counters are zero.
    #[must_use]
    pub fn is_empty(self) -> bool {
        self.stale_term == 0
            && self.wrong_transfer == 0
            && self.metadata_mismatch == 0
            && self.out_of_order_offset == 0
            && self.invalid_bounds == 0
            && self.corrupt_persisted_pending_transfer == 0
    }
}

/// Errors returned while constructing snapshot metadata.
///
/// This enum is exhaustive because snapshot metadata validation is closed over
/// these structural checks.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SnapshotMetadataError {
    ZeroApplicationSnapshotVersion,
    ZeroLastIncludedIndex,
    /// A boundary at the maximum log index has no successor: nothing could
    /// ever be appended after it, and index arithmetic on it overflows.
    LastIncludedIndexAtMaximum,
    ZeroLastIncludedTerm {
        last_included_index: LogIndex,
    },
    SnapshotTermAheadOfHardState {
        last_included_index: LogIndex,
        last_included_term: Term,
        hard_state_term: Term,
    },
}

impl fmt::Display for SnapshotMetadataError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroApplicationSnapshotVersion => {
                formatter.write_str("application snapshot version cannot be zero")
            }
            Self::ZeroLastIncludedIndex => {
                formatter.write_str("Raft snapshot last included index cannot be zero")
            }
            Self::LastIncludedIndexAtMaximum => formatter
                .write_str("Raft snapshot last included index cannot be the maximum log index"),
            Self::ZeroLastIncludedTerm {
                last_included_index,
            } => write!(
                formatter,
                "Raft snapshot last included term at index {last_included_index} cannot be zero"
            ),
            Self::SnapshotTermAheadOfHardState {
                last_included_index,
                last_included_term,
                hard_state_term,
            } => write!(
                formatter,
                "Raft snapshot term {last_included_term} at index {last_included_index} is ahead of hard-state term {hard_state_term}"
            ),
        }
    }
}

impl Error for SnapshotMetadataError {}
