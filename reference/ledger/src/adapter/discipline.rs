//! Applied-index and snapshot rules shared by both ledger state machines.
//!
//! The in-memory adapter and the durable one differ in where application state
//! lives, not in when a committed entry may execute, when a read is fresh
//! enough, or when a snapshot may be installed. Those rules live here so the
//! two machines cannot drift apart: a durable path that re-derived them would
//! be exactly the place a second implementation of "is this entry already
//! applied?" hides a double-apply. Where an install's bytes come from is here
//! for the same reason: a machine that resolved a promoted snapshot on its own
//! would be a second answer to "which bytes is this replica allowed to adopt?".
//!
//! Ledger, session, and deduplication decisions are not here and never will be.
//! They belong to [`crate::Ledger`], which both machines drive.

use rafter::{LogIndex, RaftSnapshot, SnapshotChunkSource};
use rafter_app::state_machine::{ApplicationSnapshot, ReadBarrier};
use rafter_runtime::snapshot_io::{
    SnapshotCancellation, SnapshotPayloadReader, SnapshotReadOptions,
};

use crate::{adapter::codec, Ledger, LedgerAdapterError, LedgerConfig};

const PROMOTED_READ_CHUNK: u32 = 16 * 1024;

/// Refuses a committed entry at or below the durable applied floor.
///
/// Re-applying such an entry would execute an acknowledged command twice.
pub(crate) fn admit_entry(
    entry_index: LogIndex,
    applied_index: LogIndex,
) -> Result<(), LedgerAdapterError> {
    if entry_index <= applied_index {
        return Err(LedgerAdapterError::AppliedIndexRegression {
            entry_index,
            applied_index,
        });
    }
    Ok(())
}

/// Refuses a read whose barrier demands freshness this replica has not applied.
pub(crate) fn admit_read(
    barrier: ReadBarrier,
    applied_index: LogIndex,
) -> Result<(), LedgerAdapterError> {
    if applied_index < barrier.required_applied_index {
        return Err(LedgerAdapterError::ReadBarrierUnsatisfied {
            required_applied_index: barrier.required_applied_index,
            applied_index,
        });
    }
    Ok(())
}

/// Refuses a snapshot request at an index this state machine cannot reproduce.
///
/// A state machine holds the state of its current applied index and no other,
/// so `at` must be exactly that index.
pub(crate) fn admit_snapshot_request(
    at: LogIndex,
    applied_index: LogIndex,
) -> Result<(), LedgerAdapterError> {
    if at != applied_index {
        return Err(LedgerAdapterError::SnapshotIndexUnavailable {
            requested_index: at,
            applied_index,
        });
    }
    Ok(())
}

/// Validates inline or promoted bytes into temporary, bounded ledger state.
///
/// The applied-floor check precedes source effects. Both forms share the same
/// field decoder and model restorer. Promoted bytes additionally require exact
/// descriptor identity, length, CRC32, and full consumption before state can
/// leave this function. A refusal changes no installed state or applied floor.
pub(crate) fn admit_install(
    snapshot: &ApplicationSnapshot,
    applied_index: LogIndex,
    config: LedgerConfig,
    source: Option<&dyn SnapshotChunkSource>,
) -> Result<Ledger, LedgerAdapterError> {
    if let Some(refusal) = behind_floor(snapshot, applied_index) {
        return Err(refusal);
    }
    let (payload_index, ledger) = if snapshot.payload.is_empty() {
        let unavailable = LedgerAdapterError::SnapshotPayloadUnavailable {
            applied_index: snapshot.applied_index,
        };
        let descriptor = admit_descriptor(snapshot, config)?;
        let Some(source) = source else {
            return Err(unavailable);
        };
        read_promoted(descriptor, source, applied_index, config)?
    } else {
        codec::decode_snapshot::<LedgerAdapterError>(&snapshot.payload, config)?
    };
    if payload_index != snapshot.applied_index.0 {
        return Err(LedgerAdapterError::SnapshotIndexMismatch {
            payload_index: LogIndex(payload_index),
            declared_index: snapshot.applied_index,
        });
    }
    Ok(ledger)
}

/// Whether an install needs bytes from a snapshot source at all.
///
/// A caller whose source costs something to reach — a file store that must be
/// opened, and whose opening creates a directory — asks this before paying for
/// one. The answer is here rather than at the call site so that "which installs
/// read a store?" cannot come to disagree with which installs
/// [`admit_install`] would let through: an install refused on its index must
/// not have touched the medium, and an install carrying its own bytes has no
/// reason to.
pub(crate) fn install_needs_source(
    snapshot: &ApplicationSnapshot,
    applied_index: LogIndex,
    config: LedgerConfig,
) -> bool {
    behind_floor(snapshot, applied_index).is_none()
        && snapshot.payload.is_empty()
        && admit_descriptor(snapshot, config).is_ok()
}

fn admit_descriptor(
    snapshot: &ApplicationSnapshot,
    config: LedgerConfig,
) -> Result<&RaftSnapshot, LedgerAdapterError> {
    let unavailable = LedgerAdapterError::SnapshotPayloadUnavailable {
        applied_index: snapshot.applied_index,
    };
    let descriptor = snapshot.raft_snapshot.as_ref().ok_or(unavailable)?;
    if descriptor.metadata.last_included_index != snapshot.applied_index
        || descriptor.metadata.application.kind.as_str() != "ledger"
        || descriptor.metadata.application.version.get()
            != u16::from(codec::SNAPSHOT_FORMAT_VERSION)
        || descriptor.application_payload_len > codec::snapshot_payload_limit(config)
    {
        return Err(unavailable);
    }
    Ok(descriptor)
}

/// Names the refusal for an install that would move the applied floor
/// backwards, or `None` when it would not.
///
/// One comparison, in one place, because two callers ask it: the install that
/// refuses on it, and the predicate above that decides whether to open a store
/// for an install this would refuse anyway.
fn behind_floor(
    snapshot: &ApplicationSnapshot,
    applied_index: LogIndex,
) -> Option<LedgerAdapterError> {
    (snapshot.applied_index < applied_index).then_some(
        LedgerAdapterError::SnapshotBehindAppliedIndex {
            snapshot_index: snapshot.applied_index,
            applied_index,
        },
    )
}

fn read_promoted(
    descriptor: &RaftSnapshot,
    source: &dyn SnapshotChunkSource,
    applied_index: LogIndex,
    config: LedgerConfig,
) -> Result<(u64, Ledger), LedgerAdapterError> {
    let unavailable = LedgerAdapterError::SnapshotPayloadUnavailable {
        applied_index: descriptor.metadata.last_included_index,
    };
    let mut reader = SnapshotPayloadReader::new(
        descriptor,
        source,
        applied_index,
        SnapshotReadOptions::new(codec::snapshot_payload_limit(config))
            .with_chunk_bytes(PROMOTED_READ_CHUNK),
        SnapshotCancellation::new(),
    )
    .map_err(|_| unavailable)?;
    let decoded = codec::decode_snapshot_stream(
        &mut reader,
        descriptor.application_payload_len,
        descriptor.metadata.last_included_index,
        config,
    )?;
    reader.finish().map_err(|_| unavailable)?;
    Ok(decoded)
}

#[cfg(test)]
#[path = "discipline/stream_tests.rs"]
mod stream_tests;
