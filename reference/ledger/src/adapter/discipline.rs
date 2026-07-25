//! Applied-index and snapshot rules shared by both ledger state machines.
//!
//! The in-memory adapter and the durable one differ in where application state
//! lives, not in when a committed entry may execute, when a read is fresh
//! enough, or when a snapshot may be installed. Those rules live here so the
//! two machines cannot drift apart: a durable path that re-derived them would
//! be exactly the place a second implementation of "is this entry already
//! applied?" hides a double-apply.
//!
//! Ledger, session, and deduplication decisions are not here and never will be.
//! They belong to [`crate::Ledger`], which both machines drive.

use rafter::LogIndex;
use rafter_app::state_machine::{ApplicationSnapshot, ReadBarrier};

use crate::{adapter::codec, LedgerAdapterError, LedgerSnapshot};

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

/// Validates a snapshot install and returns the model snapshot it carries.
///
/// Three refusals, in the order a reader should think about them: an install
/// that would move the applied floor backwards makes acknowledged commands
/// executable again; a descriptor with no inline payload has nothing to
/// install; and a payload whose own index disagrees with the declared one is
/// not the snapshot the installer thinks it is.
pub(crate) fn admit_install(
    snapshot: &ApplicationSnapshot,
    applied_index: LogIndex,
) -> Result<LedgerSnapshot, LedgerAdapterError> {
    if snapshot.applied_index < applied_index {
        return Err(LedgerAdapterError::SnapshotBehindAppliedIndex {
            snapshot_index: snapshot.applied_index,
            applied_index,
        });
    }
    if snapshot.payload.is_empty() {
        return Err(LedgerAdapterError::SnapshotPayloadUnavailable {
            applied_index: snapshot.applied_index,
        });
    }

    let (payload_index, ledger_snapshot) = codec::decode_snapshot(&snapshot.payload)?;
    if payload_index != snapshot.applied_index.0 {
        return Err(LedgerAdapterError::SnapshotIndexMismatch {
            payload_index: LogIndex(payload_index),
            declared_index: snapshot.applied_index,
        });
    }
    Ok(ledger_snapshot)
}
