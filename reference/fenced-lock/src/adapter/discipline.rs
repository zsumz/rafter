//! Applied-index and snapshot rules shared by both lock state machines.
//!
//! The in-memory adapter and the durable one differ in where application state
//! lives, not in when a committed entry may execute, when a read is fresh
//! enough, or when a snapshot may be built or installed. Those rules live here
//! so the two machines cannot drift apart: a durable path that re-derived them
//! would be exactly the place a second implementation of "is this entry already
//! applied?" hides a double-apply, and a double-apply of an acquisition reissues
//! a fencing token.
//!
//! Lock, session, token, and expiry decisions are not here and never will be.
//! They belong to [`crate::LockService`], which both machines drive.

use rafter::LogIndex;
use rafter_app::state_machine::{ApplicationSnapshot, ReadBarrier};

use crate::{adapter::codec, LockAdapterError, LockServiceSnapshot};

/// Refuses a committed entry at or below the durable applied floor.
///
/// Re-applying such an entry would execute an acknowledged command twice.
pub(crate) fn admit_entry(
    entry_index: LogIndex,
    applied_index: LogIndex,
) -> Result<(), LockAdapterError> {
    if entry_index <= applied_index {
        return Err(LockAdapterError::AppliedIndexRegression {
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
) -> Result<(), LockAdapterError> {
    if applied_index < barrier.required_applied_index {
        return Err(LockAdapterError::ReadBarrierUnsatisfied {
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
) -> Result<(), LockAdapterError> {
    if at != applied_index {
        return Err(LockAdapterError::SnapshotIndexUnavailable {
            requested_index: at,
            applied_index,
        });
    }
    Ok(())
}

/// Validates a snapshot install and returns the model snapshot it carries.
///
/// Three refusals, in the order a reader should think about them. An install
/// that would move the applied floor backwards makes acknowledged commands
/// executable again, and for this application that means reissuing a fencing
/// token. A descriptor with no inline payload has nothing to install: Rafter's
/// own install path supplies a [`rafter::RaftSnapshot`] descriptor whose
/// application bytes live in the replica's snapshot store rather than in the
/// message, and this application has no path to fetch them, so it refuses
/// rather than installing an empty state over live high-water marks. A payload
/// whose own index disagrees with the declared one is not the snapshot the
/// installer thinks it is.
pub(crate) fn admit_install(
    snapshot: &ApplicationSnapshot,
    applied_index: LogIndex,
) -> Result<LockServiceSnapshot, LockAdapterError> {
    if snapshot.applied_index < applied_index {
        return Err(LockAdapterError::SnapshotBehindAppliedIndex {
            snapshot_index: snapshot.applied_index,
            applied_index,
        });
    }
    if snapshot.payload.is_empty() {
        return Err(LockAdapterError::SnapshotPayloadUnavailable {
            applied_index: snapshot.applied_index,
        });
    }

    let (payload_index, service_snapshot) = codec::decode_snapshot(&snapshot.payload)?;
    if payload_index != snapshot.applied_index.0 {
        return Err(LockAdapterError::SnapshotIndexMismatch {
            payload_index: LogIndex(payload_index),
            declared_index: snapshot.applied_index,
        });
    }
    Ok(service_snapshot)
}
