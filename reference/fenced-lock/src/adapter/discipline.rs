//! Applied-index and snapshot rules shared by both lock state machines.
//!
//! The in-memory adapter and the durable one differ in where application state
//! lives, not in when a committed entry may execute, when a read is fresh
//! enough, or when a snapshot may be built or installed. Those rules live here
//! so the two machines cannot drift apart: a durable path that re-derived them
//! would be exactly the place a second implementation of "is this entry already
//! applied?" hides a double-apply, and a double-apply of an acquisition reissues
//! a fencing token. Where an install's bytes come from is here for the same
//! reason: a machine that resolved a promoted snapshot on its own would be a
//! second answer to "which bytes is this replica allowed to adopt?".
//!
//! Lock, session, token, and expiry decisions are not here and never will be.
//! They belong to [`crate::LockService`], which both machines drive.

use std::borrow::Cow;

use rafter::{LogIndex, RaftSnapshot, SnapshotChunkRequest, SnapshotChunkSource};
use rafter_app::state_machine::{ApplicationSnapshot, ReadBarrier};

use crate::{adapter::codec, LockAdapterError, LockServiceSnapshot};

/// Bytes pulled per read while streaming a promoted payload out of a snapshot
/// source.
///
/// The payload is assembled from bounded reads rather than one read the length
/// of the descriptor, because that length is a number the sending replica
/// chose. A snapshot large enough to matter is streamed either way; a snapshot
/// small enough not to costs one read.
const PROMOTED_READ_CHUNK: u32 = 16 * 1024;

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
/// token, so it is refused before anything is read. A payload that cannot be
/// obtained at all has nothing to install. A payload whose own index disagrees
/// with the declared one is not the snapshot the installer thinks it is.
///
/// # Where the bytes come from
///
/// An install arrives in one of two shapes and this function is the only place
/// that knows the difference. A locally built snapshot carries its bytes
/// inline. A Raft-driven install carries none: Rafter hands over a
/// [`RaftSnapshot`] descriptor whose application bytes were already staged and
/// promoted into the replica's own snapshot store, and `source` is this
/// replica's read side of that store. The bytes are then streamed back in
/// bounded chunks keyed by [`RaftSnapshot::transfer_id`], which is what makes
/// the store answer for *this* transfer rather than whatever it happens to
/// hold.
///
/// Both shapes leave here through the same decode and the same index check.
/// That is the point of resolving the bytes rather than validating them: a
/// second path would be a second opinion about what a legal lock snapshot is,
/// and the one that only promoted transfers take is the one nobody reads.
///
/// A refusal is still the answer whenever the bytes genuinely are not there —
/// no descriptor and no inline payload, no configured source, or a source that
/// cannot serve that transfer id, descriptor, length, and checksum. Every one
/// of those is [`LockAdapterError::SnapshotPayloadUnavailable`], and none of
/// them installs an empty state over live high-water marks.
pub(crate) fn admit_install(
    snapshot: &ApplicationSnapshot,
    applied_index: LogIndex,
    source: Option<&dyn SnapshotChunkSource>,
) -> Result<LockServiceSnapshot, LockAdapterError> {
    if let Some(refusal) = behind_floor(snapshot, applied_index) {
        return Err(refusal);
    }
    let payload = resolve_payload(snapshot, source)?;

    let (payload_index, service_snapshot) = codec::decode_snapshot(&payload)?;
    if payload_index != snapshot.applied_index.0 {
        return Err(LockAdapterError::SnapshotIndexMismatch {
            payload_index: LogIndex(payload_index),
            declared_index: snapshot.applied_index,
        });
    }
    Ok(service_snapshot)
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
) -> bool {
    behind_floor(snapshot, applied_index).is_none()
        && snapshot.payload.is_empty()
        && snapshot.raft_snapshot.is_some()
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
) -> Option<LockAdapterError> {
    (snapshot.applied_index < applied_index).then_some(
        LockAdapterError::SnapshotBehindAppliedIndex {
            snapshot_index: snapshot.applied_index,
            applied_index,
        },
    )
}

/// Produces the application bytes one install must decode.
///
/// Inline bytes win when they are present, so a caller that already holds the
/// payload never consults a store. Everything else is the promoted form, and it
/// needs both halves — a descriptor naming the transfer and a source holding it
/// — before a byte is read.
fn resolve_payload<'a>(
    snapshot: &'a ApplicationSnapshot,
    source: Option<&dyn SnapshotChunkSource>,
) -> Result<Cow<'a, [u8]>, LockAdapterError> {
    if !snapshot.payload.is_empty() {
        return Ok(Cow::Borrowed(&snapshot.payload));
    }
    let unavailable = LockAdapterError::SnapshotPayloadUnavailable {
        applied_index: snapshot.applied_index,
    };
    let (Some(descriptor), Some(source)) = (snapshot.raft_snapshot.as_ref(), source) else {
        return Err(unavailable);
    };
    read_promoted(descriptor, source)
        .map(Cow::Owned)
        .ok_or(unavailable)
}

/// Streams one promoted payload out of `source` in bounded reads.
///
/// `None` means the source did not serve some chunk of this transfer, which is
/// the only answer a [`SnapshotChunkSource`] gives for a snapshot it does not
/// hold, cannot read, or holds under a different descriptor or checksum. The
/// caller turns that into the one typed refusal rather than guessing which of
/// those it was: none of them yields bytes this replica may install.
fn read_promoted(descriptor: &RaftSnapshot, source: &dyn SnapshotChunkSource) -> Option<Vec<u8>> {
    // Deliberately not reserved from `application_payload_len`: the length is
    // the sender's claim, and the loop below already refuses to read past it.
    let mut payload = Vec::new();
    let mut offset = 0_u64;
    while offset < descriptor.application_payload_len {
        let remaining = descriptor.application_payload_len - offset;
        let len = u32::try_from(remaining.min(u64::from(PROMOTED_READ_CHUNK))).ok()?;
        let chunk = source.snapshot_chunk(SnapshotChunkRequest {
            transfer_id: descriptor.transfer_id(),
            metadata: &descriptor.metadata,
            total_payload_len: descriptor.application_payload_len,
            application_payload_crc32: descriptor.application_payload_crc32,
            offset,
            len,
        })?;
        payload.extend_from_slice(&chunk);
        offset += u64::from(len);
    }
    Some(payload)
}
