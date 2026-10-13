//! The entry point that keeps neither reading.
//!
//! [`LockStore::discard_and_reseed`] deletes both slot files and lets the
//! replicated log refill the store. It is the third of the three decisions the
//! [module documentation](super) describes as a ladder, and the only one that
//! opens a directory both reading entry points refuse.

use std::{fs, io, path::Path};

use rafter::LogIndex;

use crate::LockConfig;

use super::{
    adopt::OnUnreadableSlot,
    damage::{SlotDamage, SlotState},
    error::LockStoreError,
    fault::FaultPlan,
    format::{as_u64, SlotIndex, SEALED_MARK},
    image::{verify_sealed_slot, verify_slot},
    report::Reseed,
    slot_file::{slot_path, slot_state, sync_directory},
    LockStore,
};

impl LockStore {
    /// Deletes this replica's durable lock state and opens an empty store for
    /// the replicated log to fill.
    ///
    /// This is not a third way of reading the directory. It reads nothing it
    /// keeps: both slot files are removed and recreated empty, **whatever they
    /// held**, including a store with nothing wrong with it. A caller reaching
    /// for it has decided that this replica's local state is to be abandoned,
    /// not recovered.
    ///
    /// # Why the store needs one at all
    ///
    /// [`LockStoreError::DiscardWouldRegressMark`] is refused by both
    /// [`LockStore::open`] and [`LockStore::open_and_repair`], deliberately and
    /// with no override, and one power cut during an acquisition produces it —
    /// see [`LockStore::open_and_repair`] for why that is the common case
    /// rather than a corner. Neither entry point moves a byte, so every later
    /// attempt lands in the same place: without this call the directory has no
    /// entry point that opens it, and the way forward begins with an operator
    /// deleting files by hand. `gen5_a_wedged_store_has_exactly_one_way_forward`
    /// is that state and this way out of it.
    ///
    /// # Why deleting it is the right answer, and what is checked
    ///
    /// This store is one replica's projection of a replicated log, and it
    /// publishes only what that log has already committed: a state machine
    /// applies an entry after the entry commits, and this store publishes
    /// during the apply. So every fencing high-water mark this directory has
    /// ever held came from an entry a quorum had already accepted, under
    /// **both** readings of the damage — the interrupted publication and the
    /// rotted mark alike. Deleting the projection cannot lose a mark the log
    /// does not still hold.
    ///
    /// What refills it is this replica's own **retained** log: this call
    /// empties the application store and touches nothing else, so the Raft log
    /// beside it survives, the reopened store reports [`LogIndex::ZERO`], and
    /// the entries replay.
    /// `a_reseeded_replica_recovers_its_marks_from_the_group` runs that end to
    /// end and checks the mark comes back at or above the quorum's.
    ///
    /// # A compacted prefix needs snapshot recovery
    ///
    /// Replaying the retained suffix onto the empty store is not sufficient.
    /// A follower whose Raft log already matches the leader's need not receive
    /// a new snapshot from the group. The local snapshot must supply the prefix
    /// compaction removed, before any later application entry is replayed.
    ///
    /// This function only empties the application store. The process composition
    /// drains runtime recovery through `RaftGroup::apply_recovery_outputs`.
    /// When those outputs contain committed application entries above the local
    /// snapshot boundary, that operation first installs the retained snapshot.
    /// `explicit_reseed_restores_a_compacted_snapshot_before_replaying_its_suffix`
    /// checks this path over real files and authenticated TLS, including cold
    /// session-cache recovery and fencing marks from both snapshot and suffix.
    ///
    /// This does not make every compacted replica recoverable. With no committed
    /// application suffix and no recovery snapshot directive, the recovery pump
    /// performs no implicit install. A store still below the snapshot boundary
    /// is refused on use. Missing, invalid or unsupported snapshots also prevent
    /// recovery; a caller must not bypass the applied-floor checks. The raw
    /// output-pump cases in `gen6_reseed_compaction.rs` retain those guards.
    /// Repeating a reseed cannot reconstruct missing state. Restore a validated
    /// snapshot through the application recovery path or use the embedding's
    /// explicit replica-replacement procedure while preserving quorum state.
    ///
    /// # What it costs, and the premise nothing here can check
    ///
    /// Until the replay has run, this store holds no acknowledged marks, so the
    /// two checks that defend them — `verify_marks_dominate` on a publication
    /// and on recovery — have nothing to compare against and will accept any
    /// state offered. Re-seeding gives up this replica's ability to refuse a
    /// mark regression, in exchange for the log's authority over the same fact.
    ///
    /// And the premise: that the group still holds those entries. Re-seeding
    /// one replica of three is recoverable. Re-seeding a quorum destroys the
    /// marks outright, and a guarded resource downstream may then accept two
    /// independent tenures under one token. Nothing in this call can tell those
    /// two situations apart — it sees one directory — so it does not try, and
    /// it is not the place that decides.
    ///
    /// # Errors
    ///
    /// Returns [`LockStoreError::Io`] when a slot file cannot be removed or the
    /// emptied directory cannot be made durable, and afterwards the same errors
    /// as [`LockStore::open`] over the store it created — which, over a
    /// directory this call has just emptied, is a fresh store.
    pub fn discard_and_reseed(
        directory: &Path,
        config: LockConfig,
    ) -> Result<Self, LockStoreError> {
        rafter_reference_harness::durable_directory::create_durable_directory(directory).map_err(
            |source| LockStoreError::Io {
                operation: "durably create the lock store directory",
                path: directory.to_path_buf(),
                source,
            },
        )?;

        let reseed = survey_for_reseed(directory)?;
        for slot in [SlotIndex::Zero, SlotIndex::One] {
            let path = slot_path(directory, slot);
            match fs::remove_file(&path) {
                Ok(()) => {}
                Err(source) if source.kind() == io::ErrorKind::NotFound => {}
                Err(source) => {
                    return Err(LockStoreError::Io {
                        operation: "remove a lock store slot to re-seed it",
                        path,
                        source,
                    })
                }
            }
        }
        // Both names are gone before anything recreates them, so a crash here
        // leaves the directory in the one shape `establish_slot_files` treats
        // as a store that has never published. Re-running this call from there
        // reaches the same place, which is what makes it repeatable.
        sync_directory(directory)?;

        let mut store = Self::open_inner(
            directory,
            config,
            FaultPlan::none(),
            OnUnreadableSlot::Refuse,
        )?;
        store.recovery.reseed = Some(reseed);
        Ok(store)
    }
}

/// Summarizes one slot's bytes without decoding its payload.
/// Records what a re-seed is about to delete, without letting any of it refuse.
///
/// Every refusal the opening path can raise is a refusal *about* state this
/// call is about to remove, so raising one here would make the entry point that
/// exists for an unreadable store unreachable on exactly the stores it was
/// added for. So nothing below returns an error except a read that fails for a
/// reason unrelated to the bytes, and a slot file that is not there is not one
/// of those: a re-seed over a half-created store is still a re-seed.
///
/// The one thing it reads past the slot state is the applied index of a whole
/// image whose mark is unsealed, which `slot_state` reports only as damage.
/// That is the wedged store's newest image, so leaving it unread would mean the
/// report said least about the case this exists for.
fn survey_for_reseed(directory: &Path) -> Result<Reseed, LockStoreError> {
    let mut slots = [SlotState::Empty; 2];
    let mut discarded_bytes = 0_u64;
    let mut discarded_applied_index = None;

    for slot in [SlotIndex::Zero, SlotIndex::One] {
        let path = slot_path(directory, slot);
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(source) if source.kind() == io::ErrorKind::NotFound => continue,
            Err(source) => {
                return Err(LockStoreError::Io {
                    operation: "read a lock store slot before re-seeding it",
                    path,
                    source,
                })
            }
        };
        discarded_bytes += as_u64(bytes.len());
        slots[slot.position()] = slot_state(&bytes);
        if let Some(applied_index) = surveyed_applied_index(&bytes) {
            discarded_applied_index = Some(
                discarded_applied_index
                    .map_or(applied_index, |held: LogIndex| held.max(applied_index)),
            );
        }
    }

    Ok(Reseed {
        slots,
        discarded_bytes,
        discarded_applied_index,
    })
}

/// Returns the applied index a slot's bytes declare, when they are a whole
/// image under either mark.
fn surveyed_applied_index(bytes: &[u8]) -> Option<LogIndex> {
    match verify_slot(bytes) {
        Ok(Some(sealed)) => Some(sealed.applied_index),
        Err(SlotDamage::UnsealedCompleteImage { .. }) => {
            let mut restored = bytes.to_vec();
            restored[0] = SEALED_MARK;
            verify_sealed_slot(&restored)
                .ok()
                .map(|sealed| sealed.applied_index)
        }
        _ => None,
    }
}
