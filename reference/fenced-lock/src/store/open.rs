//! The two entry points that open the store by reading it.
//!
//! [`LockStore::open`] reads, and [`LockStore::open_and_repair`] chooses between
//! two readings. They share one opening path and differ in exactly one branch;
//! the third decision, which keeps neither reading, is `reseed`.

use std::{fs, path::Path};

use rafter::LogIndex;

use crate::{LockConfig, LockService};

use super::{
    adopt::{choose_live_slot, OnUnreadableSlot},
    damage::{SlotDamage, SlotState},
    domination::marks_of,
    error::LockStoreError,
    fault::FaultPlan,
    format::{SlotIndex, SEALED_MARK},
    image::{decode_image, verify_sealed_slot, verify_slot, DecodedImage},
    report::{RecoveryReport, Repair},
    slot_file::{establish_slot_files, read_slot},
    Health, LockStore,
};

impl LockStore {
    /// Opens the store in `directory`, creating and recovering as needed.
    ///
    /// # Errors
    ///
    /// Returns an error when the directory or a slot cannot be read, when no
    /// slot holds a readable image, when a slot was written under different
    /// resource bounds, when a sealed image is malformed or violates a lock
    /// service invariant, or when the two slots disagree about generation
    /// ordering or fencing high-water marks.
    pub fn open(directory: &Path, config: LockConfig) -> Result<Self, LockStoreError> {
        Self::open_with_faults(directory, config, FaultPlan::none())
    }

    /// Opens the store, giving up a slot this build cannot read.
    ///
    /// This is the destructive half of [`LockStore::open`], and it is a separate
    /// entry point because it is a separate decision. Opening a store is a read;
    /// a caller that runs this one has decided that a store it cannot fully read
    /// is better opened one generation back than left alone, and
    /// [`RecoveryReport::repair`] tells it exactly which slot was given up and
    /// what it held.
    ///
    /// The store had no such entry point while its refusals were rarer, and the
    /// sibling ledger has had one for as long as it has refused anything. The
    /// argument for adding it is not symmetry: it is that
    /// [`SlotDamage::UnsealedCompleteImage`] turns an ordinary crash — a
    /// publication interrupted between its barrier and its seal — into a
    /// refusal, and that refusal needs somewhere to go.
    ///
    /// **How much of that crash this reaches, stated in the direction the code
    /// decides it.** It reaches an interrupted publication whose image the
    /// stale partner still dominates — which, for this store, means a
    /// publication that raised no fencing high-water mark. A release is one. So
    /// is an expiry, a renewal, and a session open. An **acquisition** is not:
    /// it raises a mark by construction, the interrupted image is the newer one,
    /// and no older partner can dominate a mark that image was the first to
    /// hold. That discard is [`LockStoreError::DiscardWouldRegressMark`] and is
    /// refused here as well as in [`LockStore::open`];
    /// `verify_discard_preserves_marks` argues why, and the refusal stands.
    ///
    /// Acquisition is the operation a fencing lock exists to perform, so this
    /// entry point does **not** cover the ordinary crash in general, and it is
    /// worth saying which half it leaves: a store crashed mid-acquisition is
    /// refused by every entry point that tries to read it.
    /// [`LockStore::discard_and_reseed`] is where that store goes, and it is a
    /// third decision rather than a flag on this one.
    /// `gen5_the_ordinary_crash_on_a_release_is_repairable` and
    /// `gen5_the_ordinary_crash_on_an_acquisition_has_no_way_forward` run the
    /// same crash on either side of that line.
    ///
    /// Beyond the mark rule it gives up exactly the refusal
    /// [`LockStoreError::UnreadableSlot`] names, and nothing else. It does
    /// **not** clear:
    ///
    /// - a damaged slot whose partner is not intact, which stays
    ///   [`LockStoreError::UnreadableSlot`]. There is nothing to fall back to,
    ///   so this is the same refusal `open` gives, reached through the repair.
    ///
    /// - [`SlotDamage::UnsupportedFormatVersion`], which needs no corruption at
    ///   all: a binary downgrade produces it from two healthy files, so the slot
    ///   holds a newer build's committed work and the remedy for damage must not
    ///   delete it. The ledger's repair refuses the same shape for the same
    ///   reason.
    /// - [`LockStoreError::NoReadableImage`]. There is no image to adopt in the
    ///   damaged slot's place, and opening empty would hand out token 1 for a
    ///   resource whose guarded downstream has already accepted far more. A
    ///   repair chooses between two readings of a store; it does not invent one.
    /// - [`LockStoreError::MissingSlot`]. A file that is gone is not damage this
    ///   build found in an artifact it read, and re-creating it is a different
    ///   act from choosing between two files that are both present.
    ///
    /// A store with nothing wrong is opened exactly as [`LockStore::open`] opens
    /// it, and reports no repair. Repairing is not the same as being willing to
    /// repair.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`LockStore::open`], with one narrowed and one
    /// widened — it is not uniformly more permissive, and reading it that way is
    /// how the deadlock above went unnoticed.
    ///
    /// - [`LockStoreError::UnreadableSlot`] in strictly fewer cases: only where
    ///   the damaged slot's partner is not an intact image, or the damage is
    ///   [`SlotDamage::UnsupportedFormatVersion`].
    /// - [`LockStoreError::DiscardWouldRegressMark`] in strictly *more*, because
    ///   the mark comparison on the slot being given up is only reached once a
    ///   caller has asked to give one up. A store `open` refuses with
    ///   `UnreadableSlot` can refuse here with this instead, which is a
    ///   different refusal and not a resolution.
    pub fn open_and_repair(directory: &Path, config: LockConfig) -> Result<Self, LockStoreError> {
        Self::open_inner(directory, config, FaultPlan::none(), OnUnreadableSlot::Give)
    }

    /// Opens the store with a deterministic fault schedule.
    ///
    /// This is the crash-test construction described on [`FaultPlan`]. A store
    /// opened with [`FaultPlan::none`] behaves exactly as [`LockStore::open`].
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`LockStore::open`]. Faults apply to
    /// publications, so opening never injects one.
    pub fn open_with_faults(
        directory: &Path,
        config: LockConfig,
        faults: FaultPlan,
    ) -> Result<Self, LockStoreError> {
        Self::open_inner(directory, config, faults, OnUnreadableSlot::Refuse)
    }

    /// The one opening path, parameterized by what it does with a slot it cannot
    /// read.
    ///
    /// Both entry points read the same bytes and run the same classification,
    /// and exactly one branch reads `on_unreadable` — the composite refusal in
    /// `choose_live_slot`. That is the point: a reader auditing this can see
    /// that refusing and repairing agree about everything except whether a slot
    /// which may have held the newest committed state is allowed to be given up.
    ///
    /// One consequence is worth naming rather than leaving to be inferred. The
    /// repair runs a check the refusal never reaches — `verify_discard_preserves_marks`
    /// on the slot it is about to give up — so the repair can fail with an error
    /// `open` cannot produce. "Strictly fewer refusals" is therefore false of
    /// the variants and true only of `LockStoreError::UnreadableSlot`; the
    /// [`LockStore::open_and_repair`] errors section says which.
    ///
    /// [`LockStore::discard_and_reseed`] is not this function's caller at all.
    /// It empties the directory first and then opens it, so it reaches here with
    /// nothing to classify.
    pub(super) fn open_inner(
        directory: &Path,
        config: LockConfig,
        faults: FaultPlan,
        on_unreadable: OnUnreadableSlot,
    ) -> Result<Self, LockStoreError> {
        fs::create_dir_all(directory).map_err(|source| LockStoreError::Io {
            operation: "create the lock store directory",
            path: directory.to_path_buf(),
            source,
        })?;

        let created = establish_slot_files(directory)?;

        let mut states = [SlotState::Empty; 2];
        let mut images: [Option<DecodedImage>; 2] = [None, None];
        for slot in [SlotIndex::Zero, SlotIndex::One] {
            let bytes = read_slot(directory, slot)?;
            match verify_slot(&bytes) {
                Ok(None) => states[slot.position()] = SlotState::Empty,
                Ok(Some(sealed)) => {
                    let image = decode_image(slot, &sealed, config)?;
                    states[slot.position()] = SlotState::Intact {
                        generation: sealed.generation,
                        applied_index: image.applied_index,
                    };
                    images[slot.position()] = Some(image);
                }
                Err(damage) => {
                    states[slot.position()] = SlotState::Damaged(damage);
                    // A slot whose *only* fault is its mark still holds a whole
                    // image that verified under the restored mark, and
                    // `classify_unsealed` has already decoded enough of it to
                    // report a generation. Decoding the rest of it here is what
                    // lets everything that discards or sets aside this slot say
                    // what it gave up in the dimension that matters — the
                    // fencing marks — instead of only how many generations
                    // apart the two slots were.
                    //
                    // Every other damage leaves nothing to decode, and there
                    // the old sentence is the true one: reading the slot is
                    // exactly what failed, so nobody can say what was in it.
                    //
                    // A decode that fails here is not an error. This slot is
                    // damaged and is going to be refused or given up either
                    // way, and turning "the image this build was about to
                    // discard is also invalid" into a new refusal would take
                    // away a repair that used to work. It stays undecoded, and
                    // `Repair::marks_cross_checked` reports `false`.
                    if let SlotDamage::UnsealedCompleteImage { .. } = damage {
                        let mut restored = bytes.clone();
                        restored[0] = SEALED_MARK;
                        images[slot.position()] = verify_sealed_slot(&restored)
                            .ok()
                            .and_then(|sealed| decode_image(slot, &sealed, config).ok());
                    }
                }
            }
        }

        let adopted = choose_live_slot(&states, images, on_unreadable)?;
        let (service, applied_index, generation, live_slot, cross_checked_marks, given_up) =
            match adopted {
                Some(adopted) => (
                    adopted.image.service,
                    adopted.image.applied_index,
                    adopted.generation,
                    Some(adopted.slot),
                    adopted.cross_checked_marks,
                    adopted.given_up,
                ),
                None => (
                    LockService::new(config),
                    LogIndex::ZERO,
                    0,
                    None,
                    false,
                    None,
                ),
            };
        let acknowledged_marks = marks_of(&service);
        let repair =
            given_up
                .zip(live_slot)
                .map(|((slot, damage, marks_cross_checked), adopted)| Repair {
                    slot,
                    damage,
                    adopted,
                    adopted_generation: generation,
                    discarded_generation: match damage {
                        SlotDamage::UnsealedCompleteImage { generation, .. } => Some(generation),
                        _ => None,
                    },
                    marks_cross_checked,
                });

        Ok(Self {
            directory: directory.to_path_buf(),
            config,
            service,
            applied_index,
            generation,
            live_slot,
            acknowledged_marks,
            health: Health::Healthy,
            faults,
            publications: 0,
            fired_fault: None,
            recovery: RecoveryReport {
                created,
                slots: states,
                live_slot,
                cross_checked_marks,
                repair,
                // Set by `discard_and_reseed` after this returns. No branch
                // inside the opening path can reach one: a re-seed happens
                // before the opening, to the directory rather than in it.
                reseed: None,
            },
        })
    }
}
