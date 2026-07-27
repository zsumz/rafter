//! The two entry points that open the store.
//!
//! [`LedgerStore::open`] refuses a region it positively cannot read, and
//! [`LedgerStore::open_and_repair`] discards it and reports what that cost.
//! They share one opening path and differ in exactly one branch — narrower than
//! "one truncates and one does not", because the truncatable-residue filter
//! sits above that branch and both entry points shorten a zero-filled tail.

use std::{fs, path::Path};

use rafter::LogIndex;

use crate::{Ledger, LedgerConfig};

use super::{
    damage::TornTail,
    error::LedgerStoreError,
    fault::FaultPlan,
    format::JOURNAL_FILE_NAME,
    journal_file::{create_journal, sweep_staged_file, truncate_journal},
    report::{RecoveryReport, Repair},
    scan::scan_journal,
    Health, LedgerStore,
};

impl LedgerStore {
    /// Opens the store in `directory`, creating and recovering as needed.
    ///
    /// # Errors
    ///
    /// Returns an error when the directory or journal cannot be opened, when
    /// the journal header is corrupt or was written under different resource
    /// bounds, when a committed frame's image is malformed or violates a model
    /// invariant, or when the journal holds a frame recovery cannot show an
    /// interrupted append left — see [`LedgerStoreError::UnreadableFrame`], and
    /// [`LedgerStore::open_and_repair`] for the entry point that discards it.
    pub fn open(directory: &Path, config: LedgerConfig) -> Result<Self, LedgerStoreError> {
        Self::open_with_faults(directory, config, FaultPlan::none())
    }

    /// Opens the store, discarding an unreadable frame and everything after it.
    ///
    /// This is the larger destructive half of [`LedgerStore::open`], and it is
    /// a separate entry point because it is a separate decision: a caller that
    /// runs this one has decided that a journal it cannot fully read is better
    /// shortened than left alone, and [`RecoveryReport::repair`] tells it what
    /// that cost — the offset, the corruption, and the byte count.
    ///
    /// "Larger" rather than "the" destructive half, and the difference is not
    /// pedantry. [`LedgerStore::open`] shortens the journal too, in one case:
    /// a zero-filled tail, which it truncates without being able to show no
    /// commit point covered it — see
    /// [`RecoveryReport::discarded_without_proof`] and, for why that trade is
    /// made rather than gated behind this entry point,
    /// [`TornTail::is_truncatable_residue`]. What this one adds is that it
    /// discards a region recovery *positively* cannot read, of any length,
    /// wherever the scan stopped.
    ///
    /// A journal with nothing wrong is opened exactly as [`LedgerStore::open`]
    /// opens it, and reports no repair. Repairing is not the same as being
    /// willing to repair.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`LedgerStore::open`] except
    /// [`LedgerStoreError::UnreadableFrame`], which is what this discards.
    pub fn open_and_repair(
        directory: &Path,
        config: LedgerConfig,
    ) -> Result<Self, LedgerStoreError> {
        Self::open_inner(
            directory,
            config,
            FaultPlan::none(),
            OnUnreadableFrame::Discard,
        )
    }

    /// Opens the store with a deterministic fault schedule.
    ///
    /// This is the crash-test construction described on [`FaultPlan`]. A store
    /// opened with [`FaultPlan::none`] behaves exactly as [`LedgerStore::open`].
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`LedgerStore::open`]. Faults apply to write
    /// plans, so opening never injects one.
    pub fn open_with_faults(
        directory: &Path,
        config: LedgerConfig,
        faults: FaultPlan,
    ) -> Result<Self, LedgerStoreError> {
        Self::open_inner(directory, config, faults, OnUnreadableFrame::Refuse)
    }

    /// The one opening path, parameterized by what it does with an unreadable
    /// frame.
    ///
    /// Both entry points read the same bytes and run the same scan, and they
    /// differ in exactly one branch — the `match` on `on_unreadable` below.
    /// What they differ *about* is narrower than this used to say. It said they
    /// agree on everything except whether a region a commit point may have
    /// covered is allowed to disappear. They do not: the
    /// `is_truncatable_residue` filter sits **above** that branch, so both entry
    /// points let a zero-filled tail disappear, and a commit point may have
    /// covered it.
    ///
    /// The branch decides whether a region this build positively cannot read —
    /// one that failed identity, or a whole frame with an unsealed mark, or
    /// damage to a sealed frame — is discarded or refused. That is the
    /// difference, and it is the larger one.
    fn open_inner(
        directory: &Path,
        config: LedgerConfig,
        faults: FaultPlan,
        on_unreadable: OnUnreadableFrame,
    ) -> Result<Self, LedgerStoreError> {
        fs::create_dir_all(directory).map_err(|source| LedgerStoreError::Io {
            operation: "create the ledger store directory",
            path: directory.to_path_buf(),
            source,
        })?;

        let journal_path = directory.join(JOURNAL_FILE_NAME);
        // Before anything looks at the journal, so an interrupted creation's
        // staging file is gone by the time `exists` decides whether to create.
        let removed_staged_bytes = sweep_staged_file(directory)?;

        let created = !journal_path.exists();
        if created {
            create_journal(directory, config)?;
        }

        let bytes = fs::read(&journal_path).map_err(|source| LedgerStoreError::Io {
            operation: "read the ledger journal",
            path: journal_path.clone(),
            source,
        })?;
        let scan = scan_journal(&bytes, config)?;

        // A frame this build cannot read is not damage, so it is not something
        // a repair may clear. Discarding it would delete a newer build's
        // committed work on the strength of a version byte, so both entry
        // points refuse it, above the repair branch rather than inside it.
        if let Some(TornTail::UnsupportedFrameVersion { version }) = scan.torn_tail {
            return Err(LedgerStoreError::UnsupportedFrameVersion {
                offset: scan.committed_len as u64,
                version,
            });
        }

        // The whole fail-closed rule, in one branch. Truncating is only ever
        // legal for residue `TornTail::is_truncatable_residue` covers; anything
        // else may sit at or below the last commit point, and shortening the
        // file there would delete acknowledged history during a read.
        let unreadable = scan
            .torn_tail
            .filter(|tail| !tail.is_truncatable_residue())
            .map(|corruption| Repair {
                offset: scan.committed_len as u64,
                corruption,
                discarded_bytes: (bytes.len() - scan.committed_len) as u64,
            });
        let repair = match (unreadable, on_unreadable) {
            (Some(repair), OnUnreadableFrame::Refuse) => {
                return Err(LedgerStoreError::UnreadableFrame {
                    offset: repair.offset,
                    corruption: repair.corruption,
                    committed_frames: scan.committed_frames,
                    unreadable_bytes: repair.discarded_bytes,
                })
            }
            (repair, OnUnreadableFrame::Discard) => repair,
            (None, OnUnreadableFrame::Refuse) => None,
        };

        if scan.committed_len < bytes.len() {
            truncate_journal(&journal_path, scan.committed_len)?;
        }

        let (ledger, applied_index) = match scan.image {
            Some((ledger, applied_index)) => (ledger, applied_index),
            None => (Ledger::new(config), LogIndex::ZERO),
        };

        Ok(Self {
            directory: directory.to_path_buf(),
            journal_path,
            config,
            ledger,
            applied_index,
            journal_len: scan.committed_len as u64,
            health: Health::Healthy,
            faults,
            write_plans: 0,
            fired_fault: None,
            recovery: RecoveryReport {
                created,
                committed_frames: scan.committed_frames,
                torn_tail: scan.torn_tail,
                // A repair's losses are counted by the repair, not here.
                //
                // What is left here is what `open` itself shortened, which is
                // two different things under one total: bytes rule one proved
                // no commit point covered, and bytes rule two discarded on the
                // weaker premise. The second number is broken out rather than
                // left inside the first, because a caller reading one total
                // cannot tell a crash that cost nothing from one that may have
                // cost an acknowledged transaction.
                discarded_bytes: if repair.is_some() {
                    0
                } else {
                    (bytes.len() - scan.committed_len) as u64
                },
                //
                // There is no `repair.is_some()` guard here and it would be
                // unreachable if there were: `ZeroFilledToEnd` is truncatable
                // residue, so the filter above never turns one into a repair.
                // A guard for a state that cannot arise reads as though it can,
                // and `a_zero_filled_tail_is_truncatable_residue_at_every_length`
                // is where that reason is checked instead.
                discarded_without_proof: match scan.torn_tail {
                    Some(TornTail::ZeroFilledToEnd { present }) => present,
                    _ => 0,
                },
                removed_staged_bytes,
                repair,
            },
        })
    }
}

/// What an opening does when it meets a frame recovery cannot show an
/// interrupted append left.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OnUnreadableFrame {
    /// Refuse to open. This is [`LedgerStore::open`].
    Refuse,
    /// Discard from that frame to the end of the journal, and report it. This
    /// is [`LedgerStore::open_and_repair`].
    Discard,
}
