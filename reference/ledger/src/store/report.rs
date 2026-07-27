//! What opening the store found, and what a repair discarded.
//!
//! [`RecoveryReport`] describes one opening and never changes afterwards. It
//! counts two different losses separately, because they are different:
//! [`RecoveryReport::discarded_without_proof`] is what `open` itself shortened
//! on the weaker premise, and [`Repair::discarded_bytes`] is what the larger,
//! separate decision gave up.

use std::fmt;

use super::damage::TornTail;

// Imported for the intra-doc links the prose below carries.
#[allow(unused_imports)]
use super::{error::LedgerStoreError, LedgerStore};

/// What opening the store found and did.
///
/// Recovery actions are observations, not failures, so they stay out of
/// [`LedgerStoreError`]. A test asserts on this report to show which crash
/// window it actually reproduced.
///
/// The report describes one opening and never changes afterwards. A later
/// transaction does not edit the history of how this handle came to exist; a
/// caller that wants the journal's current shape reads
/// [`LedgerStore::journal_len`], and a caller that wants to know what a fresh
/// opener would find reopens.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecoveryReport {
    pub(super) created: bool,
    pub(super) committed_frames: u64,
    pub(super) torn_tail: Option<TornTail>,
    pub(super) discarded_bytes: u64,
    pub(super) discarded_without_proof: u64,
    pub(super) removed_staged_bytes: Option<u64>,
    pub(super) repair: Option<Repair>,
}

/// What [`LedgerStore::open_and_repair`] discarded, when it discarded anything.
///
/// A repair is the largest thing this store does that can lose committed
/// transactions, so it is recorded rather than implied, and it is unreachable
/// through [`LedgerStore::open`] at all. It is not the *only* such thing:
/// `open` truncates a zero-filled tail, counted by
/// [`RecoveryReport::discarded_without_proof`], and this type used to claim
/// otherwise.
///
/// The count of *transactions* lost is deliberately absent. Frames past a
/// corrupt one cannot be located, let alone decoded, so nobody can count them;
/// pretending otherwise would put a number in a report that no one computed.
/// The byte count and the offset are what is actually known.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Repair {
    pub(super) offset: u64,
    pub(super) corruption: TornTail,
    pub(super) discarded_bytes: u64,
}

impl Repair {
    /// Byte offset the unreadable frame began at.
    #[must_use]
    pub const fn offset(&self) -> u64 {
        self.offset
    }

    /// Why the frame at [`Repair::offset`] could not be read.
    #[must_use]
    pub const fn corruption(&self) -> TornTail {
        self.corruption
    }

    /// Bytes discarded, from [`Repair::offset`] to the end of the journal.
    ///
    /// Any number of committed transactions may have been inside them.
    #[must_use]
    pub const fn discarded_bytes(&self) -> u64 {
        self.discarded_bytes
    }
}

impl fmt::Display for Repair {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "discarded {} bytes from byte {}, where the journal held {}",
            self.discarded_bytes, self.offset, self.corruption
        )
    }
}

impl RecoveryReport {
    /// Whether the journal was created by this open.
    #[must_use]
    pub const fn created(&self) -> bool {
        self.created
    }

    /// Number of committed frames recovery replayed.
    #[must_use]
    pub const fn committed_frames(&self) -> u64 {
        self.committed_frames
    }

    /// What recovery found past the last committed frame, if anything.
    ///
    /// Not "the residue an interrupted transaction left", which is what this
    /// said and is what only [`TornTail::UnsealedAppend`] can be shown to be.
    /// Every other variant is a shape an interrupted transaction and a damaged
    /// committed frame both produce, which is the whole reason they are separate
    /// variants.
    #[must_use]
    pub const fn torn_tail(&self) -> Option<TornTail> {
        self.torn_tail
    }

    /// Bytes truncated from the journal's uncommitted tail.
    ///
    /// What is promised about them depends on which residue they were, and
    /// [`RecoveryReport::torn_tail`] says which. Under
    /// [`TornTail::UnsealedAppend`] these are bytes no commit point ever covered
    /// and discarding them discards nothing. Under
    /// [`TornTail::ZeroFilledToEnd`] the weaker statement is the true one: every
    /// byte discarded was a zero, and there was no byte beyond them.
    /// [`TornTail::is_truncatable_residue`] carries both arguments and the limit
    /// of each; this counter is a byte count and settles neither.
    ///
    /// Bytes lost to a repair are counted separately, by
    /// [`Repair::discarded_bytes`], because they are a different kind of loss.
    #[must_use]
    pub const fn discarded_bytes(&self) -> u64 {
        self.discarded_bytes
    }

    /// Of [`RecoveryReport::discarded_bytes`], how many `open` deleted without
    /// being able to show no commit point covered them.
    ///
    /// Non-zero means the journal ended in a zero-filled tail and this opening
    /// shortened the file there. The bytes may have been an interrupted append,
    /// which is what the rule exists for, and they may equally have been
    /// committed frames a zeroed region erased — including transactions this
    /// replica already acknowledged. [`TornTail::is_truncatable_residue`] argues
    /// why the trade is made rather than gated, and states its bound.
    ///
    /// This exists because the distinction was reachable only by matching on
    /// [`RecoveryReport::torn_tail`] and reading two pages of prose, while the
    /// prose in front of a caller said `open` could not lose data at all. A loss
    /// a caller has to already know about in order to look for is one most
    /// callers will not look for, so it is a number they receive instead.
    ///
    /// Zero for a clean opening, for a tail proved uncommitted, and for a
    /// repair — whose losses [`Repair::discarded_bytes`] counts, and which is
    /// the larger and separate decision.
    #[must_use]
    pub const fn discarded_without_proof(&self) -> u64 {
        self.discarded_without_proof
    }

    /// Whether an abandoned staging file was removed.
    #[must_use]
    pub const fn removed_staged_file(&self) -> bool {
        self.removed_staged_bytes.is_some()
    }

    /// How large the removed staging file was, when one was removed.
    ///
    /// Deleting a file is worth more than a bit in a report. The sweep now
    /// removes exactly one name, so the name is known from the format, and this
    /// is the rest of what was lost.
    #[must_use]
    pub const fn removed_staged_bytes(&self) -> Option<u64> {
        self.removed_staged_bytes
    }

    /// What a repair discarded, when this opening was a repair that found work.
    ///
    /// Always `None` for [`LedgerStore::open`], which refuses rather than
    /// repairing.
    #[must_use]
    pub const fn repair(&self) -> Option<Repair> {
        self.repair
    }

    /// Whether this opening found nothing that needs a decision.
    ///
    /// A clean opening read a journal that was already there, whole. Anything
    /// else — residue from an interrupted transaction, a staging file an
    /// earlier incarnation abandoned, a repair that discarded a region, or
    /// creating the journal — is a fact a caller reopening a store after a
    /// crash should have to look at rather than step over.
    ///
    /// Creation counts deliberately. This store cannot tell a genuinely fresh
    /// replica from one whose journal was deleted, because both arrive here as
    /// an absent file; only the caller knows which it is. Leaving creation out
    /// of this predicate made the difference invisible to the one party that
    /// could see it — a vanished journal opened at applied index zero and
    /// reported a clean start — and made `created()` a report nothing read. A
    /// caller that expects to be creating a journal looks at
    /// [`RecoveryReport::created`] and carries on.
    #[must_use]
    pub const fn is_clean(&self) -> bool {
        !self.created
            && self.torn_tail.is_none()
            && self.removed_staged_bytes.is_none()
            && self.repair.is_none()
    }
}
