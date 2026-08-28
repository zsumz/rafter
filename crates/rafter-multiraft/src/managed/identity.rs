//! Stable scheduling identities and the closed work-class vocabulary.
//!
//! Each identity is assigned once by the scheduler and never reissued within
//! one instance. The work classes are a fixed priority ladder: every group
//! queue holds exactly one lane per class.

use std::num::NonZeroU64;

/// Neutral work classes, in service-priority order within one group turn.
///
/// This enum is exhaustive because the scheduler owns this closed priority
/// vocabulary and every queue has exactly one lane for each variant.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum WorkClass {
    /// Membership, leadership, and other control work.
    Control,
    /// Ordinary application commands.
    Command,
    /// Snapshot construction, transfer, or installation work.
    Snapshot,
    /// Bulk replication or maintenance work.
    Bulk,
}

impl WorkClass {
    pub(super) const COUNT: usize = 4;

    pub(super) const fn index(self) -> usize {
        match self {
            Self::Control => 0,
            Self::Command => 1,
            Self::Snapshot => 2,
            Self::Bulk => 3,
        }
    }

    pub(super) const fn from_index(index: usize) -> Self {
        match index {
            0 => Self::Control,
            1 => Self::Command,
            2 => Self::Snapshot,
            3 => Self::Bulk,
            _ => unreachable!(),
        }
    }
}

/// Stable identity assigned when work is admitted.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct WorkId(NonZeroU64);

impl WorkId {
    pub(super) const fn new(value: NonZeroU64) -> Self {
        Self(value)
    }

    /// Returns the nonzero numeric identity.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

/// Stable identity of one ready-set pass.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PassId(NonZeroU64);

impl PassId {
    pub(super) const fn new(value: NonZeroU64) -> Self {
        Self(value)
    }

    /// Returns the nonzero numeric identity.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

/// Stable identity of one in-flight dispatch.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DispatchId(NonZeroU64);

impl DispatchId {
    pub(super) const fn new(value: NonZeroU64) -> Self {
        Self(value)
    }

    /// Returns the nonzero numeric identity.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}
