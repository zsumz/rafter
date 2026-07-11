//! Immutable, allocation-sharing `AppendEntries` payloads.
//!
//! Sharing is an in-process optimization only. Peers still observe normal
//! ordered log-entry slices, and codecs encode ordinary entries.

use std::{fmt, ops::Deref, sync::Arc};

use super::entry::LogEntry;

/// Immutable append-entry payload shared across `AppendEntries` messages.
///
/// This is local process ownership, not a wire-format distinction: each peer
/// still receives a normal Raft `AppendEntries` frame. Sharing only prevents a
/// leader from rebuilding the same bounded log slice for every follower in one
/// deterministic broadcast round.
#[derive(Clone, Eq, Hash, PartialEq)]
pub struct SharedEntries(SharedEntriesInner);

#[derive(Clone, Eq, Hash, PartialEq)]
enum SharedEntriesInner {
    Empty,
    Shared(Arc<[LogEntry]>),
}

impl SharedEntries {
    /// Returns an empty append-entry slice.
    #[must_use]
    pub fn empty() -> Self {
        Self(SharedEntriesInner::Empty)
    }

    /// Returns the entries as an immutable slice.
    #[must_use]
    pub fn as_slice(&self) -> &[LogEntry] {
        match &self.0 {
            SharedEntriesInner::Empty => &[],
            SharedEntriesInner::Shared(entries) => entries,
        }
    }

    /// Returns the number of entries.
    #[must_use]
    pub fn len(&self) -> usize {
        self.as_slice().len()
    }

    /// Returns whether this batch carries no entries.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.as_slice().is_empty()
    }

    /// Returns an iterator over the entries.
    pub fn iter(&self) -> std::slice::Iter<'_, LogEntry> {
        self.as_slice().iter()
    }

    /// Whether two shared entry slices use the same storage.
    ///
    /// Empty batches return true for each other because they intentionally
    /// carry no heap allocation.
    #[must_use]
    pub fn shares_allocation(&self, other: &Self) -> bool {
        match (&self.0, &other.0) {
            (SharedEntriesInner::Empty, SharedEntriesInner::Empty) => true,
            (SharedEntriesInner::Shared(left), SharedEntriesInner::Shared(right)) => {
                Arc::ptr_eq(left, right)
            }
            (SharedEntriesInner::Empty, SharedEntriesInner::Shared(_))
            | (SharedEntriesInner::Shared(_), SharedEntriesInner::Empty) => false,
        }
    }

    /// Materializes an owned vector of entries for code that must mutate or
    /// take ownership of individual entries.
    #[must_use]
    pub fn to_vec(&self) -> Vec<LogEntry> {
        self.as_slice().to_vec()
    }
}

impl Default for SharedEntries {
    fn default() -> Self {
        Self::empty()
    }
}

impl Deref for SharedEntries {
    type Target = [LogEntry];

    fn deref(&self) -> &[LogEntry] {
        self.as_slice()
    }
}

impl AsRef<[LogEntry]> for SharedEntries {
    fn as_ref(&self) -> &[LogEntry] {
        self.as_slice()
    }
}

impl From<Vec<LogEntry>> for SharedEntries {
    fn from(entries: Vec<LogEntry>) -> Self {
        if entries.is_empty() {
            Self::empty()
        } else {
            Self(SharedEntriesInner::Shared(entries.into()))
        }
    }
}

impl FromIterator<LogEntry> for SharedEntries {
    fn from_iter<T: IntoIterator<Item = LogEntry>>(iter: T) -> Self {
        iter.into_iter().collect::<Vec<_>>().into()
    }
}

impl<'a> IntoIterator for &'a SharedEntries {
    type Item = &'a LogEntry;
    type IntoIter = std::slice::Iter<'a, LogEntry>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl fmt::Debug for SharedEntries {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_slice().fmt(formatter)
    }
}
