//! Version-1 persisted-log-entry value types.
//!
//! These types name one durable log entry, owned or borrowed, and the
//! constructors replay and append share. Envelope bytes live in the codec.

use rafter::{ConfigurationEntry, LogEntryKind, LogIndex, Term};

/// One Raft log entry after assigning its durable log index.
///
/// `kind` carries either an opaque application payload or a stable/joint
/// configuration entry for membership changes. The codec preserves that
/// distinction so replay can rebuild both application history and Raft
/// membership state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PersistedRaftLogEntry {
    /// Durable Raft log index assigned by the log segment.
    pub index: LogIndex,
    /// Raft term stored with the entry.
    pub term: Term,
    /// Application payload or configuration-membership entry.
    pub kind: LogEntryKind,
}

impl PersistedRaftLogEntry {
    /// Builds one persisted application log entry.
    #[must_use]
    pub fn application(index: LogIndex, term: Term, payload: Vec<u8>) -> Self {
        Self {
            index,
            term,
            kind: LogEntryKind::application(payload),
        }
    }

    /// Builds one persisted membership-configuration log entry.
    #[must_use]
    pub fn configuration(index: LogIndex, term: Term, configuration: ConfigurationEntry) -> Self {
        Self {
            index,
            term,
            kind: LogEntryKind::configuration(configuration),
        }
    }

    /// Builds one persisted no-op log entry.
    #[must_use]
    pub const fn noop(index: LogIndex, term: Term) -> Self {
        Self {
            index,
            term,
            kind: LogEntryKind::noop(),
        }
    }

    /// Returns the application payload when this entry carries application
    /// data.
    #[must_use]
    pub fn application_payload(&self) -> Option<&[u8]> {
        self.kind.application_payload()
    }

    /// Returns the configuration entry when this entry carries a membership
    /// change.
    #[must_use]
    pub fn configuration_entry(&self) -> Option<&ConfigurationEntry> {
        self.kind.configuration_entry()
    }
}

/// Borrowed persisted log entry used when a caller already owns kernel log
/// entries and only needs to stamp durable indexes while encoding them.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BorrowedPersistedRaftLogEntry<'a> {
    /// Durable Raft log index assigned by the log segment.
    pub index: LogIndex,
    /// Raft term stored with the entry.
    pub term: Term,
    /// Application payload or configuration-membership entry.
    pub kind: &'a LogEntryKind,
}

impl<'a> BorrowedPersistedRaftLogEntry<'a> {
    /// Builds a borrowed persisted log entry view.
    #[must_use]
    pub const fn new(index: LogIndex, term: Term, kind: &'a LogEntryKind) -> Self {
        Self { index, term, kind }
    }
}

impl<'a> From<&'a PersistedRaftLogEntry> for BorrowedPersistedRaftLogEntry<'a> {
    fn from(entry: &'a PersistedRaftLogEntry) -> Self {
        Self::new(entry.index, entry.term, &entry.kind)
    }
}

impl From<BorrowedPersistedRaftLogEntry<'_>> for PersistedRaftLogEntry {
    fn from(entry: BorrowedPersistedRaftLogEntry<'_>) -> Self {
        Self {
            index: entry.index,
            term: entry.term,
            kind: entry.kind.clone(),
        }
    }
}
