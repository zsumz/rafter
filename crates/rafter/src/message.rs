use std::{fmt, ops::Deref, sync::Arc};

use crate::{
    ConfigurationEntry, LogEntryKind, LogIndex, NodeId, RaftSnapshotMetadata, SharedPayload,
    SnapshotChunkRequest, SnapshotChunkSend, SnapshotChunkSource, SnapshotTransferId, Term,
};

const APPLICATION_LOG_ENTRY_REPLICATION_OVERHEAD_BYTES: usize = 64;
const NOOP_LOG_ENTRY_REPLICATION_BYTES: usize = 16;
// Conservative upper bound on the wire encoding of a configuration entry:
// a fixed header plus a per-member cost across every voter and learner in
// both joint halves. Pinned as an upper bound of the real encoding by
// rafter-codec's configuration_entry_size_accounting_is_upper_bound test.
const CONFIGURATION_LOG_ENTRY_BASE_BYTES: usize = 64;
const CONFIGURATION_LOG_ENTRY_PER_MEMBER_BYTES: usize = 12;

/// Raft protocol message exchanged between nodes.
///
/// This enum is exhaustive because the protocol message vocabulary is closed
/// over these request and response frames.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum Message {
    AppendEntries(AppendEntries),
    AppendEntriesResponse(AppendEntriesResponse),
    InstallSnapshot(InstallSnapshot),
    InstallSnapshotChunk(InstallSnapshotChunk),
    InstallSnapshotResponse(InstallSnapshotResponse),
    PreVote(PreVote),
    PreVoteResponse(PreVoteResponse),
    TimeoutNow(TimeoutNow),
    RequestVote(RequestVote),
    RequestVoteResponse(RequestVoteResponse),
}

/// One Raft log entry: term plus logical entry kind.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct LogEntry {
    pub term: Term,
    pub kind: LogEntryKind,
}

impl LogEntry {
    /// Builds an application log entry.
    #[must_use]
    pub fn application<P>(term: Term, payload: P) -> Self
    where
        P: Into<SharedPayload>,
    {
        Self {
            term,
            kind: LogEntryKind::application(payload),
        }
    }

    /// Builds a configuration log entry.
    #[must_use]
    pub fn configuration(term: Term, configuration: ConfigurationEntry) -> Self {
        Self {
            term,
            kind: LogEntryKind::configuration(configuration),
        }
    }

    /// Builds a leadership no-op log entry.
    #[must_use]
    pub const fn noop(term: Term) -> Self {
        Self {
            term,
            kind: LogEntryKind::noop(),
        }
    }

    /// Returns the application payload when this is an application entry.
    #[must_use]
    pub fn application_payload(&self) -> Option<&[u8]> {
        self.kind.application_payload()
    }

    /// Returns the configuration payload when this is a configuration entry.
    #[must_use]
    pub fn configuration_entry(&self) -> Option<&ConfigurationEntry> {
        self.kind.configuration_entry()
    }

    #[must_use]
    pub(crate) fn application_replication_bytes(payload_len: usize) -> usize {
        payload_len.saturating_add(APPLICATION_LOG_ENTRY_REPLICATION_OVERHEAD_BYTES)
    }

    #[must_use]
    pub(crate) fn max_application_payload_len(max_replication_bytes: usize) -> usize {
        max_replication_bytes.saturating_sub(APPLICATION_LOG_ENTRY_REPLICATION_OVERHEAD_BYTES)
    }

    /// Size this entry contributes to an append-entries batch budget: an
    /// upper bound of its wire encoding, so transports can derive frame
    /// limits from the configured budget.
    #[must_use]
    pub fn replication_bytes(&self) -> usize {
        match &self.kind {
            LogEntryKind::Application(payload) => {
                Self::application_replication_bytes(payload.len())
            }
            LogEntryKind::Configuration(entry) => {
                let members = match entry.membership_config() {
                    crate::MembershipConfig::Stable(membership) => {
                        membership.voters().len() + membership.learners().len()
                    }
                    crate::MembershipConfig::Joint(joint) => {
                        joint.old().voters().len()
                            + joint.old().learners().len()
                            + joint.new_membership().voters().len()
                            + joint.new_membership().learners().len()
                    }
                };
                CONFIGURATION_LOG_ENTRY_BASE_BYTES.saturating_add(
                    CONFIGURATION_LOG_ENTRY_PER_MEMBER_BYTES.saturating_mul(members),
                )
            }
            LogEntryKind::Noop => NOOP_LOG_ENTRY_REPLICATION_BYTES,
        }
    }
}

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

#[cfg(test)]
mod tests {
    use super::{LogEntry, SharedEntries};
    use crate::Term;

    #[test]
    fn empty_shared_entries_have_zero_storage_semantics() {
        let first = SharedEntries::empty();
        let second = SharedEntries::default();
        let from_empty_vec = Vec::<LogEntry>::new().into();

        assert!(first.is_empty());
        assert_eq!(first.as_slice(), &[]);
        assert_eq!(first.len(), 0);
        assert!(first.shares_allocation(&second));
        assert!(second.shares_allocation(&from_empty_vec));
        assert_eq!(first.to_vec(), Vec::<LogEntry>::new());
    }

    #[test]
    fn non_empty_shared_entries_still_share_one_allocation() {
        let entries: SharedEntries = vec![LogEntry::noop(Term(1))].into();
        let clone = entries.clone();
        let empty = SharedEntries::empty();

        assert!(!entries.is_empty());
        assert!(entries.shares_allocation(&clone));
        assert!(!entries.shares_allocation(&empty));
    }
}

/// `AppendEntries` request carrying heartbeats, replication batches, or both.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct AppendEntries {
    pub term: Term,
    pub leader_id: NodeId,
    pub prev_log_index: LogIndex,
    pub prev_log_term: Term,
    /// Heartbeat round this append belongs to; echoed by the response so
    /// leaders can order acknowledgements against read-index registrations.
    /// Zero means unknown and by construction never satisfies a read barrier.
    pub sequence: u64,
    pub entries: SharedEntries,
    pub leader_commit: LogIndex,
}

/// Response to an [`AppendEntries`] request.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct AppendEntriesResponse {
    pub term: Term,
    pub follower_id: NodeId,
    pub success: bool,
    pub match_index: LogIndex,
    /// Echo of the request's heartbeat round; zero is an unknown sequence echo.
    pub sequence: u64,
}

/// A whole snapshot in one message: metadata plus the complete payload.
///
/// The kernel never sends this — leaders stream
/// [`InstallSnapshotChunk`] messages — and `rafter-codec` does not encode it
/// in the current peer wire format. Direct kernel embeddings may still submit
/// it when they intentionally carry a complete snapshot payload in memory.
/// Payload bytes in a message are transient: an accepted whole snapshot is
/// handed to the receiver's store as a single staged chunk, never retained in
/// kernel state.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct InstallSnapshot {
    pub term: Term,
    pub leader_id: NodeId,
    pub metadata: RaftSnapshotMetadata,
    pub application_payload: Vec<u8>,
}

/// One chunk of an install-snapshot transfer.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct InstallSnapshotChunk {
    pub term: Term,
    pub leader_id: NodeId,
    pub transfer_id: SnapshotTransferId,
    pub metadata: RaftSnapshotMetadata,
    pub total_payload_len: u64,
    pub application_payload_crc32: u32,
    pub offset: u64,
    pub chunk: Vec<u8>,
    pub done: bool,
}

impl SnapshotChunkSend {
    /// Materializes the wire message for this directive by reading the chunk
    /// bytes from `source`.
    ///
    /// Returns `None` when the source cannot serve the snapshot or returns a
    /// chunk of the wrong length; the caller drops the directive like a lost
    /// message and the transfer resumes from the follower's acknowledged
    /// offset.
    #[must_use]
    pub fn resolve<S: SnapshotChunkSource + ?Sized>(
        &self,
        source: &S,
    ) -> Option<InstallSnapshotChunk> {
        let chunk = source.snapshot_chunk(SnapshotChunkRequest {
            transfer_id: self.transfer_id,
            metadata: &self.metadata,
            total_payload_len: self.total_payload_len,
            application_payload_crc32: self.application_payload_crc32,
            offset: self.offset,
            len: self.len,
        })?;
        if chunk.len() != self.len as usize {
            return None;
        }
        Some(InstallSnapshotChunk {
            term: self.term,
            leader_id: self.leader_id,
            transfer_id: self.transfer_id,
            metadata: self.metadata.clone(),
            total_payload_len: self.total_payload_len,
            application_payload_crc32: self.application_payload_crc32,
            offset: self.offset,
            chunk,
            done: self.done,
        })
    }
}

/// Response to an install-snapshot message or chunk.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct InstallSnapshotResponse {
    pub term: Term,
    pub follower_id: NodeId,
    pub success: bool,
    pub last_included_index: LogIndex,
    pub transfer_id: Option<SnapshotTransferId>,
    pub next_offset: u64,
}

/// Pre-vote poll preceding a real election (thesis 4.2.3 / 9.6).
///
/// `term` is the PROPOSED term (the sender's current term + 1), not a term
/// the sender holds. Granting a pre-vote never mutates the granter's term or
/// `voted_for`, and pre-vote grants are never persisted, so multiple pre-vote
/// grants in one term are allowed by design: a pre-vote is a non-binding poll
/// of whether a real election could be won.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct PreVote {
    pub term: Term,
    pub candidate_id: NodeId,
    pub last_log_index: LogIndex,
    pub last_log_term: Term,
}

/// Response to a [`PreVote`] poll; `term` echoes the proposed request term.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct PreVoteResponse {
    pub term: Term,
    pub voter_id: NodeId,
    pub vote_granted: bool,
}

/// Instructs the recipient to start an election immediately, bypassing
/// pre-vote and leader stickiness; sent by a leader completing a leadership
/// transfer (thesis 3.10).
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct TimeoutNow {
    pub term: Term,
    pub leader_id: NodeId,
}

/// Real `RequestVote` election request.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct RequestVote {
    pub term: Term,
    pub candidate_id: NodeId,
    pub last_log_index: LogIndex,
    pub last_log_term: Term,
}

/// Response to a [`RequestVote`] election request.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct RequestVoteResponse {
    pub term: Term,
    pub voter_id: NodeId,
    pub vote_granted: bool,
}
