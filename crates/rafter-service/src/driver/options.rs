use rafter::{LogIndex, Term};
use rafter_app::{proposal::ClientRequestId, read::ReadProof};

/// Options for a managed write.
///
/// [`WriteOptions::with_client_request_id`] is the way to build one, matching
/// [`ReadOptions`], its documented pair. Both are `#[non_exhaustive]`, so a
/// field added here is additive for every caller that builds through the
/// setter.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub struct WriteOptions {
    /// The caller's own name for this command, carried through unchanged.
    ///
    /// Rafter neither generates these nor deduplicates on them: the ID travels
    /// with the proposal and comes back on
    /// [`crate::WriteError::UnknownOutcome`], which is what makes an unresolved
    /// write retryable under the same identity. Idempotency itself is the state
    /// machine's obligation — a command retried under the same ID may be applied
    /// twice unless the state machine refuses the duplicate.
    ///
    /// `None` means the caller has nothing to correlate, so an unknown outcome
    /// reports the driver's `LocalProposalId` and nothing else.
    pub client_request_id: Option<ClientRequestId>,
}

impl WriteOptions {
    /// Carries the caller's own name for this command through the write.
    ///
    /// A setter rather than struct-update syntax, for the reason
    /// [`ReadOptions::with_min_applied_index`] is one: the type is
    /// `#[non_exhaustive]`, so an embedder outside this crate cannot name every
    /// field, and a later field must not break their construction.
    #[must_use]
    pub const fn with_client_request_id(mut self, client_request_id: ClientRequestId) -> Self {
        self.client_request_id = Some(client_request_id);
        self
    }
}

/// Options for a managed read.
///
/// The read counterpart of [`WriteOptions`], and the reason reads stopped being
/// the asymmetric half of the pair: a caller that just observed a write had no
/// way to say "at least as fresh as that", and no workaround, because freshness
/// is a property of the barrier and cannot be applied after it is granted.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub struct ReadOptions {
    /// The applied index this read must observe, if the caller has one.
    ///
    /// Honored verbatim by the app layer: it is not capped at the read index,
    /// not lowered, and not snapped to an application entry. The natural source
    /// is the `index` of a [`WriteReceipt`] the caller already holds, which
    /// always names an application entry. An index taken from a commit index, a
    /// read index, or a snapshot boundary may name an entry the state machine
    /// is never told about and will stall forever; convert such an index with
    /// [`crate::TransportRaftDriver::committed_application_index`] first.
    pub min_applied_index: Option<LogIndex>,
}

impl ReadOptions {
    /// Requires the read to observe at least `min_applied_index`.
    ///
    /// A setter rather than struct-update syntax, because the type is
    /// `#[non_exhaustive]`: an embedder outside this crate cannot name every
    /// field, and a later field must not break their construction.
    #[must_use]
    pub const fn with_min_applied_index(mut self, min_applied_index: LogIndex) -> Self {
        self.min_applied_index = Some(min_applied_index);
        self
    }
}

/// One command in an explicit managed write batch.
///
/// Each entry carries its own options and receives its own result, because a
/// batch shares one proposing step but not one fate: the entries land at
/// different indices, and a failure part-way through leaves earlier entries
/// appended and later ones not.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WriteBatchEntry<C> {
    /// The command to propose.
    pub command: C,
    /// This entry's own write options; see [`WriteOptions`].
    pub options: WriteOptions,
}

impl<C> WriteBatchEntry<C> {
    /// Creates a batch entry with default write options.
    #[must_use]
    pub fn new(command: C) -> Self {
        Self {
            command,
            options: WriteOptions::default(),
        }
    }

    /// Creates a batch entry with caller-supplied write options.
    #[must_use]
    pub fn with_options(command: C, options: WriteOptions) -> Self {
        Self { command, options }
    }
}

/// Receipt returned only after the proposed command has committed and applied.
///
/// Holding one is the proof that the command is durable and visible: the
/// managed layer builds it from an apply event, never from a local append.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WriteReceipt<R = ()> {
    /// The log index the command committed at.
    ///
    /// It names an application entry, which makes it the one index safe to feed
    /// back as [`ReadOptions::min_applied_index`] for a read-your-writes read.
    pub index: LogIndex,
    /// The term the command committed in.
    ///
    /// Together with [`WriteReceipt::index`] this identifies the entry
    /// uniquely; an index alone does not, because an uncommitted entry at that
    /// index may be overwritten by a later leader.
    pub term: Term,
    /// Whatever the state machine returned from applying the command.
    pub result: R,
}

/// Receipt returned by a managed read.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryReceipt<G, R = ()> {
    /// Whatever the state machine returned from running the query.
    pub result: R,
    /// The freshness proof, when the read consistency produced one.
    ///
    /// `Some` for a linearizable read: the barrier's quorum round certified a
    /// read index, and the proof reports it alongside the applied floor the
    /// state machine had to reach. `None` for a local read, which submits no
    /// read-index round and therefore proves nothing about other replicas — the
    /// absence is the honest answer, not a missing value.
    ///
    /// A caller that requires linearizability must ask for it through
    /// [`rafter_app::read::ReadConsistency`] rather than inspecting this field
    /// afterwards; a `None` here means the read it already ran was local.
    pub proof: Option<ReadProof<G>>,
}
