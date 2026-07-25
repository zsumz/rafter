use rafter::{LogIndex, Term};
use rafter_app::{proposal::ClientRequestId, read::ReadProof};

/// Options for a managed write.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WriteOptions {
    pub client_request_id: Option<ClientRequestId>,
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
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WriteBatchEntry<C> {
    pub command: C,
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
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WriteReceipt<R = ()> {
    pub index: LogIndex,
    pub term: Term,
    pub result: R,
}

/// Receipt returned by a managed read.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryReceipt<G, R = ()> {
    pub result: R,
    pub proof: Option<ReadProof<G>>,
}
