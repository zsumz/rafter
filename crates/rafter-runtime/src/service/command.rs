//! Immutable provenance passed to the embedding's application adapter.

use rafter::{LocalProposalId, LogIndex, SharedPayload, Term};

/// One committed command and its preceding protocol-only log range.
///
/// The adapter must preserve both indexes in its [`super::super::application::ApplicationEntry`],
/// execute the command exactly once, and conservatively account for all entry
/// and outcome allocations. Copying a shared payload's visible bytes avoids
/// retaining an unseen larger backing frame. The driver verifies indexes, not
/// application semantics or byte estimates.
#[derive(Debug)]
pub struct CommittedCommand {
    /// First covered position, including any preceding protocol-only entries.
    pub first_index: LogIndex,
    /// Committed command's position and required durable completion floor.
    pub index: LogIndex,
    /// Committed command's term.
    pub term: Term,
    /// Opaque application bytes, possibly a view into a larger allocation.
    pub payload: SharedPayload,
    /// Volatile local correlation; absent on replay and remote commands.
    pub local_proposal_id: Option<LocalProposalId>,
}
