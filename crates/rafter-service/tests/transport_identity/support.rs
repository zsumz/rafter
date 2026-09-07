//! The frame, the two client calls, and the refusal readers this suite reuses.
//!
//! Every scenario here asks the same three questions of a driver — will it take
//! a vote, will it take a write, will it take a read — and reads the answer as a
//! typed reason rather than a rendered string. Kept in one place so the suite
//! differs in which identity it is asking about, and not in how it asks.

use rafter_service::{
    AuthenticatedPeerEnvelope, DriverUnavailableReason, ReadOptions, WriteOptions,
};

use super::support::scripted::ScriptedDriver;
use super::support::transport::{Principal, GROUP};
use super::support::{
    LocalProposalId, LogIndex, Message, NodeId, ReadError, ReadId, RequestVote, Term, WriteError,
    WriteFate,
};

/// Where a replacement incarnation's runtime opens, for the cases that rebuild
/// one after a removal has already committed.
///
/// Above the position the retired incarnation reached, because that is the only
/// honest place for it: the committed membership at one log index is one set,
/// and a replacement claiming an older index for a newer membership would be
/// stating that everything the newer one names and the older did not had been
/// removed in between.
pub(super) const AFTER_THE_REMOVAL: LogIndex = LogIndex(7);

/// Whether one write refusal names this driver's own service state.
///
/// A typed variant and a typed reason, which is what changed: these refusals
/// used to ride `WriteError::Transport` with a crate-private cause, so a test —
/// like any external client — could only read them by rendering the error and
/// searching the string. Both facts a caller needs are now values: the reason,
/// and the fate, which is `NotAppended` from the variant alone.
pub(super) fn write_refused_for(error: &WriteError, expected: DriverUnavailableReason) -> bool {
    let WriteError::Unavailable { reason } = error else {
        return false;
    };
    *reason == expected && error.fate() == WriteFate::NotAppended
}

pub(super) fn read_refused_for(error: &ReadError, expected: DriverUnavailableReason) -> bool {
    matches!(error, ReadError::Unavailable { reason } if *reason == expected)
}

pub(super) fn a_write(driver: &ScriptedDriver) -> Result<LocalProposalId, WriteError> {
    driver
        .begin_write(
            ("key".to_owned(), "value".to_owned()),
            WriteOptions::default(),
        )
        .map(|(local_proposal_id, _future)| local_proposal_id)
}

pub(super) fn a_read(driver: &ScriptedDriver) -> Result<ReadId, ReadError> {
    driver
        .begin_read("key".to_owned(), ReadOptions::default())
        .map(|(read_id, _future)| read_id)
}

pub(super) fn a_vote(from: NodeId) -> AuthenticatedPeerEnvelope<u64, Principal> {
    AuthenticatedPeerEnvelope {
        group_id: GROUP,
        authenticated_peer: Principal::for_node(from),
        raft_from: from,
        raft_to: NodeId(1),
        message: Message::RequestVote(RequestVote {
            term: Term(1),
            candidate_id: from,
            last_log_index: LogIndex(5),
            last_log_term: Term(1),
        }),
    }
}
