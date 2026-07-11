use super::super::*;
use rafter::Message;

pub(super) fn bootstrap_state(
    current_term: Term,
    voted_for: Option<NodeId>,
    entries: &[(Term, &[u8])],
) -> BootstrapState {
    BootstrapState {
        current_term,
        voted_for,
        commit_index: LogIndex::ZERO,
        committed_configuration: None,
        snapshot: None,
        log: entries
            .iter()
            .enumerate()
            .map(|(offset, (term, payload))| {
                BootstrapLogEntry::application(
                    LogIndex(offset as u64 + 1),
                    *term,
                    (*payload).to_vec(),
                )
            })
            .collect(),
    }
}

pub(super) fn vote_response(from: NodeId, to: NodeId) -> impl FnMut(&Envelope) -> bool {
    move |envelope| {
        envelope.from == from
            && envelope.to == to
            && matches!(envelope.message, Message::RequestVoteResponse(_))
    }
}
