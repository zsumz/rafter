use super::super::*;
use rafter::{Message, PreVoteResponse};

pub(super) fn pre_vote_grant(voter: u64, proposed_term: Term) -> RaftInput {
    RaftInput::Message {
        from: RaftNodeId(voter),
        message: Message::PreVoteResponse(PreVoteResponse {
            term: proposed_term,
            voter_id: RaftNodeId(voter),
            vote_granted: true,
        }),
    }
}

pub(super) fn committed_append_entries_input() -> RaftInput {
    RaftInput::Message {
        from: RaftNodeId(1),
        message: Message::AppendEntries(AppendEntries {
            sequence: 0,
            term: Term(2),
            leader_id: RaftNodeId(1),
            prev_log_index: LogIndex::ZERO,
            prev_log_term: Term::default(),
            entries: vec![LogEntry::application(Term(2), b"committed".to_vec())].into(),
            leader_commit: LogIndex(1),
        }),
    }
}
