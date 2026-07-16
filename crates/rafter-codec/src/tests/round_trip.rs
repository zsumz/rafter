//! Round-trip coverage for every encodable top-level and log-entry shape.

use rafter::{
    AppendEntries, AppendEntriesResponse, LogEntry, LogIndex, Message, NodeId, PreVote,
    PreVoteResponse, RequestVote, RequestVoteResponse, Term, TimeoutNow,
};

use super::support::{
    append_entries_with, joint_configuration_entry, round_trip, stable_configuration_entry,
};

#[test]
fn request_vote_round_trips() {
    round_trip(Message::RequestVote(RequestVote {
        term: Term(7),
        candidate_id: NodeId(2),
        last_log_index: LogIndex(55),
        last_log_term: Term(6),
    }));
}

#[test]
fn request_vote_response_round_trips() {
    round_trip(Message::RequestVoteResponse(RequestVoteResponse {
        term: Term(7),
        voter_id: NodeId(3),
        vote_granted: true,
    }));
}

#[test]
fn pre_vote_round_trips() {
    round_trip(Message::PreVote(PreVote {
        term: Term(8),
        candidate_id: NodeId(2),
        last_log_index: LogIndex(55),
        last_log_term: Term(6),
    }));
}

#[test]
fn pre_vote_response_round_trips() {
    round_trip(Message::PreVoteResponse(PreVoteResponse {
        term: Term(8),
        voter_id: NodeId(3),
        vote_granted: true,
    }));
}

#[test]
fn timeout_now_round_trips() {
    round_trip(Message::TimeoutNow(TimeoutNow {
        term: Term(9),
        leader_id: NodeId(4),
    }));
}

#[test]
fn append_entries_round_trips_with_empty_batch() {
    round_trip(append_entries_with(Vec::new()));
}

#[test]
fn append_entries_round_trips_with_opaque_payloads() {
    round_trip(append_entries_with(vec![
        LogEntry::application(Term(8), b"stream command bytes".to_vec()),
        LogEntry::application(Term(8), vec![0, 159, 146, 150, 255]),
    ]));
}

#[test]
fn append_entries_round_trips_with_configuration_entries() {
    round_trip(append_entries_with(vec![
        stable_configuration_entry(),
        joint_configuration_entry(),
    ]));
}

#[test]
fn append_entries_round_trips_with_noop_entries() {
    round_trip(append_entries_with(vec![LogEntry::noop(Term(8))]));
}

#[test]
fn append_entries_response_round_trips() {
    round_trip(Message::AppendEntriesResponse(AppendEntriesResponse {
        sequence: 0,
        term: Term(8),
        follower_id: NodeId(2),
        success: false,
        match_index: LogIndex(10),
    }));
}

#[test]
fn append_fixture_field_order_remains_constructible() {
    round_trip(Message::AppendEntries(AppendEntries {
        sequence: 7,
        term: Term(8),
        leader_id: NodeId(1),
        prev_log_index: LogIndex(10),
        prev_log_term: Term(7),
        entries: Vec::new().into(),
        leader_commit: LogIndex(11),
    }));
}
