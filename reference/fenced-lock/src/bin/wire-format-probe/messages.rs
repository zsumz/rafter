//! Public message values shared by independently compiled release consumers.

mod snapshot;

use rafter::{
    AppendEntries, AppendEntriesResponse, ConfigurationEntry, ConfigurationId, JointMembership,
    LogEntry, LogIndex, MembershipConfig, MembershipSet, Message, NodeId, PreVote, PreVoteResponse,
    RequestVote, RequestVoteResponse, Term, TimeoutNow,
};

pub(super) fn cases() -> Vec<(String, Message)> {
    let mut cases = vec![
        (
            "request-vote-zero".into(),
            Message::RequestVote(RequestVote {
                term: Term(0),
                candidate_id: NodeId(1),
                last_log_index: LogIndex(0),
                last_log_term: Term(0),
            }),
        ),
        (
            "request-vote-wide".into(),
            Message::RequestVote(RequestVote {
                term: Term(u64::MAX),
                candidate_id: NodeId(1 << 48),
                last_log_index: LogIndex(u64::MAX - 1),
                last_log_term: Term(u64::MAX - 2),
            }),
        ),
        (
            "pre-vote".into(),
            Message::PreVote(PreVote {
                term: Term(21),
                candidate_id: NodeId(3),
                last_log_index: LogIndex(900),
                last_log_term: Term(19),
            }),
        ),
        ("heartbeat".into(), append(0, Vec::new())),
        ("append-mixed".into(), append(1 << 48, entries())),
        (
            "timeout-now".into(),
            Message::TimeoutNow(TimeoutNow {
                term: Term(21),
                leader_id: NodeId(2),
            }),
        ),
    ];
    for granted in [false, true] {
        cases.push((
            format!("vote-response-{granted}"),
            Message::RequestVoteResponse(RequestVoteResponse {
                term: Term(21),
                voter_id: NodeId(3),
                vote_granted: granted,
            }),
        ));
        cases.push((
            format!("pre-vote-response-{granted}"),
            Message::PreVoteResponse(PreVoteResponse {
                term: Term(22),
                voter_id: NodeId(4),
                vote_granted: granted,
            }),
        ));
        cases.push((
            format!("append-response-{granted}"),
            Message::AppendEntriesResponse(AppendEntriesResponse {
                sequence: 1 << 48,
                term: Term(21),
                follower_id: NodeId(3),
                success: granted,
                match_index: LogIndex(902),
            }),
        ));
    }
    cases.extend(snapshot::cases());
    cases
}

fn append(sequence: u64, entries: Vec<LogEntry>) -> Message {
    Message::AppendEntries(AppendEntries {
        sequence,
        term: Term(21),
        leader_id: NodeId(2),
        prev_log_index: LogIndex(900),
        prev_log_term: Term(19),
        entries: entries.into(),
        leader_commit: LogIndex(901),
    })
}

fn entries() -> Vec<LogEntry> {
    vec![
        LogEntry::application(Term(20), Vec::new()),
        LogEntry::application(Term(20), vec![0, 255, 128, 17, 1]),
        LogEntry::application(Term(21), vec![0xa7; 65_536]),
        LogEntry::noop(Term(21)),
        LogEntry::configuration(
            Term(21),
            ConfigurationEntry::stable(ConfigurationId(8), stable()),
        ),
        LogEntry::configuration(
            Term(21),
            ConfigurationEntry::joint(ConfigurationId(9), joint()),
        ),
    ]
}

fn stable() -> MembershipSet {
    MembershipSet::new(vec![NodeId(1), NodeId(2), NodeId(3)], vec![NodeId(4)]).unwrap()
}

fn joint() -> JointMembership {
    JointMembership::new(
        stable(),
        MembershipSet::new(vec![NodeId(1), NodeId(3), NodeId(4)], vec![NodeId(2)]).unwrap(),
    )
}

pub(super) fn membership_cases() -> [Option<MembershipConfig>; 3] {
    [
        None,
        Some(MembershipConfig::Stable(stable())),
        Some(MembershipConfig::Joint(joint())),
    ]
}

pub(super) fn whole_snapshot() -> Message {
    snapshot::whole_snapshot()
}

// Exhaustiveness keeps new kernel message variants visible to this independent
// consumer. Tag values are the public RFPM v1 grammar, not codec internals.
pub(super) fn tag(message: &Message) -> u8 {
    match message {
        Message::RequestVote(_) => 1,
        Message::RequestVoteResponse(_) => 2,
        Message::AppendEntries(_) => 3,
        Message::AppendEntriesResponse(_) => 4,
        Message::InstallSnapshot(_) => panic!("whole snapshots have no peer representation"),
        Message::InstallSnapshotResponse(_) => 6,
        Message::InstallSnapshotChunk(_) => 7,
        Message::PreVote(_) => 8,
        Message::PreVoteResponse(_) => 9,
        Message::TimeoutNow(_) => 10,
    }
}
