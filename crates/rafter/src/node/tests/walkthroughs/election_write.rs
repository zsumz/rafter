//! Election, current-term commitment, and explicitly labeled guard probes.

use super::support::*;

pub(super) fn election_and_write() -> Scenario {
    let node = bootstrap(
        1,
        0,
        vec![BootstrapLogEntry::application(
            LogIndex(1),
            Term(1),
            b"older write".to_vec(),
        )],
    );
    let mut steps = campaign(2);
    steps.extend([
        Step::inputs(
            "Synthetic guard probe: an older-term quorum threshold",
            "This injected partial success is not a follower response to the emitted no-op frame, which ends at index 2. It isolates CM-03: even quorum replication of index 1 cannot directly commit that older-term entry.",
            vec![message(2, Message::AppendEntriesResponse(AppendEntriesResponse {
                follower_id: NodeId(2), term: Term(2), match_index: LogIndex(1), sequence: 1, success: true,
            }))],
        ),
        Step::replies(
            "Current-term no-op commits the preceding write",
            "This success echoes the actual no-op append. Its current-term entry authorizes commitment of the preceding write. The no-op advances dispatch without an application-command output.",
            vec![AppendReply::accepted(NodeId(2), LogIndex(2))],
        ),
        Step::inputs(
            "Two tracked writes enter the log",
            "Appending a local proposal only assigns its index. A later quorum acknowledgement permits commitment and command dispatch.",
            vec![
                Input::TrackedClientProposal { proposal_id: LocalProposalId(41), payload: b"first".to_vec() },
                Input::TrackedClientProposal { proposal_id: LocalProposalId(42), payload: b"second".to_vec() },
            ],
        ),
        Step::replies(
            "New matching prefix advances commitment",
            "The reply echoes the emitted frame that actually ends at index 4. Its sequence is selected from the traffic, so single steps and batching may echo different rounds.",
            vec![AppendReply::accepted(NodeId(2), LogIndex(4))],
        ),
        Step::inputs(
            "Injected stale response cannot regress progress",
            "This old-term response represents delayed traffic from before the recorded election. Term validation discards it before replication progress can change.",
            vec![message(2, Message::AppendEntriesResponse(AppendEntriesResponse {
                follower_id: NodeId(2), term: Term(1), match_index: LogIndex(1), sequence: 1, success: true,
            }))],
        ),
    ]);
    Scenario {
        name: "Election and committed write",
        explanation: "Follow [election](src/node/election.rs), [acknowledgements](src/node/replication/response.rs), and [commit authorization](src/node/commit/advance.rs). CM-03 separates a replicated threshold from permission to commit it. The [prior-term commit detector](../rafter-sim/src/model_check/invariants/tests/commit_history.rs) provides the counterexample evidence. The two injected guard probes below are labeled; the ordinary successes are correlated with emitted requests and also checked against a real follower.",
        node, steps, expected: (Term(2), Role::Leader, LogIndex(4), LogIndex(4), 0),
    }
}

pub(super) fn higher_term_rejection() -> Scenario {
    Scenario {
        name: "A rejected vote still changes authority",
        explanation: "Follow [handle_request_vote](src/node/election.rs). Term adoption precedes vote eligibility (EL-01, EL-03). The [durable vote rejection case](../rafter-runtime/src/tests/hard_state/voting.rs) checks the same order through persistence.",
        node: Node::from_bootstrap(config(1, &[2, 3]), BootstrapState {
            current_term: Term(7), voted_for: Some(NodeId(3)), commit_index: LogIndex::ZERO,
            committed_configuration: None, snapshot: None,
            log: vec![BootstrapLogEntry::application(LogIndex(1), Term(7), b"local".to_vec())],
        }).unwrap(),
        steps: vec![
            Step::inputs("A newer term, but an older log",
                "Learning term 8 happens before comparing logs. The rejection must carry term 8, and the term-7 vote must be cleared even though no new vote is granted.",
                vec![message(2, Message::RequestVote(RequestVote {
                    term: Term(8), candidate_id: NodeId(2), last_log_index: LogIndex::ZERO,
                    last_log_term: Term::default(),
                }))]),
            Step::inputs("An older term cannot regain authority",
                "The node remembers term 8 from the rejected request. Moving term adoption after log eligibility would incorrectly leave term-7 authority in place.",
                vec![message(3, Message::RequestVote(RequestVote {
                    term: Term(7), candidate_id: NodeId(3), last_log_index: LogIndex(1), last_log_term: Term(7),
                }))]),
        ],
        expected: (Term(8), Role::Follower, LogIndex::ZERO, LogIndex(1), 0),
    }
}
