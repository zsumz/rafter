//! Election, current-term commitment, and read sequencing stories.

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
        (
            "Older-term quorum replication cannot directly commit",
            vec![ack(2, 2, 1, 1, true)],
        ),
        (
            "Current-term no-op commits the preceding write",
            vec![ack(2, 2, 2, 1, true)],
        ),
        (
            "Tracked writes enter together",
            vec![
                Input::TrackedClientProposal {
                    proposal_id: LocalProposalId(41),
                    payload: b"first".to_vec(),
                },
                Input::TrackedClientProposal {
                    proposal_id: LocalProposalId(42),
                    payload: b"second".to_vec(),
                },
            ],
        ),
        (
            "New matching prefix advances commitment",
            vec![ack(2, 2, 4, 1, true)],
        ),
        (
            "Stale acknowledgement cannot regress progress",
            vec![ack(2, 1, 1, 1, true)],
        ),
    ]);
    Scenario {
        name: "Election and committed write",
        explanation: "Follow [election](src/node/election.rs), then [acknowledgement](src/node/replication/response.rs) and [commit authorization](src/node/commit/advance.rs). A quorum-replicated older-term entry cannot advance commitment directly (CM-03); acknowledging the current-term no-op commits its prefix. The no-op advances the dispatch cursor without emitting an application command. See [commit_certificate_detects_prior_term_candidate_commit](../rafter-sim/src/model_check/invariants/tests/commit_history.rs) for the counterexample detector.",
        node, steps, expected: (Term(2), Role::Leader, LogIndex(4), LogIndex(4), 0),
    }
}

pub(super) fn higher_term_rejection() -> Scenario {
    Scenario {
        name: "A rejected vote still changes authority",
        explanation: "Follow [handle_request_vote](src/node/election.rs). Learning term 8 precedes testing the candidate's log. Rejecting the stale log must retain the newer term and clear the old vote (EL-01, EL-03). Moving term adoption below vote eligibility would allow subsequent term-7 messages to retain obsolete authority. The [higher-term durable vote rejection](../rafter-runtime/src/tests/hard_state/voting.rs) exercises this order through persistence.",
        node: Node::from_bootstrap(config(1, &[2, 3]), BootstrapState {
            current_term: Term(7), voted_for: Some(NodeId(3)), commit_index: LogIndex::ZERO,
            committed_configuration: None, snapshot: None,
            log: vec![BootstrapLogEntry::application(LogIndex(1), Term(7), b"local".to_vec())],
        }).unwrap(),
        steps: vec![
            ("Higher-term candidate has an older log", vec![message(2, Message::RequestVote(RequestVote {
                term: Term(8), candidate_id: NodeId(2), last_log_index: LogIndex::ZERO,
                last_log_term: Term::default(),
            }))]),
            ("An older term cannot regain authority", vec![message(3, Message::RequestVote(RequestVote {
                term: Term(7), candidate_id: NodeId(3), last_log_index: LogIndex(1),
                last_log_term: Term(7),
            }))]),
        ],
        expected: (Term(8), Role::Follower, LogIndex::ZERO, LogIndex(1), 0),
    }
}

pub(super) fn read_barrier() -> Scenario {
    let mut steps = campaign(1);
    steps.extend([
        ("Commit the leader's no-op", vec![ack(2, 1, 1, 1, true)]),
        (
            "Two reads register together",
            vec![
                Input::ReadIndex { read_id: ReadId(1) },
                Input::ReadIndex { read_id: ReadId(2) },
            ],
        ),
        (
            "A pre-registration echo cannot confirm either read",
            vec![ack(2, 1, 1, 1, true)],
        ),
        (
            "A fresh rejection confirms contact, without new replication",
            vec![ack(2, 1, 0, 2, false)],
        ),
        (
            "A later read must obtain its own confirmation",
            vec![Input::ReadIndex { read_id: ReadId(3) }],
        ),
        (
            "Higher term cancels the pending read",
            vec![ack(2, 2, 1, 4, true)],
        ),
    ]);
    Scenario {
        name: "Read barriers and acknowledgement meanings",
        explanation: "Follow [response handling](src/node/replication/response.rs) and [read barriers](src/node/read_index.rs). Same-term contact and sequence-qualified read confirmation happen before the success branch; a rejection can confirm authority while leaving the matching prefix unchanged. An old sequence cannot grant a newly registered read (RD-02). See [delayed_ack_from_an_older_round_never_confirms_a_barrier](src/node/tests/read/barrier.rs) for the existing guard evidence. Application reads still wait for execution of application entries through the barrier; a no-op has no callback to wait for.",
        node: Node::new(config(1, &[2, 3])), steps,
        expected: (Term(2), Role::Follower, LogIndex(1), LogIndex(1), 0),
    }
}
