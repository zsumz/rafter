use super::*;
use rafter::{Message, PreVoteResponse, RequestVote};

fn pre_vote_grant(voter: u64, proposed_term: Term) -> RaftInput {
    RaftInput::Message {
        from: RaftNodeId(voter),
        message: Message::PreVoteResponse(PreVoteResponse {
            term: proposed_term,
            voter_id: RaftNodeId(voter),
            vote_granted: true,
        }),
    }
}

#[test]
fn election_persists_term_and_vote_before_vote_requests_escape() {
    let mut runtime = durable_node(1, &[2, 3], InMemoryRaftHardStateStore::new());

    // The timeout opens a pre-vote poll, which persists nothing.
    let outputs = runtime.step(RaftInput::Tick).expect("pre-vote poll opens");
    assert_eq!(runtime.hard_state_store.current(), RaftHardState::default());
    assert_eq!(outputs.len(), 2);
    assert!(outputs.iter().all(|output| matches!(
        output,
        RaftOutput::Send {
            message: Message::PreVote(_),
            ..
        }
    )));

    // The granted poll starts the real election: term and vote are durable
    // before the vote requests escape.
    let outputs = runtime
        .step(pre_vote_grant(2, Term(1)))
        .expect("hard state writes");

    assert_eq!(
        runtime.hard_state_store.current(),
        RaftHardState {
            current_term: Term(1),
            voted_for: Some(RaftNodeId(1)),
            commit_index: LogIndex::ZERO,
            committed_configuration: None,
        }
    );
    assert_eq!(outputs.len(), 2);
    assert!(outputs.iter().all(|output| matches!(
        output,
        RaftOutput::Send {
            message: Message::RequestVote(_),
            ..
        }
    )));
}

#[test]
fn granted_vote_is_persisted_before_vote_response_escapes() {
    let mut runtime = durable_node(1, &[2, 3], InMemoryRaftHardStateStore::new());

    let outputs = runtime
        .step(RaftInput::Message {
            from: RaftNodeId(2),
            message: Message::RequestVote(RequestVote {
                term: Term(3),
                candidate_id: RaftNodeId(2),
                last_log_index: LogIndex::ZERO,
                last_log_term: Term::default(),
            }),
        })
        .expect("hard state writes");

    assert_eq!(
        runtime.hard_state_store.current(),
        RaftHardState {
            current_term: Term(3),
            voted_for: Some(RaftNodeId(2)),
            commit_index: LogIndex::ZERO,
            committed_configuration: None,
        }
    );
    assert!(matches!(
        outputs.as_slice(),
        [RaftOutput::Send {
            message: Message::RequestVoteResponse(response),
            ..
        }] if response.vote_granted
    ));
}

#[test]
fn hard_state_write_failure_suppresses_vote_requests() {
    let mut runtime = durable_node(
        1,
        &[2, 3],
        FailingHardStateStore {
            current: RaftHardState::default(),
        },
    );

    assert_eq!(runtime.role(), RaftRole::Follower);
    assert_eq!(runtime.current_term(), Term::default());
    assert_eq!(runtime.hard_state(), RaftHardState::default());

    // The pre-vote poll persists nothing, so the failing store is not
    // exercised until the granted poll starts the real election.
    let _ = runtime.step(RaftInput::Tick).expect("pre-vote poll opens");
    let error = runtime
        .step(pre_vote_grant(2, Term(1)))
        .expect_err("hard-state write fails");

    assert!(matches!(
        error,
        RaftRuntimeError::HardStateWrite(RaftHardStateStoreWriteError::Io {
            operation: "write test raft hard state",
            ..
        })
    ));
    // The vote requests were suppressed with the failed step's outputs;
    // poisoned accessors may run ahead of the durable hard state, and the
    // runtime refuses everything until restart.
    let error = runtime
        .step(RaftInput::Tick)
        .expect_err("a poisoned runtime refuses further inputs");
    assert!(matches!(error, RaftRuntimeError::Poisoned { .. }));
}

#[test]
fn hard_state_write_failure_poisons_runtime_until_restart() {
    let mut runtime = durable_node(
        1,
        &[2, 3],
        FailingHardStateStore {
            current: RaftHardState::default(),
        },
    );

    let _ = runtime.step(RaftInput::Tick).expect("pre-vote poll opens");
    let error = runtime
        .step(pre_vote_grant(2, Term(1)))
        .expect_err("hard-state write fails");
    assert!(matches!(error, RaftRuntimeError::HardStateWrite(_)));

    assert_poisoned_after_failure(&mut runtime, |cause| {
        matches!(cause, RaftRuntimeFatalError::HardStateWrite(_))
    });
}

#[test]
fn restarted_node_preserves_persisted_vote() {
    let mut runtime = durable_node(1, &[2, 3], InMemoryRaftHardStateStore::new());
    let _ = runtime
        .step(RaftInput::Message {
            from: RaftNodeId(2),
            message: Message::RequestVote(RequestVote {
                term: Term(3),
                candidate_id: RaftNodeId(2),
                last_log_index: LogIndex::ZERO,
                last_log_term: Term::default(),
            }),
        })
        .expect("hard state writes");
    let store = runtime.hard_state_store.clone();
    let mut restarted = durable_node(1, &[2, 3], store);

    let outputs = restarted
        .step(RaftInput::Message {
            from: RaftNodeId(3),
            message: Message::RequestVote(RequestVote {
                term: Term(3),
                candidate_id: RaftNodeId(3),
                last_log_index: LogIndex::ZERO,
                last_log_term: Term::default(),
            }),
        })
        .expect("unchanged hard state does not write");

    assert!(matches!(
        outputs.as_slice(),
        [RaftOutput::Send {
            message: Message::RequestVoteResponse(response),
            ..
        }] if !response.vote_granted
    ));
}
