use rafter::{NodeConfig, NodeId, Term};
use rafter_runtime::DurableRaftNode;
use rafter_storage::{InMemoryRaftHardStateStore, RaftHardState, RaftHardStateStore};

#[test]
fn errors_propagate_with_question_mark() {
    fn hydrate_with_vote_in_zero_term() -> Result<(), Box<dyn std::error::Error>> {
        let config = NodeConfig::new(NodeId(1), vec![NodeId(2)], 3)?;
        let mut hard_state_store = InMemoryRaftHardStateStore::new();
        hard_state_store.write_hard_state(RaftHardState {
            current_term: Term::default(),
            voted_for: Some(NodeId(2)),
            commit_index: rafter::LogIndex::ZERO,
            committed_configuration: None,
        })?;
        DurableRaftNode::new(config, hard_state_store)?;
        Ok(())
    }

    let error = hydrate_with_vote_in_zero_term().expect_err("vote in term zero must fail");

    assert_eq!(
        error.to_string(),
        "Raft bootstrap validation failed: \
         Raft bootstrap records a vote for node-2 in term zero"
    );
    let source = error
        .source()
        .expect("runtime error wraps the bootstrap error");
    assert_eq!(
        source.to_string(),
        "Raft bootstrap records a vote for node-2 in term zero"
    );
}
