use rafter::{BootstrapState, Node, NodeConfig, NodeId, Term};

#[test]
fn errors_propagate_with_question_mark() {
    fn bootstrap_with_vote_in_zero_term() -> Result<(), Box<dyn std::error::Error>> {
        let config = NodeConfig::new(NodeId(1), vec![NodeId(2), NodeId(3)], 3)?;
        Node::from_bootstrap(
            config,
            BootstrapState {
                current_term: Term::default(),
                voted_for: Some(NodeId(2)),
                ..BootstrapState::default()
            },
        )?;
        Ok(())
    }

    let error = bootstrap_with_vote_in_zero_term().expect_err("vote in term zero must fail");

    assert_eq!(
        error.to_string(),
        "Raft bootstrap records a vote for node-2 in term zero"
    );
}
