use proptest::prelude::*;
use proptest::test_runner::FileFailurePersistence;
use rafter::{
    AppendEntriesResponse, Input, LogIndex, Message, Node, NodeConfig, NodeId, ReadId,
    RequestVoteResponse, Role,
};
use rafter_invariant_test::oracle_prop_assert;

fn elected_leader() -> Node {
    let config = NodeConfig::new(NodeId(1), vec![NodeId(2), NodeId(3)], 3)
        .expect("property leader config is valid")
        .with_pre_vote(false)
        .with_check_quorum(false);
    let mut node = Node::new(config);
    for _ in 0..3 {
        let _ = node.step(Input::Tick);
    }
    let term = node.current_term();
    let _ = node.step(Input::Message {
        from: NodeId(2),
        message: Message::RequestVoteResponse(RequestVoteResponse {
            term,
            voter_id: NodeId(2),
            vote_granted: true,
        }),
    });
    assert_eq!(node.role(), Role::Leader);
    node
}

fn generated_input(node: &Node, kind: u8, value: u64, flag: bool) -> Input {
    match kind % 5 {
        0 => Input::Tick,
        1 => Input::ClientProposal {
            payload: value.to_le_bytes().to_vec(),
        },
        2 => Input::ReadIndex {
            read_id: ReadId(value),
        },
        3 => Input::Message {
            from: NodeId(2),
            message: Message::AppendEntriesResponse(AppendEntriesResponse {
                term: node.current_term(),
                follower_id: NodeId(2),
                success: flag,
                match_index: LogIndex(value.min(node.last_log_index().0)),
                sequence: value,
            }),
        },
        _ => Input::Message {
            from: NodeId(2),
            message: Message::RequestVoteResponse(RequestVoteResponse {
                term: node.current_term(),
                voter_id: NodeId(2),
                vote_granted: flag,
            }),
        },
    }
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 128,
        failure_persistence: Some(Box::new(FileFailurePersistence::Direct(
            "proptest-regressions/leader_append_only_property.txt",
        ))),
        ..ProptestConfig::default()
    })]

    #[test]
    fn leader_same_term_log_is_prefix_monotone_across_generated_inputs(
        actions in proptest::collection::vec((any::<u8>(), any::<u64>(), any::<bool>()), 1..=64),
    ) {
        let mut leader = elected_leader();
        for (kind, value, flag) in actions {
            let before_term = leader.current_term();
            let before = leader.log_entries_from(LogIndex(1));
            let input = generated_input(&leader, kind, value, flag);
            let _ = leader.step(input);
            if leader.role() != Role::Leader || leader.current_term() != before_term {
                continue;
            }
            let after = leader.log_entries_from(LogIndex(1));
            oracle_prop_assert!(
                after.starts_with(&before),
                "term-{before_term} leader rewrote or removed its own prefix: before={before:?} after={after:?}"
            );
        }
        oracle_prop_assert!(true, "every retained same-term leader prefix was monotone");
    }
}
