//! A self-removing leader authorizes one whole batch before stepping down.
use super::support::*;

fn replicas() -> (Node, Node, Node) {
    let joint = ConfigurationEntry::joint(
        ConfigurationId(1),
        JointMembership::new(membership(&[1, 2, 3]), membership(&[2, 3])),
    );
    let make = |id| {
        Node::from_bootstrap(
            NodeConfig::new(
                NodeId(id),
                (1..=3).filter(|peer| *peer != id).map(NodeId).collect(),
                3,
            )
            .unwrap(),
            BootstrapState {
                current_term: Term(2),
                voted_for: Some(NodeId(1)),
                commit_index: LogIndex(1),
                committed_configuration: None,
                snapshot: None,
                log: vec![BootstrapLogEntry::configuration(
                    LogIndex(1),
                    Term(2),
                    joint.clone(),
                )],
            },
        )
        .unwrap()
    };
    (make(1), make(2), make(3))
}

// Replicate every emitted request but withhold intermediate acknowledgements.
// The final acknowledgement certifies the actual stored prefix on each peer.
fn replicate(outputs: Vec<Output>, second: &mut Node, third: &mut Node) -> [Input; 2] {
    let mut replies = [None, None];
    for output in outputs {
        if let Output::Send {
            to,
            message: Message::AppendEntries(request),
        } = output
        {
            let slot = usize::from(to == NodeId(3));
            let follower = if slot == 0 { &mut *second } else { &mut *third };
            assert_eq!(to, follower.id());
            for reply in follower.step(Input::Message {
                from: NodeId(1),
                message: Message::AppendEntries(request),
            }) {
                if let Output::Send {
                    message: Message::AppendEntriesResponse(response),
                    ..
                } = reply
                {
                    assert!(response.success);
                    replies[slot] = Some(Input::Message {
                        from: to,
                        message: Message::AppendEntriesResponse(response),
                    });
                }
            }
        }
    }
    replies.map(Option::unwrap)
}

fn self_removal_batch(prefix: bool) {
    let (mut leader, mut second, mut third) = replicas();
    let mut initial = leader.become_leader();
    initial.extend(leader.step(Input::Tick));
    for reply in replicate(initial, &mut second, &mut third) {
        let _ = leader.step(reply);
    }
    assert_eq!(leader.commit_index(), LogIndex(2));
    let mut pending = Vec::new();
    if prefix {
        pending.extend(leader.step(Input::ClientProposal {
            payload: b"prefix".to_vec(),
        }));
    }
    pending.extend(leader.step(Input::LeaveJoint));
    let removal_index = leader.last_log_index();
    assert!(!leader.is_effective_voter(NodeId(1)));
    assert_eq!(leader.role(), Role::Leader);
    pending.extend(leader.step(Input::ClientProposal {
        payload: b"suffix".to_vec(),
    }));
    let last = leader.last_log_index();
    assert_eq!(last, removal_index.next());
    let [first, second_reply] = replicate(pending, &mut second, &mut third);
    assert_eq!(second.last_log_index(), last);
    assert_eq!(third.last_log_index(), last);

    // Node 1 cannot count itself in the new two-voter quorum.
    let _ = leader.step(first);
    assert_eq!(leader.commit_index(), LogIndex(2));
    assert_eq!(leader.role(), Role::Leader);
    let outputs = leader.step(second_reply);
    assert_eq!(leader.commit_index(), last);
    assert_eq!(leader.role(), Role::Follower);
    let removal = outputs
        .iter()
        .position(|event| {
            matches!(event,
        Output::ConfigurationCommitted { index, .. } if *index == removal_index)
        })
        .unwrap();
    let suffix = outputs
        .iter()
        .position(|event| {
            matches!(event,
        Output::Apply { index, .. } if *index == last)
        })
        .unwrap();
    assert!(removal < suffix);
    if prefix {
        assert!(outputs[..removal].iter().any(|event| matches!(event,
            Output::Apply { payload, .. } if payload.as_slice() == b"prefix")));
    }
    let rejected = leader.step(Input::ClientProposal {
        payload: b"after-removal".to_vec(),
    });
    assert!(matches!(
        rejected.as_slice(),
        [Output::RejectProposal {
            reason: ProposalRejection::NotLeader { .. },
            ..
        }]
    ));
    assert_eq!(leader.last_log_index(), last);
    leader.validate_derived_state().unwrap();
}

#[test]
fn self_removal_batch_commits_a_suffix_and_then_refuses_new_proposals() {
    self_removal_batch(false);
}

#[test]
fn self_removal_batch_commits_both_a_prefix_and_suffix_under_the_new_quorum() {
    self_removal_batch(true);
}
