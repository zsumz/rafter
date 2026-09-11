//! The successful write story must agree with replies from an actual follower.

use super::{election_write, observation, steps::Stimulus, support::*};

#[test]
fn correlated_write_responses_match_real_follower() {
    for batch in [false, true] {
        let story = observation::run(
            &election_write::election_and_write(),
            batch,
            &mut String::new(),
        );
        let mut follower = Node::from_bootstrap(
            config(2, &[1, 3]),
            BootstrapState {
                current_term: Term(1),
                voted_for: None,
                commit_index: LogIndex::ZERO,
                committed_configuration: None,
                snapshot: None,
                log: vec![BootstrapLogEntry::application(
                    LogIndex(1),
                    Term(1),
                    b"older write".to_vec(),
                )],
            },
        )
        .unwrap();
        let mut last_response = None;
        let mut checked = 0;
        for step in story {
            if matches!(
                step.name,
                "Current-term no-op commits the preceding write"
                    | "New matching prefix advances commitment"
            ) {
                assert_eq!(
                    step.inputs,
                    vec![last_response
                        .clone()
                        .expect("follower answered an emitted append")]
                );
                checked += 1;
                if step.name == "New matching prefix advances commitment" {
                    let Input::Message {
                        message: Message::AppendEntriesResponse(response),
                        ..
                    } = &step.inputs[0]
                    else {
                        panic!("expected append response")
                    };
                    assert_eq!(response.match_index, LogIndex(4));
                    // Payload-only replication reuses the current contact round.
                    assert_eq!(response.sequence, 1);
                }
            }
            for output in step.outputs {
                if let Output::Send {
                    to: NodeId(2),
                    message: Message::AppendEntries(append),
                } = output
                {
                    for response in follower.step(message(1, Message::AppendEntries(append))) {
                        if let Output::Send {
                            to: NodeId(1),
                            message: Message::AppendEntriesResponse(response),
                        } = response
                        {
                            assert!(
                                response.success,
                                "the follower must accept the actual emitted prefix"
                            );
                            last_response =
                                Some(message(2, Message::AppendEntriesResponse(response)));
                        }
                    }
                }
            }
        }
        assert_eq!(checked, 2);
        assert_eq!(follower.last_log_index(), LogIndex(4));
    }
}

#[test]
#[should_panic(expected = "no emitted append matches this walkthrough reply")]
fn write_cannot_echo_a_contact_round_that_was_never_sent() {
    let story = observation::run(
        &election_write::election_and_write(),
        false,
        &mut String::new(),
    );
    let sent: Vec<_> = story.into_iter().flat_map(|step| step.outputs).collect();
    Stimulus::Reply(AppendReply {
        follower: NodeId(2),
        through: LogIndex(4),
        sequence: Some(2),
        outcome: ReplyOutcome::Accepted,
    })
    .resolve(&sent);
}
