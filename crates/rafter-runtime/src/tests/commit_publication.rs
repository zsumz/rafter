//! A rejoining follower may replace and commit the same durable suffix.
//!
//! Every intermediate store image must reopen without replaying the old
//! command or publishing configuration metadata ahead of its replacement.

use super::*;
mod stores;
use stores::{HardState, Image, Journal, Log};

#[test]
fn single_step_commit_publication_survives_every_crash_cut() {
    assert_crash_cuts(false, false);
    assert_crash_cuts(false, true);
}

#[test]
fn batched_commit_publication_survives_every_crash_cut() {
    assert_crash_cuts(true, false);
    assert_crash_cuts(true, true);
}

fn assert_crash_cuts(batched: bool, configuration: bool) {
    let initial = initial_image(configuration);
    let before = initial.hard_state.current();
    let journal = Journal::new(initial);
    let mut runtime =
        durable_node_with_log(2, &[1, 3], HardState(journal.clone()), Log(journal.clone()));
    let input = replacement(configuration);
    let outputs = if batched {
        // Two inputs exercise the actual batch path, not its singleton shortcut.
        runtime.step_batch(vec![input, RaftInput::Tick])
    } else {
        runtime.step(input)
    }
    .expect("replacement commits");
    assert!(outputs.iter().any(|output| matches!(output,
        RaftOutput::Send { message: Message::AppendEntriesResponse(response), .. }
            if response.success && response.match_index == LogIndex(2)
    )));
    let images = journal.images();
    for (cut, image) in images.iter().enumerate() {
        let mut reopened = DurableRaftNode::with_storage(
            raft_config(2, &[1, 3]),
            image.hard_state.clone(),
            image.log.clone(),
        )
        .expect("every durable boundary reopens");
        let applied = reopened.drain_committed_outputs();
        assert!(
            !applied.iter().any(|output| matches!(output,
                RaftOutput::Apply { payload, .. } if payload.as_ref() == b"stale-suffix"
            )),
            "crash cut {cut} replayed the stale command"
        );
        assert_eq!(reopened.current_term(), Term(3));
        if cut < 3 {
            assert_eq!(image.hard_state.current().commit_index, before.commit_index);
            assert_eq!(
                image.hard_state.current().committed_configuration,
                before.committed_configuration
            );
        } else {
            assert_eq!(reopened.commit_index(), LogIndex(2));
            if configuration {
                assert_eq!(
                    image.hard_state.current().committed_configuration,
                    Some(CommittedConfiguration {
                        index: LogIndex(2),
                        config_id: ConfigurationId(2)
                    })
                );
            } else {
                assert!(applied.iter().any(|output| matches!(output,
                    RaftOutput::Apply { payload, .. } if payload.as_ref() == b"committed-truth"
                )));
            }
        }
    }
    // Term fence, truncate, replacement append, then commit publication.
    assert_eq!(images.len(), 4);
    assert_eq!(images[0].log.next_index(), LogIndex(3));
    assert_eq!(images[1].log.next_index(), LogIndex(2));
    assert_eq!(images[2].log.next_index(), LogIndex(3));
}

fn initial_image(configuration: bool) -> Image {
    let mut hard_state = hard_state_store(1, None);
    let first = if configuration {
        hard_state
            .write_hard_state(RaftHardState {
                commit_index: LogIndex(1),
                committed_configuration: Some(CommittedConfiguration {
                    index: LogIndex(1),
                    config_id: ConfigurationId(1),
                }),
                ..hard_state.current()
            })
            .expect("initial configuration commits");
        PersistedRaftLogEntry::configuration(LogIndex(1), Term(1), config(1, false))
    } else {
        PersistedRaftLogEntry::application(LogIndex(1), Term(1), b"shared".to_vec())
    };
    let mut log = InMemoryRaftLogSegment::new();
    log.append_entries(&[
        first,
        PersistedRaftLogEntry::application(LogIndex(2), Term(1), b"stale-suffix".to_vec()),
    ])
    .expect("initial suffix persists");
    Image { hard_state, log }
}

fn replacement(configuration: bool) -> RaftInput {
    let entry = if configuration {
        LogEntry::configuration(Term(2), config(2, true))
    } else {
        LogEntry::application(Term(2), b"committed-truth".to_vec())
    };
    RaftInput::Message {
        from: RaftNodeId(1),
        message: Message::AppendEntries(AppendEntries {
            sequence: 0,
            term: Term(3),
            leader_id: RaftNodeId(1),
            prev_log_index: LogIndex(1),
            prev_log_term: Term(1),
            entries: vec![entry].into(),
            leader_commit: LogIndex(2),
        }),
    }
}

fn config(id: u64, learner: bool) -> ConfigurationEntry {
    ConfigurationEntry::stable(
        ConfigurationId(id),
        MembershipSet::new(
            vec![RaftNodeId(1), RaftNodeId(2), RaftNodeId(3)],
            if learner {
                vec![RaftNodeId(4)]
            } else {
                Vec::new()
            },
        )
        .expect("configuration is valid"),
    )
}
