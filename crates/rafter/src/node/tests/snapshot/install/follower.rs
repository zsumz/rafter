//! Follower snapshot authority, installation, compaction, and membership recovery.

use super::*;

#[test]
fn follower_installs_newer_snapshot_and_emits_apply_snapshot() {
    let mut follower = node(2, &[1, 3]);
    push_log_entry(&mut follower, Term(2), b"local-one");
    push_log_entry(&mut follower, Term(3), b"local-two");
    let snapshot = test_snapshot(3, 4, 5, b"stream snapshot");

    let outputs = follower.step(Input::Message {
        from: NodeId(1),
        message: Message::InstallSnapshot(crate::InstallSnapshot {
            term: Term(5),
            leader_id: NodeId(1),
            metadata: snapshot.metadata.clone(),
            application_payload: b"stream snapshot".to_vec(),
        }),
    });

    assert_eq!(follower.current_term(), Term(5));
    assert_eq!(follower.snapshot_index(), LogIndex(3));
    assert_eq!(follower.commit_index(), LogIndex(3));
    assert_eq!(follower.last_log_index(), LogIndex(3));
    assert_eq!(follower.log_entries_from(LogIndex(1)), Vec::new());
    assert!(matches!(
        outputs.as_slice(),
        [
            Output::StageSnapshotChunk { chunk },
            Output::ApplySnapshot { snapshot: applied },
            Output::Send {
                to: NodeId(1),
                message: Message::InstallSnapshotResponse(response),
            },
        ] if applied == &snapshot
            && chunk.leader_id == NodeId(1)
            && chunk.transfer_id == snapshot.transfer_id()
            && chunk.metadata == snapshot.metadata
            && chunk.total_payload_len == snapshot.application_payload_len
            && chunk.offset == 0
            && chunk.bytes == b"stream snapshot"
            && chunk.done
            && response.term == Term(5)
            && response.follower_id == NodeId(2)
            && response.success
            && response.last_included_index == LogIndex(3)
            && response.transfer_id == Some(snapshot.transfer_id())
            && response.next_offset == b"stream snapshot".len() as u64
    ));
}
#[test]
fn local_snapshot_covering_tracked_proposal_emits_dropped_event() {
    let mut node = node(1, &[2, 3]);
    let _ = elect_leader(&mut node);
    assert_eq!(node.role(), Role::Leader);

    let proposal_id = LocalProposalId(17);
    let _ = node.step(Input::TrackedClientProposal {
        proposal_id,
        payload: b"covered".to_vec(),
    });
    assert!(node.volatile.local_proposals.contains_key(LogIndex(2)));

    // Commit the leader's own no-op so a boundary at index 1 is installable at
    // all: a local descriptor carries no quorum evidence, so the boundary must
    // lie at or below the committed prefix.
    acknowledge_match(&mut node, LogIndex(1));
    assert_eq!(node.commit_index(), LogIndex(1));

    // The descriptor's boundary term contradicts the retained entry at that
    // index, so the suffix above it cannot be proven to belong to the same
    // history and is discarded with the snapshot. The tracked proposal in that
    // suffix is reported rather than forgotten.
    let snapshot = test_snapshot(1, 2, 2, b"covered snapshot");
    let outputs = node
        .install_local_snapshot(snapshot)
        .expect("a boundary at the committed index installs");

    assert!(node.volatile.local_proposals.is_empty());
    assert_eq!(
        outputs,
        vec![Output::LocalProposalDropped {
            proposal_id,
            index: LogIndex(2),
            term: Term(1),
            reason: LocalProposalDropReason::SnapshotCovered,
        }]
    );
}

/// `install_local_snapshot` is the application-driven compaction path, and a
/// descriptor handed to it proves nothing about what a quorum accepted. A
/// boundary beyond the committed prefix is refused rather than manufacturing
/// commitment out of a local call — which is what the leader-sent install path
/// is allowed to do, because a leader only snapshots committed state.
#[test]
fn local_snapshot_beyond_the_commit_index_is_refused() {
    let mut follower = node(2, &[1, 3]);
    let entries: Vec<crate::LogEntry> = (1u8..=3)
        .map(|index| crate::LogEntry::application(Term(1), vec![index]))
        .collect();
    let _ = follower.step(Input::Message {
        from: NodeId(1),
        message: Message::AppendEntries(crate::AppendEntries {
            term: Term(1),
            leader_id: NodeId(1),
            prev_log_index: LogIndex::ZERO,
            prev_log_term: Term::default(),
            entries: entries.into(),
            leader_commit: LogIndex::ZERO,
            sequence: 1,
        }),
    });
    assert_eq!(follower.last_log_index(), LogIndex(3));
    assert_eq!(follower.commit_index(), LogIndex::ZERO);

    let error = follower
        .install_local_snapshot(test_snapshot(3, 1, 1, b"uncommitted"))
        .expect_err("a boundary beyond the committed prefix must be refused");

    assert_eq!(
        error,
        crate::LocalSnapshotInstallError::BoundaryAheadOfCommit {
            snapshot_index: LogIndex(3),
            commit_index: LogIndex::ZERO,
        }
    );
    // Nothing moved: no manufactured commitment, and no entry compacted away.
    assert_eq!(follower.commit_index(), LogIndex::ZERO);
    assert_eq!(follower.applied_index(), LogIndex::ZERO);
    assert_eq!(follower.snapshot_index(), LogIndex::ZERO);
    assert_eq!(follower.last_log_index(), LogIndex(3));
}

fn acknowledge_match(leader: &mut Node, match_index: LogIndex) {
    let term = leader.current_term();
    let _ = leader.step(Input::Message {
        from: NodeId(2),
        message: Message::AppendEntriesResponse(crate::AppendEntriesResponse {
            term,
            follower_id: NodeId(2),
            success: true,
            match_index,
            sequence: 1,
        }),
    });
}
#[test]
fn follower_installs_snapshot_committed_configuration_identity_and_next_id_advances() {
    let mut follower = node(2, &[1, 3]);
    let membership = MembershipSet::new(vec![NodeId(1), NodeId(2), NodeId(3)], Vec::new())
        .expect("test membership is valid");
    let committed = CommittedConfiguration {
        index: LogIndex(7),
        config_id: ConfigurationId(9),
    };
    let metadata = crate::RaftSnapshotMetadata::new(
        crate::SnapshotGroupId::new("data-group-10").expect("valid snapshot group"),
        NodeId(1),
        LogIndex(10),
        Term(4),
        Term(5),
        crate::ApplicationSnapshotMetadata::new(
            crate::ApplicationSnapshotKind::new("stream_data").expect("valid kind"),
            crate::ApplicationSnapshotVersion::new(1).expect("valid version"),
        ),
    )
    .expect("valid snapshot metadata")
    .with_committed_configuration(SnapshotCommittedConfiguration::new(
        Some(committed),
        MembershipConfig::stable(membership),
    ));
    let snapshot = crate::RaftSnapshot::from_payload(metadata, b"config snapshot");

    let outputs = follower.step(Input::Message {
        from: NodeId(1),
        message: Message::InstallSnapshot(crate::InstallSnapshot {
            term: Term(5),
            leader_id: NodeId(1),
            metadata: snapshot.metadata.clone(),
            application_payload: b"config snapshot".to_vec(),
        }),
    });

    assert!(outputs
        .iter()
        .any(|output| matches!(output, Output::ApplySnapshot { .. })));
    assert_eq!(
        follower.snapshot_committed_configuration_state(),
        Some(committed)
    );
    assert_eq!(follower.committed_configuration_state(), Some(committed));

    follower.become_leader();
    let _ = follower.step(Input::RemoveVoter {
        voter_id: NodeId(3),
    });

    assert_eq!(
        follower
            .effective_configuration_entry()
            .expect("removing a voter appends the next configuration")
            .config_id(),
        ConfigurationId(10)
    );
}
#[test]
fn follower_rejects_install_snapshot_written_by_non_voter() {
    let mut follower = node(2, &[1, 3]);
    let metadata = crate::RaftSnapshotMetadata::new(
        crate::SnapshotGroupId::new("data-group-10").expect("valid snapshot group"),
        NodeId(99),
        LogIndex(3),
        Term(4),
        Term(5),
        crate::ApplicationSnapshotMetadata::new(
            crate::ApplicationSnapshotKind::new("stream_data").expect("valid kind"),
            crate::ApplicationSnapshotVersion::new(1).expect("valid version"),
        ),
    )
    .expect("valid snapshot metadata");

    let outputs = follower.step(Input::Message {
        from: NodeId(1),
        message: Message::InstallSnapshot(crate::InstallSnapshot {
            term: Term(5),
            leader_id: NodeId(1),
            metadata,
            application_payload: b"non-voter snapshot".to_vec(),
        }),
    });

    assert_eq!(follower.snapshot_index(), LogIndex::ZERO);
    assert!(matches!(
        outputs.as_slice(),
        [Output::Send {
            to: NodeId(1),
            message: Message::InstallSnapshotResponse(response),
        }] if !response.success && response.last_included_index == LogIndex::ZERO
    ));
}
#[test]
fn newly_added_leader_with_older_boundary_snapshot_is_rejected() {
    let mut follower = node(2, &[1, 3, 4]);
    // Node 4 is a known peer in the current embedding, but this older
    // snapshot boundary still authorizes only voters 1, 2, and 3.
    let snapshot = test_snapshot_with_committed_voters(3, 4, 5, b"dynamic snapshot", &[1, 2, 3]);

    let outputs = follower.step(Input::Message {
        from: NodeId(4),
        message: Message::InstallSnapshot(crate::InstallSnapshot {
            term: Term(5),
            leader_id: NodeId(4),
            metadata: snapshot.metadata,
            application_payload: b"dynamic snapshot".to_vec(),
        }),
    });

    assert_eq!(follower.current_term(), Term(5));
    assert_eq!(follower.leader_hint(), None);
    assert_eq!(follower.snapshot_index(), LogIndex::ZERO);
    assert!(outputs.iter().all(|output| !matches!(
        output,
        Output::StageSnapshotChunk { .. } | Output::ApplySnapshot { .. }
    )));
    assert!(matches!(
        outputs.as_slice(),
        [Output::Send {
            to: NodeId(4),
            message: Message::InstallSnapshotResponse(response),
        }] if !response.success && response.last_included_index == LogIndex::ZERO
    ));
}
#[test]
fn same_term_install_snapshot_step_down_preserves_recorded_vote() {
    let mut candidate = node(1, &[2, 3]);
    let _ = super::helpers::campaign(&mut candidate);
    assert_eq!(candidate.current_term(), Term(1));
    assert_eq!(candidate.voted_for(), Some(NodeId(1)));

    let outputs = candidate.step(Input::Message {
        from: NodeId(2),
        message: Message::InstallSnapshot(crate::InstallSnapshot {
            term: Term(1),
            leader_id: NodeId(2),
            metadata: test_snapshot(1, 1, 1, b"snapshot").metadata,
            application_payload: b"snapshot".to_vec(),
        }),
    });

    assert_eq!(candidate.role(), Role::Follower);
    assert_eq!(candidate.voted_for(), Some(NodeId(1)));
    assert!(matches!(
        outputs.as_slice(),
        [
            Output::StageSnapshotChunk { .. },
            Output::ApplySnapshot { .. },
            Output::Send {
                to: NodeId(2),
                message: Message::InstallSnapshotResponse(response),
            },
        ] if response.success
    ));
}
#[test]
fn follower_install_snapshot_retains_matching_suffix_after_boundary() {
    let mut follower = node(2, &[1, 3]);
    push_log_entry(&mut follower, Term(2), b"one");
    push_log_entry(&mut follower, Term(3), b"two");
    push_log_entry(&mut follower, Term(4), b"boundary");
    push_log_entry(&mut follower, Term(5), b"retained");
    let snapshot = test_snapshot(3, 4, 5, b"snapshot through three");

    let outputs = follower.step(Input::Message {
        from: NodeId(1),
        message: Message::InstallSnapshot(crate::InstallSnapshot {
            term: Term(5),
            leader_id: NodeId(1),
            metadata: snapshot.metadata.clone(),
            application_payload: b"snapshot through three".to_vec(),
        }),
    });

    assert!(matches!(
        outputs.as_slice(),
        [
            Output::StageSnapshotChunk { .. },
            Output::ApplySnapshot { .. },
            Output::Send { .. },
        ]
    ));
    assert_eq!(follower.snapshot_index(), LogIndex(3));
    assert_eq!(follower.last_log_index(), LogIndex(4));
    assert_eq!(
        follower.log_entries_from(LogIndex(1)),
        vec![LogEntry::application(Term(5), b"retained".to_vec())]
    );
}
#[test]
fn follower_ignores_snapshot_at_or_below_commit_index() {
    let mut follower = node(2, &[1, 3]);
    push_log_entry(&mut follower, Term(1), b"one");
    push_log_entry(&mut follower, Term(1), b"two");
    push_log_entry(&mut follower, Term(1), b"three");
    push_log_entry(&mut follower, Term(1), b"four");
    push_log_entry(&mut follower, Term(1), b"five");
    follower.volatile.commit_index = LogIndex(5);
    follower.volatile.applied_index = LogIndex(5);

    let snapshot = test_snapshot(3, 1, 1, b"stale snapshot");
    let outputs = follower.step(Input::Message {
        from: NodeId(1),
        message: Message::InstallSnapshot(crate::InstallSnapshot {
            term: Term(1),
            leader_id: NodeId(1),
            metadata: snapshot.metadata.clone(),
            application_payload: b"stale snapshot".to_vec(),
        }),
    });

    assert!(
        !outputs.iter().any(|output| matches!(
            output,
            Output::ApplySnapshot { .. } | Output::StageSnapshotChunk { .. }
        )),
        "a snapshot at or below the commit index must never rewind the state machine"
    );
    let response = install_snapshot_response_from_outputs(&outputs);
    assert!(response.success);
    assert_eq!(
        response.last_included_index,
        LogIndex(5),
        "the response must report the covered boundary so the leader stops streaming"
    );
}
