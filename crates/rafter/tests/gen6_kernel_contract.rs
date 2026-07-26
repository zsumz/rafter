//! Regression suite, adopted from the gen-6 hunt: public-API doc claims that
//! the body did not deliver. Each test asserts the DOCUMENTED behaviour, so it
//! fails if a doc and its body drift apart again.
//!
//! Two of the three were resolved by correcting the body, and one by
//! correcting the claim: `leader_replication_progress` reports followers, and
//! a leader has no replication stream toward itself to report.

use rafter::{
    ApplicationSnapshotKind, ApplicationSnapshotMetadata, ApplicationSnapshotVersion, Input,
    LogIndex, Message, Node, NodeConfig, NodeId, Output, RaftSnapshot, RaftSnapshotMetadata,
    ReadId, RequestVoteResponse, Role, SnapshotGroupId, Term,
};

const ELECTION_TIMEOUT_TICKS: u64 = 8;

fn minimal_leader() -> Node {
    let config = NodeConfig::new(
        NodeId(1),
        vec![NodeId(2), NodeId(3)],
        ELECTION_TIMEOUT_TICKS,
    )
    .expect("valid config")
    .with_pre_vote(false)
    .with_check_quorum(false);
    let mut node = Node::new(config);
    for _ in 0..ELECTION_TIMEOUT_TICKS {
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

fn lease_leader_with_commit() -> Node {
    let config = NodeConfig::new(
        NodeId(1),
        vec![NodeId(2), NodeId(3)],
        ELECTION_TIMEOUT_TICKS,
    )
    .expect("valid config")
    .with_lease_reads(true);
    let mut node = Node::new(config);
    for _ in 0..ELECTION_TIMEOUT_TICKS {
        let _ = node.step(Input::Tick);
    }
    let proposed = node.current_term().next();
    let _ = node.step(Input::Message {
        from: NodeId(2),
        message: Message::PreVoteResponse(rafter::PreVoteResponse {
            term: proposed,
            voter_id: NodeId(2),
            vote_granted: true,
        }),
    });
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
    let _ = node.step(Input::Message {
        from: NodeId(2),
        message: Message::AppendEntriesResponse(rafter::AppendEntriesResponse {
            term: node.current_term(),
            follower_id: NodeId(2),
            success: true,
            match_index: LogIndex(1),
            sequence: 1,
        }),
    });
    assert_eq!(node.commit_index(), LogIndex(1));
    assert!(node.read_lease_active());
    node
}

/// `Node::read_lease_active` doc:
///   "Whether a read barrier requested right now would grant from the leader
///    lease without a quorum round trip."
#[test]
fn read_lease_active_predicts_whether_a_barrier_grants_from_the_lease() {
    let mut leader = lease_leader_with_commit();
    let _ = leader.step(Input::TransferLeadership { target: NodeId(2) });

    let predicted = leader.read_lease_active();
    let outputs = leader.step(Input::ReadIndex { read_id: ReadId(1) });
    let actually_granted = outputs
        .iter()
        .any(|output| matches!(output, Output::ReadIndexGranted { .. }));

    assert_eq!(
        predicted, actually_granted,
        "read_lease_active() promised {predicted} but the barrier produced {outputs:?}"
    );
}

/// `Node::leader_replication_progress` doc:
///   "Returns leader-side replication progress for every effective *follower*,
///    learners included. The leader's own slot is not a row here [...] Its own
///    match index is `Node::last_log_index` by construction."
///
/// Both halves are pinned, because the second is what a caller doing quorum
/// arithmetic over these rows has to add back, and a doc that named the
/// exclusion without naming the substitute would leave that caller guessing.
#[test]
fn leader_replication_progress_covers_every_effective_follower_and_excludes_the_leader() {
    let leader = minimal_leader();
    let effective: Vec<NodeId> = leader.effective_membership().replica_ids();
    let reported: Vec<NodeId> = leader
        .leader_replication_progress()
        .into_iter()
        .map(|progress| progress.follower_id)
        .collect();

    let missing: Vec<NodeId> = effective
        .iter()
        .copied()
        .filter(|id| *id != leader.id() && !reported.contains(id))
        .collect();
    assert!(
        missing.is_empty(),
        "effective followers {effective:?} but progress reported {reported:?}; missing {missing:?}"
    );
    assert!(
        !reported.contains(&leader.id()),
        "a leader has no replication stream toward itself: {reported:?}"
    );
    assert_eq!(
        leader.last_log_index(),
        LogIndex(1),
        "and its own match index is its last log index, with no row needed"
    );
}

fn descriptor(index: u64, term: u64) -> RaftSnapshot {
    let metadata = RaftSnapshotMetadata::new(
        SnapshotGroupId::new("g").expect("group id"),
        NodeId(1),
        LogIndex(index),
        Term(term),
        Term(term),
        ApplicationSnapshotMetadata::new(
            ApplicationSnapshotKind::new("kv").expect("kind"),
            ApplicationSnapshotVersion::new(1).expect("version"),
        ),
    )
    .expect("valid snapshot metadata");
    RaftSnapshot::from_payload(metadata, b"payload")
}

/// `Node::install_local_snapshot` used to document no precondition at all,
/// while the body force-raised `commit_index` and `applied_index` to the
/// caller's boundary. It now states the precondition and enforces it:
/// installing a *descriptor* cannot manufacture commitment.
#[test]
fn install_local_snapshot_does_not_manufacture_commitment() {
    // A plain follower with three replicated-but-uncommitted entries.
    let mut follower = Node::new(
        NodeConfig::new(
            NodeId(2),
            vec![NodeId(1), NodeId(3)],
            ELECTION_TIMEOUT_TICKS,
        )
        .expect("valid config"),
    );
    let entries: Vec<rafter::LogEntry> = (1u8..=3)
        .map(|index| rafter::LogEntry::application(Term(1), vec![index]))
        .collect();
    let _ = follower.step(Input::Message {
        from: NodeId(1),
        message: Message::AppendEntries(rafter::AppendEntries {
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
    assert_eq!(
        follower.commit_index(),
        LogIndex::ZERO,
        "the leader committed nothing"
    );

    let refused = follower.install_local_snapshot(descriptor(3, 1)).is_err();

    assert_eq!(
        follower.commit_index(),
        LogIndex::ZERO,
        "install_local_snapshot advanced the commit index with no quorum evidence"
    );
    assert!(
        refused,
        "and the caller is told, rather than left to compare indexes afterwards"
    );
}
