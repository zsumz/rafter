//! Who may author a snapshot, and who may send one.
//!
//! Two independent questions that were once answered by one membership lookup.
//! The **author** is a fact about the snapshot: `writer_id` must name a replica
//! — voter or learner — of the membership committed at the snapshot's own
//! boundary. The **sender** is a fact about right now: the leader delivering
//! the transfer must be a replica this receiver can currently see, and is never
//! required to appear in the historical boundary it is relaying.
//!
//! The author rule is checked at all three places a descriptor can enter a
//! node — local install, leader-sent receive, and bootstrap — so a descriptor
//! one of them would refuse can never be admitted by another.

use super::super::helpers::{bootstrap_entry, node};
use super::support::{install_snapshot_response_from_outputs, test_snapshot};
use super::*;

/// The cluster these scenarios use: voters 1, 2, 3 with node 4 as a learner.
fn learner_membership_set() -> MembershipSet {
    MembershipSet::new(vec![NodeId(1), NodeId(2), NodeId(3)], vec![NodeId(4)])
        .expect("test membership is valid")
}

fn learner_membership() -> MembershipConfig {
    MembershipConfig::stable(learner_membership_set())
}

/// The committed configuration state the learner derives at boundary index 3:
/// the configuration entry at index 1 plus the membership it installed. A local
/// install compares both against what the node derives, so a descriptor that
/// carries only the membership is refused for the *configuration* — which would
/// hide the author rule this module is about.
fn learner_boundary_configuration() -> SnapshotCommittedConfiguration {
    SnapshotCommittedConfiguration::new(
        Some(CommittedConfiguration {
            index: LogIndex(1),
            config_id: ConfigurationId(7),
        }),
        learner_membership(),
    )
}

/// A descriptor authored by `writer_id`, optionally carrying committed
/// configuration state. Everything else matches [`test_snapshot`].
fn authored_snapshot(
    writer_id: u64,
    last_included_index: u64,
    last_included_term: u64,
    hard_state_term: u64,
    payload: &[u8],
    committed: Option<SnapshotCommittedConfiguration>,
) -> crate::RaftSnapshot {
    let mut metadata = test_snapshot(
        last_included_index,
        last_included_term,
        hard_state_term,
        payload,
    )
    .metadata;
    metadata.writer_id = NodeId(writer_id);
    let metadata = match committed {
        Some(committed) => metadata.with_committed_configuration(committed),
        None => metadata,
    };
    crate::RaftSnapshot::from_payload(metadata, payload)
}

/// Boundary membership without a configuration identity, for the receive-path
/// scenarios whose fixtures have no committed configuration entry to name.
fn boundary_membership_only() -> SnapshotCommittedConfiguration {
    SnapshotCommittedConfiguration::new(None, learner_membership())
}

fn learner_bootstrap_config() -> NodeConfig {
    NodeConfig::new_non_voter(NodeId(4), vec![NodeId(1), NodeId(2), NodeId(3)], 3)
        .expect("test config is valid")
}

/// The learner's durable image: the configuration entry that makes node 4 a
/// learner, committed at index 1, plus two committed application entries.
fn learner_bootstrap_log() -> Vec<BootstrapLogEntry> {
    vec![
        BootstrapLogEntry::configuration(
            LogIndex(1),
            Term(1),
            ConfigurationEntry::stable(ConfigurationId(7), learner_membership_set()),
        ),
        bootstrap_entry(2, 1, b"two"),
        bootstrap_entry(3, 1, b"three"),
    ]
}

/// Node 4 as the cluster's learner, applied through its whole committed log.
fn applied_learner() -> Node {
    let mut node = Node::from_bootstrap(
        learner_bootstrap_config(),
        BootstrapState {
            current_term: Term(1),
            voted_for: None,
            commit_index: LogIndex(3),
            committed_configuration: None,
            snapshot: None,
            log: learner_bootstrap_log(),
        },
    )
    .expect("the learner's durable image is valid");
    let _ = node.drain_committed_outputs();
    assert_eq!(node.applied_index(), LogIndex(3));
    assert!(node.effective_membership().contains_learner(NodeId(4)));
    node
}

/// The natural call: a learner compacts at its own applied index and signs the
/// descriptor with its own id. Accepting the install and then refusing the same
/// descriptor at restart is the brick — the node would come up only by losing
/// the snapshot it was told to keep.
#[test]
fn learner_authored_snapshot_installs_locally_and_survives_restart() {
    let mut learner = applied_learner();
    let snapshot = authored_snapshot(
        4,
        3,
        1,
        1,
        b"learner state",
        Some(learner_boundary_configuration()),
    );

    learner
        .install_local_snapshot(snapshot.clone())
        .expect("a learner may author the snapshot it compacts");
    assert_eq!(learner.snapshot_index(), LogIndex(3));

    let restarted = Node::from_bootstrap(
        learner_bootstrap_config(),
        BootstrapState {
            current_term: Term(1),
            voted_for: None,
            commit_index: LogIndex(3),
            committed_configuration: None,
            snapshot: Some(snapshot),
            log: Vec::new(),
        },
    )
    .expect("a learner-authored snapshot must still hydrate its own author");

    assert_eq!(restarted.snapshot_index(), LogIndex(3));
    assert!(restarted.effective_membership().contains_learner(NodeId(4)));
}

/// A snapshot is not less usable for having been written by a learner: it
/// captures the same committed prefix, so a leader may relay it and a follower
/// must install it.
#[test]
fn learner_authored_snapshot_transfers_to_a_peer() {
    let mut follower = node(2, &[1, 3]);
    let payload = b"learner state".to_vec();
    let snapshot = authored_snapshot(4, 3, 4, 5, &payload, Some(boundary_membership_only()));

    let outputs = follower.step(Input::Message {
        from: NodeId(1),
        message: Message::InstallSnapshot(crate::InstallSnapshot {
            term: Term(5),
            leader_id: NodeId(1),
            metadata: snapshot.metadata,
            application_payload: payload,
        }),
    });

    assert_eq!(follower.snapshot_index(), LogIndex(3));
    assert!(outputs
        .iter()
        .any(|output| matches!(output, Output::ApplySnapshot { .. })));
    let response = install_snapshot_response_from_outputs(&outputs);
    assert!(response.success);
    assert_eq!(response.last_included_index, LogIndex(3));
}

/// The local-install path is the one that used to accept what bootstrap would
/// later refuse. It now applies the same author rule, so the refusal arrives at
/// the call that made the mistake rather than at the next restart.
#[test]
fn local_install_refuses_a_writer_outside_the_boundary_membership() {
    let mut learner = applied_learner();
    let snapshot = authored_snapshot(
        9,
        3,
        1,
        1,
        b"stranger state",
        Some(learner_boundary_configuration()),
    );

    let error = learner
        .install_local_snapshot(snapshot)
        .expect_err("a writer outside the boundary membership is refused");

    assert_eq!(
        error,
        crate::LocalSnapshotInstallError::WriterNotBoundaryReplica {
            snapshot_index: LogIndex(3),
            writer_id: NodeId(9),
            membership: Box::new(learner_membership()),
        }
    );
    assert_eq!(learner.snapshot_index(), LogIndex::ZERO);
    learner
        .validate_derived_state()
        .expect("a refused local install leaves derived state valid");
}

/// A descriptor carrying no boundary membership is judged against the
/// membership this node derives at the boundary, which is what the runtime
/// would have stamped into it.
#[test]
fn local_install_refuses_an_undeclared_writer_outside_the_derived_membership() {
    let mut learner = applied_learner();
    let snapshot = authored_snapshot(9, 3, 1, 1, b"stranger state", None);

    let error = learner
        .install_local_snapshot(snapshot)
        .expect_err("a writer outside the derived boundary membership is refused");

    assert_eq!(
        error,
        crate::LocalSnapshotInstallError::WriterNotBoundaryReplica {
            snapshot_index: LogIndex(3),
            writer_id: NodeId(9),
            membership: Box::new(learner_membership()),
        }
    );
}

/// The same author rule at the receive path.
#[test]
fn receive_rejects_a_writer_outside_the_boundary_membership() {
    let mut follower = node(2, &[1, 3]);
    let payload = b"stranger state".to_vec();
    let snapshot = authored_snapshot(9, 3, 4, 5, &payload, Some(boundary_membership_only()));

    let outputs = follower.step(Input::Message {
        from: NodeId(1),
        message: Message::InstallSnapshot(crate::InstallSnapshot {
            term: Term(5),
            leader_id: NodeId(1),
            metadata: snapshot.metadata,
            application_payload: payload,
        }),
    });

    assert_eq!(follower.snapshot_index(), LogIndex::ZERO);
    assert!(!install_snapshot_response_from_outputs(&outputs).success);
}

/// And at bootstrap, which is where the rule was already strict — it is now the
/// same strictness the other two sites apply.
#[test]
fn bootstrap_refuses_a_writer_outside_the_boundary_membership() {
    assert_eq!(
        Node::from_bootstrap(
            learner_bootstrap_config(),
            BootstrapState {
                current_term: Term(1),
                voted_for: None,
                commit_index: LogIndex(3),
                committed_configuration: None,
                snapshot: Some(authored_snapshot(
                    9,
                    3,
                    1,
                    1,
                    b"stranger state",
                    Some(learner_boundary_configuration()),
                )),
                log: Vec::new(),
            },
        ),
        Err(BootstrapValidationError::SnapshotWriterNotReplica {
            writer_id: NodeId(9)
        })
    );
}

/// With no boundary membership to consult, bootstrap falls back to the static
/// membership the process was configured with.
#[test]
fn bootstrap_refuses_a_writer_outside_the_static_membership() {
    assert_eq!(
        Node::from_bootstrap(
            NodeConfig::new(NodeId(1), vec![NodeId(2), NodeId(3)], 3)
                .expect("test config is valid"),
            BootstrapState {
                current_term: Term(1),
                voted_for: None,
                commit_index: LogIndex(3),
                committed_configuration: None,
                snapshot: Some(authored_snapshot(9, 3, 1, 1, b"stranger state", None)),
                log: Vec::new(),
            },
        ),
        Err(BootstrapValidationError::SnapshotWriterNotReplica {
            writer_id: NodeId(9)
        })
    );
}

/// A sender the receiver cannot place in any membership it can see is still
/// refused: dropping the historical-boundary rule for senders did not drop the
/// sender check.
#[test]
fn receive_rejects_a_sender_outside_every_visible_membership() {
    let mut follower = node(2, &[1, 3]);
    let payload = b"relayed state".to_vec();
    let snapshot = authored_snapshot(1, 3, 4, 5, &payload, Some(boundary_membership_only()));

    let outputs = follower.step(Input::Message {
        from: NodeId(9),
        message: Message::InstallSnapshot(crate::InstallSnapshot {
            term: Term(5),
            leader_id: NodeId(9),
            metadata: snapshot.metadata,
            application_payload: payload,
        }),
    });

    assert_eq!(follower.current_term(), Term(5));
    assert_eq!(follower.leader_hint(), None);
    assert_eq!(follower.snapshot_index(), LogIndex::ZERO);
    assert!(!install_snapshot_response_from_outputs(&outputs).success);
}
