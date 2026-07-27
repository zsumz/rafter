//! Application-driven local snapshot installation and its preconditions.
//!
//! Every refusal here asserts the *whole* node is unchanged, not just the
//! index the rule names. A local install compacts, replaces the descriptor,
//! and moves two indexes in one transition, so "nothing happened" is the only
//! claim worth pinning — and `validate_derived_state` is asserted on every
//! post-state, refusal or not, because the geometry these rules protect is
//! exactly what it checks.

use super::*;

/// Everything a local install can move, captured for an unchanged-assertion.
#[derive(Debug, Eq, PartialEq)]
struct NodeSnapshotState {
    snapshot: Option<crate::RaftSnapshot>,
    log: Vec<LogEntry>,
    commit_index: LogIndex,
    applied_index: LogIndex,
    committed_configuration: Option<CommittedConfiguration>,
    committed_membership: MembershipConfig,
}

impl NodeSnapshotState {
    fn of(node: &Node) -> Self {
        Self {
            snapshot: node.snapshot().cloned(),
            log: node.log_entries_from(LogIndex(1)),
            commit_index: node.commit_index(),
            applied_index: node.applied_index(),
            committed_configuration: node.committed_configuration_state(),
            committed_membership: node.committed_membership(),
        }
    }
}

/// Asserts `node` refuses `snapshot` with `expected` and is bit-identical
/// afterwards, with valid derived state on both sides of the call.
fn assert_refused_and_unchanged(
    node: &mut Node,
    snapshot: crate::RaftSnapshot,
    expected: &crate::LocalSnapshotInstallError,
) {
    node.validate_derived_state()
        .expect("the fixture starts from valid derived state");
    let before = NodeSnapshotState::of(node);

    let error = node
        .install_local_snapshot(snapshot)
        .expect_err("the local install must refuse");

    assert_eq!(&error, expected);
    assert_eq!(
        NodeSnapshotState::of(node),
        before,
        "a refused local install must leave every part of the node untouched"
    );
    node.validate_derived_state()
        .expect("a refused local install must leave derived state valid");
}

/// Bootstraps a node whose retained log runs 1..=`last_index` in `term`, with
/// every entry committed and applied through the local apply loop.
fn applied_node(last_index: u64, term: u64) -> Node {
    let log = (1..=last_index)
        .map(|index| {
            BootstrapLogEntry::application(
                LogIndex(index),
                Term(term),
                format!("entry-{index}").into_bytes(),
            )
        })
        .collect();
    let mut node = Node::from_bootstrap(
        NodeConfig::new(NodeId(2), vec![NodeId(1), NodeId(3)], 3).expect("test config is valid"),
        BootstrapState {
            current_term: Term(term),
            voted_for: None,
            commit_index: LogIndex(last_index),
            committed_configuration: None,
            snapshot: None,
            log,
        },
    )
    .expect("bootstrap state is valid");
    let _ = node.drain_committed_outputs();
    assert_eq!(node.applied_index(), LogIndex(last_index));
    assert_eq!(node.commit_index(), LogIndex(last_index));
    node
}

/// A boundary term the local log contradicts is caller error on this path, not
/// evidence of a divergent history. Discarding the suffix on its word — which
/// is right for a leader-sent install, and stays right there — leaves the
/// commit index above the logical last index, a state
/// [`Node::validate_derived_state`] rejects outright.
#[test]
fn local_snapshot_with_wrong_boundary_term_is_refused() {
    let mut node = applied_node(10, 1);

    assert_refused_and_unchanged(
        &mut node,
        test_snapshot(5, 2, 2, b"wrong boundary term"),
        &crate::LocalSnapshotInstallError::BoundaryTermMismatch {
            snapshot_index: LogIndex(5),
            snapshot_term: Term(2),
            local_term: Some(Term(1)),
        },
    );
}

/// A boundary below the installed snapshot proves nothing about itself — the
/// entries it names are already compacted away — and installing it would swap
/// a newer descriptor for an older one.
#[test]
fn local_snapshot_below_the_installed_boundary_is_refused() {
    let mut node = applied_node(10, 1);
    let outputs = node
        .install_local_snapshot(test_snapshot(8, 1, 1, b"newer"))
        .expect("a boundary at or below the applied index with a matching term installs");
    assert!(outputs.is_empty());
    assert_eq!(node.snapshot_index(), LogIndex(8));

    assert_refused_and_unchanged(
        &mut node,
        test_snapshot(3, 1, 1, b"older"),
        &crate::LocalSnapshotInstallError::BoundaryBelowInstalledSnapshot {
            snapshot_index: LogIndex(3),
            installed_index: LogIndex(8),
        },
    );
}

/// The exact installed boundary is a defined re-record, not an accident: the
/// descriptor is stored and nothing else moves. Repeating the same descriptor
/// leaves the node identical, which is what a retry after a partially completed
/// compaction needs, and a composition whose durable log is still behind an
/// already-installed boundary repairs it through this same call.
#[test]
fn local_snapshot_at_the_exact_installed_boundary_re_records_and_moves_nothing() {
    let mut node = applied_node(10, 1);
    let installed = test_snapshot(8, 1, 1, b"installed");
    let _ = node
        .install_local_snapshot(installed.clone())
        .expect("the first compaction installs");
    let after_first = NodeSnapshotState::of(&node);

    let outputs = node
        .install_local_snapshot(installed)
        .expect("the same descriptor at the installed boundary is accepted");

    assert!(outputs.is_empty());
    assert_eq!(
        NodeSnapshotState::of(&node),
        after_first,
        "repeating a local install with the same descriptor must change nothing"
    );

    // A different payload at the same boundary re-records the descriptor and
    // still moves nothing else: the committed prefix it names is identical.
    let rebuilt = test_snapshot(8, 1, 1, b"rebuilt at the same boundary");
    let outputs = node
        .install_local_snapshot(rebuilt.clone())
        .expect("a re-record at the installed boundary is accepted");

    assert!(outputs.is_empty());
    assert_eq!(node.snapshot(), Some(&rebuilt));
    assert_eq!(node.snapshot_index(), LogIndex(8));
    assert_eq!(node.last_log_index(), LogIndex(10));
    assert_eq!(node.commit_index(), LogIndex(10));
    assert_eq!(node.applied_index(), LogIndex(10));
    node.validate_derived_state()
        .expect("a re-record leaves derived state valid");
}

/// The installed boundary is a re-record, not a rewrite: rules 4 and 5 still
/// hold there, so neither the boundary's term nor its committed membership can
/// be changed under it.
#[test]
fn local_snapshot_cannot_rewrite_the_installed_boundary_term() {
    let mut node = applied_node(10, 1);
    let _ = node
        .install_local_snapshot(test_snapshot(8, 1, 1, b"installed"))
        .expect("the first compaction installs");

    assert_refused_and_unchanged(
        &mut node,
        test_snapshot(8, 2, 2, b"different boundary term"),
        &crate::LocalSnapshotInstallError::BoundaryTermMismatch {
            snapshot_index: LogIndex(8),
            snapshot_term: Term(2),
            local_term: Some(Term(1)),
        },
    );
}

/// A node recovered below its committed prefix has not handed the entries in
/// the gap to any state machine. Compacting through one would raise the applied
/// index over them, and they would never be emitted afterwards.
#[test]
fn local_snapshot_above_applied_but_within_commit_is_refused() {
    let mut node = Node::from_bootstrap(
        NodeConfig::new(NodeId(2), vec![NodeId(1), NodeId(3)], 3).expect("test config is valid"),
        BootstrapState {
            current_term: Term(3),
            voted_for: None,
            commit_index: LogIndex(3),
            committed_configuration: None,
            snapshot: None,
            log: vec![
                BootstrapLogEntry::application(LogIndex(1), Term(1), b"one".to_vec()),
                BootstrapLogEntry::application(LogIndex(2), Term(2), b"two".to_vec()),
                BootstrapLogEntry::application(LogIndex(3), Term(3), b"three".to_vec()),
            ],
        },
    )
    .expect("bootstrap state is valid");
    assert_eq!(node.applied_index(), LogIndex::ZERO);
    assert_eq!(node.commit_index(), LogIndex(3));

    assert_refused_and_unchanged(
        &mut node,
        test_snapshot(2, 2, 3, b"never applied"),
        &crate::LocalSnapshotInstallError::BoundaryAheadOfApplied {
            snapshot_index: LogIndex(2),
            applied_index: LogIndex::ZERO,
        },
    );

    // Draining is what makes the same call legitimate: the entries reach the
    // caller, and only then does the boundary describe consumed state.
    assert_eq!(node.drain_committed_outputs().len(), 3);
    let _ = node
        .install_local_snapshot(test_snapshot(2, 2, 3, b"applied through two"))
        .expect("the same boundary installs once the entries have been emitted");
    assert_eq!(node.snapshot_index(), LogIndex(2));
}

/// A boundary beyond the committed prefix keeps its own error rather than
/// collapsing into the applied-index rule that subsumes it: it is a different
/// mistake, and the more severe one.
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

    assert_refused_and_unchanged(
        &mut follower,
        test_snapshot(3, 1, 1, b"uncommitted"),
        &crate::LocalSnapshotInstallError::BoundaryAheadOfCommit {
            snapshot_index: LogIndex(3),
            commit_index: LogIndex::ZERO,
        },
    );
}

/// The descriptor outlives the entries it compacts and becomes this node's
/// membership of record below the boundary, so a copy the node does not derive
/// there would redefine the voter set out of a local call.
#[test]
fn local_snapshot_with_foreign_committed_membership_is_refused() {
    let mut node = applied_node(10, 1);
    let local = node.membership_at_index(LogIndex(5));
    let foreign = MembershipConfig::stable(
        MembershipSet::new(vec![NodeId(7), NodeId(8), NodeId(9)], Vec::new())
            .expect("test membership is valid"),
    );

    assert_refused_and_unchanged(
        &mut node,
        test_snapshot_with_committed_voters(5, 1, 1, b"foreign membership", &[7, 8, 9]),
        &crate::LocalSnapshotInstallError::CommittedMembershipMismatch {
            snapshot_index: LogIndex(5),
            expected: Box::new(local),
            actual: Box::new(foreign),
        },
    );
}

/// A descriptor carrying no committed configuration is accepted, and the node
/// keeps deriving that state locally rather than having the kernel rewrite the
/// caller's descriptor.
#[test]
fn local_snapshot_without_committed_configuration_derives_it_locally() {
    let mut node = applied_node(10, 1);
    let expected = node.membership_at_index(LogIndex(5));

    let _ = node
        .install_local_snapshot(test_snapshot(5, 1, 1, b"no configuration metadata"))
        .expect("a descriptor without committed configuration installs");

    assert_eq!(node.snapshot_committed_membership(), None);
    assert_eq!(node.committed_membership(), expected);
    node.validate_derived_state()
        .expect("derived state stays valid");
}

/// The success path: a matching boundary term keeps the suffix above it, both
/// indexes stay where they were, and the geometry the refusals protect holds.
#[test]
fn valid_local_snapshot_retains_the_suffix_and_moves_no_index() {
    let mut node = applied_node(10, 1);

    let outputs = node
        .install_local_snapshot(test_snapshot(6, 1, 1, b"compaction through six"))
        .expect("a matching boundary at or below the applied index installs");

    assert!(
        outputs.is_empty(),
        "a valid local install retains the suffix, so it drops no tracked proposal"
    );
    assert_eq!(node.snapshot_index(), LogIndex(6));
    assert_eq!(node.last_log_index(), LogIndex(10));
    assert_eq!(node.commit_index(), LogIndex(10));
    assert_eq!(node.applied_index(), LogIndex(10));
    assert_eq!(
        node.log_entries_from(LogIndex(1)),
        (7..=10)
            .map(|index| LogEntry::application(Term(1), format!("entry-{index}").into_bytes()))
            .collect::<Vec<_>>()
    );
    node.validate_derived_state()
        .expect("a valid compaction leaves derived state valid");
}

/// The leader-sent path keeps discarding a suffix whose term the descriptor
/// contradicts. That is what makes an installed snapshot commit evidence, and
/// hardening the local path must not reach it.
#[test]
fn remote_install_still_discards_a_suffix_whose_term_the_snapshot_contradicts() {
    let mut follower = node(2, &[1, 3]);
    push_log_entry(&mut follower, Term(2), b"local-one");
    push_log_entry(&mut follower, Term(2), b"local-two");
    push_log_entry(&mut follower, Term(2), b"local-three");
    let snapshot = test_snapshot(2, 4, 5, b"divergent history");

    let outputs = follower.step(Input::Message {
        from: NodeId(1),
        message: Message::InstallSnapshot(crate::InstallSnapshot {
            term: Term(5),
            leader_id: NodeId(1),
            metadata: snapshot.metadata.clone(),
            application_payload: b"divergent history".to_vec(),
        }),
    });

    assert!(outputs
        .iter()
        .any(|output| matches!(output, Output::ApplySnapshot { .. })));
    assert_eq!(follower.snapshot_index(), LogIndex(2));
    assert_eq!(
        follower.last_log_index(),
        LogIndex(2),
        "the leader-sent install discards a suffix it cannot prove belongs to this history"
    );
    assert_eq!(follower.log_entries_from(LogIndex(1)), Vec::new());
    assert_eq!(follower.commit_index(), LogIndex(2));
    follower
        .validate_derived_state()
        .expect("the remote install leaves derived state valid");
}
