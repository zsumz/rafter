//! Append rejection reasons preserve the pre-splice state and their distinct rules.

use super::AppendRejection;
use crate::{
    BootstrapLogEntry, BootstrapState, ConfigurationEntry, ConfigurationId, LogEntry, LogIndex,
    MembershipSet, Node, NodeConfig, NodeId, Term,
};

fn node(commit: u64) -> Node {
    Node::from_bootstrap(
        NodeConfig::new(NodeId(1), vec![NodeId(2), NodeId(3)], 10).unwrap(),
        BootstrapState {
            current_term: Term(3),
            voted_for: None,
            commit_index: LogIndex(commit),
            committed_configuration: None,
            snapshot: None,
            log: vec![BootstrapLogEntry::application(
                LogIndex(1),
                Term(1),
                vec![1],
            )],
        },
    )
    .unwrap()
}

#[test]
fn committed_conflict_identifies_the_protected_index() {
    let mut node = node(1);
    let before = node.clone();
    assert_eq!(
        node.splice_entries_after(
            LogIndex::ZERO,
            &vec![LogEntry::application(Term(3), vec![2])].into(),
            LogIndex(1),
        ),
        Err(AppendRejection::CommittedEntryConflict { index: LogIndex(1) })
    );
    assert_eq!(node, before);
}

#[test]
fn new_configuration_overlap_is_distinct_from_a_committed_conflict() {
    let mut node = node(0);
    let before = node.clone();
    let membership = MembershipSet::new(vec![NodeId(1), NodeId(2), NodeId(3)], vec![]).unwrap();
    let entries = (1..=2)
        .map(|id| {
            LogEntry::configuration(
                Term(3),
                ConfigurationEntry::stable(ConfigurationId(id), membership.clone()),
            )
        })
        .collect::<Vec<_>>()
        .into();
    assert_eq!(
        node.splice_entries_after(LogIndex(1), &entries, LogIndex::ZERO),
        Err(AppendRejection::OverlappingConfigurationChanges)
    );
    assert_eq!(node, before);
    // The same current-term history is accepted once this frame establishes
    // its predecessor's commitment; entry terms alone cannot identify overlap.
    assert_eq!(
        node.splice_entries_after(LogIndex(1), &entries, LogIndex(3)),
        Ok(vec![])
    );
    assert_eq!(node.last_log_index(), LogIndex(3));
}
