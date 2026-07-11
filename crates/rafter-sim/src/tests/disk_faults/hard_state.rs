use super::super::*;
use super::fixtures::{
    committed_configuration_bootstrap, retained_last_log_index, test_node_config,
};
use crate::disk_fault::FaultInjectingDisk;
use rafter::BootstrapValidationError;

#[test]
fn lost_unfsynced_suffix_is_term_vote_only_survival() {
    let clean = committed_configuration_bootstrap();
    let recovery = FaultInjectingDisk::new(clean.clone()).lost_unfsynced_suffix(LogIndex(1));

    assert_eq!(recovery.bootstrap.current_term, clean.current_term);
    assert_eq!(recovery.bootstrap.voted_for, clean.voted_for);
    assert_eq!(recovery.bootstrap.commit_index, LogIndex::ZERO);
    assert_eq!(recovery.bootstrap.committed_configuration, None);
    assert_eq!(retained_last_log_index(&recovery.bootstrap), LogIndex(1));

    let node = Node::from_bootstrap(test_node_config(), recovery.bootstrap)
        .expect("term/vote-only truncation remains a legal bootstrap image");
    assert_eq!(node.commit_index(), LogIndex::ZERO);
}

#[test]
fn hard_state_log_reorder_preserves_commit_and_config_beyond_retained_log() {
    let clean = committed_configuration_bootstrap();
    let recovery = FaultInjectingDisk::new(clean.clone()).hard_state_log_reorder(LogIndex(1));

    assert_eq!(recovery.bootstrap.current_term, clean.current_term);
    assert_eq!(recovery.bootstrap.voted_for, clean.voted_for);
    assert_eq!(recovery.bootstrap.commit_index, clean.commit_index);
    assert_eq!(
        recovery.bootstrap.committed_configuration,
        clean.committed_configuration
    );
    assert_eq!(retained_last_log_index(&recovery.bootstrap), LogIndex(1));

    let error = Node::from_bootstrap(test_node_config(), recovery.bootstrap)
        .expect_err("commit index ahead of retained log should be rejected");
    assert!(matches!(
        error,
        BootstrapValidationError::CommitIndexBeyondLog {
            commit_index: LogIndex(2),
            last_log_index: LogIndex(1),
        }
    ));
}

#[test]
fn hard_state_log_reorder_reopens_when_retained_log_covers_commit() {
    let clean = committed_configuration_bootstrap();
    let recovery = FaultInjectingDisk::new(clean.clone()).hard_state_log_reorder(LogIndex(2));

    assert_eq!(retained_last_log_index(&recovery.bootstrap), LogIndex(2));
    let node = Node::from_bootstrap(test_node_config(), recovery.bootstrap)
        .expect("retained log covers committed hard state");

    assert_eq!(node.commit_index(), clean.commit_index);
    assert_eq!(
        node.committed_configuration_state(),
        clean.committed_configuration
    );
}
