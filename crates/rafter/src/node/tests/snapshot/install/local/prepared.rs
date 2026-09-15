//! Prepared local-snapshot ownership and deferred-retirement scenarios.

use super::*;

#[test]
fn prepared_local_snapshot_is_inert_until_committed() {
    let mut node = applied_node(10, 1);
    let snapshot = test_snapshot(8, 1, 1, b"prepared");
    let before = NodeSnapshotState::of(&node);

    let prepared = node
        .prepare_local_snapshot_install(snapshot.clone())
        .expect("the valid transition prepares");
    drop(prepared);

    assert_eq!(NodeSnapshotState::of(&node), before);
    let outputs = node
        .prepare_local_snapshot_install(snapshot.clone())
        .expect("the unchanged node prepares again")
        .commit();
    assert!(outputs.is_empty());
    assert_eq!(node.snapshot(), Some(&snapshot));
    assert_eq!(node.snapshot_index(), LogIndex(8));
    assert_eq!(node.last_log_index(), LogIndex(10));
    node.validate_derived_state()
        .expect("the prepared transition commits valid derived state");
}

#[test]
fn prepared_local_snapshot_can_return_retired_entries_for_deferred_drop() {
    let mut node = applied_node(10, 1);
    let snapshot = test_snapshot(8, 1, 1, b"prepared");

    let (outputs, retired) = node
        .prepare_local_snapshot_install(snapshot.clone())
        .expect("the valid transition prepares")
        .commit_with_retired_entries();

    assert!(outputs.is_empty());
    assert_eq!(retired.len(), 8);
    assert_eq!(retired.payload_bytes(), 56);
    assert_eq!(node.snapshot(), Some(&snapshot));
    assert_eq!(node.last_log_index(), LogIndex(10));
    node.validate_derived_state()
        .expect("deferred retirement leaves derived state valid");

    let (_, rerecorded) = node
        .prepare_local_snapshot_install(snapshot)
        .expect("the installed boundary prepares again")
        .commit_with_retired_entries();
    assert!(rerecorded.is_empty());
}

#[test]
fn committed_local_snapshot_abandons_pending_inbound_transfer() {
    let mut node = applied_node(10, 1);
    let incoming = test_snapshot(11, 1, 1, b"future snapshot");
    node.resume_pending_snapshot_transfer(PendingSnapshotTransfer {
        leader_id: NodeId(1),
        transfer_id: incoming.transfer_id(),
        metadata: incoming.metadata.clone(),
        total_payload_len: incoming.application_payload_len,
        application_payload_crc32: incoming.application_payload_crc32,
        received_len: 4,
    })
    .expect("partial inbound transfer resumes");
    assert!(node.pending_snapshot_transfer().is_some());

    let _ = node
        .prepare_local_snapshot_install(test_snapshot(8, 1, 1, b"local snapshot"))
        .expect("local snapshot prepares")
        .commit();

    assert_eq!(node.pending_snapshot_transfer(), None);
    node.validate_derived_state()
        .expect("abandoning the inbound transfer leaves valid state");
}
