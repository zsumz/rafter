use std::num::NonZeroUsize;

use rafter_multiraft::managed::ManagedConfig;
use rafter_reference_sharded_counter::{
    adapter::{
        CounterAdmissionRejection, CounterGroupCheckpoint, ManagedCounterCluster, NetworkConfig,
        RoutedPeerEnvelope,
    },
    AdmissionRejection, ClientId, CounterCommand, GroupId, GroupIncarnation, GroupLifecycle,
    LifecycleOutcome, LifecycleRequest, RequestFingerprint, RequestIdentity, Sequence,
    SessionEpoch, SystemClass, WorkClass, WorkQuota,
};

fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).expect("test bound is nonzero")
}

fn cluster() -> ManagedCounterCluster {
    ManagedCounterCluster::new(
        ManagedConfig::new(nonzero(2), nonzero(8), nonzero(16), nonzero(2))
            .expect("managed bounds are valid"),
        NetworkConfig {
            max_pending_messages: nonzero(256),
            max_sessions_per_group: nonzero(2),
        },
    )
}

fn request() -> RequestIdentity {
    RequestIdentity {
        client_id: ClientId::new(0),
        session_epoch: SessionEpoch::new(1).expect("epoch is nonzero"),
        sequence: Sequence::new(1).expect("sequence is nonzero"),
        fingerprint: RequestFingerprint::of(&CounterCommand::Read),
    }
}

fn capture_recovery_peer(
    cluster: &mut ManagedCounterCluster,
    group_id: GroupId,
) -> RoutedPeerEnvelope {
    cluster
        .lifecycle(
            group_id,
            LifecycleRequest::Create {
                quota: WorkQuota::new(2).expect("quota is nonzero"),
            },
        )
        .expect("group opens");
    cluster
        .lifecycle(group_id, LifecycleRequest::Recover)
        .expect("recovery starts");
    cluster
        .drive_round()
        .expect("one recovery round emits peer traffic");
    let peer = cluster
        .take_pending_peer()
        .expect("three-node recovery emits more than one peer envelope");
    cluster
        .drive_until_idle(256)
        .expect("remaining recovery traffic quiesces");
    cluster
        .lifecycle(group_id, LifecycleRequest::Serve)
        .expect("group serves");
    peer
}

fn assert_client_refusal(
    cluster: &mut ManagedCounterCluster,
    group_id: GroupId,
    incarnation: GroupIncarnation,
    expected: AdmissionRejection,
) {
    let rejected = cluster
        .submit_for(group_id, incarnation, request(), CounterCommand::Read)
        .expect_err("late client traffic is refused");
    assert_eq!(rejected.reason, CounterAdmissionRejection::Policy(expected));
}

fn assert_peer_refusal(
    cluster: &mut ManagedCounterCluster,
    peer: RoutedPeerEnvelope,
    expected: AdmissionRejection,
) {
    cluster
        .enqueue_peer(peer)
        .expect("one late envelope fits the bounded network");
    let report = cluster
        .drive_round()
        .expect("late peer traffic is a policy value");
    assert_eq!(report.refused_peer_traffic.len(), 1);
    assert_eq!(report.refused_peer_traffic[0].reason, expected);
}

fn drain_remove_and_checkpoint(
    cluster: &mut ManagedCounterCluster,
    group_id: GroupId,
    old_peer: &RoutedPeerEnvelope,
) -> CounterGroupCheckpoint {
    let first = GroupIncarnation::first();
    cluster
        .lifecycle(group_id, LifecycleRequest::Drain)
        .expect("serving group starts draining");
    assert_client_refusal(
        cluster,
        group_id,
        first,
        AdmissionRejection::GroupNotAcceptingWork {
            state: GroupLifecycle::Draining,
            class: WorkClass::Command,
        },
    );
    assert_peer_refusal(
        cluster,
        old_peer.clone(),
        AdmissionRejection::GroupNotAcceptingWork {
            state: GroupLifecycle::Draining,
            class: WorkClass::Control,
        },
    );

    cluster
        .lifecycle(group_id, LifecycleRequest::Remove)
        .expect("drained group removes");
    assert_client_refusal(
        cluster,
        group_id,
        first,
        AdmissionRejection::GroupNotAcceptingWork {
            state: GroupLifecycle::Removed,
            class: WorkClass::Command,
        },
    );
    assert_peer_refusal(
        cluster,
        old_peer.clone(),
        AdmissionRejection::GroupNotAcceptingWork {
            state: GroupLifecycle::Removed,
            class: WorkClass::Control,
        },
    );
    cluster
        .checkpoint_group(group_id)
        .expect("removed identity checkpoints")
}

fn restart_removed_and_reopen(
    group_id: GroupId,
    removed: CounterGroupCheckpoint,
    old_peer: &RoutedPeerEnvelope,
) -> ManagedCounterCluster {
    let first = GroupIncarnation::first();
    let mut restarted = cluster();
    restarted
        .restore_inactive_checkpoint(removed, 2)
        .expect("removed identity survives local restart");
    assert_client_refusal(
        &mut restarted,
        group_id,
        first,
        AdmissionRejection::GroupNotAcceptingWork {
            state: GroupLifecycle::Removed,
            class: WorkClass::Command,
        },
    );
    assert_peer_refusal(
        &mut restarted,
        old_peer.clone(),
        AdmissionRejection::GroupNotAcceptingWork {
            state: GroupLifecycle::Removed,
            class: WorkClass::Control,
        },
    );

    restarted
        .lifecycle(
            group_id,
            LifecycleRequest::Create {
                quota: WorkQuota::new(1).expect("quota is nonzero"),
            },
        )
        .expect("removed group reopens");
    let second = GroupIncarnation::new(2).expect("second incarnation is nonzero");
    assert_client_refusal(
        &mut restarted,
        group_id,
        first,
        AdmissionRejection::StaleIncarnation { current: second },
    );
    assert_peer_refusal(
        &mut restarted,
        old_peer.clone(),
        AdmissionRejection::StaleIncarnation { current: second },
    );
    restarted
}

fn tombstone_and_restart(
    mut restarted: ManagedCounterCluster,
    group_id: GroupId,
    old_peer: RoutedPeerEnvelope,
) {
    let second = GroupIncarnation::new(2).expect("second incarnation is nonzero");
    restarted
        .lifecycle(group_id, LifecycleRequest::Drain)
        .expect("reopened group drains");
    restarted
        .lifecycle(group_id, LifecycleRequest::Remove)
        .expect("reopened group removes");
    restarted
        .lifecycle(group_id, LifecycleRequest::Tombstone)
        .expect("removed identity tombstones");
    assert_client_refusal(
        &mut restarted,
        group_id,
        second,
        AdmissionRejection::GroupTombstoned,
    );
    let mut current_peer = old_peer;
    current_peer.incarnation = second;
    assert_peer_refusal(
        &mut restarted,
        current_peer.clone(),
        AdmissionRejection::GroupTombstoned,
    );

    let tombstone = restarted
        .checkpoint_group(group_id)
        .expect("tombstone checkpoints");
    let mut restarted_again = cluster();
    restarted_again
        .restore_inactive_checkpoint(tombstone, 2)
        .expect("tombstone survives local restart");
    assert_client_refusal(
        &mut restarted_again,
        group_id,
        second,
        AdmissionRejection::GroupTombstoned,
    );
    assert_peer_refusal(
        &mut restarted_again,
        current_peer,
        AdmissionRejection::GroupTombstoned,
    );
}

#[test]
fn late_client_and_peer_traffic_is_explicit_across_every_inactive_boundary() {
    let group_id = GroupId::new(7);
    let mut live = cluster();
    let old_peer = capture_recovery_peer(&mut live, group_id);
    let removed = drain_remove_and_checkpoint(&mut live, group_id, &old_peer);
    let restarted = restart_removed_and_reopen(group_id, removed, &old_peer);
    tombstone_and_restart(restarted, group_id, old_peer);
}

#[test]
fn draining_protocol_continues_only_while_accepted_proposal_remains() {
    let group_id = GroupId::new(9);
    let first = GroupIncarnation::first();
    let mut cluster = cluster();
    let peer = capture_recovery_peer(&mut cluster, group_id);
    cluster.set_service_delay(group_id, 4);
    cluster
        .open_session_for(
            group_id,
            first,
            ClientId::new(1),
            SessionEpoch::new(1).expect("epoch is nonzero"),
        )
        .expect("session proposal is accepted");
    cluster
        .lifecycle(group_id, LifecycleRequest::Drain)
        .expect("serving group starts draining");

    cluster
        .enqueue_peer(peer.clone())
        .expect("one continuation frame fits");
    let continuing = cluster
        .drive_round()
        .expect("accepted work permits current-incarnation continuation");
    assert!(
        continuing.refused_peer_traffic.is_empty(),
        "peer traffic is continuation while the accepted proposal remains"
    );
    cluster
        .submit_system(group_id, first, SystemClass::Control)
        .expect("a protocol tick continues the accepted proposal");
    cluster
        .drive_until_idle(256)
        .expect("the accepted proposal and its continuation settle");

    assert_peer_refusal(
        &mut cluster,
        peer,
        AdmissionRejection::GroupNotAcceptingWork {
            state: GroupLifecycle::Draining,
            class: WorkClass::Control,
        },
    );
    let tick = cluster
        .submit_system(group_id, first, SystemClass::Control)
        .expect_err("protocol traffic cannot keep an empty drain alive");
    assert_eq!(
        tick.reason,
        CounterAdmissionRejection::Policy(AdmissionRejection::GroupNotAcceptingWork {
            state: GroupLifecycle::Draining,
            class: WorkClass::Control,
        })
    );
    assert!(matches!(
        cluster
            .lifecycle(group_id, LifecycleRequest::Remove)
            .expect("empty draining group removes")
            .outcome,
        LifecycleOutcome::Applied {
            to: GroupLifecycle::Removed,
            ..
        }
    ));
}
