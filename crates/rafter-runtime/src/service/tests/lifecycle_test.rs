//! Snapshot replacement and recovery boundaries.

use super::{super::*, support::*};
use rafter::{
    ApplicationSnapshotKind, ApplicationSnapshotMetadata, ApplicationSnapshotVersion, Input,
    LogIndex, RaftSnapshotMetadata, SnapshotGroupId,
};
use rafter_storage::PersistedRaftSnapshot;

fn snapshotted() -> Node {
    let mut node = log_with_commands(3);
    let metadata = RaftSnapshotMetadata::new(
        SnapshotGroupId::new("owner-tests").unwrap(),
        node.id(),
        LogIndex(4),
        node.term_at_index(LogIndex(4)).unwrap(),
        node.current_term(),
        ApplicationSnapshotMetadata::new(
            ApplicationSnapshotKind::new("counter").unwrap(),
            ApplicationSnapshotVersion::new(1).unwrap(),
        ),
    )
    .unwrap();
    node.compact_log_with_snapshot(PersistedRaftSnapshot {
        metadata,
        application_payload: b"three".to_vec(),
    })
    .unwrap();
    node.step(Input::ClientProposal {
        payload: b"four".to_vec(),
    })
    .unwrap();
    node
}

#[test]
fn recovery_hands_off_snapshot_once_before_suffix_and_keeps_shutdown_closed() {
    let mut service = owner(snapshotted(), Store::default(), 1);
    let Some(ServiceEvent::InstallSnapshot {
        snapshot,
        mut application,
    }) = service.poll().unwrap()
    else {
        panic!("snapshot handoff required");
    };
    assert_eq!(snapshot.metadata.last_included_index, LogIndex(4));
    assert_eq!(service.state(), ServiceState::InstallingSnapshot);
    assert!(service.poll().unwrap().is_none());
    assert!(matches!(
        service.try_step(ServiceInput::from(Input::Tick)),
        Err(ServiceStepError::Unavailable {
            state: ServiceState::InstallingSnapshot,
            ..
        })
    ));
    service.begin_shutdown();
    assert!(service.finish_shutdown().is_err());
    application.floor = LogIndex(4);
    service.resume_snapshot(application).unwrap();
    let events = until(&mut service, DurableService::idle);
    assert!(
        matches!(events.as_slice(), [ServiceEvent::Applied(done)] if done.entries()[0].first == LogIndex(5) && done.entries()[0].last == LogIndex(5))
    );
    assert_eq!(service.finish_shutdown().unwrap().floor, LogIndex(5));
}

#[test]
fn snapshot_floor_mismatch_poison_prevents_suffix_application() {
    let mut service = owner(snapshotted(), Store::default(), 1);
    let Some(ServiceEvent::InstallSnapshot { application, .. }) = service.poll().unwrap() else {
        panic!("handoff");
    };
    assert!(matches!(
        service.resume_snapshot(application),
        Err(ServiceError::ApplicationFloor {
            expected: LogIndex(4),
            actual: LogIndex::ZERO
        })
    ));
    assert_eq!(service.state(), ServiceState::Failed);
    assert!(service.poll().is_err());
}

#[test]
fn already_installed_recovery_snapshot_is_not_reapplied() {
    let mut service = owner(
        snapshotted(),
        Store {
            floor: LogIndex(4),
            ..Store::default()
        },
        1,
    );
    let events = until(&mut service, |s| s.state() == ServiceState::Ready);
    assert!(
        matches!(events.as_slice(), [ServiceEvent::Applied(done)] if done.entries()[0].last == LogIndex(5))
    );
}

#[test]
fn input_limits_refuse_without_changing_consensus_or_consuming_input() {
    let mut service = empty();
    until(&mut service, |s| s.state() == ServiceState::Ready);
    let before = service.progress();
    for input in [
        ServiceInput::Proposals(vec![]),
        ServiceInput::from(Input::ClientProposal {
            payload: vec![0; 256 * 1024 + 1],
        }),
    ] {
        assert!(
            matches!(service.try_step(input.clone()), Err(ServiceStepError::ProposalLimit { input: original }) if original == input)
        );
        assert_eq!(service.progress(), before);
    }
}
