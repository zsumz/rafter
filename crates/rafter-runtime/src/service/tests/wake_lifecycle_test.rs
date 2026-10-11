//! Worker restarts retain the notification contract without stale-token masking.

use super::{super::*, support::*, wake_test::*};
use rafter::{Input, LogIndex};
use std::time::Duration;

#[test]
fn snapshot_resumption_keeps_wakes_for_replayed_suffix() {
    let (mut owner, wake) = notified(super::lifecycle_test::snapshotted(), Store::default(), 1);
    let ServicePoll::Event(ServiceEvent::InstallSnapshot {
        snapshot,
        mut application,
    }) = owner.poll_next().unwrap()
    else {
        panic!("recovery snapshot required");
    };
    assert!(matches!(owner.poll_next().unwrap(), ServicePoll::Pending));
    application.floor = snapshot.metadata.last_included_index;
    let (started, release) = gate(&mut application);
    // The old worker is joined. Remove its terminal wake before restarting.
    while wake.try_recv().is_ok() {}
    owner.resume_snapshot(application).unwrap();
    let mut release = release;
    assert!(drain(&mut owner).is_empty());
    started.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(wake.try_recv().is_err());
    release.now();
    wake.recv_timeout(Duration::from_secs(5)).unwrap();
    let events = finish_recovery(&mut owner, &wake);
    assert!(
        matches!(events.as_slice(), [ServiceEvent::Applied(done)] if done.entries()[0].last == LogIndex(5))
    );
}

#[test]
fn local_compaction_keeps_wakes_for_new_application_work() {
    let (mut owner, wake) = notified(log_with_commands(3), Store::default(), 1);
    finish_recovery(&mut owner, &wake);
    let mut next_gate = None;
    drop(
        owner
            .compact_snapshot_with(|store, node| {
                next_gate = Some(gate(store));
                super::compaction_test::snapshot(store, node)
            })
            .unwrap(),
    );
    let (started, release) = next_gate.unwrap();
    // Joining the old worker is synchronous: its wake cannot arrive later.
    while wake.try_recv().is_ok() {}
    owner.try_step(Input::Tick.into()).unwrap();
    drain(&mut owner);
    owner
        .try_step(Input::ClientProposal { payload: vec![1] }.into())
        .unwrap();
    let mut release = release;
    assert!(drain(&mut owner)
        .iter()
        .all(|e| !matches!(e, ServiceEvent::Applied(_))));
    started.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(wake.try_recv().is_err());
    release.now();
    wake.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(
        matches!(drain(&mut owner).as_slice(), [ServiceEvent::Applied(done)] if done.entries()[0].last == LogIndex(6))
    );
    owner.begin_shutdown();
    assert_eq!(owner.finish_shutdown().unwrap().floor, LogIndex(6));
}
