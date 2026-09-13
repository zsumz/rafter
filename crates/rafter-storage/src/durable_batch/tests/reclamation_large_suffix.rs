//! Repeated reclamation with large live suffixes and phase telemetry.

use super::*;
use std::collections::BTreeMap;
use std::sync::{mpsc, Arc, TryLockError};
use std::thread;
use std::time::Instant;

#[test]
fn repeated_reclamation_preserves_large_retained_suffixes_and_reports_each_phase() {
    for retained in [10_000, 100_000] {
        let dir = Directory::new();
        let (h, mut l, mut s) = dir.open();
        let total = retained + 1;
        let entries: Vec<_> = (1..=total)
            .map(|index| entry(index, &index.to_be_bytes()))
            .collect();
        publish(&h, &mut l, &entries, total, None).unwrap();

        let before: BTreeMap<_, _> = crate::telemetry::snapshot()
            .into_iter()
            .map(|(name, metric)| (name, metric.calls))
            .collect();
        crate::telemetry::set_enabled(true);
        s.write_snapshot(snapshot(1)).unwrap();
        l.compact_prefix_through(LogIndex(1)).unwrap();
        publish(
            &h,
            &mut l,
            &[entry(total + 1, &(total + 1).to_be_bytes())],
            total + 1,
            None,
        )
        .unwrap();
        s.write_snapshot(snapshot(2)).unwrap();
        l.compact_prefix_through(LogIndex(2)).unwrap();
        crate::telemetry::set_enabled(false);

        let after: BTreeMap<_, _> = crate::telemetry::snapshot()
            .into_iter()
            .map(|(name, metric)| (name, metric.calls))
            .collect();
        for name in [
            "wal_reclamation",
            "wal_checkpoint_prepare",
            "wal_manifest_publish",
            "wal_cleanup",
        ] {
            assert_eq!(after[name] - before[name], 2, "{name}; retained {retained}");
        }
        assert_eq!(super::reclamation::managed_generation_files(&dir), 3);
        drop((h, l, s));

        let (h, l, _s) = dir.open();
        assert_eq!(h.current(), hard(total + 1));
        assert_eq!(l.compacted_through(), LogIndex(2));
        let replay = l.replay_entries();
        assert_eq!(replay.len(), usize::try_from(retained).unwrap());
        assert_eq!(replay.first(), Some(&entries[2]));
        assert_eq!(
            replay.last(),
            Some(&entry(total + 1, &(total + 1).to_be_bytes()))
        );
    }
}

#[test]
fn append_waiting_on_reclamation_resumes_durably_and_reopens_in_order() {
    let dir = Directory::new();
    let (h, mut l, mut s) = dir.open();
    let total = 10_001;
    let entries: Vec<_> = (1..=total)
        .map(|index| entry(index, &index.to_be_bytes()))
        .collect();
    publish(&h, &mut l, &entries, total, None).unwrap();
    s.write_snapshot(snapshot(1)).unwrap();

    let shared = l.0.clone();
    let gate = Arc::new(super::super::state::ReclamationGate::new());
    shared.lock().unwrap().reclaim_gate = Some(gate.clone());
    let compaction = thread::spawn(move || {
        crate::telemetry::set_enabled(true);
        l.compact_prefix_through(LogIndex(1)).unwrap();
        crate::telemetry::set_enabled(false);
        crate::telemetry::snapshot()
            .into_iter()
            .collect::<BTreeMap<_, _>>()
    });

    gate.wait_until_entered();
    assert!(matches!(shared.try_lock(), Err(TryLockError::WouldBlock)));
    let (attempted_tx, attempted_rx) = mpsc::channel();
    let append_shared = shared.clone();
    let append = thread::spawn(move || {
        assert!(matches!(
            append_shared.try_lock(),
            Err(TryLockError::WouldBlock)
        ));
        attempted_tx.send(()).unwrap();
        let append_hard = WalRaftHardStateStore(append_shared.clone());
        let mut append_log = WalRaftLogSegment(append_shared);
        let started = Instant::now();
        let receipt = publish(
            &append_hard,
            &mut append_log,
            &[entry(total + 1, &(total + 1).to_be_bytes())],
            total + 1,
            None,
        )
        .unwrap();
        (receipt, started.elapsed())
    });
    attempted_rx.recv().unwrap();
    gate.release();

    let metrics = compaction.join().unwrap();
    let (receipt, append_wait) = append.join().unwrap();
    assert_eq!(receipt.hard_state, hard(total + 1));
    assert!(append_wait > std::time::Duration::ZERO);
    for name in [
        "wal_reclamation",
        "wal_checkpoint_prepare",
        "wal_manifest_publish",
        "wal_cleanup",
    ] {
        assert_eq!(metrics[name].calls, 1, "{name}");
        assert!(metrics[name].max_ns > 0, "{name}");
    }
    assert_eq!(super::reclamation::managed_generation_files(&dir), 3);

    drop((h, s, shared, gate));
    let (h, l, _s) = dir.open();
    assert_eq!(h.current(), hard(total + 1));
    assert_eq!(l.compacted_through(), LogIndex(1));
    let replay = l.replay_entries();
    assert_eq!(replay.len(), usize::try_from(total).unwrap());
    assert_eq!(replay.first(), Some(&entries[1]));
    assert_eq!(
        replay.last(),
        Some(&entry(total + 1, &(total + 1).to_be_bytes()))
    );
}
