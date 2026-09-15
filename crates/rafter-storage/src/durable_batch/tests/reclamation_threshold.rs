//! Deferred physical reclamation retains immediate logical-compaction durability.

use super::*;

#[test]
fn threshold_defers_only_physical_reclamation_and_reopens_logical_compaction() {
    let dir = Directory::new();
    let (h, mut l, mut s) = dir.open_with_reclamation_threshold(1024);
    let entries: Vec<_> = (1..=4)
        .map(|index| entry(index, &[u8::try_from(index).unwrap(); 16]))
        .collect();
    publish(&h, &mut l, &entries, 4, None).unwrap();
    s.write_snapshot(snapshot(2)).unwrap();
    l.compact_prefix_through(LogIndex(2)).unwrap();
    assert_eq!(l.sync_count(), 2);
    assert_eq!(l.compacted_through(), LogIndex(2));
    assert_eq!(l.replay_entries(), entries[2..]);
    assert_eq!(reclamation::managed_generation_files(&dir), 0);
    assert!(dir.0.join("hard-state").exists());
    drop((h, l, s));

    let (h, mut l, mut s) = dir.open_with_reclamation_threshold(1024);
    assert_eq!(l.compacted_through(), LogIndex(2));
    assert_eq!(l.replay_entries(), entries[2..]);
    let large = entry(5, &[5; 2048]);
    let receipt = publish(&h, &mut l, std::slice::from_ref(&large), 5, None).unwrap();
    assert_eq!(receipt.operation, 3);
    s.write_snapshot(snapshot(3)).unwrap();
    l.compact_prefix_through(LogIndex(3)).unwrap();
    assert_eq!(reclamation::managed_generation_files(&dir), 3);
    assert!(!dir.0.join("hard-state").exists());
    assert_eq!(l.compacted_through(), LogIndex(3));
    assert_eq!(l.replay_entries(), vec![entries[3].clone(), large]);
    let receipt = publish(&h, &mut l, &[entry(6, b"next")], 6, None).unwrap();
    assert_eq!(receipt.operation, 5);
}

#[test]
fn deferred_compaction_still_requires_a_covering_snapshot_on_reopen() {
    let dir = Directory::new();
    let (h, mut l, mut s) = dir.open_with_reclamation_threshold(1024 * 1024);
    publish(&h, &mut l, &[entry(1, b"one")], 1, None).unwrap();
    s.write_snapshot(snapshot(1)).unwrap();
    l.compact_prefix_through(LogIndex(1)).unwrap();
    assert_eq!(reclamation::managed_generation_files(&dir), 0);
    drop((h, l, s));

    fs::remove_dir_all(dir.0.join("snapshots")).unwrap();
    fs::create_dir(dir.0.join("snapshots")).unwrap();
    let error = WalRaftNodeStores::open_with_options(
        &dir.0,
        WalRaftNodeStoresOptions::new()
            .with_reclamation_threshold_bytes(std::num::NonZeroU64::new(1024 * 1024).unwrap()),
    )
    .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    assert!(error
        .to_string()
        .contains("compacted WAL requires a current snapshot"));
}
