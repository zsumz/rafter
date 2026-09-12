//! Checkpoint publication, crash windows, reclamation, and selected-generation corruption.

use super::*;
use crate::{storage_failpoint_test::arm, storage_failpoint_test::DurabilityPoint};

const CHECKPOINT_PREFIX: &str = "raft-wal-checkpoint-";
const SEGMENT_PREFIX: &str = "raft-wal-segment-";

#[test]
fn compaction_bounds_physical_history_and_continues_publication_sequence() {
    let dir = Directory::new();
    let (h, mut l, mut s) = dir.open();
    let entries: Vec<_> = (1..=100)
        .map(|index| entry(index, &[u8::try_from(index).unwrap(); 128]))
        .collect();
    let first = publish(&h, &mut l, &entries, 100, None).unwrap();
    assert_eq!(first.operation, 1);
    let legacy_bytes = fs::metadata(dir.0.join("hard-state")).unwrap().len();

    s.write_snapshot(snapshot(90)).unwrap();
    l.compact_prefix_through(LogIndex(90)).unwrap();
    assert_eq!(l.sync_count(), 5);
    assert_eq!(l.compacted_through(), LogIndex(90));
    assert_eq!(l.replay_entries(), entries[90..]);
    assert!(!dir.0.join("hard-state").exists());
    assert_eq!(managed_generation_files(&dir), 3);
    assert!(managed_generation_bytes(&dir) < legacy_bytes);

    let receipt = publish(&h, &mut l, &[entry(101, b"next")], 101, None).unwrap();
    assert_eq!(receipt.operation, 3);
    drop((h, l, s));

    let (h, l, _s) = dir.open();
    assert_eq!(h.current(), hard(101));
    assert_eq!(l.compacted_through(), LogIndex(90));
    assert_eq!(l.replay_entries().last(), Some(&entry(101, b"next")));
}

#[test]
fn later_checkpoint_reclaims_the_previous_generation() {
    let dir = Directory::new();
    let (h, mut l, mut s) = dir.open();
    let entries: Vec<_> = (1..=4)
        .map(|index| entry(index, &[u8::try_from(index).unwrap()]))
        .collect();
    publish(&h, &mut l, &entries, 4, None).unwrap();
    s.write_snapshot(snapshot(2)).unwrap();
    l.compact_prefix_through(LogIndex(2)).unwrap();
    let first_names = managed_names(&dir);
    assert!(first_names
        .iter()
        .any(|name| name.contains("00000000000000000001")));

    s.write_snapshot(snapshot(3)).unwrap();
    l.compact_prefix_through(LogIndex(3)).unwrap();
    let second_names = managed_names(&dir);
    assert_eq!(second_names.len(), 3);
    assert!(second_names
        .iter()
        .filter(|name| name.starts_with(CHECKPOINT_PREFIX) || name.starts_with(SEGMENT_PREFIX))
        .all(|name| name.contains("00000000000000000002")));
    assert!(second_names
        .iter()
        .all(|name| !name.contains("00000000000000000001")));
}

#[test]
fn repeated_checkpoint_restart_soak_keeps_only_live_wal_state() {
    let dir = Directory::new();
    let mut first_generation_bytes = None;
    for index in 1..=64 {
        let (h, mut l, mut s) = dir.open();
        publish(
            &h,
            &mut l,
            &[entry(index, &[u8::try_from(index).unwrap(); 32])],
            index,
            None,
        )
        .unwrap();
        s.write_snapshot(snapshot(index)).unwrap();
        l.compact_prefix_through(LogIndex(index)).unwrap();
        assert_eq!(l.compacted_through(), LogIndex(index));
        assert!(l.replay_entries().is_empty());
        assert_eq!(managed_generation_files(&dir), 3);
        let bytes = managed_generation_bytes(&dir);
        let first = *first_generation_bytes.get_or_insert(bytes);
        assert_eq!(bytes, first, "generation {index}");
        drop((h, l, s));
    }

    let (h, l, _s) = dir.open();
    assert_eq!(h.current(), hard(64));
    assert_eq!(l.compacted_through(), LogIndex(64));
    assert!(l.replay_entries().is_empty());
    assert_eq!(managed_generation_files(&dir), 3);
}

#[test]
fn every_checkpoint_publication_failure_reopens_to_committed_compaction() {
    for point in [
        DurabilityPoint::WalCheckpointAfterWrite,
        DurabilityPoint::WalCheckpointAfterSync,
        DurabilityPoint::WalCheckpointAfterDirectorySync,
        DurabilityPoint::WalManifestAfterRename,
        DurabilityPoint::WalManifestAfterDirectorySync,
        DurabilityPoint::WalCleanupAfterDelete,
    ] {
        let dir = Directory::new();
        let (h, mut l, mut s) = dir.open();
        publish(&h, &mut l, &[entry(1, b"one"), entry(2, b"two")], 2, None).unwrap();
        s.write_snapshot(snapshot(1)).unwrap();

        let guard = arm(point);
        let error = l
            .compact_prefix_through(LogIndex(1))
            .expect_err("injected checkpoint publication failure");
        guard.assert_triggered();
        assert!(matches!(
            error,
            crate::RaftLogSegmentCompactError::CompactedButReclamationFailed {
                compacted_through: LogIndex(1),
                ..
            }
        ));
        assert_eq!(l.compacted_through(), LogIndex(1));
        drop((h, l, s));

        let (h, l, _s) = dir.open();
        assert_eq!(h.current(), hard(2), "point {point:?}");
        assert_eq!(l.compacted_through(), LogIndex(1), "point {point:?}");
        assert_eq!(
            l.replay_entries(),
            vec![entry(2, b"two")],
            "point {point:?}"
        );
    }
}

#[test]
fn missing_covering_snapshot_reports_committed_compaction_and_requires_reopen() {
    let dir = Directory::new();
    let (h, mut l, s) = dir.open();
    publish(&h, &mut l, &[entry(1, b"one"), entry(2, b"two")], 2, None).unwrap();
    let error = l.compact_prefix_through(LogIndex(1)).unwrap_err();
    assert!(matches!(
        error,
        crate::RaftLogSegmentCompactError::CompactedButReclamationFailed {
            compacted_through: LogIndex(1),
            ..
        }
    ));
    assert_eq!(l.compacted_through(), LogIndex(1));
    assert!(matches!(
        l.append_entries(&[entry(3, b"blocked")]),
        Err(crate::RaftLogSegmentAppendError::StoreRequiresReopen)
    ));
    drop((h, l, s));

    let (h, l, _s) = dir.open();
    assert_eq!(h.current(), hard(2));
    assert_eq!(l.compacted_through(), LogIndex(1));
    assert_eq!(l.replay_entries(), vec![entry(2, b"two")]);
}

#[test]
fn old_manifest_outcome_after_ambiguous_replacement_still_recovers_compaction() {
    let dir = Directory::new();
    let (h, mut l, mut s) = dir.open();
    publish(
        &h,
        &mut l,
        &[entry(1, b"one"), entry(2, b"two"), entry(3, b"three")],
        3,
        None,
    )
    .unwrap();
    s.write_snapshot(snapshot(1)).unwrap();
    l.compact_prefix_through(LogIndex(1)).unwrap();
    let manifest_path = dir.0.join("raft-wal-current");
    let old_manifest = fs::read(&manifest_path).unwrap();

    s.write_snapshot(snapshot(2)).unwrap();
    let guard = arm(DurabilityPoint::WalManifestAfterRename);
    let error = l.compact_prefix_through(LogIndex(2)).unwrap_err();
    guard.assert_triggered();
    assert!(matches!(
        error,
        crate::RaftLogSegmentCompactError::CompactedButReclamationFailed {
            compacted_through: LogIndex(2),
            ..
        }
    ));

    // Model a crash where the replacement rename was visible to the process but
    // its missing directory fence allowed the previous manifest to survive.
    fs::write(&manifest_path, old_manifest).unwrap();
    drop((h, l, s));
    let (h, l, _s) = dir.open();
    assert_eq!(h.current(), hard(3));
    assert_eq!(l.compacted_through(), LogIndex(2));
    assert_eq!(l.replay_entries(), vec![entry(3, b"three")]);
    assert!(managed_names(&dir)
        .iter()
        .filter(|name| name.starts_with(CHECKPOINT_PREFIX) || name.starts_with(SEGMENT_PREFIX))
        .all(|name| name.contains("00000000000000000001")));
}

#[test]
fn selected_checkpoint_and_segment_corruption_fail_closed() {
    for kind in [CHECKPOINT_PREFIX, SEGMENT_PREFIX] {
        let dir = checkpointed_directory();
        let path = managed_path(&dir, kind);
        let mut bytes = fs::read(&path).unwrap();
        let offset = bytes.len() / 2;
        bytes[offset] ^= 1;
        fs::write(path, bytes).unwrap();
        let error = WalRaftNodeStores::open(&dir.0).unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData, "kind {kind}");
    }
}

#[test]
fn selected_generation_preserves_uncommitted_suffix_entries() {
    let dir = Directory::new();
    let (h, mut l, mut s) = dir.open();
    let entries: Vec<_> = (1..=4)
        .map(|index| entry(index, &[u8::try_from(index).unwrap()]))
        .collect();
    publish(&h, &mut l, &entries, 2, None).unwrap();
    s.write_snapshot(snapshot(2)).unwrap();
    l.compact_prefix_through(LogIndex(2)).unwrap();
    drop((h, l, s));

    let (h, l, _s) = dir.open();
    assert_eq!(h.current(), hard(2));
    assert_eq!(l.replay_entries(), entries[2..]);
    assert_eq!(l.next_index(), LogIndex(5));
}

#[test]
fn complete_corruption_in_selected_generation_record_is_rejected() {
    let dir = Directory::new();
    let (h, mut l, mut s) = dir.open();
    publish(&h, &mut l, &[entry(1, b"one"), entry(2, b"two")], 2, None).unwrap();
    s.write_snapshot(snapshot(1)).unwrap();
    l.compact_prefix_through(LogIndex(1)).unwrap();
    publish(&h, &mut l, &[entry(3, b"three")], 3, None).unwrap();
    drop((h, l, s));

    let segment = managed_path(&dir, SEGMENT_PREFIX);
    let mut bytes = fs::read(&segment).unwrap();
    *bytes.last_mut().unwrap() ^= 1;
    fs::write(segment, bytes).unwrap();
    let error = WalRaftNodeStores::open(&dir.0).unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
}

#[test]
fn corrupt_selected_manifest_never_falls_back_to_obsolete_state() {
    let dir = checkpointed_directory();
    fs::write(dir.0.join("hard-state"), codec::MAGIC).unwrap();
    let manifest = dir.0.join("raft-wal-current");
    let mut bytes = fs::read(&manifest).unwrap();
    bytes[8] ^= 1;
    fs::write(manifest, bytes).unwrap();
    let error = WalRaftNodeStores::open(&dir.0).unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
}

#[test]
fn cleanup_preserves_foreign_files_in_the_replica_directory() {
    let dir = Directory::new();
    let foreign = dir.0.join("raft-wal-operator-notes.txt");
    fs::write(&foreign, b"not owned by the WAL").unwrap();
    let (h, mut l, mut s) = dir.open();
    publish(&h, &mut l, &[entry(1, b"one")], 1, None).unwrap();
    s.write_snapshot(snapshot(1)).unwrap();
    l.compact_prefix_through(LogIndex(1)).unwrap();
    drop((h, l, s));
    let stores = dir.open();
    assert_eq!(fs::read(foreign).unwrap(), b"not owned by the WAL");
    drop(stores);
}

#[test]
fn partial_final_generation_record_is_discarded_as_a_whole() {
    let dir = Directory::new();
    let (h, mut l, mut s) = dir.open();
    publish(&h, &mut l, &[entry(1, b"one"), entry(2, b"two")], 2, None).unwrap();
    s.write_snapshot(snapshot(1)).unwrap();
    l.compact_prefix_through(LogIndex(1)).unwrap();
    publish(&h, &mut l, &[entry(3, b"three")], 3, None).unwrap();
    drop((h, l, s));

    let segment = managed_path(&dir, SEGMENT_PREFIX);
    let complete = fs::read(&segment).unwrap();
    fs::write(segment, &complete[..complete.len() - 1]).unwrap();
    let (h, l, _s) = dir.open();
    assert_eq!(h.current(), hard(2));
    assert_eq!(l.replay_entries(), vec![entry(2, b"two")]);
    assert_eq!(l.next_index(), LogIndex(3));
}

#[test]
fn checkpoint_snapshot_identity_rejects_a_conflicting_same_boundary_snapshot() {
    let dir = Directory::new();
    let (h, mut l, mut s) = dir.open();
    publish(&h, &mut l, &[entry(1, b"one"), entry(2, b"two")], 2, None).unwrap();
    s.write_snapshot(snapshot(1)).unwrap();
    l.compact_prefix_through(LogIndex(1)).unwrap();
    let mut conflict = snapshot(1);
    conflict.application_payload = b"conflicting-state".to_vec();
    s.write_snapshot(conflict).unwrap();
    drop((h, l, s));

    let error = WalRaftNodeStores::open(&dir.0).unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
}

fn checkpointed_directory() -> Directory {
    let dir = Directory::new();
    let (h, mut l, mut s) = dir.open();
    publish(&h, &mut l, &[entry(1, b"one"), entry(2, b"two")], 2, None).unwrap();
    s.write_snapshot(snapshot(1)).unwrap();
    l.compact_prefix_through(LogIndex(1)).unwrap();
    drop((h, l, s));
    dir
}

fn managed_names(dir: &Directory) -> Vec<String> {
    let mut names: Vec<_> = fs::read_dir(&dir.0)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|name| name.starts_with("raft-wal-"))
        .collect();
    names.sort();
    names
}

fn managed_generation_files(dir: &Directory) -> usize {
    managed_names(dir).len()
}

fn managed_generation_bytes(dir: &Directory) -> u64 {
    fs::read_dir(&dir.0)
        .unwrap()
        .map(|entry| entry.unwrap())
        .filter(|entry| entry.file_name().to_string_lossy().starts_with("raft-wal-"))
        .map(|entry| entry.metadata().unwrap().len())
        .sum()
}

fn managed_path(dir: &Directory, prefix: &str) -> PathBuf {
    fs::read_dir(&dir.0)
        .unwrap()
        .map(|entry| entry.unwrap())
        .find(|entry| entry.file_name().to_string_lossy().starts_with(prefix))
        .unwrap()
        .path()
}
