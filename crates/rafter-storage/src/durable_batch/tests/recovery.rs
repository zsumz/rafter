//! Every truncation point retains whole records; complete corruption fails closed.
use super::*;

#[test]
fn every_torn_suffix_recovers_the_whole_previous_or_whole_next_batch() {
    let dir = Directory::new();
    let (h, mut l, s) = dir.open();
    publish(&h, &mut l, &[entry(1, b"one"), entry(2, b"old")], 1, None).unwrap();
    let boundary = usize::try_from(fs::metadata(dir.0.join("hard-state")).unwrap().len()).unwrap();
    publish(
        &h,
        &mut l,
        &[entry(2, b"replacement"), entry(3, b"three")],
        3,
        Some(LogIndex(2)),
    )
    .unwrap();
    drop((h, l, s));
    let complete = fs::read(dir.0.join("hard-state")).unwrap();
    for end in boundary..=complete.len() {
        fs::write(dir.0.join("hard-state"), &complete[..end]).unwrap();
        let (h, l, s) = dir.open();
        let expected = if end == complete.len() {
            (
                hard(3),
                vec![
                    entry(1, b"one"),
                    entry(2, b"replacement"),
                    entry(3, b"three"),
                ],
            )
        } else {
            (hard(1), vec![entry(1, b"one"), entry(2, b"old")])
        };
        assert_eq!((h.current(), l.replay_entries()), expected, "cut {end}");
        assert_eq!(
            usize::try_from(fs::metadata(dir.0.join("hard-state")).unwrap().len()).unwrap(),
            if end == complete.len() { end } else { boundary }
        );
        drop((h, l, s));
    }
}
#[test]
fn every_corrupted_byte_of_a_complete_record_is_rejected() {
    let dir = Directory::new();
    let (h, mut l, s) = dir.open();
    publish(&h, &mut l, &[entry(1, b"one")], 1, None).unwrap();
    drop((h, l, s));
    let complete = fs::read(dir.0.join("hard-state")).unwrap();
    for offset in 0..complete.len() {
        let mut bytes = complete.clone();
        bytes[offset] ^= 1;
        fs::write(dir.0.join("hard-state"), bytes).unwrap();
        assert!(
            WalRaftNodeStores::open(&dir.0).is_err(),
            "corruption at {offset}"
        );
    }
}
#[test]
fn legacy_files_and_duplicate_ownership_are_rejected() {
    let dir = Directory::new();
    let stores = dir.open();
    assert!(WalRaftNodeStores::open(&dir.0).is_err());
    drop(stores);
    fs::write(dir.0.join("log"), b"legacy").unwrap();
    assert!(WalRaftNodeStores::open(&dir.0).is_err());
}
#[test]
fn compaction_replays_before_the_next_contiguous_append() {
    let dir = Directory::new();
    let (h, mut l, mut s) = dir.open();
    publish(&h, &mut l, &[entry(1, b"one"), entry(2, b"two")], 2, None).unwrap();
    s.write_snapshot(snapshot(1)).unwrap();
    l.compact_prefix_through(LogIndex(1)).unwrap();
    assert_eq!(l.compacted_through(), LogIndex(1));
    drop((h, l, s));
    let (h, mut l, _s) = dir.open();
    assert_eq!(l.replay_entries(), vec![entry(2, b"two")]);
    publish(&h, &mut l, &[entry(3, b"three")], 3, None).unwrap();
    assert_eq!(l.next_index(), LogIndex(4));
}

#[test]
fn snapshot_data_without_wal_is_not_silently_reinitialized() {
    let dir = Directory::new();
    fs::create_dir(dir.0.join("snapshots")).unwrap();
    fs::write(dir.0.join("snapshots/current"), b"existing snapshot").unwrap();
    assert!(WalRaftNodeStores::open(&dir.0).is_err());
    assert!(!dir.0.join("hard-state").exists());
}
