//! Repeated reclamation with large live suffixes and phase telemetry.

use super::*;
use std::collections::BTreeMap;

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
