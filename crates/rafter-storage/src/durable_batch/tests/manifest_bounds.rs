//! A malformed authoritative manifest must not select an older WAL or repair files.
use super::*;
use std::io::{Read, Seek, SeekFrom};

#[test]
fn oversized_manifest_preserves_selected_wal_files_and_never_falls_back() {
    let dir = checkpointed_directory();
    let names = managed_names(&dir);
    let before: Vec<_> = names
        .iter()
        .map(|name| (name.clone(), fs::read(dir.0.join(name)).unwrap()))
        .collect();
    let manifest_path = dir.0.join("raft-wal-current");
    let canonical = fs::read(&manifest_path).unwrap();
    // A perfectly valid old-format empty WAL must not hide selected authority
    // corruption. Its bytes must not be replayed, repaired, or cleaned up.
    fs::write(dir.0.join("hard-state"), codec::MAGIC).unwrap();
    let mut manifest = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&manifest_path)
        .unwrap();
    manifest.set_len(8 * 1024 * 1024).unwrap();
    for _ in 0..2 {
        let error = WalRaftNodeStores::open(&dir.0).unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        assert_eq!(managed_names(&dir), names);
        assert_eq!(fs::read(dir.0.join("hard-state")).unwrap(), codec::MAGIC);
        assert_eq!(manifest.metadata().unwrap().len(), 8 * 1024 * 1024);
        manifest.seek(SeekFrom::Start(0)).unwrap();
        let mut prefix = vec![0; canonical.len() + 1];
        manifest.read_exact(&mut prefix).unwrap();
        assert_eq!(&prefix[..canonical.len()], &canonical);
        assert_eq!(prefix[canonical.len()], 0);
        for (name, bytes) in &before {
            if name != "raft-wal-current" {
                assert_eq!(fs::read(dir.0.join(name)).unwrap(), *bytes);
            }
        }
    }
    // Explicitly restore the original authority and prove its retained state
    // still opens; the refused opens did not modify the selected generation.
    manifest.set_len(canonical.len() as u64).unwrap();
    drop(manifest);
    let (h, l, _s) = dir.open();
    assert_eq!(h.current(), hard(2));
    assert_eq!(l.compacted_through(), LogIndex(1));
    assert_eq!(l.replay_entries(), vec![entry(2, b"two")]);
}
