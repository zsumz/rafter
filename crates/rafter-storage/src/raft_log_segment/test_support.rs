use std::{
    fs,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use rafter::{LogIndex, Term};

use crate::PersistedRaftLogEntry;

static TEST_FILE_ID: AtomicU64 = AtomicU64::new(0);

pub(super) fn entry(index: u64, value: &[u8]) -> PersistedRaftLogEntry {
    PersistedRaftLogEntry::application(LogIndex(index), Term(7), value.to_vec())
}

pub(super) fn test_segment_path(name: &str) -> PathBuf {
    let id = TEST_FILE_ID.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "rafter-storage-{name}-{}-{id}.raftlog",
        std::process::id()
    ))
}

pub(super) fn remove_test_file(path: PathBuf) {
    let _ = fs::remove_file(compact_marker_path_for_test(&path));
    let mut compact_temp = compact_marker_path_for_test(&path).into_os_string();
    compact_temp.push(format!(".{}.tmp", std::process::id()));
    let _ = fs::remove_file(PathBuf::from(compact_temp));
    let _ = fs::remove_file(path.with_extension(format!("rewrite-{}.tmp", std::process::id())));
    let _ = fs::remove_file(path);
}

pub(super) fn compact_marker_path_for_test(path: &Path) -> PathBuf {
    super::compaction_marker_path(path)
}
