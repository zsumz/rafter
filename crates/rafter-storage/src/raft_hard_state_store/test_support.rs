//! Shared hard-state fixtures, unique paths, and filesystem cleanup.

use std::{
    fs,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use rafter::{LogIndex, NodeId, Term};

use crate::RaftHardState;

static TEST_FILE_ID: AtomicU64 = AtomicU64::new(0);

pub(super) fn hard_state(term: u64, voted_for: Option<u64>) -> RaftHardState {
    RaftHardState {
        current_term: Term(term),
        voted_for: voted_for.map(NodeId),
        commit_index: LogIndex::ZERO,
        committed_configuration: None,
    }
}

pub(super) fn test_store_path(name: &str) -> PathBuf {
    test_store_directory(name).with_extension("rafthard")
}

pub(super) fn test_store_directory(name: &str) -> PathBuf {
    let id = TEST_FILE_ID.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("rafter-storage-{name}-{}-{id}", std::process::id()))
}

pub(super) fn hard_state_temp_path(path: &Path) -> PathBuf {
    let mut temp = path.as_os_str().to_os_string();
    temp.push(".tmp");
    PathBuf::from(temp)
}

pub(super) fn remove_test_file(path: PathBuf) {
    let _ = fs::remove_file(hard_state_temp_path(&path));
    let _ = fs::remove_file(path);
}
