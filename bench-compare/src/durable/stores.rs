//! Selectable hard-state backend; log and snapshot behavior stays identical.

use super::config::HardStateBackend;
use rafter_storage::{
    FileRaftHardStateStore, FileRaftLogSegment, FileRaftNodeStores, FileRaftSnapshotStore,
    JournalRaftHardStateStore, JournalRaftNodeStores, RaftHardState, RaftHardStateStore,
    RaftHardStateStoreWriteError,
};
use std::path::Path;

#[derive(Debug)]
pub(super) enum HardState {
    Replace(FileRaftHardStateStore),
    Journal(JournalRaftHardStateStore),
}

pub(super) fn open(
    directory: &Path,
    backend: HardStateBackend,
) -> (HardState, FileRaftLogSegment, FileRaftSnapshotStore) {
    match backend {
        HardStateBackend::Replace => {
            let (hard, log, snapshots) = FileRaftNodeStores::open(directory)
                .expect("replace stores open")
                .into_parts();
            (HardState::Replace(hard), log, snapshots)
        }
        HardStateBackend::Journal => {
            let (hard, log, snapshots) = JournalRaftNodeStores::open(directory)
                .expect("journal stores open")
                .into_parts();
            (HardState::Journal(hard), log, snapshots)
        }
    }
}

impl RaftHardStateStore for HardState {
    fn write_hard_state(
        &mut self,
        state: RaftHardState,
    ) -> Result<(), RaftHardStateStoreWriteError> {
        match self {
            Self::Replace(store) => store.write_hard_state(state),
            Self::Journal(store) => store.write_hard_state(state),
        }
    }
    fn current(&self) -> RaftHardState {
        match self {
            Self::Replace(store) => store.current(),
            Self::Journal(store) => store.current(),
        }
    }
}
