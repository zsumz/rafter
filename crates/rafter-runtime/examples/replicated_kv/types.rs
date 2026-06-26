use std::collections::BTreeMap;
use std::time::Duration;

use rafter::{LogIndex, NodeId};
use rafter_runtime::DurableRaftNode;
use rafter_storage::{FileRaftHardStateStore, FileRaftLogSegment, FileRaftSnapshotStore};

pub(crate) const NODE_IDS: [NodeId; 3] = [NodeId(1), NodeId(2), NodeId(3)];
pub(crate) const ELECTION_TIMEOUT_TICKS: u64 = 3;
pub(crate) const HEARTBEAT_INTERVAL_TICKS: u64 = 2;
pub(crate) const PROCESS_STEP_TIMEOUT: Duration = Duration::from_secs(20);
pub(crate) const PROCESS_DRIVER_INTERVAL: Duration = Duration::from_millis(50);
pub(crate) const PROCESS_PENDING_LIMIT: usize = 1024;

pub(crate) type FileNode =
    DurableRaftNode<FileRaftHardStateStore, FileRaftLogSegment, FileRaftSnapshotStore>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScenarioReport {
    pub initial_leader: NodeId,
    pub transferred_leader: NodeId,
    pub alpha_read: Option<String>,
    pub final_values: BTreeMap<String, String>,
    pub snapshot_index: LogIndex,
    pub restarted_applied_floor: LogIndex,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScenarioOptions {
    pub keep_dir: bool,
    pub verbose: bool,
}

impl Default for ScenarioOptions {
    fn default() -> Self {
        Self {
            keep_dir: false,
            verbose: true,
        }
    }
}
