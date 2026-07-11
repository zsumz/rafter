use std::{
    error::Error,
    path::{Path, PathBuf},
};

use rafter::{
    LogIndex, NodeConfig, NodeId, RaftSnapshot, SnapshotChunkRequest, SnapshotChunkSource,
};
use rafter_runtime::DurableRaftNode;
use rafter_storage::{
    FileRaftHardStateStore, FileRaftLogSegment, FileRaftNodeStores, FileRaftSnapshotStore,
};

const ELECTION_TIMEOUT_TICKS: u64 = 5;
const HEARTBEAT_INTERVAL_TICKS: u64 = 2;

pub(crate) type FileNode =
    DurableRaftNode<FileRaftHardStateStore, FileRaftLogSegment, FileRaftSnapshotStore>;

pub(crate) fn open_node(
    root: &Path,
    node_id: NodeId,
    peers: Vec<NodeId>,
    applied_through: LogIndex,
) -> Result<FileNode, Box<dyn Error>> {
    let raft_dir = root.join("raft");
    std::fs::create_dir_all(&raft_dir)?;
    let (hard_state, log, snapshots) = FileRaftNodeStores::open(&raft_dir)?.into_parts();
    let config = NodeConfig::new(node_id, peers, ELECTION_TIMEOUT_TICKS)?
        .with_heartbeat_interval_ticks(HEARTBEAT_INTERVAL_TICKS);
    Ok(
        DurableRaftNode::with_storage_and_snapshot_store_applied_through(
            config,
            hard_state,
            log,
            snapshots,
            applied_through,
        )?,
    )
}

pub(crate) fn node_root(node_name: &str) -> PathBuf {
    std::env::var_os("RAFTER_MAELSTROM_ROOT").map_or_else(
        || {
            std::env::temp_dir()
                .join("rafter-maelstrom")
                .join(node_name)
        },
        |root| PathBuf::from(root).join(node_name),
    )
}

pub(crate) fn snapshot_every_from_env() -> Result<u64, Box<dyn Error>> {
    match std::env::var("RAFTER_MAELSTROM_SNAPSHOT_EVERY") {
        Ok(raw) => raw.parse::<u64>().map_err(|error| {
            format!("RAFTER_MAELSTROM_SNAPSHOT_EVERY must be u64: {error}").into()
        }),
        Err(std::env::VarError::NotPresent) => Ok(0),
        Err(error) => {
            Err(format!("RAFTER_MAELSTROM_SNAPSHOT_EVERY is not valid UTF-8: {error}").into())
        }
    }
}

pub(crate) fn read_snapshot_payload(
    node: &FileNode,
    snapshot: &RaftSnapshot,
) -> Result<Vec<u8>, String> {
    let mut payload = Vec::new();
    let mut offset = 0_u64;
    while offset < snapshot.application_payload_len {
        let len = u32::try_from((snapshot.application_payload_len - offset).min(64 * 1024))
            .expect("snapshot read chunk fits u32");
        let bytes = node
            .snapshot_store()
            .snapshot_chunk(SnapshotChunkRequest {
                transfer_id: snapshot.transfer_id(),
                metadata: &snapshot.metadata,
                total_payload_len: snapshot.application_payload_len,
                application_payload_crc32: snapshot.application_payload_crc32,
                offset,
                len,
            })
            .ok_or_else(|| format!("snapshot chunk unavailable at offset {offset}"))?;
        payload.extend_from_slice(&bytes);
        offset += u64::from(len);
    }
    Ok(payload)
}
