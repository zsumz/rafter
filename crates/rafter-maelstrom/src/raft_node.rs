use std::{
    error::Error,
    path::{Path, PathBuf},
};

use rafter::{
    LogIndex, NodeConfig, NodeId, Output, RaftSnapshot, SnapshotChunkRequest, SnapshotChunkSource,
};
use rafter_runtime::DurableRaftNode;
use rafter_storage::{
    FileRaftHardStateStore, FileRaftLogSegment, FileRaftNodeStores, FileRaftSnapshotStore,
};

const ELECTION_TIMEOUT_TICKS: u64 = 5;
const HEARTBEAT_INTERVAL_TICKS: u64 = 2;

pub(crate) type FileNode =
    DurableRaftNode<FileRaftHardStateStore, FileRaftLogSegment, FileRaftSnapshotStore>;

pub(crate) struct OpenedFileNode {
    pub(crate) node: FileNode,
    pub(crate) recovery_outputs: Vec<Output>,
}

pub(crate) fn open_node(
    root: &Path,
    node_id: NodeId,
    peers: Vec<NodeId>,
    applied_through: LogIndex,
) -> Result<OpenedFileNode, Box<dyn Error>> {
    let raft_dir = root.join("raft");
    std::fs::create_dir_all(&raft_dir)?;
    let (hard_state, log, snapshots) = FileRaftNodeStores::open(&raft_dir)?.into_parts();
    let election_timeout_ticks = timing_from_env(
        "RAFTER_MAELSTROM_ELECTION_TIMEOUT_TICKS",
        ELECTION_TIMEOUT_TICKS,
    )?;
    let heartbeat_interval_ticks = timing_from_env(
        "RAFTER_MAELSTROM_HEARTBEAT_INTERVAL_TICKS",
        HEARTBEAT_INTERVAL_TICKS,
    )?;
    let config = NodeConfig::new(node_id, peers, election_timeout_ticks)?
        .with_heartbeat_interval_ticks(heartbeat_interval_ticks)
        .with_lease_reads(lease_reads_enabled_from_env());
    let recovered = DurableRaftNode::recover_with_storage_and_snapshot_store_applied_through(
        config,
        hard_state,
        log,
        snapshots,
        applied_through,
    )?;
    let (node, recovery_outputs) = recovered.into_parts();
    Ok(OpenedFileNode {
        node,
        recovery_outputs,
    })
}

fn lease_reads_enabled_from_env() -> bool {
    lease_reads_enabled(
        std::env::var("RAFTER_MAELSTROM_LEASE_EVIDENCE")
            .ok()
            .as_deref(),
    )
}

fn lease_reads_enabled(value: Option<&str>) -> bool {
    value.is_some_and(|value| matches!(value, "1" | "true" | "yes"))
}

fn timing_from_env(name: &str, default: u64) -> Result<u64, Box<dyn Error>> {
    timing_value(name, std::env::var(name).ok().as_deref(), default)
        .map_err(std::convert::Into::into)
}

fn timing_value(name: &str, value: Option<&str>, default: u64) -> Result<u64, String> {
    let Some(value) = value else {
        return Ok(default);
    };
    value
        .parse::<u64>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| format!("{name} must be a positive u64"))
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

#[cfg(test)]
mod tests {
    use super::{lease_reads_enabled, timing_value};

    #[test]
    fn lease_reads_are_enabled_only_by_an_explicit_evidence_flag() {
        assert!(lease_reads_enabled(Some("1")));
        assert!(lease_reads_enabled(Some("true")));
        assert!(!lease_reads_enabled(None));
        assert!(!lease_reads_enabled(Some("0")));
        assert!(!lease_reads_enabled(Some("TRUE")));
    }

    #[test]
    fn evidence_timing_values_are_explicit_positive_ticks() {
        assert_eq!(timing_value("ticks", None, 5), Ok(5));
        assert_eq!(timing_value("ticks", Some("20"), 5), Ok(20));
        assert!(timing_value("ticks", Some("0"), 5).is_err());
        assert!(timing_value("ticks", Some("nope"), 5).is_err());
    }
}
