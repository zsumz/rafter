use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use rafter::{
    ApplicationSnapshotKind, ApplicationSnapshotMetadata, ApplicationSnapshotVersion, LogIndex,
    NodeConfig, NodeId, Output, RaftSnapshot, RaftSnapshotMetadata, SnapshotChunkRequest,
    SnapshotChunkSource, SnapshotGroupId,
};
use rafter_storage::{FileRaftNodeStores, PersistedRaftSnapshot};

use super::{
    codec::encode_snapshot,
    types::{FileNode, ELECTION_TIMEOUT_TICKS, HEARTBEAT_INTERVAL_TICKS, NODE_IDS},
};

pub(crate) fn open_node(
    root: &Path,
    node_id: NodeId,
    applied_through: LogIndex,
) -> (FileNode, Vec<Output>) {
    let dir = root.join(format!("node-{}", node_id.0));
    std::fs::create_dir_all(&dir).expect("create node directory");
    let (hard_state, log, snapshots) = FileRaftNodeStores::open(&dir)
        .expect("open file-backed node stores")
        .into_parts();
    let peers = NODE_IDS
        .into_iter()
        .filter(|peer| *peer != node_id)
        .collect();
    let config = NodeConfig::new(node_id, peers, ELECTION_TIMEOUT_TICKS)
        .expect("valid static membership")
        .with_heartbeat_interval_ticks(HEARTBEAT_INTERVAL_TICKS);
    DurableRecover::recover(config, hard_state, log, snapshots, applied_through)
}

struct DurableRecover;

impl DurableRecover {
    fn recover(
        config: NodeConfig,
        hard_state: rafter_storage::FileRaftHardStateStore,
        log: rafter_storage::FileRaftLogSegment,
        snapshots: rafter_storage::FileRaftSnapshotStore,
        applied_through: LogIndex,
    ) -> (FileNode, Vec<Output>) {
        rafter_runtime::DurableRaftNode::recover_with_storage_and_snapshot_store_applied_through(
            config,
            hard_state,
            log,
            snapshots,
            applied_through,
        )
        .expect("hydrate durable node")
        .into_parts()
    }
}

pub(crate) fn compact_kv_snapshot(
    node_id: NodeId,
    node: &mut FileNode,
    kv: &BTreeMap<String, String>,
    applied: LogIndex,
) -> LogIndex {
    // The runtime fills Raft-owned membership metadata before writing this
    // snapshot. Dynamic-membership embeddings must serve snapshots only from
    // leaders authorized by that boundary membership.
    let term = node
        .term_at_index(applied)
        .expect("applied boundary term is retained");
    let metadata = RaftSnapshotMetadata::new(
        SnapshotGroupId::new("replicated-kv").expect("valid group id"),
        node_id,
        applied,
        term,
        node.current_term(),
        ApplicationSnapshotMetadata::new(
            ApplicationSnapshotKind::new("kv-v1").expect("valid kind"),
            ApplicationSnapshotVersion::new(1).expect("valid version"),
        ),
    )
    .expect("snapshot metadata is valid");
    node.compact_log_with_snapshot(PersistedRaftSnapshot {
        metadata,
        application_payload: encode_snapshot(kv),
    })
    .expect("leader compacts through local snapshot");
    applied
}

pub(crate) fn node_dir(root: &Path, node_id: NodeId) -> PathBuf {
    root.join(format!("node-{}", node_id.0))
}

pub(crate) fn read_snapshot_payload(node: &FileNode, snapshot: &RaftSnapshot) -> Vec<u8> {
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
            .expect("snapshot store serves applied snapshot");
        payload.extend_from_slice(&bytes);
        offset += u64::from(len);
    }
    payload
}
