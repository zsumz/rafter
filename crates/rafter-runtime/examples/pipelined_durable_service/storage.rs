//! WAL recovery and application snapshot helpers for the reference service.

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use rafter::{
    ApplicationSnapshotKind, ApplicationSnapshotMetadata, ApplicationSnapshotVersion, LogIndex,
    NodeConfig, NodeId, Output, RaftSnapshot, RaftSnapshotMetadata, SnapshotChunkRequest,
    SnapshotChunkSource, SnapshotGroupId,
};
use rafter_storage::durable_batch::WalRaftNodeStores;
use rafter_storage::{PersistedRaftSnapshot, SnapshotRetention};

use super::{
    codec::encode_snapshot,
    driver::{DurableNode, PipelinedNode},
};

const NODE_IDS: [NodeId; 3] = [NodeId(1), NodeId(2), NodeId(3)];
const ELECTION_TIMEOUT_TICKS: u64 = 3;
const HEARTBEAT_INTERVAL_TICKS: u64 = 2;

pub(crate) fn open_node(
    root: &Path,
    node_id: NodeId,
    applied_through: LogIndex,
) -> (PipelinedNode, Vec<Output>) {
    let dir = node_dir(root, node_id);
    std::fs::create_dir_all(&dir).expect("create reference replica directory");
    let (hard_state, log, snapshots) = WalRaftNodeStores::open(&dir)
        .expect("open shared WAL stores")
        .into_parts();
    let peers = NODE_IDS
        .into_iter()
        .filter(|peer| *peer != node_id)
        .collect();
    let config = NodeConfig::new(node_id, peers, ELECTION_TIMEOUT_TICKS)
        .expect("valid fixed membership")
        .with_heartbeat_interval_ticks(HEARTBEAT_INTERVAL_TICKS);
    let recovered = DurableNode::recover_with_storage_and_snapshot_store_applied_through(
        config,
        hard_state,
        log,
        snapshots,
        applied_through,
    )
    .expect("recover durable node");
    let (node, outputs) = recovered.into_parts();
    (PipelinedNode::start(node), outputs)
}

pub(crate) fn compact_snapshot(
    node_id: NodeId,
    node: &mut PipelinedNode,
    kv: &BTreeMap<String, String>,
    applied: LogIndex,
) -> LogIndex {
    let node = node.ready_mut();
    let term = node
        .term_at_index(applied)
        .expect("applied boundary term is retained");
    let metadata = RaftSnapshotMetadata::new(
        SnapshotGroupId::new("pipelined-durable-service").expect("valid group id"),
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
    .expect("compact WAL through durable application state");
    node.prune_snapshot_files(SnapshotRetention::CurrentOnly)
        .expect("prune superseded snapshot envelopes");
    applied
}

pub(crate) fn read_snapshot_payload(node: &PipelinedNode, snapshot: &RaftSnapshot) -> Vec<u8> {
    let mut payload = Vec::new();
    let mut offset = 0_u64;
    while offset < snapshot.application_payload_len {
        let len = u32::try_from((snapshot.application_payload_len - offset).min(64 * 1024))
            .expect("snapshot chunk fits u32");
        let bytes = node
            .ready()
            .snapshot_store()
            .snapshot_chunk(SnapshotChunkRequest {
                transfer_id: snapshot.transfer_id(),
                metadata: &snapshot.metadata,
                total_payload_len: snapshot.application_payload_len,
                application_payload_crc32: snapshot.application_payload_crc32,
                offset,
                len,
            })
            .expect("snapshot store serves the promoted snapshot");
        payload.extend_from_slice(&bytes);
        offset += u64::from(len);
    }
    payload
}

pub(crate) fn node_dir(root: &Path, node_id: NodeId) -> PathBuf {
    root.join(format!("node-{}", node_id.0))
}

pub(crate) const fn node_ids() -> [NodeId; 3] {
    NODE_IDS
}

pub(crate) const fn election_timeout_ticks() -> u64 {
    ELECTION_TIMEOUT_TICKS
}
