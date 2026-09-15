//! Raft output routing into bounded peer and durable application work.

use rafter::{LogIndex, NodeId, Output};
use rafter_storage::SnapshotRetention;

use super::super::{
    application::AppliedCommand, codec::decode_snapshot, storage::read_snapshot_payload,
};
use super::{Cluster, Envelope, PEER_QUEUE_CAPACITY};

impl Cluster {
    pub(super) fn handle_outputs(&mut self, node_id: NodeId, outputs: Vec<Output>) {
        for output in outputs {
            match output {
                Output::Send { to, message } => self.enqueue_peer(node_id, to, message),
                Output::Apply {
                    index,
                    payload,
                    local_proposal_id,
                    ..
                } => {
                    let replica = self.replicas.get_mut(&node_id).expect("replica exists");
                    let next = replica
                        .dispatched
                        .0
                        .checked_add(1)
                        .expect("reference log index remains advanceable");
                    assert!(next <= index.0, "application outputs remain ordered");
                    let mut entries: Vec<_> = (next..index.0)
                        .map(|marker| AppliedCommand {
                            index: LogIndex(marker),
                            payload: None,
                            proposal_id: None,
                        })
                        .collect();
                    entries.push(AppliedCommand {
                        index,
                        payload: Some(payload.as_slice().to_vec()),
                        proposal_id: local_proposal_id,
                    });
                    replica
                        .application
                        .as_ref()
                        .expect("application worker is running")
                        .try_submit(entries)
                        .expect("reference application admission remains within bounds");
                    replica.dispatched = index;
                }
                Output::ApplySnapshot { snapshot } => {
                    self.drain_application(node_id);
                    let payload = read_snapshot_payload(
                        &self.replicas.get(&node_id).expect("replica exists").node,
                        &snapshot,
                    );
                    self.install_application_snapshot(
                        node_id,
                        decode_snapshot(&payload),
                        snapshot.metadata.last_included_index,
                    );
                    self.replicas
                        .get_mut(&node_id)
                        .expect("replica exists")
                        .node
                        .ready_mut()
                        .prune_snapshot_files(SnapshotRetention::CurrentOnly)
                        .expect("prune superseded inbound snapshot envelopes");
                }
                Output::RejectProposal { reason, .. } => panic!("proposal rejected: {reason}"),
                Output::LocalProposalDropped { reason, .. } => {
                    panic!("local proposal outcome became unknown: {reason:?}")
                }
                Output::ReadIndexRejected { reason, .. } => {
                    panic!("unexpected read rejection: {reason}")
                }
                Output::ReadIndexCanceled { reason, .. } => {
                    panic!("unexpected read cancellation: {reason:?}")
                }
                Output::LeadershipTransferRejected { target, reason } => {
                    panic!("leadership transfer to {target} rejected: {reason}")
                }
                Output::ConfigurationCommitted { .. }
                | Output::LocalProposalAppended { .. }
                | Output::ReadIndexGranted { .. }
                | Output::StageSnapshotChunk { .. } => {}
                Output::SendSnapshotChunk { .. } => {
                    panic!("runtime must resolve snapshot chunk sends")
                }
            }
        }
    }

    fn enqueue_peer(&mut self, from: NodeId, to: NodeId, message: rafter::Message) {
        if self.paused.contains(&to) {
            return;
        }
        assert!(
            self.peer_queue.len() < PEER_QUEUE_CAPACITY,
            "bounded peer queue refused a message"
        );
        self.peer_queue.push_back(Envelope { from, to, message });
        self.max_peer_queue_depth = self.max_peer_queue_depth.max(self.peer_queue.len());
    }
}
