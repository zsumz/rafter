//! Recording of kernel outputs into simulator history.
//!
//! Every output a node emits passes through here exactly once: sends are
//! queued, applies and snapshot installs extend the append-only recorder
//! histories, and explicit rejections are preserved as verifier evidence.

use rafter::{LogIndex, NodeId, Output, RaftSnapshot};

use crate::records::{ProposalRejected, RecordedOutputs, TransferRejected};
use crate::{Applied, Cluster, Envelope, ReadGranted, ReadTerminalOutput, SnapshotInstalled};

use super::proposal::local_proposal_event;

impl Cluster {
    pub(crate) fn record_outputs(&mut self, from: NodeId, outputs: Vec<Output>) -> Vec<Envelope> {
        self.record_outputs_observed(from, outputs).emitted
    }

    pub(crate) fn record_outputs_observed(
        &mut self,
        from: NodeId,
        outputs: Vec<Output>,
    ) -> RecordedOutputs {
        let mut emitted = Vec::new();
        let mut local_proposals = Vec::new();
        for output in outputs {
            if let Some(event) = local_proposal_event(from, &output) {
                local_proposals.push(event);
            }
            self.record_output(from, output, &mut emitted);
        }
        self.record_execution_history(from);
        RecordedOutputs {
            emitted,
            local_proposals,
        }
    }

    fn record_output(&mut self, from: NodeId, output: Output, emitted: &mut Vec<Envelope>) {
        match output {
            Output::Apply { index, payload, .. } => self.record_apply(from, index, payload),
            Output::ApplySnapshot { snapshot } => self.record_snapshot_apply(from, &snapshot),
            Output::SendSnapshotChunk { to, chunk } => {
                self.record_snapshot_send(from, to, &chunk, emitted);
            }
            Output::StageSnapshotChunk { chunk } => self.stage_snapshot_chunk(from, chunk),
            Output::ReadIndexGranted {
                read_id,
                read_index,
            } => {
                let operation_id = self.pending_read_operation_id(from, read_id.0);
                self.record_read_output_correlation(from, read_id.0, operation_id, "grant");
                let application_epoch = self.application_epoch(from);
                self.read_grants.push(ReadGranted {
                    node_id: from,
                    operation_id,
                    application_epoch,
                    request_id: read_id.0,
                    read_index,
                    local_applied_index: self.local_applied_index(from),
                });
            }
            Output::ReadIndexRejected { read_id, reason } => {
                let operation_id = self.pending_read_operation_id(from, read_id.0);
                self.record_read_output_correlation(from, read_id.0, operation_id, "rejection");
                self.read_terminal_outputs
                    .push(ReadTerminalOutput::Rejected {
                        node_id: from,
                        operation_id,
                        request_id: read_id.0,
                        reason,
                    });
            }
            Output::ReadIndexCanceled { read_id, reason } => {
                let operation_id = self.pending_read_operation_id(from, read_id.0);
                self.record_read_output_correlation(from, read_id.0, operation_id, "cancellation");
                self.read_terminal_outputs
                    .push(ReadTerminalOutput::Canceled {
                        node_id: from,
                        operation_id,
                        request_id: read_id.0,
                        reason,
                    });
            }
            Output::RejectProposal {
                proposal_id,
                reason: _,
            } => self.proposal_rejections.push(ProposalRejected {
                node_id: from,
                proposal_id,
            }),
            Output::LeadershipTransferRejected { target, reason: _ } => {
                self.transfer_rejections.push(TransferRejected {
                    node_id: from,
                    target,
                });
            }
            // The reference application already crosses configuration entries:
            // `record_execution_history` walks the log itself between the old and
            // new applied indexes and folds every entry kind into
            // `ReferenceState`, so a committed configuration reaches the AP-02
            // oracle through the entry rather than through this announcement.
            // Recording it here as well would witness the same transition twice
            // and make equal executions compare unequal.
            Output::ConfigurationCommitted { .. }
            | Output::LocalProposalAppended { .. }
            | Output::LocalProposalDropped { .. } => {}
            Output::Send { to, message } => {
                let envelope = Envelope { from, to, message };
                emitted.push(envelope.clone());
                self.enqueue(envelope);
            }
        }
    }

    fn record_apply(&mut self, from: NodeId, index: LogIndex, payload: rafter::SharedPayload) {
        let commit_index_at_emit = self.commit_index(from);
        self.record_durable_applied(from, index);
        let application_epoch = self.application_epoch(from);
        self.applied.push(Applied {
            node_id: from,
            application_epoch,
            commit_index_at_emit,
            index,
            payload,
        });
    }

    fn record_snapshot_apply(&mut self, from: NodeId, snapshot: &RaftSnapshot) {
        let commit_index_at_emit = self.commit_index(from);
        self.record_durable_applied(from, snapshot.metadata.last_included_index);
        let payload = self.take_installed_snapshot_payload(from, snapshot);
        self.reset_execution_cursor_to_snapshot(from, snapshot, payload.clone());
        let application_epoch = self.application_epoch(from);
        self.snapshot_installs.push(SnapshotInstalled {
            node_id: from,
            application_epoch,
            commit_index_at_emit,
            last_included_index: snapshot.metadata.last_included_index,
            last_included_term: snapshot.metadata.last_included_term,
            committed_membership: snapshot.metadata.committed_membership().cloned(),
            payload,
            applied_records_before_install: self.applied.len(),
        });
    }

    fn record_snapshot_send(
        &mut self,
        from: NodeId,
        to: NodeId,
        chunk: &rafter::SnapshotChunkSend,
        emitted: &mut Vec<Envelope>,
    ) {
        let resolved = self
            .snapshot_sources
            .get(&from)
            .and_then(|source| chunk.resolve(source));
        if let Some(message) = resolved {
            let envelope = Envelope {
                from,
                to,
                message: rafter::Message::InstallSnapshotChunk(message),
            };
            emitted.push(envelope.clone());
            self.enqueue(envelope);
        }
    }
}
