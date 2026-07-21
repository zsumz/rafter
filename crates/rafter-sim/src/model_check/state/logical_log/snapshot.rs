//! Snapshot logical-prefix provenance and transfer ownership.

use rafter::{LogIndex, Message, NodeId, SnapshotTransferId, Term};

use crate::{Cluster, Envelope};

use super::super::super::catalog;
use super::{LogPrefixWitness, LogicalLogHistory, LogicalLogView, LogicalLogViolation};

impl LogicalLogHistory {
    pub(super) fn attach_snapshot_prefix(
        &mut self,
        node_id: NodeId,
        mut view: LogicalLogView,
    ) -> LogicalLogView {
        let Some(snapshot) = view.snapshot.as_ref() else {
            return view;
        };
        let transfer_id = snapshot.transfer_id;
        let index = snapshot.index;
        let term = snapshot.term;
        let local_prefix = self
            .last_views_by_node
            .get(&node_id)
            .and_then(|previous| self.prefix_from_view(previous, index));
        let recorded_prefix = self
            .snapshot_prefixes_by_owner_transfer
            .get(&(node_id, transfer_id))
            .cloned();
        if let (Some(local), Some(recorded)) = (&local_prefix, &recorded_prefix) {
            if local != recorded {
                self.violations.insert(LogicalLogViolation {
                    invariant: catalog::LG_03_LOG_MATCHING,
                    message: format!(
                        "{node_id} snapshot transfer {transfer_id} conflicts with its local logical prefix"
                    ),
                });
            }
        }
        let prefix = local_prefix.or(recorded_prefix);

        let Some(prefix) = prefix else {
            self.unwitnessed_snapshots
                .insert((node_id, transfer_id, index, term));
            return view;
        };
        self.unwitnessed_snapshots
            .remove(&(node_id, transfer_id, index, term));
        if !self.insert_snapshot_prefix(node_id, transfer_id, index, term, prefix.clone()) {
            self.unwitnessed_snapshots
                .insert((node_id, transfer_id, index, term));
            return view;
        }
        if let Some(snapshot) = view.snapshot.as_mut() {
            snapshot.prefix = Some(Box::new(prefix));
        }
        view
    }

    fn insert_snapshot_prefix(
        &mut self,
        node_id: NodeId,
        transfer_id: SnapshotTransferId,
        index: LogIndex,
        term: Term,
        prefix: LogPrefixWitness,
    ) -> bool {
        let boundary_matches = prefix.through() == index
            && prefix
                .last()
                .map_or(index == LogIndex::ZERO, |entry| entry.term == term);
        if !boundary_matches {
            self.violations.insert(LogicalLogViolation {
                invariant: catalog::LG_03_LOG_MATCHING,
                message: format!(
                    "{node_id} snapshot transfer {transfer_id} has a logical-prefix witness that does not match boundary ({index}, term {term})"
                ),
            });
            return false;
        }
        self.insert_prefix(node_id, index, term, prefix.clone());
        let key = (node_id, transfer_id);
        if let Some(previous) = self.snapshot_prefixes_by_owner_transfer.get(&key) {
            if previous != &prefix {
                self.violations.insert(LogicalLogViolation {
                    invariant: catalog::LG_03_LOG_MATCHING,
                    message: format!(
                        "{node_id} observed snapshot transfer {transfer_id} with a different logical prefix"
                    ),
                });
                return false;
            }
            return true;
        }
        self.snapshot_prefixes_by_owner_transfer.insert(key, prefix);
        true
    }

    pub(in crate::model_check::state) fn record_snapshot_installation(
        &mut self,
        before: &Cluster,
        after: &Cluster,
        delivered: Option<&Envelope>,
    ) {
        let Some(envelope) = delivered else {
            return;
        };
        if !matches!(
            envelope.message,
            Message::InstallSnapshot(_) | Message::InstallSnapshotChunk(_)
        ) {
            return;
        }
        let before_snapshot = before.bootstrap_state(envelope.to).snapshot;
        let Some(after_snapshot) = after.bootstrap_state(envelope.to).snapshot else {
            return;
        };
        if before_snapshot.as_ref() == Some(&after_snapshot) {
            return;
        }
        let transfer_id = after_snapshot.transfer_id();
        let index = after_snapshot.metadata.last_included_index;
        let term = after_snapshot.metadata.last_included_term;
        let Some(transferred_prefix) = self
            .snapshot_prefixes_by_owner_transfer
            .get(&(envelope.from, transfer_id))
            .cloned()
        else {
            self.unwitnessed_snapshots
                .insert((envelope.to, transfer_id, index, term));
            self.last_views_by_node.remove(&envelope.to);
            return;
        };
        let source_view = self
            .last_views_by_node
            .get(&envelope.from)
            .cloned()
            .unwrap_or_else(|| LogicalLogView::from_cluster(before, envelope.from));
        if let Some(current_prefix) = self.prefix_from_view(&source_view, index) {
            if current_prefix != transferred_prefix {
                self.violations.insert(LogicalLogViolation {
                    invariant: catalog::LG_03_LOG_MATCHING,
                    message: format!(
                        "{} snapshot transfer {transfer_id} no longer matches its current source prefix",
                        envelope.from
                    ),
                });
            }
        }
        let witnessed =
            self.insert_snapshot_prefix(envelope.to, transfer_id, index, term, transferred_prefix);
        // Installation replaces the receiver's old logical prefix, which may
        // be divergent. The next observation must use transfer evidence, not
        // infer provenance from that retired view.
        self.last_views_by_node.remove(&envelope.to);
        if !witnessed {
            self.unwitnessed_snapshots
                .insert((envelope.to, transfer_id, index, term));
        }
    }
}
