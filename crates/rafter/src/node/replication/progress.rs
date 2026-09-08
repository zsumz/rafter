//! Maintenance of leader-side replication progress.
//!
//! Reconciles effective membership slots while preserving observed follower
//! acknowledgements. Only the slot mapping is locally reconstructible; a
//! follower's matching prefix is evidence received from that follower.

use crate::NodeId;

use super::super::state::Progress;
use super::super::{Node, Role};

impl Node {
    /// Reconciles membership and local progress before looking up a follower.
    pub(in crate::node) fn reconcile_follower_progress_mut(
        &mut self,
        follower_id: NodeId,
    ) -> Option<&mut Progress> {
        self.reconcile_replication_progress();
        self.leader.progress.get_mut(follower_id)
    }

    pub(in crate::node) fn reconcile_replication_progress(&mut self) {
        if self.role() != Role::Leader {
            return;
        }

        let self_id = self.id();
        let first_sendable_index = self.snapshot_index().next();
        let local_match_index = self.last_log_index();
        if self.derived.configuration.is_empty() {
            let membership = self
                .persistent
                .snapshot
                .as_ref()
                .and_then(|snapshot| snapshot.metadata.committed_membership())
                .unwrap_or_else(|| self.config.static_membership_ref());
            self.leader.progress.reconcile_membership(
                membership,
                self_id,
                first_sendable_index,
                local_match_index,
            );
            return;
        }

        let membership = self.effective_membership();
        self.leader.progress.reconcile_membership(
            &membership,
            self_id,
            first_sendable_index,
            local_match_index,
        );
    }

    pub(in crate::node) fn record_local_progress(&mut self) {
        let last_log_index = self.last_log_index();
        self.reconcile_replication_progress();

        if let Some(local) = self.leader.progress.get_mut(self.id()) {
            local.match_index = last_log_index;
            local.next_index = last_log_index.next();
        }
    }
}
