//! Maintenance of leader-side replication progress.
//!
//! Progress is derived against effective membership and refreshed before any
//! send, acknowledgement update, or quorum-derived commit calculation.

use crate::NodeId;

use super::super::state::Progress;
use super::super::{Node, Role};

impl Node {
    pub(in crate::node) fn try_follower_progress_mut(
        &mut self,
        follower_id: NodeId,
    ) -> Option<&mut Progress> {
        self.refresh_leader_progress_index();
        self.leader.progress.get_mut(follower_id)
    }

    pub(in crate::node) fn refresh_leader_progress_index(&mut self) {
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
            self.leader.progress.rebuild(
                membership,
                self_id,
                first_sendable_index,
                local_match_index,
            );
            return;
        }

        let membership = self.effective_membership();
        self.leader.progress.rebuild(
            &membership,
            self_id,
            first_sendable_index,
            local_match_index,
        );
    }

    pub(in crate::node) fn record_local_progress(&mut self) {
        let last_log_index = self.last_log_index();
        self.refresh_leader_progress_index();

        if let Some(local) = self.leader.progress.get_mut(self.id()) {
            local.match_index = last_log_index;
            local.next_index = last_log_index.next();
        }
    }
}
