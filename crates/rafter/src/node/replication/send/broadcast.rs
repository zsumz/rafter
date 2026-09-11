//! Broadcast demand separates new payload availability from required contact.

use super::{LogBatchCache, Node, Output, ReplicationDemand};

impl Node {
    pub(in crate::node) fn broadcast_append_entries(&mut self) -> Vec<Output> {
        let mut outputs = Vec::new();
        self.broadcast_append_entries_into(&mut outputs);
        outputs
    }

    pub(in crate::node) fn broadcast_append_entries_into(&mut self, outputs: &mut Vec<Output>) {
        self.broadcast_replication_into(ReplicationDemand::EnsureContact, outputs);
    }

    pub(in crate::node) fn broadcast_replication_into(
        &mut self,
        demand: ReplicationDemand,
        outputs: &mut Vec<Output>,
    ) {
        // Proposal nudges may reach only some followers. They never postpone the
        // contact round needed to recover another follower's full/lost window.
        if demand.requires_message() {
            self.begin_contact_round();
        }
        self.reconcile_replication_progress();

        let local_id = self.id();
        let replica_count = self.leader.progress.replica_count();
        outputs.reserve(replica_count.saturating_sub(1));

        let mut batch_cache = LogBatchCache::default();
        for slot in 0..replica_count {
            let Some(follower_id) = self.leader.progress.replica_id_at(slot) else {
                continue;
            };
            if follower_id == local_id {
                continue;
            }

            self.replicate_to_follower_with_cache_fresh(
                follower_id,
                demand,
                outputs,
                &mut batch_cache,
            );
        }
    }
}
