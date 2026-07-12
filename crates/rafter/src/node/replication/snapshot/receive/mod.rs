//! Follower-side snapshot authority and shared receive state.

mod chunk;
mod disposition;
mod whole;

use crate::{LogIndex, NodeId, Term};

use super::super::super::{Node, Output, Role};

impl Node {
    fn adopt_snapshot_term(&mut self, term: Term) -> Vec<Output> {
        if term > self.current_term() || self.role() != Role::Follower {
            self.become_follower(term)
        } else {
            Vec::new()
        }
    }

    fn record_accepted_snapshot_leader(&mut self, leader_id: NodeId) {
        self.election.reset_timeout();
        self.volatile.leader_hint = Some(leader_id);
    }

    fn snapshot_covered_through(&self) -> LogIndex {
        self.snapshot_index().max(self.commit_index())
    }
}
