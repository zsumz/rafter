//! Follower-side `AppendEntries` validation and log splicing.
//!
//! Owns term and prefix gates, conflict repair, follower commit advancement,
//! and the ordered response emitted after mutation.

use crate::{AppendEntries, AppendEntriesResponse, LogIndex, Message, NodeId, SharedEntries};

use super::super::{LocalProposalDropReason, Node, Output, Role};

impl Node {
    pub(in crate::node) fn handle_append_entries(
        &mut self,
        leader_id: NodeId,
        request: &AppendEntries,
    ) -> Vec<Output> {
        let sequence = request.sequence;
        if request.term < self.current_term() {
            return vec![self.reject_append_entries(leader_id, sequence)];
        }

        let mut outputs = if request.term > self.current_term() || self.role() != Role::Follower {
            self.become_follower(request.term)
        } else {
            Vec::new()
        };
        self.election.reset_timeout();
        // Accepted leader traffic refreshes the pre-vote stickiness hint.
        self.volatile.leader_hint = Some(leader_id);

        if self.term_at(request.prev_log_index) != Some(request.prev_log_term) {
            outputs.push(self.reject_append_entries(leader_id, sequence));
            return outputs;
        }

        let match_index = request_match_index(request.prev_log_index, request.entries.len());
        let confirmed_commit_index = self.confirmed_commit_index(request, match_index);

        let Some(splice_outputs) = self.splice_entries_after(
            request.prev_log_index,
            &request.entries,
            confirmed_commit_index,
        ) else {
            outputs.push(self.reject_append_entries(leader_id, sequence));
            return outputs;
        };
        outputs.extend(splice_outputs);

        if request.leader_commit > self.volatile.commit_index {
            self.volatile.commit_index = confirmed_commit_index;
            self.apply_committed_into(&mut outputs);
        }

        outputs.push(self.accept_append_entries(leader_id, match_index, sequence));
        outputs
    }

    /// Returns the highest commit index this append frame proves locally.
    ///
    /// Raft Figure 2 bounds follower commit advancement by the final index
    /// confirmed by this frame, not by an unrelated local suffix. The local
    /// floor keeps commit monotone when a probe walks back below it.
    fn confirmed_commit_index(&self, request: &AppendEntries, match_index: LogIndex) -> LogIndex {
        self.commit_index()
            .max(request.leader_commit.min(match_index))
    }

    fn accept_append_entries(
        &self,
        leader_id: NodeId,
        match_index: LogIndex,
        sequence: u64,
    ) -> Output {
        self.append_entries_response(leader_id, true, match_index, sequence)
    }

    fn reject_append_entries(&self, leader_id: NodeId, sequence: u64) -> Output {
        self.append_entries_response(leader_id, false, LogIndex::ZERO, sequence)
    }

    fn append_entries_response(
        &self,
        leader_id: NodeId,
        success: bool,
        match_index: LogIndex,
        sequence: u64,
    ) -> Output {
        Output::Send {
            to: leader_id,
            message: Message::AppendEntriesResponse(AppendEntriesResponse {
                term: self.current_term(),
                follower_id: self.id(),
                success,
                match_index,
                sequence,
            }),
        }
    }

    /// Splices `entries` after `prev_log_index`.
    ///
    /// The matching prefix is skipped, the first divergent index truncates the
    /// local suffix, and the remainder appends. Validation completes before
    /// mutation, so rejection needs no rollback. A divergence at or below the
    /// commit index is rejected, as is a result containing more than one
    /// uncommitted configuration after this frame's commit floor takes effect.
    fn splice_entries_after(
        &mut self,
        prev_log_index: LogIndex,
        entries: &SharedEntries,
        configuration_commit_floor: LogIndex,
    ) -> Option<Vec<Output>> {
        // Indexes ascend, so a committed conflict appears before any
        // acceptable divergence.
        let mut divergence: Option<(usize, LogIndex)> = None;
        for (offset, entry) in entries.iter().enumerate() {
            let index = LogIndex(prev_log_index.0 + 1 + offset as u64);
            match self.term_at(index) {
                Some(existing_term) if existing_term == entry.term => {}
                Some(_) if index <= self.volatile.commit_index => return None,
                _ => {
                    divergence = Some((offset, index));
                    break;
                }
            }
        }

        let Some((first_offset, first_index)) = divergence else {
            // The whole batch already matches. The log's existing
            // single-uncommitted-configuration invariant remains sufficient.
            return Some(Vec::new());
        };

        // Count surviving configuration entries below the divergence and
        // incoming configuration entries above the frame's commit floor.
        let first_log_index = self.snapshot_index().next();
        let surviving_configurations = self.derived.configuration.count_between(
            first_log_index,
            configuration_commit_floor,
            first_index,
        );
        let incoming_configurations = entries.as_slice()[first_offset..]
            .iter()
            .enumerate()
            .filter(|(offset, entry)| {
                let index = LogIndex(first_index.0 + *offset as u64);
                index > configuration_commit_floor && entry.kind.is_configuration()
            })
            .count();
        if surviving_configurations + incoming_configurations > 1 {
            return None;
        }

        let outputs = if first_index <= self.last_log_index() {
            self.truncate_from(first_index, LocalProposalDropReason::LogOverwritten)
        } else {
            Vec::new()
        };
        for entry in entries.iter().skip(first_offset).cloned() {
            self.append_log_entry(entry);
        }
        Some(outputs)
    }
}

fn request_match_index(prev_log_index: LogIndex, entry_count: usize) -> LogIndex {
    LogIndex(prev_log_index.0 + entry_count as u64)
}
