use std::collections::{BTreeMap, BTreeSet};

use crate::{
    AppendEntries, AppendEntriesResponse, ConfigurationEntry, ConfigurationId, ConfigurationPhase,
    JointMembership, LocalProposalId, LogEntry, LogIndex, MembershipConfig, MembershipSet,
    MembershipValidationError, Message, NodeId, PromotionBarrier, SharedEntries,
};

mod append;
mod proposal;
mod snapshot;

pub use snapshot::PendingSnapshotTransferResumeError;

use super::commit::CommitTracker;
use super::state::{Progress, ProgressMode};
use super::{
    ConfigurationProposalRejection, LocalProposalDropReason, Node, Output, ProposalRejection, Role,
};

impl Node {
    /// Drains application outputs for committed log entries that have not
    /// yet been applied in this process.
    ///
    /// This is the recovery companion to
    /// [`Node::from_bootstrap_applied_through`](crate::Node::from_bootstrap_applied_through):
    /// after constructing from durable state, call this once to replay
    /// committed entries above the application's durable applied floor
    /// without waiting for a later commit-index advance.
    ///
    /// # Panics
    ///
    /// Panics only if the committed index points beyond the retained log — a
    /// kernel bug or invalid bootstrap state, since bootstrap validation and
    /// log mutation maintain that invariant.
    #[must_use]
    pub fn drain_committed_outputs(&mut self) -> Vec<Output> {
        self.apply_committed()
    }

    pub(super) fn handle_append_entries(
        &mut self,
        leader_id: NodeId,
        request: &AppendEntries,
    ) -> Vec<Output> {
        let sequence = request.sequence;
        if request.term < self.current_term() {
            return vec![self.append_entries_response(leader_id, false, LogIndex::ZERO, sequence)];
        }

        let mut outputs = if request.term > self.current_term() || self.role() != Role::Follower {
            self.become_follower(request.term)
        } else {
            Vec::new()
        };
        self.election_elapsed = 0;
        // Accepted leader traffic refreshes the pre-vote stickiness hint.
        self.volatile.leader_hint = Some(leader_id);

        if self.term_at(request.prev_log_index) != Some(request.prev_log_term) {
            outputs.push(self.append_entries_response(leader_id, false, LogIndex::ZERO, sequence));
            return outputs;
        }

        let match_index = request_match_index(request.prev_log_index, request.entries.len());

        let confirmed_commit_index = if request.leader_commit > self.volatile.commit_index {
            let confirmed = std::cmp::min(request.leader_commit, match_index);
            std::cmp::max(self.volatile.commit_index, confirmed)
        } else {
            self.volatile.commit_index
        };

        let Some(splice_outputs) = self.splice_entries_after(
            request.prev_log_index,
            &request.entries,
            confirmed_commit_index,
        ) else {
            outputs.push(self.append_entries_response(leader_id, false, LogIndex::ZERO, sequence));
            return outputs;
        };
        outputs.extend(splice_outputs);

        if request.leader_commit > self.volatile.commit_index {
            // The commit index advances no further than the last entry THIS
            // frame confirmed (figure 2: min(leaderCommit, index of last new
            // entry)) — never to the local log tail, which may hold an
            // uncommitted divergent suffix above the confirmed prefix that a
            // pipelined empty append says nothing about. The max floor keeps
            // it monotone: a probe that walked back to a low prev index
            // confirms little, but a higher leader_commit must never drag an
            // already-committed index backwards.
            self.volatile.commit_index = confirmed_commit_index;
            self.apply_committed_into(&mut outputs);
            outputs.push(self.append_entries_response(leader_id, true, match_index, sequence));
            return outputs;
        }

        outputs.push(self.append_entries_response(leader_id, true, match_index, sequence));
        outputs
    }

    pub(super) fn handle_append_entries_response(
        &mut self,
        follower_id: NodeId,
        response: AppendEntriesResponse,
    ) -> Vec<Output> {
        if response.term > self.current_term() {
            return self.become_follower(response.term);
        }

        if self.role() != Role::Leader || response.term != self.current_term() {
            return Vec::new();
        }

        // Any same-term response proves the follower still recognizes this
        // leader: it counts for check-quorum, for the read lease, and, via
        // its echoed round, for pending read barriers.
        self.record_quorum_ack(follower_id);
        self.acknowledge_read_lease(follower_id, response.sequence);
        let mut read_grants = self.acknowledge_read_barriers(follower_id, response.sequence);

        if response.success {
            let snapshot_index = self.snapshot_index();
            let reported_match_index = std::cmp::min(response.match_index, self.last_log_index());
            let commit_index = self.volatile.commit_index;
            let Some(can_advance_commit) =
                self.try_follower_progress_mut(follower_id).map(|progress| {
                    let old_match_index = progress.match_index;
                    progress.match_index = progress.match_index.max(reported_match_index);
                    let acknowledged = progress.match_index;
                    progress.inflights.free_through(acknowledged);
                    // A successful append acknowledgement at or beyond the snapshot
                    // boundary confirms the follower has the log — including one
                    // that arrives mid-snapshot, which cancels the transfer. Below
                    // the boundary it proves only a compacted prefix: a stale
                    // pre-snapshot ack must not reset a live transfer's cursor.
                    if !matches!(progress.mode, ProgressMode::Snapshot { .. })
                        || acknowledged >= snapshot_index
                    {
                        progress.confirm_replicating();
                    }
                    progress.next_index = progress.next_index.max(acknowledged.next());
                    successful_ack_can_advance_commit(old_match_index, acknowledged, commit_index)
                })
            else {
                return read_grants;
            };
            read_grants.extend(self.maybe_complete_leadership_transfer(follower_id));
            if can_advance_commit {
                self.advance_commit_index_into(&mut read_grants);
            }
            // Freed window slots pull the next batches immediately: catch-up
            // is acknowledgement-paced, not heartbeat-paced.
            self.replicate_to_peer(follower_id, false, &mut read_grants);
            return read_grants;
        }

        let snapshot_index = self.snapshot_index();
        let Some(progress) = self.try_follower_progress_mut(follower_id) else {
            return read_grants;
        };
        if !matches!(progress.mode, ProgressMode::Snapshot { .. }) {
            progress.collapse_into_probe(snapshot_index);
        }
        self.replicate_to_peer(follower_id, true, &mut read_grants);
        read_grants
    }

    pub(super) fn add_learner(&mut self, learner_id: NodeId) -> Vec<Output> {
        if let Err(rejection) = self.validate_configuration_proposal_preflight() {
            return Self::reject_proposal(None, rejection);
        }

        let current = match self.stable_effective_membership() {
            Ok(current) => current,
            Err(rejection) => return Self::reject_configuration(rejection),
        };
        if current.voters().contains(&learner_id) || current.learners().contains(&learner_id) {
            return Self::reject_configuration(ConfigurationProposalRejection::NodeAlreadyMember {
                node_id: learner_id,
            });
        }

        let mut learners = current.learners().to_vec();
        learners.push(learner_id);
        let membership = match validated_derived_membership(MembershipSet::new(
            current.voters().to_vec(),
            learners,
        )) {
            Ok(membership) => membership,
            Err(rejection) => return Self::reject_configuration(rejection),
        };
        let configuration = ConfigurationEntry::stable(self.next_configuration_id(), membership);
        self.append_valid_configuration_proposal(configuration, &[])
    }

    pub(super) fn promote_learner(
        &mut self,
        learner_id: NodeId,
        promotion_barrier: PromotionBarrier,
    ) -> Vec<Output> {
        if let Err(rejection) = self.validate_configuration_proposal_preflight() {
            return Self::reject_proposal(None, rejection);
        }

        let current = match self.stable_effective_membership() {
            Ok(current) => current,
            Err(rejection) => return Self::reject_configuration(rejection),
        };
        if !current.learners().contains(&learner_id) {
            return Self::reject_configuration(
                ConfigurationProposalRejection::PromotionTargetNotLearner {
                    node_id: learner_id,
                },
            );
        }

        let mut voters = current.voters().to_vec();
        voters.push(learner_id);
        let learners = current
            .learners()
            .iter()
            .copied()
            .filter(|node_id| *node_id != learner_id)
            .collect();
        let target = match validated_derived_membership(MembershipSet::new(voters, learners)) {
            Ok(target) => target,
            Err(rejection) => return Self::reject_configuration(rejection),
        };
        let configuration = ConfigurationEntry::joint(
            self.next_configuration_id(),
            JointMembership::new(current, target),
        );
        self.append_valid_configuration_proposal(configuration, &[promotion_barrier])
    }

    pub(super) fn remove_voter(&mut self, voter_id: NodeId) -> Vec<Output> {
        if let Err(rejection) = self.validate_configuration_proposal_preflight() {
            return Self::reject_proposal(None, rejection);
        }

        let current = match self.stable_effective_membership() {
            Ok(current) => current,
            Err(rejection) => return Self::reject_configuration(rejection),
        };
        if !current.voters().contains(&voter_id) {
            return Self::reject_configuration(ConfigurationProposalRejection::VoterNotFound {
                node_id: voter_id,
            });
        }
        if current.voters().len() == 1 {
            return Self::reject_configuration(
                ConfigurationProposalRejection::CannotRemoveLastVoter { node_id: voter_id },
            );
        }

        let voters = current
            .voters()
            .iter()
            .copied()
            .filter(|node_id| *node_id != voter_id)
            .collect();
        let target = match validated_derived_membership(MembershipSet::new(
            voters,
            current.learners().to_vec(),
        )) {
            Ok(target) => target,
            Err(rejection) => return Self::reject_configuration(rejection),
        };
        let configuration = ConfigurationEntry::joint(
            self.next_configuration_id(),
            JointMembership::new(current, target),
        );
        self.append_valid_configuration_proposal(configuration, &[])
    }

    pub(super) fn enter_joint(
        &mut self,
        target: MembershipSet,
        promotion_barriers: &[PromotionBarrier],
    ) -> Vec<Output> {
        if let Err(rejection) = self.validate_configuration_proposal_preflight() {
            return Self::reject_proposal(None, rejection);
        }

        let current = match self.stable_effective_membership() {
            Ok(current) => current,
            Err(rejection) => return Self::reject_configuration(rejection),
        };
        if target == current {
            return Self::reject_configuration(
                ConfigurationProposalRejection::TargetMembershipUnchanged,
            );
        }

        let configuration = ConfigurationEntry::joint(
            self.next_configuration_id(),
            JointMembership::new(current, target),
        );
        self.append_valid_configuration_proposal(configuration, promotion_barriers)
    }

    pub(super) fn leave_joint(&mut self) -> Vec<Output> {
        if let Err(rejection) = self.validate_configuration_proposal_preflight() {
            return Self::reject_proposal(None, rejection);
        }

        let MembershipConfig::Joint(joint) = self.effective_membership() else {
            return Self::reject_configuration(
                ConfigurationProposalRejection::JointConfigurationRequired {
                    phase: ConfigurationPhase::Stable,
                },
            );
        };
        let configuration = ConfigurationEntry::stable(
            self.next_configuration_id(),
            joint.new_membership().clone(),
        );
        self.append_valid_configuration_proposal(configuration, &[])
    }

    pub(super) fn change_membership(
        &mut self,
        target: MembershipSet,
        promotion_barriers: &[PromotionBarrier],
    ) -> Vec<Output> {
        if let Err(rejection) = self.validate_configuration_proposal_preflight() {
            return Self::reject_proposal(None, rejection);
        }

        let configuration = match self.effective_membership() {
            MembershipConfig::Stable(current) if target == current => {
                return Self::reject_configuration(
                    ConfigurationProposalRejection::TargetMembershipUnchanged,
                );
            }
            MembershipConfig::Stable(current) if target.voters() == current.voters() => {
                ConfigurationEntry::stable(self.next_configuration_id(), target)
            }
            MembershipConfig::Stable(current) => ConfigurationEntry::joint(
                self.next_configuration_id(),
                JointMembership::new(current, target),
            ),
            MembershipConfig::Joint(joint) if target == *joint.new_membership() => {
                ConfigurationEntry::stable(self.next_configuration_id(), target)
            }
            MembershipConfig::Joint(_) => {
                return Self::reject_configuration(
                    ConfigurationProposalRejection::TargetMembershipDoesNotMatchJointNewSide,
                );
            }
        };

        self.append_valid_configuration_proposal(configuration, promotion_barriers)
    }

    pub(super) fn dangerous_raw_configuration_proposal(
        &mut self,
        configuration: ConfigurationEntry,
        promotion_barriers: &[PromotionBarrier],
    ) -> Vec<Output> {
        if let Err(rejection) = self.validate_configuration_proposal_preflight() {
            return Self::reject_proposal(None, rejection);
        }
        self.append_valid_configuration_proposal(configuration, promotion_barriers)
    }

    /// Feeds a follower acknowledgement of `sequence` to the lease
    /// checkpoint machine; a quorum confirms the pending basis and re-arms
    /// the next checkpoint at the current tick.
    pub(super) fn acknowledge_read_lease(&mut self, follower_id: NodeId, sequence: u64) {
        if !self.config.lease_reads() {
            return;
        }
        let membership = self.effective_membership();
        let self_id = self.id();
        if !self
            .leader
            .lease
            .record_ack(follower_id, sequence, &membership, self_id)
        {
            return;
        }
        if self
            .leader
            .lease
            .acks
            .has_quorum_with_self(&membership, self_id)
        {
            let now = self.leader.ticks;
            let next_sequence = self.leader.heartbeat_sequence + 1;
            self.leader.lease.confirm_and_rearm(now, next_sequence);
        }
    }

    /// Re-arms a lease checkpoint whose basis has aged past the window:
    /// confirming it could no longer extend the lease, so a fresh basis
    /// starts accumulating acknowledgements instead.
    pub(super) fn tick_read_lease(&mut self) {
        if !self.config.lease_reads() {
            return;
        }
        let stale = self
            .leader
            .ticks
            .saturating_sub(self.leader.lease.pending_basis_tick)
            >= self.config.read_lease_ticks();
        if stale {
            let now = self.leader.ticks;
            let next_sequence = self.leader.heartbeat_sequence + 1;
            self.leader.lease.rearm(now, next_sequence);
        }
    }

    /// Steps down when check-quorum is enabled and a full election timeout
    /// passed without hearing from a quorum (thesis 6.2). Returns true when
    /// the node stepped down.
    pub(super) fn tick_check_quorum(&mut self) -> Option<Vec<Output>> {
        if !self.config.check_quorum() {
            return None;
        }
        self.leader.quorum_check_elapsed += 1;
        if self.leader.quorum_check_elapsed < self.config.election_timeout_ticks() {
            return None;
        }
        let membership = self.effective_membership();
        let self_id = self.id();
        if self
            .leader
            .quorum_acks
            .has_quorum_with_self(&membership, self_id)
        {
            self.leader.quorum_acks.clear();
            self.leader.quorum_check_elapsed = 0;
            return None;
        }
        // Stepping down in the same term: explicitly forget the self-hint so
        // this node grants pre-votes immediately — it has just concluded no
        // current leader can reach a quorum through it.
        let outputs = self.become_follower(self.current_term());
        self.volatile.leader_hint = None;
        Some(outputs)
    }

    pub(super) fn try_follower_progress_mut(&mut self, peer: NodeId) -> Option<&mut Progress> {
        self.refresh_leader_progress_index();
        self.leader.progress.get_mut(peer)
    }

    pub(super) fn refresh_leader_progress_index(&mut self) {
        if self.role() != Role::Leader {
            return;
        }
        let self_id = self.id();
        let first_sendable_index = self.snapshot_index().next();
        let local_match_index = self.last_log_index();
        if self.configuration_offsets.is_empty() {
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

    fn validate_configuration_proposal_preflight(&self) -> Result<(), ProposalRejection> {
        if self.role() != Role::Leader {
            return Err(ProposalRejection::NotLeader {
                role: self.role(),
                term: self.current_term(),
                payload_len: 0,
            });
        }
        if let Some(transfer) = self.leader.pending_transfer.as_ref() {
            return Err(ProposalRejection::LeadershipTransferInProgress {
                target: transfer.target,
            });
        }
        if let Some(index) = self.uncommitted_configuration_indexes().first().copied() {
            return Err(ProposalRejection::Configuration(
                ConfigurationProposalRejection::UncommittedConfiguration { index },
            ));
        }
        Ok(())
    }

    fn record_quorum_ack(&mut self, follower_id: NodeId) {
        let self_id = self.id();
        if self.configuration_offsets.is_empty() {
            let membership = self
                .persistent
                .snapshot
                .as_ref()
                .and_then(|snapshot| snapshot.metadata.committed_membership())
                .unwrap_or_else(|| self.config.static_membership_ref());
            self.leader
                .quorum_acks
                .insert(follower_id, membership, self_id);
            return;
        }
        let membership = self.effective_membership();
        self.leader
            .quorum_acks
            .insert(follower_id, &membership, self_id);
    }

    fn stable_effective_membership(&self) -> Result<MembershipSet, ConfigurationProposalRejection> {
        match self.effective_membership() {
            MembershipConfig::Stable(membership) => Ok(membership),
            MembershipConfig::Joint(_) => Err(
                ConfigurationProposalRejection::StableConfigurationRequired {
                    phase: ConfigurationPhase::Joint,
                },
            ),
        }
    }

    fn next_configuration_id(&self) -> ConfigurationId {
        self.effective_configuration_entry()
            .map(|entry| entry.config_id())
            .or_else(|| {
                self.committed_configuration_state_at(self.commit_index())
                    .map(|state| state.config_id)
            })
            .unwrap_or(ConfigurationId(0))
            .next()
    }

    fn reject_configuration(rejection: ConfigurationProposalRejection) -> Vec<Output> {
        Self::reject_proposal(None, ProposalRejection::Configuration(rejection))
    }

    fn reject_proposal(
        proposal_id: Option<LocalProposalId>,
        reason: ProposalRejection,
    ) -> Vec<Output> {
        vec![Output::RejectProposal {
            proposal_id,
            reason,
        }]
    }

    fn append_valid_configuration_proposal(
        &mut self,
        configuration: ConfigurationEntry,
        promotion_barriers: &[PromotionBarrier],
    ) -> Vec<Output> {
        if let Err(rejection) =
            self.validate_configuration_promotion_barriers(&configuration, promotion_barriers)
        {
            return Self::reject_configuration(rejection);
        }

        let entry = LogEntry::configuration(self.current_term(), configuration);
        self.append_log_entry(entry);
        self.record_local_progress();

        let mut outputs = Vec::new();
        self.advance_commit_index_into(&mut outputs);
        if self.role() == Role::Leader {
            self.broadcast_append_entries_into(&mut outputs);
        }
        outputs
    }

    fn record_local_progress(&mut self) {
        let last_log_index = self.last_log_index();
        self.refresh_leader_progress_index();
        if let Some(local) = self.leader.progress.get_mut(self.id()) {
            local.match_index = last_log_index;
            local.next_index = last_log_index.next();
        }
    }

    fn validate_configuration_promotion_barriers(
        &self,
        configuration: &ConfigurationEntry,
        promotion_barriers: &[PromotionBarrier],
    ) -> Result<(), ConfigurationProposalRejection> {
        let mut barriers = BTreeMap::new();
        for barrier in promotion_barriers {
            if barriers.insert(barrier.learner_id, *barrier).is_some() {
                return Err(ConfigurationProposalRejection::DuplicatePromotionBarrier {
                    learner_id: barrier.learner_id,
                });
            }
        }

        let ConfigurationEntry::Joint { membership, .. } = configuration else {
            if let Some(barrier) = promotion_barriers.first() {
                return Err(ConfigurationProposalRejection::UnusedPromotionBarrier {
                    learner_id: barrier.learner_id,
                });
            }
            return Ok(());
        };

        let current_membership = self.effective_membership();
        let mut used_barriers = BTreeSet::new();

        for promoted_node in membership.new_membership().voters() {
            if current_membership.contains_voter(*promoted_node) {
                continue;
            }

            if !current_membership.contains_learner(*promoted_node) {
                return Err(ConfigurationProposalRejection::PromotionTargetNotLearner {
                    node_id: *promoted_node,
                });
            }

            let Some(barrier) = barriers.get(promoted_node).copied() else {
                return Err(ConfigurationProposalRejection::MissingPromotionBarrier {
                    learner_id: *promoted_node,
                });
            };
            used_barriers.insert(*promoted_node);

            if barrier.required_match_index != self.commit_index() {
                return Err(ConfigurationProposalRejection::StalePromotionBarrier {
                    learner_id: *promoted_node,
                    required_match_index: self.commit_index(),
                    supplied_match_index: barrier.required_match_index,
                });
            }

            let actual_match_index = self
                .leader
                .progress
                .get(*promoted_node)
                .map(|progress| progress.match_index)
                .unwrap_or_default();
            if actual_match_index < barrier.required_match_index {
                return Err(ConfigurationProposalRejection::PromotionBarrierNotReached {
                    learner_id: *promoted_node,
                    required_match_index: barrier.required_match_index,
                    actual_match_index,
                });
            }
        }

        if let Some(barrier) = promotion_barriers
            .iter()
            .find(|barrier| !used_barriers.contains(&barrier.learner_id))
        {
            return Err(ConfigurationProposalRejection::UnusedPromotionBarrier {
                learner_id: barrier.learner_id,
            });
        }

        Ok(())
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

    /// Splices `entries` after `prev_log_index`: the matching prefix is
    /// skipped, the first divergent index truncates the local suffix, and
    /// the remainder appends. Validation runs entirely before mutation, so
    /// rejection never needs a rollback (and acceptance never clones the
    /// log): a divergence at or below the commit index rejects, as does a
    /// splice whose resulting log would hold more than one uncommitted
    /// configuration entry after this frame's commit floor is applied.
    fn splice_entries_after(
        &mut self,
        prev_log_index: LogIndex,
        entries: &SharedEntries,
        configuration_commit_floor: LogIndex,
    ) -> Option<Vec<Output>> {
        // Indexes ascend, so a committed conflict is always encountered
        // before any acceptable divergence.
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
            // The whole batch matches the existing log: nothing to splice,
            // and the single-uncommitted-configuration invariant already
            // held.
            return Some(Vec::new());
        };

        // The spliced log's uncommitted configuration entries after this
        // frame's commit floor takes effect: survivors below the divergence
        // plus incoming appends whose indexes remain above the floor.
        let first_log_index = self.snapshot_index().next();
        let surviving_configurations = self
            .configuration_offsets
            .iter()
            .filter(|offset| {
                let index = LogIndex(first_log_index.0 + **offset as u64);
                index > configuration_commit_floor && index < first_index
            })
            .count();
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

    pub(super) fn advance_commit_index(&mut self) -> Vec<Output> {
        let mut outputs = Vec::new();
        self.advance_commit_index_into(&mut outputs);
        outputs
    }

    pub(super) fn advance_commit_index_into(&mut self, outputs: &mut Vec<Output>) {
        self.refresh_leader_progress_index();
        let Some(candidate) = CommitTracker::new(&self.leader.progress).candidate() else {
            return;
        };
        if candidate <= self.volatile.commit_index {
            return;
        }
        if self.term_at(candidate) != Some(self.current_term()) {
            return;
        }

        self.volatile.commit_index = candidate;
        self.apply_committed_into(outputs);
    }

    fn apply_committed(&mut self) -> Vec<Output> {
        let mut outputs = Vec::new();
        self.apply_committed_into(&mut outputs);
        outputs
    }

    fn apply_committed_into(&mut self, outputs: &mut Vec<Output>) {
        while self.volatile.applied_index < self.volatile.commit_index {
            let index = self.volatile.applied_index.next();
            let Some(entry) = self.entry_at(index) else {
                break;
            };
            let entry_term = entry.term;
            let application_payload = match &entry.kind {
                crate::LogEntryKind::Application(payload) => Some(payload.clone()),
                _ => None,
            };
            let membership = entry
                .configuration_entry()
                .map(ConfigurationEntry::membership_config);
            self.volatile.applied_index = index;
            let local_proposal_id = self
                .volatile
                .local_proposals
                .remove(index)
                .and_then(|proposal| (proposal.term == entry_term).then_some(proposal.id));
            if let Some(payload) = application_payload {
                outputs.push(Output::Apply {
                    index,
                    term: entry_term,
                    payload,
                    local_proposal_id,
                });
            } else if let Some(membership) = membership {
                if self.role() == Role::Leader && !membership.contains_voter(self.id()) {
                    outputs.extend(self.become_follower(self.current_term()));
                }
            }
        }
    }
}

fn validated_derived_membership(
    result: Result<MembershipSet, MembershipValidationError>,
) -> Result<MembershipSet, ConfigurationProposalRejection> {
    result.map_err(|error| ConfigurationProposalRejection::InvalidMembership { error })
}

fn successful_ack_can_advance_commit(
    old_match_index: LogIndex,
    acknowledged: LogIndex,
    commit_index: LogIndex,
) -> bool {
    acknowledged > old_match_index && acknowledged > commit_index
}

fn request_match_index(prev_log_index: LogIndex, entry_count: usize) -> LogIndex {
    LogIndex(prev_log_index.0 + entry_count as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commit_advance_requires_new_evidence() {
        assert!(successful_ack_can_advance_commit(
            LogIndex(2),
            LogIndex(5),
            LogIndex(4),
        ));
        assert!(!successful_ack_can_advance_commit(
            LogIndex(5),
            LogIndex(5),
            LogIndex(4),
        ));
        assert!(!successful_ack_can_advance_commit(
            LogIndex(2),
            LogIndex(4),
            LogIndex(4),
        ));
    }
}
