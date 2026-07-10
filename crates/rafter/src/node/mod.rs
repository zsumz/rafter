mod bootstrap;
mod commit;
mod config;
mod election;
mod event;
mod lifecycle;
mod log;
mod read_index;
mod replication;
mod state;
mod transfer;

#[cfg(test)]
mod tests;

use std::collections::BTreeSet;

use crate::{
    CommittedConfiguration, ConfigurationEntry, FollowerSnapshotTransferStatus,
    LeaderSnapshotTransferStatus, LogEntry, LogIndex, MembershipConfig, Message, NodeId,
    PromotionBarrier, ReplicationProgress, ReplicationState, SnapshotChunkRejectionCounters,
    SnapshotTransferStatus, Term,
};

pub use bootstrap::{BootstrapLogEntry, BootstrapState, BootstrapValidationError};
pub use config::{NodeConfig, NodeConfigError};
pub use event::{
    ClientProposalInput, ConfigurationProposalRejection, Input, LeadershipTransferRejection,
    LocalProposalDropReason, Output, ProposalRejection, ReadIndexCancelReason, ReadIndexRejection,
    Role,
};
pub use replication::PendingSnapshotTransferResumeError;
use state::{LeaderState, LocalProposalTracker, MembershipIndex, PersistentState, VolatileState};

/// Pure deterministic Raft state machine.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Node {
    config: NodeConfig,
    persistent: PersistentState,
    volatile: VolatileState,
    leader: LeaderState,
    election_elapsed: u64,
    granted_votes: BTreeSet<NodeId>,
    // Grants collected during a pre-vote round; volatile by design, since
    // pre-vote state is never persisted (thesis 9.6).
    granted_pre_votes: BTreeSet<NodeId>,
    snapshot_chunk_rejections: SnapshotChunkRejectionCounters,
    /// Offsets of configuration entries within `persistent.log`, maintained
    /// on every log mutation so membership lookups never scan the log.
    /// Purely derived state: equal logs always carry equal offsets.
    configuration_offsets: Vec<usize>,
}

impl Node {
    /// Builds a fresh node with empty durable state.
    #[must_use]
    pub fn new(config: NodeConfig) -> Self {
        Self {
            config,
            persistent: PersistentState::default(),
            volatile: VolatileState::default(),
            leader: LeaderState::default(),
            election_elapsed: 0,
            granted_votes: BTreeSet::new(),
            granted_pre_votes: BTreeSet::new(),
            snapshot_chunk_rejections: SnapshotChunkRejectionCounters::default(),
            configuration_offsets: Vec::new(),
        }
    }

    /// Constructs a deterministic Raft node from persisted protocol state.
    ///
    /// Hydration restores hard state and log entries, then starts the node as a
    /// follower with default volatile election and replication progress.
    ///
    /// # Errors
    ///
    /// Returns [`BootstrapValidationError`] when the bootstrap state is not a
    /// valid persisted Raft state for the supplied static configuration.
    pub fn from_bootstrap(
        config: NodeConfig,
        bootstrap: BootstrapState,
    ) -> Result<Self, BootstrapValidationError> {
        let parts = bootstrap.into_parts(&config)?;
        let applied_index = parts.snapshot.as_ref().map_or(LogIndex::ZERO, |snapshot| {
            snapshot.metadata.last_included_index
        });
        let persistent = PersistentState {
            current_term: parts.current_term,
            voted_for: parts.voted_for,
            committed_configuration: parts.committed_configuration,
            snapshot: parts.snapshot,
            log: parts.log,
        };

        let configuration_offsets = configuration_offsets_of(&persistent.log);
        let mut volatile = VolatileState::at_applied_index(applied_index);
        volatile.commit_index = parts.commit_index;
        Ok(Self {
            config,
            persistent,
            volatile,
            leader: LeaderState::default(),
            election_elapsed: 0,
            granted_votes: BTreeSet::new(),
            granted_pre_votes: BTreeSet::new(),
            snapshot_chunk_rejections: SnapshotChunkRejectionCounters::default(),
            configuration_offsets,
        })
    }

    /// Like [`Node::from_bootstrap`], but the application declares it has
    /// already durably applied entries through `applied_through`: committed
    /// entries at or below the floor are not re-emitted as
    /// [`Output::Apply`] after this restart. Call
    /// [`Node::drain_committed_outputs`] after construction to replay
    /// committed entries above the floor immediately, without waiting for a
    /// later commit-index advance. Use this when the state machine persists
    /// its own state; without a floor, every committed entry above the
    /// snapshot boundary replays and the application must deduplicate.
    ///
    /// # Errors
    ///
    /// Returns [`BootstrapValidationError`] as [`Node::from_bootstrap`]
    /// does, or [`BootstrapValidationError::AppliedFloorBeyondLog`] when the
    /// floor lies beyond the persisted log.
    pub fn from_bootstrap_applied_through(
        config: NodeConfig,
        bootstrap: BootstrapState,
        applied_through: LogIndex,
    ) -> Result<Self, BootstrapValidationError> {
        let mut node = Self::from_bootstrap(config, bootstrap)?;
        if applied_through > node.last_log_index() {
            return Err(BootstrapValidationError::AppliedFloorBeyondLog {
                applied_through,
                last_log_index: node.last_log_index(),
            });
        }
        if applied_through > node.commit_index() {
            return Err(BootstrapValidationError::AppliedFloorBeyondCommit {
                applied_through,
                commit_index: node.commit_index(),
            });
        }
        let floor = node.volatile.applied_index.max(applied_through);
        node.volatile.applied_index = floor;
        Ok(node)
    }

    /// Returns this node's id.
    #[must_use]
    pub fn id(&self) -> NodeId {
        self.config.id()
    }

    /// Returns this node's current role.
    #[must_use]
    pub fn role(&self) -> Role {
        self.volatile.role
    }

    /// Returns this node's current term.
    #[must_use]
    pub fn current_term(&self) -> Term {
        self.persistent.current_term
    }

    /// Returns the candidate this node voted for in the current term.
    #[must_use]
    pub fn voted_for(&self) -> Option<NodeId> {
        self.persistent.voted_for
    }

    /// Returns the node this replica believes is the current leader, based on
    /// the most recently accepted leader traffic. Purely volatile.
    #[must_use]
    pub fn leader_hint(&self) -> Option<NodeId> {
        self.volatile.leader_hint
    }

    /// Returns this node's committed index.
    #[must_use]
    pub fn commit_index(&self) -> LogIndex {
        self.volatile.commit_index
    }

    /// Returns this node's applied index.
    #[must_use]
    pub fn applied_index(&self) -> LogIndex {
        self.volatile.applied_index
    }

    /// Returns this node's last local log index.
    #[must_use]
    pub fn last_log_index(&self) -> LogIndex {
        LogIndex(self.snapshot_index().0 + self.persistent.log.len() as u64)
    }

    /// Returns leader-side replication progress for every effective replica.
    #[must_use]
    pub fn leader_replication_progress(&self) -> Vec<ReplicationProgress> {
        if self.role() != Role::Leader {
            return Vec::new();
        }
        self.leader
            .progress
            .iter_followers()
            .map(|(follower_id, progress)| {
                let state = match progress.mode {
                    state::ProgressMode::Probe { .. } => ReplicationState::Probing,
                    state::ProgressMode::Replicate => ReplicationState::Replicating,
                    state::ProgressMode::Snapshot { next_offset } => {
                        ReplicationState::Snapshotting { next_offset }
                    }
                };
                ReplicationProgress {
                    follower_id,
                    match_index: progress.match_index,
                    next_index: progress.next_index,
                    state,
                }
            })
            .collect()
    }

    /// Returns the currently effective membership.
    #[must_use]
    pub fn effective_membership(&self) -> MembershipConfig {
        self.effective_configuration_entry().map_or_else(
            || self.effective_base_membership_ref().clone(),
            |entry| entry.membership_config(),
        )
    }

    pub(super) fn effective_base_membership_ref(&self) -> &MembershipConfig {
        self.persistent
            .snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.metadata.committed_membership())
            .unwrap_or_else(|| self.config.static_membership_ref())
    }

    /// Membership committed at `index`, as recoverable from the retained
    /// log, the previous snapshot's compacted state, or finally the static
    /// bootstrap configuration.
    ///
    /// If an internal configuration-offset entry is stale, it is ignored.
    /// Normal log mutation keeps the index exact, so this only protects
    /// defensive callers from propagating a panic.
    #[must_use]
    pub fn membership_at_index(&self, index: LogIndex) -> MembershipConfig {
        let first_log_index = self.snapshot_index().next();
        self.configuration_offsets
            .iter()
            .rev()
            .find_map(|offset| {
                let entry_index = LogIndex(first_log_index.0 + *offset as u64);
                if entry_index <= index {
                    self.configuration_entry_at_offset(*offset)
                        .map(ConfigurationEntry::membership_config)
                } else {
                    None
                }
            })
            .or_else(|| {
                let snapshot_index = self.snapshot_index();
                (snapshot_index <= index)
                    .then(|| self.snapshot_committed_membership())
                    .flatten()
            })
            .unwrap_or_else(|| self.config.static_membership())
    }

    /// Returns `None` if the internal configuration-offset index is stale.
    #[must_use]
    pub fn effective_configuration_entry(&self) -> Option<ConfigurationEntry> {
        self.configuration_offsets
            .last()
            .and_then(|offset| self.configuration_entry_at_offset(*offset))
            .cloned()
    }

    /// Returns the committed membership.
    #[must_use]
    pub fn committed_membership(&self) -> MembershipConfig {
        self.committed_configuration_entry()
            .map(|entry| entry.membership_config())
            .or_else(|| self.snapshot_committed_membership())
            .unwrap_or_else(|| self.config.static_membership())
    }

    /// Returns committed membership captured in the installed snapshot.
    #[must_use]
    pub fn snapshot_committed_membership(&self) -> Option<MembershipConfig> {
        self.persistent
            .snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.metadata.committed_membership().cloned())
    }

    /// Returns committed configuration identity captured in the installed
    /// snapshot.
    #[must_use]
    pub fn snapshot_committed_configuration_state(&self) -> Option<CommittedConfiguration> {
        self.persistent
            .snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.metadata.committed_configuration_state())
    }

    /// Returns `None` if the internal configuration-offset index is stale or
    /// no configuration entry is committed in the retained log.
    #[must_use]
    pub fn committed_configuration_entry(&self) -> Option<ConfigurationEntry> {
        let first_log_index = self.snapshot_index().next();
        self.configuration_offsets
            .iter()
            .rev()
            .find(|offset| {
                LogIndex(first_log_index.0 + **offset as u64) <= self.volatile.commit_index
            })
            .and_then(|offset| self.configuration_entry_at_offset(*offset).cloned())
    }

    /// Returns the committed configuration identity at the current commit
    /// index, if known.
    #[must_use]
    pub fn committed_configuration_state(&self) -> Option<CommittedConfiguration> {
        self.committed_configuration_state_at(self.volatile.commit_index)
    }

    /// Returns whether `node_id` is an effective voter.
    #[must_use]
    pub fn is_effective_voter(&self, node_id: NodeId) -> bool {
        self.effective_membership().contains_voter(node_id)
    }

    /// Returns whether `node_id` is an effective learner.
    #[must_use]
    pub fn is_effective_learner(&self, node_id: NodeId) -> bool {
        self.effective_membership().contains_learner(node_id)
    }

    /// Returns the current learner promotion barrier, if one can be issued.
    #[must_use]
    pub fn promotion_barrier(&self, learner_id: NodeId) -> Option<PromotionBarrier> {
        (self.role() == Role::Leader && self.is_effective_learner(learner_id))
            .then(|| PromotionBarrier::new(learner_id, self.commit_index()))
    }

    /// Whether a read barrier requested right now would grant from the
    /// leader lease without a quorum round trip.
    #[must_use]
    pub fn read_lease_active(&self) -> bool {
        self.role() == Role::Leader
            && self.config.lease_reads()
            && self
                .leader
                .lease
                .holds(self.leader.ticks, self.config.read_lease_ticks())
    }

    /// Returns snapshot transfer observability for this node.
    #[must_use]
    pub fn snapshot_transfer_status(&self) -> SnapshotTransferStatus {
        let leader = self
            .persistent
            .snapshot
            .as_ref()
            .map(|snapshot| {
                let total_bytes = snapshot.application_payload_len;
                self.leader
                    .progress
                    .iter_followers()
                    .filter_map(|(follower_id, progress)| {
                        let next_offset = match progress.mode {
                            state::ProgressMode::Snapshot { next_offset } => next_offset,
                            _ if progress.next_index <= snapshot.metadata.last_included_index => 0,
                            _ => return None,
                        };
                        Some(LeaderSnapshotTransferStatus {
                            follower_id,
                            transfer_id: snapshot.transfer_id(),
                            last_included_index: snapshot.metadata.last_included_index,
                            total_bytes,
                            next_offset: next_offset.min(total_bytes),
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();

        let follower = self.volatile.incoming_snapshot.as_ref().map(|transfer| {
            FollowerSnapshotTransferStatus {
                leader_id: transfer.leader_id,
                transfer_id: transfer.transfer_id,
                last_included_index: transfer.metadata.last_included_index,
                total_bytes: transfer.total_payload_len,
                received_bytes: transfer.next_offset(),
            }
        });

        SnapshotTransferStatus {
            leader,
            follower,
            rejected_chunks: self.snapshot_chunk_rejections,
        }
    }

    /// Applies one input event and returns ordered side effects.
    #[must_use]
    pub fn step(&mut self, input: Input) -> Vec<Output> {
        match input {
            Input::Tick => self.tick(),
            Input::Message { from, message } => self.receive(from, message),
            Input::ClientProposal { payload } => self.step_client_proposal(ClientProposalInput {
                proposal_id: None,
                payload,
            }),
            Input::TrackedClientProposal {
                proposal_id,
                payload,
            } => self.step_client_proposal(ClientProposalInput {
                proposal_id: Some(proposal_id),
                payload,
            }),
            Input::AddLearner { learner_id } => self.add_learner(learner_id),
            Input::PromoteLearner {
                learner_id,
                promotion_barrier,
            } => self.promote_learner(learner_id, promotion_barrier),
            Input::RemoveVoter { voter_id } => self.remove_voter(voter_id),
            Input::EnterJoint {
                target,
                promotion_barriers,
            } => self.enter_joint(target, &promotion_barriers),
            Input::LeaveJoint => self.leave_joint(),
            Input::ChangeMembership {
                target,
                promotion_barriers,
            } => self.change_membership(target, &promotion_barriers),
            Input::DangerousRawConfigurationProposal {
                configuration,
                promotion_barriers,
            } => self.dangerous_raw_configuration_proposal(configuration, &promotion_barriers),
            Input::TransferLeadership { target } => self.transfer_leadership(target),
            Input::ReadIndex { read_id } => self.read_index(read_id),
        }
    }

    /// Applies several input events while coalescing adjacent client
    /// proposals into one deterministic proposal batch and adjacent read
    /// barriers into one deterministic confirmation round.
    ///
    /// Messages, membership changes, ticks, and leadership transfer requests
    /// retain their original one-step semantics. They also form batch
    /// boundaries because they can change the term, role, quorum, heartbeat
    /// sequencing, or output ordering obligations that deterministic batching
    /// must not cross.
    #[must_use]
    pub fn step_batch(&mut self, inputs: Vec<Input>) -> Vec<Output> {
        let mut inputs = inputs.into_iter();
        let Some(first) = inputs.next() else {
            return Vec::new();
        };
        let Some(second) = inputs.next() else {
            return self.step(first);
        };

        let mut outputs = Vec::new();
        let mut proposals = Vec::new();
        let mut reads = Vec::new();

        for input in std::iter::once(first)
            .chain(std::iter::once(second))
            .chain(inputs)
        {
            match input {
                Input::ClientProposal { payload } => {
                    if !reads.is_empty() {
                        outputs.extend(self.read_index_batch(std::mem::take(&mut reads)));
                    }
                    proposals.push(ClientProposalInput {
                        proposal_id: None,
                        payload,
                    });
                }
                Input::TrackedClientProposal {
                    proposal_id,
                    payload,
                } => {
                    if !reads.is_empty() {
                        outputs.extend(self.read_index_batch(std::mem::take(&mut reads)));
                    }
                    proposals.push(ClientProposalInput {
                        proposal_id: Some(proposal_id),
                        payload,
                    });
                }
                Input::ReadIndex { read_id } => {
                    if !proposals.is_empty() {
                        outputs.extend(self.step_proposal_batch(std::mem::take(&mut proposals)));
                    }
                    reads.push(read_id);
                }
                input => {
                    if !proposals.is_empty() {
                        outputs.extend(self.step_proposal_batch(std::mem::take(&mut proposals)));
                    }
                    if !reads.is_empty() {
                        outputs.extend(self.read_index_batch(std::mem::take(&mut reads)));
                    }
                    outputs.extend(self.step(input));
                }
            }
        }

        if !proposals.is_empty() {
            outputs.extend(self.step_proposal_batch(proposals));
        }
        if !reads.is_empty() {
            outputs.extend(self.read_index_batch(reads));
        }

        outputs
    }

    fn receive(&mut self, from: NodeId, message: Message) -> Vec<Output> {
        // Membership does not gate message processing wholesale: servers
        // outside the receiver's configuration may still carry relevant
        // terms, log probes, or snapshot state during membership changes.
        // Vote handlers apply candidate-voter fencing before granting.
        if !message_sender_matches(from, &message) {
            return Vec::new();
        }

        match message {
            Message::AppendEntries(request) => self.handle_append_entries(from, &request),
            Message::AppendEntriesResponse(response) => {
                self.handle_append_entries_response(from, response)
            }
            Message::InstallSnapshot(request) => self.handle_install_snapshot(from, request),
            Message::InstallSnapshotChunk(request) => {
                self.handle_install_snapshot_chunk(from, request)
            }
            Message::InstallSnapshotResponse(response) => {
                self.handle_install_snapshot_response(from, response)
            }
            Message::PreVote(request) => self.handle_pre_vote(from, request),
            Message::PreVoteResponse(response) => self.handle_pre_vote_response(from, response),
            Message::TimeoutNow(request) => self.handle_timeout_now(request.term),
            Message::RequestVote(request) => self.handle_request_vote(from, request),
            Message::RequestVoteResponse(response) => {
                self.handle_request_vote_response(from, response)
            }
        }
    }

    fn has_effective_quorum<I>(&self, acknowledgements: I) -> bool
    where
        I: IntoIterator<Item = NodeId>,
    {
        if self.configuration_offsets.is_empty() {
            return MembershipIndex::new(self.effective_base_membership_ref(), self.id())
                .has_quorum(acknowledgements);
        }
        MembershipIndex::new(&self.effective_membership(), self.id()).has_quorum(acknowledgements)
    }

    fn uncommitted_configuration_indexes(&self) -> Vec<LogIndex> {
        let first_log_index = self.snapshot_index().next();
        self.configuration_offsets
            .iter()
            .map(|offset| LogIndex(first_log_index.0 + *offset as u64))
            .filter(|index| *index > self.volatile.commit_index)
            .collect()
    }

    /// Returns `None` if the internal configuration-offset index is stale or
    /// no configuration entry is committed at or below `commit_index`.
    #[must_use]
    pub fn committed_configuration_state_at(
        &self,
        commit_index: LogIndex,
    ) -> Option<CommittedConfiguration> {
        let first_log_index = self.snapshot_index().next();
        self.configuration_offsets
            .iter()
            .rev()
            .find_map(|offset| {
                let index = LogIndex(first_log_index.0 + *offset as u64);
                if index <= commit_index {
                    self.configuration_entry_at_offset(*offset).map(|entry| {
                        CommittedConfiguration {
                            index,
                            config_id: entry.config_id(),
                        }
                    })
                } else {
                    None
                }
            })
            .or_else(|| {
                self.persistent
                    .committed_configuration
                    .filter(|state| state.index <= commit_index)
            })
            .or_else(|| {
                self.snapshot_committed_configuration_state()
                    .filter(|state| state.index <= commit_index)
            })
    }

    fn configuration_entry_at_offset(&self, offset: usize) -> Option<&ConfigurationEntry> {
        self.persistent
            .log
            .get(offset)
            .and_then(LogEntry::configuration_entry)
    }

    #[cfg(any(test, feature = "internal-test-hooks"))]
    #[doc(hidden)]
    pub fn validate_derived_state(&self) -> Result<(), String> {
        let expected = configuration_offsets_of(&self.persistent.log);
        if self.configuration_offsets == expected {
            return Ok(());
        }

        Err(format!(
            "configuration_offsets mismatch: stored {:?}, expected {:?}",
            self.configuration_offsets, expected
        ))
    }
}

impl Node {
    /// Appends one entry, keeping the configuration-offset index exact.
    pub(super) fn append_log_entry(&mut self, entry: crate::LogEntry) {
        if entry.kind.is_configuration() {
            self.configuration_offsets.push(self.persistent.log.len());
        }
        self.persistent.log.push(entry);
    }

    /// Replaces the whole log (bootstrap restores, splice rollbacks,
    /// snapshot installs) and rebuilds the offset index from it.
    pub(super) fn replace_log(
        &mut self,
        log: Vec<crate::LogEntry>,
        reason: LocalProposalDropReason,
    ) -> Vec<Output> {
        self.configuration_offsets = configuration_offsets_of(&log);
        self.persistent.log = log;
        let mut retained = LocalProposalTracker::default();
        let mut outputs = Vec::new();
        let snapshot_index = self.snapshot_index();
        for (index, proposal) in std::mem::take(&mut self.volatile.local_proposals) {
            let covered_by_snapshot =
                reason == LocalProposalDropReason::SnapshotCovered && index <= snapshot_index;
            if !covered_by_snapshot && self.term_at(index) == Some(proposal.term) {
                retained.insert(index, proposal);
            } else {
                outputs.push(Output::LocalProposalDropped {
                    proposal_id: proposal.id,
                    index,
                    term: proposal.term,
                    reason,
                });
            }
        }
        self.volatile.local_proposals = retained;
        outputs
    }
}

fn configuration_offsets_of(log: &[crate::LogEntry]) -> Vec<usize> {
    log.iter()
        .enumerate()
        .filter_map(|(offset, entry)| entry.kind.is_configuration().then_some(offset))
        .collect()
}

fn message_sender_matches(from: NodeId, message: &Message) -> bool {
    match message {
        Message::AppendEntries(request) => request.leader_id == from,
        Message::AppendEntriesResponse(response) => response.follower_id == from,
        Message::InstallSnapshot(request) => request.leader_id == from,
        Message::InstallSnapshotChunk(request) => request.leader_id == from,
        Message::InstallSnapshotResponse(response) => response.follower_id == from,
        Message::PreVote(request) => request.candidate_id == from,
        Message::PreVoteResponse(response) => response.voter_id == from,
        Message::TimeoutNow(request) => request.leader_id == from,
        Message::RequestVote(request) => request.candidate_id == from,
        Message::RequestVoteResponse(response) => response.voter_id == from,
    }
}
