//! Shared drivers and fixtures for the many-group host's regression suites.
//!
//! Every driver here is deliberately misbehaved in exactly one way. A host is
//! a scheduler over things it does not control, so its guards are only worth
//! what its worst driver proves.

use std::{cell::Cell, rc::Rc};

use rafter::{
    LocalProposalId, LogIndex, MembershipConfig, MembershipSet, Message, NodeId, RequestVote, Role,
    Term,
};
use rafter_app::{
    group::{GroupFatalState, GroupInput, GroupStepReport},
    metrics::RaftGroupMetrics,
    state_machine::ApplyResult,
    transport::PeerEnvelope,
};
use rafter_multiraft::GroupDriver;

/// A shared step counter, so a test can prove which groups a pass reached.
pub type StepCounter = Rc<Cell<usize>>;

/// Applies one entry per step and reports it, like a group whose tick
/// advanced the commit index.
#[derive(Debug)]
pub struct ApplyingDriver {
    group_id: u64,
    applied: u64,
    steps: StepCounter,
}

impl ApplyingDriver {
    #[must_use]
    pub fn new(group_id: u64) -> Self {
        Self::with_counter(group_id, StepCounter::default())
    }

    #[must_use]
    pub fn with_counter(group_id: u64, steps: StepCounter) -> Self {
        Self {
            group_id,
            applied: 0,
            steps,
        }
    }
}

impl GroupDriver<u64> for ApplyingDriver {
    fn step(
        &mut self,
        _input: GroupInput<u64, Vec<u8>>,
    ) -> Result<GroupStepReport<u64, Vec<u8>>, String> {
        self.steps.set(self.steps.get() + 1);
        self.applied += 1;
        let mut report = report(self.group_id);
        report.applied.push(ApplyResult {
            index: LogIndex(self.applied),
            term: Term(1),
            result: b"applied".to_vec(),
            local_proposal_id: Some(LocalProposalId(self.applied)),
        });
        Ok(report)
    }

    fn metrics(&self) -> RaftGroupMetrics<u64> {
        metrics(self.group_id, self.applied)
    }
}

/// Refuses every step, like a group `rafter-app` has poisoned.
#[derive(Debug)]
pub struct FailingDriver {
    group_id: u64,
    message: &'static str,
    steps: StepCounter,
}

impl FailingDriver {
    #[must_use]
    pub fn new(group_id: u64, message: &'static str) -> Self {
        Self::with_counter(group_id, message, StepCounter::default())
    }

    #[must_use]
    pub fn with_counter(group_id: u64, message: &'static str, steps: StepCounter) -> Self {
        Self {
            group_id,
            message,
            steps,
        }
    }
}

impl GroupDriver<u64> for FailingDriver {
    fn step(
        &mut self,
        _input: GroupInput<u64, Vec<u8>>,
    ) -> Result<GroupStepReport<u64, Vec<u8>>, String> {
        self.steps.set(self.steps.get() + 1);
        Err(self.message.to_owned())
    }

    fn metrics(&self) -> RaftGroupMetrics<u64> {
        metrics(self.group_id, 0)
    }
}

/// An empty report with no effects, for a group whose step did nothing.
#[must_use]
pub fn report(group_id: u64) -> GroupStepReport<u64, Vec<u8>> {
    GroupStepReport {
        group_id,
        peer_messages: Vec::new(),
        applied: Vec::new(),
        proposal_events: Vec::new(),
        read_events: Vec::new(),
        leadership_transfer_events: Vec::new(),
        snapshot_events: Vec::new(),
        membership_events: Vec::new(),
        metrics: None,
    }
}

/// A peer envelope stamped with `group_id`.
#[must_use]
pub fn envelope(group_id: u64) -> PeerEnvelope<u64> {
    PeerEnvelope {
        group_id,
        from: NodeId(2),
        to: NodeId(1),
        message: Message::RequestVote(RequestVote {
            term: Term(3),
            candidate_id: NodeId(2),
            last_log_index: LogIndex(9),
            last_log_term: Term(2),
        }),
    }
}

/// A metrics snapshot claiming `group_id` and `applied` applied entries.
#[must_use]
pub fn metrics(group_id: u64, applied: u64) -> RaftGroupMetrics<u64> {
    RaftGroupMetrics {
        group_id,
        node_id: NodeId(1),
        role: Role::Follower,
        term: Term(1),
        leader_hint: None,
        commit_index: LogIndex(applied),
        applied_index: LogIndex(applied),
        last_log_index: LogIndex(applied),
        snapshot_index: LogIndex::ZERO,
        membership: MembershipConfig::Stable(
            MembershipSet::new(vec![NodeId(1)], Vec::new()).expect("valid membership"),
        ),
        replication: Vec::new(),
        pending_proposals: 0,
        pending_reads: 0,
        pending_read_barriers: 0,
        pending_query_reads: 0,
        completed_query_reads: 0,
        reserved_reads: 0,
        fatal_state: GroupFatalState::Healthy,
    }
}
