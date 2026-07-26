use std::cell::Cell;

use rafter::{
    LocalProposalId, LogIndex, MembershipConfig, MembershipSet, Message, NodeId, ReadId,
    RequestVote, Role, Term,
};
use rafter_app::{
    group::{GroupFatalState, GroupInput, GroupStepReport},
    metrics::RaftGroupMetrics,
    proposal::Proposal,
    read::{ReadEvent, ReadProof},
    transport::PeerEnvelope,
};

use super::*;

#[derive(Debug)]
struct RecordingDriver {
    group_id: u64,
    steps: Vec<GroupInput<u64, Vec<u8>>>,
    applied_count: u64,
}

impl RecordingDriver {
    fn new(group_id: u64) -> Self {
        Self {
            group_id,
            steps: Vec::new(),
            applied_count: 0,
        }
    }
}

impl GroupDriver<u64> for RecordingDriver {
    fn step(
        &mut self,
        input: GroupInput<u64, Vec<u8>>,
    ) -> Result<GroupStepReport<u64, Vec<u8>>, String> {
        match &input {
            GroupInput::Proposal { .. } => {
                self.applied_count += 1;
            }
            GroupInput::ProposalBatch { proposals } => {
                self.applied_count += proposals.len() as u64;
            }
            GroupInput::Tick
            | GroupInput::PeerMessage { .. }
            | GroupInput::ReadBarrier { .. }
            | GroupInput::TransferLeadership { .. }
            | GroupInput::Membership { .. } => {}
        }
        self.steps.push(input);
        Ok(report(self.group_id))
    }

    fn metrics(&self) -> RaftGroupMetrics<u64> {
        metrics(self.group_id, self.steps.len() as u64, self.applied_count)
    }
}

#[derive(Debug)]
struct FixedReportDriver {
    metrics_group_id: u64,
    report: GroupStepReport<u64, Vec<u8>>,
}

impl FixedReportDriver {
    fn new(metrics_group_id: u64, report: GroupStepReport<u64, Vec<u8>>) -> Self {
        Self {
            metrics_group_id,
            report,
        }
    }
}

impl GroupDriver<u64> for FixedReportDriver {
    fn step(
        &mut self,
        _input: GroupInput<u64, Vec<u8>>,
    ) -> Result<GroupStepReport<u64, Vec<u8>>, String> {
        Ok(self.report.clone())
    }

    fn metrics(&self) -> RaftGroupMetrics<u64> {
        metrics(self.metrics_group_id, 0, 0)
    }
}

#[derive(Debug)]
struct FlippingMetricsDriver {
    first_group_id: u64,
    later_group_id: u64,
    calls: Cell<usize>,
}

impl FlippingMetricsDriver {
    fn new(first_group_id: u64, later_group_id: u64) -> Self {
        Self {
            first_group_id,
            later_group_id,
            calls: Cell::new(0),
        }
    }
}

impl GroupDriver<u64> for FlippingMetricsDriver {
    fn step(
        &mut self,
        _input: GroupInput<u64, Vec<u8>>,
    ) -> Result<GroupStepReport<u64, Vec<u8>>, String> {
        Ok(report(self.first_group_id))
    }

    fn metrics(&self) -> RaftGroupMetrics<u64> {
        let calls = self.calls.get();
        self.calls.set(calls + 1);
        let group_id = if calls == 0 {
            self.first_group_id
        } else {
            self.later_group_id
        };
        metrics(group_id, 0, 0)
    }
}

#[test]
fn two_groups_tick_independently() {
    let mut host = MultiRaftHost::new();
    host.open_group(1, RecordingDriver::new(1))
        .expect("open group 1");
    host.open_group(2, RecordingDriver::new(2))
        .expect("open group 2");

    let pass = host.tick_all();

    assert!(pass.is_complete(), "both groups stepped");
    assert_eq!(pass.visited(), host.len(), "the pass visited every group");
    assert_eq!(
        pass.reports()
            .map(|report| report.group_id)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    let metrics = host.metrics().expect("metrics");
    assert_eq!(
        metrics
            .groups
            .iter()
            .map(|metrics| (metrics.group_id, metrics.commit_index))
            .collect::<Vec<_>>(),
        vec![(1, LogIndex(1)), (2, LogIndex(1))]
    );
}

#[test]
fn messages_route_by_group_id() {
    let mut host = MultiRaftHost::new();
    host.open_group(1, RecordingDriver::new(1))
        .expect("open group");

    let report = host
        .step_group(
            &1,
            GroupInput::PeerMessage {
                envelope: envelope(1),
            },
        )
        .expect("message routes");

    assert_eq!(report.group_id, 1);
}

#[test]
fn wrong_group_message_is_rejected_before_driver_step() {
    let mut host = MultiRaftHost::new();
    host.open_group(1, RecordingDriver::new(1))
        .expect("open group");

    let error = host
        .step_group(
            &1,
            GroupInput::PeerMessage {
                envelope: envelope(2),
            },
        )
        .expect_err("wrong group is rejected");

    assert_eq!(
        error,
        MultiRaftError::WrongGroup {
            expected: 1,
            actual: 2,
        }
    );
    assert_eq!(
        host.metrics().expect("metrics").groups[0].commit_index,
        LogIndex::ZERO
    );
}

#[test]
fn unknown_group_is_rejected() {
    let mut host = MultiRaftHost::<u64>::new();

    let error = host
        .step_group(&99, GroupInput::Tick)
        .expect_err("unknown group is rejected");

    assert_eq!(error, MultiRaftError::UnknownGroup { group_id: 99 });
}

#[test]
fn writes_in_one_group_do_not_affect_another() {
    let mut host = MultiRaftHost::new();
    host.open_group(1, RecordingDriver::new(1))
        .expect("open group 1");
    host.open_group(2, RecordingDriver::new(2))
        .expect("open group 2");

    host.step_group(
        &1,
        GroupInput::Proposal {
            proposal: Proposal {
                local_proposal_id: LocalProposalId(10),
                client_request_id: None,
                command: b"put".to_vec(),
            },
        },
    )
    .expect("proposal routes");

    host.step_group(
        &1,
        GroupInput::ProposalBatch {
            proposals: vec![
                Proposal {
                    local_proposal_id: LocalProposalId(11),
                    client_request_id: None,
                    command: b"batch-1".to_vec(),
                },
                Proposal {
                    local_proposal_id: LocalProposalId(12),
                    client_request_id: None,
                    command: b"batch-2".to_vec(),
                },
            ],
        },
    )
    .expect("proposal batch routes");

    let metrics = host.metrics().expect("metrics");
    assert_eq!(
        metrics
            .groups
            .iter()
            .map(|metrics| (metrics.group_id, metrics.applied_index))
            .collect::<Vec<_>>(),
        vec![(1, LogIndex(3)), (2, LogIndex::ZERO)]
    );
}

#[test]
fn metrics_expose_all_groups() {
    let mut host = MultiRaftHost::new();
    host.open_group(2, RecordingDriver::new(2))
        .expect("open group 2");
    host.open_group(1, RecordingDriver::new(1))
        .expect("open group 1");

    let metrics = host.metrics().expect("metrics");

    assert_eq!(
        metrics
            .groups
            .iter()
            .map(|metrics| metrics.group_id)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
}

#[test]
fn duplicate_group_open_is_rejected() {
    let mut host = MultiRaftHost::new();
    host.open_group(1, RecordingDriver::new(1))
        .expect("open group");

    let error = host
        .open_group(1, RecordingDriver::new(1))
        .expect_err("duplicate is rejected");

    assert_eq!(error, MultiRaftError::GroupAlreadyOpen { group_id: 1 });
}

#[test]
fn open_group_rejects_driver_metrics_group_mismatch() {
    let mut host = MultiRaftHost::new();

    let error = host
        .open_group(1, RecordingDriver::new(2))
        .expect_err("driver group mismatch is rejected");

    assert_eq!(
        error,
        MultiRaftError::WrongGroup {
            expected: 1,
            actual: 2,
        }
    );
}

#[test]
fn metrics_rejects_driver_that_later_reports_another_group() {
    let mut host = MultiRaftHost::new();
    host.open_group(1, FlippingMetricsDriver::new(1, 2))
        .expect("initial metrics group matches");

    let error = host
        .metrics()
        .expect_err("changed metrics group is rejected");

    assert_eq!(
        error,
        MultiRaftError::WrongGroup {
            expected: 1,
            actual: 2,
        }
    );
}

#[test]
fn step_group_rejects_mismatched_report_group_id() {
    let mut host = MultiRaftHost::new();
    host.open_group(1, FixedReportDriver::new(1, report(2)))
        .expect("open group");

    let error = host
        .step_group(&1, GroupInput::Tick)
        .expect_err("mismatched report group is rejected");

    assert_eq!(
        error,
        MultiRaftError::WrongGroup {
            expected: 1,
            actual: 2,
        }
    );
}

#[test]
fn step_group_rejects_peer_messages_for_another_group() {
    let mut bad_report = report(1);
    bad_report.peer_messages.push(envelope(2));
    let mut host = MultiRaftHost::new();
    host.open_group(1, FixedReportDriver::new(1, bad_report))
        .expect("open group");

    let error = host
        .step_group(&1, GroupInput::Tick)
        .expect_err("mismatched peer message is rejected");

    assert_eq!(
        error,
        MultiRaftError::WrongGroup {
            expected: 1,
            actual: 2,
        }
    );
}

#[test]
fn step_group_rejects_read_events_for_another_group() {
    let mut bad_report = report(1);
    bad_report.read_events.push(ReadEvent::Granted {
        read_id: ReadId(9),
        proof: ReadProof {
            group_id: 2,
            issued_by: NodeId(1),
            term: Term(1),
            read_index: LogIndex(3),
            required_applied_index: LogIndex(3),
            local_applied_index: LogIndex(3),
        },
    });
    let mut host = MultiRaftHost::new();
    host.open_group(1, FixedReportDriver::new(1, bad_report))
        .expect("open group");

    let error = host
        .step_group(&1, GroupInput::Tick)
        .expect_err("mismatched read event is rejected");

    assert_eq!(
        error,
        MultiRaftError::WrongGroup {
            expected: 1,
            actual: 2,
        }
    );
}

fn report(group_id: u64) -> GroupStepReport<u64, Vec<u8>> {
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

fn envelope(group_id: u64) -> PeerEnvelope<u64> {
    PeerEnvelope {
        group_id,
        from: NodeId(2),
        to: NodeId(1),
        message: vote_from(NodeId(2)),
    }
}

fn vote_from(node_id: NodeId) -> Message {
    Message::RequestVote(RequestVote {
        term: Term(3),
        candidate_id: node_id,
        last_log_index: LogIndex(9),
        last_log_term: Term(2),
    })
}

fn metrics(group_id: u64, steps: u64, applied: u64) -> RaftGroupMetrics<u64> {
    RaftGroupMetrics {
        group_id,
        node_id: NodeId(1),
        role: Role::Follower,
        term: Term(1),
        leader_hint: None,
        commit_index: LogIndex(steps),
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
