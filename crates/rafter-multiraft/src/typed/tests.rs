use std::cell::Cell;

use rafter::{LogIndex, MembershipConfig, MembershipSet, NodeId, Role, Term};
use rafter_app::{
    group::{GroupFatalState, GroupInput, GroupStepReport},
    membership::MembershipEvent,
    metrics::RaftGroupMetrics,
    proposal::Proposal,
    state_machine::ApplyResult,
    transport::PeerEnvelope,
};

use super::*;

#[derive(Clone, Debug, Eq, PartialEq)]
enum TestCommand {
    Put(u64),
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum TestResult {
    Stored(u64),
}

#[derive(Debug)]
struct RecordingTypedDriver {
    group_id: u64,
    applied_count: u64,
}

impl RecordingTypedDriver {
    fn new(group_id: u64) -> Self {
        Self {
            group_id,
            applied_count: 0,
        }
    }
}

impl TypedGroupDriver<u64> for RecordingTypedDriver {
    type Command = TestCommand;
    type CommandResult = TestResult;

    fn step(
        &mut self,
        input: GroupInput<u64, Self::Command>,
    ) -> Result<GroupStepReport<u64, Self::CommandResult>, String> {
        match input {
            GroupInput::Tick
            | GroupInput::PeerMessage { .. }
            | GroupInput::ReadBarrier { .. }
            | GroupInput::TransferLeadership { .. }
            | GroupInput::Membership { .. } => Ok(report(self.group_id)),
            GroupInput::Proposal { proposal } => {
                self.applied_count += 1;
                let TestCommand::Put(value) = proposal.command;
                let apply_result = ApplyResult {
                    index: LogIndex(self.applied_count),
                    term: Term(1),
                    result: TestResult::Stored(value),
                    local_proposal_id: Some(proposal.local_proposal_id),
                };
                let mut report = report(self.group_id);
                report.applied.push(apply_result);
                Ok(report)
            }
            GroupInput::ProposalBatch { proposals } => {
                let mut report = report(self.group_id);
                for proposal in proposals {
                    self.applied_count += 1;
                    let TestCommand::Put(value) = proposal.command;
                    report.applied.push(ApplyResult {
                        index: LogIndex(self.applied_count),
                        term: Term(1),
                        result: TestResult::Stored(value),
                        local_proposal_id: Some(proposal.local_proposal_id),
                    });
                }
                Ok(report)
            }
        }
    }

    fn metrics(&self) -> RaftGroupMetrics<u64> {
        metrics(self.group_id, self.applied_count)
    }
}

#[derive(Debug)]
struct FixedTypedReportDriver {
    metrics_group_id: u64,
    report: GroupStepReport<u64, TestResult>,
}

impl FixedTypedReportDriver {
    fn new(metrics_group_id: u64, report: GroupStepReport<u64, TestResult>) -> Self {
        Self {
            metrics_group_id,
            report,
        }
    }
}

impl TypedGroupDriver<u64> for FixedTypedReportDriver {
    type Command = TestCommand;
    type CommandResult = TestResult;

    fn step(
        &mut self,
        _input: GroupInput<u64, Self::Command>,
    ) -> Result<GroupStepReport<u64, Self::CommandResult>, String> {
        Ok(self.report.clone())
    }

    fn metrics(&self) -> RaftGroupMetrics<u64> {
        metrics(self.metrics_group_id, 0)
    }
}

#[derive(Debug)]
struct FlippingTypedMetricsDriver {
    first_group_id: u64,
    later_group_id: u64,
    calls: Cell<usize>,
}

impl FlippingTypedMetricsDriver {
    fn new(first_group_id: u64, later_group_id: u64) -> Self {
        Self {
            first_group_id,
            later_group_id,
            calls: Cell::new(0),
        }
    }
}

impl TypedGroupDriver<u64> for FlippingTypedMetricsDriver {
    type Command = TestCommand;
    type CommandResult = TestResult;

    fn step(
        &mut self,
        _input: GroupInput<u64, Self::Command>,
    ) -> Result<GroupStepReport<u64, Self::CommandResult>, String> {
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
        metrics(group_id, 0)
    }
}

#[test]
fn typed_groups_run_independently() {
    let mut host = TypedMultiRaftHost::<u64, TestCommand, TestResult>::new();
    host.open_group(1, RecordingTypedDriver::new(1))
        .expect("open group 1");
    host.open_group(2, RecordingTypedDriver::new(2))
        .expect("open group 2");

    let reports = host.tick_all().expect("tick groups");
    assert_eq!(
        reports
            .iter()
            .map(|report| report.group_id)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );

    let report = host
        .step_group(
            &1,
            GroupInput::Proposal {
                proposal: Proposal {
                    local_proposal_id: rafter::LocalProposalId(7),
                    client_request_id: None,
                    command: TestCommand::Put(99),
                },
            },
        )
        .expect("typed proposal applies");
    assert_eq!(report.applied[0].result, TestResult::Stored(99));

    let report = host
        .step_group(
            &1,
            GroupInput::ProposalBatch {
                proposals: vec![
                    Proposal {
                        local_proposal_id: rafter::LocalProposalId(8),
                        client_request_id: None,
                        command: TestCommand::Put(100),
                    },
                    Proposal {
                        local_proposal_id: rafter::LocalProposalId(9),
                        client_request_id: None,
                        command: TestCommand::Put(101),
                    },
                ],
            },
        )
        .expect("typed proposal batch applies");
    assert_eq!(
        report
            .applied
            .iter()
            .map(|result| result.result.clone())
            .collect::<Vec<_>>(),
        vec![TestResult::Stored(100), TestResult::Stored(101)]
    );

    let metrics = host.metrics().expect("metrics");
    assert_eq!(
        metrics
            .groups
            .iter()
            .map(|group| (group.group_id, group.applied_index))
            .collect::<Vec<_>>(),
        vec![(1, LogIndex(3)), (2, LogIndex::ZERO)]
    );
}

#[test]
fn typed_host_rejects_wrong_group_envelope() {
    let mut host = TypedMultiRaftHost::<u64, TestCommand, TestResult>::new();
    host.open_group(1, RecordingTypedDriver::new(1))
        .expect("open group 1");

    let error = host
        .step_group(
            &1,
            GroupInput::PeerMessage {
                envelope: PeerEnvelope {
                    group_id: 2,
                    from: NodeId(2),
                    to: NodeId(1),
                    message: rafter::Message::RequestVote(rafter::RequestVote {
                        term: Term(1),
                        candidate_id: NodeId(2),
                        last_log_index: LogIndex::ZERO,
                        last_log_term: Term::default(),
                    }),
                },
            },
        )
        .expect_err("wrong group rejected");

    assert_eq!(
        error,
        MultiRaftError::WrongGroup {
            expected: 1,
            actual: 2
        }
    );
}

#[test]
fn typed_open_group_rejects_driver_metrics_group_mismatch() {
    let mut host = TypedMultiRaftHost::<u64, TestCommand, TestResult>::new();

    let error = host
        .open_group(1, RecordingTypedDriver::new(2))
        .expect_err("driver group mismatch is rejected");

    assert_eq!(
        error,
        MultiRaftError::WrongGroup {
            expected: 1,
            actual: 2
        }
    );
}

#[test]
fn typed_metrics_rejects_driver_that_later_reports_another_group() {
    let mut host = TypedMultiRaftHost::<u64, TestCommand, TestResult>::new();
    host.open_group(1, FlippingTypedMetricsDriver::new(1, 2))
        .expect("initial metrics group matches");

    let error = host
        .metrics()
        .expect_err("changed metrics group is rejected");

    assert_eq!(
        error,
        MultiRaftError::WrongGroup {
            expected: 1,
            actual: 2
        }
    );
}

#[test]
fn typed_step_group_rejects_mismatched_report_group_id() {
    let mut host = TypedMultiRaftHost::<u64, TestCommand, TestResult>::new();
    host.open_group(1, FixedTypedReportDriver::new(1, report(2)))
        .expect("open group");

    let error = host
        .step_group(&1, GroupInput::Tick)
        .expect_err("mismatched report group is rejected");

    assert_eq!(
        error,
        MultiRaftError::WrongGroup {
            expected: 1,
            actual: 2
        }
    );
}

#[test]
fn typed_step_group_rejects_membership_events_for_another_group() {
    let mut bad_report = report(1);
    bad_report.membership_events.push(MembershipEvent::Applied {
        group_id: 2,
        index: LogIndex(5),
        term: Term(1),
        membership: MembershipConfig::stable(
            MembershipSet::new(vec![NodeId(1)], Vec::new()).expect("valid membership"),
        ),
    });
    let mut host = TypedMultiRaftHost::<u64, TestCommand, TestResult>::new();
    host.open_group(1, FixedTypedReportDriver::new(1, bad_report))
        .expect("open group");

    let error = host
        .step_group(&1, GroupInput::Tick)
        .expect_err("mismatched membership event is rejected");

    assert_eq!(
        error,
        MultiRaftError::WrongGroup {
            expected: 1,
            actual: 2
        }
    );
}

fn report(group_id: u64) -> GroupStepReport<u64, TestResult> {
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

fn metrics(group_id: u64, applied: u64) -> RaftGroupMetrics<u64> {
    RaftGroupMetrics {
        group_id,
        node_id: NodeId(1),
        role: Role::Leader,
        term: Term(1),
        leader_hint: Some(NodeId(1)),
        commit_index: LogIndex(applied),
        applied_index: LogIndex(applied),
        last_log_index: LogIndex(applied),
        snapshot_index: LogIndex::ZERO,
        membership: MembershipConfig::stable(
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
