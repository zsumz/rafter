//! Typed manual many-group KV routing over `TypedMultiRaftHost`.
//!
//! This example is intentionally small. It shows where typed command semantics
//! live for homogeneous multi-group applications while the host still routes by
//! explicit group ID and returns peer messages to the caller.
//! It does not solve production storage, authentication, transport, shard
//! placement, or application durability.
//!
//! Run with:
//!
//! ```text
//! cargo run -p rafter-multiraft --example typed_multi_group_kv
//! ```

use std::collections::BTreeMap;

use rafter::{LocalProposalId, LogIndex, Message, NodeId, RequestVote, Role, Term};
use rafter_app::{
    group::{GroupFatalState, GroupInput, GroupStepReport},
    metrics::RaftGroupMetrics,
    proposal::{Proposal, ProposalEvent},
    state_machine::ApplyResult,
    transport::PeerEnvelope,
};
use rafter_multiraft::{MultiRaftError, MultiRaftMetrics, TypedGroupDriver, TypedMultiRaftHost};

fn main() {
    let mut host = TypedMultiRaftHost::<ShardId, KvCommand, KvResult>::new();
    host.open_group(ShardId(1), TypedKvShardDriver::new(ShardId(1)))
        .expect("open shard 1");
    host.open_group(ShardId(2), TypedKvShardDriver::new(ShardId(2)))
        .expect("open shard 2");

    let report = host
        .step_group(
            &ShardId(1),
            GroupInput::Proposal {
                proposal: Proposal {
                    local_proposal_id: LocalProposalId(10),
                    client_request_id: None,
                    command: KvCommand::Put {
                        key: "alpha".to_owned(),
                        value: "one".to_owned(),
                    },
                },
            },
        )
        .expect("typed write routes to shard 1");

    assert_eq!(
        report.applied[0].result,
        KvResult::Stored {
            previous: None,
            current: "one".to_owned()
        }
    );
    let routed = route_peer_messages(&mut host, report).expect("peer messages route by group id");
    assert_eq!(routed.len(), 1);
    assert_eq!(routed[0].group_id, ShardId(1));

    let metrics = host.metrics().expect("host metrics");
    assert_group_indexes(
        &metrics,
        &[(ShardId(1), LogIndex(1)), (ShardId(2), LogIndex::ZERO)],
    );
    println!(
        "typed shard 1 applied {:?}, shard 2 applied {:?}",
        metrics.groups[0].applied_index, metrics.groups[1].applied_index
    );
}

fn route_peer_messages(
    host: &mut TypedMultiRaftHost<ShardId, KvCommand, KvResult>,
    report: GroupStepReport<ShardId, KvResult>,
) -> Result<Vec<GroupStepReport<ShardId, KvResult>>, MultiRaftError<ShardId>> {
    report
        .peer_messages
        .into_iter()
        .map(|envelope| {
            let group_id = envelope.group_id;
            host.step_group(&group_id, GroupInput::PeerMessage { envelope })
        })
        .collect()
}

fn assert_group_indexes(metrics: &MultiRaftMetrics<ShardId>, expected: &[(ShardId, LogIndex)]) {
    let actual = metrics
        .groups
        .iter()
        .map(|metrics| (metrics.group_id, metrics.applied_index))
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ShardId(u64);

#[derive(Clone, Debug, Eq, PartialEq)]
enum KvCommand {
    Put { key: String, value: String },
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum KvResult {
    Stored {
        previous: Option<String>,
        current: String,
    },
}

#[derive(Debug)]
struct TypedKvShardDriver {
    group_id: ShardId,
    values: BTreeMap<String, String>,
    applied_index: LogIndex,
}

impl TypedKvShardDriver {
    fn new(group_id: ShardId) -> Self {
        Self {
            group_id,
            values: BTreeMap::new(),
            applied_index: LogIndex::ZERO,
        }
    }
}

impl TypedGroupDriver<ShardId> for TypedKvShardDriver {
    type Command = KvCommand;
    type CommandResult = KvResult;

    fn step(
        &mut self,
        input: GroupInput<ShardId, Self::Command>,
    ) -> Result<GroupStepReport<ShardId, Self::CommandResult>, String> {
        match input {
            GroupInput::Proposal { proposal } => {
                let apply_result = self.apply_command(proposal);
                let mut report = self.report();
                let local_proposal_id = apply_result
                    .local_proposal_id
                    .expect("local proposals carry local ids");
                report.proposal_events.push(ProposalEvent::Applied {
                    local_proposal_id,
                    index: apply_result.index,
                    term: apply_result.term,
                    result: apply_result.result.clone(),
                });
                report.applied.push(apply_result);
                report.peer_messages.push(PeerEnvelope {
                    group_id: self.group_id,
                    from: NodeId(1),
                    to: NodeId(2),
                    message: vote_message(NodeId(1)),
                });
                Ok(report)
            }
            GroupInput::ProposalBatch { proposals } => {
                let mut report = self.report();
                for proposal in proposals {
                    let apply_result = self.apply_command(proposal);
                    let local_proposal_id = apply_result
                        .local_proposal_id
                        .expect("local proposals carry local ids");
                    report.proposal_events.push(ProposalEvent::Applied {
                        local_proposal_id,
                        index: apply_result.index,
                        term: apply_result.term,
                        result: apply_result.result.clone(),
                    });
                    report.applied.push(apply_result);
                }
                report.peer_messages.push(PeerEnvelope {
                    group_id: self.group_id,
                    from: NodeId(1),
                    to: NodeId(2),
                    message: vote_message(NodeId(1)),
                });
                Ok(report)
            }
            GroupInput::PeerMessage { envelope } => {
                if envelope.group_id != self.group_id {
                    return Err("wrong group".to_owned());
                }
                Ok(self.report())
            }
            GroupInput::Tick
            | GroupInput::ReadBarrier { .. }
            | GroupInput::TransferLeadership { .. }
            | GroupInput::Membership { .. } => Ok(self.report()),
        }
    }

    fn metrics(&self) -> RaftGroupMetrics<ShardId> {
        RaftGroupMetrics {
            group_id: self.group_id,
            node_id: NodeId(1),
            role: Role::Leader,
            term: Term(1),
            leader_hint: Some(NodeId(1)),
            commit_index: self.applied_index,
            applied_index: self.applied_index,
            last_log_index: self.applied_index,
            snapshot_index: LogIndex::ZERO,
            membership: rafter::MembershipConfig::stable(
                rafter::MembershipSet::new(vec![NodeId(1)], Vec::new()).expect("valid membership"),
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
}

impl TypedKvShardDriver {
    fn apply_command(&mut self, proposal: Proposal<KvCommand>) -> ApplyResult<KvResult> {
        let KvCommand::Put { key, value } = proposal.command;
        self.applied_index = LogIndex(self.applied_index.0 + 1);
        let previous = self.values.insert(key, value.clone());
        ApplyResult {
            index: self.applied_index,
            term: Term(1),
            result: KvResult::Stored {
                previous,
                current: value,
            },
            local_proposal_id: Some(proposal.local_proposal_id),
        }
    }

    fn report(&self) -> GroupStepReport<ShardId, KvResult> {
        GroupStepReport {
            group_id: self.group_id,
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
}

fn vote_message(candidate_id: NodeId) -> Message {
    Message::RequestVote(RequestVote {
        term: Term(1),
        candidate_id,
        last_log_index: LogIndex::ZERO,
        last_log_term: Term::default(),
    })
}
