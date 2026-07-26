//! Manual many-group KV routing over `MultiRaftHost`.
//!
//! The drivers here are intentionally tiny byte-command KV shards. The point
//! is the host shape: open multiple caller-identified groups, tick them
//! together, route peer envelopes by group ID, and keep writes isolated per
//! group.
//! The example does not provide production storage, authentication, transport,
//! or shard-allocation policy.
//!
//! Run with:
//!
//! ```text
//! cargo run -p rafter-multiraft --example multi_group_kv
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
use rafter_multiraft::{GroupDriver, MultiRaftHost, MultiRaftMetrics};

fn main() {
    let mut host = MultiRaftHost::new();
    host.open_group(ShardId(1), KvShardDriver::new(ShardId(1)))
        .expect("open shard 1");
    host.open_group(ShardId(2), KvShardDriver::new(ShardId(2)))
        .expect("open shard 2");

    let pass = host.tick_all();
    assert!(pass.is_complete(), "every shard ticked");
    assert_eq!(pass.visited(), host.len(), "the pass visited every shard");
    assert_eq!(
        pass.reports()
            .map(|report| report.group_id)
            .collect::<Vec<_>>(),
        vec![ShardId(1), ShardId(2)]
    );

    let report = host
        .step_group(
            &ShardId(1),
            GroupInput::Proposal {
                proposal: Proposal {
                    local_proposal_id: LocalProposalId(10),
                    client_request_id: None,
                    command: put("alpha", "one"),
                },
            },
        )
        .expect("write routes to shard 1");
    let routed = route_peer_messages(&mut host, report).expect("peer messages route by group id");
    assert_eq!(routed.len(), 1);
    assert_eq!(routed[0].group_id, ShardId(1));

    let metrics = host.metrics().expect("host metrics");
    assert_group_indexes(
        &metrics,
        &[(ShardId(1), LogIndex(1)), (ShardId(2), LogIndex::ZERO)],
    );
    println!(
        "shard 1 applied {:?}, shard 2 applied {:?}",
        metrics.groups[0].applied_index, metrics.groups[1].applied_index
    );
}

fn route_peer_messages(
    host: &mut MultiRaftHost<ShardId>,
    report: GroupStepReport<ShardId, Vec<u8>>,
) -> Result<Vec<GroupStepReport<ShardId, Vec<u8>>>, rafter_multiraft::MultiRaftError<ShardId>> {
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

fn put(key: &str, value: &str) -> Vec<u8> {
    let mut bytes = b"put\0".to_vec();
    bytes.extend_from_slice(key.as_bytes());
    bytes.push(0);
    bytes.extend_from_slice(value.as_bytes());
    bytes
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ShardId(u64);

#[derive(Debug)]
struct KvShardDriver {
    group_id: ShardId,
    values: BTreeMap<String, String>,
    applied_index: LogIndex,
}

impl KvShardDriver {
    fn new(group_id: ShardId) -> Self {
        Self {
            group_id,
            values: BTreeMap::new(),
            applied_index: LogIndex::ZERO,
        }
    }
}

impl GroupDriver<ShardId> for KvShardDriver {
    fn step(
        &mut self,
        input: GroupInput<ShardId, Vec<u8>>,
    ) -> Result<GroupStepReport<ShardId, Vec<u8>>, String> {
        match input {
            GroupInput::Proposal { proposal } => {
                let apply_result = self.apply_put(&proposal)?;
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
                    let apply_result = self.apply_put(&proposal)?;
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

impl KvShardDriver {
    fn apply_put(&mut self, proposal: &Proposal<Vec<u8>>) -> Result<ApplyResult<Vec<u8>>, String> {
        let (key, value) = decode_put(&proposal.command)?;
        self.applied_index = LogIndex(self.applied_index.0 + 1);
        self.values.insert(key, value);
        Ok(ApplyResult {
            index: self.applied_index,
            term: Term(1),
            result: b"ok".to_vec(),
            local_proposal_id: Some(proposal.local_proposal_id),
        })
    }

    fn report(&self) -> GroupStepReport<ShardId, Vec<u8>> {
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

fn decode_put(payload: &[u8]) -> Result<(String, String), String> {
    let parts = payload.split(|byte| *byte == 0).collect::<Vec<_>>();
    if parts.len() != 3 || parts[0] != b"put" {
        return Err("invalid put command".to_owned());
    }
    let key = String::from_utf8(parts[1].to_vec()).map_err(|error| error.to_string())?;
    let value = String::from_utf8(parts[2].to_vec()).map_err(|error| error.to_string())?;
    Ok((key, value))
}

fn vote_message(candidate_id: NodeId) -> Message {
    Message::RequestVote(RequestVote {
        term: Term(1),
        candidate_id,
        last_log_index: LogIndex::ZERO,
        last_log_term: Term::default(),
    })
}
