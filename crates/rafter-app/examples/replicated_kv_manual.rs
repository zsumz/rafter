//! Manual replicated KV over `rafter-app`.
//!
//! The application owns the driver loop: each `GroupStepReport` returns peer
//! envelopes, and this example explicitly routes those envelopes to the
//! destination group. No async runtime, sockets, or background tasks are used.
//! It uses in-memory stores and local routing, so it is not a production
//! durability, authentication, or transport-security template.
//!
//! Run with:
//!
//! ```text
//! cargo run -p rafter-app --example replicated_kv_manual
//! ```

use std::collections::{BTreeMap, VecDeque};

use rafter::{LocalProposalId, LogIndex, NodeConfig, NodeId, ReadId, Role};
use rafter_app::group::{GroupInput, GroupStepReport, RaftGroup, ReadReport};
use rafter_app::proposal::{Proposal, ProposalEvent};
use rafter_app::read::{ReadOutcome, ReadRequest};
use rafter_app::state_machine::{
    ApplicationSnapshot, ApplyBatch, ApplyResult, ReadBarrier, ReplicatedStateMachine,
};
use rafter_app::transport::PeerEnvelope;
use rafter_runtime::DurableRaftNode;
use rafter_storage::InMemoryRaftHardStateStore;

const GROUP_ID: ShardGroupId = ShardGroupId(7);

type KvGroup = RaftGroup<ShardGroupId, KvStateMachine, DurableRaftNode>;
type KvReport = GroupStepReport<ShardGroupId, KvCommandResult>;

fn main() {
    let mut groups = vec![
        group(1, &[2, 3], 3),
        group(2, &[1, 3], 9),
        group(3, &[1, 2], 9),
    ];
    let mut network = VecDeque::new();
    let mut reports = Vec::new();

    elect_node_one(&mut groups, &mut network, &mut reports);
    reports.clear();

    let proposal_id = LocalProposalId(100);
    let proposal_report = group_mut(&mut groups, NodeId(1))
        .step(GroupInput::Proposal {
            proposal: Proposal {
                local_proposal_id: proposal_id,
                client_request_id: None,
                command: KvCommand::Put {
                    key: "alpha".to_owned(),
                    value: "one".to_owned(),
                },
            },
        })
        .expect("leader accepts proposal");
    assert!(proposal_report.proposal_events.iter().any(|event| matches!(
        event,
        ProposalEvent::Appended {
            local_proposal_id,
            ..
        } if *local_proposal_id == proposal_id
    )));
    record_report(proposal_report, &mut network, &mut reports);
    dispatch_all(&mut groups, &mut network, &mut reports);
    tick_leader_until(&mut groups, &mut network, &mut reports, |groups| {
        groups
            .iter()
            .all(|group| group.state_machine().get("alpha") == Some("one"))
    });

    assert!(reports.iter().any(|report| {
        report.proposal_events.iter().any(|event| {
            matches!(
                event,
                ProposalEvent::Applied {
                    local_proposal_id,
                    result: KvCommandResult::Put { previous: None },
                    ..
                } if *local_proposal_id == proposal_id
            )
        })
    }));
    println!("put alpha=one committed and applied on all replicas");

    reports.clear();
    let read_id = ReadId(200);
    let mut read = record_read(
        group_mut(&mut groups, NodeId(1))
            .read(read_request(read_id))
            .expect("linearizable read starts"),
        &mut network,
        &mut reports,
    );
    dispatch_all(&mut groups, &mut network, &mut reports);
    for _ in 0..8 {
        if matches!(read, ReadPoll::Ready { .. }) {
            break;
        }
        let report = group_mut(&mut groups, NodeId(1))
            .step(GroupInput::Tick)
            .expect("leader tick succeeds");
        record_report(report, &mut network, &mut reports);
        dispatch_all(&mut groups, &mut network, &mut reports);
        read = record_read(
            group_mut(&mut groups, NodeId(1))
                .read(read_request(read_id))
                .expect("linearizable read completes"),
            &mut network,
            &mut reports,
        );
        dispatch_all(&mut groups, &mut network, &mut reports);
    }

    let value = match read {
        ReadPoll::Ready { result } => result,
        ReadPoll::Pending => panic!("linearizable read result is ready"),
    };
    assert_eq!(value, Some("one".to_owned()));
    println!("linearizable read returned alpha={value:?}");
}

fn group(id: u64, peers: &[u64], election_timeout_ticks: u64) -> KvGroup {
    let config = NodeConfig::new(
        NodeId(id),
        peers.iter().copied().map(NodeId).collect(),
        election_timeout_ticks,
    )
    .expect("static Raft config is valid");
    let raft = DurableRaftNode::new(config, InMemoryRaftHardStateStore::new())
        .expect("in-memory durable node opens");
    let app = KvStateMachine::default();
    let applied_index = app
        .applied_index()
        .expect("fresh state machine reports its applied floor");
    RaftGroup::with_applied_index(GROUP_ID, NodeId(id), raft, app, applied_index)
}

fn elect_node_one(
    groups: &mut [KvGroup],
    network: &mut VecDeque<PeerEnvelope<ShardGroupId>>,
    reports: &mut Vec<KvReport>,
) {
    for _ in 0..8 {
        let report = group_mut(groups, NodeId(1))
            .step(GroupInput::Tick)
            .expect("tick succeeds");
        record_report(report, network, reports);
        dispatch_all(groups, network, reports);
        if group_mut(groups, NodeId(1)).metrics().role == Role::Leader {
            return;
        }
    }
    panic!("node 1 did not become leader");
}

fn tick_leader_until(
    groups: &mut [KvGroup],
    network: &mut VecDeque<PeerEnvelope<ShardGroupId>>,
    reports: &mut Vec<KvReport>,
    mut done: impl FnMut(&[KvGroup]) -> bool,
) {
    for _ in 0..8 {
        if done(groups) {
            return;
        }
        let report = group_mut(groups, NodeId(1))
            .step(GroupInput::Tick)
            .expect("leader tick succeeds");
        record_report(report, network, reports);
        dispatch_all(groups, network, reports);
    }
    assert!(done(groups), "manual driver did not reach expected state");
}

fn dispatch_all(
    groups: &mut [KvGroup],
    network: &mut VecDeque<PeerEnvelope<ShardGroupId>>,
    reports: &mut Vec<KvReport>,
) {
    while let Some(envelope) = network.pop_front() {
        let to = envelope.to;
        let report = group_mut(groups, to)
            .step(GroupInput::PeerMessage { envelope })
            .expect("peer message step succeeds");
        record_report(report, network, reports);
    }
}

fn record_report(
    report: KvReport,
    network: &mut VecDeque<PeerEnvelope<ShardGroupId>>,
    reports: &mut Vec<KvReport>,
) {
    network.extend(report.peer_messages.iter().cloned());
    reports.push(report);
}

fn group_mut(groups: &mut [KvGroup], node_id: NodeId) -> &mut KvGroup {
    groups
        .iter_mut()
        .find(|group| group.node_id() == node_id)
        .expect("group exists for node")
}

fn read_request(read_id: ReadId) -> ReadRequest<ShardGroupId, KvQuery> {
    ReadRequest::Linearizable {
        group_id: GROUP_ID,
        read_id,
        query: KvQuery::Get {
            key: "alpha".to_owned(),
        },
        min_applied_index: None,
        context: b"get alpha".to_vec(),
    }
}

/// Routes everything a read's step emitted, then classifies the outcome.
///
/// The report is the caller's copy of the step's protocol effects, so it is
/// recorded exactly like any other step's — the outcome alone would leave a
/// stalled or rejected read's peer traffic undelivered.
fn record_read(
    read: ReadReport<ShardGroupId, Option<String>, KvCommandResult>,
    network: &mut VecDeque<PeerEnvelope<ShardGroupId>>,
    reports: &mut Vec<KvReport>,
) -> ReadPoll {
    record_report(read.report, network, reports);
    match read.outcome {
        ReadOutcome::Ready { result, .. } => ReadPoll::Ready { result },
        ReadOutcome::Pending { .. }
        | ReadOutcome::LinearizableFreshnessUnavailable { .. }
        | ReadOutcome::LocalFreshnessUnavailable { .. } => ReadPoll::Pending,
        ReadOutcome::Rejected { reason, .. } => {
            panic!("linearizable read rejected: {reason:?}");
        }
        ReadOutcome::Canceled { reason, .. } => {
            panic!("linearizable read canceled: {reason:?}");
        }
        _ => {
            panic!("unsupported read outcome in manual example");
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ReadPoll {
    Pending,
    Ready { result: Option<String> },
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ShardGroupId(u64);

#[derive(Clone, Debug, Eq, PartialEq)]
enum KvCommand {
    Put { key: String, value: String },
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum KvCommandResult {
    Put { previous: Option<String> },
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum KvQuery {
    Get { key: String },
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct KvStateMachine {
    applied_index: LogIndex,
    values: BTreeMap<String, String>,
}

impl KvStateMachine {
    fn get(&self, key: &str) -> Option<&str> {
        self.values.get(key).map(String::as_str)
    }
}

impl ReplicatedStateMachine for KvStateMachine {
    type Command = KvCommand;
    type CommandResult = KvCommandResult;
    type Query = KvQuery;
    type QueryResult = Option<String>;
    type Error = String;

    fn applied_index(&self) -> Result<LogIndex, Self::Error> {
        Ok(self.applied_index)
    }

    fn encode_command(&self, command: &Self::Command) -> Result<Vec<u8>, Self::Error> {
        match command {
            KvCommand::Put { key, value } => {
                let mut bytes = b"put\0".to_vec();
                bytes.extend_from_slice(key.as_bytes());
                bytes.push(0);
                bytes.extend_from_slice(value.as_bytes());
                Ok(bytes)
            }
        }
    }

    fn decode_command(&self, payload: &[u8]) -> Result<Self::Command, Self::Error> {
        let parts = payload.split(|byte| *byte == 0).collect::<Vec<_>>();
        if parts.len() != 3 || parts[0] != b"put" {
            return Err("invalid KV command frame".to_owned());
        }
        let key = String::from_utf8(parts[1].to_vec()).map_err(|error| error.to_string())?;
        let value = String::from_utf8(parts[2].to_vec()).map_err(|error| error.to_string())?;
        Ok(KvCommand::Put { key, value })
    }

    fn apply_batch(
        &mut self,
        batch: ApplyBatch<Self::Command>,
    ) -> Result<Vec<ApplyResult<Self::CommandResult>>, Self::Error> {
        let mut results = Vec::with_capacity(batch.entries.len());
        for entry in batch.entries {
            let result = match entry.command {
                KvCommand::Put { key, value } => KvCommandResult::Put {
                    previous: self.values.insert(key, value),
                },
            };
            self.applied_index = entry.index;
            results.push(ApplyResult {
                index: entry.index,
                term: entry.term,
                result,
                local_proposal_id: entry.local_proposal_id,
            });
        }
        Ok(results)
    }

    fn read(
        &self,
        query: Self::Query,
        barrier: ReadBarrier,
    ) -> Result<Self::QueryResult, Self::Error> {
        if self.applied_index < barrier.required_applied_index {
            return Err("read barrier has not been reached".to_owned());
        }
        match query {
            KvQuery::Get { key } => Ok(self.values.get(&key).cloned()),
        }
    }

    fn build_snapshot(&mut self, at: LogIndex) -> Result<ApplicationSnapshot, Self::Error> {
        Ok(ApplicationSnapshot {
            applied_index: at,
            payload: Vec::new(),
            raft_snapshot: None,
        })
    }

    fn install_snapshot(&mut self, snapshot: ApplicationSnapshot) -> Result<(), Self::Error> {
        self.applied_index = snapshot.applied_index;
        self.values.clear();
        Ok(())
    }
}
