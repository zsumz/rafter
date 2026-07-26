//! Real `RaftGroup`s under `TypedMultiRaftHost`.
//!
//! The host owns one local node for each group. Remote peer responses are
//! injected as explicit `PeerEnvelope`s, which is the same boundary a real
//! transport would use after routing by group ID.
//! It uses in-memory stores and hand-built responses, so it is not a
//! production transport, storage-layout, or peer-authentication template.
//!
//! Run with:
//!
//! ```text
//! cargo run -p rafter-multiraft --example real_raft_groups
//! ```

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;

use rafter::{
    AppendEntries, AppendEntriesResponse, LocalProposalId, LogIndex, Message, NodeConfig, NodeId,
    RequestVote, RequestVoteResponse, Term,
};
use rafter_app::{
    group::{GroupInput, GroupStepReport, RaftGroup},
    proposal::{Proposal, ProposalEvent},
    state_machine::{
        ApplicationSnapshot, ApplicationSnapshotError, ApplyBatch, ApplyResult, ReadBarrier,
        ReplicatedStateMachine, SnapshotSupport,
    },
    transport::PeerEnvelope,
};
use rafter_multiraft::{MultiRaftError, TypedMultiRaftHost};
use rafter_runtime::DurableRaftNode;
use rafter_storage::InMemoryRaftHardStateStore;

type Host = TypedMultiRaftHost<ShardId, KvCommand, KvCommandResult>;
type Report = GroupStepReport<ShardId, KvCommandResult>;

fn main() {
    let mut host = Host::new();
    host.open_group(ShardId(1), group(ShardId(1)))
        .expect("open shard 1");
    host.open_group(ShardId(2), group(ShardId(2)))
        .expect("open shard 2");

    elect_local_node(&mut host, ShardId(1));

    let proposal_report = host
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
        .expect("leader accepts shard 1 write");
    assert_peer_messages_route_by_group(&proposal_report, ShardId(1));

    let append = append_to_peer(&proposal_report, NodeId(2)).expect("proposal is sent to peer 2");
    let apply_report = route_peer_message(
        &mut host,
        append_ack(
            ShardId(1),
            NodeId(2),
            append.term,
            append.match_index,
            append.sequence,
        ),
    )
    .expect("peer acknowledgement routes by group id");

    assert!(apply_report.proposal_events.iter().any(|event| matches!(
        event,
        ProposalEvent::Applied {
            local_proposal_id: LocalProposalId(10),
            result: KvCommandResult::Put { previous: None },
            ..
        }
    )));

    let metrics = host.metrics();
    let shard_1 = metrics
        .groups
        .iter()
        .find(|metrics| metrics.group_id == ShardId(1))
        .expect("shard 1 metrics exist");
    let shard_2 = metrics
        .groups
        .iter()
        .find(|metrics| metrics.group_id == ShardId(2))
        .expect("shard 2 metrics exist");
    assert_eq!(shard_1.applied_index, LogIndex(2));
    assert_eq!(shard_2.applied_index, LogIndex::ZERO);

    println!(
        "real shard 1 applied {:?}; shard 2 stayed at zero",
        shard_1.applied_index
    );
}

fn group(group_id: ShardId) -> RaftGroup<ShardId, KvStateMachine, DurableRaftNode> {
    let config = NodeConfig::new(NodeId(1), vec![NodeId(2), NodeId(3)], 3)
        .expect("static Raft config is valid")
        .with_pre_vote(false);
    let raft = DurableRaftNode::new(config, InMemoryRaftHardStateStore::new())
        .expect("in-memory durable node opens");
    RaftGroup::new(group_id, NodeId(1), raft, KvStateMachine::default())
}

fn elect_local_node(host: &mut Host, group_id: ShardId) {
    let mut election_report = None;
    for _ in 0..3 {
        let report = host
            .step_group(&group_id, GroupInput::Tick)
            .expect("election tick succeeds");
        if request_vote_term(&report).is_some() {
            election_report = Some(report);
            break;
        }
    }
    let election_report = election_report.expect("local node starts election");
    assert_peer_messages_route_by_group(&election_report, group_id);
    let term = request_vote_term(&election_report).expect("request-vote term is present");

    let leader_report = route_peer_message(host, vote_granted(group_id, NodeId(2), term))
        .expect("vote response routes by group id");
    assert_peer_messages_route_by_group(&leader_report, group_id);
    let noop = append_to_peer(&leader_report, NodeId(2)).expect("leader no-op is sent to peer 2");

    let commit_report = route_peer_message(
        host,
        append_ack(
            group_id,
            NodeId(2),
            noop.term,
            noop.match_index,
            noop.sequence,
        ),
    )
    .expect("no-op acknowledgement routes by group id");
    assert_peer_messages_route_by_group(&commit_report, group_id);
}

fn route_peer_message(
    host: &mut Host,
    envelope: PeerEnvelope<ShardId>,
) -> Result<Report, MultiRaftError<ShardId>> {
    let group_id = envelope.group_id;
    host.step_group(&group_id, GroupInput::PeerMessage { envelope })
}

fn assert_peer_messages_route_by_group(report: &Report, group_id: ShardId) {
    assert!(
        report
            .peer_messages
            .iter()
            .all(|envelope| envelope.group_id == group_id),
        "peer messages must carry the stepped group id"
    );
}

fn request_vote_term(report: &Report) -> Option<Term> {
    report
        .peer_messages
        .iter()
        .find_map(|envelope| match &envelope.message {
            Message::RequestVote(RequestVote { term, .. }) => Some(*term),
            _ => None,
        })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct AppendSummary {
    term: Term,
    sequence: u64,
    match_index: LogIndex,
}

fn append_to_peer(report: &Report, peer: NodeId) -> Option<AppendSummary> {
    report
        .peer_messages
        .iter()
        .find_map(|envelope| match &envelope.message {
            Message::AppendEntries(AppendEntries {
                term,
                sequence,
                prev_log_index,
                entries,
                ..
            }) if envelope.to == peer => Some(AppendSummary {
                term: *term,
                sequence: *sequence,
                match_index: LogIndex(prev_log_index.0 + entries.len() as u64),
            }),
            _ => None,
        })
}

fn vote_granted(group_id: ShardId, voter: NodeId, term: Term) -> PeerEnvelope<ShardId> {
    PeerEnvelope {
        group_id,
        from: voter,
        to: NodeId(1),
        message: Message::RequestVoteResponse(RequestVoteResponse {
            term,
            voter_id: voter,
            vote_granted: true,
        }),
    }
}

fn append_ack(
    group_id: ShardId,
    follower: NodeId,
    term: Term,
    match_index: LogIndex,
    sequence: u64,
) -> PeerEnvelope<ShardId> {
    PeerEnvelope {
        group_id,
        from: follower,
        to: NodeId(1),
        message: Message::AppendEntriesResponse(AppendEntriesResponse {
            term,
            follower_id: follower,
            success: true,
            match_index,
            sequence,
        }),
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ShardId(u64);

#[derive(Clone, Debug, Eq, PartialEq)]
enum KvCommand {
    Put { key: String, value: String },
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum KvCommandResult {
    Put { previous: Option<String> },
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct KvStateMachine {
    applied_index: LogIndex,
    values: BTreeMap<String, String>,
}

/// A failure from this example's key-value state machine.
///
/// `ReplicatedStateMachine::Error` is part of the public app/service error
/// stack, so it is a real `std::error::Error` an operator can walk rather than
/// a `String` every layer above has to render.
#[derive(Clone, Debug, Eq, PartialEq)]
struct KvError(String);

impl KvError {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for KvError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for KvError {}

impl KvStateMachine {
    /// Encodes the whole map so an install can restore it.
    ///
    /// An example is a template. An `install_snapshot` that reported the
    /// snapshot's applied index while discarding the data it carries would
    /// claim durability through a boundary whose effects it just deleted, and
    /// every later read and every readiness gate would believe it.
    fn encode_snapshot(&self, at: LogIndex) -> Result<Vec<u8>, KvError> {
        let mut text = format!("index {}\n", at.0);
        for (key, value) in &self.values {
            if key.contains(['\n', '=']) || value.contains('\n') {
                return Err(KvError::new(
                    "example snapshot keys and values must be line-safe",
                ));
            }
            text.push_str(key);
            text.push('=');
            text.push_str(value);
            text.push('\n');
        }
        Ok(text.into_bytes())
    }

    fn decode_snapshot(payload: &[u8]) -> Result<(LogIndex, BTreeMap<String, String>), KvError> {
        let text =
            String::from_utf8(payload.to_vec()).map_err(|error| KvError::new(error.to_string()))?;
        let mut lines = text.lines();
        let index = lines
            .next()
            .and_then(|header| header.strip_prefix("index "))
            .ok_or_else(|| KvError::new("snapshot payload has no index header"))?
            .parse::<u64>()
            .map(LogIndex)
            .map_err(|error| KvError::new(error.to_string()))?;
        let mut values = BTreeMap::new();
        for line in lines {
            let (key, value) = line
                .split_once('=')
                .ok_or_else(|| KvError::new("snapshot payload has an invalid key/value row"))?;
            values.insert(key.to_owned(), value.to_owned());
        }
        Ok((index, values))
    }
}

impl ReplicatedStateMachine for KvStateMachine {
    type Command = KvCommand;
    type CommandResult = KvCommandResult;
    type Query = String;
    type QueryResult = Option<String>;
    type Error = KvError;

    /// Declared `Supported` because both snapshot methods below round-trip the
    /// map. A state machine that has no snapshot format declares
    /// `SnapshotSupport::Unsupported` and inherits the provided bodies.
    const SNAPSHOT_SUPPORT: SnapshotSupport = SnapshotSupport::Supported;

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
            return Err(KvError::new("invalid put command"));
        }
        let key = String::from_utf8(parts[1].to_vec())
            .map_err(|error| KvError::new(error.to_string()))?;
        let value = String::from_utf8(parts[2].to_vec())
            .map_err(|error| KvError::new(error.to_string()))?;
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

    fn read(&self, query: String, barrier: ReadBarrier) -> Result<Self::QueryResult, Self::Error> {
        if self.applied_index < barrier.required_applied_index {
            return Err(KvError::new("read barrier has not been reached"));
        }
        Ok(self.values.get(&query).cloned())
    }

    fn build_snapshot(
        &mut self,
        at: LogIndex,
    ) -> Result<ApplicationSnapshot, ApplicationSnapshotError<Self::Error>> {
        Ok(ApplicationSnapshot {
            applied_index: at,
            payload: self.encode_snapshot(at)?,
            raft_snapshot: None,
        })
    }

    /// Restores the map the snapshot carries, then adopts its applied index.
    ///
    /// This example keeps its snapshot bytes inline. A Raft-driven install
    /// whose payload the runtime promoted into a snapshot store arrives with an
    /// empty payload and a descriptor instead; refusing it is honest, and
    /// `rafter-app`'s `snapshot_install` example shows how to read the promoted
    /// bytes back.
    fn install_snapshot(
        &mut self,
        snapshot: ApplicationSnapshot,
    ) -> Result<(), ApplicationSnapshotError<Self::Error>> {
        if snapshot.payload.is_empty() && snapshot.raft_snapshot.is_some() {
            return Err(KvError::new(
                "this example reads inline snapshot bytes and was handed a promoted payload",
            )
            .into());
        }
        let (payload_index, values) = Self::decode_snapshot(&snapshot.payload)?;
        if payload_index != snapshot.applied_index {
            return Err(
                KvError::new("snapshot payload index does not match the installed index").into(),
            );
        }
        self.values = values;
        self.applied_index = snapshot.applied_index;
        Ok(())
    }
}
