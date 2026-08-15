//! Synchronous embedded replicated-state-machine support for Rafter.
//!
//! `rafter-app` is the manual application-facing layer above the deterministic
//! Raft kernel and the `rafter-runtime-api` contract. It is intended for
//! databases, replicated services, sharded systems, and other embedded
//! runtimes that own their storage, transport, routing, authorization,
//! recovery loops, and application command semantics.
//!
//! This crate owns `RaftGroup` orchestration, proposal/read bookkeeping,
//! state-machine apply guards, app-facing reports, and group-level metrics.
//! This crate does not spawn tasks, open sockets, require Tokio, or assume
//! one process maps to one Raft group. It exposes explicit group-step reports
//! so callers can dispatch peer messages, apply committed entries, publish
//! metrics, and handle recovery under their own runtime policy.
//!
//! # Embedding a state machine
//!
//! A replicated counter, driven by a lone voter so the whole path fits in one
//! screen: no peer routing is needed to reach a decision.
//!
//! A group is generic over its runtime and names only the
//! [`PersistedRaftRuntime`](rafter_runtime_api::PersistedRaftRuntime) contract,
//! which is what keeps this layer off any one storage or scheduling choice — so
//! the example carries a small persisted runtime of its own. The shipped durable
//! one is `DurableRaftNode` in the `rafter-runtime` crate, and
//! `cargo run -p rafter-app --example replicated_kv_manual` drives three of
//! them, with the caller routing the peer envelopes each report hands back.
//!
//! ```
//! # use std::{convert::Infallible, error::Error, fmt};
//! # use rafter::{
//! #     ClientProposalInput, Input, MembershipConfig, Node, Output, ReplicationProgress, Term,
//! # };
//! # use rafter_runtime_api::PersistedRaftRuntime;
//! use rafter::{LocalProposalId, LogIndex, NodeConfig, NodeId, Role};
//! use rafter_app::group::{GroupInput, RaftGroup};
//! use rafter_app::proposal::Proposal;
//! use rafter_app::state_machine::{
//!     ApplicationSnapshot, ApplicationSnapshotError, ApplyBatch, ApplyResult, ReadBarrier,
//!     ReplicatedStateMachine, SnapshotSupport,
//! };
//!
//! /// A replicated counter. `Command` is how much to add; `CommandResult` is
//! /// the total that addition produced.
//! #[derive(Debug, Default)]
//! struct Counter {
//!     applied_index: LogIndex,
//!     total: u64,
//! }
//!
//! impl ReplicatedStateMachine for Counter {
//!     type Command = u64;
//!     type CommandResult = u64;
//!     type Query = ();
//!     type QueryResult = u64;
//!     type Error = CounterError;
//!
//!     const SNAPSHOT_SUPPORT: SnapshotSupport = SnapshotSupport::Supported;
//!
//!     /// The durable floor a restart resumes from. This example keeps it in
//!     /// memory; a real one reads it back from the transaction that persisted
//!     /// the effects beside it, because the two must never disagree.
//!     fn applied_index(&self) -> Result<LogIndex, Self::Error> {
//!         Ok(self.applied_index)
//!     }
//!
//!     /// Encoded once, on the proposing node, and decoded by every replica —
//!     /// so the encoding has to be deterministic and stable across versions.
//!     fn encode_command(&self, command: &u64) -> Result<Vec<u8>, Self::Error> {
//!         Ok(command.to_be_bytes().to_vec())
//!     }
//!
//!     fn decode_command(&self, payload: &[u8]) -> Result<u64, Self::Error> {
//!         let bytes = <[u8; 8]>::try_from(payload).map_err(|_| CounterError)?;
//!         Ok(u64::from_be_bytes(bytes))
//!     }
//!
//!     /// Effects and the applied index move together, in log order. A state
//!     /// machine that persists one without the other lies to the next restart.
//!     fn apply_batch(
//!         &mut self,
//!         batch: ApplyBatch<u64>,
//!     ) -> Result<Vec<ApplyResult<u64>>, Self::Error> {
//!         batch
//!             .entries
//!             .into_iter()
//!             .map(|entry| {
//!                 self.total += entry.command;
//!                 self.applied_index = entry.index;
//!                 Ok(ApplyResult {
//!                     index: entry.index,
//!                     term: entry.term,
//!                     result: self.total,
//!                     local_proposal_id: entry.local_proposal_id,
//!                 })
//!             })
//!             .collect()
//!     }
//!
//!     fn read(&self, _query: (), barrier: ReadBarrier) -> Result<u64, Self::Error> {
//!         if self.applied_index < barrier.required_applied_index {
//!             return Err(CounterError);
//!         }
//!         Ok(self.total)
//!     }
//! #
//! #   fn build_snapshot(
//! #       &mut self,
//! #       at: LogIndex,
//! #   ) -> Result<ApplicationSnapshot, ApplicationSnapshotError<Self::Error>> {
//! #       Ok(ApplicationSnapshot {
//! #           applied_index: at,
//! #           payload: self.total.to_be_bytes().to_vec(),
//! #           raft_snapshot: None,
//! #       })
//! #   }
//! #
//! #   fn install_snapshot(
//! #       &mut self,
//! #       snapshot: ApplicationSnapshot,
//! #   ) -> Result<(), ApplicationSnapshotError<Self::Error>> {
//! #       let bytes = <[u8; 8]>::try_from(&snapshot.payload[..]).map_err(|_| CounterError)?;
//! #       self.total = u64::from_be_bytes(bytes);
//! #       self.applied_index = snapshot.applied_index;
//! #       Ok(())
//! #   }
//! }
//! #
//! # /// A real `std::error::Error`, because `ReplicatedStateMachine::Error` is
//! # /// part of the public app/service error stack an operator has to walk.
//! # #[derive(Debug)]
//! # struct CounterError;
//! # impl fmt::Display for CounterError {
//! #     fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
//! #         formatter.write_str("counter command or snapshot payload is malformed")
//! #     }
//! # }
//! # impl Error for CounterError {}
//! #
//! # /// A persisted runtime whose medium is a `Vec`: it steps the kernel, writes
//! # /// what the step promised, and only then releases the outputs.
//! # #[derive(Debug)]
//! # struct LocalRuntime {
//! #     node: Node,
//! #     written: Vec<(Term, Option<NodeId>)>,
//! # }
//! #
//! # impl LocalRuntime {
//! #     fn new(config: NodeConfig) -> Self {
//! #         Self { node: Node::new(config), written: Vec::new() }
//! #     }
//! #     fn persist(&mut self) {
//! #         self.written.push((self.node.current_term(), self.node.voted_for()));
//! #     }
//! # }
//! #
//! # impl PersistedRaftRuntime for LocalRuntime {
//! #     type Error = Infallible;
//! #     fn id(&self) -> NodeId { self.node.id() }
//! #     fn leader_hint(&self) -> Option<NodeId> { self.node.leader_hint() }
//! #     fn role(&self) -> Role { self.node.role() }
//! #     fn current_term(&self) -> Term { self.node.current_term() }
//! #     fn commit_index(&self) -> LogIndex { self.node.commit_index() }
//! #     fn last_log_index(&self) -> LogIndex { self.node.last_log_index() }
//! #     fn snapshot_index(&self) -> LogIndex { self.node.snapshot_index() }
//! #     fn membership(&self) -> MembershipConfig { self.node.effective_membership() }
//! #     fn committed_membership(&self) -> MembershipConfig { self.node.committed_membership() }
//! #     fn replication(&self) -> Vec<ReplicationProgress> {
//! #         self.node.leader_replication_progress()
//! #     }
//! #     fn term_at_index(&self, index: LogIndex) -> Option<Term> { self.node.term_at_index(index) }
//! #     fn committed_application_index_through(&self, index: LogIndex) -> LogIndex {
//! #         let bound = index.min(self.node.commit_index());
//! #         self.node
//! #             .log_entries_slice_from(LogIndex(1))
//! #             .iter()
//! #             .enumerate()
//! #             .rev()
//! #             .map(|(offset, entry)| (LogIndex(offset as u64 + 1), entry))
//! #             .find(|(at, entry)| *at <= bound && entry.application_payload().is_some())
//! #             .map_or(LogIndex::ZERO, |(at, _)| at)
//! #     }
//! #     fn step(&mut self, input: Input) -> Result<Vec<Output>, Infallible> {
//! #         let outputs = self.node.step(input);
//! #         self.persist();
//! #         Ok(outputs)
//! #     }
//! #     fn step_proposal_batch(
//! #         &mut self,
//! #         proposals: Vec<ClientProposalInput>,
//! #     ) -> Result<Vec<Output>, Infallible> {
//! #         let outputs = self.node.step_proposal_batch(proposals);
//! #         self.persist();
//! #         Ok(outputs)
//! #     }
//! #     fn step_batch(&mut self, inputs: Vec<Input>) -> Result<Vec<Output>, Infallible> {
//! #         let outputs = self.node.step_batch(inputs);
//! #         self.persist();
//! #         Ok(outputs)
//! #     }
//! # }
//! #
//! let config = NodeConfig::new(NodeId(1), Vec::new(), 3).expect("valid raft config");
//! let raft = LocalRuntime::new(config);
//! let counter = Counter::default();
//! let applied = counter
//!     .applied_index()
//!     .expect("a fresh state machine reports its applied floor");
//! let mut group = RaftGroup::with_applied_index("credits", NodeId(1), raft, counter, applied);
//!
//! for _ in 0..3 {
//!     group.step(GroupInput::Tick).expect("tick succeeds");
//! }
//! assert_eq!(group.metrics().role, Role::Leader);
//!
//! let report = group
//!     .step(GroupInput::Proposal {
//!         proposal: Proposal {
//!             local_proposal_id: LocalProposalId(1),
//!             client_request_id: None,
//!             command: 7,
//!         },
//!     })
//!     .expect("the leader accepts the command");
//!
//! // `applied` is the one list that proves a write took effect: an entry
//! // reaching it has committed and been applied. `proposal_events` carries the
//! // lifecycle, and `Appended` there is not yet success.
//! assert_eq!(report.applied.len(), 1);
//! assert_eq!(report.applied[0].result, 7);
//! assert_eq!(report.applied[0].local_proposal_id, Some(LocalProposalId(1)));
//! assert_eq!(group.state_machine().total, 7);
//! ```

/// Group-level error types surfaced by the app orchestration layer.
pub mod error;
/// Stateful replicated group orchestration over a persisted Raft runtime.
pub mod group;
/// Membership planning and reporting helpers for app-managed changes.
pub mod membership;
/// App-layer group metrics snapshots.
pub mod metrics;
/// Proposal request, completion, and unknown-outcome types.
pub mod proposal;
/// Linearizable and local read request/report types.
pub mod read;
/// Application snapshot events emitted through group reports.
pub mod snapshot;
/// Replicated state-machine traits and apply/read payload types.
pub mod state_machine;
/// Peer envelope authentication and validation helpers.
pub mod transport;
