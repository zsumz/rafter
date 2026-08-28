//! Runtime trait boundary for persisted Rafter nodes.
//!
//! This crate owns only the application-facing runtime contract. It depends on
//! the deterministic `rafter` kernel and deliberately does not depend on
//! storage, durable runtime, app, service, transport, or async crates.
//!
//! # The boundary as code
//!
//! [`PersistedRaftRuntime`] is one rule about ordering: an output is released
//! only after everything it depends on is durable. The implementation below
//! writes to a `Vec` standing in for the medium, which is what makes it small
//! enough to read — the medium is not the lesson, the order is. `rafter-runtime`
//! ships the real one over the `rafter-storage` traits.
//!
//! ```
//! # use std::{error::Error, fmt};
//! # use rafter::{
//! #     ClientProposalInput, LogIndex, MembershipConfig, ReplicationProgress, Role,
//! # };
//! use rafter::{Input, Message, Node, NodeConfig, NodeId, Output, PreVoteResponse, Term};
//! use rafter_runtime_api::PersistedRaftRuntime;
//!
//! /// Term and vote, as a durable medium would hold them.
//! #[derive(Clone, Copy, Debug, Eq, PartialEq)]
//! struct DurableRecord {
//!     term: Term,
//!     voted_for: Option<NodeId>,
//! }
//!
//! #[derive(Debug)]
//! struct RecordingRuntime {
//!     node: Node,
//!     written: Vec<DurableRecord>,
//! }
//!
//! impl PersistedRaftRuntime for RecordingRuntime {
//!     type Error = MediumFailure;
//!
//!     /// Step the kernel, write what the step promised, then release the
//!     /// outputs. A caller may send or apply these without a second fence.
//!     fn step(&mut self, input: Input) -> Result<Vec<Output>, Self::Error> {
//!         let outputs = self.node.step(input);
//!         self.written.push(DurableRecord {
//!             term: self.node.current_term(),
//!             voted_for: self.node.voted_for(),
//!         });
//!         Ok(outputs)
//!     }
//! #
//! #   fn id(&self) -> NodeId { self.node.id() }
//! #   fn leader_hint(&self) -> Option<NodeId> { self.node.leader_hint() }
//! #   fn role(&self) -> Role { self.node.role() }
//! #   fn current_term(&self) -> Term { self.node.current_term() }
//! #   fn commit_index(&self) -> LogIndex { self.node.commit_index() }
//! #   fn last_log_index(&self) -> LogIndex { self.node.last_log_index() }
//! #   fn snapshot_index(&self) -> LogIndex { self.node.snapshot_index() }
//! #   fn membership(&self) -> MembershipConfig { self.node.effective_membership() }
//! #   fn committed_membership(&self) -> MembershipConfig { self.node.committed_membership() }
//! #   fn replication(&self) -> Vec<ReplicationProgress> { self.node.leader_replication_progress() }
//! #   fn term_at_index(&self, index: LogIndex) -> Option<Term> { self.node.term_at_index(index) }
//! #
//! #   // Honest for a runtime that never compacts, which this one does not:
//! #   // every retained index is searchable and the snapshot boundary is zero.
//! #   fn committed_application_index_through(&self, index: LogIndex) -> LogIndex {
//! #       let bound = index.min(self.node.commit_index());
//! #       self.node
//! #           .log_entries_slice_from(LogIndex(1))
//! #           .iter()
//! #           .enumerate()
//! #           .rev()
//! #           .map(|(offset, entry)| (LogIndex(offset as u64 + 1), entry))
//! #           .find(|(at, entry)| *at <= bound && entry.application_payload().is_some())
//! #           .map_or(LogIndex::ZERO, |(at, _)| at)
//! #   }
//! #
//! #   fn step_proposal_batch(
//! #       &mut self,
//! #       proposals: Vec<ClientProposalInput>,
//! #   ) -> Result<Vec<Output>, Self::Error> {
//! #       let outputs = self.node.step_proposal_batch(proposals);
//! #       self.record();
//! #       Ok(outputs)
//! #   }
//! #
//! #   fn step_batch(&mut self, inputs: Vec<Input>) -> Result<Vec<Output>, Self::Error> {
//! #       let outputs = self.node.step_batch(inputs);
//! #       self.record();
//! #       Ok(outputs)
//! #   }
//! }
//! #
//! # impl RecordingRuntime {
//! #     fn record(&mut self) {
//! #         self.written.push(DurableRecord {
//! #             term: self.node.current_term(),
//! #             voted_for: self.node.voted_for(),
//! #         });
//! #     }
//! # }
//! #
//! # /// A typed error, because `PersistedRaftRuntime::Error` is part of the
//! # /// public app/service error stack and must be walkable rather than rendered.
//! # #[derive(Debug)]
//! # struct MediumFailure;
//! # impl fmt::Display for MediumFailure {
//! #     fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
//! #         formatter.write_str("durable medium rejected the write")
//! #     }
//! # }
//! # impl Error for MediumFailure {}
//! #
//! let config = NodeConfig::new(NodeId(1), vec![NodeId(2), NodeId(3)], 3)
//!     .expect("valid raft config");
//! let mut runtime = RecordingRuntime {
//!     node: Node::new(config),
//!     written: Vec::new(),
//! };
//!
//! // Pre-vote proposes a term without adopting it, so three ticks reach the
//! // election timeout and still promise nothing a restart must remember.
//! for _ in 0..3 {
//!     runtime.step(Input::Tick).expect("the medium accepts the write");
//! }
//! assert_eq!(
//!     runtime.written.last().copied(),
//!     Some(DurableRecord { term: Term(0), voted_for: None }),
//! );
//!
//! // A granted poll turns the probe into a campaign: the node raises its term
//! // and votes for itself. Those two facts are on the medium in the same step
//! // that hands back the vote requests which depend on them.
//! let outputs = runtime
//!     .step(Input::Message {
//!         from: NodeId(2),
//!         message: Message::PreVoteResponse(PreVoteResponse {
//!             term: Term(1),
//!             voter_id: NodeId(2),
//!             vote_granted: true,
//!         }),
//!     })
//!     .expect("the medium accepts the write");
//! assert!(outputs.iter().any(|output| matches!(
//!     output,
//!     Output::Send { message: Message::RequestVote(_), .. }
//! )));
//! assert_eq!(
//!     runtime.written.last().copied(),
//!     Some(DurableRecord { term: Term(1), voted_for: Some(NodeId(1)) }),
//! );
//! ```

mod contract;

pub use contract::PersistedRaftRuntime;
