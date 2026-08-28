//! Constructing a group, and the result vocabulary its operations return.
//!
//! The two constructors differ in one thing: where the applied floor comes
//! from. The aliases beside them spell the group's fallible-result shapes
//! once, so every sibling names a runtime and a state machine the same way.

use super::{
    ApplyEntry, BTreeMap, GroupError, GroupFatalState, GroupStepReport, LogIndex,
    MembershipReportMark, NodeId, PersistedRaftRuntime, PoisonedWaiters, ProposalBatchBeginReport,
    ProposalBegin, ProposalBeginReport, RaftGroup, ReadBarrierBeginReport, ReadOutcome, ReadReport,
    ReplicatedStateMachine,
};

pub(super) type RuntimeGroupError<A, R> =
    GroupError<<A as ReplicatedStateMachine>::Error, <R as PersistedRaftRuntime>::Error>;
pub(super) type GroupResult<A, R, T> = Result<T, RuntimeGroupError<A, R>>;
pub(super) type StepReportResult<G, A, R> =
    GroupResult<A, R, GroupStepReport<G, <A as ReplicatedStateMachine>::CommandResult>>;
pub(super) type ProposalBeginResult<G, A, R> =
    GroupResult<A, R, ProposalBegin<G, <A as ReplicatedStateMachine>::CommandResult>>;
pub(super) type ProposalBeginReportResult<G, A, R> =
    GroupResult<A, R, ProposalBeginReport<G, <A as ReplicatedStateMachine>::CommandResult>>;
pub(super) type ProposalBatchBeginReportResult<G, A, R> =
    GroupResult<A, R, ProposalBatchBeginReport<G, <A as ReplicatedStateMachine>::CommandResult>>;
pub(super) type ReadBarrierBeginReportResult<G, A, R> =
    GroupResult<A, R, ReadBarrierBeginReport<G, <A as ReplicatedStateMachine>::CommandResult>>;
pub(super) type ReadOutcomeResult<G, A, R> =
    GroupResult<A, R, ReadOutcome<G, <A as ReplicatedStateMachine>::QueryResult>>;
pub(super) type ReadReportResult<G, A, R> = GroupResult<
    A,
    R,
    ReadReport<
        G,
        <A as ReplicatedStateMachine>::QueryResult,
        <A as ReplicatedStateMachine>::CommandResult,
    >,
>;
pub(super) type ApplyEntryResult<A, R> =
    GroupResult<A, R, ApplyEntry<<A as ReplicatedStateMachine>::Command>>;

impl<G, A, R> RaftGroup<G, A, R> {
    /// Creates a group with an empty local applied floor.
    ///
    /// Restart paths should prefer [`RaftGroup::with_applied_index`] and pass
    /// the state machine's durable applied index alongside a runtime recovered
    /// with the same applied-through floor.
    #[must_use]
    pub fn new(group_id: G, node_id: NodeId, raft: R, app: A) -> Self
    where
        R: PersistedRaftRuntime,
    {
        Self::with_applied_index(group_id, node_id, raft, app, LogIndex::ZERO)
    }

    /// Creates a group whose local applied floor is known at construction.
    ///
    /// Use this constructor on restart after reading
    /// [`ReplicatedStateMachine::applied_index`] from durable application
    /// state. The group still validates the state machine's reported applied
    /// index before every apply batch and poisons itself rather than replaying
    /// entries that the application already says are durable.
    ///
    /// The runtime's two memberships are read here to seed the group's
    /// membership comparison. A group reports transitions it *moves through*, so
    /// the configuration it opens over is never one of them; see
    /// [`RaftGroup::drain_membership_events`].
    ///
    /// The restart below reopens an application whose durable state reached
    /// index 2 and hands the group that same index as its floor. A runtime that
    /// then re-emits index 2 — the ordinary shape of a replayed recovery
    /// output — is refused before `apply_batch` runs, so the command cannot take
    /// effect a second time.
    ///
    /// ```
    /// # use std::{convert::Infallible, error::Error, fmt};
    /// # use rafter::{
    /// #     ClientProposalInput, Input, MembershipConfig, Node, ReplicationProgress, Role,
    /// # };
    /// # use rafter_runtime_api::PersistedRaftRuntime;
    /// # use rafter_app::state_machine::{ApplyBatch, ApplyResult, ReadBarrier, SnapshotSupport};
    /// use rafter::{LogIndex, NodeConfig, NodeId, Output, SharedPayload, Term};
    /// use rafter_app::group::{GroupFatalState, RaftGroup};
    /// use rafter_app::state_machine::ReplicatedStateMachine;
    /// #
    /// # /// A replicated counter; `Command` is how much to add.
    /// # #[derive(Debug, Default)]
    /// # struct Counter {
    /// #     applied_index: LogIndex,
    /// #     total: u64,
    /// # }
    /// #
    /// # impl ReplicatedStateMachine for Counter {
    /// #     type Command = u64;
    /// #     type CommandResult = u64;
    /// #     type Query = ();
    /// #     type QueryResult = u64;
    /// #     type Error = CounterError;
    /// #     const SNAPSHOT_SUPPORT: SnapshotSupport = SnapshotSupport::Unsupported;
    /// #     fn applied_index(&self) -> Result<LogIndex, Self::Error> { Ok(self.applied_index) }
    /// #     fn encode_command(&self, command: &u64) -> Result<Vec<u8>, Self::Error> {
    /// #         Ok(command.to_be_bytes().to_vec())
    /// #     }
    /// #     fn decode_command(&self, payload: &[u8]) -> Result<u64, Self::Error> {
    /// #         let bytes = <[u8; 8]>::try_from(payload).map_err(|_| CounterError)?;
    /// #         Ok(u64::from_be_bytes(bytes))
    /// #     }
    /// #     fn apply_batch(
    /// #         &mut self,
    /// #         batch: ApplyBatch<u64>,
    /// #     ) -> Result<Vec<ApplyResult<u64>>, Self::Error> {
    /// #         let mut results = Vec::new();
    /// #         for entry in batch.entries {
    /// #             self.total += entry.command;
    /// #             self.applied_index = entry.index;
    /// #             results.push(ApplyResult {
    /// #                 index: entry.index, term: entry.term,
    /// #                 result: self.total, local_proposal_id: entry.local_proposal_id,
    /// #             });
    /// #         }
    /// #         Ok(results)
    /// #     }
    /// #     fn read(&self, _q: (), _b: ReadBarrier) -> Result<u64, Self::Error> { Ok(self.total) }
    /// # }
    /// #
    /// # #[derive(Debug)] struct CounterError;
    /// # impl fmt::Display for CounterError {
    /// #     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.write_str("bad command") }
    /// # }
    /// # impl Error for CounterError {}
    /// #
    /// # /// A persisted runtime over a `Vec` medium; `DurableRaftNode` in the
    /// # /// `rafter-runtime` crate is the shipped durable one.
    /// # #[derive(Debug)]
    /// # struct LocalRuntime { node: Node }
    /// # impl PersistedRaftRuntime for LocalRuntime {
    /// #     type Error = Infallible;
    /// #     fn id(&self) -> NodeId { self.node.id() }
    /// #     fn leader_hint(&self) -> Option<NodeId> { self.node.leader_hint() }
    /// #     fn role(&self) -> Role { self.node.role() }
    /// #     fn current_term(&self) -> Term { self.node.current_term() }
    /// #     fn commit_index(&self) -> LogIndex { self.node.commit_index() }
    /// #     fn last_log_index(&self) -> LogIndex { self.node.last_log_index() }
    /// #     fn snapshot_index(&self) -> LogIndex { self.node.snapshot_index() }
    /// #     fn membership(&self) -> MembershipConfig { self.node.effective_membership() }
    /// #     fn committed_membership(&self) -> MembershipConfig { self.node.committed_membership() }
    /// #     fn replication(&self) -> Vec<ReplicationProgress> {
    /// #         self.node.leader_replication_progress()
    /// #     }
    /// #     fn term_at_index(&self, index: LogIndex) -> Option<Term> { self.node.term_at_index(index) }
    /// #     fn committed_application_index_through(&self, index: LogIndex) -> LogIndex {
    /// #         index.min(self.node.commit_index())
    /// #     }
    /// #     fn step(&mut self, input: Input) -> Result<Vec<Output>, Infallible> {
    /// #         Ok(self.node.step(input))
    /// #     }
    /// #     fn step_proposal_batch(
    /// #         &mut self,
    /// #         proposals: Vec<ClientProposalInput>,
    /// #     ) -> Result<Vec<Output>, Infallible> {
    /// #         Ok(self.node.step_proposal_batch(proposals))
    /// #     }
    /// #     fn step_batch(&mut self, inputs: Vec<Input>) -> Result<Vec<Output>, Infallible> {
    /// #         Ok(self.node.step_batch(inputs))
    /// #     }
    /// # }
    /// #
    /// let config = NodeConfig::new(NodeId(1), Vec::new(), 3).expect("valid raft config");
    /// let runtime = LocalRuntime { node: Node::new(config) };
    ///
    /// // The application reopened with 7 credits durably applied through index 2,
    /// // and says so. That declaration is the group's floor.
    /// let app = Counter { applied_index: LogIndex(2), total: 7 };
    /// let applied = app.applied_index().expect("durable application floor");
    /// let mut group = RaftGroup::with_applied_index("credits", NodeId(1), runtime, app, applied);
    ///
    /// // A committed entry at or below that floor is refused, not applied.
    /// let replayed = group.apply_raft_outputs(vec![Output::Apply {
    ///     index: LogIndex(2),
    ///     term: Term(1),
    ///     payload: SharedPayload::from(7_u64.to_be_bytes().to_vec()),
    ///     local_proposal_id: None,
    /// }]);
    ///
    /// assert!(replayed.is_err());
    /// assert_eq!(group.state_machine().total, 7, "the credit was not added twice");
    /// assert!(matches!(group.fatal_state(), GroupFatalState::Poisoned { .. }));
    /// ```
    #[must_use]
    pub fn with_applied_index(
        group_id: G,
        node_id: NodeId,
        raft: R,
        app: A,
        applied_index: LogIndex,
    ) -> Self
    where
        R: PersistedRaftRuntime,
    {
        let reported_membership = MembershipReportMark {
            effective: raft.membership(),
            committed: raft.committed_membership(),
            crossed: Vec::new(),
        };
        Self {
            group_id,
            node_id,
            raft,
            app,
            pending_proposals: BTreeMap::new(),
            last_seen_local_proposal_id: None,
            pending_reads: BTreeMap::new(),
            pending_query_reads: BTreeMap::new(),
            completed_query_reads: BTreeMap::new(),
            last_seen_read_id: None,
            last_applied_index: applied_index,
            fatal_state: GroupFatalState::Healthy,
            poison_cause: None,
            poisoned_waiters: PoisonedWaiters::default(),
            reported_membership,
        }
    }
}
