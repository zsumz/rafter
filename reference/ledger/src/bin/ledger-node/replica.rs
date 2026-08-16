//! One ledger replica: durable stores, the `rafter-app` group, and readiness.
//!
//! The recipe here is the documented one, in the documented order:
//!
//! 1. take exclusive ownership of the replica directory;
//! 2. open the durable application store and read its applied floor;
//! 3. recover the Raft runtime *through that floor*, so committed entries the
//!    application has already durably applied are not replayed into it;
//! 4. build the group at the same floor and consume the recovery outputs
//!    before anything else touches it; then
//! 5. serve clients only once the application has applied every command this
//!    replica knows to be committed.
//!
//! Step 5 is the readiness gate, and it is [`Replica::is_ready`]. The floor it
//! waits for is the group's committed *application* index, never the commit
//! index: elections and membership changes commit entries the state machine is
//! never told about, so a gate on the commit index would wait for an index the
//! application can never report and the replica would never serve anything.
//!
//! # Ownership order
//!
//! Ownership is acquired first, before the application store is opened, and
//! that ordering is the only thing standing between two live processes and one
//! directory. `rafter-storage` takes a real operating-system lock on the Raft
//! store directory; the ledger's own journal has no such lock, so this process
//! relies on holding the Raft lock for the whole life of the replica. The
//! consequence is stated plainly in `CONTRACT.md`: the invariant is "one
//! process owns one replica directory", enforced by one lock in a sibling
//! directory, and a second process is refused at step 1 rather than at the
//! journal it would have corrupted.

use std::{
    collections::BTreeMap,
    error::Error,
    fmt,
    path::{Path, PathBuf},
    time::Instant,
};

use rafter::{
    LocalProposalId, LogIndex, Message, NodeConfig, NodeId, ProposalRejection, ReadId, Role,
};
use rafter_app::{
    group::{GroupInput, GroupStepReport, RaftGroup, ReadReport},
    proposal::{Proposal, ProposalBegin, ProposalEvent},
    read::{ReadEvent, ReadOutcome as GroupReadOutcome, ReadRequest},
    state_machine::ReplicatedStateMachine,
    transport::PeerEnvelope,
};
use rafter_reference_ledger::{
    store::{LedgerStore, LedgerStoreError},
    ApplyDisposition, ApplyOutcome, Command, DurableLedgerStateMachine, LedgerConfig, LedgerQuery,
    LedgerQueryResult, LedgerResponse,
};
use rafter_runtime::DurableRaftNode;
use rafter_storage::{
    FileRaftHardStateStore, FileRaftLogSegment, FileRaftNodeStores, FileRaftSnapshotStore,
    OpenFileRaftNodeStoresError,
};

/// Caller-defined identity of the single ledger group each replica serves.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct LedgerGroupId(pub u64);

/// The one group every replica in this deployment serves.
pub const GROUP_ID: LedgerGroupId = LedgerGroupId(1);

type LedgerRuntime =
    DurableRaftNode<FileRaftHardStateStore, FileRaftLogSegment, FileRaftSnapshotStore>;
type LedgerGroup = RaftGroup<LedgerGroupId, DurableLedgerStateMachine, LedgerRuntime>;
type LedgerReport = GroupStepReport<LedgerGroupId, ApplyOutcome>;

/// Why a replica could not be opened.
#[derive(Debug)]
pub enum OpenError {
    /// Another live process owns this replica directory.
    ///
    /// This is the ownership refusal, kept separate from every other failure
    /// because it is the one a restarting replica may legitimately wait out.
    DirectoryOwned { directory: PathBuf },
    /// A durable store could not be opened or recovered.
    Store { detail: String },
    /// The application journal holds a region this build cannot read.
    ///
    /// This is kept apart from every other store failure because it is the one
    /// a human has to answer. The journal is intact enough to open at a shorter
    /// history and no further, and shortening it destroys transactions this
    /// replica may already have acknowledged, so the store refuses and says so
    /// rather than deciding for an operator. Restarting will not help; running
    /// with `--repair-app-store true` discards the unreadable region and reports
    /// what it cost.
    ApplicationStoreNeedsRepair { detail: String },
    /// The Raft runtime could not recover through the application's floor.
    Runtime { detail: String },
    /// The replica directory could not be created.
    Io {
        operation: &'static str,
        path: PathBuf,
        source: std::io::Error,
    },
    /// The static configuration this replica was given is not a valid cluster.
    Config { detail: String },
}

impl fmt::Display for OpenError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DirectoryOwned { directory } => write!(
                formatter,
                "another process owns the replica directory {}",
                directory.display()
            ),
            Self::Store { detail } => write!(formatter, "durable store failed: {detail}"),
            Self::ApplicationStoreNeedsRepair { detail } => write!(
                formatter,
                "the application journal needs an operator decision and this replica will not \
                 serve until it gets one: {detail}. Restarting will not change it. Running with \
                 --repair-app-store true discards the unreadable region and reports the offset \
                 and byte count it discarded, which is an upper bound on the loss and not a \
                 count of transactions — frames past an unreadable one cannot be located, so \
                 nothing can count them"
            ),
            Self::Runtime { detail } => write!(formatter, "raft recovery failed: {detail}"),
            Self::Io {
                operation,
                path,
                source,
            } => write!(
                formatter,
                "could not {operation} at {}: {source}",
                path.display()
            ),
            Self::Config { detail } => write!(formatter, "invalid replica configuration: {detail}"),
        }
    }
}

impl Error for OpenError {}

/// Terminal outcome of one replicated command, as a client sees it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SubmitOutcome {
    /// The command committed and applied; this is its replicated result.
    Applied {
        disposition: ApplyDisposition,
        response: LedgerResponse,
    },
    /// The local node refused the command before replication, so it provably
    /// never entered the log.
    NotCommitted {
        reason: ProposalRejection,
        leader_hint: Option<NodeId>,
    },
    /// The outcome is unknown; the client must retry the same request identity.
    Unknown { reason: String },
}

/// Terminal outcome of one linearizable query.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryOutcome {
    /// The barrier was granted and the query ran against fresh state.
    Ready(LedgerQueryResult),
    /// The query returned no value; it constrains no ordering.
    Abandoned { reason: String },
}

/// A client request the replica has started and not yet answered.
#[derive(Debug)]
struct PendingSubmit {
    ticket: u64,
    deadline: Instant,
}

#[derive(Debug)]
struct PendingQuery {
    ticket: u64,
    query: LedgerQuery,
    deadline: Instant,
}

/// One answer the replica owes a client, addressed by the client's ticket.
#[derive(Debug)]
pub enum Answer {
    /// A replicated command reached a terminal outcome.
    Submit { ticket: u64, outcome: SubmitOutcome },
    /// A linearizable query reached a terminal outcome.
    Query { ticket: u64, outcome: QueryOutcome },
}

/// One ledger replica driven by a consumer-owned process loop.
#[derive(Debug)]
pub struct Replica {
    node_id: NodeId,
    group: LedgerGroup,
    ready: bool,
    outbound: Vec<PeerEnvelope<LedgerGroupId>>,
    pending_submits: BTreeMap<LocalProposalId, PendingSubmit>,
    pending_queries: BTreeMap<ReadId, PendingQuery>,
    submit_outcomes: BTreeMap<LocalProposalId, SubmitOutcome>,
    query_failures: BTreeMap<ReadId, QueryOutcome>,
    answers: Vec<Answer>,
    next_local_proposal_id: u64,
    next_read_id: u64,
}

impl Replica {
    /// Opens one replica's durable state and recovers it.
    ///
    /// # Errors
    ///
    /// Returns [`OpenError::DirectoryOwned`] when another process holds the
    /// replica directory, and otherwise an error naming the store, runtime,
    /// configuration, or filesystem operation that failed.
    pub fn open(
        node_dir: &Path,
        node_id: NodeId,
        peers: &[NodeId],
        election_timeout_ticks: u64,
        ledger_config: LedgerConfig,
        repair_app_store: bool,
    ) -> Result<Self, OpenError> {
        let raft_dir = node_dir.join("raft");
        let app_dir = node_dir.join("app");
        for directory in [node_dir, &raft_dir, &app_dir] {
            std::fs::create_dir_all(directory).map_err(|source| OpenError::Io {
                operation: "create a replica directory",
                path: directory.to_path_buf(),
                source,
            })?;
        }

        // Ownership first. Everything below this line assumes this process is
        // the only one publishing into this directory.
        let stores = FileRaftNodeStores::open(&raft_dir).map_err(|error| match error {
            OpenFileRaftNodeStoresError::AlreadyOpen { directory } => {
                OpenError::DirectoryOwned { directory }
            }
            other => OpenError::Store {
                detail: other.to_string(),
            },
        })?;
        let (hard_state, log_segment, snapshot_store) = stores.into_parts();

        // Opening is not a read, and this comment used to say it was.
        //
        // Two things shorten this journal. The repair path below discards a
        // region recovery positively cannot read, of any length, and takes an
        // explicit flag because the transactions in it may be ones this replica
        // already acknowledged to a client. The plain `open` beside it discards
        // a zero-filled tail, and those bytes may equally have been an
        // acknowledged transaction — a zeroed sector over the last frames
        // leaves exactly that shape. No flag gates the second one; the store's
        // `TornTail::is_truncatable_residue` argues why, and this process's
        // obligation is to announce it, which is what the `possibly_committed=`
        // field below is for.
        let opened = if repair_app_store {
            LedgerStore::open_and_repair(&app_dir, ledger_config)
        } else {
            LedgerStore::open(&app_dir, ledger_config)
        };
        let store = opened.map_err(|error| match error {
            LedgerStoreError::UnreadableFrame { .. } => OpenError::ApplicationStoreNeedsRepair {
                detail: error.to_string(),
            },
            other => OpenError::Store {
                detail: other.to_string(),
            },
        })?;

        // The recovery report is consumed rather than dropped. Residue from an
        // interrupted transaction is ordinary after a kill and is announced so
        // an operator can see it; a repair is announced because it is the
        // largest thing this process does that can lose acknowledged work.
        //
        // `possibly_committed=` is the second such thing, and it is on the
        // ordinary `RECOVERED` line rather than a line of its own because that
        // is where a restart after a power cut actually lands. Zero on that
        // field is the common case and says the residue was proved uncommitted;
        // non-zero says this restart may have deleted an acknowledged
        // transaction, which is a sentence no other output of this process
        // would have let an operator reach.
        //
        // Creating the journal gets its own line rather than sharing one. It is
        // the ordinary first act of a replica that has never run, and it is
        // also exactly what a replica whose journal was deleted does — it opens
        // at applied index zero and serves an empty ledger. Nothing inside this
        // store can tell those apart, because both arrive as an absent file, so
        // the announcement is where the difference becomes visible to the only
        // party that can judge it: creation on a first boot is expected, and
        // creation on a restart means the durable state is gone.
        let recovery = *store.recovery();
        if let Some(repair) = recovery.repair() {
            crate::emit(&format!("REPAIRED {} {repair}", node_id.0));
        } else if recovery.created() {
            crate::emit(&format!("CREATED {} {}", node_id.0, app_dir.display()));
        } else if !recovery.is_clean() {
            crate::emit(&format!(
                "RECOVERED {} frames={} discarded={} possibly_committed={} swept={}",
                node_id.0,
                recovery.committed_frames(),
                recovery.discarded_bytes(),
                recovery.discarded_without_proof(),
                recovery.removed_staged_bytes().unwrap_or(0)
            ));
        }

        // The machine is handed the same `snapshots` directory the runtime's
        // own snapshot store owns, because a Raft-driven install gives the
        // application a descriptor rather than bytes and this is where the
        // bytes have already been promoted. It is a read-only second view: the
        // runtime below still owns the writing handle.
        let app = DurableLedgerStateMachine::new(store, raft_dir.join("snapshots"));
        let applied_index = app.applied_index().map_err(|error| OpenError::Store {
            detail: error.to_string(),
        })?;

        let config =
            NodeConfig::new(node_id, peers.to_vec(), election_timeout_ticks).map_err(|error| {
                OpenError::Config {
                    detail: format!("{error:?}"),
                }
            })?;
        let recovered = DurableRaftNode::recover_with_storage_and_snapshot_store_applied_through(
            config,
            hard_state,
            log_segment,
            snapshot_store,
            applied_index,
        )
        .map_err(|error| OpenError::Runtime {
            detail: format!("{error:?}"),
        })?;
        let (raft, recovery_outputs) = recovered.into_parts();

        let mut replica = Self {
            node_id,
            group: RaftGroup::with_applied_index(GROUP_ID, node_id, raft, app, applied_index),
            ready: false,
            outbound: Vec::new(),
            pending_submits: BTreeMap::new(),
            pending_queries: BTreeMap::new(),
            submit_outcomes: BTreeMap::new(),
            query_failures: BTreeMap::new(),
            answers: Vec::new(),
            next_local_proposal_id: 1,
            next_read_id: 1,
        };
        // The recovery outputs are consumed before anything else touches the
        // group, which is what makes the applied floor below meaningful — and
        // through the ordered operation, so a replica that opened below its own
        // snapshot boundary installs that snapshot before the committed suffix
        // lands on top of it rather than after.
        let report = replica
            .group
            .apply_recovery_outputs(recovery_outputs)
            .map_err(|error| OpenError::Runtime {
                detail: error.to_string(),
            })?;
        replica.absorb(report);
        replica.refresh_readiness();
        Ok(replica)
    }

    /// Whether this replica has applied everything it knows to be committed.
    pub const fn is_ready(&self) -> bool {
        self.ready
    }

    /// Returns this replica's applied index.
    pub fn applied_index(&self) -> LogIndex {
        self.group
            .state_machine()
            .applied_index()
            .unwrap_or(LogIndex::ZERO)
    }

    /// Returns the index this replica must apply through to be current.
    pub fn committed_application_index(&self) -> LogIndex {
        self.group.committed_application_index()
    }

    /// Returns this replica's role, term, and leader hint.
    pub fn status(&self) -> (Role, u64, Option<u64>) {
        let metrics = self.group.metrics();
        (
            metrics.role,
            metrics.term.0,
            metrics.leader_hint.map(|leader| leader.0),
        )
    }

    /// Advances one tick.
    ///
    /// # Errors
    ///
    /// Returns an error when the group refuses the step, which for this
    /// application means the durable backend could not commit a transaction.
    pub fn tick(&mut self) -> Result<(), String> {
        self.step(GroupInput::Tick)
    }

    /// Delivers one peer message.
    ///
    /// # Errors
    ///
    /// As [`Replica::tick`].
    pub fn deliver(&mut self, from: NodeId, message: Message) -> Result<(), String> {
        self.step(GroupInput::PeerMessage {
            envelope: PeerEnvelope {
                group_id: GROUP_ID,
                from,
                to: self.node_id,
                message,
            },
        })
    }

    /// Starts one replicated command on behalf of a client.
    ///
    /// # Errors
    ///
    /// As [`Replica::tick`].
    pub fn submit(
        &mut self,
        ticket: u64,
        command: Command,
        deadline: Instant,
    ) -> Result<(), String> {
        let local_proposal_id = LocalProposalId(self.next_local_proposal_id);
        self.next_local_proposal_id += 1;
        let started = self
            .group
            .begin_proposal(Proposal {
                local_proposal_id,
                client_request_id: None,
                command,
            })
            .map_err(|error| error.to_string())?;
        let immediate = immediate_outcome(&started.begin);
        self.absorb(started.report);
        match immediate.or_else(|| self.submit_outcomes.remove(&local_proposal_id)) {
            Some(outcome) => self.answers.push(Answer::Submit { ticket, outcome }),
            None => {
                self.pending_submits
                    .insert(local_proposal_id, PendingSubmit { ticket, deadline });
            }
        }
        Ok(())
    }

    /// Starts one linearizable query on behalf of a client.
    ///
    /// # Errors
    ///
    /// As [`Replica::tick`].
    pub fn query(
        &mut self,
        ticket: u64,
        query: LedgerQuery,
        deadline: Instant,
    ) -> Result<(), String> {
        let read_id = ReadId(self.next_read_id);
        self.next_read_id += 1;
        self.pending_queries.insert(
            read_id,
            PendingQuery {
                ticket,
                query,
                deadline,
            },
        );
        self.attempt_query(read_id)
    }

    /// Answers one query from this replica's own applied state.
    ///
    /// This is a local read: it may be stale, and it names no barrier.
    ///
    /// # Errors
    ///
    /// Returns an error when the local read itself fails.
    pub fn local_query(&mut self, query: LedgerQuery) -> Result<LedgerQueryResult, String> {
        let ReadReport { outcome, report } = self
            .group
            .read(ReadRequest::Local {
                group_id: GROUP_ID,
                query,
                min_applied_index: None,
            })
            .map_err(|error| error.to_string())?;
        self.absorb(report);
        match outcome {
            GroupReadOutcome::Ready { result, .. } => Ok(result),
            other => Err(format!("{other:?}")),
        }
    }

    /// Retries every in-flight barrier and expires everything past its deadline.
    ///
    /// # Errors
    ///
    /// As [`Replica::tick`].
    pub fn service_pending(&mut self, now: Instant) -> Result<(), String> {
        let read_ids: Vec<ReadId> = self.pending_queries.keys().copied().collect();
        for read_id in read_ids {
            self.attempt_query(read_id)?;
        }

        let expired_submits: Vec<LocalProposalId> = self
            .pending_submits
            .iter()
            .filter(|(_, pending)| now >= pending.deadline)
            .map(|(id, _)| *id)
            .collect();
        for local_proposal_id in expired_submits {
            if let Some(pending) = self.pending_submits.remove(&local_proposal_id) {
                // A client that stops waiting observes an unknown outcome. The
                // proposal may still commit, so nothing weaker than `Unknown`
                // is honest here.
                self.answers.push(Answer::Submit {
                    ticket: pending.ticket,
                    outcome: SubmitOutcome::Unknown {
                        reason: String::from("deadline"),
                    },
                });
            }
        }

        let expired_queries: Vec<ReadId> = self
            .pending_queries
            .iter()
            .filter(|(_, pending)| now >= pending.deadline)
            .map(|(id, _)| *id)
            .collect();
        for read_id in expired_queries {
            if let Some(pending) = self.pending_queries.remove(&read_id) {
                self.group.cancel_read(read_id);
                self.answers.push(Answer::Query {
                    ticket: pending.ticket,
                    outcome: QueryOutcome::Abandoned {
                        reason: String::from("deadline"),
                    },
                });
            }
        }
        Ok(())
    }

    /// Takes every peer envelope this replica owes the network.
    pub fn take_outbound(&mut self) -> Vec<PeerEnvelope<LedgerGroupId>> {
        std::mem::take(&mut self.outbound)
    }

    /// Takes every answer this replica owes its clients.
    pub fn take_answers(&mut self) -> Vec<Answer> {
        std::mem::take(&mut self.answers)
    }

    /// Fails every waiting client with an unknown outcome.
    ///
    /// A replica that is stopping has not learned anything about its in-flight
    /// proposals, so it must not claim they did not commit.
    pub fn abandon_waiters(&mut self, reason: &str) {
        for (_, pending) in std::mem::take(&mut self.pending_submits) {
            self.answers.push(Answer::Submit {
                ticket: pending.ticket,
                outcome: SubmitOutcome::Unknown {
                    reason: reason.to_string(),
                },
            });
        }
        for (_, pending) in std::mem::take(&mut self.pending_queries) {
            self.answers.push(Answer::Query {
                ticket: pending.ticket,
                outcome: QueryOutcome::Abandoned {
                    reason: reason.to_string(),
                },
            });
        }
    }

    fn step(&mut self, input: GroupInput<LedgerGroupId, Command>) -> Result<(), String> {
        let report = self.group.step(input).map_err(|error| error.to_string())?;
        self.absorb(report);
        Ok(())
    }

    fn attempt_query(&mut self, read_id: ReadId) -> Result<(), String> {
        let Some(pending) = self.pending_queries.get(&read_id) else {
            return Ok(());
        };
        let query = pending.query;
        // A terminal read event can arrive in the report of an unrelated tick
        // or delivery. The group drops its waiter with that event, so the
        // answer is taken from here rather than by asking again.
        if let Some(outcome) = self.query_failures.remove(&read_id) {
            self.finish_query(read_id, outcome);
            return Ok(());
        }

        let ReadReport { outcome, report } = self
            .group
            .read(ReadRequest::Linearizable {
                group_id: GROUP_ID,
                read_id,
                query,
                min_applied_index: None,
                context: Vec::new(),
            })
            .map_err(|error| error.to_string())?;
        self.absorb(report);
        if let Some(outcome) = self.query_failures.remove(&read_id) {
            self.finish_query(read_id, outcome);
            return Ok(());
        }
        match outcome {
            GroupReadOutcome::Ready { result, .. } => {
                self.finish_query(read_id, QueryOutcome::Ready(result));
            }
            // Still in flight, or this replica has not applied through the
            // barrier yet. Either way the contract is to keep driving and retry
            // with the same read ID, freshness, and context.
            GroupReadOutcome::Pending { .. }
            | GroupReadOutcome::LinearizableFreshnessUnavailable { .. } => {}
            other => {
                self.finish_query(
                    read_id,
                    QueryOutcome::Abandoned {
                        reason: format!("{other:?}"),
                    },
                );
            }
        }
        Ok(())
    }

    fn finish_query(&mut self, read_id: ReadId, outcome: QueryOutcome) {
        if let Some(pending) = self.pending_queries.remove(&read_id) {
            self.answers.push(Answer::Query {
                ticket: pending.ticket,
                outcome,
            });
        }
    }

    fn absorb(&mut self, report: LedgerReport) {
        let LedgerReport {
            peer_messages,
            proposal_events,
            read_events,
            ..
        } = report;
        self.outbound.extend(peer_messages);
        for event in &proposal_events {
            self.record_proposal_event(event);
        }
        for event in &read_events {
            self.record_read_event(event);
        }
        self.refresh_readiness();
    }

    /// Recomputes the readiness gate.
    ///
    /// Readiness is one-way. A replica that has caught up once and then falls
    /// behind a newly committed entry is a normal follower, not a replica in
    /// recovery, and flapping the gate would make a healthy cluster refuse
    /// service every time a write landed.
    fn refresh_readiness(&mut self) {
        if !self.ready && self.applied_index() >= self.committed_application_index() {
            self.ready = true;
        }
    }

    fn record_proposal_event(&mut self, event: &ProposalEvent<ApplyOutcome>) {
        let (local_proposal_id, outcome) = match event {
            ProposalEvent::Applied {
                local_proposal_id,
                result,
                ..
            } => (
                *local_proposal_id,
                SubmitOutcome::Applied {
                    disposition: result.disposition,
                    response: result.response.clone(),
                },
            ),
            // The app layer emits this only from the pre-append admission
            // check, so the command never entered this node's log and never
            // left it. That is the contract's provable-refusal criterion.
            ProposalEvent::Rejected {
                local_proposal_id,
                reason,
                leader_hint,
            } => (
                *local_proposal_id,
                SubmitOutcome::NotCommitted {
                    reason: reason.clone(),
                    leader_hint: *leader_hint,
                },
            ),
            ProposalEvent::UnknownOutcome {
                local_proposal_id,
                reason,
                ..
            } => (
                *local_proposal_id,
                SubmitOutcome::Unknown {
                    reason: format!("{reason:?}"),
                },
            ),
            _ => return,
        };
        match self.pending_submits.remove(&local_proposal_id) {
            Some(pending) => self.answers.push(Answer::Submit {
                ticket: pending.ticket,
                outcome,
            }),
            // A proposal can reach a terminal event inside the very step that
            // started it, before the caller has recorded it as pending.
            None => {
                self.submit_outcomes.insert(local_proposal_id, outcome);
            }
        }
    }

    fn record_read_event(&mut self, event: &ReadEvent<LedgerGroupId>) {
        let (read_id, reason) = match event {
            ReadEvent::Rejected {
                read_id, reason, ..
            } => (*read_id, format!("{reason:?}")),
            ReadEvent::Canceled {
                read_id, reason, ..
            } => (*read_id, format!("{reason:?}")),
            _ => return,
        };
        self.query_failures
            .insert(read_id, QueryOutcome::Abandoned { reason });
    }
}

/// Returns the terminal outcome a proposal already reached while starting.
fn immediate_outcome(begin: &ProposalBegin<LedgerGroupId, ApplyOutcome>) -> Option<SubmitOutcome> {
    match begin {
        ProposalBegin::Completed { result, .. } => Some(SubmitOutcome::Applied {
            disposition: result.disposition,
            response: result.response.clone(),
        }),
        ProposalBegin::Rejected {
            reason,
            leader_hint,
            ..
        } => Some(SubmitOutcome::NotCommitted {
            reason: reason.clone(),
            leader_hint: *leader_hint,
        }),
        ProposalBegin::UnknownOutcome { .. } => Some(SubmitOutcome::Unknown {
            reason: String::from("proposal begin reported an unknown outcome"),
        }),
        _ => None,
    }
}
