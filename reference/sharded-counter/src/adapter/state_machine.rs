use std::{collections::BTreeMap, error::Error, fmt};

use rafter::{InMemorySnapshotChunkSource, LogIndex, RaftSnapshot};
use rafter_app::state_machine::{
    ApplicationSnapshot, ApplicationSnapshotError, ApplyBatch, ApplyResult, ReadBarrier,
    ReplicatedStateMachine, SnapshotSupport,
};

use crate::{
    ClientId, CounterCommand, CounterRejection, CounterResult, RequestFingerprint, RequestIdentity,
    Sequence, SessionEpoch,
};

use super::codec;

/// Replicated counter command owned by the consumer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplicatedCounterCommand {
    /// Establishes or advances a replicated client session.
    OpenSession {
        /// Bounded client slot.
        client_id: ClientId,
        /// Requested session generation.
        epoch: SessionEpoch,
    },
    /// Applies or replays one counter request.
    Counter {
        /// Exact request identity.
        request: RequestIdentity,
        /// Counter operation.
        command: CounterCommand,
    },
    /// Models an irrecoverable consumer apply fault.
    ///
    /// The contract already exposes faulty work as a consumer-owned shape.
    /// This variant lets the real adapter prove the corresponding Raft-group
    /// poison and isolation path without adding a Rafter test hook.
    Faulty,
    /// Models the ordinary session-capacity invariant error for quarantine tests.
    ///
    /// Unlike [`Self::Faulty`], this returns the invariant error variant that a
    /// corrupt old snapshot or incompatible historical writer could expose.
    CapacityFault,
}

/// Replicated session outcome.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionApplyResult {
    /// A vacant client slot opened.
    Opened,
    /// The exact epoch was already active.
    AlreadyOpen,
    /// A greater epoch replaced the old session and its dedup cache.
    Replaced,
}

/// Refusal reached after a command was already replicated.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CounterApplyRejection {
    /// The command names a client slot outside the configured range.
    ClientOutOfRange,
    /// The request did not name an open client slot.
    SessionNotOpen,
    /// The request named an older session generation.
    StaleSession { current: SessionEpoch },
    /// The request named a generation the slot has not reached.
    FutureSession { current: SessionEpoch },
    /// The request fingerprint did not describe its command.
    FingerprintMismatch,
    /// The sequence is older than the cached completion.
    StaleSequence { highest: Sequence },
    /// The sequence skipped the next admissible value.
    SequenceGap { expected: Sequence },
    /// The identity was reused for different command content.
    ConflictingRetry,
}

/// Result emitted by the replicated counter state machine.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CounterApplyResult {
    /// Session establishment completed.
    Session(SessionApplyResult),
    /// A counter request completed or replayed.
    Counter(CounterResult),
    /// A replicated request was refused without changing counter state.
    Rejected(CounterApplyRejection),
}

/// Authoritative decision made from one quorum-confirmed application view.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CounterAdmissionDecision {
    /// The operation is new and may proceed to durable reservation.
    Proceed,
    /// The exact session epoch is already open.
    SessionAlreadyOpen,
    /// The exact counter request already completed.
    CounterReplay(CounterResult),
    /// The operation is deterministically refused without admission.
    Rejected(CounterApplyRejection),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct Completed {
    pub(super) sequence: Sequence,
    pub(super) command: CounterCommand,
    pub(super) result: CounterResult,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct Session {
    pub(super) epoch: SessionEpoch,
    pub(super) completed: Option<Completed>,
}

/// Read-only application state reconstructed from live state or a snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CounterStateView {
    /// Highest applied Raft log index.
    pub applied_index: LogIndex,
    /// Current counter value.
    pub value: i64,
    /// Replicated session slots in client order.
    pub sessions: Vec<CounterSessionView>,
}

/// One replicated session in an application-state view.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CounterSessionView {
    /// Client slot.
    pub client_id: ClientId,
    /// Active session generation.
    pub epoch: SessionEpoch,
    /// Highest cached completion, when one exists.
    pub completed: Option<CounterCompletedView>,
}

/// One cached request completion in an application-state view.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CounterCompletedView {
    /// Completed request sequence.
    pub sequence: Sequence,
    /// Exact command bound to the request identity.
    pub command: CounterCommand,
    /// Cached deterministic result.
    pub result: CounterResult,
}

/// Small replicated counter with bounded session state and snapshot support.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CounterStateMachine {
    applied_index: LogIndex,
    value: i64,
    max_sessions: usize,
    sessions: BTreeMap<ClientId, Session>,
    promoted_payloads: InMemorySnapshotChunkSource,
}

impl CounterStateMachine {
    /// Creates an empty state machine with a fixed replicated-session bound.
    ///
    /// # Panics
    ///
    /// Panics when `max_sessions` is zero or cannot fit in the bounded snapshot
    /// representation.
    #[must_use]
    pub fn new(max_sessions: usize) -> Self {
        assert!(max_sessions != 0, "session bound must be nonzero");
        assert!(
            codec::supports_session_bound(max_sessions),
            "session bound exceeds the snapshot representation"
        );
        Self {
            applied_index: LogIndex::ZERO,
            value: 0,
            max_sessions,
            sessions: BTreeMap::new(),
            promoted_payloads: InMemorySnapshotChunkSource::new(),
        }
    }

    /// Restores a fresh state machine from one bounded application snapshot.
    ///
    /// # Errors
    ///
    /// Returns the snapshot codec, identity, or capacity failure.
    ///
    /// # Panics
    ///
    /// Panics when `max_sessions` is zero or exceeds the bounded snapshot
    /// representation.
    pub fn from_snapshot(
        max_sessions: usize,
        snapshot: ApplicationSnapshot,
    ) -> Result<Self, ApplicationSnapshotError<CounterStateMachineError>> {
        let mut state = Self::new(max_sessions);
        state.install_snapshot(snapshot)?;
        Ok(state)
    }

    /// Returns a complete read-only view of snapshot-relevant state.
    #[must_use]
    pub fn view(&self) -> CounterStateView {
        CounterStateView {
            applied_index: self.applied_index,
            value: self.value,
            sessions: self
                .sessions
                .iter()
                .map(|(client_id, session)| CounterSessionView {
                    client_id: *client_id,
                    epoch: session.epoch,
                    completed: session.completed.map(|completed| CounterCompletedView {
                        sequence: completed.sequence,
                        command: completed.command,
                        result: completed.result,
                    }),
                })
                .collect(),
        }
    }

    /// Decides one client operation from the current replicated session state.
    ///
    /// The caller must establish whatever freshness guarantee its API needs.
    /// The process fixture invokes this only after a granted linearizable read
    /// barrier; deterministic adapters call it from their already-ordered
    /// state transition.
    #[must_use]
    pub fn admission_decision(
        &self,
        command: ReplicatedCounterCommand,
    ) -> CounterAdmissionDecision {
        match command {
            ReplicatedCounterCommand::OpenSession { client_id, epoch } => {
                if !self.client_in_range(client_id) {
                    return CounterAdmissionDecision::Rejected(
                        CounterApplyRejection::ClientOutOfRange,
                    );
                }
                let Some(session) = self.sessions.get(&client_id) else {
                    return CounterAdmissionDecision::Proceed;
                };
                match epoch.cmp(&session.epoch) {
                    std::cmp::Ordering::Less => {
                        CounterAdmissionDecision::Rejected(CounterApplyRejection::StaleSession {
                            current: session.epoch,
                        })
                    }
                    std::cmp::Ordering::Equal => CounterAdmissionDecision::SessionAlreadyOpen,
                    std::cmp::Ordering::Greater => CounterAdmissionDecision::Proceed,
                }
            }
            ReplicatedCounterCommand::Counter { request, command } => {
                if !self.client_in_range(request.client_id) {
                    return CounterAdmissionDecision::Rejected(
                        CounterApplyRejection::ClientOutOfRange,
                    );
                }
                let Some(session) = self.sessions.get(&request.client_id) else {
                    return CounterAdmissionDecision::Rejected(
                        CounterApplyRejection::SessionNotOpen,
                    );
                };
                if request.session_epoch < session.epoch {
                    return CounterAdmissionDecision::Rejected(
                        CounterApplyRejection::StaleSession {
                            current: session.epoch,
                        },
                    );
                }
                if request.session_epoch > session.epoch {
                    return CounterAdmissionDecision::Rejected(
                        CounterApplyRejection::FutureSession {
                            current: session.epoch,
                        },
                    );
                }
                if request.fingerprint != RequestFingerprint::of(&command) {
                    return CounterAdmissionDecision::Rejected(
                        CounterApplyRejection::FingerprintMismatch,
                    );
                }
                if let Some(completed) = session.completed {
                    if request.sequence < completed.sequence {
                        return CounterAdmissionDecision::Rejected(
                            CounterApplyRejection::StaleSequence {
                                highest: completed.sequence,
                            },
                        );
                    }
                    if request.sequence == completed.sequence {
                        return if command == completed.command {
                            CounterAdmissionDecision::CounterReplay(completed.result)
                        } else {
                            CounterAdmissionDecision::Rejected(
                                CounterApplyRejection::ConflictingRetry,
                            )
                        };
                    }
                }
                let expected = match session.completed {
                    Some(completed) => {
                        let Some(expected) = completed.sequence.successor() else {
                            return CounterAdmissionDecision::Rejected(
                                CounterApplyRejection::StaleSequence {
                                    highest: completed.sequence,
                                },
                            );
                        };
                        expected
                    }
                    None => Sequence::first(),
                };
                if request.sequence == expected {
                    CounterAdmissionDecision::Proceed
                } else {
                    CounterAdmissionDecision::Rejected(CounterApplyRejection::SequenceGap {
                        expected,
                    })
                }
            }
            ReplicatedCounterCommand::Faulty | ReplicatedCounterCommand::CapacityFault => {
                CounterAdmissionDecision::Proceed
            }
        }
    }

    fn client_in_range(&self, client_id: ClientId) -> bool {
        usize::try_from(client_id.get()).is_ok_and(|id| id < self.max_sessions)
    }

    /// Registers bytes already promoted by the caller's Rafter snapshot store.
    ///
    /// A Raft-driven install carries only a descriptor into the application
    /// callback. The embedding resolves that descriptor from its durable
    /// snapshot store and registers the exact bounded payload here before the
    /// group applies the snapshot output.
    ///
    /// # Errors
    ///
    /// Returns a length mismatch when the supplied bytes contradict the
    /// snapshot descriptor.
    pub fn register_promoted_snapshot(
        &mut self,
        snapshot: &RaftSnapshot,
        payload: Vec<u8>,
    ) -> Result<(), CounterStateMachineError> {
        self.promoted_payloads
            .insert(snapshot, payload)
            .map_err(|_| CounterStateMachineError::SnapshotPayloadLengthMismatch)
    }

    fn apply_open_session(
        &mut self,
        client_id: ClientId,
        epoch: SessionEpoch,
    ) -> Result<CounterApplyResult, CounterStateMachineError> {
        match self.admission_decision(ReplicatedCounterCommand::OpenSession { client_id, epoch }) {
            CounterAdmissionDecision::Rejected(rejection) => {
                Ok(CounterApplyResult::Rejected(rejection))
            }
            CounterAdmissionDecision::SessionAlreadyOpen => {
                Ok(CounterApplyResult::Session(SessionApplyResult::AlreadyOpen))
            }
            CounterAdmissionDecision::Proceed => {
                let Some(session) = self.sessions.get_mut(&client_id) else {
                    if self.sessions.len() >= self.max_sessions {
                        return Err(CounterStateMachineError::SessionCapacity);
                    }
                    self.sessions.insert(
                        client_id,
                        Session {
                            epoch,
                            completed: None,
                        },
                    );
                    return Ok(CounterApplyResult::Session(SessionApplyResult::Opened));
                };
                session.epoch = epoch;
                session.completed = None;
                Ok(CounterApplyResult::Session(SessionApplyResult::Replaced))
            }
            CounterAdmissionDecision::CounterReplay(_) => {
                unreachable!("a session preflight cannot replay a counter")
            }
        }
    }

    fn apply_counter(
        &mut self,
        request: RequestIdentity,
        command: CounterCommand,
    ) -> CounterApplyResult {
        match self.admission_decision(ReplicatedCounterCommand::Counter { request, command }) {
            CounterAdmissionDecision::Rejected(rejection) => {
                return CounterApplyResult::Rejected(rejection);
            }
            CounterAdmissionDecision::CounterReplay(result) => {
                return CounterApplyResult::Counter(result);
            }
            CounterAdmissionDecision::Proceed => {}
            CounterAdmissionDecision::SessionAlreadyOpen => {
                unreachable!("a counter preflight cannot replay a session")
            }
        }

        let session = self
            .sessions
            .get_mut(&request.client_id)
            .expect("preflight proved that the session is open");
        let result = match command {
            CounterCommand::Add { delta } => match self.value.checked_add(delta.get()) {
                Some(value) => {
                    self.value = value;
                    CounterResult::Added { value }
                }
                None => CounterResult::Rejected(CounterRejection::CounterOverflow {
                    current: self.value,
                }),
            },
            CounterCommand::Read => CounterResult::Value { value: self.value },
        };
        session.completed = Some(Completed {
            sequence: request.sequence,
            command,
            result,
        });
        CounterApplyResult::Counter(result)
    }
}

impl ReplicatedStateMachine for CounterStateMachine {
    type Command = ReplicatedCounterCommand;
    type CommandResult = CounterApplyResult;
    type Query = ();
    type QueryResult = i64;
    type Error = CounterStateMachineError;

    const SNAPSHOT_SUPPORT: SnapshotSupport = SnapshotSupport::Supported;

    fn applied_index(&self) -> Result<LogIndex, Self::Error> {
        Ok(self.applied_index)
    }

    fn encode_command(&self, command: &Self::Command) -> Result<Vec<u8>, Self::Error> {
        codec::encode_command(command)
    }

    fn decode_command(&self, payload: &[u8]) -> Result<Self::Command, Self::Error> {
        codec::decode_command(payload)
    }

    fn apply_batch(
        &mut self,
        batch: ApplyBatch<Self::Command>,
    ) -> Result<Vec<ApplyResult<Self::CommandResult>>, Self::Error> {
        let mut results = Vec::with_capacity(batch.entries.len());
        for entry in batch.entries {
            let result = match entry.command {
                ReplicatedCounterCommand::OpenSession { client_id, epoch } => {
                    self.apply_open_session(client_id, epoch)?
                }
                ReplicatedCounterCommand::Counter { request, command } => {
                    self.apply_counter(request, command)
                }
                ReplicatedCounterCommand::Faulty => {
                    return Err(CounterStateMachineError::InjectedFault);
                }
                ReplicatedCounterCommand::CapacityFault => {
                    return Err(CounterStateMachineError::SessionCapacity);
                }
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

    fn read(&self, (): (), barrier: ReadBarrier) -> Result<Self::QueryResult, Self::Error> {
        if self.applied_index < barrier.required_applied_index {
            return Err(CounterStateMachineError::ReadBarrierNotReached);
        }
        Ok(self.value)
    }

    fn build_snapshot(
        &mut self,
        at: LogIndex,
    ) -> Result<ApplicationSnapshot, ApplicationSnapshotError<Self::Error>> {
        if at != self.applied_index {
            return Err(CounterStateMachineError::SnapshotIndexMismatch.into());
        }
        Ok(ApplicationSnapshot {
            applied_index: at,
            payload: codec::encode_snapshot(self.applied_index, self.value, &self.sessions)?,
            raft_snapshot: None,
        })
    }

    fn install_snapshot(
        &mut self,
        snapshot: ApplicationSnapshot,
    ) -> Result<(), ApplicationSnapshotError<Self::Error>> {
        let payload = if snapshot.payload.is_empty() {
            let descriptor = snapshot
                .raft_snapshot
                .as_ref()
                .ok_or(CounterStateMachineError::PromotedSnapshotMissing)?;
            self.promoted_payloads
                .payload(descriptor.transfer_id())
                .ok_or(CounterStateMachineError::PromotedSnapshotMissing)?
                .to_vec()
        } else {
            snapshot.payload
        };
        let (applied_index, value, sessions) = codec::decode_snapshot(&payload, self.max_sessions)?;
        if applied_index != snapshot.applied_index {
            return Err(CounterStateMachineError::SnapshotIndexMismatch.into());
        }
        self.applied_index = applied_index;
        self.value = value;
        self.sessions = sessions;
        Ok(())
    }
}

/// Bounded codec or state-machine failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CounterStateMachineError {
    CommandTooLarge,
    MalformedCommand,
    MalformedSnapshot,
    UnsupportedVersion,
    SessionCapacity,
    SnapshotTooLarge,
    SnapshotIndexMismatch,
    SnapshotPayloadLengthMismatch,
    PromotedSnapshotMissing,
    ReadBarrierNotReached,
    InjectedFault,
}

impl fmt::Display for CounterStateMachineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::CommandTooLarge => "replicated command exceeds its bound",
            Self::MalformedCommand => "replicated command is malformed",
            Self::MalformedSnapshot => "counter snapshot is malformed",
            Self::UnsupportedVersion => "counter schema version is unsupported",
            Self::SessionCapacity => "replicated session capacity is exhausted",
            Self::SnapshotTooLarge => "counter snapshot exceeds its bound",
            Self::SnapshotIndexMismatch => "counter snapshot index does not match its payload",
            Self::SnapshotPayloadLengthMismatch => {
                "promoted counter snapshot payload length contradicts its descriptor"
            }
            Self::PromotedSnapshotMissing => "promoted counter snapshot payload is missing",
            Self::ReadBarrierNotReached => "counter read barrier has not been reached",
            Self::InjectedFault => "consumer fault poisoned the counter group",
        })
    }
}

impl Error for CounterStateMachineError {}
