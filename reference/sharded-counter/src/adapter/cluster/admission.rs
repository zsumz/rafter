use std::{
    collections::BTreeMap,
    num::{NonZeroU64, NonZeroUsize},
};

use rafter::{LocalProposalId, NodeConfig, NodeId};
use rafter_app::{
    group::{GroupInput, RaftGroup},
    proposal::Proposal,
};
use rafter_multiraft::{
    managed::{ManagedOpenError, RegisterError, WorkClass},
    MultiRaftErrorKind,
};
use rafter_runtime::DurableRaftNode;
use rafter_storage::InMemoryRaftHardStateStore;

use crate::{
    AdmissionRejection as PolicyRejection, ClientId, CounterCommand, GroupId, GroupIncarnation,
    GroupLifecycle, RequestFingerprint, RequestIdentity, Sequence, SessionEpoch, SystemClass,
    WorkQuota,
};

use super::{
    AdapterError, CounterAdmissionRejected, CounterAdmissionRejection, CounterGroup,
    CounterSubmitOutcome, GroupSlot, ManagedCounterCluster, OutstandingRequest, PendingProposal,
    ProposalReceipt, SessionSubmitOutcome,
};
use crate::adapter::state_machine::{CounterStateMachine, ReplicatedCounterCommand};

impl ManagedCounterCluster {
    /// Registers one three-node group in the consumer's `Creating` state.
    ///
    /// This compatibility helper creates only a first incarnation. Full
    /// lifecycle/reopen behavior is exposed by [`Self::lifecycle`].
    ///
    /// # Errors
    ///
    /// Refuses an existing group or a driver rejected by the managed host.
    pub fn register_group(
        &mut self,
        group_id: GroupId,
        quota: NonZeroUsize,
    ) -> Result<(), AdapterError> {
        if self.groups.contains_key(&group_id) {
            return Err(AdapterError::GroupAlreadyRegistered(group_id));
        }
        let quota_u32 =
            u32::try_from(quota.get()).map_err(|_| AdapterError::QuotaOutOfRange(quota.get()))?;
        let quota = WorkQuota::new(quota_u32).ok_or(AdapterError::QuotaOutOfRange(
            usize::try_from(quota_u32).unwrap_or(usize::MAX),
        ))?;
        self.open_physical_group(group_id, quota)?;
        self.groups.insert(
            group_id,
            GroupSlot {
                incarnation: GroupIncarnation::first(),
                lifecycle: GroupLifecycle::Creating,
                quota,
                applied_index: rafter::LogIndex::ZERO,
                value: 0,
                sessions: BTreeMap::default(),
            },
        );
        Ok(())
    }

    /// Moves one created group into recovery and schedules its election tick.
    ///
    /// # Errors
    ///
    /// Refuses any lifecycle other than `Creating`.
    pub fn recover_group(&mut self, group_id: GroupId) -> Result<(), AdapterError> {
        self.require_lifecycle(group_id, GroupLifecycle::Creating)?;
        self.host
            .set_available(&group_id, true)
            .map_err(|_| AdapterError::Lifecycle {
                group_id,
                expected: GroupLifecycle::Creating,
                actual: None,
            })?;
        if let Err(rejected) = self
            .host
            .admit(&group_id, WorkClass::Control, GroupInput::Tick)
        {
            let _ = self.host.set_available(&group_id, false);
            return Err(AdapterError::RecoveryAdmission {
                group_id,
                reason: rejected.reason,
            });
        }
        if let Some(slot) = self.groups.get_mut(&group_id) {
            slot.lifecycle = GroupLifecycle::Recovering;
        }
        Ok(())
    }

    /// Marks a recovered group ready for counter work.
    ///
    /// # Errors
    ///
    /// Refuses any lifecycle other than `Recovering`.
    pub fn serve_group(&mut self, group_id: GroupId) -> Result<(), AdapterError> {
        self.require_lifecycle(group_id, GroupLifecycle::Recovering)?;
        if let Some(slot) = self.groups.get_mut(&group_id) {
            slot.lifecycle = GroupLifecycle::Serving;
        }
        Ok(())
    }

    /// Admits replicated session establishment for the current incarnation.
    ///
    /// # Errors
    ///
    /// Returns the typed consumer or managed refusal and untouched input.
    pub fn open_session(
        &mut self,
        group_id: GroupId,
        client_id: ClientId,
        epoch: SessionEpoch,
    ) -> Result<SessionSubmitOutcome, Box<CounterAdmissionRejected>> {
        let Some(incarnation) = self.groups.get(&group_id).map(|slot| slot.incarnation) else {
            let input =
                self.proposal_input(ReplicatedCounterCommand::OpenSession { client_id, epoch });
            return Err(refusal(
                CounterAdmissionRejection::Policy(PolicyRejection::GroupUnknown),
                input,
            ));
        };
        self.open_session_for(group_id, incarnation, client_id, epoch)
    }

    /// Admits replicated session establishment under an exact incarnation.
    ///
    /// # Errors
    ///
    /// Returns a precise identity, lifecycle, session, or queue refusal with
    /// the untouched proposal input.
    pub fn open_session_for(
        &mut self,
        group_id: GroupId,
        incarnation: GroupIncarnation,
        client_id: ClientId,
        epoch: SessionEpoch,
    ) -> Result<SessionSubmitOutcome, Box<CounterAdmissionRejected>> {
        let command = ReplicatedCounterCommand::OpenSession { client_id, epoch };
        if let Some(pending) = self.pending.values().copied().find(|pending| {
            matches!(
                *pending,
                PendingProposal::OpenSession {
                    group_id: pending_group,
                    incarnation: pending_incarnation,
                    client_id: pending_client,
                    epoch: pending_epoch,
                    ..
                } if pending_group == group_id
                    && pending_incarnation == incarnation
                    && pending_client == client_id
                    && pending_epoch == epoch
            )
        }) {
            return Ok(SessionSubmitOutcome::AlreadyQueued(pending.receipt()));
        }
        let input = self.proposal_input(command);
        self.gate_session(group_id, incarnation, client_id, epoch)
            .map_err(|reason| refusal(CounterAdmissionRejection::Policy(reason), input.clone()))?;
        if self
            .groups
            .get(&group_id)
            .and_then(|slot| slot.sessions.get(&client_id))
            .is_some_and(|session| session.epoch == epoch)
        {
            return Ok(SessionSubmitOutcome::AlreadyOpen);
        }
        let receipt = self.admit_proposal_input(group_id, WorkClass::Control, input)?;
        self.pending.insert(
            receipt.proposal_id,
            PendingProposal::OpenSession {
                group_id,
                incarnation,
                client_id,
                epoch,
                receipt,
            },
        );
        self.work_proposals
            .insert(receipt.admission.work_id, receipt.proposal_id);
        Ok(SessionSubmitOutcome::Queued(receipt))
    }

    /// Admits one counter request for the current incarnation.
    ///
    /// # Errors
    ///
    /// Returns the typed consumer or managed refusal and untouched input.
    pub fn submit(
        &mut self,
        group_id: GroupId,
        request: RequestIdentity,
        command: CounterCommand,
    ) -> Result<CounterSubmitOutcome, Box<CounterAdmissionRejected>> {
        let Some(incarnation) = self.groups.get(&group_id).map(|slot| slot.incarnation) else {
            let input = self.proposal_input(ReplicatedCounterCommand::Counter { request, command });
            return Err(refusal(
                CounterAdmissionRejection::Policy(PolicyRejection::GroupUnknown),
                input,
            ));
        };
        self.submit_for(group_id, incarnation, request, command)
    }

    /// Admits one replicated counter command under an exact incarnation.
    ///
    /// Retry decisions happen before managed queue bounds. Completed requests
    /// remain replayable and outstanding requests retain their one queue slot
    /// even while either queue is full.
    ///
    /// # Errors
    ///
    /// Returns the typed consumer or managed refusal and untouched input.
    pub fn submit_for(
        &mut self,
        group_id: GroupId,
        incarnation: GroupIncarnation,
        request: RequestIdentity,
        command: CounterCommand,
    ) -> Result<CounterSubmitOutcome, Box<CounterAdmissionRejected>> {
        let input = self.proposal_input(ReplicatedCounterCommand::Counter { request, command });
        match self.counter_gate(group_id, incarnation, request, command) {
            Ok(Some(outcome)) => return Ok(outcome),
            Ok(None) => {}
            Err(reason) => {
                return Err(refusal(CounterAdmissionRejection::Policy(reason), input));
            }
        }
        let receipt = self.admit_proposal_input(group_id, WorkClass::Command, input)?;
        if let Some(session) = self
            .groups
            .get_mut(&group_id)
            .and_then(|slot| slot.sessions.get_mut(&request.client_id))
        {
            session.outstanding = Some(OutstandingRequest {
                sequence: request.sequence,
                command,
                receipt,
            });
        }
        self.pending.insert(
            receipt.proposal_id,
            PendingProposal::Counter {
                group_id,
                incarnation,
                request,
                command,
                receipt,
            },
        );
        self.work_proposals
            .insert(receipt.admission.work_id, receipt.proposal_id);
        Ok(CounterSubmitOutcome::Queued(receipt))
    }

    /// Admits one real Rafter input in a consumer-owned non-command class.
    ///
    /// The input is a genuine group tick. Its class is chosen at this boundary
    /// to exercise scheduler priority independently from the Raft input enum.
    ///
    /// # Errors
    ///
    /// Returns a precise policy or queue refusal.
    pub fn submit_system(
        &mut self,
        group_id: GroupId,
        incarnation: GroupIncarnation,
        class: SystemClass,
    ) -> Result<rafter_multiraft::managed::AdmissionReceipt, Box<CounterAdmissionRejected>> {
        let input = GroupInput::Tick;
        let policy = if class == SystemClass::Control {
            self.gate_protocol_continuation(group_id, incarnation)
        } else {
            self.gate_work(group_id, incarnation, class.class())
        };
        policy
            .map_err(|reason| refusal(CounterAdmissionRejection::Policy(reason), input.clone()))?;
        self.host
            .admit(&group_id, managed_class(class), input)
            .map_err(|rejected| {
                refusal(
                    CounterAdmissionRejection::Managed(rejected.reason),
                    rejected.payload,
                )
            })
    }

    /// Admits the contract's consumer-owned faulty work shape.
    ///
    /// # Errors
    ///
    /// Returns the typed consumer or managed refusal and untouched input.
    pub fn submit_fault(
        &mut self,
        group_id: GroupId,
        class: SystemClass,
    ) -> Result<ProposalReceipt, Box<CounterAdmissionRejected>> {
        let Some(incarnation) = self.groups.get(&group_id).map(|slot| slot.incarnation) else {
            let input = self.proposal_input(ReplicatedCounterCommand::Faulty);
            return Err(refusal(
                CounterAdmissionRejection::Policy(PolicyRejection::GroupUnknown),
                input,
            ));
        };
        let input = self.proposal_input(ReplicatedCounterCommand::Faulty);
        self.gate_work(group_id, incarnation, class.class())
            .map_err(|reason| refusal(CounterAdmissionRejection::Policy(reason), input.clone()))?;
        let receipt = self.admit_proposal_input(group_id, managed_class(class), input)?;
        self.pending.insert(
            receipt.proposal_id,
            PendingProposal::Fault {
                group_id,
                incarnation,
                receipt,
            },
        );
        self.work_proposals
            .insert(receipt.admission.work_id, receipt.proposal_id);
        Ok(receipt)
    }

    fn gate_session(
        &self,
        group_id: GroupId,
        incarnation: GroupIncarnation,
        client_id: ClientId,
        epoch: SessionEpoch,
    ) -> Result<(), PolicyRejection> {
        self.admit_group(group_id, incarnation, crate::WorkClass::Control)?;
        let slot = self
            .groups
            .get(&group_id)
            .ok_or(PolicyRejection::GroupUnknown)?;
        if !matches!(
            slot.lifecycle,
            GroupLifecycle::Recovering | GroupLifecycle::Serving
        ) {
            return Err(PolicyRejection::GroupNotAcceptingSessions {
                state: slot.lifecycle,
            });
        }
        if self.poisoned.contains(&group_id) {
            return Err(PolicyRejection::GroupPoisoned);
        }
        if usize::try_from(client_id.get()).map_or(true, |id| {
            id >= self.network_config.max_sessions_per_group.get()
        }) {
            return Err(PolicyRejection::ClientOutOfRange);
        }
        if let Some(session) = slot.sessions.get(&client_id) {
            if epoch < session.epoch {
                return Err(PolicyRejection::StaleSession {
                    current: session.epoch,
                });
            }
        }
        Ok(())
    }

    fn gate_work(
        &self,
        group_id: GroupId,
        incarnation: GroupIncarnation,
        class: crate::WorkClass,
    ) -> Result<(), PolicyRejection> {
        self.admit_group(group_id, incarnation, class)?;
        let slot = self
            .groups
            .get(&group_id)
            .ok_or(PolicyRejection::GroupUnknown)?;
        if !slot.lifecycle.admits(class) {
            return Err(PolicyRejection::GroupNotAcceptingWork {
                state: slot.lifecycle,
                class,
            });
        }
        if self.poisoned.contains(&group_id) {
            return Err(PolicyRejection::GroupPoisoned);
        }
        Ok(())
    }

    fn counter_gate(
        &self,
        group_id: GroupId,
        incarnation: GroupIncarnation,
        request: RequestIdentity,
        command: CounterCommand,
    ) -> Result<Option<CounterSubmitOutcome>, PolicyRejection> {
        self.gate_work(group_id, incarnation, crate::WorkClass::Command)?;
        if usize::try_from(request.client_id.get()).map_or(true, |id| {
            id >= self.network_config.max_sessions_per_group.get()
        }) {
            return Err(PolicyRejection::ClientOutOfRange);
        }
        let slot = self
            .groups
            .get(&group_id)
            .ok_or(PolicyRejection::GroupUnknown)?;
        let session = slot
            .sessions
            .get(&request.client_id)
            .ok_or(PolicyRejection::SessionNotOpen)?;
        if request.session_epoch < session.epoch {
            return Err(PolicyRejection::StaleSession {
                current: session.epoch,
            });
        }
        if request.session_epoch > session.epoch {
            return Err(PolicyRejection::FutureSession {
                current: session.epoch,
            });
        }
        let expected_fingerprint = RequestFingerprint::of(&command);
        if request.fingerprint != expected_fingerprint {
            return Err(PolicyRejection::FingerprintMismatch {
                expected: expected_fingerprint,
            });
        }
        if let Some(completed) = session.completed {
            if request.sequence < completed.sequence {
                return Err(PolicyRejection::StaleSequence {
                    highest: completed.sequence,
                });
            }
            if request.sequence == completed.sequence {
                return if command == completed.command {
                    Ok(Some(CounterSubmitOutcome::Replayed(completed.result)))
                } else {
                    Err(PolicyRejection::ConflictingRetry)
                };
            }
        }
        if let Some(outstanding) = session.outstanding {
            if request.sequence == outstanding.sequence {
                return if command == outstanding.command {
                    Ok(Some(CounterSubmitOutcome::AlreadyQueued(
                        outstanding.receipt,
                    )))
                } else {
                    Err(PolicyRejection::ConflictingRetry)
                };
            }
        }
        let expected = session
            .completed
            .and_then(|completed| completed.sequence.successor())
            .unwrap_or_else(Sequence::first);
        if request.sequence != expected {
            return Err(PolicyRejection::SequenceGap { expected });
        }
        Ok(None)
    }

    fn proposal_input(
        &mut self,
        command: ReplicatedCounterCommand,
    ) -> GroupInput<GroupId, ReplicatedCounterCommand> {
        GroupInput::Proposal {
            proposal: Proposal {
                local_proposal_id: self.next_proposal_id(),
                client_request_id: None,
                command,
            },
        }
    }

    fn admit_proposal_input(
        &mut self,
        group_id: GroupId,
        class: WorkClass,
        input: GroupInput<GroupId, ReplicatedCounterCommand>,
    ) -> Result<ProposalReceipt, Box<CounterAdmissionRejected>> {
        let GroupInput::Proposal { proposal } = &input else {
            unreachable!("counter admission constructs only proposal inputs");
        };
        let proposal_id = proposal.local_proposal_id;
        self.host
            .admit(&group_id, class, input)
            .map(|admission| ProposalReceipt {
                admission,
                proposal_id,
            })
            .map_err(|rejected| {
                refusal(
                    CounterAdmissionRejection::Managed(rejected.reason),
                    rejected.payload,
                )
            })
    }

    pub(super) fn open_physical_group(
        &mut self,
        group_id: GroupId,
        quota: WorkQuota,
    ) -> Result<(), AdapterError> {
        let local = self.group(group_id, NodeId(1));
        let quota = NonZeroUsize::new(
            usize::try_from(quota.get()).map_err(|_| AdapterError::QuotaOutOfRange(usize::MAX))?,
        )
        .ok_or(AdapterError::QuotaOutOfRange(0))?;
        if let Err(rejected) = self.host.open_group(&group_id, local, Some(quota)) {
            return Err(AdapterError::OpenGroup {
                group_id,
                kind: match rejected.error {
                    ManagedOpenError::Scheduler(RegisterError::AlreadyRegistered(_)) => {
                        MultiRaftErrorKind::GroupAlreadyOpen
                    }
                    ManagedOpenError::Host(error) => error.kind(),
                },
            });
        }
        self.peers
            .insert((group_id, NodeId(2)), self.group(group_id, NodeId(2)));
        self.peers
            .insert((group_id, NodeId(3)), self.group(group_id, NodeId(3)));
        Ok(())
    }

    fn group(&self, group_id: GroupId, node_id: NodeId) -> CounterGroup {
        let peers = [NodeId(1), NodeId(2), NodeId(3)]
            .into_iter()
            .filter(|peer| *peer != node_id)
            .collect();
        let config = NodeConfig::new(node_id, peers, 1)
            .expect("the fixed three-node adapter membership is valid")
            .with_pre_vote(false);
        let runtime = DurableRaftNode::new(config, InMemoryRaftHardStateStore::new())
            .expect("fresh in-memory durable storage opens");
        RaftGroup::new(
            group_id,
            node_id,
            runtime,
            CounterStateMachine::new(self.network_config.max_sessions_per_group.get()),
        )
    }

    fn require_lifecycle(
        &self,
        group_id: GroupId,
        expected: GroupLifecycle,
    ) -> Result<(), AdapterError> {
        let actual = self.groups.get(&group_id).map(|slot| slot.lifecycle);
        if actual == Some(expected) {
            Ok(())
        } else {
            Err(AdapterError::Lifecycle {
                group_id,
                expected,
                actual,
            })
        }
    }

    fn next_proposal_id(&mut self) -> LocalProposalId {
        let value = NonZeroU64::new(self.next_proposal_id)
            .expect("adapter stops before proposal identity wraps");
        self.next_proposal_id = self.next_proposal_id.checked_add(1).unwrap_or(0);
        LocalProposalId(value.get())
    }
}

fn refusal(
    reason: CounterAdmissionRejection,
    input: GroupInput<GroupId, ReplicatedCounterCommand>,
) -> Box<CounterAdmissionRejected> {
    Box::new(CounterAdmissionRejected { reason, input })
}

const fn managed_class(class: SystemClass) -> WorkClass {
    match class {
        SystemClass::Control => WorkClass::Control,
        SystemClass::Snapshot => WorkClass::Snapshot,
        SystemClass::Bulk => WorkClass::Bulk,
    }
}
