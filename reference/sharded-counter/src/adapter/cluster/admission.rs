use std::num::{NonZeroU64, NonZeroUsize};

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
    ClientId, CounterCommand, GroupId, GroupLifecycle, RequestIdentity, SessionEpoch, SystemClass,
};

use super::{
    AdapterError, CounterAdmissionRejected, CounterAdmissionRejection, CounterGroup,
    ManagedCounterCluster, ProposalReceipt,
};
use crate::adapter::state_machine::{CounterStateMachine, ReplicatedCounterCommand};

impl ManagedCounterCluster {
    /// Registers one three-node group in the consumer's `Creating` state.
    ///
    /// # Errors
    ///
    /// Refuses an existing group or a driver rejected by the managed host.
    pub fn register_group(
        &mut self,
        group_id: GroupId,
        quota: NonZeroUsize,
    ) -> Result<(), AdapterError> {
        if self.lifecycles.contains_key(&group_id) {
            return Err(AdapterError::GroupAlreadyRegistered(group_id));
        }
        let local = self.group(group_id, NodeId(1));
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
        self.lifecycles.insert(group_id, GroupLifecycle::Creating);
        Ok(())
    }

    /// Moves one created group into recovery and schedules its election tick.
    ///
    /// # Errors
    ///
    /// Refuses any lifecycle other than `Creating`.
    pub fn recover_group(&mut self, group_id: GroupId) -> Result<(), AdapterError> {
        self.require_lifecycle(group_id, GroupLifecycle::Creating)?;
        self.lifecycles.insert(group_id, GroupLifecycle::Recovering);
        self.host
            .set_available(&group_id, true)
            .map_err(|_| AdapterError::Lifecycle {
                group_id,
                expected: GroupLifecycle::Creating,
                actual: None,
            })?;
        self.host
            .admit(&group_id, WorkClass::Control, GroupInput::Tick)
            .map_err(|rejected| AdapterError::RecoveryAdmission {
                group_id,
                reason: rejected.reason,
            })?;
        Ok(())
    }

    /// Marks a recovered group ready for counter work.
    ///
    /// # Errors
    ///
    /// Refuses any lifecycle other than `Recovering`.
    pub fn serve_group(&mut self, group_id: GroupId) -> Result<(), AdapterError> {
        self.require_lifecycle(group_id, GroupLifecycle::Recovering)?;
        self.lifecycles.insert(group_id, GroupLifecycle::Serving);
        Ok(())
    }

    /// Admits replicated session establishment as control work.
    ///
    /// The rejection returns the untouched `GroupInput`, including its exact
    /// proposal command.
    ///
    /// # Errors
    ///
    /// Returns the typed consumer or managed refusal and untouched input.
    pub fn open_session(
        &mut self,
        group_id: GroupId,
        client_id: ClientId,
        epoch: SessionEpoch,
    ) -> Result<ProposalReceipt, Box<CounterAdmissionRejected>> {
        let input = self.proposal_input(ReplicatedCounterCommand::OpenSession { client_id, epoch });
        let Some(state) = self.lifecycles.get(&group_id).copied() else {
            return Err(refusal(CounterAdmissionRejection::UnknownGroup, input));
        };
        if !matches!(state, GroupLifecycle::Recovering | GroupLifecycle::Serving) {
            return Err(refusal(
                CounterAdmissionRejection::Lifecycle { state: Some(state) },
                input,
            ));
        }
        self.admit_proposal_input(group_id, WorkClass::Control, input)
    }

    /// Admits one replicated counter command through managed queue bounds.
    ///
    /// # Errors
    ///
    /// Returns the typed consumer or managed refusal and untouched input.
    pub fn submit(
        &mut self,
        group_id: GroupId,
        request: RequestIdentity,
        command: CounterCommand,
    ) -> Result<ProposalReceipt, Box<CounterAdmissionRejected>> {
        let input = self.proposal_input(ReplicatedCounterCommand::Counter { request, command });
        self.admit_serving(group_id, WorkClass::Command, input)
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
        let input = self.proposal_input(ReplicatedCounterCommand::Faulty);
        self.admit_serving(group_id, managed_class(class), input)
    }

    fn admit_serving(
        &mut self,
        group_id: GroupId,
        class: WorkClass,
        input: GroupInput<GroupId, ReplicatedCounterCommand>,
    ) -> Result<ProposalReceipt, Box<CounterAdmissionRejected>> {
        if self.poisoned.contains(&group_id) {
            return Err(refusal(CounterAdmissionRejection::GroupPoisoned, input));
        }
        let Some(state) = self.lifecycles.get(&group_id).copied() else {
            return Err(refusal(CounterAdmissionRejection::UnknownGroup, input));
        };
        if state != GroupLifecycle::Serving {
            return Err(refusal(
                CounterAdmissionRejection::Lifecycle { state: Some(state) },
                input,
            ));
        }
        self.admit_proposal_input(group_id, class, input)
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
        let actual = self.lifecycles.get(&group_id).copied();
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
