//! Admission and restart re-admission for the process host.

use std::time::Instant;

use rafter::LocalProposalId;
use rafter_app::{group::GroupInput, proposal::Proposal, transport::PeerEnvelope};
use rafter_multiraft::managed::WorkClass;
use rafter_reference_sharded_counter::{
    adapter::ReplicatedCounterCommand, GroupId, GroupIncarnation, GroupLifecycle,
    WorkClass as PolicyWorkClass,
};

use super::{ClientAdmissionRefusal, Engine, PendingClient, WorkKind, MAX_PEERS_PER_LOOP};
use crate::{
    app_store::{AcceptedOperation, OutstandingPhase, TerminalFailure},
    group::SharedGroup,
    protocol::ClientReply,
};

impl Engine {
    pub(super) fn serving_driver(
        &self,
        group_id: GroupId,
        incarnation: GroupIncarnation,
    ) -> Result<SharedGroup, String> {
        let Some(entry) = self.groups.get(&group_id) else {
            return Err("ERR GROUP_UNKNOWN".to_string());
        };
        let policy = entry.record.policy();
        if incarnation < policy.incarnation {
            return Err(format!(
                "ERR STALE_INCARNATION current={}",
                policy.incarnation.get()
            ));
        }
        if incarnation > policy.incarnation {
            return Err(format!(
                "ERR FUTURE_INCARNATION current={}",
                policy.incarnation.get()
            ));
        }
        if policy.lifecycle == GroupLifecycle::Tombstoned {
            return Err("ERR TOMBSTONED".to_string());
        }
        if policy.lifecycle != GroupLifecycle::Serving {
            return Err(format!("ERR LIFECYCLE {:?}", policy.lifecycle));
        }
        if policy.poisoned || self.poisoned.contains(&group_id) {
            return Err("ERR GROUP_POISONED".to_string());
        }
        let Some(driver) = entry.driver.clone() else {
            return Err("ERR GROUP_REMOVED".to_string());
        };
        if !driver.is_ready() {
            return Err("ERR NOT_READY".to_string());
        }
        Ok(driver)
    }

    pub(super) fn admit_client_proposal(
        &mut self,
        group_id: GroupId,
        class: WorkClass,
        command: ReplicatedCounterCommand,
        operation: Option<AcceptedOperation>,
        reply: ClientReply,
    ) -> Result<(), ClientAdmissionRefusal> {
        let proposal_id = LocalProposalId(self.next_proposal_id);
        let Some(next) = self.next_proposal_id.checked_add(1) else {
            return Err(ClientAdmissionRefusal {
                reply,
                response: "ERR PROPOSAL_ID_EXHAUSTED".to_string(),
                managed: false,
            });
        };
        let input = GroupInput::Proposal {
            proposal: Proposal {
                local_proposal_id: proposal_id,
                client_request_id: None,
                command,
            },
        };
        let receipt = match self.host.admit(&group_id, class, input) {
            Ok(receipt) => receipt,
            Err(rejected) => {
                return Err(ClientAdmissionRefusal {
                    reply,
                    response: format!("ERR BACKPRESSURE {:?}", rejected.reason),
                    managed: true,
                });
            }
        };
        self.audit
            .observe_admission(group_id, receipt.work_id, class);
        self.next_proposal_id = next;
        if let Some(operation) = operation {
            self.pending_operations
                .insert((group_id, operation.client_id()), proposal_id);
        }
        self.pending.insert(
            proposal_id,
            PendingClient {
                group_id,
                operation,
                replies: vec![reply],
                deadline: Some(Instant::now() + self.request_timeout),
                recovered: false,
            },
        );
        self.work
            .insert(receipt.work_id, WorkKind::Proposal(proposal_id));
        Ok(())
    }

    pub(super) fn recover_outstanding(&mut self) -> Result<(), String> {
        let mut interrupted = Vec::new();
        for (group_id, entry) in &self.groups {
            let policy = entry.record.policy();
            let draining = policy.lifecycle == GroupLifecycle::Draining;
            for outstanding in policy.outstanding.into_values() {
                if self
                    .pending_operations
                    .contains_key(&(*group_id, outstanding.operation.client_id()))
                {
                    continue;
                }
                let failure = if policy.poisoned {
                    Some(match outstanding.phase {
                        OutstandingPhase::Queued => TerminalFailure::GroupPoisoned,
                        OutstandingPhase::EnteredDriver => TerminalFailure::GroupPoisonedUnknown,
                    })
                } else if draining && outstanding.phase == OutstandingPhase::Queued {
                    Some(TerminalFailure::ProcessRestarted)
                } else {
                    None
                };
                if let Some(failure) = failure {
                    interrupted.push((entry.record.clone(), outstanding, failure));
                }
            }
        }
        for (record, outstanding, failure) in interrupted {
            record
                .fail_reservation(outstanding.operation, failure)
                .map_err(|error| error.to_string())?;
        }
        let recoverable = self
            .groups
            .iter()
            .filter(|(_, entry)| entry.driver.is_some())
            .flat_map(|(group_id, entry)| {
                entry
                    .record
                    .policy()
                    .outstanding
                    .into_values()
                    .map(|outstanding| (*group_id, outstanding))
                    .collect::<Vec<_>>()
            })
            .filter(|(group_id, outstanding)| {
                !self
                    .pending_operations
                    .contains_key(&(*group_id, outstanding.operation.client_id()))
            })
            .collect::<Vec<_>>();
        for (group_id, outstanding) in recoverable {
            let proposal_id = LocalProposalId(self.next_proposal_id);
            let Some(next) = self.next_proposal_id.checked_add(1) else {
                return Err("proposal identity exhausted while recovering durable work".to_string());
            };
            let input = GroupInput::Proposal {
                proposal: Proposal {
                    local_proposal_id: proposal_id,
                    client_request_id: None,
                    command: outstanding.operation.replicated_command(),
                },
            };
            let class = match outstanding.operation {
                AcceptedOperation::OpenSession { .. } => WorkClass::Control,
                AcceptedOperation::Counter { .. } => WorkClass::Command,
            };
            let Ok(receipt) = self.host.admit(&group_id, class, input) else {
                continue;
            };
            self.audit
                .observe_admission(group_id, receipt.work_id, class);
            self.next_proposal_id = next;
            self.pending_operations
                .insert((group_id, outstanding.operation.client_id()), proposal_id);
            self.pending.insert(
                proposal_id,
                PendingClient {
                    group_id,
                    operation: Some(outstanding.operation),
                    replies: Vec::new(),
                    deadline: None,
                    recovered: true,
                },
            );
            self.work
                .insert(receipt.work_id, WorkKind::Proposal(proposal_id));
        }
        Ok(())
    }

    pub(super) fn admit_peer_frames(&mut self) {
        for frame in self.link.drain_inbound(MAX_PEERS_PER_LOOP) {
            let Some(entry) = self.groups.get(&frame.group_id) else {
                self.refused_peer += 1;
                continue;
            };
            let policy = entry.record.policy();
            let accepted_work_remains = !policy.outstanding.is_empty()
                || self
                    .pending
                    .values()
                    .any(|pending| pending.group_id == frame.group_id);
            if frame.incarnation != policy.incarnation
                || !policy
                    .lifecycle
                    .permits_protocol_continuation(accepted_work_remains)
                || entry.driver.is_none()
                || self.poisoned.contains(&frame.group_id)
            {
                self.refused_peer += 1;
                continue;
            }
            let input = GroupInput::PeerMessage {
                envelope: PeerEnvelope {
                    group_id: frame.group_id,
                    from: frame.from,
                    to: frame.to,
                    message: frame.message,
                },
            };
            match self.host.admit(&frame.group_id, WorkClass::Control, input) {
                Ok(receipt) => {
                    self.audit.observe_admission(
                        frame.group_id,
                        receipt.work_id,
                        WorkClass::Control,
                    );
                    self.work.insert(receipt.work_id, WorkKind::Peer);
                }
                Err(_) => self.refused_peer += 1,
            }
        }
    }

    pub(super) fn admit_ticks(&mut self) {
        let group_ids = self.groups.keys().copied().collect::<Vec<_>>();
        for group_id in group_ids {
            if self.tick_pending.contains(&group_id) || self.poisoned.contains(&group_id) {
                continue;
            }
            let entry = &self.groups[&group_id];
            let policy = entry.record.policy();
            let accepted_work_remains = !policy.outstanding.is_empty()
                || self
                    .pending
                    .values()
                    .any(|pending| pending.group_id == group_id);
            let admits_tick = policy.lifecycle.admits(PolicyWorkClass::Control)
                || (policy.lifecycle == GroupLifecycle::Draining && accepted_work_remains);
            if !admits_tick || entry.driver.is_none() {
                continue;
            }
            if let Ok(receipt) = self
                .host
                .admit(&group_id, WorkClass::Control, GroupInput::Tick)
            {
                self.audit
                    .observe_admission(group_id, receipt.work_id, WorkClass::Control);
                self.tick_pending.insert(group_id);
                self.work.insert(receipt.work_id, WorkKind::Tick);
            }
        }
    }
}
