//! Admission and restart re-admission for the process host.

use std::time::Instant;

use rafter::{LocalProposalId, ReadId};
use rafter_app::{
    group::GroupInput,
    proposal::Proposal,
    read::{ReadBarrierRequest, ReadEvent},
    transport::PeerEnvelope,
};
use rafter_multiraft::{driver::DriverErrorKind, managed::WorkClass};
use rafter_reference_sharded_counter::{
    adapter::{CounterAdmissionDecision, ReplicatedCounterCommand},
    GroupId, GroupIncarnation, GroupLifecycle, WorkClass as PolicyWorkClass,
};

use super::{
    render_apply_rejection, render_counter_result, render_terminal_failure, ClientAdmissionRefusal,
    Engine, PendingAdmission, PendingClient, WorkKind, MAX_PEERS_PER_LOOP,
};
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

    pub(super) fn begin_authoritative_admission(
        &mut self,
        group_id: GroupId,
        incarnation: GroupIncarnation,
        operation: AcceptedOperation,
        reply: ClientReply,
    ) -> Result<(), String> {
        let record = match self.admission_record(group_id, incarnation) {
            Ok(record) => record,
            Err(response) => {
                reply.send(response, false);
                return Ok(());
            }
        };
        if usize::try_from(operation.client_id().get())
            .map_or(true, |client| client >= self.max_sessions)
        {
            reply.send("ERR CLIENT_OUT_OF_RANGE".to_string(), false);
            return Ok(());
        }
        let Some(reply) = self.attach_exact_pending(group_id, operation, reply) else {
            return Ok(());
        };
        let Some(reply) = self.attach_exact_admission(group_id, operation, reply) else {
            return Ok(());
        };
        match record.replay_terminal_failure(operation) {
            Ok(Some(failure)) => {
                reply.send(render_terminal_failure(failure).to_string(), false);
                return Ok(());
            }
            Ok(None) => {}
            Err(_) => {
                reply.send("ERR CONFLICTING_OUTSTANDING".to_string(), false);
                return Ok(());
            }
        }
        let driver = match self.serving_driver(group_id, incarnation) {
            Ok(driver) => driver,
            Err(response) => {
                reply.send(response, false);
                return Ok(());
            }
        };

        let read_id = ReadId(self.next_read_id);
        let Some(next) = self.next_read_id.checked_add(1) else {
            reply.send("ERR READ_ID_EXHAUSTED".to_string(), false);
            return Ok(());
        };
        self.next_read_id = next;
        self.pending_admission_operations
            .insert((group_id, operation.client_id()), read_id);
        self.pending_admissions.insert(
            read_id,
            PendingAdmission {
                group_id,
                incarnation,
                operation,
                replies: vec![reply],
                deadline: Instant::now() + self.request_timeout,
            },
        );
        let input = GroupInput::ReadBarrier {
            request: ReadBarrierRequest {
                group_id,
                read_id,
                min_applied_index: None,
                context: Vec::new(),
            },
        };
        match driver.step_direct(input) {
            Ok(report) => self.collect_report(group_id, report),
            Err(error) => {
                self.finish_failed_admission_read(
                    read_id,
                    &format!("read barrier failed: {error}"),
                );
                if error.kind() == DriverErrorKind::Poisoned {
                    self.persist_runtime_poison(group_id)?;
                }
                Ok(())
            }
        }
    }

    fn admission_record(
        &self,
        group_id: GroupId,
        incarnation: GroupIncarnation,
    ) -> Result<crate::app_store::ApplicationRecord, String> {
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
        Ok(entry.record.clone())
    }

    pub(super) fn finish_admission_read_event(
        &mut self,
        event: &ReadEvent<GroupId>,
    ) -> Result<(), String> {
        match event {
            ReadEvent::Granted { read_id, .. } => {
                self.finish_granted_admission(*read_id)?;
            }
            ReadEvent::Rejected {
                read_id,
                reason,
                leader_hint,
            } => self.finish_failed_admission_read(
                *read_id,
                &format!("read rejected: {reason:?} leader={leader_hint:?}"),
            ),
            ReadEvent::Canceled {
                read_id,
                reason,
                leader_hint,
            } => self.finish_failed_admission_read(
                *read_id,
                &format!("read canceled: {reason:?} leader={leader_hint:?}"),
            ),
            ReadEvent::FreshnessUnavailable { .. } => {}
            _ => return Err("unsupported admission read event".to_string()),
        }
        Ok(())
    }

    fn finish_granted_admission(&mut self, read_id: ReadId) -> Result<(), String> {
        let Some(pending) = self.take_pending_admission(read_id) else {
            return Ok(());
        };
        let driver = match self.serving_driver(pending.group_id, pending.incarnation) {
            Ok(driver) => driver,
            Err(response) => {
                for reply in pending.replies {
                    reply.send(response.clone(), false);
                }
                return Ok(());
            }
        };
        match driver.admission_decision(pending.operation.replicated_command()) {
            CounterAdmissionDecision::SessionAlreadyOpen => {
                for reply in pending.replies {
                    reply.send("OK SESSION already_open".to_string(), false);
                }
            }
            CounterAdmissionDecision::CounterReplay(result) => {
                let response = format!("OK REPLAY {}", render_counter_result(result));
                for reply in pending.replies {
                    reply.send(response.clone(), false);
                }
            }
            CounterAdmissionDecision::Rejected(rejection) => {
                let response = render_apply_rejection(rejection);
                for reply in pending.replies {
                    reply.send(response.clone(), false);
                }
            }
            CounterAdmissionDecision::Proceed => {
                let record = self
                    .groups
                    .get(&pending.group_id)
                    .expect("serving group remains installed")
                    .record
                    .clone();
                let reservation = match record.reserve(pending.operation) {
                    Ok(reservation) => reservation,
                    Err(error) => {
                        let response = format!("ERR ADMISSION {error}");
                        for reply in pending.replies {
                            reply.send(response.clone(), false);
                        }
                        return Ok(());
                    }
                };
                let class = match pending.operation {
                    AcceptedOperation::OpenSession { .. } => WorkClass::Control,
                    AcceptedOperation::Counter { .. } => WorkClass::Command,
                };
                if let Err(refusal) = self.admit_client_proposal(
                    pending.group_id,
                    class,
                    pending.operation.replicated_command(),
                    Some(pending.operation),
                    pending.replies,
                ) {
                    Self::finish_reserved_admission(
                        &record,
                        pending.operation,
                        reservation,
                        refusal,
                    )?;
                }
            }
        }
        Ok(())
    }

    fn attach_exact_pending(
        &mut self,
        group_id: GroupId,
        operation: AcceptedOperation,
        reply: ClientReply,
    ) -> Option<ClientReply> {
        let Some(proposal_id) = self
            .pending_operations
            .get(&(group_id, operation.client_id()))
            .copied()
        else {
            return Some(reply);
        };
        let pending = self
            .pending
            .get_mut(&proposal_id)
            .expect("pending operation index names a pending proposal");
        if pending.operation == Some(operation) {
            pending.replies.push(reply);
            pending.deadline = Some(Instant::now() + self.request_timeout);
        } else {
            reply.send("ERR CONFLICTING_OUTSTANDING".to_string(), false);
        }
        None
    }

    fn attach_exact_admission(
        &mut self,
        group_id: GroupId,
        operation: AcceptedOperation,
        reply: ClientReply,
    ) -> Option<ClientReply> {
        let Some(read_id) = self
            .pending_admission_operations
            .get(&(group_id, operation.client_id()))
            .copied()
        else {
            return Some(reply);
        };
        let pending = self
            .pending_admissions
            .get_mut(&read_id)
            .expect("pending admission index names a read");
        if pending.operation == operation {
            pending.replies.push(reply);
            pending.deadline = Instant::now() + self.request_timeout;
        } else {
            reply.send("ERR CONFLICTING_OUTSTANDING".to_string(), false);
        }
        None
    }

    fn take_pending_admission(&mut self, read_id: ReadId) -> Option<PendingAdmission> {
        let pending = self.pending_admissions.remove(&read_id)?;
        self.pending_admission_operations
            .remove(&(pending.group_id, pending.operation.client_id()));
        Some(pending)
    }

    fn finish_failed_admission_read(&mut self, read_id: ReadId, detail: &str) {
        let Some(pending) = self.take_pending_admission(read_id) else {
            return;
        };
        for reply in pending.replies {
            reply.send(format!("ERR NOT_COMMITTED {detail}"), false);
        }
    }

    pub(super) fn expire_admission_reads(&mut self, now: Instant) {
        let expired = self
            .pending_admissions
            .iter()
            .filter_map(|(read_id, pending)| (now >= pending.deadline).then_some(*read_id))
            .collect::<Vec<_>>();
        for read_id in expired {
            let Some(pending) = self.take_pending_admission(read_id) else {
                continue;
            };
            if let Some(driver) = self
                .groups
                .get(&pending.group_id)
                .and_then(|entry| entry.driver.as_ref())
            {
                driver.cancel_read(read_id);
            }
            for reply in pending.replies {
                reply.send(
                    "ERR NOT_COMMITTED admission barrier deadline elapsed".to_string(),
                    false,
                );
            }
        }
    }

    pub(super) fn cancel_admission_reads_for_group(&mut self, group_id: GroupId, response: &str) {
        let reads = self
            .pending_admissions
            .iter()
            .filter_map(|(read_id, pending)| (pending.group_id == group_id).then_some(*read_id))
            .collect::<Vec<_>>();
        for read_id in reads {
            let Some(pending) = self.take_pending_admission(read_id) else {
                continue;
            };
            if let Some(driver) = self
                .groups
                .get(&pending.group_id)
                .and_then(|entry| entry.driver.as_ref())
            {
                driver.cancel_read(read_id);
            }
            for reply in pending.replies {
                reply.send(response.to_string(), false);
            }
        }
    }

    pub(super) fn persist_runtime_poison(&mut self, group_id: GroupId) -> Result<(), String> {
        let record = self
            .groups
            .get(&group_id)
            .ok_or_else(|| format!("poisoned group {} disappeared", group_id.get()))?
            .record
            .clone();
        if !record.policy().poisoned {
            record.mark_poisoned().map_err(|error| error.to_string())?;
            crate::directed_failpoint("after_poison_publication_before_driver_error");
        }
        self.poisoned.insert(group_id);
        self.host
            .set_available(&group_id, false)
            .map_err(|error| format!("poisoned group availability failed: {error:?}"))?;
        self.audit.set_available(group_id, false);
        self.cancel_admission_reads_for_group(group_id, "ERR NOT_COMMITTED GROUP_POISONED");
        Ok(())
    }

    fn finish_reserved_admission(
        record: &crate::app_store::ApplicationRecord,
        operation: AcceptedOperation,
        reservation: crate::app_store::ReserveOutcome,
        refusal: ClientAdmissionRefusal,
    ) -> Result<(), String> {
        if reservation == crate::app_store::ReserveOutcome::ExactRetry {
            for reply in refusal.replies {
                reply.send(
                    "ERR UNKNOWN accepted operation remains durable".to_string(),
                    false,
                );
            }
            return Ok(());
        }
        if refusal.managed {
            crate::directed_failpoint("after_managed_refusal_before_durable_cancellation");
        }
        record
            .cancel_reservation(operation)
            .map_err(|error| error.to_string())?;
        if refusal.managed {
            crate::directed_failpoint("after_durable_cancellation_before_backpressure_response");
        }
        for reply in refusal.replies {
            reply.send(refusal.response.clone(), false);
        }
        Ok(())
    }

    pub(super) fn admit_client_proposal(
        &mut self,
        group_id: GroupId,
        class: WorkClass,
        command: ReplicatedCounterCommand,
        operation: Option<AcceptedOperation>,
        replies: Vec<ClientReply>,
    ) -> Result<(), ClientAdmissionRefusal> {
        let client_operation = operation.is_some();
        let proposal_id = LocalProposalId(self.next_proposal_id);
        let Some(next) = self.next_proposal_id.checked_add(1) else {
            return Err(ClientAdmissionRefusal {
                replies,
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
                    replies,
                    response: format!("ERR BACKPRESSURE {:?}", rejected.reason),
                    managed: true,
                });
            }
        };
        self.audit
            .observe_admission(group_id, receipt.work_id, class);
        if client_operation {
            self.client_admitted = self.client_admitted.saturating_add(1);
        }
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
                replies,
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

    pub(super) fn admit_peer_frames(&mut self) -> Result<(), String> {
        if self.peers_paused {
            return Ok(());
        }
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
            let admission_read_pending = self
                .pending_admissions
                .values()
                .any(|pending| pending.group_id == frame.group_id);
            if admission_read_pending {
                let driver = entry
                    .driver
                    .as_ref()
                    .expect("validated driver exists")
                    .clone();
                match driver.step_direct(input) {
                    Ok(report) => self.collect_report(frame.group_id, report)?,
                    Err(error) if error.kind() == DriverErrorKind::Poisoned => {
                        self.persist_runtime_poison(frame.group_id)?;
                    }
                    Err(_) => self.refused_peer += 1,
                }
                continue;
            }
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
        Ok(())
    }

    pub(super) fn admit_ticks(&mut self) -> Result<(), String> {
        let group_ids = self.groups.keys().copied().collect::<Vec<_>>();
        for group_id in group_ids {
            if self.poisoned.contains(&group_id) {
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
            let admission_read_pending = self
                .pending_admissions
                .values()
                .any(|pending| pending.group_id == group_id);
            if admission_read_pending {
                let driver = entry
                    .driver
                    .as_ref()
                    .expect("validated driver exists")
                    .clone();
                match driver.step_direct(GroupInput::Tick) {
                    Ok(report) => self.collect_report(group_id, report)?,
                    Err(error) if error.kind() == DriverErrorKind::Poisoned => {
                        self.persist_runtime_poison(group_id)?;
                    }
                    Err(_) => {}
                }
                continue;
            }
            if self.tick_pending.contains(&group_id) {
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
        Ok(())
    }
}
