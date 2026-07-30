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
    driver_application_durability_failed, render_apply_rejection, render_counter_result,
    render_terminal_failure, ClientAdmissionRefusal, Engine, PendingAdmission,
    PendingAdmissionCandidate, PendingClient, WorkKind, MAX_PEERS_PER_LOOP, RECOVERY_RETRY_DELAY,
};
use crate::{
    app_store::{
        AcceptedOperation, DurableOperationState, OutstandingPhase, OutstandingWork,
        ReservationError, ReserveOutcome, TerminalFailure,
    },
    group::SharedGroup,
    protocol::ClientReply,
};

fn admission_candidate(
    operation: AcceptedOperation,
    reply: ClientReply,
    durable_outstanding: bool,
) -> PendingAdmissionCandidate {
    PendingAdmissionCandidate {
        operation,
        replies: vec![reply],
        durable_outstanding,
    }
}

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

        let durable_state = record.durable_operation_state(operation);
        let durable_outstanding = match durable_state {
            DurableOperationState::ExactTerminal(failure) => {
                reply.send(render_terminal_failure(failure).to_string(), false);
                return Ok(());
            }
            DurableOperationState::ConflictingTerminal => {
                reply.send("ERR CONFLICTING_OUTSTANDING".to_string(), false);
                return Ok(());
            }
            DurableOperationState::ExactOutstanding(_) => true,
            DurableOperationState::ConflictingOutstanding | DurableOperationState::Absent => false,
        };

        let Some(reply) =
            self.attach_or_queue_pending_admission(group_id, operation, reply, durable_outstanding)
        else {
            return Ok(());
        };

        self.start_admission_barrier(group_id, incarnation, operation, reply, durable_outstanding)
    }

    fn start_admission_barrier(
        &mut self,
        group_id: GroupId,
        incarnation: GroupIncarnation,
        operation: AcceptedOperation,
        reply: ClientReply,
        durable_outstanding: bool,
    ) -> Result<(), String> {
        let driver = match self.serving_driver(group_id, incarnation) {
            Ok(driver) => driver,
            Err(response) => {
                Self::send_admission_failure(
                    admission_candidate(operation, reply, durable_outstanding),
                    &response,
                );
                return Ok(());
            }
        };

        let read_id = ReadId(self.next_read_id);
        let Some(next) = self.next_read_id.checked_add(1) else {
            Self::send_admission_failure(
                admission_candidate(operation, reply, durable_outstanding),
                "ERR READ_ID_EXHAUSTED",
            );
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
                client_id: operation.client_id(),
                candidates: vec![admission_candidate(operation, reply, durable_outstanding)],
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
                if driver_application_durability_failed(&error) {
                    return Err(format!(
                        "group {} application durability failed during admission: {error}",
                        group_id.get()
                    ));
                }
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

    fn attach_or_queue_pending_admission(
        &mut self,
        group_id: GroupId,
        operation: AcceptedOperation,
        reply: ClientReply,
        durable_outstanding: bool,
    ) -> Option<ClientReply> {
        if let Some(proposal_id) = self
            .pending_operations
            .get(&(group_id, operation.client_id()))
            .copied()
        {
            let pending = self
                .pending
                .get_mut(&proposal_id)
                .expect("pending operation index names a pending proposal");
            if pending.operation == Some(operation) {
                pending.replies.push(reply);
                pending.deadline = Some(Instant::now() + self.request_timeout);
                return None;
            }
        }

        if let Some(read_id) = self
            .pending_admission_operations
            .get(&(group_id, operation.client_id()))
            .copied()
        {
            let pending = self
                .pending_admissions
                .get_mut(&read_id)
                .expect("pending admission index names a read");
            if let Some(candidate) = pending
                .candidates
                .iter_mut()
                .find(|candidate| candidate.operation == operation)
            {
                candidate.replies.push(reply);
                candidate.durable_outstanding |= durable_outstanding;
            } else {
                pending
                    .candidates
                    .push(admission_candidate(operation, reply, durable_outstanding));
            }
            pending.deadline = Instant::now() + self.request_timeout;
            return None;
        }
        Some(reply)
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
                for candidate in pending.candidates {
                    Self::send_admission_failure(candidate, &response);
                }
                return Ok(());
            }
        };
        let record = self
            .groups
            .get(&pending.group_id)
            .expect("serving group remains installed")
            .record
            .clone();
        let mut proceeding = Vec::new();
        for candidate in pending.candidates {
            match driver.admission_decision(candidate.operation.replicated_command()) {
                CounterAdmissionDecision::SessionAlreadyOpen => {
                    if candidate.durable_outstanding {
                        record
                            .reconcile_authoritative_completion(candidate.operation)
                            .map_err(|error| {
                                format!(
                                    "authoritative session completion could not be published for group {} client {}: {error}",
                                    pending.group_id.get(),
                                    candidate.operation.client_id().get()
                                )
                            })?;
                    }
                    for reply in candidate.replies {
                        reply.send("OK SESSION already_open".to_string(), false);
                    }
                }
                CounterAdmissionDecision::CounterReplay(result) => {
                    if candidate.durable_outstanding {
                        record
                            .reconcile_authoritative_completion(candidate.operation)
                            .map_err(|error| {
                                format!(
                                    "authoritative replay completion could not be published for group {} client {}: {error}",
                                    pending.group_id.get(),
                                    candidate.operation.client_id().get()
                                )
                            })?;
                    }
                    let response = format!("OK REPLAY {}", render_counter_result(result));
                    for reply in candidate.replies {
                        reply.send(response.clone(), false);
                    }
                }
                CounterAdmissionDecision::Rejected(rejection) => {
                    let response = render_apply_rejection(rejection);
                    for reply in candidate.replies {
                        reply.send(response.clone(), false);
                    }
                }
                CounterAdmissionDecision::Proceed => proceeding.push(candidate),
            }
        }

        for candidate in proceeding {
            let Some(candidate) = self.attach_exact_pending_candidate(pending.group_id, candidate)
            else {
                continue;
            };
            if self.has_conflicting_outstanding(pending.group_id, candidate.operation) {
                for reply in candidate.replies {
                    reply.send("ERR CONFLICTING_OUTSTANDING".to_string(), false);
                }
                continue;
            }
            self.finish_proceeding_admission(pending.group_id, candidate)?;
        }
        Ok(())
    }

    fn attach_exact_pending_candidate(
        &mut self,
        group_id: GroupId,
        mut candidate: PendingAdmissionCandidate,
    ) -> Option<PendingAdmissionCandidate> {
        let Some(proposal_id) = self
            .pending_operations
            .get(&(group_id, candidate.operation.client_id()))
            .copied()
        else {
            return Some(candidate);
        };
        let pending = self
            .pending
            .get_mut(&proposal_id)
            .expect("pending operation index names a pending proposal");
        if pending.operation != Some(candidate.operation) {
            return Some(candidate);
        }
        pending.replies.append(&mut candidate.replies);
        pending.deadline = Some(Instant::now() + self.request_timeout);
        None
    }

    fn has_conflicting_outstanding(&self, group_id: GroupId, operation: AcceptedOperation) -> bool {
        if let Some(proposal_id) = self
            .pending_operations
            .get(&(group_id, operation.client_id()))
        {
            let pending = self
                .pending
                .get(proposal_id)
                .expect("pending operation index names a pending proposal");
            if pending.operation != Some(operation) {
                return true;
            }
        }
        let record = &self
            .groups
            .get(&group_id)
            .expect("serving group remains installed")
            .record;
        matches!(
            record.durable_operation_state(operation),
            DurableOperationState::ConflictingOutstanding
                | DurableOperationState::ConflictingTerminal
        )
    }

    fn finish_proceeding_admission(
        &mut self,
        group_id: GroupId,
        candidate: PendingAdmissionCandidate,
    ) -> Result<(), String> {
        let record = self
            .groups
            .get(&group_id)
            .expect("serving group remains installed")
            .record
            .clone();
        let reservation = match record.reserve(candidate.operation) {
            Ok(reservation) => reservation,
            Err(ReservationError::Rejected(error)) => {
                let response = format!("ERR ADMISSION {error}");
                for reply in candidate.replies {
                    reply.send(response.clone(), false);
                }
                return Ok(());
            }
            Err(ReservationError::PublicationUncertain(error)) => {
                return Err(format!(
                    "reservation publication is uncertain for group {} client {}: {error}",
                    group_id.get(),
                    candidate.operation.client_id().get()
                ));
            }
        };
        if reservation == ReserveOutcome::Reserved {
            crate::directed_failpoint("after_reservation_publication_before_managed_admission");
        }
        let class = match candidate.operation {
            AcceptedOperation::OpenSession { .. } => WorkClass::Control,
            AcceptedOperation::Counter { .. } => WorkClass::Command,
        };
        if let Err(refusal) = self.admit_client_proposal(
            group_id,
            class,
            candidate.operation.replicated_command(),
            Some(candidate.operation),
            candidate.replies,
        ) {
            Self::finish_reserved_admission(&record, candidate.operation, reservation, refusal)?;
        } else {
            self.deferred_recovery
                .remove(&(group_id, candidate.operation.client_id()));
        }
        Ok(())
    }

    pub(super) fn send_admission_failure(candidate: PendingAdmissionCandidate, response: &str) {
        let response = if candidate.durable_outstanding {
            "ERR UNKNOWN accepted operation remains durable".to_string()
        } else {
            response.to_string()
        };
        for reply in candidate.replies {
            reply.send(response.clone(), false);
        }
    }

    fn send_barrier_failure(candidate: PendingAdmissionCandidate, detail: &str) {
        let response = if candidate.durable_outstanding {
            "ERR UNKNOWN accepted operation remains durable".to_string()
        } else {
            format!("ERR NOT_COMMITTED {detail}")
        };
        for reply in candidate.replies {
            reply.send(response.clone(), false);
        }
    }

    fn take_pending_admission(&mut self, read_id: ReadId) -> Option<PendingAdmission> {
        let pending = self.pending_admissions.remove(&read_id)?;
        self.pending_admission_operations
            .remove(&(pending.group_id, pending.client_id));
        Some(pending)
    }

    fn finish_failed_admission_read(&mut self, read_id: ReadId, detail: &str) {
        let Some(pending) = self.take_pending_admission(read_id) else {
            return;
        };
        for candidate in pending.candidates {
            Self::send_barrier_failure(candidate, detail);
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
            for candidate in pending.candidates {
                Self::send_barrier_failure(candidate, "admission barrier deadline elapsed");
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
            for candidate in pending.candidates {
                Self::send_admission_failure(candidate, response);
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
        let durable_keys = self
            .groups
            .iter()
            .flat_map(|(group_id, entry)| {
                entry
                    .record
                    .policy()
                    .outstanding
                    .into_values()
                    .map(|outstanding| (*group_id, outstanding.operation.client_id()))
                    .collect::<Vec<_>>()
            })
            .collect::<std::collections::BTreeSet<_>>();
        self.deferred_recovery
            .retain(|key, _| durable_keys.contains(key));
        let now = Instant::now();
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
                let key = (*group_id, outstanding.operation.client_id());
                !self.pending_operations.contains_key(&key)
                    && self
                        .deferred_recovery
                        .get(&key)
                        .is_none_or(|retry_at| now >= *retry_at)
            })
            .collect::<Vec<_>>();
        for (group_id, outstanding) in recoverable {
            self.admit_recovered_outstanding(group_id, outstanding, now)?;
        }
        Ok(())
    }

    fn admit_recovered_outstanding(
        &mut self,
        group_id: GroupId,
        outstanding: OutstandingWork,
        now: Instant,
    ) -> Result<(), String> {
        let key = (group_id, outstanding.operation.client_id());
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
            self.recovery_refused = self.recovery_refused.saturating_add(1);
            self.deferred_recovery
                .insert(key, now + RECOVERY_RETRY_DELAY);
            return Ok(());
        };
        self.deferred_recovery.remove(&key);
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
                    Err(error) if driver_application_durability_failed(&error) => {
                        return Err(format!(
                            "group {} application durability failed while handling peer input: {error}",
                            frame.group_id.get()
                        ));
                    }
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
                    Err(error) if driver_application_durability_failed(&error) => {
                        return Err(format!(
                            "group {} application durability failed while ticking: {error}",
                            group_id.get()
                        ));
                    }
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
