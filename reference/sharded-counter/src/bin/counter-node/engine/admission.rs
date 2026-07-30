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
    ClientId, GroupId, GroupIncarnation, GroupLifecycle, WorkClass as PolicyWorkClass,
};

use super::{
    driver_application_durability_failed, render_apply_rejection, render_counter_result,
    render_terminal_failure, ClientAdmissionRefusal, Engine, PendingClient, WorkKind,
    MAX_PEERS_PER_LOOP, RECOVERY_RETRY_DELAY,
};
use crate::{
    app_store::{
        AcceptedOperation, DurableOperationState, OutstandingPhase, OutstandingWork,
        ReservationError, ReserveOutcome, TerminalFailure,
    },
    group::SharedGroup,
    protocol::ClientReply,
};

#[derive(Debug)]
struct PendingAdmissionCandidate {
    operation: AcceptedOperation,
    replies: Vec<ClientReply>,
    durable_outstanding: bool,
}

#[derive(Debug)]
struct QueuedAdmission {
    candidates: Vec<PendingAdmissionCandidate>,
    deadline: Instant,
}

#[derive(Debug)]
pub(super) struct PendingAdmission {
    group_id: GroupId,
    incarnation: GroupIncarnation,
    client_id: ClientId,
    candidates: Vec<PendingAdmissionCandidate>,
    deadline: Instant,
    successor: Option<QueuedAdmission>,
}

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

fn unresolved_admission_response(durable_outstanding: bool, detail: &str) -> String {
    if durable_outstanding {
        "ERR UNKNOWN accepted operation remains durable".to_string()
    } else {
        let detail = detail.strip_prefix("ERR ").unwrap_or(detail);
        format!("ERR UNKNOWN authoritative admission unresolved: {detail}")
    }
}

impl PendingAdmission {
    pub(super) const fn group_id(&self) -> GroupId {
        self.group_id
    }

    pub(super) fn candidate_count(&self) -> usize {
        self.candidates.len() + self.successor_count()
    }

    pub(super) fn successor_count(&self) -> usize {
        self.successor
            .as_ref()
            .map_or(0, |successor| successor.candidates.len())
    }

    fn queue_successor(
        &mut self,
        candidate: PendingAdmissionCandidate,
        deadline: Instant,
        bound: usize,
    ) -> Result<(), PendingAdmissionCandidate> {
        let successor = self.successor.get_or_insert_with(|| QueuedAdmission {
            candidates: Vec::new(),
            deadline,
        });
        if let Some(existing) = successor
            .candidates
            .iter_mut()
            .find(|existing| existing.operation == candidate.operation)
        {
            existing.replies.extend(candidate.replies);
            existing.durable_outstanding |= candidate.durable_outstanding;
            return Ok(());
        }
        if successor.candidates.len() >= bound {
            return Err(candidate);
        }
        successor.candidates.push(candidate);
        Ok(())
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
        self.start_admission_batch(
            group_id,
            incarnation,
            operation.client_id(),
            QueuedAdmission {
                candidates: vec![admission_candidate(operation, reply, durable_outstanding)],
                deadline: Instant::now() + self.request_timeout,
            },
        )
    }

    fn start_admission_batch(
        &mut self,
        group_id: GroupId,
        incarnation: GroupIncarnation,
        client_id: ClientId,
        batch: QueuedAdmission,
    ) -> Result<(), String> {
        debug_assert!(!batch.candidates.is_empty());
        debug_assert!(batch
            .candidates
            .iter()
            .all(|candidate| candidate.operation.client_id() == client_id));
        if Instant::now() >= batch.deadline {
            for candidate in batch.candidates {
                Self::send_unresolved_admission(candidate, "queued admission deadline elapsed");
            }
            return Ok(());
        }
        let driver = match self.serving_driver(group_id, incarnation) {
            Ok(driver) => driver,
            Err(response) => {
                for candidate in batch.candidates {
                    Self::send_unresolved_admission(candidate, &response);
                }
                return Ok(());
            }
        };

        let read_id = ReadId(self.next_read_id);
        let Some(next) = self.next_read_id.checked_add(1) else {
            for candidate in batch.candidates {
                Self::send_unresolved_admission(candidate, "read id exhausted");
            }
            return Ok(());
        };
        self.next_read_id = next;
        self.admission_barriers_started = self.admission_barriers_started.saturating_add(1);
        self.pending_admission_operations
            .insert((group_id, client_id), read_id);
        self.pending_admissions.insert(
            read_id,
            PendingAdmission {
                group_id,
                incarnation,
                client_id,
                candidates: batch.candidates,
                deadline: batch.deadline,
                successor: None,
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
                )?;
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
            let candidate = admission_candidate(operation, reply, durable_outstanding);
            let deadline = Instant::now() + self.request_timeout;
            let bound = self.max_group_queue;
            let pending = self
                .pending_admissions
                .get_mut(&read_id)
                .expect("pending admission index names a read");
            if let Err(candidate) = pending.queue_successor(candidate, deadline, bound) {
                Self::send_unresolved_admission(candidate, "admission successor queue is full");
            }
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
            )?,
            ReadEvent::Canceled {
                read_id,
                reason,
                leader_hint,
            } => self.finish_failed_admission_read(
                *read_id,
                &format!("read canceled: {reason:?} leader={leader_hint:?}"),
            )?,
            ReadEvent::FreshnessUnavailable { .. } => {}
            _ => return Err("unsupported admission read event".to_string()),
        }
        Ok(())
    }

    fn finish_granted_admission(&mut self, read_id: ReadId) -> Result<(), String> {
        let Some(pending) = self.take_pending_admission(read_id) else {
            return Ok(());
        };
        let group_id = pending.group_id;
        let incarnation = pending.incarnation;
        let client_id = pending.client_id;
        let successor = pending.successor;
        let driver = match self.serving_driver(group_id, incarnation) {
            Ok(driver) => driver,
            Err(response) => {
                for candidate in pending.candidates {
                    Self::send_unresolved_admission(candidate, &response);
                }
                return self.start_successor_admission(group_id, incarnation, client_id, successor);
            }
        };
        let record = self
            .groups
            .get(&group_id)
            .expect("serving group remains installed")
            .record
            .clone();
        let mut proceeding = Vec::new();
        for candidate in pending.candidates {
            let decision = driver.admission_decision(candidate.operation.replicated_command());
            if !matches!(&decision, CounterAdmissionDecision::Proceed) {
                record
                    .reconcile_authoritative_completion(candidate.operation)
                    .map_err(|error| {
                        format!(
                            "authoritative terminal completion could not be published for group {} client {}: {error}",
                            group_id.get(),
                            candidate.operation.client_id().get()
                        )
                    })?;
            }
            match decision {
                CounterAdmissionDecision::SessionAlreadyOpen => {
                    for reply in candidate.replies {
                        reply.send("OK SESSION already_open".to_string(), false);
                    }
                }
                CounterAdmissionDecision::CounterReplay(result) => {
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
            let Some(candidate) = self.attach_exact_pending_candidate(group_id, candidate) else {
                continue;
            };
            if self.has_conflicting_outstanding(group_id, candidate.operation) {
                for reply in candidate.replies {
                    reply.send("ERR CONFLICTING_OUTSTANDING".to_string(), false);
                }
                continue;
            }
            self.finish_proceeding_admission(group_id, candidate)?;
        }
        self.start_successor_admission(group_id, incarnation, client_id, successor)
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

    fn send_unresolved_admission(candidate: PendingAdmissionCandidate, detail: &str) {
        let response = unresolved_admission_response(candidate.durable_outstanding, detail);
        for reply in candidate.replies {
            reply.send(response.clone(), false);
        }
    }

    pub(super) fn finish_pending_admission(pending: PendingAdmission, detail: &str) {
        for candidate in pending.candidates {
            Self::send_unresolved_admission(candidate, detail);
        }
        if let Some(successor) = pending.successor {
            for candidate in successor.candidates {
                Self::send_unresolved_admission(candidate, detail);
            }
        }
    }

    fn send_barrier_failure(candidate: PendingAdmissionCandidate, detail: &str) {
        Self::send_unresolved_admission(candidate, detail);
    }

    fn take_pending_admission(&mut self, read_id: ReadId) -> Option<PendingAdmission> {
        let pending = self.pending_admissions.remove(&read_id)?;
        self.pending_admission_operations
            .remove(&(pending.group_id, pending.client_id));
        Some(pending)
    }

    fn start_successor_admission(
        &mut self,
        group_id: GroupId,
        incarnation: GroupIncarnation,
        client_id: ClientId,
        successor: Option<QueuedAdmission>,
    ) -> Result<(), String> {
        match successor {
            Some(batch) => self.start_admission_batch(group_id, incarnation, client_id, batch),
            None => Ok(()),
        }
    }

    fn finish_failed_admission_read(
        &mut self,
        read_id: ReadId,
        detail: &str,
    ) -> Result<(), String> {
        let Some(pending) = self.take_pending_admission(read_id) else {
            return Ok(());
        };
        let group_id = pending.group_id;
        let incarnation = pending.incarnation;
        let client_id = pending.client_id;
        for candidate in pending.candidates {
            Self::send_barrier_failure(candidate, detail);
        }
        self.start_successor_admission(group_id, incarnation, client_id, pending.successor)
    }

    pub(super) fn expire_admission_reads(&mut self, now: Instant) -> Result<(), String> {
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
            self.start_successor_admission(
                pending.group_id,
                pending.incarnation,
                pending.client_id,
                pending.successor,
            )?;
        }
        Ok(())
    }

    pub(super) fn cancel_admission_reads_for_group(
        &mut self,
        group_id: GroupId,
        response: &str,
    ) -> Result<(), String> {
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
                Self::send_unresolved_admission(candidate, response);
            }
            self.start_successor_admission(
                pending.group_id,
                pending.incarnation,
                pending.client_id,
                pending.successor,
            )?;
        }
        Ok(())
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
        self.cancel_admission_reads_for_group(group_id, "GROUP_POISONED")?;
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
            if self.direct_control_plane_required(frame.group_id) {
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
            if self.direct_control_plane_required(group_id) {
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

    fn direct_control_plane_required(&self, group_id: GroupId) -> bool {
        // `SLOW` is a process-fixture delay for non-control dispatches. It may
        // deliberately hold every managed worker on other groups, but it must
        // not manufacture a Raft outage while a test is arranging queue
        // pressure. Do not bypass ordering for a group whose own dispatch is
        // delayed. Admission barriers need the same direct tick/peer path until
        // their read ends.
        let other_groups_hold_every_worker = self.delayed.len() >= self.worker_capacity
            && self
                .delayed
                .iter()
                .all(|delayed| delayed.dispatch.group_id != group_id);
        other_groups_hold_every_worker
            || self
                .pending_admissions
                .values()
                .any(|pending| pending.group_id == group_id)
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use rafter_reference_sharded_counter::{ClientId, GroupId, GroupIncarnation, SessionEpoch};

    use super::{unresolved_admission_response, PendingAdmission, PendingAdmissionCandidate};
    use crate::app_store::AcceptedOperation;

    fn open_session(epoch: u64) -> AcceptedOperation {
        AcceptedOperation::OpenSession {
            client_id: ClientId::new(7),
            epoch: SessionEpoch::new(epoch).expect("test epoch is nonzero"),
        }
    }

    fn candidate(epoch: u64) -> PendingAdmissionCandidate {
        PendingAdmissionCandidate {
            operation: open_session(epoch),
            replies: Vec::new(),
            durable_outstanding: false,
        }
    }

    fn submitted_admission() -> PendingAdmission {
        PendingAdmission {
            group_id: GroupId::new(3),
            incarnation: GroupIncarnation::new(1).expect("test incarnation is nonzero"),
            client_id: ClientId::new(7),
            candidates: vec![candidate(1)],
            deadline: Instant::now() + Duration::from_secs(1),
            successor: None,
        }
    }

    #[test]
    fn submitted_generation_is_sealed_from_later_invocations() {
        let mut pending = submitted_admission();
        let successor_deadline = Instant::now() + Duration::from_secs(1);

        pending
            .queue_successor(candidate(2), successor_deadline, 2)
            .expect("successor accepts a later invocation");
        pending
            .queue_successor(candidate(2), successor_deadline, 2)
            .expect("an exact successor retry coalesces");

        assert_eq!(pending.candidates.len(), 1);
        assert_eq!(pending.candidates[0].operation, open_session(1));
        assert_eq!(pending.successor_count(), 1);
        assert_eq!(
            pending
                .successor
                .as_ref()
                .expect("the successor exists")
                .candidates[0]
                .operation,
            open_session(2)
        );
    }

    #[test]
    fn successor_generation_is_bounded_without_extending_its_deadline() {
        let mut pending = submitted_admission();
        let first_deadline = Instant::now() + Duration::from_secs(1);
        pending
            .queue_successor(candidate(2), first_deadline, 1)
            .expect("the bounded successor accepts its first candidate");

        let rejected = pending
            .queue_successor(candidate(3), first_deadline + Duration::from_secs(1), 1)
            .expect_err("a distinct candidate cannot exceed the successor bound");

        assert_eq!(rejected.operation, open_session(3));
        assert_eq!(pending.successor_count(), 1);
        assert_eq!(
            pending
                .successor
                .as_ref()
                .expect("the successor exists")
                .deadline,
            first_deadline
        );
    }

    #[test]
    fn poison_cancellation_is_unknown_without_a_non_commitment_claim() {
        let response = unresolved_admission_response(false, "GROUP_POISONED");

        assert_eq!(
            response,
            "ERR UNKNOWN authoritative admission unresolved: GROUP_POISONED"
        );
        assert!(!response.contains("NOT_COMMITTED"));
    }
}
