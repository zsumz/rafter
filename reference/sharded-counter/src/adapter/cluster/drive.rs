use rafter::NodeId;
use rafter_app::{
    error::ErrorCause,
    group::{GroupFatalState, GroupInput, GroupStepReport},
    proposal::ProposalEvent,
};
use rafter_multiraft::{
    managed::{ArmPass, BeginDispatch, WorkClass},
    MultiRaftErrorKind,
};

use crate::{CounterResult, GroupIncarnation, GroupLifecycle};

use super::{
    AdapterError, CounterApplyResult, CounterDispatch, DelayedDispatch, DriveReport, DriveTurn,
    DrivenDisposition, DrivenItem, ManagedCounterCluster, PeerTrafficRefusal, RoutedPeerEnvelope,
};

type CounterReport = GroupStepReport<crate::GroupId, CounterApplyResult>;

impl ManagedCounterCluster {
    /// Executes one deterministic delivery/scheduling round.
    ///
    /// # Errors
    ///
    /// Returns the exact transport or group failure.
    pub fn drive_round(&mut self) -> Result<DriveReport, AdapterError> {
        self.restore_poisoned_for_explicit_drain();
        let mut report = DriveReport::default();
        self.progress_delayed(&mut report)?;
        self.route_network(&mut report)?;
        self.run_one_pass(&mut report)?;
        Ok(report)
    }

    /// Runs deterministic managed passes and peer delivery until quiescence.
    ///
    /// # Errors
    ///
    /// Returns the exact transport/remote failure or progress-budget boundary.
    pub fn drive_until_idle(&mut self, max_rounds: usize) -> Result<DriveReport, AdapterError> {
        let mut total = DriveReport::default();
        for _ in 0..max_rounds {
            self.restore_poisoned_for_explicit_drain();
            let mut round = DriveReport::default();
            let delayed_progress = self.progress_delayed(&mut round)?;
            self.route_network(&mut round)?;
            let progressed = self.run_one_pass(&mut round)?;
            let network_pending = !self.network.is_empty();
            total.merge(round);
            if !progressed
                && !delayed_progress
                && !network_pending
                && self.delayed.is_empty()
                && self.host.managed_metrics().queued == 0
            {
                return Ok(total);
            }
        }
        Err(AdapterError::ProgressBudgetExhausted { rounds: max_rounds })
    }

    fn restore_poisoned_for_explicit_drain(&mut self) {
        for group_id in &self.poisoned {
            let _ = self.host.set_available(group_id, true);
        }
    }

    fn run_one_pass(&mut self, report: &mut DriveReport) -> Result<bool, AdapterError> {
        match self
            .host
            .arm_pass()
            .map_err(|_| AdapterError::IdentityExhausted)?
        {
            ArmPass::Armed(plan) => report.plans.push(plan.groups),
            ArmPass::AlreadyArmed(_) => {}
            ArmPass::Idle => return Ok(false),
        }
        loop {
            match self
                .host
                .begin_dispatch()
                .map_err(|_| AdapterError::IdentityExhausted)?
            {
                BeginDispatch::Dispatched(dispatch) => {
                    report.opportunities += 1;
                    let delay = self
                        .service_delays
                        .get(&dispatch.group_id)
                        .copied()
                        .unwrap_or(0);
                    if delay == 0 {
                        self.execute_dispatch(dispatch, report)?;
                    } else {
                        self.delayed.push(DelayedDispatch {
                            remaining_rounds: delay,
                            dispatch,
                        });
                    }
                }
                BeginDispatch::Skipped(_) => {}
                BeginDispatch::WorkersOccupied | BeginDispatch::PassComplete(_) => return Ok(true),
                BeginDispatch::NoPass => return Ok(false),
            }
        }
    }

    fn progress_delayed(&mut self, report: &mut DriveReport) -> Result<bool, AdapterError> {
        if self.delayed.is_empty() {
            return Ok(false);
        }
        let mut ready = Vec::new();
        for delayed in &mut self.delayed {
            delayed.remaining_rounds = delayed.remaining_rounds.saturating_sub(1);
        }
        let mut index = 0;
        while index < self.delayed.len() {
            if self.delayed[index].remaining_rounds == 0 {
                ready.push(self.delayed.swap_remove(index).dispatch);
            } else {
                index += 1;
            }
        }
        ready.sort_by_key(|dispatch| dispatch.dispatch_id);
        for dispatch in ready {
            self.execute_dispatch(dispatch, report)?;
        }
        Ok(true)
    }

    fn execute_dispatch(
        &mut self,
        dispatch: CounterDispatch,
        report: &mut DriveReport,
    ) -> Result<(), AdapterError> {
        let managed = self
            .host
            .execute_dispatch(dispatch)
            .expect("the adapter executes only dispatches issued by its host");
        let mut turn = DriveTurn {
            pass_id: managed.pass_id,
            dispatch_id: managed.dispatch_id,
            group_id: managed.group_id,
            items: Vec::with_capacity(managed.items.len()),
        };
        for item in managed.items {
            let disposition = match item.result {
                Ok(group_report) => {
                    report.serviced += 1;
                    self.collect_report(group_report)?;
                    DrivenDisposition::Serviced
                }
                Err(error) => {
                    report.failed += 1;
                    let kind = error.kind();
                    self.record_failed_work(item.work_id, managed.group_id, kind);
                    if kind == MultiRaftErrorKind::DriverPoisoned {
                        self.poisoned.insert(managed.group_id);
                    }
                    DrivenDisposition::Failed { kind }
                }
            };
            turn.items.push(DrivenItem {
                work_id: item.work_id,
                class: item.class,
                disposition,
            });
        }
        report.turns.push(turn);
        Ok(())
    }

    fn collect_report(&mut self, report: CounterReport) -> Result<(), AdapterError> {
        for event in report.proposal_events {
            if let ProposalEvent::Applied {
                local_proposal_id,
                result,
                ..
            } = event
            {
                self.completed.insert(local_proposal_id, result);
                self.complete_pending(local_proposal_id, result);
            }
        }
        for applied in report.applied {
            if let Some(proposal_id) = applied.local_proposal_id {
                self.completed.insert(proposal_id, applied.result);
                if let Some(slot) = self.groups.get_mut(&report.group_id) {
                    slot.applied_index = applied.index;
                    if let CounterApplyResult::Counter(
                        CounterResult::Added { value } | CounterResult::Value { value },
                    ) = applied.result
                    {
                        slot.value = value;
                    }
                }
                self.complete_pending(proposal_id, applied.result);
            }
        }
        let incarnation = self
            .groups
            .get(&report.group_id)
            .map_or_else(GroupIncarnation::first, |slot| slot.incarnation);
        let mut envelopes = report.peer_messages.into_iter();
        while let Some(envelope) = envelopes.next() {
            let bound = self.network_config.max_pending_messages.get();
            if self.network.len() >= bound {
                let pending = std::iter::once(RoutedPeerEnvelope {
                    incarnation,
                    envelope,
                })
                .chain(envelopes.map(|envelope| RoutedPeerEnvelope {
                    incarnation,
                    envelope,
                }))
                .collect::<Vec<_>>()
                .into_boxed_slice();
                return Err(AdapterError::NetworkFull { bound, pending });
            }
            self.network.push_back(RoutedPeerEnvelope {
                incarnation,
                envelope,
            });
        }
        Ok(())
    }

    fn route_network(&mut self, report: &mut DriveReport) -> Result<(), AdapterError> {
        let mut remaining = self.network.len();
        while remaining != 0 {
            remaining -= 1;
            let routed = self
                .network
                .pop_front()
                .expect("remaining counts queued envelopes");
            let incarnation = routed.incarnation;
            let envelope = routed.envelope;
            if let Err(reason) =
                self.admit_group(envelope.group_id, incarnation, crate::WorkClass::Control)
            {
                report.refused_peer_traffic.push(PeerTrafficRefusal {
                    group_id: envelope.group_id,
                    incarnation,
                    reason,
                });
                continue;
            }
            if envelope.to == NodeId(1) {
                let group_id = envelope.group_id;
                match self.host.admit(
                    &group_id,
                    WorkClass::Control,
                    GroupInput::PeerMessage { envelope },
                ) {
                    Ok(_) => {}
                    Err(rejected) => {
                        let GroupInput::PeerMessage { envelope } = rejected.payload else {
                            unreachable!("the network admitted a peer envelope");
                        };
                        self.network.push_back(RoutedPeerEnvelope {
                            incarnation,
                            envelope,
                        });
                        break;
                    }
                }
                continue;
            }
            let group_id = envelope.group_id;
            let node_id = envelope.to;
            let Some(peer) = self.peers.get_mut(&(group_id, node_id)) else {
                return Err(AdapterError::Lifecycle {
                    group_id,
                    expected: GroupLifecycle::Recovering,
                    actual: self.groups.get(&group_id).map(|slot| slot.lifecycle),
                });
            };
            match peer.step(GroupInput::PeerMessage { envelope }) {
                Ok(peer_report) => self.collect_report(peer_report)?,
                Err(error) => {
                    if matches!(peer.fatal_state(), GroupFatalState::Poisoned { .. }) {
                        report.remote_failures += 1;
                        self.poisoned.insert(group_id);
                    } else {
                        return Err(AdapterError::RemoteStep {
                            group_id,
                            node_id,
                            cause: ErrorCause::new(error),
                        });
                    }
                }
            }
        }
        Ok(())
    }
}
