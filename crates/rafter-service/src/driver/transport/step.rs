//! One step of the protocol, and everything that step obliges.
//!
//! Every way an input reaches the group runs through here, and each of them owes
//! the same four things afterwards whichever way it went: route the report,
//! reconcile the membership the runtime moved, drain what a poison captured, and
//! publish metrics. The error path owes them too, which is why they are not
//! written once per caller.

#![allow(
    clippy::wildcard_imports,
    reason = "the driver is one state machine deliberately split across focused files, each opening the shared driver namespace"
)]

use crate::transport::{AuthenticatedPeerValidator, RaftTransport};

use super::state::{StepFailure, TransportDriverState};
use super::*;

impl<G, A, R, T, V> TransportDriverState<G, A, R, T, V>
where
    G: Clone + Ord + Debug + Send + Sync + 'static,
    A: ReplicatedStateMachine + Send + 'static,
    A::Command: Send + 'static,
    A::CommandResult: Clone + Send + 'static,
    A::Query: Clone + Send + 'static,
    A::QueryResult: Send + 'static,
    R: PersistedRaftRuntime + Send + 'static,
    T: RaftTransport<G>,
    V: AuthenticatedPeerValidator<G, T::PeerPrincipal> + Send + Sync + 'static,
{
    pub(super) fn reject_if_shutting_down(&self) -> Result<(), ManagedDriverError> {
        if self.shutting_down {
            return Err(ManagedDriverError::ShuttingDown);
        }
        Ok(())
    }

    pub(super) fn group_mut(&mut self) -> Result<&mut RaftGroup<G, A, R>, ManagedDriverError> {
        self.group.as_mut().ok_or(ManagedDriverError::NoGroup)
    }

    /// Steps the group and routes everything the step produced or captured.
    ///
    /// The group error survives here rather than being wrapped, because the
    /// caller decides how a client hears about it: `tick` and `deliver` report
    /// to a driver, and `begin_write` reports to a client through the typed
    /// write mapping. A single wrapped error could serve only one of them.
    ///
    /// The poison drain runs on both paths. A step that poisons can return
    /// `Ok`, and a step that fails is the likeliest place for a poison to have
    /// happened; draining only where the report exists would strand exactly the
    /// waiters a poison captured.
    pub(super) fn step_group(
        &mut self,
        input: GroupInput<G, A::Command>,
    ) -> Result<(), StepFailure<A::Error, R::Error>> {
        let Some(group) = self.group.as_mut() else {
            return Err(StepFailure::NoGroup);
        };
        let stepped = group.step_with_options(input, StepReportOptions::without_metrics());
        let result = match stepped {
            Ok(report) => {
                self.route_report(report);
                Ok(())
            }
            Err(error) => Err(StepFailure::Group(error)),
        };
        self.reconcile_membership();
        self.drain_poisoned_waiters();
        self.publish_metrics();
        result
    }

    /// Routes every membership fact the group has moved through and not yet
    /// handed back.
    ///
    /// **Runs after every stepping outcome, `Err` included, and that is the
    /// point.** A step that fails returns no report while the runtime has
    /// already appended, truncated, committed, or installed the configuration
    /// that moved, so a driver that routed only successful reports left the loss
    /// window open until some later successful step happened to arrive — and a
    /// stale peer set is exactly what keeps that later step from arriving.
    /// `RaftGroup::drain_membership_events` is the app layer's error-path
    /// companion for that, and this is the driver's use of it.
    ///
    /// The drained batch is one transaction like a report's, so an error path
    /// that carries a contradiction publishes nothing rather than publishing what
    /// preceded it.
    ///
    /// Empty after a successful step, because the report already carried the
    /// delta and the group advanced its own mark handing it over. So this costs
    /// one comparison on the path that works and closes the path that does not.
    ///
    /// The events are drained before any of them is routed, because routing
    /// reaches the transport and the group is borrowed to drain.
    pub(super) fn reconcile_membership(&mut self) {
        let events = {
            let Some(group) = self.group.as_mut() else {
                return;
            };
            group.drain_membership_events()
        };
        self.route_membership_events(&events);
    }

    /// Steps the group with a leadership transfer, reporting the rejection it
    /// saw.
    ///
    /// Everything else is [`TransportDriverState::step_group`], the poison drain
    /// on both paths included. The only difference is the return value: a
    /// transfer has no waiter table, because it is created and resolved inside
    /// one call, so the one fact its caller needs has to be read out of the
    /// report before `route_report` consumes it.
    ///
    /// That need is why the transfer used to step the group itself, and stepping
    /// it directly is what left this — a call site the poison drain's own design
    /// listed by name — undrained on both paths. A proposal a transfer step
    /// poisoned over stayed unresolved until some unrelated later call rescued
    /// it, and a supervisor that reacted to the failed transfer by releasing
    /// told its client `DriverReleased` for a group that had poisoned under it.
    pub(super) fn step_transfer(
        &mut self,
        target: NodeId,
    ) -> Result<Option<TransferLeadershipError>, StepFailure<A::Error, R::Error>> {
        let Some(group) = self.group.as_mut() else {
            return Err(StepFailure::NoGroup);
        };
        let stepped = group.step_with_options(
            GroupInput::TransferLeadership { target },
            StepReportOptions::without_metrics(),
        );
        let result = match stepped {
            Ok(report) => {
                let rejection =
                    report
                        .leadership_transfer_events
                        .iter()
                        .find_map(|event| match event {
                            LeadershipTransferEvent::Rejected {
                                target: event_target,
                                reason,
                                leader_hint,
                            } if *event_target == target => {
                                Some(TransferLeadershipError::Rejected {
                                    reason: *reason,
                                    leader_hint: *leader_hint,
                                })
                            }
                            _ => None,
                        });
                self.route_report(report);
                Ok(rejection)
            }
            Err(error) => Err(StepFailure::Group(error)),
        };
        self.reconcile_membership();
        self.drain_poisoned_waiters();
        self.publish_metrics();
        result
    }

    pub(super) fn step(
        &mut self,
        input: GroupInput<G, A::Command>,
    ) -> Result<(), ManagedDriverError> {
        self.step_group(input).map_err(|failure| match failure {
            StepFailure::NoGroup => ManagedDriverError::NoGroup,
            StepFailure::Group(error) => ManagedDriverError::Group {
                cause: ErrorCause::new(error),
            },
        })
    }

    /// Drains a restart's committed suffix through the group's ordered recovery
    /// operation rather than the raw output pump.
    ///
    /// The two differ only when this replica opened below its own snapshot
    /// boundary — the crash window between promoting an inbound snapshot and
    /// installing it into the application — and that is exactly the case a
    /// driver reaches. Every adoption path routes here one call after the group
    /// is built, so there is no window in which a caller could restore the state
    /// machine ahead of this; the raw pump would apply the suffix over the gap
    /// instead, which is durable corruption no later report can find.
    pub(super) fn apply_recovery_outputs(
        &mut self,
        outputs: Vec<RaftOutput>,
    ) -> Result<(), ManagedDriverError> {
        let applied = self.group_mut()?.apply_recovery_outputs(outputs);
        let result = match applied {
            Ok(report) => {
                self.route_report(report);
                Ok(())
            }
            Err(error) => Err(ManagedDriverError::Group {
                cause: ErrorCause::new(error),
            }),
        };
        self.reconcile_membership();
        self.drain_poisoned_waiters();
        self.publish_metrics();
        result
    }
}
