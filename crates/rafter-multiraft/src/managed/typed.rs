use std::{fmt::Debug, num::NonZeroUsize};

use rafter_app::group::{GroupInput, GroupStepReport};

use crate::{
    driver::DriverErrorKind,
    error::{MultiRaftError, OpenGroupRejected},
    metrics::MultiRaftMetrics,
    typed::{TypedGroupDriver, TypedMultiRaftHost},
};

use super::{
    AdmissionReceipt, AdmissionRejected, ArmPass, BeginDispatch, CompletionError, Dispatch,
    DispatchCompletion, DispatchId, FailedQueuedItem, GroupStateError, IdentityError,
    ManagedConfig, ManagedMetrics, ManagedScheduler, RegisterError, RemoveError, WorkClass,
    WorkDisposition, WorkId,
};

/// Erased typed driver returned when a managed group is removed.
pub type BoxedTypedGroupDriver<G, C, R> =
    Box<dyn TypedGroupDriver<G, Command = C, CommandResult = R>>;

/// Boxed refusal returning one typed group input that took no queue slot.
pub type ManagedAdmissionRejected<G, C> = Box<AdmissionRejected<G, GroupInput<G, C>>>;

/// Why a driver could not be opened in a managed host.
///
/// This enum is exhaustive because opening composes exactly two ownership
/// gates: scheduler registration followed by manual-host adoption.
#[derive(Debug)]
pub enum ManagedOpenError<G> {
    /// The scheduler already owns the group key.
    Scheduler(RegisterError<G>),
    /// The manual typed host refused the driver.
    Host(MultiRaftError<G>),
}

/// A refused managed open that returns the caller's driver.
#[derive(Debug)]
pub struct ManagedOpenRejected<G, D> {
    /// Typed refusal.
    pub error: ManagedOpenError<G>,
    /// Unmodified driver.
    pub driver: D,
}

/// One item outcome from a managed typed-host dispatch.
#[derive(Debug)]
pub struct ManagedItemOutcome<G, R> {
    /// Admission identity.
    pub work_id: WorkId,
    /// Class that selected this item.
    pub class: WorkClass,
    /// Full typed group report or exact host failure.
    pub result: Result<GroupStepReport<G, R>, MultiRaftError<G>>,
}

/// Lossless result of stepping every item in one managed dispatch.
#[derive(Debug)]
#[must_use = "managed reports contain the only copy of each group step report"]
pub struct ManagedDispatchReport<G, R> {
    /// Ready-set pass containing the dispatch.
    pub pass_id: super::PassId,
    /// Dispatch that was stepped.
    pub dispatch_id: DispatchId,
    /// Group that received the turn.
    pub group_id: G,
    /// Per-item typed reports and failures, in dispatch order.
    pub items: Vec<ManagedItemOutcome<G, R>>,
    /// Whether exact scheduler occupancy release succeeded.
    ///
    /// The item reports remain available even if this internal cross-check
    /// fails; no report is summarized away by a scheduling error.
    pub completion: Result<DispatchCompletion<G>, CompletionError>,
}

/// A foreign or stale dispatch refused before any driver was stepped.
#[derive(Debug)]
pub struct ExecuteDispatchRejected<G, C> {
    /// Exact validation failure.
    pub error: CompletionError,
    /// Unmodified dispatch, including every accepted payload.
    pub dispatch: Dispatch<G, GroupInput<G, C>>,
}

/// Bounded managed composition over the lower-level typed host.
///
/// A caller admits typed [`GroupInput`] values, arms a deterministic pass, and
/// begins dispatches explicitly. A dispatch occupies a worker until
/// [`ManagedTypedMultiRaftHost::execute_dispatch`] has stepped every selected
/// item and recorded an exact per-item disposition.
#[derive(Debug)]
pub struct ManagedTypedMultiRaftHost<G, C, R> {
    manual: TypedMultiRaftHost<G, C, R>,
    scheduler: ManagedScheduler<G, GroupInput<G, C>>,
}

impl<G, C, R> ManagedTypedMultiRaftHost<G, C, R>
where
    G: Clone + Ord + Debug,
{
    /// Creates an empty managed typed host.
    #[must_use]
    pub fn new(config: ManagedConfig) -> Self {
        Self {
            manual: TypedMultiRaftHost::new(),
            scheduler: ManagedScheduler::new(config),
        }
    }

    /// Opens a driver and registers its scheduling key as unavailable.
    ///
    /// # Errors
    ///
    /// Returns the driver unchanged when either layer refuses the key.
    pub fn open_group<D>(
        &mut self,
        group_id: &G,
        driver: D,
        quota: Option<NonZeroUsize>,
    ) -> Result<(), ManagedOpenRejected<G, D>>
    where
        D: TypedGroupDriver<G, Command = C, CommandResult = R> + 'static,
    {
        if let Err(error) = self.scheduler.register_group(group_id.clone(), quota) {
            return Err(ManagedOpenRejected {
                error: ManagedOpenError::Scheduler(error),
                driver,
            });
        }
        match self.manual.open_group(group_id.clone(), driver) {
            Ok(()) => Ok(()),
            Err(OpenGroupRejected { error, driver }) => {
                let _ = self.scheduler.remove_group(group_id);
                Err(ManagedOpenRejected {
                    error: ManagedOpenError::Host(error),
                    driver,
                })
            }
        }
    }

    /// Removes an idle scheduler key and returns its manual driver.
    ///
    /// # Errors
    ///
    /// Refuses removal while accepted work is queued or in flight.
    pub fn remove_group(
        &mut self,
        group_id: &G,
    ) -> Result<Option<BoxedTypedGroupDriver<G, C, R>>, RemoveError<G>> {
        if !self.scheduler.remove_group(group_id)? {
            return Ok(None);
        }
        Ok(self.manual.remove_group(group_id))
    }

    /// Checks whether removal would succeed without detaching either layer.
    ///
    /// # Errors
    ///
    /// Refuses removal while accepted work is queued or in flight.
    pub fn can_remove_group(&self, group_id: &G) -> Result<bool, RemoveError<G>> {
        self.scheduler.can_remove_group(group_id)
    }

    /// Explicitly fails queued inputs and returns every payload to the caller.
    ///
    /// In-flight work remains owned by its dispatch and must complete normally.
    ///
    /// # Errors
    ///
    /// Returns [`GroupStateError::UnknownGroup`] for an unopened key.
    pub fn fail_queued(
        &mut self,
        group_id: &G,
    ) -> Result<Vec<FailedQueuedItem<GroupInput<G, C>>>, GroupStateError<G>> {
        self.scheduler.fail_queued(group_id)
    }

    /// Sets whether a group participates in newly armed passes.
    ///
    /// # Errors
    ///
    /// Returns [`GroupStateError::UnknownGroup`] for an unopened key.
    pub fn set_available(
        &mut self,
        group_id: &G,
        available: bool,
    ) -> Result<(), GroupStateError<G>> {
        self.scheduler.set_available(group_id, available)
    }

    /// Admits one typed group input through both queue bounds.
    ///
    /// # Errors
    ///
    /// Returns the input unchanged with a typed admission refusal.
    pub fn admit(
        &mut self,
        group_id: &G,
        class: WorkClass,
        input: GroupInput<G, C>,
    ) -> Result<AdmissionReceipt, ManagedAdmissionRejected<G, C>> {
        self.scheduler
            .admit(group_id, class, input)
            .map_err(Box::new)
    }

    /// Arms one immutable ready-set pass.
    ///
    /// # Errors
    ///
    /// Returns an explicit identity-exhaustion failure.
    pub fn arm_pass(&mut self) -> Result<ArmPass<G>, IdentityError> {
        self.scheduler.arm_pass()
    }

    /// Begins the next group turn, occupying a worker only for nonempty work.
    ///
    /// # Errors
    ///
    /// Returns an explicit identity-exhaustion failure.
    pub fn begin_dispatch(&mut self) -> Result<BeginDispatch<G, GroupInput<G, C>>, IdentityError> {
        self.scheduler.begin_dispatch()
    }

    /// Steps every item in a dispatch and releases its occupancy exactly once.
    ///
    /// A permanent driver failure marks only that group unavailable. The
    /// driver is retained for caller-owned recovery/removal policy, every item
    /// receives an explicit failure, and later groups in the pass remain
    /// dispatchable.
    ///
    /// # Errors
    ///
    /// Returns a foreign or stale dispatch unchanged before any group step.
    pub fn execute_dispatch(
        &mut self,
        dispatch: Dispatch<G, GroupInput<G, C>>,
    ) -> Result<ManagedDispatchReport<G, R>, ExecuteDispatchRejected<G, C>> {
        if let Err(error) = self.scheduler.validate_dispatch(&dispatch) {
            return Err(ExecuteDispatchRejected { error, dispatch });
        }
        let pass_id = dispatch.pass_id;
        let dispatch_id = dispatch.dispatch_id;
        let completion_permit = dispatch.completion_permit();
        let group_id = dispatch.group_id;
        let mut dispositions = Vec::with_capacity(dispatch.items.len());
        let mut outcomes = Vec::with_capacity(dispatch.items.len());
        let mut poisoned = false;
        for item in dispatch.items {
            let work_id = item.work_id;
            let class = item.class;
            let result = self.manual.step_group(&group_id, item.payload);
            let disposition = match &result {
                Ok(_) => WorkDisposition::Serviced(work_id),
                Err(error) => {
                    if matches!(
                        error,
                        MultiRaftError::Driver {
                            kind: DriverErrorKind::Poisoned,
                            ..
                        }
                    ) {
                        poisoned = true;
                    }
                    WorkDisposition::Failed(work_id)
                }
            };
            dispositions.push(disposition);
            outcomes.push(ManagedItemOutcome {
                work_id,
                class,
                result,
            });
        }
        let completion = self
            .scheduler
            .complete_dispatch(&completion_permit, &dispositions);
        if poisoned {
            let _ = self.scheduler.set_available(&group_id, false);
        }
        Ok(ManagedDispatchReport {
            pass_id,
            dispatch_id,
            group_id,
            items: outcomes,
            completion,
        })
    }

    /// Scheduler metrics, including queue conservation counters.
    #[must_use]
    pub fn managed_metrics(&self) -> ManagedMetrics {
        self.scheduler.metrics()
    }

    /// Per-group Raft metrics from the manual typed host.
    #[must_use]
    pub fn raft_metrics(&self) -> MultiRaftMetrics<G> {
        self.manual.metrics()
    }
}
