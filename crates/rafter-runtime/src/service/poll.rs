//! Ordered output dispatch and non-waiting completion consumption.

use super::driver::Terminal;
use super::{CommittedCommand, DurableService, ServiceError, ServiceEvent, ServicePoll};
use crate::application::{
    ApplicationEntry, ApplicationEvent, ApplicationSubmitRejection, DurableApplication,
};
use rafter::{LogIndex, Output, SnapshotChunkSource};
use rafter_storage::{RaftHardStateStore, RaftLogSegment, RaftSnapshotStore};

impl<H, L, S, T, A> DurableService<H, L, S, T, A>
where
    H: RaftHardStateStore + Send + 'static,
    L: RaftLogSegment + Send + 'static,
    S: RaftSnapshotStore + SnapshotChunkSource + Send + 'static,
    T: ApplicationEntry,
    A: DurableApplication<T>,
{
    /// Performs bounded dispatch work and returns at most one owned event.
    ///
    /// `None` can mean an internal dispatch advanced without an external event;
    /// keep polling while draining or recovering. This never waits for outstanding
    /// I/O. Snapshot handoff joins an already idle application thread.
    /// For notification-driven scheduling use [`Self::poll_next`]. Consume
    /// all events even after closing admission with `begin_shutdown`.
    ///
    /// # Errors
    /// A worker, adapter, storage, or unserviceable application-admission failure
    /// fences the incarnation. No later success or read grant is released.
    pub fn poll(&mut self) -> Result<Option<ServiceEvent<T, A>>, ServiceError> {
        self.poll_next().map(|step| match step {
            ServicePoll::Event(event) => Some(event),
            ServicePoll::Progress | ServicePoll::Pending => None,
        })
    }

    /// Performs one bounded step with an explicit scheduling disposition.
    ///
    /// Continue polling after `Event` or `Progress`. On `Pending`, inspect
    /// lifecycle/admission state, then wait using the retained notification
    /// installed with [`Self::recover_with_wake`]. Do not clear a notification
    /// after polling: a worker may have published completion in between. If a
    /// per-turn dispatch budget is exhausted before `Pending`, schedule another
    /// turn yourself rather than relying on a future worker notification.
    ///
    /// `Pending` is not proof that the service is idle: I/O may be outstanding,
    /// or application state may be handed off for installation. New local input
    /// and `resume_snapshot` require another poll even without a worker wake.
    ///
    /// # Errors
    /// Uses the same terminal error contract as [`Self::poll`].
    pub fn poll_next(&mut self) -> Result<ServicePoll<T, A>, ServiceError> {
        if self.terminal == Some(Terminal::Failed) {
            return Err(ServiceError::Failed);
        }
        let Some(application) = self.application.as_ref() else {
            return Ok(ServicePoll::Pending);
        };
        match application.try_complete() {
            Ok(Some(ApplicationEvent::Applied(done))) => {
                return Ok(ServicePoll::Event(ServiceEvent::Applied(done)))
            }
            Ok(Some(ApplicationEvent::Failed(failure))) => {
                self.terminal = Some(Terminal::Failed);
                return Ok(ServicePoll::Event(ServiceEvent::ApplicationFailed(failure)));
            }
            Err(_) => return self.poison(ServiceError::ApplicationStopped),
            Ok(None) => {}
        }
        if !self.pending.is_empty() {
            return self.submit_pending();
        }
        let mut progressed = false;
        if self.outputs.is_empty() {
            match self.node.try_complete() {
                Ok(Some(completed)) => {
                    self.outputs = completed.into_outputs().into();
                    progressed = true;
                }
                Err(error) => return self.poison(ServiceError::Persistence(error)),
                Ok(None) => {}
            }
        }
        if self.recovering && self.idle() {
            self.recovering = false;
            progressed = true;
        }
        let Some(output) = self.outputs.pop_front() else {
            return Ok(if progressed {
                ServicePoll::Progress
            } else {
                ServicePoll::Pending
            });
        };
        match output {
            Output::Apply {
                index,
                term,
                payload,
                local_proposal_id,
            } => {
                let Some(first) = self
                    .dispatched
                    .0
                    .checked_add(1)
                    .filter(|first| *first <= index.0)
                else {
                    return self.poison(ServiceError::OutputOrder);
                };
                // A caught adapter panic must not leave a skipped command live.
                self.terminal = Some(Terminal::Failed);
                let entry = match (self.adapter)(CommittedCommand {
                    first_index: LogIndex(first),
                    index,
                    term,
                    payload,
                    local_proposal_id,
                }) {
                    Ok(entry) => entry,
                    Err(error) => return self.poison(ServiceError::Adapter(error)),
                };
                if entry.first_log_index() != LogIndex(first) || entry.log_index() != index {
                    return self.poison(ServiceError::AdapterRange);
                }
                self.terminal = None;
                self.pending.push_back(entry);
                self.submit_pending()
            }
            Output::ApplySnapshot { snapshot } => self
                .handoff_snapshot(snapshot)
                .map(|event| event.map_or(ServicePoll::Pending, ServicePoll::Event)),
            Output::ReadIndexGranted {
                read_id,
                read_index,
            } => {
                let Some(node) = self.node.ready_node() else {
                    return self.poison(ServiceError::OutputOrder);
                };
                let needed = node.committed_application_index_through(read_index);
                let output = Output::ReadIndexGranted {
                    read_id,
                    read_index,
                };
                if self.durable_through().is_some_and(|floor| floor >= needed) {
                    Ok(ServicePoll::Event(ServiceEvent::Output(output)))
                } else {
                    self.outputs.push_front(output);
                    Ok(ServicePoll::Pending)
                }
            }
            output => Ok(ServicePoll::Event(ServiceEvent::Output(output))),
        }
    }

    fn submit_pending(&mut self) -> Result<ServicePoll<T, A>, ServiceError> {
        let Some(application) = self.application.as_ref() else {
            return self.poison(ServiceError::ApplicationStopped);
        };
        let through = self.pending.back().map(ApplicationEntry::log_index);
        match application.try_submit_prefix(&mut self.pending) {
            Ok(_) => {
                if let Some(through) = through {
                    self.dispatched = through;
                }
                Ok(ServicePoll::Progress)
            }
            Err(ApplicationSubmitRejection::Full { .. } | ApplicationSubmitRejection::Stopped)
                if application.is_busy() =>
            {
                Ok(ServicePoll::Pending)
            }
            Err(error) => self.poison(ServiceError::ApplicationAdmission(error)),
        }
    }
}
