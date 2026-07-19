//! No-signal quarantine for direct children that outlive bounded cleanup.

mod request;
mod worker;

use std::{
    os::unix::net::UnixStream,
    process::Child,
    sync::{mpsc, Arc, Mutex},
    thread,
};

use super::{ProcessLifetimeLease, TargetLifetimeLease};
use request::{AnchoredGroupReapRequest, ChildReapRequest, LeasedChildReapRequest};

#[cfg(test)]
thread_local! {
    static FAIL_NEXT_ADOPTION: std::cell::Cell<bool> = const {
        std::cell::Cell::new(false)
    };
}

#[derive(Debug, Default)]
pub(super) struct ReaperState {
    adopted: usize,
    reaped: usize,
    failures: Vec<String>,
    #[cfg(test)]
    injected_wait_errors: usize,
}

/// A worker that only polls and reaps adopted children; it never sends signals.
#[derive(Clone, Debug)]
pub(crate) struct NoSignalReaper {
    child_sender: mpsc::Sender<ChildReapRequest>,
    leased_child_sender: mpsc::Sender<LeasedChildReapRequest>,
    anchored_group_sender: mpsc::Sender<AnchoredGroupReapRequest>,
    state: Arc<Mutex<ReaperState>>,
}

impl NoSignalReaper {
    pub(crate) fn start() -> Result<Self, Box<dyn std::error::Error>> {
        let (child_sender, child_receiver) = mpsc::channel();
        let (leased_child_sender, leased_child_receiver) = mpsc::channel();
        let (anchored_group_sender, anchored_group_receiver) = mpsc::channel();
        let (ready_sender, ready_receiver) = mpsc::sync_channel(0);
        let state = Arc::new(Mutex::new(ReaperState::default()));
        let worker_state = state.clone();
        thread::Builder::new()
            .name("rafter-invariants-reaper".to_owned())
            .spawn(move || {
                let _ = ready_sender.send(());
                worker::reap_children(
                    &child_receiver,
                    &leased_child_receiver,
                    &anchored_group_receiver,
                    &worker_state,
                );
            })
            .map_err(|error| format!("start no-signal process reaper: {error}"))?;
        ready_receiver
            .recv()
            .map_err(|_| "process reaper exited before publishing readiness")?;
        Ok(Self {
            child_sender,
            leased_child_sender,
            anchored_group_sender,
            state,
        })
    }

    pub(crate) fn adopt(&self, child: Child, role: &'static str) -> Result<(), (Child, String)> {
        let request = ChildReapRequest::new(child, role);
        if let Some(detail) = injected_adoption_failure(request.role(), request.child_id()) {
            return Err(request.into_failure(detail));
        }
        let child_id = request.child_id();
        if let Err(error) = self.child_sender.send(request) {
            return Err(error.0.into_failure(format!(
                "transfer {role} to no-signal reaper: reaper channel closed for child {child_id}"
            )));
        }
        lock_state(&self.state).adopted += 1;
        Ok(())
    }

    pub(crate) fn adopt_anchored_group(
        &self,
        child: Child,
        control: UnixStream,
        lifetime: TargetLifetimeLease,
    ) -> Result<(), (Child, UnixStream, TargetLifetimeLease, String)> {
        let request = AnchoredGroupReapRequest::new(child, control, lifetime);
        let role = "target-group anchor";
        if let Some(detail) = injected_adoption_failure(role, request.child_id()) {
            return Err(request.into_failure(detail));
        }
        let child_id = request.child_id();
        if let Err(error) = self.anchored_group_sender.send(request) {
            return Err(error.0.into_failure(format!(
                "transfer {role} to no-signal reaper: reaper channel closed for child {child_id}"
            )));
        }
        lock_state(&self.state).adopted += 1;
        Ok(())
    }

    pub(crate) fn adopt_leased(
        &self,
        child: Child,
        lifetime: ProcessLifetimeLease,
    ) -> Result<(), (Child, ProcessLifetimeLease, String)> {
        let request = LeasedChildReapRequest::new(child, lifetime);
        let role = "internal observer command";
        if let Some(detail) = injected_adoption_failure(role, request.child_id()) {
            return Err(request.into_failure(detail));
        }
        let child_id = request.child_id();
        if let Err(error) = self.leased_child_sender.send(request) {
            return Err(error.0.into_failure(format!(
                "transfer {role} to no-signal reaper: reaper channel closed for child {child_id}"
            )));
        }
        lock_state(&self.state).adopted += 1;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn inject_next_wait_error(&self) {
        lock_state(&self.state).injected_wait_errors += 1;
    }

    #[cfg(test)]
    pub(crate) fn snapshot(&self) -> ReaperSnapshot {
        let state = lock_state(&self.state);
        ReaperSnapshot {
            adopted: state.adopted,
            reaped: state.reaped,
            failures: state.failures.clone(),
        }
    }
}

#[cfg(test)]
fn injected_adoption_failure(role: &'static str, child_id: u32) -> Option<String> {
    FAIL_NEXT_ADOPTION
        .with(|fail| fail.replace(false))
        .then(|| {
            format!("transfer {role} to no-signal reaper: injected adoption failure for child {child_id}")
        })
}

#[cfg(not(test))]
fn injected_adoption_failure(_role: &'static str, _child_id: u32) -> Option<String> {
    None
}

#[cfg(test)]
pub(crate) fn fail_next_reaper_adoption() {
    FAIL_NEXT_ADOPTION.with(|fail| fail.set(true));
}

#[cfg(test)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ReaperSnapshot {
    pub(crate) adopted: usize,
    pub(crate) reaped: usize,
    pub(crate) failures: Vec<String>,
}

pub(super) fn lock_state(
    state: &Arc<Mutex<ReaperState>>,
) -> std::sync::MutexGuard<'_, ReaperState> {
    state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
