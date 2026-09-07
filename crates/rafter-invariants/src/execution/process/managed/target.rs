//! Target placement transitions, observation proofs, and anchor release.

use std::time::Instant;

#[cfg(test)]
use std::cell::RefCell;

use super::super::{
    process_group_observation, GroupObservation, ProcessAnchorState, ProcessSignal, SignalDelivery,
    TargetLeaseState, TargetMemberState,
};
use super::ManagedProcess;

mod placement;
mod quiescence;

use quiescence::classify_target_quiescence;
#[cfg(test)]
pub(crate) use quiescence::classify_target_quiescence_for_test;

#[cfg(test)]
thread_local! {
    static NEXT_WRAPPER_EXIT_OBSERVATION_HOOK: RefCell<Option<Box<dyn FnOnce()>>> =
        RefCell::new(None);
}

#[cfg(test)]
pub(crate) fn before_next_wrapper_exit_observation(hook: impl FnOnce() + 'static) {
    NEXT_WRAPPER_EXIT_OBSERVATION_HOOK.with(|next| {
        assert!(
            next.borrow_mut().replace(Box::new(hook)).is_none(),
            "wrapper-exit observation hook was already armed"
        );
    });
}

#[cfg(test)]
fn run_wrapper_exit_observation_hook() {
    NEXT_WRAPPER_EXIT_OBSERVATION_HOOK.with(|next| {
        if let Some(hook) = next.borrow_mut().take() {
            hook();
        }
    });
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum TargetPlacement {
    UnpublishedInWrapperGroup,
    PublishedInWrapperGroup { launcher: u32 },
    JoiningAnchorGroup { launcher: u32 },
    InAnchorGroup { launcher: u32 },
    Finished,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TargetQuiescence {
    process_group: u32,
    placement: TargetPlacement,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TargetObservation {
    rss_kib: u64,
    quiescence: Option<TargetQuiescence>,
}

impl TargetObservation {
    pub(crate) fn rss_kib(self) -> u64 {
        self.rss_kib
    }

    pub(crate) fn into_quiescence(self) -> Option<TargetQuiescence> {
        self.quiescence
    }
}

impl ManagedProcess {
    /// Observes the target group, treating a closed observation window as the
    /// error it has always been reported as.
    ///
    /// Callers whose loop already knows what a closed window means should use
    /// `try_observe_target_members` and say so explicitly.
    pub(crate) fn observe_target_members(
        &self,
        observation_deadline: Instant,
        lifecycle_deadline: Instant,
    ) -> Result<TargetObservation, Box<dyn std::error::Error>> {
        self.try_observe_target_members(observation_deadline, lifecycle_deadline)?
            .ok_or_else(|| "process-group observer exhausted its absolute deadline".into())
    }

    /// Observes the target group, or reports that the observation window closed
    /// before an observation could start.
    pub(crate) fn try_observe_target_members(
        &self,
        observation_deadline: Instant,
        lifecycle_deadline: Instant,
    ) -> Result<Option<TargetObservation>, Box<dyn std::error::Error>> {
        let observer = self
            .observer
            .as_ref()
            .ok_or("target process-group observation requires a bound observer")?;
        let lifetime = self
            .target_lifetime
            .as_ref()
            .ok_or("target process-group observation requires its lifetime lease")?;
        let process_group = self.target.id();
        let exited_before = self.target.exit_observed()?;
        let lease_before = lifetime.observe()?;
        let GroupObservation::Observed(observation) = process_group_observation(
            process_group,
            Some(process_group),
            observer,
            observation_deadline,
            lifecycle_deadline,
        )?
        else {
            return Ok(None);
        };
        #[cfg(test)]
        run_wrapper_exit_observation_hook();
        let wrapper_exited = self.wrapper.exit_observed()?;
        let lease_after = lifetime.observe()?;
        let exited_after = self.target.exit_observed()?;
        if !exited_before && !exited_after && observation.anchor != Some(ProcessAnchorState::Alive)
        {
            return Err(format!(
                "process observer omitted live group anchor {process_group}: {:?}",
                observation.anchor
            )
            .into());
        }
        if exited_after && !self.target.signal_was_sent(ProcessSignal::Kill) {
            return Err(format!(
                "process-group anchor {process_group} exited before release or SIGKILL"
            )
            .into());
        }
        let quiescent = classify_target_quiescence(
            lease_before,
            lease_after,
            wrapper_exited,
            observation.target_members,
        )?;
        Ok(Some(TargetObservation {
            rss_kib: observation.rss_kib,
            quiescence: quiescent.then_some(TargetQuiescence {
                process_group,
                placement: self.placement,
            }),
        }))
    }

    pub(crate) fn release_target_anchor(
        &mut self,
        proof: TargetQuiescence,
        deadline: Instant,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.validate_quiescence(proof)?;
        let status = self.target.release(deadline)?;
        if !status.success() {
            return Err(format!(
                "process-group anchor {} exited {:?} after release",
                self.target.id(),
                status.code()
            )
            .into());
        }
        self.placement = TargetPlacement::Finished;
        Ok(())
    }

    pub(crate) fn reap_target_anchor_after_kill(
        &mut self,
        proof: TargetQuiescence,
        deadline: Instant,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.validate_quiescence(proof)?;
        if !self.target.signal_was_sent(ProcessSignal::Kill) {
            return Err("cannot reap a process-group anchor before SIGKILL delivery".into());
        }
        self.target
            .wait_until(deadline)?
            .ok_or("process-group anchor was not reaped after SIGKILL")?;
        self.placement = TargetPlacement::Finished;
        Ok(())
    }

    pub(crate) fn signal_target_group(
        &mut self,
        signal: ProcessSignal,
    ) -> Result<SignalDelivery, Box<dyn std::error::Error>> {
        self.target.signal(signal)
    }

    pub(super) fn release_unjoined_anchor(
        &mut self,
        deadline: Instant,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if !self.wrapper.is_reaped()
            || !matches!(
                self.placement,
                TargetPlacement::UnpublishedInWrapperGroup
                    | TargetPlacement::PublishedInWrapperGroup { .. }
            )
        {
            return Err(
                "cannot release an unjoined anchor before its wrapper group is reaped".into(),
            );
        }
        let status = self.target.release(deadline)?;
        if !status.success() {
            return Err(format!(
                "unjoined process-group anchor {} exited {:?} after release",
                self.target.id(),
                status.code()
            )
            .into());
        }
        self.placement = TargetPlacement::Finished;
        Ok(())
    }

    fn validate_quiescence(
        &self,
        proof: TargetQuiescence,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if proof.process_group != self.target.id() || proof.placement != self.placement {
            return Err(format!(
                "stale target quiescence proof for group {} at {:?}; current group {} at {:?}",
                proof.process_group,
                proof.placement,
                self.target.id(),
                self.placement
            )
            .into());
        }
        if !matches!(
            self.placement,
            TargetPlacement::JoiningAnchorGroup { .. } | TargetPlacement::InAnchorGroup { .. }
        ) {
            return Err(format!(
                "target quiescence at {:?} cannot release or reap the anchor",
                self.placement
            )
            .into());
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn record_target_kill_for_test(&mut self) {
        self.target.record_signal_for_test(ProcessSignal::Kill);
    }
}
