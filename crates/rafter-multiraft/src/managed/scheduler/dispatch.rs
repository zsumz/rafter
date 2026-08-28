//! Pass arming, group turns, and exact dispatch completion.
//!
//! A pass is planned once from the ready set and is never revised afterwards.
//! Beginning a turn occupies a worker only when the group still has work to
//! service; releasing one requires a disposition for every item that was
//! handed out, in the order it was handed out.

use std::{fmt::Debug, num::NonZeroU64, sync::Arc};

use crate::managed::{
    ArmPass, BeginDispatch, CompletionError, Dispatch, DispatchCompletion,
    DispatchCompletionPermit, DispatchId, IdentityError, PassCompletion, PassId, PassPlan,
    SkipReason, SkippedOpportunity, WorkDisposition,
};

use super::{InFlight, ManagedScheduler, OpenPass};

impl<G, T> ManagedScheduler<G, T>
where
    G: Clone + Ord + Debug,
{
    /// Arms an immutable pass from the current ready set.
    ///
    /// # Errors
    ///
    /// Returns [`IdentityError::PassExhausted`] when no stable pass identity
    /// remains.
    pub fn arm_pass(&mut self) -> Result<ArmPass<G>, IdentityError> {
        if let Some(pass) = &self.open_pass {
            return Ok(ArmPass::AlreadyArmed(pass.id));
        }
        if self.ready.is_empty() {
            return Ok(ArmPass::Idle);
        }
        let Some(raw_id) = NonZeroU64::new(self.next_pass_id) else {
            return Err(IdentityError::PassExhausted);
        };
        self.next_pass_id = self.next_pass_id.checked_add(1).unwrap_or(0);
        let pass_id = PassId::new(raw_id);
        let groups = self.ready.iter().cloned().collect::<Vec<_>>();
        self.open_pass = Some(OpenPass {
            id: pass_id,
            groups: groups.iter().cloned().collect(),
            planned: groups.len(),
            dispatched: 0,
            skipped: 0,
        });
        self.passes_armed += 1;
        Ok(ArmPass::Armed(PassPlan { pass_id, groups }))
    }

    /// Opens the next planned group turn when a worker is free.
    ///
    /// # Errors
    ///
    /// Returns [`IdentityError::DispatchExhausted`] without consuming a pass
    /// position when no stable dispatch identity remains.
    pub fn begin_dispatch(&mut self) -> Result<BeginDispatch<G, T>, IdentityError> {
        let Some(pass) = &self.open_pass else {
            return Ok(BeginDispatch::NoPass);
        };
        if pass.groups.is_empty() {
            let Some(pass) = self.open_pass.take() else {
                return Ok(BeginDispatch::NoPass);
            };
            self.passes_completed += 1;
            return Ok(BeginDispatch::PassComplete(PassCompletion {
                pass_id: pass.id,
                planned: pass.planned,
                dispatched: pass.dispatched,
                skipped: pass.skipped,
            }));
        }
        if self.in_flight.len() >= self.config.workers().get() {
            return Ok(BeginDispatch::WorkersOccupied);
        }

        let Some(pass) = self.open_pass.as_mut() else {
            return Ok(BeginDispatch::NoPass);
        };
        let Some(group_id) = pass.groups.pop_front() else {
            return Ok(BeginDispatch::NoPass);
        };
        let pass_id = pass.id;
        let Some(group) = self.groups.get_mut(&group_id) else {
            pass.skipped += 1;
            return Ok(BeginDispatch::Skipped(SkippedOpportunity {
                pass_id,
                group_id,
                reason: SkipReason::Unavailable,
            }));
        };
        let skip = if !group.available {
            Some(SkipReason::Unavailable)
        } else if group.in_flight {
            Some(SkipReason::InFlight)
        } else if group.len() == 0 {
            Some(SkipReason::Empty)
        } else {
            None
        };
        if let Some(reason) = skip {
            pass.skipped += 1;
            self.refresh_ready(&group_id);
            return Ok(BeginDispatch::Skipped(SkippedOpportunity {
                pass_id,
                group_id,
                reason,
            }));
        }
        let Some(raw_id) = NonZeroU64::new(self.next_dispatch_id) else {
            pass.groups.push_front(group_id);
            return Err(IdentityError::DispatchExhausted);
        };
        let dispatch_id = DispatchId::new(raw_id);
        self.next_dispatch_id = self.next_dispatch_id.checked_add(1).unwrap_or(0);
        let quota = group.quota.get();
        let mut items = Vec::with_capacity(quota.min(group.len()));
        for _ in 0..quota {
            let Some(item) = group.pop() else {
                break;
            };
            items.push(item);
        }
        group.in_flight = true;
        self.queued -= items.len();
        self.in_flight_work += items.len();
        self.ready.remove(&group_id);
        pass.dispatched += 1;
        self.in_flight.insert(
            dispatch_id,
            InFlight {
                group_id: group_id.clone(),
                work_ids: items.iter().map(|item| item.work_id).collect(),
            },
        );
        Ok(BeginDispatch::Dispatched(Dispatch {
            authority: Arc::clone(&self.authority),
            pass_id,
            dispatch_id,
            group_id,
            items,
        }))
    }

    /// Releases one dispatch after exact per-item disposition.
    ///
    /// # Errors
    ///
    /// Refuses unknown, partial, extra, or mismatched completion without
    /// releasing the worker or changing conservation counters.
    pub fn complete_dispatch(
        &mut self,
        permit: &DispatchCompletionPermit<G>,
        dispositions: &[WorkDisposition],
    ) -> Result<DispatchCompletion<G>, CompletionError> {
        let dispatch_id = permit.dispatch_id;
        if !Arc::ptr_eq(&self.authority, &permit.authority) {
            return Err(CompletionError::ForeignDispatch(dispatch_id));
        }
        let in_flight = self
            .in_flight
            .get(&dispatch_id)
            .ok_or(CompletionError::UnknownDispatch(dispatch_id))?;
        if in_flight.group_id != permit.group_id {
            return Err(CompletionError::ForeignDispatch(dispatch_id));
        }
        if dispositions.len() != in_flight.work_ids.len() {
            return Err(CompletionError::WrongItemCount {
                dispatch_id,
                expected: in_flight.work_ids.len(),
                actual: dispositions.len(),
            });
        }
        for (expected, disposition) in in_flight.work_ids.iter().zip(dispositions) {
            if *expected != disposition.work_id() {
                return Err(CompletionError::WrongWork {
                    dispatch_id,
                    expected: *expected,
                    actual: disposition.work_id(),
                });
            }
        }
        let group_id = in_flight.group_id.clone();
        let group = self
            .groups
            .get_mut(&group_id)
            .ok_or(CompletionError::UnknownDispatch(dispatch_id))?;
        group.in_flight = false;

        let Some(in_flight) = self.in_flight.remove(&dispatch_id) else {
            return Err(CompletionError::UnknownDispatch(dispatch_id));
        };
        let serviced = dispositions
            .iter()
            .filter(|item| matches!(item, WorkDisposition::Serviced(_)))
            .count();
        let failed = dispositions.len() - serviced;
        self.in_flight_work -= dispositions.len();
        self.serviced += serviced as u64;
        self.failed += failed as u64;
        self.refresh_ready(&group_id);
        Ok(DispatchCompletion {
            dispatch_id,
            group_id: in_flight.group_id,
            serviced,
            failed,
        })
    }
}
