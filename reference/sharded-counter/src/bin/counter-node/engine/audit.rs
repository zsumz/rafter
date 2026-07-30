//! Independent pass-event audit for the process scheduler.

use std::collections::{BTreeMap, BTreeSet};

use rafter_multiraft::managed::{
    PassCompletion, SkipReason, SkippedOpportunity, WorkClass, WorkId,
};
use rafter_reference_sharded_counter::GroupId;

use super::CounterDispatch;

#[derive(Debug, Default)]
struct ShadowGroup {
    available: bool,
    queued: BTreeMap<WorkId, WorkClass>,
    in_flight: BTreeSet<WorkId>,
}

#[derive(Debug)]
struct OpenPass {
    pass_id: u64,
    planned: BTreeSet<GroupId>,
    terminal: BTreeSet<GroupId>,
    dispatched: usize,
    skipped: usize,
}

#[derive(Debug, Default)]
pub(super) struct Audit {
    pub(super) plans: u64,
    pub(super) opportunities: u64,
    pub(super) passes_completed: u64,
    pub(super) certified_passes: u64,
    pub(super) invalid_plans: u64,
    pub(super) invalid_turns: u64,
    pub(super) plan_digest: u64,
    pub(super) turn_digest: u64,
    per_group: BTreeMap<GroupId, u64>,
    groups: BTreeMap<GroupId, ShadowGroup>,
    open: Option<OpenPass>,
}

impl Audit {
    pub(super) fn register_group(&mut self, group_id: GroupId) {
        if self
            .groups
            .insert(group_id, ShadowGroup::default())
            .is_some()
        {
            self.invalid_turns += 1;
        }
        self.per_group.entry(group_id).or_default();
    }

    pub(super) fn set_available(&mut self, group_id: GroupId, available: bool) {
        let Some(group) = self.groups.get_mut(&group_id) else {
            self.invalid_turns += 1;
            return;
        };
        group.available = available;
    }

    pub(super) fn remove_group(&mut self, group_id: GroupId) {
        let Some(group) = self.groups.remove(&group_id) else {
            self.invalid_turns += 1;
            return;
        };
        if !group.queued.is_empty() || !group.in_flight.is_empty() {
            self.invalid_turns += 1;
        }
    }

    pub(super) fn observe_admission(
        &mut self,
        group_id: GroupId,
        work_id: WorkId,
        class: WorkClass,
    ) {
        let Some(group) = self.groups.get_mut(&group_id) else {
            self.invalid_turns += 1;
            return;
        };
        if group.queued.insert(work_id, class).is_some() || group.in_flight.contains(&work_id) {
            self.invalid_turns += 1;
        }
    }

    pub(super) fn observe_failed_queued(&mut self, group_id: GroupId, work_ids: &[WorkId]) {
        let Some(group) = self.groups.get_mut(&group_id) else {
            self.invalid_turns += 1;
            return;
        };
        for work_id in work_ids {
            if group.queued.remove(work_id).is_none() {
                self.invalid_turns += 1;
            }
        }
    }

    pub(super) fn observe_plan(&mut self, pass_id: u64, groups: &[GroupId]) {
        self.plans += 1;
        let expected = self
            .groups
            .iter()
            .filter_map(|(group_id, group)| {
                (group.available && group.in_flight.is_empty() && !group.queued.is_empty())
                    .then_some(*group_id)
            })
            .collect::<Vec<_>>();
        if self.open.is_some()
            || groups.is_empty()
            || groups.windows(2).any(|pair| pair[0] >= pair[1])
            || groups != expected
        {
            self.invalid_plans += 1;
        }
        Self::mix(&mut self.plan_digest, pass_id);
        let mut planned = BTreeSet::new();
        for group in groups {
            Self::mix(&mut self.plan_digest, u64::from(group.get()));
            planned.insert(*group);
            self.per_group.entry(*group).or_default();
        }
        self.open = Some(OpenPass {
            pass_id,
            planned,
            terminal: BTreeSet::new(),
            dispatched: 0,
            skipped: 0,
        });
    }

    pub(super) fn observe_dispatch(&mut self, dispatch: &CounterDispatch) {
        if dispatch
            .items
            .windows(2)
            .any(|pair| pair[0].class > pair[1].class)
        {
            self.invalid_turns += 1;
        }
        let Some(group) = self.groups.get_mut(&dispatch.group_id) else {
            self.invalid_turns += 1;
            self.observe_terminal(dispatch.pass_id.get(), dispatch.group_id, true);
            return;
        };
        if !group.available || !group.in_flight.is_empty() || dispatch.items.is_empty() {
            self.invalid_turns += 1;
        }
        for item in &dispatch.items {
            if group.queued.remove(&item.work_id) != Some(item.class)
                || !group.in_flight.insert(item.work_id)
            {
                self.invalid_turns += 1;
            }
        }
        self.observe_terminal(dispatch.pass_id.get(), dispatch.group_id, true);
        Self::mix(&mut self.turn_digest, dispatch.dispatch_id.get());
        for item in &dispatch.items {
            Self::mix(&mut self.turn_digest, item.work_id.get());
            Self::mix(
                &mut self.turn_digest,
                match item.class {
                    WorkClass::Control => 1,
                    WorkClass::Command => 2,
                    WorkClass::Snapshot => 3,
                    WorkClass::Bulk => 4,
                },
            );
        }
        *self.per_group.entry(dispatch.group_id).or_default() += 1;
    }

    pub(super) fn observe_skip(&mut self, skipped: &SkippedOpportunity<GroupId>) {
        let expected =
            self.groups
                .get(&skipped.group_id)
                .map_or(SkipReason::Unavailable, |group| {
                    if !group.available {
                        SkipReason::Unavailable
                    } else if !group.in_flight.is_empty() {
                        SkipReason::InFlight
                    } else {
                        SkipReason::Empty
                    }
                });
        if skipped.reason != expected {
            self.invalid_turns += 1;
        }
        self.observe_terminal(skipped.pass_id.get(), skipped.group_id, false);
        Self::mix(
            &mut self.turn_digest,
            match skipped.reason {
                SkipReason::Unavailable => 11,
                SkipReason::InFlight => 12,
                SkipReason::Empty => 13,
            },
        );
    }

    pub(super) fn observe_dispatch_completion(
        &mut self,
        group_id: GroupId,
        work_ids: &[WorkId],
        poisoned: bool,
    ) {
        let Some(group) = self.groups.get_mut(&group_id) else {
            self.invalid_turns += 1;
            return;
        };
        for work_id in work_ids {
            if !group.in_flight.remove(work_id) {
                self.invalid_turns += 1;
            }
        }
        if poisoned {
            group.available = false;
        }
    }

    pub(super) fn observe_completion(&mut self, completion: PassCompletion) {
        self.passes_completed += 1;
        let Some(open) = self.open.take() else {
            self.invalid_plans += 1;
            return;
        };
        let valid = open.pass_id == completion.pass_id.get()
            && open.planned.len() == completion.planned
            && open.terminal == open.planned
            && open.dispatched == completion.dispatched
            && open.skipped == completion.skipped
            && completion.planned == completion.dispatched + completion.skipped;
        if valid {
            self.certified_passes += 1;
        } else {
            self.invalid_plans += 1;
        }
    }

    fn observe_terminal(&mut self, pass_id: u64, group_id: GroupId, dispatched: bool) {
        self.opportunities += 1;
        Self::mix(&mut self.turn_digest, pass_id);
        Self::mix(&mut self.turn_digest, u64::from(group_id.get()));
        let Some(open) = self.open.as_mut() else {
            self.invalid_turns += 1;
            return;
        };
        if open.pass_id != pass_id
            || !open.planned.contains(&group_id)
            || !open.terminal.insert(group_id)
        {
            self.invalid_turns += 1;
            return;
        }
        if dispatched {
            open.dispatched += 1;
        } else {
            open.skipped += 1;
        }
    }

    pub(super) fn fairness(&self) -> (usize, u64) {
        let observed = self
            .per_group
            .values()
            .copied()
            .filter(|count| *count != 0)
            .collect::<Vec<_>>();
        let coverage = observed.len();
        let widest_gap = observed
            .iter()
            .max()
            .zip(observed.iter().min())
            .map_or(0, |(max, min)| max - min);
        (coverage, widest_gap)
    }

    fn mix(digest: &mut u64, value: u64) {
        const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;
        *digest ^= value;
        *digest = digest.wrapping_mul(FNV_PRIME);
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use rafter_multiraft::managed::{ManagedConfig, ManagedScheduler};

    use super::*;

    fn work_ids() -> [WorkId; 2] {
        let bound = NonZeroUsize::new(2).expect("test bound is nonzero");
        let config = ManagedConfig::new(bound, bound, bound, bound).expect("test bounds are valid");
        let mut scheduler = ManagedScheduler::new(config);
        let group = GroupId::new(1);
        scheduler
            .register_group(group, None)
            .expect("test group registers");
        [
            scheduler
                .admit(&group, WorkClass::Command, ())
                .expect("first test work admits")
                .work_id,
            scheduler
                .admit(&group, WorkClass::Command, ())
                .expect("second test work admits")
                .work_id,
        ]
    }

    #[test]
    fn a_plan_cannot_certify_by_omitting_an_independently_ready_group() {
        let mut audit = Audit::default();
        let first = GroupId::new(1);
        let second = GroupId::new(2);
        for group in [first, second] {
            audit.register_group(group);
            audit.set_available(group, true);
        }
        let [first_work, second_work] = work_ids();
        audit.observe_admission(first, first_work, WorkClass::Command);
        audit.observe_admission(second, second_work, WorkClass::Command);

        audit.observe_plan(1, &[first]);

        assert_eq!(audit.invalid_plans, 1);
    }

    #[test]
    fn an_exact_shadow_ready_set_is_a_valid_plan() {
        let mut audit = Audit::default();
        let ready = GroupId::new(1);
        let unavailable = GroupId::new(2);
        for group in [ready, unavailable] {
            audit.register_group(group);
        }
        audit.set_available(ready, true);
        let [ready_work, unavailable_work] = work_ids();
        audit.observe_admission(ready, ready_work, WorkClass::Command);
        audit.observe_admission(unavailable, unavailable_work, WorkClass::Control);

        audit.observe_plan(1, &[ready]);

        assert_eq!(audit.invalid_plans, 0);
    }
}
