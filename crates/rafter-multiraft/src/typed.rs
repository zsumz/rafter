//! Typed many-group host helpers.
//!
//! The primary [`crate::MultiRaftHost`] intentionally uses `Vec<u8>` commands
//! so it can remain object-safe across dynamic or heterogeneous groups. This
//! module provides a typed host for the common case where open groups share one
//! command type and one apply-result type.

use std::{collections::BTreeMap, fmt::Debug, marker::PhantomData};

use rafter_app::{
    error::ErrorCause,
    group::{GroupFatalState, GroupInput, GroupStepReport, RaftGroup},
    metrics::RaftGroupMetrics,
    state_machine::ReplicatedStateMachine,
};
use rafter_runtime_api::PersistedRaftRuntime;

use crate::{
    driver::{DriverError, DriverErrorKind},
    error::MultiRaftError,
    metrics::MultiRaftMetrics,
    pass::{GroupOutcome, TickPass},
    validate,
};

/// Typed driver surface for groups that share command and result types.
pub trait TypedGroupDriver<G>: Debug {
    type Command;
    type CommandResult;

    /// Steps one typed group input and returns explicit side effects.
    ///
    /// # Errors
    ///
    /// Returns a [`DriverError`] carrying the permanence the driver observed
    /// and the typed error that caused it. An implementation reports
    /// [`DriverErrorKind::Poisoned`] only when it observed that the group is
    /// finished — never because a category implies it.
    fn step(
        &mut self,
        input: GroupInput<G, Self::Command>,
    ) -> Result<GroupStepReport<G, Self::CommandResult>, DriverError>;

    fn metrics(&self) -> RaftGroupMetrics<G>;
}

impl<G, A, R> TypedGroupDriver<G> for RaftGroup<G, A, R>
where
    G: Clone + Ord + Debug + Send + Sync + 'static,
    A: ReplicatedStateMachine + Debug,
    A::CommandResult: Clone,
    R: PersistedRaftRuntime + Debug,
{
    type Command = A::Command;
    type CommandResult = A::CommandResult;

    /// Steps the group and preserves its typed error.
    ///
    /// The permanence is read from [`RaftGroup::fatal_state`] *after* the
    /// step rather than inferred from the error variant, because the two
    /// disagree exactly where it matters: a failure that *causes* a poison
    /// does not return `GroupError::Poisoned` — it returns the underlying
    /// fault, and the group is poisoned afterwards. Classifying by variant
    /// would call the first poisoning failure transient and only the second
    /// one permanent.
    fn step(
        &mut self,
        input: GroupInput<G, Self::Command>,
    ) -> Result<GroupStepReport<G, Self::CommandResult>, DriverError> {
        match RaftGroup::step(self, input) {
            Ok(report) => Ok(report),
            Err(error) => {
                let kind = match RaftGroup::fatal_state(self) {
                    GroupFatalState::Poisoned { .. } => DriverErrorKind::Poisoned,
                    GroupFatalState::Healthy => DriverErrorKind::Transient,
                };
                Err(DriverError::new(kind, ErrorCause::new(error)))
            }
        }
    }

    fn metrics(&self) -> RaftGroupMetrics<G> {
        RaftGroup::metrics(self)
    }
}

/// Manual typed host for many Raft groups in one process.
///
/// This host keeps group IDs explicit and stores typed trait objects whose
/// command/result associated types are fixed by `C` and `R`. Use
/// [`crate::MultiRaftHost`] when groups need different command/result types or
/// when the caller wants to manage the encoded `Vec<u8>` boundary directly.
///
/// This host steps what it is told to step. It does no scheduling, no
/// fairness enforcement, no admission control, and no queueing; see the crate
/// documentation for the list of what a managed scheduler would add.
#[derive(Debug)]
pub struct TypedMultiRaftHost<G, C, R> {
    groups: BTreeMap<G, Box<dyn TypedGroupDriver<G, Command = C, CommandResult = R>>>,
    marker: PhantomData<fn(C) -> R>,
}

impl<G, C, R> Default for TypedMultiRaftHost<G, C, R>
where
    G: Clone + Ord + Debug,
{
    /// Creates an empty typed many-group host.
    ///
    /// Hand-written rather than derived. The derive bounds every type
    /// parameter on `Default`, including `C` and `R` — the command and result
    /// types, which this host never stores by value — so
    /// `TypedMultiRaftHost::<_, MyCommand, MyResult>::default()` did not
    /// compile for any command type that is not itself `Default`.
    fn default() -> Self {
        Self::new()
    }
}

impl<G, C, R> TypedMultiRaftHost<G, C, R>
where
    G: Clone + Ord + Debug,
{
    /// Creates an empty typed many-group host.
    #[must_use]
    pub fn new() -> Self {
        Self {
            groups: BTreeMap::new(),
            marker: PhantomData,
        }
    }

    /// The number of groups currently open.
    ///
    /// This is the size of one complete tick pass; see [`TickPass::visited`].
    #[must_use]
    pub fn len(&self) -> usize {
        self.groups.len()
    }

    /// Whether no group is open.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.groups.is_empty()
    }

    /// Whether `group_id` is open.
    ///
    /// False for a key that was retired and false for a key that never
    /// existed; this host does not distinguish them. See
    /// [`TypedMultiRaftHost::remove_group`].
    #[must_use]
    pub fn contains_group(&self, group_id: &G) -> bool {
        self.groups.contains_key(group_id)
    }

    /// The open group keys, in the order a tick pass visits them.
    pub fn group_ids(&self) -> impl Iterator<Item = &G> {
        self.groups.keys()
    }

    /// Opens a typed group under `group_id`.
    ///
    /// # Errors
    ///
    /// Returns [`MultiRaftError::GroupAlreadyOpen`] when a driver is already
    /// registered for `group_id`, or [`MultiRaftError::WrongGroup`] when the
    /// driver's metrics report a different group ID.
    pub fn open_group<D>(&mut self, group_id: G, driver: D) -> Result<(), MultiRaftError<G>>
    where
        D: TypedGroupDriver<G, Command = C, CommandResult = R> + 'static,
    {
        if self.groups.contains_key(&group_id) {
            return Err(MultiRaftError::GroupAlreadyOpen { group_id });
        }
        let actual = driver.metrics().group_id;
        if actual != group_id {
            return Err(MultiRaftError::WrongGroup {
                expected: group_id,
                actual,
            });
        }
        self.groups.insert(group_id, Box::new(driver));
        Ok(())
    }

    /// Retires `group_id`, returning its driver.
    ///
    /// The driver is returned rather than dropped so a caller can drain it —
    /// step it to quiescence, read its final metrics, close what it owns —
    /// after the host has stopped scheduling it. Retiring is how a group that
    /// can no longer make progress stops consuming a scheduling opportunity in
    /// every later [`TypedMultiRaftHost::tick_all`].
    ///
    /// Idempotent: retiring a key that is not open returns `None`.
    ///
    /// This host never retires a group on its own, not even one whose driver
    /// reports a permanent failure. A driver owns a runtime, a state machine,
    /// and open storage; deciding it is finished is the caller's, and the
    /// party whose judgement is in question when a driver misbehaves is the
    /// driver.
    ///
    /// **No tombstone is kept.** A later input for a retired key is
    /// [`MultiRaftError::UnknownGroup`], indistinguishable from a key that
    /// never existed, and [`TypedMultiRaftHost::open_group`] will reopen it.
    /// How long after a removal its traffic may still arrive is a deployment
    /// property this host cannot see, and retaining every retired key forever
    /// would grow without bound in service of it — so a caller that must fence
    /// late traffic against a removed group holds that tombstone itself.
    pub fn remove_group(
        &mut self,
        group_id: &G,
    ) -> Option<Box<dyn TypedGroupDriver<G, Command = C, CommandResult = R>>> {
        self.groups.remove(group_id)
    }

    /// Steps one typed group by explicit group identity.
    ///
    /// # Errors
    ///
    /// Returns [`MultiRaftError::UnknownGroup`] when the group is not open,
    /// [`MultiRaftError::WrongGroup`] when the caller's input names another
    /// group — in which case nothing was stepped —
    /// [`MultiRaftError::Driver`] when the group driver refuses the input, or
    /// [`MultiRaftError::InvalidReport`] / [`MultiRaftError::UnrecognizedEvent`]
    /// when the driver returns a report this host cannot trust.
    ///
    /// The last two arrive **after** the driver has stepped: a report cannot
    /// be checked before it exists, so whatever it described has happened and
    /// its effects are not recoverable through this host. That is why they are
    /// not `WrongGroup` — the two say opposite things about whether an effect
    /// occurred, and a caller has to be able to tell them apart. `open_group`
    /// checks a driver's claimed identity up front to keep this case rare; the
    /// repair when it happens is to retire the group.
    ///
    /// Only `PeerMessage` and `ReadBarrier` inputs are checked against
    /// `group_id`, because they are the only two that carry a group ID. A
    /// `Tick`, `Proposal`, `ProposalBatch`, `Membership`, or
    /// `TransferLeadership` routed to the wrong group by a caller's shard map
    /// is accepted, and this host cannot detect it.
    pub fn step_group(
        &mut self,
        group_id: &G,
        input: GroupInput<G, C>,
    ) -> Result<GroupStepReport<G, R>, MultiRaftError<G>> {
        validate::input_group(group_id, &input)?;
        let driver = self
            .groups
            .get_mut(group_id)
            .ok_or_else(|| MultiRaftError::UnknownGroup {
                group_id: group_id.clone(),
            })?;
        let report = driver.step(input).map_err(|error| MultiRaftError::Driver {
            group_id: group_id.clone(),
            kind: error.kind(),
            cause: error.into_cause(),
        })?;
        validate::report_group(group_id, &report)?;
        Ok(report)
    }

    /// Ticks every open group in deterministic key order.
    ///
    /// Every open group is stepped and gets an outcome, whatever any other
    /// group did. A failing group consumes its own opportunity and nobody
    /// else's: it neither ends the pass nor deprives a later key of its tick,
    /// and — the reason this returns a pass rather than a `Result` — it does
    /// not take an earlier group's report down with it. A report's `applied`
    /// list is the only proof a write took effect and nothing re-emits it, so
    /// a pass that dropped the reports it had already collected would lose
    /// committed writes with no recovery path.
    ///
    /// Failures are per group, in [`TickPass::failures`]. There is no
    /// pass-level error.
    pub fn tick_all(&mut self) -> TickPass<G, R> {
        let group_ids = self.groups.keys().cloned().collect::<Vec<_>>();
        let outcomes = group_ids
            .into_iter()
            .map(|group_id| {
                let result = self.step_group(&group_id, GroupInput::Tick);
                GroupOutcome { group_id, result }
            })
            .collect();
        TickPass::new(outcomes)
    }

    /// Returns metrics for every open group in deterministic key order.
    ///
    /// # Errors
    ///
    /// Returns [`MultiRaftError::WrongGroup`] if a driver reports metrics for a
    /// different group than the host key it is registered under.
    pub fn metrics(&self) -> Result<MultiRaftMetrics<G>, MultiRaftError<G>> {
        let mut groups = Vec::with_capacity(self.groups.len());
        for (group_id, driver) in &self.groups {
            let metrics = driver.metrics();
            validate::metrics_group(group_id, &metrics.group_id)?;
            groups.push(metrics);
        }
        Ok(MultiRaftMetrics { groups })
    }
}

#[cfg(test)]
#[path = "typed/tests.rs"]
mod tests;
