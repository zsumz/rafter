//! Retirement and single-group stepping for the typed many-group host.
//!
//! Both operations name their group explicitly. Stepping checks the caller's
//! input against that key before the driver runs and checks the driver's
//! report after it, so the two failures stay distinguishable: one happened
//! before any effect, the other after.

use std::fmt::Debug;

use rafter_app::group::{GroupInput, GroupStepReport};

use crate::{error::MultiRaftError, validate};

use super::{TypedGroupDriver, TypedMultiRaftHost};

impl<G, C, R> TypedMultiRaftHost<G, C, R>
where
    G: Clone + Ord + Debug,
{
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
}
