//! Manual many-group host.

use std::{collections::BTreeMap, fmt::Debug};

use rafter_app::{
    group::{GroupInput, GroupStepReport},
    membership::MembershipEvent,
    read::ReadEvent,
    snapshot::SnapshotEvent,
};

use crate::{
    driver::GroupDriver,
    error::MultiRaftError,
    metrics::MultiRaftMetrics,
    pass::{GroupOutcome, TickPass},
};

/// Manual host for many Raft groups in one process.
///
/// This host steps what it is told to step. It does no scheduling, no
/// fairness enforcement, no admission control, and no queueing; see the crate
/// documentation for the list of what a managed scheduler would add.
#[derive(Debug)]
pub struct MultiRaftHost<G> {
    groups: BTreeMap<G, Box<dyn GroupDriver<G>>>,
}

impl<G> Default for MultiRaftHost<G>
where
    G: Clone + Ord + Debug,
{
    /// Creates an empty many-group host.
    ///
    /// Hand-written rather than derived: the derive bounds every type
    /// parameter on `Default`, which would demand it of a group key that this
    /// host only ever compares and clones.
    fn default() -> Self {
        Self::new()
    }
}

impl<G> MultiRaftHost<G>
where
    G: Clone + Ord + Debug,
{
    /// Creates an empty many-group host.
    #[must_use]
    pub fn new() -> Self {
        Self {
            groups: BTreeMap::new(),
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
    /// [`MultiRaftHost::remove_group`].
    #[must_use]
    pub fn contains_group(&self, group_id: &G) -> bool {
        self.groups.contains_key(group_id)
    }

    /// The open group keys, in the order a tick pass visits them.
    pub fn group_ids(&self) -> impl Iterator<Item = &G> {
        self.groups.keys()
    }

    /// Opens a group under `group_id`.
    ///
    /// # Errors
    ///
    /// Returns [`MultiRaftError::GroupAlreadyOpen`] when a driver is already
    /// registered for `group_id`, or [`MultiRaftError::WrongGroup`] when the
    /// driver's metrics report a different group ID.
    pub fn open_group<D>(&mut self, group_id: G, driver: D) -> Result<(), MultiRaftError<G>>
    where
        D: GroupDriver<G> + 'static,
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
    /// every later [`MultiRaftHost::tick_all`].
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
    /// never existed, and [`MultiRaftHost::open_group`] will reopen it. How
    /// long after a removal its traffic may still arrive is a deployment
    /// property this host cannot see, and retaining every retired key forever
    /// would grow without bound in service of it — so a caller that must fence
    /// late traffic against a removed group holds that tombstone itself.
    pub fn remove_group(&mut self, group_id: &G) -> Option<Box<dyn GroupDriver<G>>> {
        self.groups.remove(group_id)
    }

    /// Steps one group by explicit group identity.
    ///
    /// # Errors
    ///
    /// Returns [`MultiRaftError::UnknownGroup`] when the group is not open,
    /// [`MultiRaftError::WrongGroup`] when the input or returned report carries
    /// a different group ID, or [`MultiRaftError::Driver`] when the group
    /// driver rejects the input.
    pub fn step_group(
        &mut self,
        group_id: &G,
        input: GroupInput<G, Vec<u8>>,
    ) -> Result<GroupStepReport<G, Vec<u8>>, MultiRaftError<G>> {
        Self::validate_input_group(group_id, &input)?;
        let driver = self
            .groups
            .get_mut(group_id)
            .ok_or_else(|| MultiRaftError::UnknownGroup {
                group_id: group_id.clone(),
            })?;
        let report = driver
            .step(input)
            .map_err(|message| MultiRaftError::Driver {
                group_id: group_id.clone(),
                message,
            })?;
        Self::validate_report_group(group_id, &report)?;
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
    pub fn tick_all(&mut self) -> TickPass<G, Vec<u8>> {
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
            Self::validate_group_id(group_id, &metrics.group_id)?;
            groups.push(metrics);
        }
        Ok(MultiRaftMetrics { groups })
    }

    fn validate_input_group(
        expected: &G,
        input: &GroupInput<G, Vec<u8>>,
    ) -> Result<(), MultiRaftError<G>> {
        let actual = match input {
            GroupInput::PeerMessage { envelope } => Some(&envelope.group_id),
            GroupInput::ReadBarrier { request } => Some(&request.group_id),
            GroupInput::Tick
            | GroupInput::Proposal { .. }
            | GroupInput::ProposalBatch { .. }
            | GroupInput::Membership { .. }
            | GroupInput::TransferLeadership { .. } => None,
        };
        if let Some(actual) = actual {
            if actual != expected {
                return Err(MultiRaftError::WrongGroup {
                    expected: expected.clone(),
                    actual: actual.clone(),
                });
            }
        }
        Ok(())
    }

    fn validate_report_group(
        expected: &G,
        report: &GroupStepReport<G, Vec<u8>>,
    ) -> Result<(), MultiRaftError<G>> {
        Self::validate_group_id(expected, &report.group_id)?;
        for envelope in &report.peer_messages {
            Self::validate_group_id(expected, &envelope.group_id)?;
        }
        for event in &report.read_events {
            Self::validate_read_event_group(expected, event)?;
        }
        for event in &report.snapshot_events {
            Self::validate_snapshot_event_group(expected, event)?;
        }
        for event in &report.membership_events {
            Self::validate_membership_event_group(expected, event)?;
        }
        if let Some(metrics) = &report.metrics {
            Self::validate_group_id(expected, &metrics.group_id)?;
        }
        Ok(())
    }

    fn validate_read_event_group(
        expected: &G,
        event: &ReadEvent<G>,
    ) -> Result<(), MultiRaftError<G>> {
        match event {
            ReadEvent::Granted { proof, .. } => {
                Self::validate_group_id(expected, &proof.group_id)?;
            }
            ReadEvent::Rejected { .. }
            | ReadEvent::Canceled { .. }
            | ReadEvent::FreshnessUnavailable { .. } => {}
            _ => {
                return Err(MultiRaftError::Driver {
                    group_id: expected.clone(),
                    message: "unsupported non-exhaustive read event variant".to_owned(),
                });
            }
        }
        Ok(())
    }

    fn validate_snapshot_event_group(
        expected: &G,
        event: &SnapshotEvent<G>,
    ) -> Result<(), MultiRaftError<G>> {
        let (SnapshotEvent::Apply {
            group_id: actual, ..
        }
        | SnapshotEvent::StageChunk {
            group_id: actual, ..
        }
        | SnapshotEvent::SendChunk {
            group_id: actual, ..
        }) = event
        else {
            return Err(MultiRaftError::Driver {
                group_id: expected.clone(),
                message: "unsupported non-exhaustive snapshot event variant".to_owned(),
            });
        };
        Self::validate_group_id(expected, actual)
    }

    fn validate_membership_event_group(
        expected: &G,
        event: &MembershipEvent<G>,
    ) -> Result<(), MultiRaftError<G>> {
        let (MembershipEvent::Appended {
            group_id: actual, ..
        }
        | MembershipEvent::Applied {
            group_id: actual, ..
        }
        | MembershipEvent::Rejected {
            group_id: actual, ..
        }) = event
        else {
            return Err(MultiRaftError::Driver {
                group_id: expected.clone(),
                message: "unsupported non-exhaustive membership event variant".to_owned(),
            });
        };
        Self::validate_group_id(expected, actual)
    }

    fn validate_group_id(expected: &G, actual: &G) -> Result<(), MultiRaftError<G>> {
        if actual != expected {
            return Err(MultiRaftError::WrongGroup {
                expected: expected.clone(),
                actual: actual.clone(),
            });
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "host/tests.rs"]
mod tests;
