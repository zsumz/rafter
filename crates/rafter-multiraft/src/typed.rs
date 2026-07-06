//! Typed many-group host helpers.
//!
//! The primary [`crate::MultiRaftHost`] intentionally uses `Vec<u8>` commands
//! so it can remain object-safe across dynamic or heterogeneous groups. This
//! module provides a typed host for the common case where open groups share one
//! command type and one apply-result type.

use std::{collections::BTreeMap, fmt::Debug, marker::PhantomData};

use rafter_app::{
    group::{GroupInput, GroupStepReport, RaftGroup},
    membership::MembershipEvent,
    metrics::RaftGroupMetrics,
    read::ReadEvent,
    snapshot::SnapshotEvent,
    state_machine::ReplicatedStateMachine,
};
use rafter_runtime_api::PersistedRaftRuntime;

use crate::{error::MultiRaftError, metrics::MultiRaftMetrics};

/// Typed driver surface for groups that share command and result types.
pub trait TypedGroupDriver<G>: Debug {
    type Command;
    type CommandResult;

    /// Steps one typed group input and returns explicit side effects.
    ///
    /// # Errors
    ///
    /// Returns an implementation-defined message when the group driver cannot
    /// process the input.
    fn step(
        &mut self,
        input: GroupInput<G, Self::Command>,
    ) -> Result<GroupStepReport<G, Self::CommandResult>, String>;

    fn metrics(&self) -> RaftGroupMetrics<G>;
}

impl<G, A, R> TypedGroupDriver<G> for RaftGroup<G, A, R>
where
    G: Clone + Ord + Debug + Send + Sync + 'static,
    A: ReplicatedStateMachine + Debug,
    A::CommandResult: Clone,
    A::Error: Debug,
    R: PersistedRaftRuntime + Debug,
{
    type Command = A::Command;
    type CommandResult = A::CommandResult;

    fn step(
        &mut self,
        input: GroupInput<G, Self::Command>,
    ) -> Result<GroupStepReport<G, Self::CommandResult>, String> {
        RaftGroup::step(self, input).map_err(|error| format!("{error:?}"))
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
#[derive(Debug, Default)]
pub struct TypedMultiRaftHost<G, C, R> {
    groups: BTreeMap<G, Box<dyn TypedGroupDriver<G, Command = C, CommandResult = R>>>,
    marker: PhantomData<fn(C) -> R>,
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

    /// Steps one typed group by explicit group identity.
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
        input: GroupInput<G, C>,
    ) -> Result<GroupStepReport<G, R>, MultiRaftError<G>> {
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
    /// # Errors
    ///
    /// Returns [`MultiRaftError::Driver`] if any group driver rejects its tick,
    /// or [`MultiRaftError::WrongGroup`] if a driver returns a report,
    /// envelope, event, or metrics snapshot for a group other than the host key
    /// being ticked. `WrongGroup` can therefore come from report validation, not
    /// only from caller-supplied group IDs.
    pub fn tick_all(&mut self) -> Result<Vec<GroupStepReport<G, R>>, MultiRaftError<G>> {
        let group_ids = self.groups.keys().cloned().collect::<Vec<_>>();
        group_ids
            .into_iter()
            .map(|group_id| self.step_group(&group_id, GroupInput::Tick))
            .collect()
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
        input: &GroupInput<G, C>,
    ) -> Result<(), MultiRaftError<G>> {
        let actual = match input {
            GroupInput::PeerMessage { envelope } => Some(&envelope.group_id),
            GroupInput::ReadBarrier { request } => Some(&request.group_id),
            GroupInput::Tick
            | GroupInput::Proposal { .. }
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
        report: &GroupStepReport<G, R>,
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
#[path = "typed/tests.rs"]
mod tests;
