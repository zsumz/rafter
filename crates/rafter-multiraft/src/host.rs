//! Manual many-group host.

use std::{collections::BTreeMap, fmt::Debug};

use rafter_app::{
    group::{GroupInput, GroupStepReport},
    membership::MembershipEvent,
    read::ReadEvent,
    snapshot::SnapshotEvent,
};

use crate::{driver::GroupDriver, error::MultiRaftError, metrics::MultiRaftMetrics};

/// Manual host for many Raft groups in one process.
#[derive(Debug, Default)]
pub struct MultiRaftHost<G> {
    groups: BTreeMap<G, Box<dyn GroupDriver<G>>>,
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
    /// # Errors
    ///
    /// Returns [`MultiRaftError::Driver`] if any group driver rejects its tick,
    /// or [`MultiRaftError::WrongGroup`] if a driver returns a report,
    /// envelope, event, or metrics snapshot for a group other than the host key
    /// being ticked. `WrongGroup` can therefore come from report validation, not
    /// only from caller-supplied group IDs.
    pub fn tick_all(&mut self) -> Result<Vec<GroupStepReport<G, Vec<u8>>>, MultiRaftError<G>> {
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
        input: &GroupInput<G, Vec<u8>>,
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
