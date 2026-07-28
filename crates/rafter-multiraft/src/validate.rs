//! Group-identity checks shared by both many-group hosts.
//!
//! These live here rather than in each host because the two hosts are
//! line-for-line mirrors and a defect written once in that shape was, for the
//! life of this crate, a defect present twice. Nothing below depends on the
//! command or result type, so one copy serves both.

use rafter_app::{
    group::{GroupInput, GroupStepReport},
    membership::MembershipEvent,
    read::ReadEvent,
    snapshot::SnapshotEvent,
};

use crate::error::MultiRaftError;

/// Checks a caller-supplied input against the host key it was routed under.
///
/// **Only two of the seven [`GroupInput`] variants can be checked at all.**
/// `PeerMessage` and `ReadBarrier` carry a group ID; `Tick`, `Proposal`,
/// `ProposalBatch`, `Membership`, and `TransferLeadership` do not. For those
/// five a caller's shard-map bug routes the input into the wrong group and
/// this host cannot detect it, because the information required to detect it
/// is not in the input.
pub(crate) fn input_group<G, C>(
    expected: &G,
    input: &GroupInput<G, C>,
) -> Result<(), MultiRaftError<G>>
where
    G: Clone + PartialEq,
{
    let actual = match input {
        GroupInput::PeerMessage { envelope } => Some(&envelope.group_id),
        GroupInput::ReadBarrier { request } => Some(&request.group_id),
        GroupInput::Tick
        | GroupInput::Proposal { .. }
        | GroupInput::ProposalBatch { .. }
        | GroupInput::Membership { .. }
        | GroupInput::TransferLeadership { .. } => None,
    };
    match actual {
        Some(actual) if actual != expected => Err(MultiRaftError::WrongGroup {
            expected: expected.clone(),
            actual: actual.clone(),
        }),
        Some(_) | None => Ok(()),
    }
}

/// Checks a report the driver has **already produced** against the host key.
///
/// Every failure here is after the fact: the driver stepped, mutated itself,
/// and whatever the report describes has happened. That is why these are
/// [`MultiRaftError::InvalidReport`] and [`MultiRaftError::UnrecognizedEvent`]
/// rather than [`MultiRaftError::WrongGroup`] — the two say opposite things
/// about whether an effect occurred, and a caller needs to tell them apart.
pub(crate) fn report_group<G, R>(
    expected: &G,
    report: &GroupStepReport<G, R>,
) -> Result<(), MultiRaftError<G>>
where
    G: Clone + PartialEq,
{
    field_group(expected, "group_id", &report.group_id)?;
    for envelope in &report.peer_messages {
        field_group(expected, "peer_messages", &envelope.group_id)?;
    }
    for event in &report.read_events {
        read_event_group(expected, event)?;
    }
    for event in &report.snapshot_events {
        snapshot_event_group(expected, event)?;
    }
    for event in &report.membership_events {
        membership_event_group(expected, event)?;
    }
    if let Some(metrics) = &report.metrics {
        field_group(expected, "metrics", &metrics.group_id)?;
    }
    Ok(())
}

fn read_event_group<G>(expected: &G, event: &ReadEvent<G>) -> Result<(), MultiRaftError<G>>
where
    G: Clone + PartialEq,
{
    match event {
        ReadEvent::Granted { proof, .. } => field_group(expected, "read_events", &proof.group_id),
        ReadEvent::Rejected { .. }
        | ReadEvent::Canceled { .. }
        | ReadEvent::FreshnessUnavailable { .. } => Ok(()),
        _ => Err(MultiRaftError::UnrecognizedEvent {
            group_id: expected.clone(),
            field: "read_events",
        }),
    }
}

fn snapshot_event_group<G>(expected: &G, event: &SnapshotEvent<G>) -> Result<(), MultiRaftError<G>>
where
    G: Clone + PartialEq,
{
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
        return Err(MultiRaftError::UnrecognizedEvent {
            group_id: expected.clone(),
            field: "snapshot_events",
        });
    };
    field_group(expected, "snapshot_events", actual)
}

fn membership_event_group<G>(
    expected: &G,
    event: &MembershipEvent<G>,
) -> Result<(), MultiRaftError<G>>
where
    G: Clone + PartialEq,
{
    let (MembershipEvent::EffectiveChanged {
        group_id: actual, ..
    }
    | MembershipEvent::Applied {
        group_id: actual, ..
    }
    | MembershipEvent::CommittedEndpoint {
        group_id: actual, ..
    }
    | MembershipEvent::Rejected {
        group_id: actual, ..
    }) = event
    else {
        return Err(MultiRaftError::UnrecognizedEvent {
            group_id: expected.clone(),
            field: "membership_events",
        });
    };
    field_group(expected, "membership_events", actual)
}

fn field_group<G>(expected: &G, field: &'static str, actual: &G) -> Result<(), MultiRaftError<G>>
where
    G: Clone + PartialEq,
{
    if actual == expected {
        return Ok(());
    }
    Err(MultiRaftError::InvalidReport {
        group_id: expected.clone(),
        field,
        reported: actual.clone(),
    })
}

/// Checks a driver's metrics snapshot against the host key.
///
/// Nothing is stepped to take a metrics snapshot, so a mismatch here is
/// [`MultiRaftError::WrongGroup`]: no effect occurred and none was lost.
pub(crate) fn metrics_group<G>(expected: &G, actual: &G) -> Result<(), MultiRaftError<G>>
where
    G: Clone + PartialEq,
{
    if actual == expected {
        return Ok(());
    }
    Err(MultiRaftError::WrongGroup {
        expected: expected.clone(),
        actual: actual.clone(),
    })
}
