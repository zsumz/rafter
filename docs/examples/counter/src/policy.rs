//! This example accepts only its original three voters, with no learners.
use crate::{disk, GROUP, MEMBERS};
use rafter::{LogIndex, MembershipConfig, NodeId, Term};
use rafter_service::{CurrentCommittedState, PeerControlPlaneCheckpoint};
use std::io;

pub fn membership(configuration: &MembershipConfig) -> io::Result<()> {
    match configuration {
        MembershipConfig::Stable(set)
            if set.voters() == MEMBERS.map(NodeId) && set.learners().is_empty() =>
        {
            Ok(())
        }
        _ => Err(disk::invalid(
            "counter requires the original three voters; membership changes are unsupported",
        )),
    }
}

pub fn restore(
    checkpoint: &PeerControlPlaneCheckpoint<u64>,
    commit: LogIndex,
    last: LogIndex,
    term: Term,
) -> io::Result<()> {
    if checkpoint.group != GROUP || checkpoint.contradicted_at.is_some() {
        return Err(disk::invalid("foreign or contradictory counter checkpoint"));
    }
    match (
        &checkpoint.current_committed,
        checkpoint.committed_id_high_water,
    ) {
        (None, None) if commit == LogIndex::ZERO && last == LogIndex::ZERO && term == Term(0) => {
            Ok(())
        }
        (Some(current), Some(NodeId(3)))
            if current.through <= commit
                && current.membership == MEMBERS.into_iter().map(NodeId).collect() =>
        {
            Ok(())
        }
        _ => Err(disk::invalid(
            "counter checkpoint would forget membership or retirement history",
        )),
    }
}

pub fn observed(through: LogIndex) -> PeerControlPlaneCheckpoint<u64> {
    let mut checkpoint = PeerControlPlaneCheckpoint::empty(GROUP);
    checkpoint.committed_id_high_water = Some(NodeId(3));
    checkpoint.current_committed = Some(CurrentCommittedState::new(
        through,
        MEMBERS.into_iter().map(NodeId).collect(),
    ));
    checkpoint
}
