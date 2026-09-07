//! Hand-built checkpoints, and the drivers this suite joins them into.
//!
//! Two positions on one chain, a record constructor that keeps the current
//! state travelling with the retirement mark, and a driver opened through the
//! documented restore path so the state a scenario joins into is a state a
//! driver can actually be in. Nothing here asserts; the join is the scenarios'
//! claim to make.

use std::collections::BTreeSet;

use rafter_service::{CurrentCommittedState, PeerControlPlaneCheckpoint, TransportDriverOptions};

use super::support::scripted::{
    scripted_driver_with_checkpoint, ScriptedDriver, ScriptedMembershipRuntime,
};
use super::support::transport::{Nameable, QueueTransport, GROUP};
use super::support::{LogIndex, NodeId};

pub(super) fn ids(node_ids: &[u64]) -> BTreeSet<NodeId> {
    node_ids.iter().copied().map(NodeId).collect()
}

/// Where a hand-built record observed the committed membership.
///
/// **Above the scripted runtime's commit index on purpose**, which is the
/// inversion the chain rule forces. A driver's register ends up at the later of
/// its record and its runtime, so a record at or after this position is the one a
/// mid-life adoption may still offer — and every adoption's endpoint publication
/// is then the older observation, which contributes nothing and leaves these
/// cases measuring the identity lattice rather than the register's ordering.
pub(super) const OBSERVED_AT: LogIndex = LogIndex(9);

/// Where a record offered *after* one at [`OBSERVED_AT`] stands.
///
/// The next link of one chain. Records at one position meet the contradiction
/// arm, which is a different rule and has its own cases.
pub(super) const LATER_AT: LogIndex = LogIndex(12);

/// A checkpoint built by hand, the way a durable file hands one back.
///
/// **The current state travels with the retirement record**, because they are
/// one record and a driver never writes either without the other. A helper that
/// left it out would be building a shape the validator now refuses — which is
/// the point of `a_record_that_separates_retirement_from_its_current_state_is_refused`
/// in the suite, and not something every other case there should be quietly
/// asserting.
pub(super) fn checkpoint(mark: Option<u64>, live: &[u64]) -> PeerControlPlaneCheckpoint<u64> {
    checkpoint_at(mark, live, Some(OBSERVED_AT))
}

/// The same, with the observation's position chosen by the caller.
///
/// `None` builds a record with no current state at all, which is the shape the
/// coupling biconditional refuses beside any retirement state.
pub(super) fn checkpoint_at(
    mark: Option<u64>,
    live: &[u64],
    through: Option<LogIndex>,
) -> PeerControlPlaneCheckpoint<u64> {
    let mut checkpoint = PeerControlPlaneCheckpoint::empty(GROUP);
    checkpoint.committed_id_high_water = mark.map(NodeId);
    checkpoint.current_committed =
        through.map(|through| CurrentCommittedState::new(through, ids(live)));
    checkpoint
}

/// A driver holding `{mark, live}` and nothing else, ready to be joined into.
///
/// Built through the documented restore path rather than by reaching inside, so
/// the state under test is a state a driver can actually be in. `mark: None`
/// means no record at all, and the mark the driver ends up with is the one its
/// own adoption derives from the runtime — which is what a first incarnation
/// looks like.
pub(super) fn driver_holding(mark: Option<u64>, live: &[u64]) -> (ScriptedDriver, QueueTransport) {
    driver_holding_named(mark, live, Nameable::all())
}

/// The same, with a directory that can name only some replicas.
///
/// A publication naming a replica this directory cannot resolve is withheld
/// whole, which is how a test reaches a driver whose link layer is behind it
/// without arranging a transport refusal.
pub(super) fn driver_holding_named(
    mark: Option<u64>,
    live: &[u64],
    nameable: Nameable,
) -> (ScriptedDriver, QueueTransport) {
    let record = match mark {
        Some(mark) => checkpoint(Some(mark), live),
        None => PeerControlPlaneCheckpoint::empty(GROUP),
    };
    scripted_driver_with_checkpoint(
        ScriptedMembershipRuntime::new(live, live),
        nameable,
        &[NodeId(2), NodeId(5)],
        TransportDriverOptions::default(),
        record,
    )
}
