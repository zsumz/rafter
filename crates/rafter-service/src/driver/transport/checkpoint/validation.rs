#![allow(
    clippy::wildcard_imports,
    reason = "the driver is one state machine deliberately split across focused files, each opening the shared driver namespace"
)]

//! What makes one record readable.
//!
//! Every clause holds by construction for a record a driver wrote, so each
//! failure means the durable state was damaged, truncated, or belongs to another
//! replica — and each one lowers a retirement record in the dangerous direction
//! if it is absorbed instead of refused. Nothing here reads driver state.

use super::*;

impl<G> PeerControlPlaneCheckpoint<G> {
    /// The membership of this record's current state, or the empty set.
    pub(super) fn membership(&self) -> BTreeSet<NodeId> {
        self.current_committed
            .as_ref()
            .map(|current| current.membership.clone())
            .unwrap_or_default()
    }

    /// Refuses a checkpoint that contradicts the invariants a driver maintains.
    ///
    /// Every clause holds by construction for a checkpoint a driver produced, so
    /// each failure means the durable record was damaged, truncated, or belongs
    /// to another replica — and each one lowers a retirement record in the
    /// dangerous direction if it is absorbed instead of refused.
    ///
    /// * A live set needs a mark, and every live identity sits at or below it:
    ///   `observe_committed` raises the mark to the greatest identity of
    ///   the configuration it is assigning as live, in the same call, so the two
    ///   can never disagree. A lowered mark is how a corrupted record un-retires
    ///   everything above it.
    ///
    /// * **The mark and the current state are one record, and the coupling is a
    ///   biconditional.** Two clauses about the obligation ledger used to sit
    ///   beside this one, and they left with it: a fence was the residue of a
    ///   committed removal and therefore had to name a spent identity, which is
    ///   a rule about a set that no longer exists. Retirement is the mark and the
    ///   live set now, and the mark and the live set are exactly what this
    ///   couples.
    ///
    ///   Every observation of a committed configuration raises the mark — a
    ///   committed configuration always names at least one replica, since
    ///   [`rafter::MembershipSet`] refuses an empty voter set — and assigns the
    ///   current state in the same call, because a first observation is always
    ///   the latest one this record has. So `mark.is_some() ⟺
    ///   current_committed.is_some()`, and the two ways to break it are separate
    ///   variants because they fail in opposite directions:
    ///   [`ControlPlaneCheckpointError::RetirementWithoutCurrentState`] leaves a
    ///   mark with nothing to compare it against — every identity at or below the
    ///   mark reads as spent, which is the whole cluster — and
    ///   [`ControlPlaneCheckpointError::CurrentStateWithoutRetirement`] is a
    ///   membership this record could not have observed, since observing it
    ///   would have raised a mark.
    ///
    ///   An embedder with a format of its own should refuse the same two shapes
    ///   at its own decoder, so the refusal names the file rather than the
    ///   value.
    ///
    /// * **The contradiction marker is coupled to the register, and stands at or
    ///   above it.** A contradiction is a disagreement between the register and
    ///   something else, so there is no marker without a register to have
    ///   disagreed with — `merge_current_state` returns `Ok` outright when the
    ///   held state is absent, and so does the ancestry check. And the register
    ///   freezes the moment the marker is set, while the candidate that found the
    ///   contradiction may already have folded earlier facts of the same batch:
    ///   the marker's position is therefore at or above the register's, never
    ///   below it. A record that says otherwise has had one of the two fields
    ///   damaged, and absorbing it would either freeze a driver at a position
    ///   nothing disagreed at or — worse — let a marker be attached to a record
    ///   with no observation, which is unreadable rather than terminal.
    pub(super) fn validate(&self, group: &G) -> Result<(), ControlPlaneCheckpointError>
    where
        G: Ord,
    {
        if &self.group != group {
            return Err(ControlPlaneCheckpointError::ForeignGroup);
        }
        if let Some(contradicted_at) = self.contradicted_at {
            let Some(current) = self.current_committed.as_ref() else {
                return Err(ControlPlaneCheckpointError::ContradictionWithoutCurrentState);
            };
            if contradicted_at < current.through {
                return Err(
                    ControlPlaneCheckpointError::ContradictionBeneathCurrentState {
                        contradicted_at,
                        through: current.through,
                    },
                );
            }
        }
        // **One record, checked as one.** A mark is raised by an observation and
        // an observation assigns the current state, so neither stands alone.
        match (
            self.current_committed.is_some(),
            self.committed_id_high_water.is_some(),
        ) {
            (false, true) => {
                return Err(ControlPlaneCheckpointError::RetirementWithoutCurrentState)
            }
            (true, false) => {
                return Err(ControlPlaneCheckpointError::CurrentStateWithoutRetirement)
            }
            (false, false) | (true, true) => {}
        }
        for node_id in self.membership() {
            let Some(mark) = self.committed_id_high_water else {
                // Unreachable behind the biconditional above, and kept because
                // the loop must not read a mark it has not proved is there.
                return Err(ControlPlaneCheckpointError::RetirementWithoutCurrentState);
            };
            if node_id > mark {
                return Err(ControlPlaneCheckpointError::LiveMemberAboveMark { node_id, mark });
            }
        }
        Ok(())
    }
}
