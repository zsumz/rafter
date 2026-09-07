//! What an adopted runtime says about the committed membership.
//!
//! A runtime handed to this driver reports one endpoint observation, and that
//! observation is asked twice: once before anything is installed, so an adoption
//! that would contradict the restored record refuses instead of opening, and
//! once after the recovery outputs are replayed, so the link layer hears the
//! whole of what the incarnation requires.

#![allow(
    clippy::wildcard_imports,
    reason = "the driver is one state machine deliberately split across focused files, each opening the shared driver namespace"
)]

use std::collections::BTreeSet;

use crate::transport::{AuthenticatedPeerValidator, RaftTransport};

use super::candidate::MembershipCandidate;
use super::checkpoint::{restore_checkpoint, RecordJoin};
use super::observation::{CommittedObservation, MembershipFact};
use super::state::TransportDriverState;
use super::*;

impl<G, A, R, T, V> TransportDriverState<G, A, R, T, V>
where
    G: Clone + Ord + Debug + Send + Sync + 'static,
    A: ReplicatedStateMachine + Send + 'static,
    A::Command: Send + 'static,
    A::CommandResult: Clone + Send + 'static,
    A::Query: Clone + Send + 'static,
    A::QueryResult: Send + 'static,
    R: PersistedRaftRuntime + Send + 'static,
    T: RaftTransport<G>,
    V: AuthenticatedPeerValidator<G, T::PeerPrincipal> + Send + Sync + 'static,
{
    /// Joins a recovered checkpoint into a candidate and asks the offered runtime
    /// whether it agrees, installing nothing either way.
    ///
    /// **The adoption transaction, and both halves have to be in it.** The join
    /// moves the mark, the register, and therefore the checkpoint an embedder
    /// persists; the runtime beside it can contradict the result at a position
    /// both stand at. Running the join against live state and the check
    /// afterwards left a refused adoption holding durable state recovered from the
    /// very input it had just declared contradictory — and the epoch move telling
    /// the embedder to write it down.
    ///
    /// The returned candidate carries the *record* and not the runtime's
    /// endpoint. The endpoint is folded afterwards by
    /// [`TransportDriverState::publish_adopted_membership`], once the recovery
    /// outputs have been replayed, because a recovered runtime's endpoint is
    /// newer than its own history and folding it first reads every replayed
    /// crossing as a removal of what the endpoint added.
    ///
    /// # Errors
    ///
    /// Returns [`ControlPlaneCheckpointError`] when the checkpoint names another
    /// group, contradicts the invariants a driver maintains for one, stands
    /// before what this driver already holds, or disagrees with either this
    /// driver's register or the offered runtime at a position they share.
    pub(super) fn adoption_candidate(
        &self,
        checkpoint: PeerControlPlaneCheckpoint<G>,
        runtime: &R,
    ) -> Result<MembershipCandidate, ControlPlaneCheckpointError> {
        let mut candidate = self.membership_candidate();
        restore_checkpoint(
            &mut candidate,
            checkpoint,
            &self.group_id,
            RecordJoin::Merge,
        )?;
        let (committed, _) = Self::adopted_observation(runtime);
        candidate.probe(&committed)?;
        Ok(candidate)
    }

    /// Publishes the adopted group's membership, so the transport's peer set is
    /// defined from adoption rather than from the first change this incarnation
    /// happens to observe.
    ///
    /// A committed fact, because an adoption is where a *change* can be observed
    /// without any event announcing it. The supervisor pattern this driver
    /// documents is release, rebuild the runtime from durable storage, adopt:
    /// the committed membership a rebuilt runtime reports can have advanced past
    /// a removal while the driver held no group, and no `Applied` will ever be
    /// emitted for it because the change committed elsewhere. The driver still
    /// holds its own committed membership from before the release, so the
    /// difference is there to be taken — and taking it is the only thing that
    /// makes one committed removal mean the same at adoption as it does on a
    /// routed event.
    ///
    /// The effective membership travels with it rather than instead of it. A
    /// runtime rebuilt from durable storage can hold an appended-but-uncommitted
    /// change, which makes its effective membership *narrower* than its
    /// committed one for a removal in flight; publishing that alone would take
    /// authorization away for a change that may still revert. The union is what
    /// keeps both readings correct at once.
    ///
    /// Adoption also republishes whatever the previous incarnation could not, and
    /// gets that for free rather than by arrangement: publishing runs the flush,
    /// and the policy is the driver's rather than the group's, so a release does
    /// not cancel it. It is also where a fresh incarnation retires the identity
    /// it used to be — the old `node_id` is at or below the floor and absent from
    /// the peer set the moment a different identity is installed, which is the
    /// whole of what a deferred self-fence used to arrange by hand.
    ///
    /// A driver holding no group publishes nothing. The early return skips the
    /// *derivation*, which needs a runtime; the policy it already published stays
    /// installed, because a release retracts nothing.
    ///
    /// **This is the endpoint of the stream, so it stands at the commit index.**
    /// The runtime does not publish the index of the entry its committed
    /// configuration came from, and it does not need to: `committed_membership`
    /// is by definition the latest configuration at or below `commit_index`, so
    /// the commit index is a sound position *for an endpoint*.
    ///
    /// **It carries no removal evidence, and that is the whole of what makes it
    /// safe to fold from anywhere.** A commit index is volatile — a recovered
    /// runtime can legitimately report a lower one than the incarnation that
    /// wrote the checkpoint had reached — and an endpoint standing beneath the
    /// register is simply an older observation of the same register, which
    /// [`merge_current_state`](super::checkpoint::merge_current_state) answers
    /// by keeping the later one. The gate this
    /// used to need, and the two-cursor apparatus behind it, went with the
    /// provenance tag: nothing here reads a position to decide whether a fact has
    /// been consumed, because every fact is monotone evidence that can be folded
    /// again.
    ///
    /// **What the position still decides is the tie.** A runtime and a restored
    /// record that both stand at the same index are two claims about one
    /// committed configuration, so they must agree once every proven removal and
    /// every spent identity is taken out of both — and if they do not, this
    /// refuses rather than letting the runtime overwrite the record. That is the
    /// one direction in which an endpoint can still do permanent damage: silently
    /// retiring a live replica, or silently raising the floor past an identity the
    /// durable record says was never committed.
    ///
    /// **It is not the only publisher of an endpoint, and it is not sampled per
    /// step.** `rafter-app` emits [`MembershipEvent::CommittedEndpoint`] whenever
    /// a step moves the committed membership with no crossing to carry it, and the
    /// driver reconciles the event stream after every step outcome including
    /// errors. So the floor tracks the runtime without this method being called
    /// again; this one exists for the moment *before* any step, when the driver
    /// has just been handed a group and no event has announced anything.
    ///
    /// # Errors
    ///
    /// Returns [`ControlPlaneCheckpointError::ContradictoryCurrentState`] when
    /// the adopted runtime and the record this driver restored stand at one
    /// position and disagree about the committed membership there. Nothing is
    /// published and nothing moves.
    pub(super) fn publish_adopted_membership(&mut self) -> Result<(), ControlPlaneCheckpointError> {
        let Some(group) = self.group.as_ref() else {
            return Ok(());
        };
        let (committed, effective) = Self::adopted_observation(group.runtime());
        let mut candidate = self.membership_candidate();
        if let Err(reason) = candidate.apply(MembershipFact::Committed {
            committed,
            effective,
        }) {
            self.record_contradiction(reason);
            return Err(reason);
        }
        self.install_membership(candidate);
        Ok(())
    }

    /// The endpoint observation and effective membership one runtime reports.
    ///
    /// Shared by the adoption probe and the publication so the question asked
    /// before an adoption is the same one answered after it.
    pub(super) fn adopted_observation(runtime: &R) -> (CommittedObservation, BTreeSet<NodeId>) {
        (
            CommittedObservation::endpoint(runtime.commit_index(), &runtime.committed_membership()),
            runtime.membership().replica_ids().into_iter().collect(),
        )
    }
}
