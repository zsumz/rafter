//! Adopting a group beside a record from before this driver existed.
//!
//! A takeover, or a driver re-armed from another process's persisted state: the
//! record is merged rather than assigned, because this driver may already hold
//! retirement state a release did not cancel. Everything above the installation
//! is a refusal that leaves no group behind; only the recovery outputs and the
//! publication they feed can fail with one installed.

#![allow(
    clippy::wildcard_imports,
    reason = "the driver is one state machine deliberately split across focused files, each opening the shared driver namespace"
)]

use crate::transport::{AuthenticatedPeerValidator, RaftTransport};

use super::adoption::{adopted_watermarks, highest, PendingProposals};
use super::*;

impl<G, A, R, T, V> TransportRaftDriver<G, A, R, T, V>
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
    /// Installs a new incarnation and merges a recovered control-plane
    /// checkpoint into what this driver already holds.
    ///
    /// The adoption counterpart of
    /// [`TransportRaftDriver::with_control_plane_checkpoint`], for a supervisor
    /// that rebuilds a replica's runtime from durable storage and has a
    /// checkpoint from *before* this driver existed — a takeover, or a driver
    /// re-armed from another process's persisted state.
    ///
    /// Merged rather than assigned, because this driver may already hold
    /// retirement state of its own and a released group does not cancel it: the
    /// mark is the higher of the two and the current state is the later
    /// observation. An in-process release and re-adopt needs none of this and
    /// passes an empty checkpoint through
    /// [`TransportRaftDriver::adopt_group`], because nothing was lost.
    ///
    /// # What an `Err` leaves behind
    ///
    /// **Two kinds of `Err`, and they differ in whether the group was
    /// installed.** `Result<(), _>` cannot say which, so it is said here and
    /// pinned in `tests/adoption.rs`.
    ///
    /// Everything above the installation is a *refusal*: shutdown, a group
    /// already held, a foreign group ID, a checkpoint that contradicts this
    /// driver's invariants, a runtime whose committed membership contradicts the
    /// restored record at a position both observed, a node ID a committed removal
    /// has spent, and watermarks a released incarnation had already passed. Each
    /// leaves the driver holding no group, so
    /// [`TransportRaftDriver::with_group`] answers
    /// [`ManagedDriverError::NoGroup`] and the next adoption is an ordinary
    /// first attempt. The group itself is consumed and dropped, which is what a
    /// refusal means: the supervisor rebuilds and offers a new one.
    ///
    /// **Only the recovery outputs and the publication they feed fail after the
    /// group is installed, and that adoption is not rolled back.** The driver
    /// holds the group, `with_group` reads it, and `service_state` answers for it
    /// — [`TransportRaftDriver::release_group`] is how a supervisor gets it back,
    /// and is required before another adoption, which would otherwise raise
    /// [`ManagedDriverError::GroupAlreadyAdopted`]. Rolling back instead would
    /// *drop* the group on the floor, which is strictly worse: a caller holding
    /// a `Result<(), _>` has no other way to reach it.
    ///
    /// The link layer is told what the installed group requires whether or not
    /// the outputs applied, because that is the one statement a later call
    /// cannot repair on its own: a policy left describing the retired
    /// incarnation is not stale, it is wrong about who may speak, and nothing
    /// re-derives it until the cluster's next configuration change. The single
    /// exception is a contradiction the recovery outputs themselves introduced,
    /// where publishing nothing *is* the correct statement — see
    /// [`DriverServiceState::ContradictoryCurrentState`].
    ///
    /// **Retry is legal, and re-sending is sound.** The checkpoint is merged
    /// before the failure and stays merged; the join is monotone and idempotent,
    /// so offering the same record again adds nothing. Transport calls that
    /// already flew are lost-message-equivalent under this repo's own model —
    /// [`RaftTransport`] states that Raft safety tolerates dropped, duplicated,
    /// and reordered peer messages — so a supervisor that releases, rebuilds the
    /// runtime, and adopts again re-sends what it must and duplicates what it
    /// need not.
    ///
    /// # Errors
    ///
    /// As [`TransportRaftDriver::adopt_group`].
    pub fn adopt_group_with_checkpoint(
        &self,
        group: RaftGroup<G, A, R>,
        recovery_outputs: Vec<RaftOutput>,
        checkpoint: PeerControlPlaneCheckpoint<G>,
    ) -> Result<(), ManagedDriverError> {
        let mut state = self.inner.lock();
        // Shutdown is terminal, and `shutdown` itself says so by refusing a
        // second call. A driver that could be re-armed by adopting a group would
        // make the entry's own distinction — a supervisor restarting a replica
        // releases, a supervisor stopping one shuts down and then releases —
        // a distinction with no consequence.
        state.reject_if_shutting_down()?;
        // Before anything about the incoming group is consumed: a contradiction
        // is terminal for this incarnation, and releasing the old group did not
        // resolve it. Refusing here keeps the partial-adoption contract exact —
        // a group is left installed only by a failure the newly adopted group's
        // own recovery outputs produced, never by terminal state that predates
        // the adoption. The error names the driver's own condition rather than
        // the incoming record, because nothing offered here is what is wrong.
        if let Some(reason) = state.recorded_contradiction() {
            return Err(ManagedDriverError::ControlPlaneContradicted { reason });
        }
        if state.group.is_some() {
            return Err(ManagedDriverError::GroupAlreadyAdopted);
        }
        // The driver's group ID is fixed at construction and adoption does not
        // republish it: handles, the metrics watch, and every client-facing
        // group check keep comparing against the original. A group serving a
        // different ID would be driven under this driver's ID, and nothing
        // downstream would catch it — `GroupInput::Proposal` carries no group
        // ID, so a client write addressed to this driver would be proposed into
        // the foreign group's log and answered with a real index and term.
        // `InMemoryRaftDriver::new` refuses the same mismatch with the same
        // error.
        if group.group_id() != &state.group_id {
            return Err(ManagedDriverError::MixedGroups);
        }
        // **The record and the runtime are one transaction, staged.** The join
        // moves the mark and the register — the two fields an embedder persists —
        // and the runtime offered beside them can contradict the result at a
        // position both stand at. Running the join against live state and asking
        // the runtime afterwards left a refused adoption holding durable state
        // recovered from the very input it had just declared contradictory, with
        // an epoch move telling the embedder to write it down. So both questions
        // are asked of a candidate, and nothing is installed until every refusal
        // above the installation has had its say.
        //
        // The checkpoint is joined before the identity gate so the gate reads the
        // recovered mark as well as the held one: a takeover handed a checkpoint
        // that spent the offered ID must refuse it, and joining afterwards would
        // install the identity and only then learn it was spent.
        let candidate = state
            .adoption_candidate(checkpoint, group.runtime())
            .map_err(|reason| ManagedDriverError::InvalidControlPlaneCheckpoint { reason })?;
        if candidate.is_spent(group.node_id()) {
            return Err(ManagedDriverError::RetiredNodeId {
                node_id: group.node_id(),
            });
        }
        let (next_proposal_id, next_read_id) = adopted_watermarks(&group, PendingProposals::Carry)?;
        state.node_id = group.node_id();
        state.next_proposal_id = highest(state.next_proposal_id, next_proposal_id);
        state.next_read_id = highest(state.next_read_id, next_read_id);
        state.group = Some(group);
        // After the group, so the identity this driver now is is the one every
        // later derivation excludes. Nothing is stated to the link layer here:
        // the publication belongs to the endpoint fold below, once the recovery
        // outputs this incarnation is replaying have been folded first.
        state.install_restored_membership(candidate);
        // History then endpoint, for the reason
        // [`TransportRaftDriver::with_control_plane_checkpoint`] gives: the
        // recovery outputs are older than the committed membership the rebuilt
        // runtime reports, and a driver that took the endpoint first read every
        // one of them as a removal of what the endpoint had added. A takeover
        // reaches this with a checkpoint from another process and is the case
        // that most needs it.
        // **The publication runs whether or not the outputs applied**, and the
        // `?` that used to sit on this line is the whole of the defect. The
        // group is installed by now, so this driver *is* the replica the link
        // layer authorizes for — and returning early left the transport
        // describing the retired incarnation with nothing to re-derive it from,
        // because the policy is only republished when the cluster's membership
        // moves.
        let applied = if recovery_outputs.is_empty() {
            Ok(())
        } else {
            state.apply_recovery_outputs(recovery_outputs)
        };
        // The check above cleared the record against the runtime, so the only way
        // this refuses is a contradiction the recovery outputs themselves
        // introduced — which is a group already installed, exactly like a failed
        // apply. The driver records the refusal and stops serving; the supervisor
        // releases and rebuilds.
        let published = state
            .recorded_contradiction()
            .map_or_else(|| state.publish_adopted_membership(), Err)
            .map_err(|reason| ManagedDriverError::InvalidControlPlaneCheckpoint { reason });
        state.publish_metrics();
        applied.and(published)
    }
}
