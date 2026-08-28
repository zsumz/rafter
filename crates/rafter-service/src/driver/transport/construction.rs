#![allow(
    clippy::wildcard_imports,
    reason = "the driver is one state machine deliberately split across focused files, each opening the shared driver namespace"
)]

//! Opening a driver over one group.
//!
//! Both constructors are one membership transaction: the recovered record, the
//! replay's crossings, and the runtime's endpoint fold together and reach the
//! link layer as a single statement or not at all. A first incarnation over
//! empty storage is the empty-checkpoint case of the same path.

use crate::transport::{AuthenticatedPeerValidator, RaftTransport};

use super::adoption::{adopted_watermarks, PendingProposals};
use super::state::{DriverShared, TransportDriverState};
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
    /// Builds a driver over one already-configured group and routes its
    /// recovery outputs.
    ///
    /// The group must be quiescent — no pending proposals and no reserved
    /// reads — for the same reason [`InMemoryRaftDriver::new`] requires it: the
    /// driver correlates outcomes to waiters it created, and a waiter it did
    /// not create can never be resolved. Generated IDs start above the group's
    /// adopted watermarks.
    ///
    /// `recovery_outputs` are the outputs the recovered runtime released, taken
    /// here for the reason [`TransportRaftDriver::adopt_group`] takes them: a
    /// recovery report carries peer messages and snapshot directives that must
    /// be routed, and a caller that applied them outside the driver would drop
    /// exactly the effects a restart depends on. A first incarnation over empty
    /// storage recovers nothing and passes an empty vector.
    ///
    /// # Errors
    ///
    /// Returns [`ManagedDriverError`] when the group is poisoned, holds
    /// undrained poisoned waiters, is not quiescent, has exhausted a local
    /// ID space, the options are out of range, or the recovery outputs fail to
    /// apply.
    pub fn new(
        group: RaftGroup<G, A, R>,
        recovery_outputs: Vec<RaftOutput>,
        transport: T,
        validator: V,
        options: TransportDriverOptions,
    ) -> Result<Self, ManagedDriverError> {
        let checkpoint = PeerControlPlaneCheckpoint::empty(group.group_id().clone());
        Self::with_control_plane_checkpoint(
            group,
            recovery_outputs,
            transport,
            validator,
            options,
            checkpoint,
        )
    }

    /// Builds a driver whose peer control plane resumes from a recovered
    /// checkpoint.
    ///
    /// **The constructor a process that can crash uses.**
    /// [`TransportRaftDriver::new`] is this with an empty checkpoint, and an
    /// empty checkpoint is the honest description of a *first* incarnation over
    /// empty storage — every later one has state the Raft log cannot give back.
    /// A driver reconstructed after a crash without one starts with no
    /// high-water mark and no current committed state: the retirement floor it
    /// publishes falls back to whatever the surviving configuration names, so the
    /// identity a committed removal consumed stops being retired and becomes
    /// allocatable again. See [`PeerControlPlaneCheckpoint`] for why neither fact
    /// is re-derivable and what a stale checkpoint costs.
    ///
    /// The checkpoint is installed before any membership fact is derived from
    /// `group`, so a recovered mark of 5 beats a reconstructed committed set of
    /// `{1,2}` rather than losing to it.
    ///
    /// # Errors
    ///
    /// As [`TransportRaftDriver::new`].
    pub fn with_control_plane_checkpoint(
        group: RaftGroup<G, A, R>,
        recovery_outputs: Vec<RaftOutput>,
        transport: T,
        validator: V,
        options: TransportDriverOptions,
        checkpoint: PeerControlPlaneCheckpoint<G>,
    ) -> Result<Self, ManagedDriverError> {
        let options = options.validate()?;
        let group_id = group.group_id().clone();
        let node_id = group.node_id();
        let (next_proposal_id, next_read_id) =
            adopted_watermarks(&group, PendingProposals::Refuse)?;
        let metrics = MetricsPublisher::new(group.metrics());
        let driver = Self {
            inner: Arc::new(DriverShared::new(TransportDriverState {
                group_id,
                node_id,
                group: Some(group),
                transport,
                validator,
                options,
                metrics,
                next_proposal_id,
                next_read_id,
                write_waiters: BTreeMap::new(),
                read_waiters: BTreeMap::new(),
                refused_sends: 0,
                refused_peer_updates: 0,
                refused_non_member_frames: 0,
                effective_members: BTreeSet::new(),
                committed_members: BTreeSet::new(),
                current_committed: None,
                committed_id_high_water: None,
                published_policy: None,
                contradiction: None,
                staged_membership: None,
                checkpoint_epoch: 0,
                shutting_down: false,
            })),
        };
        // **One membership transaction spans this whole constructor**, and the
        // three statements below are its three inputs rather than three
        // transactions. It opens here because that order is also the contract:
        // the spent test reads the recovered mark and the recovered current state
        // together, and a membership fact derived ahead of them would be derived
        // against state the crash erased.
        driver
            .inner
            .lock()
            .open_membership_transaction(checkpoint)
            .map_err(|reason| ManagedDriverError::InvalidControlPlaneCheckpoint { reason })?;
        // **The history, then the endpoint**, which is a preference rather than a
        // correctness requirement: each recovery output carries its own
        // transition, so folding one out of order proves the same removals. What
        // the order buys is a current committed state that ends level with the
        // runtime rather than at the last entry replayed.
        //
        // The membership these outputs carry folds into the open transaction and
        // installs nothing. Everything else in the report — peer messages,
        // snapshot directives, waiter resolutions — routes normally, because none
        // of it is a permanent statement about who may speak.
        if !recovery_outputs.is_empty() {
            driver
                .inner
                .lock()
                .apply_recovery_outputs(recovery_outputs)?;
        }
        // The transaction closes: the runtime's endpoint is the last input, and
        // the single installation and single publication behind it are the only
        // things this constructor states to the link layer.
        //
        // Published before the driver serves anything, so the transport's policy
        // is the group's membership from construction onward rather than
        // undefined until the first membership change. A group that never changes
        // membership would otherwise never tell its link layer anything, and a
        // recovery report carries no membership event to stand in.
        //
        // Fallible, and the failures are the shapes a constructor must not
        // absorb: a crossing the replay carried disagreeing with the restored
        // record, and the record and the runtime standing at one position and
        // disagreeing about the committed membership there. **Nothing has been
        // published when this refuses**, including whatever a valid prefix of the
        // replay would have licensed.
        driver
            .inner
            .lock()
            .commit_membership_transaction()
            .map_err(|reason| ManagedDriverError::InvalidControlPlaneCheckpoint { reason })?;
        Ok(driver)
    }
}
