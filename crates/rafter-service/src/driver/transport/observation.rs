#![allow(clippy::wildcard_imports)]

//! What one membership event asserts, before anything is concluded from it.
//!
//! Split from [`super::control_plane`] along the line between a *fact* and a
//! *decision*. Nothing here reads driver state or writes any: an event arrives,
//! and this file says which of the two membership facts it is and — for a
//! committed one — where it stands, what it names, and what it proves was
//! removed. [`super::reconciliation`] decides what a batch of them does to the
//! driver, and [`super::policy`] decides what the result licenses.
//!
//! The value of the separation is that the conversion is total and side-effect
//! free. It used to sit inside the router, so the one place a new
//! [`MembershipEvent`] variant can be missed was interleaved with the merge, the
//! epoch bookkeeping, and the transport call — and a reader checking the
//! `#[non_exhaustive]` wildcard had to read all three to see that it changed
//! nothing.

use std::collections::BTreeSet;

use super::super::*;

/// The membership fact one publication is derived from.
///
/// A fact rather than a set plus a decision, and that is the whole point of the
/// type. Publishing answers two questions — which principals the link layer may
/// authorize, and how far retirement reaches — and both are licensed by the same
/// one fact: what the cluster has *committed*. A caller that supplied a set and a
/// retirement flag as separate arguments could answer the two inconsistently, and
/// one did: adoption published a narrowed peer set for a committed removal and
/// withheld the retirement for it, because the two travelled apart. Here they
/// cannot.
///
/// Each variant *assigns* the fact it carries rather than merging it into what
/// was there before. Two facts are tracked separately for exactly this reason: a
/// single merged set could only ever grow, and a configuration that appended and
/// was then truncated back off the log would leave the replica it named
/// authorized forever, because no committed removal would ever arrive to take it
/// out.
pub(super) enum MembershipFact {
    /// A configuration that is effective and may still be uncommitted.
    ///
    /// It replaces the effective half and nothing else, so what it can do to the
    /// published peer set depends on which direction it moved. A replica joining
    /// under joint consensus has to be able to speak before the change commits,
    /// or it can never catch up and the change can never commit; a replica the
    /// change *drops* keeps speaking, because the committed configuration is
    /// still the floor and this fact cannot narrow past it.
    Effective(BTreeSet<NodeId>),
    /// A committed configuration, and the effective one beside it.
    ///
    /// Both halves are load-bearing and neither stands alone. `committed` is the
    /// only fact that licenses narrowing the set and retiring what left it.
    /// `effective` is what keeps an in-flight change's joiner able to speak
    /// across the same publication — a replica that rebuilt its runtime from
    /// durable storage can hold an appended-but-uncommitted addition in its log,
    /// and publishing the committed set alone would take the joiner's
    /// authorization away and stall the change that needs it.
    Committed {
        committed: CommittedObservation,
        effective: BTreeSet<NodeId>,
    },
}

/// What one membership event asserts, before it is paired with anything.
///
/// The committed arm carries only its own half: the effective membership beside
/// it is read from the runtime, which is the authority on what is in effect, and
/// this conversion deliberately holds no driver state to read it from.
pub(super) enum ObservedMembership {
    /// A configuration that is now in effect here.
    Effective(BTreeSet<NodeId>),
    /// A committed configuration, as a crossing or as an endpoint.
    Committed(CommittedObservation),
    /// The event carries no membership fact to act on.
    Nothing,
}

/// One committed membership fact: where it stands, what it names, and what it
/// proves was removed.
///
/// **A position and a removal set, and the removal set is what a position could
/// not replace.** This driver used to keep a consumer offset per provenance and
/// skip any fact at or below it, because folding a historical membership against
/// a present one reads as a removal of everything the present added. That was
/// the wrong repair for a real hazard. An offset claims a *prefix* has been
/// consumed, and nothing a driver observes is a prefix: a snapshot-recovered
/// process that then folds a crossing at index 8 has consumed neither 6 nor 7,
/// and an offset reading 8 says it has.
///
/// The repair is at the source instead. A crossing arrives as the *transition*
/// the kernel computed where the chronology is known — see
/// [`rafter::Output::ConfigurationCommitted`] — so its removal set is exact
/// wherever, whenever and however often it is folded, and there is nothing left
/// for an offset to protect.
///
/// The position still travels, and it now decides one thing: which of two
/// observations of the *current* committed membership is later, and therefore
/// which one this driver believes. That is
/// [`super::checkpoint::CurrentCommittedState`], and the comparison is the same
/// one the checkpoint join makes between two records.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct CommittedObservation {
    /// The log position this fact stands at.
    ///
    /// A configuration entry's own index for a crossing, this replica's commit
    /// index for an endpoint observation. Both name a point at which the
    /// committed membership is exactly `membership`, which is the only property
    /// the comparison needs — so the two provenances no longer need separate
    /// positions, and this is where the second cursor went.
    pub(super) through: LogIndex,
    /// The committed membership at `through`, raw as the cluster reported it.
    pub(super) membership: BTreeSet<NodeId>,
    /// The identities this fact *proves* a committed removal consumed.
    ///
    /// Non-empty only for a crossing, where it is the kernel's own
    /// `previous − configuration`. An endpoint carries no transition and proves
    /// nothing by itself; what it can still contribute is inferred by comparing
    /// its position against the one this driver holds, which is where the
    /// removals *between* two observations come from.
    pub(super) removed: BTreeSet<NodeId>,
    /// Every identity either end of this fact named, which is what the mark is
    /// raised over.
    ///
    /// A removed identity is in here, and that is the point: an ID the cluster
    /// committed is allocated whether or not it survives the transition, and a
    /// mark taken over the surviving membership alone would leave a removed ID
    /// above the mark and therefore allocatable again.
    pub(super) named: BTreeSet<NodeId>,
}

impl CommittedObservation {
    /// A configuration entry the commit index crossed, as the transition it is.
    fn crossing(
        through: LogIndex,
        previous: &MembershipConfig,
        committed: &MembershipConfig,
    ) -> Self {
        let previous: BTreeSet<NodeId> = previous.replica_ids().into_iter().collect();
        let membership: BTreeSet<NodeId> = committed.replica_ids().into_iter().collect();
        Self {
            through,
            removed: previous.difference(&membership).copied().collect(),
            named: previous.union(&membership).copied().collect(),
            membership,
        }
    }

    /// The committed membership a runtime holds, at its commit index.
    ///
    /// It proves no removal on its own — the moves that produce one are exactly
    /// the moves with no history to carry — so `removed` is empty and `named` is
    /// the membership itself.
    pub(super) fn endpoint(through: LogIndex, committed: &MembershipConfig) -> Self {
        let membership: BTreeSet<NodeId> = committed.replica_ids().into_iter().collect();
        Self {
            through,
            named: membership.clone(),
            membership,
            removed: BTreeSet::new(),
        }
    }
}

/// Reads one membership event as the fact it asserts.
///
/// `EffectiveChanged` carries the configuration this replica is operating under
/// and the two committed variants carry what the cluster has committed — that is
/// how `rafter-app` builds them — so each arm names the fact it has and nothing
/// here decides what the fact licenses.
///
/// The stream these arms read is complete: `rafter-app` reports an effective
/// change whatever moved it — a local request, replication, a truncation, or a
/// snapshot install — so a follower's joint transition and a new leader taking
/// one back both arrive. It did not always, and the widening branch was for a
/// while live code no public entry point of this driver could reach.
#[allow(
    clippy::match_same_arms,
    reason = "`Rejected` and the non-exhaustive wildcard assert nothing for \
              different reasons, and naming the known variant is the audit"
)]
pub(super) fn observed_membership<G>(event: &MembershipEvent<G>) -> ObservedMembership {
    match event {
        MembershipEvent::EffectiveChanged { membership, .. } => {
            ObservedMembership::Effective(membership.replica_ids().into_iter().collect())
        }
        // The two committed facts, read apart because they carry different
        // evidence. `Applied` is a transition — the configuration entry the
        // commit index crossed, with the membership that stood before it — so it
        // proves exactly which identities that entry removed, whatever state it
        // is folded into. `CommittedEndpoint` is an observation of the current
        // membership at this replica's commit index, for a move with no entry
        // behind it: a snapshot install, or a group opened over a runtime that
        // had already moved. It proves no removal by itself, and treating it as
        // one is what retired the replicas a catching-up replica had most
        // recently admitted.
        MembershipEvent::Applied {
            membership,
            index,
            previous,
            ..
        } => ObservedMembership::Committed(CommittedObservation::crossing(
            *index, previous, membership,
        )),
        MembershipEvent::CommittedEndpoint {
            membership, index, ..
        } => ObservedMembership::Committed(CommittedObservation::endpoint(*index, membership)),
        // A rejected change never entered the log, so there is no membership
        // fact in it to act on.
        MembershipEvent::Rejected { .. } => ObservedMembership::Nothing,
        // `MembershipEvent` is `#[non_exhaustive]`, so this arm is required and
        // is the one place a new membership fact can be missed. It is
        // deliberately not a silent skip in spirit: a variant this build does not
        // know cannot be classified as a crossing or an endpoint without guessing
        // which, and guessing wrong either manufactures a retirement or
        // suppresses one. The honest local answer is to assert nothing, and the
        // real defence is that `rafter-app` and this driver ship together — the
        // app-layer match has no wildcard, so a fourth variant stops that build
        // first.
        _ => ObservedMembership::Nothing,
    }
}
