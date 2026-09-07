//! What a group's membership reporting is owed against.
//!
//! Two memberships and a queue of crossings, durable across steps and
//! advanced in exactly one place. The take/restore pair beside them is what
//! keeps a discarded report's delta owed rather than reported into a value
//! no caller received.

use super::{
    Debug, GroupStepReport, LogIndex, MembershipConfig, MembershipEvent, PersistedRaftRuntime,
    RaftGroup, RaftOutput, ReplicatedStateMachine, Term,
};

/// One committed configuration this group crossed and has not yet reported.
///
/// Queued rather than compared, because a comparison cannot see it. The commit
/// index can cross several configuration entries in one step, and the two the
/// comparison reads — the memberships before and after — can be *equal* across
/// a pair that added a replica and removed it again. The kernel names each
/// crossing as it happens; this is where the group holds them until a report
/// carries them out.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct CommittedConfigurationCrossing {
    /// The configuration entry's own index, not the commit index the step
    /// reached.
    pub(super) index: LogIndex,
    pub(super) term: Term,
    /// The membership in effect immediately before this entry, as the kernel
    /// computed it at the crossing.
    ///
    /// Carried rather than re-derived for the reason
    /// [`rafter::Output::ConfigurationCommitted`] carries it: the difference
    /// between the two memberships is only chronological where the log is, and
    /// nothing above the kernel can recover which of its own past states stood
    /// at a historical index.
    pub(super) previous: MembershipConfig,
    pub(super) membership: MembershipConfig,
}

/// What a group's membership reporting is owed against.
///
/// Two memberships and a queue. The memberships are the state the effective and
/// final-committed comparisons are taken against; the queue is the committed
/// configurations the kernel named that no report has carried yet. Both are
/// durable across steps and both advance in exactly one place — where a report's
/// membership events are built.
///
/// Cloned before a report is built and put back when that report is discarded,
/// which is what keeps a delta owed rather than reported into a value the caller
/// never received. Every site that builds a report and then decides it cannot
/// return it takes one of these first; there is no other way to discard a report
/// without losing what it carried.
///
/// **Opaque on purpose.** The fields are private and there is no public
/// constructor: the only ways to obtain one are
/// [`RaftGroup::into_parts`] and the only thing to do with one is hand it to
/// [`RaftGroup::from_parts`]. A caller that could forge one could tell a group it
/// had already reported a configuration it never reported, which is the same
/// silent loss this whole mechanism exists to prevent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MembershipReportMark {
    pub(super) effective: MembershipConfig,
    pub(super) committed: MembershipConfig,
    /// Committed configurations named by the kernel and not yet reported, in
    /// the order the commit index crossed them.
    pub(super) crossed: Vec<CommittedConfigurationCrossing>,
}

impl<G, A, R> RaftGroup<G, A, R>
where
    G: Clone + Ord + Debug + Send + Sync + 'static,
    A: ReplicatedStateMachine,
    A::CommandResult: Clone,
    R: PersistedRaftRuntime,
{
    /// Queues every committed configuration one step's outputs name, before any
    /// of them is handled.
    ///
    /// **Infallible, and that is its whole job.** The scan that handles outputs
    /// is fallible per output — decoding an `Apply` payload runs inside it — so
    /// a configuration entry sitting behind a payload the state machine refuses
    /// is one the scan never reaches. Queueing here makes "the commit index
    /// crossed this configuration" a fact of the step rather than a consequence
    /// of the step succeeding, which is the same promise the mark already makes
    /// for the two memberships beside it.
    ///
    /// It borrows the vector rather than consuming it, so the handling scan
    /// still sees every output in kernel order. Nothing is reported from here:
    /// the queue is drained by [`RaftGroup::record_membership_changes`] at the
    /// end of the step, in index order and after the effective comparison, so
    /// the reported order is exactly what it was.
    pub(super) fn queue_committed_configurations(&mut self, outputs: &[RaftOutput]) {
        for output in outputs {
            if let RaftOutput::ConfigurationCommitted {
                index,
                term,
                previous,
                configuration,
            } = output
            {
                self.record_committed_configuration(
                    *index,
                    *term,
                    previous.clone(),
                    configuration.membership_config(),
                );
            }
        }
    }

    /// Queues one committed configuration the kernel named.
    ///
    /// Held on the group rather than pushed straight into the report, because the
    /// report's membership list is ordered effective-then-committed and the
    /// kernel's outputs arrive before the effective comparison has run. Queueing
    /// is also what makes the fact survive a step that fails after it: the queue
    /// is part of the mark, so it is owed on exactly the same terms as the two
    /// memberships beside it.
    pub(super) fn record_committed_configuration(
        &mut self,
        index: LogIndex,
        term: Term,
        previous: MembershipConfig,
        membership: MembershipConfig,
    ) {
        self.reported_membership
            .crossed
            .push(CommittedConfigurationCrossing {
                index,
                term,
                previous,
                membership,
            });
    }

    /// Takes the mark the membership reporting is currently owed against.
    ///
    /// Paired with [`RaftGroup::restore_membership_report_mark`] at every site
    /// that builds a report and then decides it cannot return it.
    pub(super) fn membership_report_mark(&self) -> MembershipReportMark {
        self.reported_membership.clone()
    }

    /// Puts back everything a report carried, because that report is being
    /// discarded.
    ///
    /// The one operation that makes "the mark advances when a report is
    /// returned" true rather than approximately true. A report a caller never
    /// receives reported nothing, so the delta it carried is owed again.
    ///
    /// **Two halves, and the second is why the discarded report is an argument.**
    /// The two memberships come from `mark`, taken *before* the report was built,
    /// which restores the whole comparison-derived delta including anything an
    /// earlier failure had already left owed. The crossing queue cannot come from
    /// there: the kernel outputs that filled it arrived *after* `mark` was taken,
    /// so putting back the mark's own queue would drop exactly the committed
    /// configurations this step discovered. It is rebuilt from the discarded
    /// report instead, which holds one `Applied` per owed transition in order and
    /// is therefore a faithful record of what the group was about to hand over.
    /// Re-reporting it produces the same event sequence: the final comparison
    /// then finds the committed membership already accounted for and adds
    /// nothing.
    ///
    /// **Only the crossings are put back, and an endpoint observation must not
    /// be.** The discarded report can end with a
    /// [`MembershipEvent::CommittedEndpoint`], and that fact is *already*
    /// restored by the line above: `mark.committed` is the pre-report value, so
    /// the comparison re-derives the endpoint on the next report by itself.
    /// Pushing it into the crossing queue as well would re-emit it as an
    /// `Applied` — a crossing at an index no configuration entry sits at, which
    /// is exactly the provenance lie the two variants exist to prevent, and a
    /// consumer would advance its crossing position over history it never saw.
    ///
    /// The match names every variant rather than ending in a wildcard, and
    /// within the defining crate `#[non_exhaustive]` does not license one. So a
    /// fourth variant stops the build here and is classified deliberately,
    /// instead of defaulting into the queue or silently out of it.
    pub(super) fn restore_membership_report_mark(
        &mut self,
        mark: MembershipReportMark,
        discarded: &GroupStepReport<G, A::CommandResult>,
    ) {
        self.reported_membership = mark;
        self.reported_membership.crossed = discarded
            .membership_events
            .iter()
            .filter_map(|event| match event {
                MembershipEvent::Applied {
                    index,
                    term,
                    previous,
                    membership,
                    ..
                } => Some(CommittedConfigurationCrossing {
                    index: *index,
                    term: *term,
                    previous: previous.clone(),
                    membership: membership.clone(),
                }),
                MembershipEvent::CommittedEndpoint { .. }
                | MembershipEvent::EffectiveChanged { .. }
                | MembershipEvent::Rejected { .. } => None,
            })
            .collect();
    }
}
