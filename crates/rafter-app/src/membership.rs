//! Application-layer membership planning and reporting types.
//!
//! These types represent safe membership flows while leaving transport
//! peer updates and fencing under the caller's runtime policy.

use std::collections::BTreeMap;

use rafter::{
    LogIndex, MembershipConfig, MembershipSet, NodeId, PromotionBarrier, ProposalRejection, Term,
};

/// Application/runtime metadata associated with a Raft node.
///
/// Rafter treats this as opaque app-layer information. Transport identity,
/// addresses, and authorization details remain the caller's responsibility.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct NodeInfo {
    pub metadata: BTreeMap<String, String>,
}

/// A requested membership change at the app layer.
///
/// This enum is exhaustive for the membership operations planned by
/// `rafter-app` today.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MembershipChange {
    AddLearner {
        node_id: NodeId,
        info: NodeInfo,
    },
    PromoteLearner {
        node_id: NodeId,
        barrier: PromotionBarrier,
    },
    RemoveNode {
        node_id: NodeId,
    },
    EnterJoint {
        target: MembershipSet,
        promotion_barriers: Vec<PromotionBarrier>,
    },
    LeaveJoint,
    ChangeVoters {
        target: MembershipSet,
    },
}

/// One explicit step in a safe membership flow.
///
/// This enum is exhaustive for the flow steps currently emitted by
/// `MembershipPlan`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MembershipStep {
    AddLearner(NodeId),
    WaitForCatchUp(NodeId),
    PromoteLearner(NodeId),
    EnterJoint(MembershipSet),
    LeaveJoint,
    RemoveNode(NodeId),
    FencePeer(NodeId),
}

/// A planned membership flow for one group.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MembershipPlan<G> {
    pub group_id: G,
    pub steps: Vec<MembershipStep>,
}

/// Status for one membership step report.
///
/// This enum is exhaustive for the step states reported by `rafter-app`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MembershipStepStatus {
    Pending,
    Completed { at: Option<LogIndex> },
    Failed { reason: String },
}

/// Report for one membership step.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MembershipStepReport {
    pub step: MembershipStep,
    pub status: MembershipStepStatus,
}

/// Progress report for a membership change flow.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MembershipChangeReport<G> {
    pub group_id: G,
    pub started_at: LogIndex,
    pub completed_at: Option<LogIndex>,
    pub steps: Vec<MembershipStepReport>,
}

/// Membership side effects observed by a manual group driver.
///
/// Three facts and a refusal, and every one of them is a different *kind* of
/// evidence. An effective configuration may still be uncommitted and may still
/// be taken back, so it may only *widen* a peer set; a committed one is the only
/// fact that licenses narrowing it or fencing what left; and the two committed
/// facts differ in what their position in the log is evidence *of*.
///
/// **That last split is the newest and the least obvious.** A committed fact is
/// read by a consumer in two ways at once — "what does the cluster have
/// committed" and "how far through the committed configuration stream am I" —
/// and only one of the two variants below answers the second question. An
/// [`MembershipEvent::Applied`] carries the index of the configuration entry it
/// crossed, so consuming it really does cover that point in the stream. A
/// [`MembershipEvent::CommittedEndpoint`] carries the commit index and covers
/// **nothing beneath itself**: a replica that installs a snapshot at commit 10
/// learns the boundary configuration and learns nothing about the configurations
/// that committed and were superseded below it.
///
/// A consumer that kept one position for both therefore claimed history coverage
/// it never had, and skipped the real crossings a *later* recovery replayed
/// beneath it — so an identity a committed removal spent was never spent locally
/// and its fence was never owed. The two facts are separate variants for the
/// same reason the effective and committed halves are: they license different
/// conclusions, and a consumer that cannot tell them apart draws the wrong one.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum MembershipEvent<G> {
    /// The configuration this replica is now operating under changed.
    ///
    /// Named for the fact rather than for a cause, because it has several and a
    /// consumer must not care which: a local membership request, a
    /// configuration entry that arrived by replication, a new leader truncating
    /// an uncommitted one back off the log, and a snapshot install all reach
    /// here. The previous name said `Appended`, which was true of exactly one
    /// of those and false of the truncation that is the most dangerous to miss.
    ///
    /// `index` and `term` are an **observation point**, not the location of a
    /// configuration entry. They are this replica's last log index and the term
    /// at it when the change was observed, so for an append they do name the
    /// entry that carried the configuration, and for a truncation or a snapshot
    /// install they name where the log now ends. A consumer that needs the
    /// entry itself must read the log; a consumer keeping a peer set current —
    /// which is what this event is for — needs only the membership.
    EffectiveChanged {
        group_id: G,
        index: LogIndex,
        term: Term,
        membership: MembershipConfig,
    },
    /// The commit index crossed this exact configuration entry.
    ///
    /// **One of these per configuration the commit index crossed, in index
    /// order, and not one per step.** A step can commit several configurations
    /// at once — a replica catching up receives them under a single leader
    /// commit — and every one of them is a membership the cluster genuinely
    /// authorized. A consumer that retires identities must see each: a
    /// configuration that added a replica and a later one that removed it can
    /// leave the committed membership *identical* across the step, so a
    /// difference of endpoints reports nothing at all while an identity was
    /// consumed in the middle.
    ///
    /// **`index` and `term` name the configuration entry itself, always.** That
    /// is what separates this variant from
    /// [`MembershipEvent::CommittedEndpoint`] and what makes the index usable as
    /// a position in the committed configuration stream: a consumer that has
    /// taken this fact has genuinely covered `index`, so a later replay of the
    /// same entry is history it may skip. The variant used to carry the commit
    /// index instead whenever the kernel could not name an entry, which made the
    /// two provenances indistinguishable and the position a claim no consumer
    /// could check.
    Applied {
        group_id: G,
        index: LogIndex,
        term: Term,
        membership: MembershipConfig,
    },
    /// The committed configuration now stands here, with no crossing to replay.
    ///
    /// The end of the stream rather than a point in it, and the two moves that
    /// produce it are exactly the two the kernel cannot attribute to an entry: a
    /// snapshot install, whose boundary configuration replaces the state
    /// machine, and a group opened over a runtime whose commit index had already
    /// moved. Both are real committed facts — they authorized replicas and spent
    /// identities — and both are reported, because a consumer that missed them
    /// would keep publishing a configuration the cluster has left behind.
    ///
    /// **`index` and `term` are an observation point, and the index covers
    /// nothing beneath itself.** It is this replica's commit index when the
    /// comparison ran, which is a sound and monotone position *for this fact*
    /// and evidence of nothing about the configurations below it. A snapshot
    /// install reports the boundary configuration and nothing about what
    /// committed and was superseded below it: those are not in the snapshot, and
    /// no replica can reconstruct them locally. See
    /// [`crate::snapshot::SnapshotEvent::Apply`].
    ///
    /// So a consumer that keeps a position in the committed configuration stream
    /// must keep this one **apart from** the position it advances for
    /// [`MembershipEvent::Applied`], and must never let this one suppress a
    /// crossing. The events of one report are still in nondecreasing `index`
    /// order, and this one is last when it appears at all.
    CommittedEndpoint {
        group_id: G,
        index: LogIndex,
        term: Term,
        membership: MembershipConfig,
    },
    Rejected {
        group_id: G,
        reason: ProposalRejection,
        leader_hint: Option<NodeId>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn membership(voters: &[u64], learners: &[u64]) -> MembershipSet {
        MembershipSet::new(
            voters.iter().copied().map(NodeId).collect(),
            learners.iter().copied().map(NodeId).collect(),
        )
        .expect("test membership is valid")
    }

    #[test]
    fn membership_change_types_represent_supported_requests() {
        let mut info = NodeInfo::default();
        info.metadata
            .insert("zone".to_owned(), "test-zone".to_owned());

        let changes = [
            MembershipChange::AddLearner {
                node_id: NodeId(4),
                info,
            },
            MembershipChange::PromoteLearner {
                node_id: NodeId(4),
                barrier: PromotionBarrier {
                    learner_id: NodeId(4),
                    required_match_index: LogIndex(9),
                },
            },
            MembershipChange::RemoveNode { node_id: NodeId(2) },
            MembershipChange::EnterJoint {
                target: membership(&[1, 3, 4], &[]),
                promotion_barriers: Vec::new(),
            },
            MembershipChange::LeaveJoint,
            MembershipChange::ChangeVoters {
                target: membership(&[1, 3, 4], &[]),
            },
        ];

        assert_eq!(changes.len(), 6);
    }

    #[test]
    fn membership_plan_can_represent_safe_flow_and_fencing() {
        let target = membership(&[1, 3, 4], &[]);
        let plan = MembershipPlan {
            group_id: "group-a",
            steps: vec![
                MembershipStep::AddLearner(NodeId(4)),
                MembershipStep::WaitForCatchUp(NodeId(4)),
                MembershipStep::PromoteLearner(NodeId(4)),
                MembershipStep::EnterJoint(target.clone()),
                MembershipStep::LeaveJoint,
                MembershipStep::RemoveNode(NodeId(2)),
                MembershipStep::FencePeer(NodeId(2)),
            ],
        };

        assert!(matches!(
            plan.steps[0],
            MembershipStep::AddLearner(NodeId(4))
        ));
        assert!(matches!(
            plan.steps[1],
            MembershipStep::WaitForCatchUp(NodeId(4))
        ));
        assert!(matches!(
            plan.steps[2],
            MembershipStep::PromoteLearner(NodeId(4))
        ));
        assert_eq!(plan.steps[3], MembershipStep::EnterJoint(target));
        assert!(matches!(plan.steps[4], MembershipStep::LeaveJoint));
        assert!(matches!(
            plan.steps[5],
            MembershipStep::RemoveNode(NodeId(2))
        ));
        assert!(matches!(
            plan.steps[6],
            MembershipStep::FencePeer(NodeId(2))
        ));
    }

    #[test]
    fn membership_change_report_tracks_step_progress() {
        let report = MembershipChangeReport {
            group_id: 7_u64,
            started_at: LogIndex(10),
            completed_at: Some(LogIndex(15)),
            steps: vec![
                MembershipStepReport {
                    step: MembershipStep::AddLearner(NodeId(4)),
                    status: MembershipStepStatus::Completed {
                        at: Some(LogIndex(11)),
                    },
                },
                MembershipStepReport {
                    step: MembershipStep::FencePeer(NodeId(2)),
                    status: MembershipStepStatus::Pending,
                },
            ],
        };

        assert_eq!(report.group_id, 7);
        assert_eq!(report.started_at, LogIndex(10));
        assert_eq!(report.completed_at, Some(LogIndex(15)));
        assert_eq!(report.steps.len(), 2);
    }
}
