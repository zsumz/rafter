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
    /// Caller-defined metadata; Rafter does not interpret or persist it.
    pub metadata: BTreeMap<String, String>,
}

/// A requested membership change at the app layer.
///
/// This enum is exhaustive because it is the closed command vocabulary the app
/// layer translates into Raft membership inputs. A new operation must make
/// planners and executors choose its safety flow explicitly.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MembershipChange {
    /// Add a non-voting replica without changing the voter quorum.
    AddLearner {
        /// Identity of the new learner.
        node_id: NodeId,
        /// Opaque caller metadata for the learner.
        info: NodeInfo,
    },
    /// Promote a caught-up learner into the voter set.
    PromoteLearner {
        /// Learner identity to promote.
        node_id: NodeId,
        /// Replication floor that must be satisfied before promotion.
        barrier: PromotionBarrier,
    },
    /// Remove a voter or learner from the group.
    RemoveNode {
        /// Replica identity to remove.
        node_id: NodeId,
    },
    /// Enter joint consensus toward a target membership.
    EnterJoint {
        /// Stable membership desired after leaving joint consensus.
        target: MembershipSet,
        /// Catch-up proofs required for learners promoted by the target.
        promotion_barriers: Vec<PromotionBarrier>,
    },
    /// Commit the stable target of the current joint configuration.
    LeaveJoint,
    /// Plan the safe learner/joint-consensus flow to a voter set.
    ChangeVoters {
        /// Desired stable membership.
        target: MembershipSet,
    },
}

/// One explicit step in a safe membership flow.
///
/// This enum is exhaustive because every emitted step is an obligation for a
/// membership executor. A new step must break executors that would otherwise
/// skip it through a wildcard.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MembershipStep {
    /// Submit an add-learner configuration.
    AddLearner(NodeId),
    /// Wait until the named learner reaches its promotion barrier.
    WaitForCatchUp(NodeId),
    /// Make the caught-up learner a voter.
    PromoteLearner(NodeId),
    /// Submit the joint configuration.
    EnterJoint(MembershipSet),
    /// Submit the stable configuration that ends joint consensus.
    LeaveJoint,
    /// Remove the named replica from membership.
    RemoveNode(NodeId),
    /// Fence the removed identity at the caller's transport boundary.
    FencePeer(NodeId),
}

/// A planned membership flow for one group.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MembershipPlan<G> {
    /// Group whose membership the plan changes.
    pub group_id: G,
    /// Ordered obligations; later steps must not run before earlier ones.
    pub steps: Vec<MembershipStep>,
}

/// Status for one membership step report.
///
/// This enum is exhaustive because a step is pending, completed, or failed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MembershipStepStatus {
    /// The step has not reached a terminal result.
    Pending,
    /// The step completed, optionally at a committed log index.
    Completed {
        /// Commit or observation index that completed the step.
        at: Option<LogIndex>,
    },
    /// The step reached a terminal failure.
    Failed {
        /// Caller- or protocol-provided diagnostic.
        reason: String,
    },
}

/// Report for one membership step.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MembershipStepReport {
    /// Planned obligation being reported.
    pub step: MembershipStep,
    /// Current or terminal state of the obligation.
    pub status: MembershipStepStatus,
}

/// Progress report for a membership change flow.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MembershipChangeReport<G> {
    /// Group whose membership is changing.
    pub group_id: G,
    /// Log index observed when the flow began.
    pub started_at: LogIndex,
    /// Log index that completed the flow, when complete.
    pub completed_at: Option<LogIndex>,
    /// Ordered status for every planned step.
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
/// committed" and "which identities did this consume" — and only one of the two
/// variants below answers the second question. An [`MembershipEvent::Applied`]
/// is a *transition*: it carries the membership that stood immediately before
/// the configuration entry as well as the entry's own, so the identities it
/// removed are exactly `previous − membership` wherever it is folded. A
/// [`MembershipEvent::CommittedEndpoint`] is a positioned *observation* of the
/// membership now, for a move with no entry behind it, and it removes nothing on
/// its own — a replica that installs a snapshot at commit 10 learns the boundary
/// configuration and learns nothing about the configurations that committed and
/// were superseded below it.
///
/// A consumer that could not tell them apart subtracted every committed fact
/// from its own current membership. That is right only when the two stand at the
/// same point, and a replayed history is exactly the case where they do not: a
/// process holding a later state read each historical configuration as a removal
/// of everything the later ones added, so a log that only ever *added* replicas
/// retired them. The two facts are separate variants for the same reason the
/// effective and committed halves are: they license different conclusions, and a
/// consumer that cannot tell them apart draws the wrong one.
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
        /// Group whose effective membership changed.
        group_id: G,
        /// Local log observation point.
        index: LogIndex,
        /// Term at the observation point.
        term: Term,
        /// Membership the replica now operates under.
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
    /// [`MembershipEvent::CommittedEndpoint`], whose index is a commit index and
    /// covers nothing beneath itself. Here the index is a position a committed
    /// configuration genuinely occupies, so two of these can be ordered against
    /// each other and against an endpoint observation.
    ///
    /// **`previous` and `membership` are the two ends of one transition, and
    /// that is what makes this event foldable out of order.** A consumer that
    /// retires identities needs the difference — who this configuration removed
    /// — and a membership state alone does not carry one. Subtracted from the
    /// consumer's own current membership it is right only when that membership
    /// happens to stand immediately before this entry, and a replayed stream is
    /// exactly the case where it does not: a process holding a later state reads
    /// each historical configuration as a removal of everything the later ones
    /// added, so an addition-only history permanently retires the replicas it
    /// added. With both ends carried, `previous − membership` is the removal set
    /// this entry actually committed, wherever and however often it is folded.
    Applied {
        /// Group whose configuration entry committed.
        group_id: G,
        /// Index of the committed configuration entry.
        index: LogIndex,
        /// Term of the committed configuration entry.
        term: Term,
        /// The membership in effect immediately before this entry, as the
        /// kernel computed it where the chronology is known.
        previous: MembershipConfig,
        /// Membership established by this committed entry.
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
    /// So this carries no `previous`, and its absence is the contract rather
    /// than an omission: there is no transition to name. A consumer may compare
    /// this observation against one it already holds and conclude what the pair
    /// proves — an identity named at the earlier position and absent at the
    /// later one was removed between them — but it may not read this fact alone
    /// as a removal of anything. The events of one report are in nondecreasing
    /// `index` order, and this one is last when it appears at all.
    CommittedEndpoint {
        /// Group whose committed endpoint was observed.
        group_id: G,
        /// Commit index at the observation point.
        index: LogIndex,
        /// Term at the observation point.
        term: Term,
        /// Committed membership observed at the endpoint.
        membership: MembershipConfig,
    },
    /// A requested membership proposal was provably not appended.
    Rejected {
        /// Group that refused the membership proposal.
        group_id: G,
        /// Protocol refusal reason.
        reason: ProposalRejection,
        /// Best-effort leader identity observed with the refusal.
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
