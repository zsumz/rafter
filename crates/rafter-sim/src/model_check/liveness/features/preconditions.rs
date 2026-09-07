//! Measured preconditions every liveness report must carry.
//!
//! A monitor may only claim progress under a stable membership with a mutually
//! reachable quorum, so the reachable group is computed exactly rather than
//! inferred from the absence of a partition.

use std::collections::BTreeSet;

use rafter::{MembershipConfig, NodeId};

use crate::model_check::state::ExplorationState;
use crate::Cluster;

use super::super::driver::single_leader;
use super::{
    EvidenceStatus, FaultStateRequirement, LivenessPreconditionProbe, LivenessPreconditions,
};

impl EvidenceStatus {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Satisfied => "satisfied",
            Self::Unsatisfied => "unsatisfied",
            Self::NotRequired => "not-required",
        }
    }

    pub(super) const fn is_satisfied(self) -> bool {
        matches!(self, Self::Satisfied)
    }

    pub(super) const fn is_required(self) -> bool {
        !matches!(self, Self::NotRequired)
    }
}

impl FaultStateRequirement {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Stopped => "stopped",
            Self::ActivePartition => "active-partition",
        }
    }
}

impl LivenessPreconditions {
    pub(in crate::model_check::liveness) fn capture(
        state: &ExplorationState,
        probe: LivenessPreconditionProbe,
    ) -> Self {
        let membership_node = probe.leader.or_else(|| single_leader(state));
        let (stable_membership, voters) = membership_node.map_or_else(
            || (EvidenceStatus::Unsatisfied, Vec::new()),
            |node_id| match state.cluster().effective_membership(node_id) {
                MembershipConfig::Stable(membership)
                    if state.cluster().committed_membership(node_id)
                        == MembershipConfig::Stable(membership.clone()) =>
                {
                    (EvidenceStatus::Satisfied, membership.voters().to_vec())
                }
                MembershipConfig::Stable(membership) => {
                    (EvidenceStatus::Unsatisfied, membership.voters().to_vec())
                }
                MembershipConfig::Joint(joint) => (
                    EvidenceStatus::Unsatisfied,
                    joint
                        .old()
                        .voters()
                        .iter()
                        .chain(joint.new_membership().voters())
                        .copied()
                        .collect::<BTreeSet<_>>()
                        .into_iter()
                        .collect(),
                ),
            },
        );
        let quorum_size = voters.len() / 2 + 1;
        let reachable_voters = largest_mutually_reachable_group(state.cluster(), &voters);
        let mutually_reachable_quorum = evidence(reachable_voters >= quorum_size);
        let stable_leader = probe
            .stable_leader_observed
            .map_or(EvidenceStatus::NotRequired, evidence);
        let accepted_proposal = probe
            .accepted_proposal_observed
            .map_or(EvidenceStatus::NotRequired, evidence);
        let authority_loss = probe
            .authority_loss_observed
            .map_or(EvidenceStatus::NotRequired, evidence);
        let partition_active = super::super::driver::has_partition(state.cluster());
        let faults_stopped = !partition_active;
        let fault_state = evidence(match probe.fault_requirement {
            FaultStateRequirement::Stopped => faults_stopped,
            FaultStateRequirement::ActivePartition => partition_active,
        });

        Self {
            fault_requirement: probe.fault_requirement,
            fault_state,
            faults_stopped,
            partition_active,
            mutually_reachable_quorum,
            stable_membership,
            stable_leader,
            accepted_proposal,
            authority_loss,
            voter_ids: voters.clone(),
            reachable_voters,
            quorum_size,
            unavailable_voters: voters.len().saturating_sub(reachable_voters),
        }
    }

    pub(super) fn validate(&self) -> Result<(), &'static str> {
        for (name, status) in [
            ("fault_state", self.fault_state),
            ("mutually_reachable_quorum", self.mutually_reachable_quorum),
            ("stable_membership", self.stable_membership),
            ("stable_leader", self.stable_leader),
            ("accepted_proposal", self.accepted_proposal),
            ("authority_loss", self.authority_loss),
        ] {
            if status == EvidenceStatus::Unsatisfied {
                return Err(name);
            }
        }
        if self.quorum_size == 0 || self.reachable_voters < self.quorum_size {
            return Err("reachable_voters");
        }
        if self.voter_ids.is_empty()
            || self.quorum_size != self.voter_ids.len() / 2 + 1
            || self.unavailable_voters != self.voter_ids.len().saturating_sub(self.reachable_voters)
        {
            return Err("voter_ids");
        }
        Ok(())
    }
}

const fn evidence(value: bool) -> EvidenceStatus {
    if value {
        EvidenceStatus::Satisfied
    } else {
        EvidenceStatus::Unsatisfied
    }
}

fn largest_mutually_reachable_group(cluster: &Cluster, voters: &[NodeId]) -> usize {
    fn search(
        cluster: &Cluster,
        voters: &[NodeId],
        chosen: &mut Vec<NodeId>,
        index: usize,
    ) -> usize {
        if index == voters.len() {
            return chosen.len();
        }
        let without = search(cluster, voters, chosen, index + 1);
        let candidate = voters[index];
        if chosen
            .iter()
            .all(|member| !cluster.partitioned(*member, candidate))
        {
            chosen.push(candidate);
            let with = search(cluster, voters, chosen, index + 1);
            chosen.pop();
            without.max(with)
        } else {
            without
        }
    }

    search(cluster, voters, &mut Vec::new(), 0)
}
