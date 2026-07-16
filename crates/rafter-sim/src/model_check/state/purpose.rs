use rafter::{
    CommittedConfiguration, LogEntryKind, MembershipConfig, Message, NodeId, ReplicationState,
};

use crate::records::LocalProposalEvent;
use crate::{Cluster, Envelope, ReadRegistered};

use super::super::{observations::Observation, scheduling::Operation};

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct JointConfigurationIdentity {
    committed: CommittedConfiguration,
    membership: MembershipConfig,
}

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq)]
pub(super) struct PurposeWitnessHistory {
    restarted_joint_configurations: Vec<JointConfigurationIdentity>,
    installed_joint_configurations: Vec<JointConfigurationIdentity>,
}

impl PurposeWitnessHistory {
    pub(super) fn record_transition(
        &mut self,
        before: &Cluster,
        after: &Cluster,
        operation: &Operation,
        emitted: &[Envelope],
        local_proposals: &[LocalProposalEvent],
        read_registration: Option<&ReadRegistered>,
    ) -> Vec<Observation> {
        let mut observations = Vec::new();
        if production_config_is_effective(after) && commit_advanced(before, after) {
            observations.push(Observation::ProductionConfigCommitObserved);
        }
        if window_one_blocked_application_proposal(
            before,
            after,
            operation,
            emitted,
            local_proposals,
        ) {
            observations.push(Observation::WindowOneBackpressureObserved);
        }
        if lease_fast_path_granted(before, after, operation, read_registration) {
            observations.push(Observation::LeaseFastPathReadGranted);
        }
        self.record_snapshot_installations(before, after, &mut observations);
        observations
    }

    pub(super) fn record_restart(
        &mut self,
        before: &Cluster,
        after: &Cluster,
        node_id: NodeId,
    ) -> Vec<Observation> {
        let mut observations = Vec::new();
        let Some(before_identity) = joint_configuration_identity(before, node_id) else {
            return observations;
        };
        if joint_configuration_identity(after, node_id).as_ref() != Some(&before_identity) {
            return observations;
        }
        push_unique(&mut self.restarted_joint_configurations, before_identity);
        self.mark_joint_recovery_if_complete(&mut observations);
        observations
    }

    fn record_snapshot_installations(
        &mut self,
        before: &Cluster,
        after: &Cluster,
        observations: &mut Vec<Observation>,
    ) {
        let installed_before = before.snapshot_installs().len();
        let Some(new_installs) = after.snapshot_installs().get(installed_before..) else {
            return;
        };
        for installation in new_installs {
            if let Some(identity) = installed_joint_configuration_identity(
                after,
                installation.node_id,
                installation.committed_membership.as_ref(),
            ) {
                push_unique(&mut self.installed_joint_configurations, identity);
            }
        }
        self.mark_joint_recovery_if_complete(observations);
    }

    fn mark_joint_recovery_if_complete(&self, observations: &mut Vec<Observation>) {
        if self.restarted_joint_configurations.iter().any(|restarted| {
            self.installed_joint_configurations
                .iter()
                .any(|installed| installed == restarted)
        }) {
            observations.push(Observation::JointConfigRestartSnapshotRecovered);
        }
    }
}

fn production_config_is_effective(cluster: &Cluster) -> bool {
    !cluster.configs.is_empty()
        && cluster
            .configs
            .values()
            .all(|config| config.pre_vote() && config.check_quorum())
}

fn commit_advanced(before: &Cluster, after: &Cluster) -> bool {
    before
        .nodes
        .keys()
        .any(|node_id| after.commit_index(*node_id) > before.commit_index(*node_id))
}

fn window_one_blocked_application_proposal(
    before: &Cluster,
    after: &Cluster,
    operation: &Operation,
    emitted: &[Envelope],
    local_proposals: &[LocalProposalEvent],
) -> bool {
    let Operation::Propose {
        to,
        proposal_id,
        stale_leader: false,
    } = operation
    else {
        return false;
    };
    let Some(config) = before.configs.get(to) else {
        return false;
    };
    if config.max_inflight_appends().max(1) != 1
        || after.last_log_index(*to) <= before.last_log_index(*to)
        || !local_proposals.iter().any(|event| {
            matches!(
                event,
                LocalProposalEvent::Appended {
                    node_id,
                    proposal_id: appended_id,
                    ..
                } if node_id == to && appended_id.0 == proposal_id.0
            )
        })
    {
        return false;
    }

    before
        .leader_replication_progress(*to)
        .into_iter()
        .any(|progress| {
            progress.state == ReplicationState::Replicating
                && progress.next_index > progress.match_index.next()
                && queued_application_append(before, *to, progress.follower_id)
                && emitted_empty_append(emitted, *to, progress.follower_id)
        })
}

fn queued_application_append(cluster: &Cluster, from: NodeId, to: NodeId) -> bool {
    cluster.network.iter().any(|queued| {
        queued.envelope.from == from
            && queued.envelope.to == to
            && matches!(
                &queued.envelope.message,
                Message::AppendEntries(request)
                    if request.entries.iter().any(|entry| {
                        matches!(entry.kind, LogEntryKind::Application(_))
                    })
            )
    })
}

fn emitted_empty_append(emitted: &[Envelope], from: NodeId, to: NodeId) -> bool {
    emitted.iter().any(|envelope| {
        envelope.from == from
            && envelope.to == to
            && matches!(
                &envelope.message,
                Message::AppendEntries(request) if request.entries.is_empty()
            )
    }) && !emitted.iter().any(|envelope| {
        envelope.from == from
            && envelope.to == to
            && matches!(
                &envelope.message,
                Message::AppendEntries(request) if !request.entries.is_empty()
            )
    })
}

fn lease_fast_path_granted(
    before: &Cluster,
    after: &Cluster,
    operation: &Operation,
    registration: Option<&ReadRegistered>,
) -> bool {
    let Operation::ReadIndex { to, request_id } = operation else {
        return false;
    };
    let Some(registration) = registration else {
        return false;
    };
    if registration.node_id != *to
        || registration.request_id != *request_id
        || !before.read_lease_active(*to)
        || before.network != after.network
    {
        return false;
    }
    let Some(new_grants) = after.read_grants().get(before.read_grants().len()..) else {
        return false;
    };
    new_grants.iter().any(|grant| {
        grant.node_id == *to
            && grant.request_id == *request_id
            && grant.operation_id == Some(registration.operation_id)
    })
}

fn joint_configuration_identity(
    cluster: &Cluster,
    node_id: NodeId,
) -> Option<JointConfigurationIdentity> {
    let membership = cluster.committed_membership(node_id);
    if !matches!(membership, MembershipConfig::Joint(_)) {
        return None;
    }
    Some(JointConfigurationIdentity {
        committed: cluster.committed_configuration_state(node_id)?,
        membership,
    })
}

fn installed_joint_configuration_identity(
    cluster: &Cluster,
    node_id: NodeId,
    installed_membership: Option<&MembershipConfig>,
) -> Option<JointConfigurationIdentity> {
    let identity = joint_configuration_identity(cluster, node_id)?;
    if installed_membership != Some(&identity.membership) {
        return None;
    }
    let bootstrap = cluster.bootstrap_state(node_id);
    let snapshot = bootstrap.snapshot.as_ref()?;
    if snapshot.metadata.committed_membership() != Some(&identity.membership)
        || snapshot.metadata.committed_configuration_state() != Some(identity.committed)
    {
        return None;
    }
    Some(identity)
}

fn push_unique(values: &mut Vec<JointConfigurationIdentity>, identity: JointConfigurationIdentity) {
    if !values.contains(&identity) {
        values.push(identity);
    }
}
