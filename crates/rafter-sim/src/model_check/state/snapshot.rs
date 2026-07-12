use std::collections::{BTreeMap, BTreeSet};

use rafter::{LogIndex, Message, NodeId, RaftSnapshot};

use crate::{Cluster, Envelope};

use super::super::observations::{Observation, ObservationSet};

#[derive(Clone, Debug, Default, Hash)]
pub(crate) struct SnapshotHistory {
    boundary_floor_by_node: BTreeMap<NodeId, LogIndex>,
    violations: BTreeSet<SnapshotHistoryViolation>,
    payload_binding_violations: BTreeSet<String>,
    transfer_identity_violations: BTreeSet<String>,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct SnapshotHistoryViolation {
    pub(crate) node_id: NodeId,
    pub(crate) previous_boundary: LogIndex,
    pub(crate) current_boundary: LogIndex,
}

impl SnapshotHistory {
    pub(super) fn from_cluster(cluster: &Cluster) -> Self {
        Self {
            boundary_floor_by_node: cluster
                .nodes
                .iter()
                .map(|(node_id, node)| (*node_id, node.snapshot_index()))
                .collect(),
            violations: BTreeSet::new(),
            payload_binding_violations: BTreeSet::new(),
            transfer_identity_violations: BTreeSet::new(),
        }
    }

    pub(super) fn observe_cluster(&mut self, cluster: &Cluster) -> ObservationSet {
        for (node_id, node) in &cluster.nodes {
            let current = node.snapshot_index();
            let floor = self.boundary_floor_by_node.entry(*node_id).or_default();
            if current < *floor {
                self.violations.insert(SnapshotHistoryViolation {
                    node_id: *node_id,
                    previous_boundary: *floor,
                    current_boundary: current,
                });
            } else {
                *floor = current;
            }
        }
        ObservationSet::default()
    }

    pub(super) fn record_transition(
        &mut self,
        before: &Cluster,
        after: &Cluster,
        delivered: Option<&Envelope>,
    ) -> ObservationSet {
        let mut observations = ObservationSet::default();
        for (node_id, node) in &after.nodes {
            let Some(previous) = before.nodes.get(node_id) else {
                continue;
            };
            if node.snapshot_index() <= previous.snapshot_index() {
                continue;
            }
            observations.mark(Observation::SnapshotBoundaryAdvances);

            match snapshot_payload_binding_issue(after, *node_id) {
                None => observations.mark(Observation::SnapshotPayloadBindingsChecked),
                Some(issue) => {
                    self.payload_binding_violations.insert(issue);
                }
            }
            match snapshot_transfer_identity_check(before, after, *node_id, delivered) {
                SnapshotTransferIdentityCheck::Verified => {
                    observations.mark(Observation::SnapshotTransferIdentitiesChecked);
                }
                SnapshotTransferIdentityCheck::CoverageUnavailable => {}
                SnapshotTransferIdentityCheck::Violation(issue) => {
                    self.transfer_identity_violations.insert(issue);
                }
            }
        }
        observations
    }

    pub(crate) const fn payload_binding_violations(&self) -> &BTreeSet<String> {
        &self.payload_binding_violations
    }

    pub(crate) const fn transfer_identity_violations(&self) -> &BTreeSet<String> {
        &self.transfer_identity_violations
    }

    pub(crate) const fn violations(&self) -> &BTreeSet<SnapshotHistoryViolation> {
        &self.violations
    }
}

pub(crate) fn snapshot_payload_binding_issue(cluster: &Cluster, node_id: NodeId) -> Option<String> {
    let snapshot = cluster.node(node_id).snapshot()?;
    let Some(payload) = cluster.snapshot_payload(node_id, snapshot) else {
        return Some(format!(
            "{node_id} published snapshot transfer {} without payload bytes",
            snapshot.transfer_id()
        ));
    };
    if RaftSnapshot::from_payload(snapshot.metadata.clone(), payload) != *snapshot {
        return Some(format!(
            "{node_id} snapshot transfer {} does not bind its metadata to the visible payload bytes",
            snapshot.transfer_id()
        ));
    }
    None
}

enum SnapshotTransferIdentityCheck {
    Verified,
    CoverageUnavailable,
    Violation(String),
}

fn snapshot_transfer_identity_check(
    before: &Cluster,
    after: &Cluster,
    node_id: NodeId,
    delivered: Option<&Envelope>,
) -> SnapshotTransferIdentityCheck {
    let envelope = delivered.filter(|envelope| envelope.to == node_id);
    let Some(envelope) = envelope else {
        return SnapshotTransferIdentityCheck::Violation(format!(
            "{node_id} advanced its snapshot boundary without an install-snapshot delivery"
        ));
    };
    let Some(installed) = after.node(node_id).snapshot() else {
        return SnapshotTransferIdentityCheck::Violation(format!(
            "{node_id} advanced its snapshot boundary without a visible descriptor"
        ));
    };
    let installed_payload = after.snapshot_payload(node_id, installed);

    let (expected, expected_payload) = match &envelope.message {
        Message::InstallSnapshot(request) => (
            RaftSnapshot::from_payload(
                request.metadata.clone(),
                request.application_payload.as_slice(),
            ),
            Some(request.application_payload.as_slice()),
        ),
        Message::InstallSnapshotChunk(request) if request.done => {
            let expected = RaftSnapshot::new(
                request.metadata.clone(),
                request.total_payload_len,
                request.application_payload_crc32,
            );
            if request.transfer_id != expected.transfer_id() {
                return SnapshotTransferIdentityCheck::Violation(format!(
                    "{node_id} installed chunk transfer {} whose descriptor identity is {}",
                    request.transfer_id,
                    expected.transfer_id()
                ));
            }
            let expected_payload = before.snapshot_payload(envelope.from, &expected);
            (expected, expected_payload)
        }
        Message::InstallSnapshotChunk(_) => {
            return SnapshotTransferIdentityCheck::Violation(format!(
                "{node_id} advanced its snapshot boundary on a non-final chunk"
            ));
        }
        Message::AppendEntries(_)
        | Message::AppendEntriesResponse(_)
        | Message::InstallSnapshotResponse(_)
        | Message::PreVote(_)
        | Message::PreVoteResponse(_)
        | Message::RequestVote(_)
        | Message::RequestVoteResponse(_)
        | Message::TimeoutNow(_) => {
            return SnapshotTransferIdentityCheck::Violation(format!(
                "{node_id} advanced its snapshot boundary on a non-snapshot message"
            ));
        }
    };

    if installed != &expected {
        return SnapshotTransferIdentityCheck::Violation(format!(
            "{node_id} installed snapshot transfer {} instead of delivered transfer {}",
            installed.transfer_id(),
            expected.transfer_id()
        ));
    }
    let Some(expected_payload) = expected_payload else {
        return SnapshotTransferIdentityCheck::CoverageUnavailable;
    };
    if installed_payload != Some(expected_payload) {
        return SnapshotTransferIdentityCheck::Violation(format!(
            "{node_id} installed bytes that differ from delivered transfer {}",
            expected.transfer_id()
        ));
    }
    SnapshotTransferIdentityCheck::Verified
}
