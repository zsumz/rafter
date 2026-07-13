use rafter::{Message, NodeId, RaftSnapshot};

use crate::{Cluster, Envelope};

pub(super) enum SnapshotTransferIdentityCheck {
    Verified,
    CoverageUnavailable(String),
    Violation(String),
}

pub(super) fn snapshot_transfer_identity_check(
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
        return SnapshotTransferIdentityCheck::CoverageUnavailable(format!(
            "{node_id} installed delivered snapshot transfer {} from {} without sender payload bytes available for identity checking",
            expected.transfer_id(),
            envelope.from
        ));
    };
    if installed_payload != Some(expected_payload) {
        return SnapshotTransferIdentityCheck::Violation(format!(
            "{node_id} installed bytes that differ from delivered transfer {}",
            expected.transfer_id()
        ));
    }
    SnapshotTransferIdentityCheck::Verified
}
