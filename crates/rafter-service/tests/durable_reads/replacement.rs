//! An old log index must never correlate a replacement command to its caller.
use super::support::*;
use rafter::{LocalProposalId, Term};
use rafter_app::proposal::ClientRequestId;
use rafter_service::{UnknownOutcomeReason, WriteError};

mod support;
use support::Partition;

#[test]
fn replaced_log_preserves_unknown_write_identity_and_retained_credit() {
    replacement(false);
}

#[test]
fn snapshot_catchup_preserves_unknown_write_identity_and_retained_credit() {
    replacement(true);
}

fn replacement(snapshot: bool) {
    let mut cluster = Partition::new();
    let writes = cluster.cluster.nodes[0].owner.sender();
    let ids = [7, 8].map(|sequence| ClientRequestId {
        client_id: 42,
        sequence,
    });
    let mut pending = ids.map(|id| {
        writes.write(
            vec![u8::try_from(id.sequence).unwrap()],
            WriteOptions::default().with_client_request_id(id),
        )
    });
    cluster.until(|c| c.appended.len() == 2);
    assert_eq!(
        cluster.appended,
        [
            (LocalProposalId(1), LogIndex(2)),
            (LocalProposalId(2), LogIndex(3))
        ]
    );
    assert!(pending.iter_mut().all(|write| poll(write).is_pending()));
    assert_eq!(writes.retained().writes, 2);
    assert_eq!(
        cluster.cluster.nodes[0]
            .owner
            .ready_node()
            .unwrap()
            .commit_index(),
        LogIndex(1)
    );

    cluster.elect_replacement();
    let mut replacement = cluster.cluster.nodes[1]
        .owner
        .sender()
        .write(vec![4], WriteOptions::default());
    let mut result = None;
    cluster.until(|_| match poll(&mut replacement) {
        Poll::Ready(value) => {
            result = Some(value.unwrap());
            true
        }
        Poll::Pending => false,
    });
    let receipt = result.unwrap();
    // The new leader's no-op occupies index 2. Its command now occupies the
    // second old caller's index, with a different term and application result.
    assert_eq!(receipt.index, LogIndex(3));
    assert_eq!(receipt.result.value, 4);
    assert!(receipt.term > Term(1));
    cluster.until(|c| c.cluster.nodes[1].owner.state() == ServiceState::Ready);
    if snapshot {
        cluster.compact();
    }
    cluster
        .cluster
        .network
        .retain(|(from, to, _)| *from != NodeId(1) && *to != NodeId(1));
    cluster.partitioned = false;
    cluster.tick(1);
    cluster.until(|c| c.cluster.nodes[0].owner.durable_through() == Some(LogIndex(3)));
    cluster.until(|c| c.cluster.nodes[0].owner.state() == ServiceState::Ready);
    assert_eq!(cluster.installs, usize::from(snapshot));
    assert_eq!(
        writes.retained().writes,
        2,
        "uncollected results retain credit"
    );
    for (i, (future, id)) in pending.iter_mut().zip(ids).enumerate() {
        let Poll::Ready(Err(error)) = poll(future) else {
            panic!("replaced proposal must have an unknown outcome");
        };
        assert!(error.fate().may_commit());
        assert!(matches!(error, WriteError::UnknownOutcome {
            local_proposal_id,
            client_request_id: Some(client),
            reason: UnknownOutcomeReason::RuntimeDroppedProposal,
        } if local_proposal_id == cluster.appended[i].0 && client == id));
        assert_eq!(writes.retained().writes, 1 - i);
    }
    // Query the actual replaced state, and show the old owner still admits
    // work. A follower refusal must not resurrect an old proposal correlation.
    let mut read = cluster.cluster.nodes[0].read(ReadConsistency::Local);
    cluster.until(|_| match poll(&mut read) {
        Poll::Ready(value) => {
            assert_eq!(value.unwrap().result, 4);
            true
        }
        Poll::Pending => false,
    });
    let mut refused = writes.write(vec![6], WriteOptions::default());
    cluster.until(|_| match poll(&mut refused) {
        Poll::Ready(value) => {
            assert!(!value.unwrap_err().fate().may_commit());
            true
        }
        Poll::Pending => false,
    });
    assert_eq!(writes.retained().writes, 0);
    assert_eq!(writes.retained().payload_bytes, 0);
}
