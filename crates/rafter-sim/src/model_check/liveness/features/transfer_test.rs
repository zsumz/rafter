//! Which rejection the LV-03 transfer detector is allowed to match.
//!
//! An explicit rejection terminates only the exact request it names and only
//! once that request's floor is passed, so a rejection belonging to an earlier
//! transfer can never be credited as this one's terminal outcome.

use rafter::NodeId;

use super::transfer_rejection_observed;
use crate::records::TransferRejected;

#[test]
fn explicit_transfer_rejection_matches_only_the_exact_request_after_its_floor() {
    let rejections = [TransferRejected {
        node_id: NodeId(1),
        target: NodeId(2),
    }];

    assert!(transfer_rejection_observed(
        &rejections,
        0,
        NodeId(1),
        NodeId(2)
    ));
    assert!(!transfer_rejection_observed(
        &rejections,
        1,
        NodeId(1),
        NodeId(2)
    ));
    assert!(!transfer_rejection_observed(
        &rejections,
        0,
        NodeId(1),
        NodeId(3)
    ));
    assert!(!transfer_rejection_observed(
        &rejections,
        0,
        NodeId(2),
        NodeId(2)
    ));
}
