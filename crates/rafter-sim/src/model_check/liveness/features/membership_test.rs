//! Exact round bounds for the LV-03 membership-transition detector.
//!
//! A completion landing on the final driven round counts as inside the bound,
//! and an explicit configuration rejection matches only once its floor is
//! passed; an off-by-one either fails a live run or credits a stale rejection.

use rafter::{LocalProposalId, NodeId};

use super::{membership_rejection_observed, operation_rounds};
use crate::records::ProposalRejected;

#[test]
fn completion_after_final_driven_round_is_within_the_exact_bound() {
    let budget = 8;
    assert_eq!(operation_rounds(budget - 1, true), budget);
    assert_eq!(operation_rounds(budget - 1, false), budget - 1);
}

#[test]
fn explicit_configuration_rejection_matches_only_after_its_floor() {
    let rejections = [
        ProposalRejected {
            node_id: NodeId(1),
            proposal_id: Some(LocalProposalId(7)),
        },
        ProposalRejected {
            node_id: NodeId(1),
            proposal_id: None,
        },
    ];

    assert!(membership_rejection_observed(&rejections, 1, NodeId(1)));
    assert!(!membership_rejection_observed(&rejections, 2, NodeId(1)));
    assert!(!membership_rejection_observed(&rejections, 0, NodeId(2)));
}
