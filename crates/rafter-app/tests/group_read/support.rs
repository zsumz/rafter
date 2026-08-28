//! What "the step reported nothing" means, spelled out once.
//!
//! A read that answers from local state or from a proof already granted must
//! leave every stream of the step report empty — peer messages, applies, and
//! all four event streams — and carry no metrics. Written here so the scenarios
//! that make the claim name it rather than re-listing eight fields each.

use super::support::GroupStepReport;

pub(super) fn assert_empty_report(report: &GroupStepReport<u64, Vec<u8>>) {
    assert_eq!(report.group_id, 7);
    assert!(report.peer_messages.is_empty());
    assert!(report.applied.is_empty());
    assert!(report.proposal_events.is_empty());
    assert!(report.read_events.is_empty());
    assert!(report.leadership_transfer_events.is_empty());
    assert!(report.snapshot_events.is_empty());
    assert!(report.membership_events.is_empty());
    assert_eq!(report.metrics, None);
}
