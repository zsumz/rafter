//! Display, ordering, and sentinel behavior for Raft domain identities.

use super::*;

#[test]
fn raft_domain_values_have_stable_display_and_debug_output() {
    assert_eq!(NodeId(7).to_string(), "node-7");
    assert_eq!(LocalProposalId(8).to_string(), "local-proposal-8");
    assert_eq!(ReadId(9).to_string(), "read-9");
    assert_eq!(Term(9).to_string(), "9");
    assert_eq!(LogIndex(11).to_string(), "11");
    assert_eq!(format!("{:?}", NodeId(7)), "NodeId(7)");
    assert_eq!(format!("{:?}", LocalProposalId(8)), "LocalProposalId(8)");
    assert_eq!(format!("{:?}", ReadId(9)), "ReadId(9)");
    assert_eq!(format!("{:?}", Term(9)), "Term(9)");
    assert_eq!(format!("{:?}", LogIndex(11)), "LogIndex(11)");
}

#[test]
fn raft_domain_values_order_by_their_protocol_value() {
    assert_eq!(Term::default(), Term(0));
    assert!(Term::default().is_zero());
    assert!(Term(4).next() > Term(4));
    assert_eq!(LogIndex::ZERO, LogIndex(0));
    assert!(LogIndex::ZERO.next() > LogIndex::ZERO);
    assert!(NodeId(2) > NodeId(1));
    assert!(LocalProposalId(2) > LocalProposalId(1));
    assert!(ReadId(2) > ReadId(1));
}
