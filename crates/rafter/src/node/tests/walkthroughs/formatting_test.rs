//! Pre-vote grants echo the proposed term; rejections carry the responder's term.

use super::{formatting, support::*};

#[test]
fn pre_vote_rejection_names_the_responder_term() {
    let mut node = voter_in_term_seven();
    let outputs = node.step(message(
        2,
        Message::PreVote(PreVote {
            term: Term(8),
            candidate_id: NodeId(2),
            last_log_index: LogIndex::ZERO,
            last_log_term: Term::default(),
        }),
    ));

    assert_eq!(node.current_term(), Term(7));
    assert_eq!(
        formatting::outputs(&outputs),
        vec!["Send to node 2: pre-vote rejected; responder term 7."]
    );
}

#[test]
fn pre_vote_grant_names_the_prospective_term() {
    let mut node = voter_in_term_seven();
    let outputs = node.step(message(
        2,
        Message::PreVote(PreVote {
            term: Term(8),
            candidate_id: NodeId(2),
            last_log_index: LogIndex(1),
            last_log_term: Term(7),
        }),
    ));

    assert_eq!(node.current_term(), Term(7));
    assert_eq!(
        formatting::outputs(&outputs),
        vec!["Send to node 2: pre-vote granted for prospective term 8."]
    );
}

fn voter_in_term_seven() -> Node {
    bootstrap(
        7,
        0,
        vec![BootstrapLogEntry::application(
            LogIndex(1),
            Term(7),
            b"local".to_vec(),
        )],
    )
}
