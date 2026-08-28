//! When an owed answer falls due, and what a repeated accept may not change.
//!
//! A record comes due on exactly the tick its deadline names, a repeat may not
//! push that deadline out or rebind who gets paid, and retiring a record leaves
//! nothing owed — otherwise the sweep pays the wrong node, twice, or never.

use super::OwedAnswers;

fn key(client: &str, in_reply_to: u64) -> (String, u64) {
    (client.to_owned(), in_reply_to)
}

/// A record is due on the tick its deadline names, not the one after.
///
/// The sweep compares against a tick counter that only ever increments by
/// one, so an off-by-one here is not a rounding difference — it is a record
/// that is skipped on the tick it was meant to fire and caught on the next,
/// or never, if the comparison is the other way round.
#[test]
fn a_record_falls_due_on_the_tick_its_deadline_names() {
    let mut owed = OwedAnswers::default();
    owed.accept(key("c1", 5), "n2".to_owned(), 7);

    assert!(owed.due(6).is_empty(), "not yet due one tick early");
    assert_eq!(
        owed.due(7),
        vec![(key("c1", 5), "n2".to_owned())],
        "due on the deadline tick itself"
    );
    assert_eq!(owed.due(8).len(), 1, "and stays due until it is retired");
}

/// A repeat of an accepted request does not move its deadline.
///
/// Otherwise a request that keeps arriving is a request that is never
/// answered: each copy pushes the deadline past the tick that would have
/// fired, and the backstop the whole construction rests on never fires.
#[test]
fn a_repeated_accept_does_not_push_the_deadline_out() {
    let mut owed = OwedAnswers::default();
    owed.accept(key("c1", 5), "n2".to_owned(), 7);
    owed.accept(key("c1", 5), "n3".to_owned(), 99);

    assert_eq!(
        owed.due(7),
        vec![(key("c1", 5), "n2".to_owned())],
        "the first accept's deadline and recipient both stand"
    );
}

/// The token an accept hands back names the recipient the ledger kept.
///
/// The token is what its holder addresses the answer to, and a repeat that
/// offers a different recipient must not be able to aim it. Handing back
/// the offered `answer_to` rather than the held one would mail this
/// request's answer to `n3` while the sweep, firing on the same record,
/// mails it to `n2` — one request, two recipients, from one ledger.
#[test]
fn a_repeated_accept_hands_back_the_recipient_the_ledger_kept() {
    let mut owed = OwedAnswers::default();
    let first = owed.accept(key("c1", 5), "n2".to_owned(), 7);
    let repeat = owed.accept(key("c1", 5), "n3".to_owned(), 99);

    assert_eq!(first.answer_to(), "n2");
    assert_eq!(
        repeat.answer_to(),
        "n2",
        "the repeat's token names whoever the sweep would pay, not the \
         recipient this accept offered"
    );
    assert_eq!(repeat.client(), "c1");
    assert_eq!(repeat.in_reply_to(), 5);
}

/// Retiring a record is what makes it stop falling due.
#[test]
fn a_retired_record_is_owed_no_longer() {
    let mut owed = OwedAnswers::default();
    owed.accept(key("c1", 5), "n2".to_owned(), 7);
    assert_eq!(owed.answer_to(&key("c1", 5)), Some("n2"));

    owed.retire(&key("c1", 5));

    assert_eq!(owed.answer_to(&key("c1", 5)), None);
    assert!(owed.is_empty());
    assert!(owed.due(u64::MAX).is_empty());
}
