//! The ledger of answers this node owes, and the deadline that makes paying
//! them total.
//!
//! A record is created when this node accepts a client request — from the
//! client itself, or relayed by a peer — and destroyed when an answer for that
//! request leaves the node. Those are the only two events that touch it: the
//! map is private to this module, [`OwedAnswers::accept`] is the only way in,
//! and [`OwedAnswers::retire`] is the only way out. Both facts are rustc's to
//! keep rather than a reader's to verify, which is the point of the module
//! boundary.
//!
//! # What the ledger is total over
//!
//! Over the client requests this process acts on, not over its own contents.
//! That distinction is the whole of the fourth defect found here. The previous
//! round proved *record outstanding implies answer outstanding* — true, and
//! checkable from the two functions above — and then wrote a sentence assuming
//! its converse, that a request acted on always has a record. It did not: a
//! read the leader served itself created a waiter and no record at all, so
//! whether the deadline covered a client request depended on which node the
//! client happened to reach.
//!
//! [`Accepted`] is what closes that. It is minted only by
//! [`OwedAnswers::accept`], its fields are unreachable outside this module, and
//! every function in `client` that acts on a client request takes one. So
//! *acted on implies recorded* is rustc's to keep too, in the direction the
//! sweep needs, rather than a list of accepting paths a reader has to certify.
//!
//! # Why every record carries a deadline
//!
//! Because the fast paths cannot be trusted to be exhaustive, and the harness
//! has three times been wrong about which they were. An apply on this node pays
//! a record; a `client_result` relayed back from the leader pays one; a
//! proposal the kernel refuses pays one; a granted barrier pays one. None of
//! that is a proof that some path always fires. A request can also be stranded
//! by an entry truncated under the next leader, by an applied index a snapshot
//! install jumped past, by a leader that answered a process which no longer
//! exists, by a barrier that neither grants nor cancels, or by a partition that
//! outlives the client. Enumerating those was the error each time — the list
//! was checked in the direction that was easy to check, and relied on in the
//! direction that mattered.
//!
//! The deadline needs no list. It fires on whatever is still owed, whatever the
//! reason it is still owed, so the obligation is discharged by construction and
//! the fast paths are left to do only what they are good at: answering sooner,
//! and saying something more useful than "I do not know".

use std::collections::BTreeMap;

/// One client request, named the way both this node and the peer that relayed
/// it can name it: the client, and the message id it is waiting on.
pub(crate) type RequestKey = (String, u64);

/// One client request this node has accepted, carried as the proof that the
/// ledger holds a record for it.
///
/// Minted in exactly one place — [`OwedAnswers::accept`] — out of fields no
/// other module can name or fill. A value of this type in hand is therefore the
/// same fact as a record in the ledger, and rustc keeps it that way.
///
/// This exists to be *required*. Every function in `client` that acts on a
/// client request — relays it, proposes it, or opens a read barrier for it —
/// takes one of these, so a fourth kind of request cannot be acted on without
/// first entering the ledger. That makes the set of accepting paths a thing the
/// compiler checks rather than a list a header asserts.
///
/// Read in one direction only: *this request is recorded*. It says nothing
/// about the record still being there — [`OwedAnswers::retire`] may already
/// have run, and after an answer goes out it has. A token is a receipt for the
/// accept, never a licence to assume the record survives.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Accepted {
    /// The node the answer for this request is addressed to: whoever the ledger
    /// will actually pay, which for a repeated accept is the recipient the
    /// first one named rather than this one's.
    answer_to: String,
    client: String,
    in_reply_to: u64,
}

impl Accepted {
    /// The node this request's answer is addressed to.
    pub(crate) fn answer_to(&self) -> &str {
        &self.answer_to
    }

    /// The client waiting on this request.
    pub(crate) fn client(&self) -> &str {
        &self.client
    }

    /// The message id the client is waiting on.
    pub(crate) fn in_reply_to(&self) -> u64 {
        self.in_reply_to
    }
}

/// One answer this node accepted responsibility for and has not yet sent.
#[derive(Clone, Debug, Eq, PartialEq)]
struct OwedAnswer {
    /// The node this answer is addressed to: the peer that relayed the request
    /// here, or this node's own name when the client reached it directly.
    ///
    /// Written down when the request is accepted, and never recovered from the
    /// committed payload afterwards. Every replica applies the entry with the
    /// same `origin` in it, so the payload can say that *some* node accepted
    /// *some* copy of the request and never that this node accepted this one.
    answer_to: String,
    /// The tick by which this node answers whether or not it has learned what
    /// became of the request.
    deadline: u64,
}

/// Every client request this node has accepted and not yet answered.
#[derive(Debug, Default)]
pub(crate) struct OwedAnswers {
    owed: BTreeMap<RequestKey, OwedAnswer>,
}

impl OwedAnswers {
    /// Records that this node owes `answer_to` an answer for `key`, and will
    /// send one by `deadline` whatever becomes of the entry behind it.
    ///
    /// The only way a record comes into being, and the only way an [`Accepted`]
    /// does. A second accept for a key already held keeps the first: the copy
    /// this node acted on is the one whose deadline governs, and a repeat must
    /// not be able to push that deadline out — which is what would let a stream
    /// of duplicates hold one client waiting indefinitely.
    ///
    /// The token names the recipient the ledger *kept*, not the one this call
    /// offered, so a caller acting on the token cannot address its answer to a
    /// node the sweep would not have paid.
    pub(crate) fn accept(&mut self, key: RequestKey, answer_to: String, deadline: u64) -> Accepted {
        let held = self.owed.entry(key.clone()).or_insert(OwedAnswer {
            answer_to,
            deadline,
        });
        Accepted {
            answer_to: held.answer_to.clone(),
            client: key.0,
            in_reply_to: key.1,
        }
    }

    /// The node this request's answer is addressed to, if this node owes one.
    pub(crate) fn answer_to(&self, key: &RequestKey) -> Option<&str> {
        self.owed.get(key).map(|owed| owed.answer_to.as_str())
    }

    /// Whether an answer for `key` is still owed.
    pub(crate) fn is_owed(&self, key: &RequestKey) -> bool {
        self.owed.contains_key(key)
    }

    /// Discards the record for `key`. The only way a record ceases to exist.
    pub(crate) fn retire(&mut self, key: &RequestKey) {
        self.owed.remove(key);
    }

    /// Every request whose deadline `now` has reached, each with the node its
    /// answer is addressed to.
    pub(crate) fn due(&self, now: u64) -> Vec<(RequestKey, String)> {
        self.owed
            .iter()
            .filter(|(_, owed)| now >= owed.deadline)
            .map(|(key, owed)| (key.clone(), owed.answer_to.clone()))
            .collect()
    }

    /// Whether this node owes nothing.
    ///
    /// Test-only, and deliberately so: production code never asks. The ledger
    /// is swept by deadline, not by emptiness, and a caller that branched on
    /// "nothing outstanding" would be reintroducing exactly the reasoning the
    /// deadline exists to replace. Tests ask because "no record survived" is
    /// half of what they are pinning.
    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.owed.is_empty()
    }
}

#[cfg(test)]
mod tests {
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
}
