//! The ledger of answers this node owes, and the deadline that makes paying
//! them total.
//!
//! A record is created when this node accepts a client request — from the
//! client itself, or relayed by a peer — and destroyed when an answer for that
//! request leaves the node. Those are the only two events that decide whether a
//! record exists: the map is private to this module,
//! [`OwedAnswers::accept`] is the only way in, and [`OwedAnswers::retire`] is
//! the only way out. Both facts are rustc's to keep rather than a reader's to
//! verify, which is the point of the module boundary.
//!
//! One further event *amends* a record without creating or destroying one.
//! [`OwedAnswers::relayed`] writes down which peer this node handed the request
//! to, once it has handed it to one; it is a no-op on a key the map does not
//! hold, so it cannot bring a record into being by the back door. The
//! distinction is stated because the sentence above is load bearing and the
//! previous revision of it said "the only two events that touch it", which this
//! would have made false.
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
//! [`Accepted`] is what closes that. Its fields are unreachable outside this
//! module, it is minted only here, and every function in `client` that acts on
//! a client request takes one. So *acted on implies recorded* is rustc's to
//! keep too, in the direction the sweep needs, rather than a list of accepting
//! paths a reader has to certify.
//!
//! There are two mints, and the difference between them is the whole of the
//! fifth defect. [`OwedAnswers::accept`] creates the record; [`OwedAnswers::recorded`]
//! finds one and creates nothing. A caller acting on a request that *arrived*
//! takes responsibility for it and uses the first. A caller reacting to another
//! node's message about a request key — a leader's `client_result` — must be
//! able to pay an obligation this node already holds and unable to invent one,
//! and uses the second. One mint would have forced that caller to accept, which
//! is "acted on implies recorded" made true by the act of acting.

use std::collections::BTreeMap;

mod ledger;

/// One client request, named the way both this node and the peer that relayed
/// it can name it: the client, and the message id it is waiting on.
pub(crate) type RequestKey = (String, u64);

/// One client request this node has accepted, carried as the proof that the
/// ledger holds a record for it.
///
/// Minted only in this module, out of fields no other module can name or fill,
/// by [`OwedAnswers::accept`] and [`OwedAnswers::recorded`]. A value of this
/// type in hand is therefore the same fact as a record in the ledger — one this
/// caller lodged, or one it found — and rustc keeps it that way.
///
/// This exists to be *required*. Every function in `client` that acts on a
/// client request — relays it, proposes it, opens a read barrier for it, or
/// pays it out of a leader's answer — takes one of these, so a fourth kind of
/// request cannot be acted on without a record existing first. That makes the
/// set of acting paths a thing the compiler checks rather than a list a header
/// asserts, which is what the header had and what was wrong with it.
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
    /// The peer this node handed the request to, once it has handed it to one.
    ///
    /// The other direction of travel, and the only party whose `client_result`
    /// for this request means anything: it is the node that took the request,
    /// so it is the node that can say what became of it. `None` is a request
    /// this node is serving itself — it proposed it, or opened a barrier for it
    /// — and for those there is nobody who could report, so nobody may.
    ///
    /// Kept here rather than compared against `known_leader` at the time a
    /// result arrives, because `known_leader` is a memory that moves. The
    /// question is who this request went to, which is a fact about the past.
    relayed_to: Option<String>,
    /// The tick by which this node answers whether or not it has learned what
    /// became of the request.
    deadline: u64,
}

/// Every client request this node has accepted and not yet answered.
#[derive(Debug, Default)]
pub(crate) struct OwedAnswers {
    owed: BTreeMap<RequestKey, OwedAnswer>,
}

#[cfg(test)]
#[path = "answers_test.rs"]
mod tests;
