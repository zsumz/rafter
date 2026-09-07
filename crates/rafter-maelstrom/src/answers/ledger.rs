//! The ways in and out of the ledger: the accept that creates a record, the
//! amendment that says where a request was handed, and the sweep that names
//! every record whose deadline has arrived.
//!
//! The map these run on is the parent module's private field, so this file is
//! the whole of what can create, amend or destroy a record.
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

use super::{Accepted, OwedAnswer, OwedAnswers, RequestKey};

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
            relayed_to: None,
            deadline,
        });
        Accepted {
            answer_to: held.answer_to.clone(),
            client: key.0,
            in_reply_to: key.1,
        }
    }

    /// Records that this node handed `key` to `peer`, which makes `peer` the
    /// one party whose `client_result` for it may be acted on.
    ///
    /// Written where the relay happens rather than passed to the accept,
    /// because the accept runs above the decision: the funnel records the
    /// obligation before it knows whether this node will serve the request or
    /// hand it on. A record with nothing here is a request this node kept.
    ///
    /// Silently does nothing when no record is held. That is the retired case
    /// — the request was answered between the accept and the relay — and the
    /// consequence is exactly right: a `client_result` for it finds no record
    /// either.
    pub(crate) fn relayed(&mut self, key: &RequestKey, peer: String) {
        if let Some(owed) = self.owed.get_mut(key) {
            owed.relayed_to = Some(peer);
        }
    }

    /// Whether `peer` is the node this request was handed to.
    ///
    /// False for a request this node never relayed, and false for a peer that
    /// is not the one it went to. Read in one direction: it licenses acting on
    /// a `client_result`, never declining to answer.
    pub(crate) fn was_relayed_to(&self, key: &RequestKey, peer: &str) -> bool {
        self.owed
            .get(key)
            .and_then(|owed| owed.relayed_to.as_deref())
            .is_some_and(|relayed_to| relayed_to == peer)
    }

    /// The node this request's answer is addressed to, if this node owes one.
    pub(crate) fn answer_to(&self, key: &RequestKey) -> Option<&str> {
        self.owed.get(key).map(|owed| owed.answer_to.as_str())
    }

    /// The token for a request this node already holds a record for.
    ///
    /// The second way to obtain an [`Accepted`], and the only one that cannot
    /// create an obligation: it reads the map and never writes it, so `None`
    /// means this node never accepted this request or has already answered it.
    ///
    /// It exists for callers reacting to somebody *else's* message about a
    /// request key — a leader's `client_result` for a request this node
    /// relayed. Such a caller must act only on a request this node already took
    /// responsibility for, and must not be able to manufacture responsibility
    /// out of the message it just received. [`Self::accept`] would do exactly
    /// that: it inserts, so a caller holding a key and no record would mint a
    /// token for one and the funnel's "acted on implies recorded" would become
    /// "acted on implies recorded, by the act of acting".
    pub(crate) fn recorded(&self, key: &RequestKey) -> Option<Accepted> {
        self.owed.get(key).map(|owed| Accepted {
            answer_to: owed.answer_to.clone(),
            client: key.0.clone(),
            in_reply_to: key.1,
        })
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
