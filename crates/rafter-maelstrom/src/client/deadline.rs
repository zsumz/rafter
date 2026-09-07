//! The record's two ends: the accept that lodges one, and the sweep that pays
//! every record whose deadline has passed.
//!
//! The sweep is deliberately not a list of the ways an answer can fail to
//! arrive. It fires on whatever is still owed, so it cannot be wrong about a
//! list it does not consult.

use crate::{
    answers::{Accepted, RequestKey},
    app::{ClientResult, ERROR_TIMEOUT},
    InitializedNode,
};

impl InitializedNode {
    /// Records that this node owes `origin` an answer for one request it has
    /// accepted, and the tick by which that answer goes out regardless.
    ///
    /// The only wrapper over the ledger's `accept`, which is in turn the only
    /// way a record comes into being and the only way an [`Accepted`] does. It
    /// has exactly one production caller — [`Self::handle_client_request`],
    /// the funnel — so "which paths accept a request?" is a question with one
    /// answer that `grep` settles, rather than a list to keep in step with the
    /// dispatch below it. The token it hands back is what the dispatch needs to
    /// do anything at all.
    pub(crate) fn accept_answer_obligation(
        &mut self,
        origin: &str,
        client: &str,
        in_reply_to: u64,
    ) -> Accepted {
        self.owed_answers.accept(
            (client.to_owned(), in_reply_to),
            origin.to_owned(),
            self.ticks + self.answer_deadline_ticks,
        )
    }

    /// Answers every request whose deadline has passed, and retires it.
    ///
    /// This is what makes the obligation total, and it is deliberately not a
    /// list of the ways an answer can fail to arrive. The fast paths — an apply
    /// here, a `client_result` relayed back, a proposal the kernel refused, a
    /// barrier the kernel granted or cancelled — answer sooner and say
    /// something more useful, but nothing rests on their being exhaustive. That
    /// reasoning is what failed each time it was tried: an entry truncated
    /// under the next leader, an applied index a snapshot install jumped past,
    /// a leader that answered a process which has since restarted, a read
    /// barrier that neither granted nor cancelled. This sweep fires on whatever
    /// is still owed without asking which of those happened, or whether the
    /// list is complete.
    ///
    /// It is total over the ledger, and the ledger is total over the requests
    /// this node acted on — see [`Self::handle_client_request`] for the second
    /// half, which is the one the previous round asserted rather than built.
    ///
    /// The error is [`ERROR_TIMEOUT`], the one indefinite code this harness
    /// sends, because indefinite is the honest reading: the request may well
    /// have committed and this node simply cannot say. Every other code asserts
    /// that it did not. The sweep does not soften that for a read, whose
    /// outcome it could in principle describe more precisely — branching on the
    /// kind of request is how a sweep grows back into a list.
    pub(crate) fn expire_owed_answers(&mut self) {
        for (key, answer_to) in self.owed_answers.due(self.ticks) {
            self.deliver_result(
                &answer_to,
                &key.0,
                key.1,
                ClientResult::Error {
                    code: ERROR_TIMEOUT,
                    text: "no committed outcome for this request before its deadline".to_owned(),
                },
            );
            self.discard_waiter(&key);
        }
        debug_assert!(
            self.owed_answers.due(self.ticks).is_empty(),
            "a sweep must leave nothing due: `deliver_result` is what retires a \
             record, so a survivor here means an answer went out without one"
        );
    }

    /// Drops the read waiter for one request whose answer has just gone out.
    ///
    /// Not a second retirement of the obligation — `deliver_result` did that,
    /// above, and it is still the only place a record dies. A waiter is a note
    /// that a barrier has not resolved, and once the answer has been sent the
    /// note has nothing left to say: a grant arriving afterwards would find it,
    /// deliver into `deliver_result`'s dedupe and retire it having sent
    /// nothing. Dropping it keeps `pending_reads` bounded by the reads still
    /// genuinely waiting rather than by every read this process ever swept.
    fn discard_waiter(&mut self, key: &RequestKey) {
        self.pending_reads
            .retain(|_, read| read.client != key.0 || read.in_reply_to != key.1);
    }
}
