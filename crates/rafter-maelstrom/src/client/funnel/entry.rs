//! The two doors a client request arrives through, and the one report a peer
//! may make about a request this node handed it.
//!
//! The first two reach the funnel, which accepts the obligation before it looks
//! at what the request is. The third pays a record this node already holds, and
//! takes a token it can only find and never mint.

use serde_json::Value;

use crate::{
    client::Reception,
    protocol::{Envelope, Peer},
    InitializedNode,
};

impl InitializedNode {
    /// Accepts a client request one peer could not serve and relayed here.
    ///
    /// The node this request's answer is addressed to is the [`Peer`] the
    /// dispatch resolved, not the `src` string on the envelope. They agree — the
    /// token is that lookup — but only one of them is a name this cluster is
    /// known to hold, and the record is what the deadline will eventually mail
    /// an answer to.
    ///
    /// Reading it off the envelope instead was the whole defect. This arm sat
    /// *above* the dispatch's catch-all, so the membership test that kept peer
    /// traffic out of the client funnel never ran on it, and the sender gate
    /// written for [`Self::handle_client_result`] one arm over did not reach it
    /// either. A `client_forward` forged by any non-node therefore lodged a
    /// record addressed to the forger: the write was executed on the named
    /// client's behalf, the answer was mailed to whoever asked, and
    /// `completed_replies` gained the key — after which [`Self::has_accepted`]
    /// refused that client's genuine request above the accept, leaving it with
    /// no record, no deadline and no answer.
    pub(crate) fn handle_forward(&mut self, peer: &Peer, envelope: Envelope) {
        let reception = Reception::FromPeer(peer.name().to_owned());
        let body = envelope.body;
        let Some(client) = body.get("client").and_then(Value::as_str) else {
            return;
        };
        let Some(in_reply_to) = body.get("in_reply_to").and_then(Value::as_u64) else {
            return;
        };
        let Some(request) = body.get("request") else {
            return;
        };
        self.handle_client_request(&reception, client, in_reply_to, request);
    }

    /// Pays a request this node relayed, out of the answer the leader sent back.
    ///
    /// A fast path, and the only one whose input is another node's claim about
    /// a client request key. So it is the one path that must not be able to act
    /// on a key it is merely told about: it takes its token from
    /// [`OwedAnswers::recorded`](crate::answers::OwedAnswers::recorded), which
    /// reads the ledger and never writes it, and does nothing at all when this
    /// node holds no record.
    ///
    /// That is not a defensive check; it is the funnel's rule applied here. It
    /// used to call [`Self::deliver_result`] directly with `origin =
    /// self.name`, which put a client-addressed answer on the wire *and* wrote
    /// `completed_replies` — the at-most-once set [`Self::has_accepted`] reads
    /// — for a key no accept had ever seen. The cost was not the stray answer,
    /// which Maelstrom discards as a stale `in_reply_to`. It was the entry in
    /// `completed_replies`: the client's genuine request for that key was then
    /// refused *above* the accept, reached no state machine, lodged no record,
    /// and no sweep recovered it.
    ///
    /// Nothing legitimate is lost. A record gone means this node already
    /// answered — by an apply, by the deadline, or by an earlier copy of this
    /// same result — and `deliver_result`'s dedupe would have suppressed the
    /// send anyway. The difference is only that the suppression now happens
    /// before the at-most-once set is written rather than after.
    ///
    /// The sender is checked as well, and separately, and the check is now the
    /// reason it was always given: **only the node this request was relayed to
    /// can report what became of it.** The record says which node that was.
    ///
    /// It used to be "any node in this cluster", which is one scope wider than
    /// its own justification — the [`Peer`] the dispatch resolves is that
    /// wider test, and it is the right test for *routing*: a `client_result` is
    /// a thing only a peer may say at all. It is not the right test here. Two
    /// things fall in the gap, and both are cases where the harness would have
    /// acted on a report nobody was in a position to make:
    ///
    /// - a request this node relayed to `n2` could be answered by `n3`, which
    ///   was never asked and cannot know; and
    /// - a request this node is serving *itself* — proposed here, or a barrier
    ///   opened here, with `relayed_to` empty — could be retired early by any
    ///   peer at all, with a result of its choosing, in place of the answer the
    ///   apply or the deadline would have paid honestly.
    ///
    /// Nothing legitimate is in that gap, and the reason is structural rather
    /// than a survey of senders. [`Reception`] has two arms and
    /// [`Self::forward_or_reply`] relays only the first, so a request travels
    /// at most one hop and the node it was handed to is the only node that ever
    /// held a copy of it. Every `client_result` this node can legitimately
    /// receive is that node's — mailed by its apply, its refusal, or its
    /// deadline sweep, all three of which address the answer to the record's
    /// `answer_to`, which is this node.
    ///
    /// Narrowing costs a fast path in the case it is wrong about, and no more:
    /// a report this refuses leaves the record standing, and the deadline pays
    /// it. Widening costs an answer to a client that says whatever a node that
    /// was not asked chose to say.
    ///
    /// Both gates are kept, and the record gate above is still not implied by
    /// this one: a peer that *was* relayed a request may still report on a key
    /// no record exists for, and that report must not mint one.
    pub(crate) fn handle_client_result(&mut self, peer: &Peer, envelope: &Envelope) {
        let Some(client) = envelope.body.get("client").and_then(Value::as_str) else {
            return;
        };
        let Some(in_reply_to) = envelope.body.get("in_reply_to").and_then(Value::as_u64) else {
            return;
        };
        let Some(result) = envelope.body.get("result") else {
            return;
        };
        let result = match serde_json::from_value(result.clone()) {
            Ok(result) => result,
            Err(error) => {
                eprintln!("ignoring invalid client_result: {error}");
                return;
            }
        };
        let key = (client.to_owned(), in_reply_to);
        if !self.owed_answers.was_relayed_to(&key, peer.name()) {
            eprintln!(
                "ignoring client_result from a node this request was not relayed to: \
                 client={client} in_reply_to={in_reply_to} from={}",
                peer.name()
            );
            return;
        }
        let Some(accepted) = self.owed_answers.recorded(&key) else {
            eprintln!(
                "ignoring client_result for a request this node holds no record for: \
                 client={client} in_reply_to={in_reply_to} from={}",
                peer.name()
            );
            return;
        };
        // The leader answered a request this node relayed. The record says who
        // that answer is addressed to — this node's own name for a client that
        // reached it directly — and the token carries it, so the recipient is
        // the ledger's rather than this envelope's.
        self.answer(&accepted, result);
    }

    pub(crate) fn handle_client(&mut self, envelope: &Envelope) {
        let Some(in_reply_to) = envelope.body.get("msg_id").and_then(Value::as_u64) else {
            return;
        };
        self.handle_client_request(
            &Reception::FromClient,
            &envelope.src,
            in_reply_to,
            &envelope.body,
        );
    }
}
