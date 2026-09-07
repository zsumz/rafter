//! Client requests: forwarding, read barriers, and the reply path.
//!
//! # Who answers
//!
//! [`InitializedNode::deliver_result`] is the mechanism that puts an answer in
//! flight. It does not decide *whether* one is owed. That decision belongs to
//! each caller, because each caller is the one holding the record of the
//! obligation.
//!
//! Every request of either kind is recorded the same way, in `owed_answers`,
//! by the one funnel every request passes through. What differs is which
//! *further* mark each kind leaves, and therefore which fast path can pay the
//! record before its deadline does:
//!
//! - **A granted read is answered by the node holding the waiter, whatever its
//!   role.** The `PendingRead` in `pending_reads` says which barrier has not
//!   resolved yet, and [`InitializedNode::flush_reads`] answers exactly what it
//!   holds once the applied state reaches the floor. A read is served out of
//!   one node's own applied state; no other node has a copy of the request, so
//!   if this one declines the read has only its deadline left. The rest of this
//!   header is the argument that answering is not merely necessary but correct.
//! - **A committed write is answered by the node that accepted the client's
//!   request, whatever its role.** Its further mark is in the replicated entry:
//!   for a client that reached this node directly, `origin == self.name`, which
//!   is what still speaks after a restart has taken the volatile ledger with
//!   it. Every node in the cluster applies the entry and computes the identical
//!   result, so unlike a read the answer is not scarce — it is the *obligation*
//!   that is scarce, and a replica holding no obligation must stay silent.
//!
//! # Why every accepted request is answered
//!
//! Not because the paths that can answer one have been enumerated. Four rounds
//! of this reply path enumerated a set and were wrong each time, in the same
//! way: a list checked in the direction that was easy and relied on in the
//! direction that mattered. The fourth round is worth naming, because it was
//! this section: the previous text proved *record implies answer* and then
//! asserted *accepted implies record*, moving the burden from "is the list of
//! answering paths exhaustive?" to "is the list of accepting paths
//! exhaustive?" — the same unproved question one level up. It was false. A read
//! the leader served itself created a waiter and no record, so whether the
//! deadline covered a client request depended on which node the client reached.
//!
//! The argument runs on three facts instead, each of which is one place in the
//! code — or one type — rather than a claim about several.
//!
//! 1. **Acting on a client request requires a record for it.**
//!    Every function that acts on one takes an [`Accepted`], and that token has
//!    exactly two sources, both inside [`answers`](crate::answers) and both over
//!    fields no other module can fill: `accept`, which creates the record, and
//!    `recorded`, which finds one and never creates. So a path acting on a
//!    request either lodged its record or was handed one that already existed,
//!    and that much is rustc's to keep rather than a reader's to certify.
//!
//!    [`InitializedNode::handle_client_request`] is the funnel for requests
//!    *arriving*: [`InitializedNode::handle_client`] and
//!    [`InitializedNode::handle_forward`] both call it, it accepts the
//!    obligation before it looks at what kind of request this is, and the three
//!    things it can then do — [`InitializedNode::forward_or_reply`],
//!    [`InitializedNode::propose`], [`InitializedNode::start_read`] — each take
//!    the token it minted.
//!
//!    The previous round said the funnel was the whole of it: "both entry points
//!    call it and nothing else acts on a request." That was false, and false the
//!    same way the round before it was — a list checked in the easy direction.
//!    [`InitializedNode::handle_client_result`] is a third path to a client
//!    request key, and it called [`InitializedNode::deliver_result`] directly.
//!    A `(client, in_reply_to)` this node never accepted therefore got a
//!    client-addressed answer *and* an entry in `completed_replies`, after which
//!    [`InitializedNode::has_accepted`] refused the client's genuine request for
//!    that key above the accept — no record, no waiter, no deadline, no answer,
//!    and the mutation never reached the state machine. It takes `recorded`'s
//!    token now, which is what makes it able to pay an obligation and unable to
//!    invent one.
//! 2. **A record is destroyed only when an answer for it leaves.**
//!    [`OwedAnswers`](crate::answers) keeps its map private and exposes one way
//!    in and one way out; [`InitializedNode::accept_answer_obligation`] is the
//!    only wrapper over the first, and [`InitializedNode::deliver_result`] the
//!    only caller of the second — and it is also the only place a client answer
//!    is emitted.
//! 3. **Every record carries a deadline, and the tick sweep pays every record
//!    that reaches one.** [`InitializedNode::expire_owed_answers`] does not ask
//!    why a request is still outstanding. It cannot be wrong about a list it
//!    does not consult.
//!
//! Together: every accepted request is answered within its deadline, whatever
//! became of it — refused by the kernel, truncated under the next leader,
//! jumped by a snapshot install, committed on a leader that answered a process
//! which has since restarted, or waiting on a read barrier that neither granted
//! nor cancelled. The faster paths remain, because an answer that says what
//! actually happened is worth more than one that says "unknown"; but nothing
//! depends on them firing.
//!
//! ## What that does *not* cover
//!
//! Stated separately, because a scope claimed one step wider than the mechanism
//! reaches is the defect this section keeps growing. Each of these has a test.
//!
//! - **An envelope that never names a request.** `handle_client` needs a
//!   `msg_id` and `handle_forward` needs a `client`, an `in_reply_to` and a
//!   `request`; without them the envelope is dropped before the funnel. That is
//!   the one honest outcome — an answer is addressed to `(client, in_reply_to)`
//!   and there is no such pair to address one to.
//! - **A repeat of a request already accepted.** [`InitializedNode::has_accepted`]
//!   returns above the accept, so a second copy lodges no second record. The
//!   first copy's record is what covers the client, and its deadline stands.
//! - **The lifetime of this process.** The ledger is volatile by choice: every
//!   obligation in it is to somebody waiting on *this* process, and a restart
//!   ends that wait. A recovered node owes nothing for what it replays.
//! - **Anything the harness never accepted at all** — a request lost in the
//!   network before it arrived. No node can answer for a request it never saw,
//!   and the client's own retry is the only thing that covers it.
//! - **[`InitializedNode::deliver_result`] itself, which takes no token.** The
//!   token gates every path that acts on a request *before* an answer for it
//!   exists; it does not gate the emit. `deliver_result` takes plain strings, so
//!   "nothing reaches it without an obligation" is a claim about its callers —
//!   a list — and it is stated here rather than dressed up as a compiler
//!   guarantee.
//!
//!   It cannot be closed by requiring a token, and the reason is a decision made
//!   above rather than an omission: the direct arm after a restart is reached
//!   through the `origin == self.name` mark in the committed entry, precisely
//!   when the volatile ledger holds no record and no token can exist. Requiring
//!   one there would drop the answer this harness deliberately re-sends. What
//!   *is* checkable is the consequence, and it is checked: a call with no
//!   obligation behind it can send at most one duplicate answer, because
//!   `completed_replies` is written in the same call. What made
//!   `handle_client_result` a defect rather than an instance of this was that
//!   its keys came from another node's message, so the set could gain a key no
//!   client request had ever produced.

use rafter::{Input, Output};

use crate::{
    answers::Accepted,
    app::{read_value, ClientMutation, ClientResult, Command, ERROR_TEMPORARILY_UNAVAILABLE},
    InitializedNode,
};

mod deadline;
mod funnel;
mod replies;

/// The reason the kernel refused this step's proposal, if it refused one.
///
/// `Output::RejectProposal` means the entry was not appended. Nothing later in
/// the cluster will ever speak for that request — there is no commit to apply,
/// no truncation to notice, and no other node holding a record of it — so the
/// caller must answer it here.
fn proposal_rejection(outputs: &[Output]) -> Option<String> {
    outputs.iter().find_map(|output| match output {
        Output::RejectProposal { reason, .. } => Some(reason.to_string()),
        _ => None,
    })
}

/// Where a client request reached this node from, and therefore what a node
/// that does not lead may do with it.
///
/// These two arms are the whole of the forwarding policy. Nothing else may
/// decide it: [`InitializedNode::forward_or_reply`] matches on this value
/// exhaustively, so a third way for a request to arrive cannot be added
/// without saying there whether it may be relayed onward.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Reception {
    /// Straight from the client, which knows no better than to ask whichever
    /// node it can reach. A node that does not lead may hand the request to
    /// the leader it last heard from: that is the one hop.
    FromClient,
    /// Relayed here by a peer that did not lead either, and that is waiting on
    /// this node for the answer. The request has already spent its hop.
    FromPeer(String),
}

impl InitializedNode {
    /// Proposes a mutation for a request this node has already accepted.
    ///
    /// Reached only on the leader. `Output::RejectProposal` appends no entry,
    /// so no apply, no commit and no truncation notice will ever follow: the
    /// refusal is the only news there will ever be about this request, and
    /// answering it here is what turns it into a definite error rather than the
    /// indefinite one the deadline would eventually send. Both discharge the
    /// obligation — the record is the funnel's, not this function's — so a
    /// refusal this misses is answered late instead of never. `proposal_rejection`
    /// scanning the outputs for one shape is therefore a fast path and not a
    /// list anything rests on, which is the point of moving the accept upstream.
    ///
    /// The outputs are examined before they are routed rather than after,
    /// because `handle_outputs` consumes them and the reason is in them.
    fn propose(&mut self, accepted: &Accepted, request: ClientMutation) {
        let command = Command {
            origin: accepted.answer_to().to_owned(),
            client: accepted.client().to_owned(),
            in_reply_to: accepted.in_reply_to(),
            request,
        };
        let payload = serde_json::to_vec(&command).expect("command serializes");
        let outputs = self.step_unrouted(Input::ClientProposal { payload });
        if let Some(reason) = proposal_rejection(&outputs) {
            self.answer(
                accepted,
                ClientResult::Error {
                    code: ERROR_TEMPORARILY_UNAVAILABLE,
                    text: reason,
                },
            );
        }
        self.handle_outputs(outputs);
    }

    /// Answers one accepted request, to whoever its record says the answer is
    /// addressed to.
    ///
    /// The token carries the recipient, so no caller holding one has to pass
    /// `origin` alongside and no caller can pass one that disagrees with the
    /// record the sweep would fire on.
    fn answer(&mut self, accepted: &Accepted, result: ClientResult) {
        self.deliver_result(
            accepted.answer_to(),
            accepted.client(),
            accepted.in_reply_to(),
            result,
        );
    }

    /// Answers every pending read whose application floor the applied state has
    /// reached.
    ///
    /// A read with no floor yet has no granted barrier and is never ready. This
    /// runs from the grant arm, from every apply, from a snapshot install, and
    /// from the tick loop — the tick is what re-examines a read that stalled
    /// and would otherwise wait for an unrelated write to arrive and trigger a
    /// pass.
    ///
    /// A waiter is retired only after its answer has been handed to
    /// [`Self::deliver_result`], never before. Retiring it first and *then*
    /// letting the reply path decide whether it could send is what once lost a
    /// read outright, and the order is kept even though the ledger record would
    /// now catch it: a read recovered by its deadline is answered `timeout`
    /// when this node could have said what the value was, and that is a worse
    /// answer, not an equal one. The backstop is not a licence to lean on it.
    ///
    /// Retiring unconditionally is sound because `deliver_result` discharges
    /// the obligation on every call. Its one non-sending arm is its
    /// `completed_replies` dedupe, which fires exactly when this node already
    /// sent that client an answer for that request — so the waiter has
    /// nothing left to pay. Conditioning retirement on a fresh send instead
    /// would strand precisely that waiter for good: nothing would ever make the
    /// duplicate go out, so every later flush would re-examine it and decline
    /// again, forever. Should `deliver_result` ever grow an arm that leaves the
    /// request genuinely unanswered, that arm must record the outstanding
    /// obligation somewhere in the same change; it cannot be dropped here.
    pub(crate) fn flush_reads(&mut self) {
        let ready = self
            .pending_reads
            .iter()
            .filter_map(|(request_id, read)| {
                read.application_floor
                    .is_some_and(|floor| self.app.applied >= floor)
                    .then_some(*request_id)
            })
            .collect::<Vec<_>>();
        for request_id in ready {
            let read = self
                .pending_reads
                .get(&request_id)
                .cloned()
                .expect("pending read exists");
            let result = read_value(&self.app.kv, &read.key);
            self.deliver_result(&read.origin, &read.client, read.in_reply_to, result);
            self.pending_reads.remove(&request_id);
        }
    }
}
