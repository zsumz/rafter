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
//!
//! # How far a request travels
//!
//! A node that cannot serve a request hands it to the leader it knows — once.
//! [`Reception`] records where the request came from, and
//! [`InitializedNode::forward_or_reply`] relays only what came from a client,
//! so a `client_forward` is never itself forwarded. The bound is structural
//! rather than a counter on the wire, and it holds however wrong `known_leader`
//! is on however many nodes.
//!
//! One consumer reads it in the other direction, and it is stated here because
//! that is the direction it is used in: a request this node relayed exists on
//! exactly one other node, so *the node it was handed to is the only node that
//! can report what became of it*. That is what makes
//! [`InitializedNode::handle_client_result`]'s sender check complete rather
//! than merely careful — a second hop would put a copy somewhere this node
//! never named, and there is none.
//!
//! # Why a request is acted on once
//!
//! "Why every accepted request is answered", above, is the *at least once*
//! half. This is the at most once half, and it is the one with teeth for
//! linearizability:
//! [`InitializedNode::has_accepted`] refuses a request this node has already
//! taken responsibility for, before anything is proposed or relayed. Without
//! it two copies of one `cas` commit as two entries and the state machine runs
//! the mutation twice — which can roll back another client's committed write
//! that linearized between them, from a request its client issued once.
//!
//! Refusing is not a loss. A repeat of `(client, in_reply_to)` is the same
//! request arriving twice, never a new attempt: Maelstrom allocates a fresh
//! `msg_id` per attempt. So an answer for it is already owed or already sent,
//! and the deadline above covers the former.
//!
//! # Why the two rules differ
//!
//! A read exists on one node. A committed write exists on all of them. A read's
//! waiter lives only where the barrier was granted, so `flush_reads` answering
//! everything it holds is both safe and required. But `Output::Apply` reaches
//! every replica with the same `origin` string in the payload, so an
//! `origin`-only rule cannot tell the node that accepted the client's request
//! from the ones that merely replicated it. Delivering on all of them mails
//! `N - 1` redundant `client_result` envelopes per write, and — because the
//! dedupe set is volatile — makes a restart replaying committed entries re-mail
//! answers for requests this node never accepted and nobody is waiting on.
//!
//! Role is not the axis that separates them. Consider a node that accepted a
//! peer's forward, proposed it, and was demoted before the entry committed
//! under the next leader. It is the only node that owes that peer an answer,
//! and it is not the leader; the new leader owes nothing and is. A role gate
//! gets both backwards. It also silences the demoted node that granted a read
//! barrier, which is the bug f3028041 fixed. The axis is the obligation, and
//! both rules key off it.
//!
//! Nothing here bounds the *direct* arm across a restart: a recovered node
//! replaying an entry it originated re-sends that client's answer, because
//! `completed_replies` did not survive either. That is the conservative
//! direction — an extra answer to a client this node genuinely served, which
//! Maelstrom discards as a stale `in_reply_to` — and it is left as it is
//! deliberately. What is not tolerable, and is what the obligation record
//! rules out, is a node re-sending an answer for a request it never accepted.
//!
//! # Why a granted read is answered regardless of the current role
//!
//! [`InitializedNode::deliver_result`] does not consult `role()`. A read whose
//! barrier was granted is answered out of this node's applied state whether or
//! not this node is still the leader. That is linearizable, and it rests on
//! three facts the kernel already establishes.
//!
//! 1. **A grant is a finished proof, not a standing permission.** The kernel
//!    emits `ReadIndexGranted` only once a quorum has acknowledged this node's
//!    leadership in a round registered at or after the barrier, which proves
//!    that at that instant no other leader had committed anything absent from
//!    this node's log at or below the granted read index. That instant lies
//!    inside the read's execution interval, and losing leadership afterwards
//!    cannot retract it. The invariant registry draws the same line: RD-01.a
//!    binds leadership to *initiating or granting* a barrier and never to
//!    serving one, and RD-01.c cancels the reads still *pending* when authority
//!    is lost. A granted barrier is neither — the kernel dropped it from the
//!    pending set at the grant, so demotion's `ReadIndexCanceled` sweep cannot
//!    reach it and nothing upstream is expected to speak for it again. This
//!    node is the party still holding the answer, and it answers.
//!
//!    That last clause is a claim about the kernel's outputs, and it is used in
//!    the one direction where being wrong is cheap: it licenses *answering*, so
//!    a stray later output for the same read finds the waiter gone or delivers
//!    into `deliver_result`'s dedupe. Nothing here declines to answer on the
//!    strength of it — that would be the converse, and it is the mistake the
//!    error-reply paragraph below used to make.
//! 2. **The obligation the kernel places on this caller is about apply, not
//!    about role.** Its wording is to wait until local apply reaches every
//!    application entry at or below the granted index. That index is
//!    `PendingRead::application_floor`, and [`InitializedNode::flush_reads`] is
//!    that wait. Nothing in the contract mentions still being leader.
//! 3. **Applied state is a committed prefix, and it only moves forward.**
//!    LG-04.a — a committed entry is never truncated or overwritten — makes the
//!    prefix below the applied index permanent, and `app.applied` advances only
//!    through an apply of a committed entry or a snapshot install. So the value
//!    read at applied index `A` is the real committed state at `A`, on a
//!    follower exactly as on a leader.
//!
//! Together these place the answer at the committed state of some real instant
//! between the grant and the reply — an instant inside the read's own interval,
//! which is what linearizability asks for. Every write that completed before
//! the read was invoked committed at or below the read index and is therefore
//! included. Every write missing from the answer committed after the grant,
//! hence concurrently with the read, and may be ordered after it (RD-06.a).
//! Demotion changes none of that; it only means fresher commits exist
//! elsewhere, and a read is free to linearize before them.
//!
//! Refusing to answer is not the conservative choice. It strands the read,
//! which is the failure RD-04.b rules out when it requires that a read never be
//! held for an index the state machine cannot reach. The same holds for the
//! error replies: when the kernel does report a barrier rejected or cancelled,
//! that reply *is* the answer, and a role gate dropped precisely the answers
//! that had to be sent — the node reporting a barrier lost for want of
//! leadership is by construction a node that no longer leads.
//!
//! What that does not say, and what this text used to say, is that
//! `ReadIndexRejected` and `ReadIndexCanceled` arrive *exactly* when leadership
//! is absent or lost. That is the converse, it was never proved, and nothing
//! here may rest on it: a barrier that neither grants nor resolves — a leader
//! whose round never completes and which never steps down — emits neither
//! output, and a read waiting on it would be held forever. The record
//! `handle_client_request` lodges before the barrier opens is what covers that,
//! and it covers it without asking which of the kernel's outputs arrived.
//!
//! None of this licenses serving a barrier that never granted. Those are
//! answered as errors — by the kernel's own report where it makes one, and by
//! the deadline where it does not — and the client retries.

use std::fmt::Write as _;

use rafter::{Input, Output, ReadId, Role};
use serde_json::{json, Value};

use crate::{
    answers::{Accepted, RequestKey},
    app::{
        parse_client_request, read_value, ClientMutation, ClientRequest, ClientResult, Command,
        ERROR_TEMPORARILY_UNAVAILABLE, ERROR_TIMEOUT,
    },
    protocol::{Envelope, Peer},
    InitializedNode, PendingRead,
};

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

    /// The one funnel every client request passes through, and the one place an
    /// obligation for one is accepted.
    ///
    /// Both entry points for an *arriving* request reach here —
    /// [`Self::handle_client`] for one the client sent to this node,
    /// [`Self::handle_forward`] for one a peer relayed. So the scope of the
    /// accept below is every request this node takes responsibility for, in the
    /// direction the sweep needs it: acted on implies recorded.
    ///
    /// That scope is stated as "arriving" rather than "every request this node
    /// acts on", which is what it used to say. [`Self::handle_client_result`]
    /// also acts on a request key and does not come through here: it pays a
    /// record rather than creating one, and takes its token from
    /// `OwedAnswers::recorded` instead. Reading this function's scope as the
    /// wider one is what let that path answer for a request no accept had seen.
    ///
    /// The accept sits above the parse and above the role check deliberately.
    /// Everything below it is a way of acting on the request, every one of them
    /// takes the [`Accepted`] this line produces, and only the ledger can mint
    /// one — so a new kind of request, or a new thing to do with an old one,
    /// cannot be added below without a record existing first. The previous
    /// round asserted that property in prose about a list of call sites, and
    /// the list was missing [`Self::start_read`].
    ///
    /// A record where no answer is owed costs nothing: `deliver_result` retires
    /// it, and both early arms below go through `deliver_result`.
    fn handle_client_request(
        &mut self,
        reception: &Reception,
        client: &str,
        in_reply_to: u64,
        body: &Value,
    ) {
        // Derived, never passed alongside: the node an answer is addressed to
        // is a function of where the request came from, and two carriers of
        // that one fact could disagree.
        let origin = match reception {
            Reception::FromClient => self.name.clone(),
            Reception::FromPeer(peer) => peer.clone(),
        };
        if self.has_accepted(&(client.to_owned(), in_reply_to)) {
            return;
        }
        let accepted = self.accept_answer_obligation(&origin, client, in_reply_to);
        let request = match parse_client_request(body) {
            Ok(request) => request,
            Err(result) => {
                self.answer(&accepted, result);
                return;
            }
        };
        if self.node.role() != Role::Leader {
            self.forward_or_reply(reception, &accepted, body);
            return;
        }
        self.known_leader = Some(self.node.id());
        match request {
            ClientRequest::Read { key } => {
                eprintln!(
                    "rafter-maelstrom lease-read node={} phase=request role=leader term={} active={} client={} msg_id={}",
                    self.name,
                    self.node.current_term(),
                    self.node.read_lease_active(),
                    accepted.client(),
                    accepted.in_reply_to()
                );
                self.start_read(&accepted, key);
            }
            ClientRequest::Write { key, value } => {
                self.propose(&accepted, ClientMutation::Write { key, value });
            }
            ClientRequest::Cas { key, from, to } => {
                self.propose(&accepted, ClientMutation::Cas { key, from, to });
            }
        }
    }

    /// Hands a request this node cannot serve to the leader it knows, or says
    /// that it cannot serve it.
    ///
    /// A request is relayed at most once, and this match is what bounds it.
    /// `known_leader` is a memory, not a fact: [`Self::observe_leader`] records
    /// whoever last led, and two nodes each holding the other bounce one
    /// request between them for as long as neither hears from the real leader
    /// — one `client_forward` per hop, without bound. Refusing to relay a
    /// request that has already been relayed caps that chain at a single hop
    /// no matter how stale either memory is.
    ///
    /// The cost is a request that would have reached the leader on its second
    /// hop and now does not. That is the right way to lose: the peer is told
    /// immediately, `ERROR_TEMPORARILY_UNAVAILABLE` is definite — this node
    /// appended nothing — and the client reissues. Circulating instead trades
    /// a bounded retry for an unbounded storm.
    ///
    /// Handing the request on does not hand the obligation on: the leader may
    /// commit the entry and answer a process that has restarted, or never
    /// commit it at all, and either way this node is the last party still
    /// holding a tie to the client. The [`Accepted`] this takes is the record
    /// that says so, lodged by the funnel before this was reached.
    ///
    /// The relay is written back to that record, because this is the one place
    /// that knows where the request went and
    /// [`Self::handle_client_result`] is the one place that needs to.
    fn forward_or_reply(&mut self, reception: &Reception, accepted: &Accepted, body: &Value) {
        let relay_to = match reception {
            Reception::FromClient => self.known_leader.filter(|leader| *leader != self.node.id()),
            Reception::FromPeer(_) => None,
        };
        let Some(leader) = relay_to else {
            let text = match reception {
                Reception::FromClient => "no Raft leader known yet",
                Reception::FromPeer(_) => "forwarded request reached a node that does not lead",
            };
            self.answer(
                accepted,
                ClientResult::Error {
                    code: ERROR_TEMPORARILY_UNAVAILABLE,
                    text: text.to_string(),
                },
            );
            return;
        };
        // A leader this node cannot name is a leader it cannot reach. The
        // record and its deadline still cover the client, which is the same
        // outcome the emit below would have had.
        let Some(leader_name) = self.id_to_name.get(&leader).cloned() else {
            return;
        };
        self.owed_answers.relayed(
            &(accepted.client().to_owned(), accepted.in_reply_to()),
            leader_name.clone(),
        );
        self.emit(
            &leader_name,
            json!({
                "type": "client_forward",
                "client": accepted.client(),
                "in_reply_to": accepted.in_reply_to(),
                "request": body,
            }),
        );
    }

    /// Opens a read barrier for a request this node accepted, and parks a
    /// waiter for it.
    ///
    /// The waiter is not the record. It says only that this barrier has not
    /// resolved yet; the record that an answer is owed is the [`Accepted`] the
    /// funnel lodged, and that is what the deadline fires on. Reversing those
    /// two was the fourth defect: a read served here held a waiter and nothing
    /// else, so a barrier that neither granted nor cancelled left the client
    /// with no answer and the sweep with nothing to see.
    fn start_read(&mut self, accepted: &Accepted, key: Value) {
        let request_id = self.next_read_id;
        self.next_read_id += 1;
        self.pending_reads.insert(
            request_id,
            PendingRead {
                origin: accepted.answer_to().to_owned(),
                client: accepted.client().to_owned(),
                in_reply_to: accepted.in_reply_to(),
                key,
                application_floor: None,
            },
        );
        self.step(Input::ReadIndex {
            read_id: ReadId(request_id),
        });
    }

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

    /// Whether this node has already taken responsibility for this request.
    ///
    /// Two states, and no third — checked rather than argued, in both halves.
    /// *Never in neither*: [`Self::handle_client_request`] accepts before it
    /// acts, and every way of acting takes an [`Accepted`], which only the
    /// ledger mints, so a request this node acted on is in one set or the
    /// other. *Never between them*: [`Self::deliver_result`] is the only way out
    /// of the ledger, and in the same call, before it can return, it puts the
    /// request into `completed_replies`. So these two together are exactly "this
    /// node has seen it".
    ///
    /// The first half is new. It read as an assertion about a list of accept
    /// sites until the list turned out to be missing [`Self::start_read`] — a
    /// read the leader served sat in neither set, and a second copy of it was
    /// accepted again, opening a second barrier for a request issued once.
    ///
    /// It has since been wrong in the other direction too, which is worse,
    /// because this half is read *above* the accept and a false positive here
    /// discards a request. [`Self::handle_client_result`] wrote
    /// `completed_replies` for a key taken from another node's message, so
    /// "this node has seen it" answered yes for a request this node had never
    /// seen and the client's own copy was refused. Only paths holding a record
    /// reach `deliver_result` with a client key now.
    ///
    /// A repeat is a duplicate delivery, never a client retry: Maelstrom gives
    /// every attempt a fresh `msg_id`, so a second `(client, in_reply_to)` is
    /// by construction the same request arriving twice. Dropping it is
    /// therefore not a lost request — an answer for it is already owed or
    /// already sent — while acting on it appends a second log entry for a
    /// request issued once and runs its mutation again.
    fn has_accepted(&self, key: &(String, u64)) -> bool {
        self.owed_answers.is_owed(key) || self.completed_replies.contains(key)
    }

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

    /// The node this command's answer is addressed to, if this node owes one.
    ///
    /// Read in the direction the caller uses it. The record comes first and the
    /// payload second, and that order is the point: `command.origin` is in the
    /// replicated entry, so every replica reads the same string, and it can say
    /// that *some* node accepted *some* copy of this request — never that this
    /// node accepted this one. A record lodged for `n2`'s forward therefore
    /// pays `n2` even when the entry that carried the request to commit was
    /// proposed for `n3`'s. Reading the payload first hands the answer to
    /// whichever origin the first matching commit happens to name, which is a
    /// node that may have forwarded nothing here at all.
    ///
    /// The `origin == self.name` fallback is the record for a request the
    /// client sent to this node directly: the command carries this node's name,
    /// that mark is in the log, and it is what still speaks after a restart has
    /// taken the volatile ledger with it. A replica that only replicated the
    /// entry matches neither and stays silent. See this module's header for why
    /// role is not one of the ways.
    ///
    /// It reads the record and does not retire it. Retirement belongs to
    /// [`Self::deliver_result`], the one place an answer leaves this node, so
    /// that "a record is destroyed only by an answer going out" needs no second
    /// proof and no second site to keep in step.
    pub(crate) fn claim_answer_for(&self, command: &Command) -> Option<String> {
        if let Some(answer_to) = self
            .owed_answers
            .answer_to(&(command.client.clone(), command.in_reply_to))
        {
            return Some(answer_to.to_owned());
        }
        (command.origin == self.name).then(|| self.name.clone())
    }

    /// Puts one request's answer in flight and retires the record that said one
    /// was owed.
    ///
    /// Total, in the sense its callers rest on: every call discharges the
    /// obligation. Either this call puts an answer on the wire, or this node
    /// already put one there for the same `(client, in_reply_to)` and declines
    /// to send a second — a suppressed duplicate, not a drop. No arm leaves the
    /// request unanswered and no arm leaves a record behind.
    ///
    /// This is the only place a client answer leaves this node and the only
    /// place a record is retired, and those two facts being one fact is what
    /// makes the obligation checkable rather than argued: a record is born when
    /// the request is accepted and dies only here, so "every accepted request
    /// is answered" follows from the deadline alone. `completed_replies` gains
    /// the request immediately before the emit and nowhere else, so membership
    /// is exactly "an answer for this has already gone out".
    ///
    /// It does not consult the current role, and it does not decide whether an
    /// answer is owed at all; that is [`Self::claim_answer_for`] and
    /// [`Self::flush_reads`], each of which holds the record. See this module's
    /// header for both rules.
    pub(crate) fn deliver_result(
        &mut self,
        origin: &str,
        client: &str,
        in_reply_to: u64,
        result: ClientResult,
    ) {
        let key = (client.to_owned(), in_reply_to);
        self.owed_answers.retire(&key);
        if !self.completed_replies.insert(key) {
            return;
        }
        if origin == self.name {
            let body = self.result_body(client, in_reply_to, result);
            self.emit(client, body);
        } else {
            self.emit(
                origin,
                json!({
                    "type": "client_result",
                    "client": client,
                    "in_reply_to": in_reply_to,
                    "result": result,
                }),
            );
        }
    }

    fn result_body(&mut self, client: &str, in_reply_to: u64, result: ClientResult) -> Value {
        let msg_id = self.next_msg_id;
        self.next_msg_id += 1;
        match result {
            ClientResult::ReadOk { value } => {
                json!({"type": "read_ok", "msg_id": msg_id, "in_reply_to": in_reply_to, "value": value})
            }
            ClientResult::WriteOk => {
                json!({"type": "write_ok", "msg_id": msg_id, "in_reply_to": in_reply_to})
            }
            ClientResult::CasOk => {
                json!({"type": "cas_ok", "msg_id": msg_id, "in_reply_to": in_reply_to})
            }
            ClientResult::Error { code, mut text } => {
                if code == ERROR_TEMPORARILY_UNAVAILABLE
                    && std::env::var("RAFTER_MAELSTROM_LEASE_EVIDENCE").as_deref() == Ok("1")
                {
                    let _ = write!(
                        text,
                        " [rafter-lease-probe client={client} msg_id={in_reply_to} code=11]"
                    );
                }
                json!({"type": "error", "msg_id": msg_id, "in_reply_to": in_reply_to, "code": code, "text": text})
            }
        }
    }
}
