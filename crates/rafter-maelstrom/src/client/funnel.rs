//! The one funnel every arriving client request passes through, and the three
//! things it may then do with one: hand it to the leader, propose it, or open a
//! read barrier for it.
//!
//! Each of those takes the token the accept minted, so a new way to act on a
//! request cannot be added below the funnel without a record existing first.
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

use rafter::{Input, ReadId, Role};
use serde_json::{json, Value};

use crate::{
    answers::Accepted,
    app::{
        parse_client_request, ClientMutation, ClientRequest, ClientResult,
        ERROR_TEMPORARILY_UNAVAILABLE,
    },
    InitializedNode, PendingRead,
};

use super::Reception;

mod entry;

impl InitializedNode {
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
}
