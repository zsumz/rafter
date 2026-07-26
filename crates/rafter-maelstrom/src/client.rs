//! Client requests: forwarding, read barriers, and the reply path.
//!
//! # Who answers
//!
//! [`InitializedNode::deliver_result`] is the mechanism that puts an answer in
//! flight. It does not decide *whether* one is owed. That decision belongs to
//! each caller, because each caller is the one holding the record of the
//! obligation, and the two kinds of request record it differently:
//!
//! - **A granted read is answered by the node holding the waiter, whatever its
//!   role.** The record is the `PendingRead` in `pending_reads`, and
//!   [`InitializedNode::flush_reads`] answers exactly what it holds. A read is
//!   served out of one node's own applied state; no other node has a copy of
//!   the obligation, so if this one declines the read is lost. The rest of this
//!   header is the argument that answering is not merely necessary but correct.
//! - **A committed write is answered by the node that accepted the client's
//!   request, whatever its role.** The record is either `origin == self.name`,
//!   carried in the command itself, for a client that reached this node
//!   directly; or an entry in `pending_forwards`, for a peer's forward this
//!   node accepted and proposed. Every node in the cluster applies the entry
//!   and computes the identical result, so unlike a read the answer is not
//!   scarce — it is the *obligation* that is scarce, and a replica holding no
//!   obligation must stay silent.
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
//! Maelstrom discards as a stale `in_reply_to` — and it is unchanged from
//! before. The remote arm's re-mail was not: it spoke for a request this node
//! never accepted.
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
//!    reach it and nothing upstream will speak for it again. This node is the
//!    only party still holding the answer.
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
//! error replies: `ReadIndexRejected` and `ReadIndexCanceled` arrive exactly
//! when leadership is absent or lost, so a role gate dropped precisely the
//! answers that had to be sent.
//!
//! None of this licenses serving a barrier that never granted. Those still end
//! as errors, and the client retries.

use std::fmt::Write as _;

use rafter::{Input, ReadId, Role};
use serde_json::{json, Value};

use crate::{
    app::{
        parse_client_request, read_value, ClientMutation, ClientRequest, ClientResult, Command,
        ERROR_TEMPORARILY_UNAVAILABLE,
    },
    protocol::Envelope,
    InitializedNode, PendingRead,
};

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
    pub(crate) fn handle_forward(&mut self, envelope: Envelope) {
        let origin = envelope.src;
        let Some(client) = envelope.body.get("client").and_then(Value::as_str) else {
            return;
        };
        let Some(in_reply_to) = envelope.body.get("in_reply_to").and_then(Value::as_u64) else {
            return;
        };
        let Some(request) = envelope.body.get("request").cloned() else {
            return;
        };
        self.handle_client_request(
            &Reception::FromPeer(origin),
            client.to_string(),
            in_reply_to,
            &request,
        );
    }

    pub(crate) fn handle_client_result(&mut self, envelope: &Envelope) {
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
        self.reply_to_client(client, in_reply_to, result);
    }

    pub(crate) fn handle_client(&mut self, envelope: Envelope) {
        let Some(in_reply_to) = envelope.body.get("msg_id").and_then(Value::as_u64) else {
            return;
        };
        self.handle_client_request(
            &Reception::FromClient,
            envelope.src,
            in_reply_to,
            &envelope.body,
        );
    }

    fn handle_client_request(
        &mut self,
        reception: &Reception,
        client: String,
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
        let request = match parse_client_request(body) {
            Ok(request) => request,
            Err(result) => {
                self.deliver_result(&origin, &client, in_reply_to, result);
                return;
            }
        };
        if self.node.role() != Role::Leader {
            self.forward_or_reply(reception, &origin, &client, in_reply_to, body);
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
                    client,
                    in_reply_to
                );
                self.start_read(origin, client, in_reply_to, key);
            }
            ClientRequest::Write { key, value } => {
                self.propose(
                    origin,
                    client,
                    in_reply_to,
                    ClientMutation::Write { key, value },
                );
            }
            ClientRequest::Cas { key, from, to } => {
                self.propose(
                    origin,
                    client,
                    in_reply_to,
                    ClientMutation::Cas { key, from, to },
                );
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
    fn forward_or_reply(
        &mut self,
        reception: &Reception,
        origin: &str,
        client: &str,
        in_reply_to: u64,
        body: &Value,
    ) {
        let relay_to = match reception {
            Reception::FromClient => self.known_leader.filter(|leader| *leader != self.node.id()),
            Reception::FromPeer(_) => None,
        };
        let Some(leader) = relay_to else {
            let text = match reception {
                Reception::FromClient => "no Raft leader known yet",
                Reception::FromPeer(_) => "forwarded request reached a node that does not lead",
            };
            self.deliver_result(
                origin,
                client,
                in_reply_to,
                ClientResult::Error {
                    code: ERROR_TEMPORARILY_UNAVAILABLE,
                    text: text.to_string(),
                },
            );
            return;
        };
        self.send_to_node(
            leader,
            json!({
                "type": "client_forward",
                "client": client,
                "in_reply_to": in_reply_to,
                "request": body,
            }),
        );
    }

    fn start_read(&mut self, origin: String, client: String, in_reply_to: u64, key: Value) {
        let request_id = self.next_read_id;
        self.next_read_id += 1;
        self.pending_reads.insert(
            request_id,
            PendingRead {
                origin,
                client,
                in_reply_to,
                key,
                application_floor: None,
            },
        );
        self.step(Input::ReadIndex {
            read_id: ReadId(request_id),
        });
    }

    /// Proposes a mutation, recording first that this node now owes an answer
    /// for it.
    ///
    /// Reached only on the leader. When `origin` is a peer, accepting that
    /// peer's forward is what makes this node the answerer for the committed
    /// write, and `pending_forwards` is the only place that is written down —
    /// the command payload's `origin` names the peer, not this node. A request
    /// the client sent here directly needs no entry: `origin == self.name`
    /// already says it, on this node and nowhere else.
    fn propose(
        &mut self,
        origin: String,
        client: String,
        in_reply_to: u64,
        request: ClientMutation,
    ) {
        if origin != self.name {
            self.pending_forwards.insert((client.clone(), in_reply_to));
        }
        let command = Command {
            origin,
            client,
            in_reply_to,
            request,
        };
        let payload = serde_json::to_vec(&command).expect("command serializes");
        self.step(Input::ClientProposal { payload });
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
    /// [`Self::deliver_result`], never before. The waiter is this node's only
    /// record that an answer is owed, so retiring it first and *then* letting
    /// the reply path decide whether it could send loses the read outright —
    /// silently, and with nothing left to retry from.
    ///
    /// Retiring unconditionally is sound because `deliver_result` discharges
    /// the obligation on every call. Its one non-sending arm is
    /// [`Self::reply_to_client`]'s dedupe, which fires exactly when this node
    /// already sent that client an answer for that request — so the waiter has
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

    /// Whether this node is the one that owes `command`'s client an answer,
    /// consuming the record if it is.
    ///
    /// Two ways to be that node, and no third: the client reached this node
    /// directly, so the command carries this node's name as its `origin`; or a
    /// peer forwarded the request here and this node proposed it, leaving the
    /// entry in `pending_forwards` that this consumes. A replica that only
    /// replicated the entry matches neither and stays silent. See this module's
    /// header for why role is not one of the ways.
    ///
    /// The record is consumed rather than read so that the answer is mailed at
    /// most once per accepted request, however many times the entry is applied.
    pub(crate) fn claim_answer_for(&mut self, command: &Command) -> bool {
        command.origin == self.name
            || self
                .pending_forwards
                .remove(&(command.client.clone(), command.in_reply_to))
    }

    /// Puts one request's answer in flight, either as a direct reply or as a
    /// `client_result` handed back to the node that forwarded the request.
    ///
    /// Total, in the sense its callers rest on: every call discharges the
    /// origin's answer obligation. Either this call puts an answer on the wire,
    /// or [`Self::reply_to_client`] finds that this node already put one there
    /// for the same `(client, in_reply_to)` and declines to send a second — a
    /// suppressed duplicate, not a drop. No arm leaves the request unanswered.
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
        if origin == self.name {
            self.reply_to_client(client, in_reply_to, result);
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

    /// Sends one answer straight to `client`, at most once per request for the
    /// life of this process.
    ///
    /// `completed_replies` gains `(client, in_reply_to)` immediately before the
    /// emit and in no other place, so a member of that set is exactly a request
    /// this node has already put an answer on the wire for. The early return is
    /// therefore an *already delivered* case, not a dropped one — which is what
    /// keeps [`Self::deliver_result`] total and lets [`Self::flush_reads`]
    /// retire a waiter without checking.
    fn reply_to_client(&mut self, client: &str, in_reply_to: u64, result: ClientResult) {
        if !self
            .completed_replies
            .insert((client.to_string(), in_reply_to))
        {
            return;
        }
        let body = self.result_body(client, in_reply_to, result);
        self.emit(client, body);
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
