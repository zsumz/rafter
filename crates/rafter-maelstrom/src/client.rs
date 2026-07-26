//! Client requests: forwarding, read barriers, and the reply path.
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
        self.handle_client_request(origin, client.to_string(), in_reply_to, &request);
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
        self.handle_client_request(self.name.clone(), envelope.src, in_reply_to, &envelope.body);
    }

    fn handle_client_request(
        &mut self,
        origin: String,
        client: String,
        in_reply_to: u64,
        body: &Value,
    ) {
        let request = match parse_client_request(body) {
            Ok(request) => request,
            Err(result) => {
                self.deliver_result(&origin, &client, in_reply_to, result);
                return;
            }
        };
        if self.node.role() != Role::Leader {
            self.forward_or_reply(&origin, &client, in_reply_to, body);
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

    fn forward_or_reply(&mut self, origin: &str, client: &str, in_reply_to: u64, body: &Value) {
        if let Some(leader) = self.known_leader.filter(|leader| *leader != self.node.id()) {
            self.send_to_node(
                leader,
                json!({
                    "type": "client_forward",
                    "client": client,
                    "in_reply_to": in_reply_to,
                    "request": body,
                }),
            );
        } else {
            self.deliver_result(
                origin,
                client,
                in_reply_to,
                ClientResult::Error {
                    code: ERROR_TEMPORARILY_UNAVAILABLE,
                    text: "no Raft leader known yet".to_string(),
                },
            );
        }
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

    fn propose(
        &mut self,
        origin: String,
        client: String,
        in_reply_to: u64,
        request: ClientMutation,
    ) {
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
    /// silently, and with nothing left to retry from. Retiring is sound here
    /// only because `deliver_result` is total; should it ever grow a case where
    /// no answer leaves the node, this retirement has to become conditional on
    /// that case in the same change.
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

    /// Puts one request's answer in flight, either as a direct reply or as a
    /// `client_result` handed back to the node that forwarded the request.
    ///
    /// Total: every call leaves the origin holding an answer. In particular it
    /// does not consult the current role — see this module's header for why a
    /// granted read stays answerable after leadership is lost, and why the
    /// rejection and cancellation replies need the same freedom.
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
