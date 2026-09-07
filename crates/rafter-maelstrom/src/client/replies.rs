//! The one place a client answer leaves this node, and the one place a record
//! of an owed answer is retired.
//!
//! Those two being one fact is what makes the obligation checkable rather than
//! argued: a record is born at the accept and dies here, so "every accepted
//! request is answered" follows from the deadline alone.
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

use serde_json::{json, Value};

use crate::{
    app::{ClientResult, Command, ERROR_TEMPORARILY_UNAVAILABLE, ERROR_TIMEOUT},
    InitializedNode,
};

impl InitializedNode {
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
                if matches!(code, ERROR_TIMEOUT | ERROR_TEMPORARILY_UNAVAILABLE)
                    && std::env::var("RAFTER_MAELSTROM_LEASE_EVIDENCE").as_deref() == Ok("1")
                {
                    let _ = write!(
                        text,
                        " [rafter-lease-probe client={client} msg_id={in_reply_to} code={code}]"
                    );
                }
                json!({"type": "error", "msg_id": msg_id, "in_reply_to": in_reply_to, "code": code, "text": text})
            }
        }
    }
}
