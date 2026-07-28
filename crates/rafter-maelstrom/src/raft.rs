//! Stepping the kernel and routing what a step released.
//!
//! Every output here is already durable — the runtime discharges its
//! persistence obligation before releasing anything — so this module sends,
//! applies, and answers without adding a fence of its own.
//!
//! The read path is where the harness earns its keep. A granted barrier waits
//! on the highest committed *application* entry at or below the certified read
//! index, never on the read index itself: a barrier grants at the leader's
//! commit index, and after an election the entry there is that leader's `Noop`,
//! which the application is never told about. Waiting for the read index would
//! hang every read on an idle cluster.

use rafter::{Input, LogIndex, Message, NodeId, Output};
use rafter_codec::{decode_message, encode_message};
use serde_json::{json, Value};

use crate::{
    app::{
        apply_committed_command, maybe_crash_after_app_persist_before_reply, AfterAppPersist,
        ClientResult, Command, CommandApplyOutcome, ERROR_TEMPORARILY_UNAVAILABLE,
    },
    protocol::{decode_hex, encode_hex, Envelope, Peer},
    InitializedNode,
};

#[cfg(test)]
mod read_tests;
#[cfg(test)]
mod reply_tests;
pub(crate) mod snapshots;

impl InitializedNode {
    /// Steps the kernel with one framed message from a peer.
    ///
    /// The [`Peer`] is the membership lookup this used to perform for itself,
    /// hoisted into the dispatch so that all three harness arms are gated in
    /// one place rather than two of three being gated where each happened to
    /// be written. It carries the id because it *is* the same lookup: the
    /// sender being a node and which node it is were two reads of one map, and
    /// only one of them was a gate.
    pub(crate) fn handle_raft(&mut self, peer: &Peer, envelope: &Envelope) {
        let from = peer.id();
        let Some(frame) = envelope.body.get("frame").and_then(Value::as_str) else {
            eprintln!("ignoring raft message without frame");
            return;
        };
        let bytes = match decode_hex(frame) {
            Ok(bytes) => bytes,
            Err(error) => {
                eprintln!("ignoring raft message with invalid hex: {error}");
                return;
            }
        };
        match decode_message(&bytes) {
            Ok(message) => {
                self.observe_leader(&message);
                self.step(Input::Message { from, message });
            }
            Err(error) => eprintln!("ignoring raft message with invalid frame: {error}"),
        }
    }

    pub(crate) fn step(&mut self, input: Input) {
        let outputs = self.step_unrouted(input);
        self.handle_outputs(outputs);
    }

    /// Steps the kernel and reports any role or lease transition, handing the
    /// outputs back unrouted.
    ///
    /// [`Self::step`] routes them immediately, which is right wherever the
    /// caller has nothing to write down first. A client proposal is the
    /// exception: the outputs are what say whether the kernel accepted it, and
    /// on a single-node cluster the same outputs can already carry the apply
    /// that pays it. The record has to be written between the step and the
    /// routing, so the two have to be separable.
    pub(crate) fn step_unrouted(&mut self, input: Input) -> Vec<Output> {
        let outputs = match self.node.step(input) {
            Ok(outputs) => outputs,
            Err(error) => {
                eprintln!("runtime step failed: {error}");
                Vec::new()
            }
        };
        self.report_role_transition();
        self.report_lease_transition();
        outputs
    }

    fn report_lease_transition(&mut self) {
        let active = self.node.read_lease_active();
        if active == self.last_reported_lease_active {
            return;
        }
        self.last_reported_lease_active = active;
        eprintln!(
            "rafter-maelstrom lease node={} state={} role={} term={}",
            self.name,
            if active { "active" } else { "inactive" },
            self.node.role(),
            self.node.current_term()
        );
    }

    fn report_role_transition(&mut self) {
        let role = self.node.role();
        if role == self.last_reported_role {
            return;
        }
        self.last_reported_role = role;
        eprintln!(
            "rafter-maelstrom role node={} role={} term={}",
            self.name,
            role,
            self.node.current_term()
        );
    }

    pub(crate) fn handle_outputs(&mut self, outputs: Vec<Output>) {
        for output in outputs {
            match output {
                Output::Send { to, message } => self.send_raft(to, &message),
                Output::Apply { index, payload, .. } => {
                    self.apply_command(index, payload.as_slice());
                }
                Output::ApplySnapshot { snapshot } => self.apply_snapshot(&snapshot),
                Output::ReadIndexGranted {
                    read_id,
                    read_index,
                } => {
                    // Resolve the floor once, here: the highest committed
                    // application entry at or below what the quorum certified.
                    let application_floor =
                        self.node.committed_application_index_through(read_index);
                    let request_id = read_id.0;
                    if let Some(read) = self.pending_reads.get_mut(&request_id) {
                        read.application_floor = Some(application_floor);
                    }
                    self.flush_reads();
                }
                Output::ReadIndexRejected { read_id, reason } => {
                    let request_id = read_id.0;
                    if let Some(read) = self.pending_reads.remove(&request_id) {
                        self.deliver_result(
                            &read.origin,
                            &read.client,
                            read.in_reply_to,
                            ClientResult::Error {
                                code: ERROR_TEMPORARILY_UNAVAILABLE,
                                text: reason.to_string(),
                            },
                        );
                    }
                }
                Output::ReadIndexCanceled { read_id, reason } => {
                    let request_id = read_id.0;
                    if let Some(read) = self.pending_reads.remove(&request_id) {
                        self.deliver_result(
                            &read.origin,
                            &read.client,
                            read.in_reply_to,
                            ClientResult::Error {
                                code: ERROR_TEMPORARILY_UNAVAILABLE,
                                text: format!("{reason:?}"),
                            },
                        );
                    }
                }
                Output::RejectProposal { reason, .. } => eprintln!("proposal rejected: {reason}"),
                Output::LeadershipTransferRejected { target, reason } => {
                    eprintln!("leadership transfer to {target} rejected: {reason}");
                }
                // Logged rather than acted on: this workload addresses peers
                // by Maelstrom node name with no admission control, so there is
                // no peer set to narrow and no identity to retire. The line is
                // for the membership-change workload's own transcript, where the
                // interesting question is which configurations actually
                // committed and in what order.
                Output::ConfigurationCommitted {
                    index,
                    term,
                    previous,
                    configuration,
                } => eprintln!(
                    "configuration committed index={index} term={term} config={} phase={} \
                     voters_before={:?}",
                    configuration.config_id(),
                    configuration.phase(),
                    previous.voter_ids()
                ),
                Output::LocalProposalAppended { .. }
                | Output::LocalProposalDropped { .. }
                | Output::StageSnapshotChunk { .. }
                | Output::SendSnapshotChunk { .. } => {}
            }
        }
    }

    /// Applies one committed command on this node and, if this node owes an
    /// answer for it, mails it to whoever the record names.
    ///
    /// Every replica reaches here for every committed command, with the same
    /// `command.origin` in hand, so the payload alone cannot say who answers —
    /// nor, on the node that does answer, *whom*. `claim_answer_for` is both
    /// decisions; see the `client` module header.
    fn apply_command(&mut self, index: LogIndex, payload: &[u8]) {
        let command: Command = match serde_json::from_slice(payload) {
            Ok(command) => command,
            Err(error) => {
                eprintln!("ignoring invalid committed command: {error}");
                return;
            }
        };
        let outcome = apply_committed_command(&self.root, &mut self.app, index, &command, |root| {
            maybe_crash_after_app_persist_before_reply(root);
            AfterAppPersist::Continue
        });
        let result = match outcome {
            CommandApplyOutcome::Applied(result) => result,
            // The mutation landed and the Raft log makes it durable; only the
            // checkpoint that lets recovery skip replay did not. Refusing to
            // answer would strand a write that every later reader on this node
            // can already see — the failure the `client` module header rejects
            // for reads, and there is no reason writes deserve it either.
            CommandApplyOutcome::AppliedWithoutCheckpoint { result, error } => {
                eprintln!("failed to persist app state: {error}");
                result
            }
            CommandApplyOutcome::AlreadyApplied | CommandApplyOutcome::Interrupted => return,
        };
        if let Some(answer_to) = self.claim_answer_for(&command) {
            self.deliver_result(&answer_to, &command.client, command.in_reply_to, result);
        }
        self.flush_reads();
        self.maybe_compact_snapshot();
    }

    fn send_raft(&mut self, to: NodeId, message: &Message) {
        let frame = match encode_message(message) {
            Ok(frame) => encode_hex(&frame),
            Err(error) => {
                eprintln!("failed to encode raft message: {error}");
                return;
            }
        };
        self.send_to_node(to, json!({ "type": "raft", "frame": frame }));
    }

    /// Remembers which node last spoke here as leader, ignoring a message from
    /// a term this node has already left behind.
    ///
    /// This runs before the step, so `current_term()` is this node's term as
    /// of just before the message is applied. A leader-bearing message from a
    /// strictly older term names a leader the cluster has already replaced;
    /// recording it aims this node's next forward at a node that can only
    /// refuse it. Accepting an equal term is required, not merely allowed —
    /// that is the ordinary case of the current leader's own heartbeats.
    ///
    /// `known_leader` stays a memory even so, and a stale one is still
    /// reachable: this node can hold a leader that was current when it last
    /// heard from it and has since been deposed silently. What bounds the
    /// damage is [`Self::forward_or_reply`] relaying at most once, not this.
    fn observe_leader(&mut self, message: &Message) {
        let announced = match message {
            Message::AppendEntries(request) => Some((request.term, request.leader_id)),
            Message::InstallSnapshot(request) => Some((request.term, request.leader_id)),
            Message::InstallSnapshotChunk(request) => Some((request.term, request.leader_id)),
            Message::TimeoutNow(request) => Some((request.term, request.leader_id)),
            Message::RequestVote(_)
            | Message::RequestVoteResponse(_)
            | Message::PreVote(_)
            | Message::PreVoteResponse(_)
            | Message::AppendEntriesResponse(_)
            | Message::InstallSnapshotResponse(_) => None,
        };
        let Some((term, leader_id)) = announced else {
            return;
        };
        if term < self.node.current_term() {
            return;
        }
        self.known_leader = Some(leader_id);
    }
}
