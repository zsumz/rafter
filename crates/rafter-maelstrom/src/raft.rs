use rafter::{Input, LogIndex, Message, NodeId, Output};
use rafter_codec::{decode_message, encode_message};
use serde_json::{json, Value};

use crate::{
    app::{
        apply_committed_command, maybe_crash_after_app_persist_before_reply, AfterAppPersist,
        ClientResult, Command, CommandApplyOutcome, ERROR_TEMPORARILY_UNAVAILABLE,
    },
    protocol::{decode_hex, encode_hex, Envelope},
    InitializedNode,
};

#[cfg(test)]
mod read_tests;
#[cfg(test)]
mod reply_tests;
pub(crate) mod snapshots;

impl InitializedNode {
    pub(crate) fn handle_raft(&mut self, envelope: &Envelope) {
        let Some(from) = self.name_to_id.get(&envelope.src).copied() else {
            eprintln!("ignoring raft message from unknown node {}", envelope.src);
            return;
        };
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
        let outputs = match self.node.step(input) {
            Ok(outputs) => outputs,
            Err(error) => {
                eprintln!("runtime step failed: {error}");
                return;
            }
        };
        self.report_role_transition();
        self.report_lease_transition();
        self.handle_outputs(outputs);
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
                Output::LocalProposalAppended { .. }
                | Output::LocalProposalDropped { .. }
                | Output::StageSnapshotChunk { .. }
                | Output::SendSnapshotChunk { .. } => {}
            }
        }
    }

    /// Applies one committed command on this node and, if this node is the one
    /// that owes the client an answer, mails it.
    ///
    /// Every replica reaches here for every committed command, with the same
    /// `command.origin` in hand, so the payload alone cannot say who answers.
    /// `claim_answer_for` is that decision; see the `client` module header.
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
            Ok(CommandApplyOutcome::Applied(result)) => result,
            Ok(CommandApplyOutcome::AlreadyApplied | CommandApplyOutcome::Interrupted) => return,
            Err(error) => {
                eprintln!("failed to persist app state: {error}");
                return;
            }
        };
        if self.claim_answer_for(&command) {
            self.deliver_result(
                &command.origin,
                &command.client,
                command.in_reply_to,
                result,
            );
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

    fn observe_leader(&mut self, message: &Message) {
        match message {
            Message::AppendEntries(request) => self.known_leader = Some(request.leader_id),
            Message::InstallSnapshot(request) => self.known_leader = Some(request.leader_id),
            Message::InstallSnapshotChunk(request) => self.known_leader = Some(request.leader_id),
            Message::TimeoutNow(request) => self.known_leader = Some(request.leader_id),
            Message::RequestVote(_)
            | Message::RequestVoteResponse(_)
            | Message::PreVote(_)
            | Message::PreVoteResponse(_)
            | Message::AppendEntriesResponse(_)
            | Message::InstallSnapshotResponse(_) => {}
        }
    }
}
