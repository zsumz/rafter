//! A Maelstrom node that runs Rafter as a real process under an external
//! linearizability checker.
//!
//! This is the adversarial half of Rafter's verification. The model checker
//! proves the kernel's transitions in isolation; this binary proves that a
//! *process* built on the kernel — with a durable log, a real application
//! checkpoint, restarts, partitions, and a hostile client workload — is
//! linearizable when an outside judge decides. Nothing here is a library, and
//! nothing here should be imitated for its shape: the harness cuts corners a
//! deployment must not, and the reason each corner is safe to cut is written at
//! the place it is cut.
//!
//! The obligations the harness exists to hold are stated where they live:
//! [`client`] argues who owes a client an answer and why a node's role does not
//! change that; [`app`] holds the durability boundary and the crash points that
//! test it. Run it through `scripts/maelstrom-lin-kv*` rather than directly.

use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::path::PathBuf;

use rafter::{Input, LogIndex, MembershipSet, NodeId, Role};
use serde_json::Value;

mod app;
mod client;
mod membership;
mod protocol;
mod raft;
mod raft_node;
mod runtime;

use app::AppState;
use membership::{membership_drive_action, MembershipDriveAction, MembershipPlan};
use protocol::{body_type, Envelope};
use raft_node::FileNode;

#[derive(Clone, Debug)]
struct PendingRead {
    origin: String,
    client: String,
    in_reply_to: u64,
    key: Value,
    /// The applied index this read waits for: the highest committed
    /// application entry at or below the granted read index, resolved once
    /// when the barrier is granted. `None` until then.
    ///
    /// Not the read index itself. A barrier grants at the leader's commit
    /// index, and after an election the entry there is that leader's `Noop`,
    /// which never reaches the application — so waiting for the read index
    /// waits forever on a read-only tail.
    application_floor: Option<LogIndex>,
}

#[derive(Debug)]
struct InitializedNode {
    name: String,
    node: FileNode,
    root: PathBuf,
    app: AppState,
    name_to_id: BTreeMap<String, NodeId>,
    id_to_name: BTreeMap<NodeId, String>,
    membership_plan: MembershipPlan,
    membership_target: Option<MembershipSet>,
    membership_reported_complete: bool,
    known_leader: Option<NodeId>,
    pending_reads: BTreeMap<u64, PendingRead>,
    /// Client requests a peer forwarded here that this node proposed on that
    /// peer's behalf and still owes it an answer for, keyed the way
    /// `completed_replies` is: `(client, in_reply_to)`.
    ///
    /// This is the record that makes this node the answerer for a committed
    /// write. A request the client sent here directly needs no record — the
    /// command carries `origin`, and `origin == self.name` says it. A forward
    /// leaves no such mark: `origin` names the peer, and reads identically on
    /// the node that accepted the forward and on every node that merely
    /// replicated the entry. Without this set those two are indistinguishable
    /// at apply. See the `client` module header for both rules.
    ///
    /// Deliberately volatile, and not part of the command payload. The
    /// obligation is to a peer waiting on this process; a restart ends that
    /// wait, so a recovered node replaying the entry owes nothing and must stay
    /// silent. A `origin`-side field in the payload would survive recovery and
    /// re-mail the answer instead.
    ///
    /// An entry is consumed by the apply that pays it. One survives only when
    /// this node never applies that entry — a snapshot install jumped the
    /// applied index past it — and then the answer was never this node's to
    /// give: the forwarding peer applies the same entry with its own name as
    /// `origin` and answers its client directly. The residue is bounded by
    /// request volume, as `completed_replies` already is.
    pending_forwards: BTreeSet<(String, u64)>,
    completed_replies: BTreeSet<(String, u64)>,
    next_msg_id: u64,
    next_read_id: u64,
    snapshot_every: u64,
    last_snapshot_index: LogIndex,
    last_reported_role: Role,
    last_reported_lease_active: bool,
    /// Test-only tap on everything this node put on the wire.
    ///
    /// The reply path's whole job is to make an answer leave the node, so a
    /// test that cannot see the wire cannot tell a read that was answered from
    /// one that was consumed in silence.
    #[cfg(test)]
    emitted: Vec<Envelope>,
}

#[derive(Debug, Default)]
struct MaelstromNode {
    initialized: Option<InitializedNode>,
}

fn main() {
    if let Err(error) = runtime::run() {
        eprintln!("rafter-maelstrom failed: {error}");
        std::process::exit(1);
    }
}

impl InitializedNode {
    fn handle_envelope(&mut self, envelope: Envelope) {
        match body_type(&envelope.body) {
            Some("raft") => self.handle_raft(&envelope),
            Some("client_forward") => self.handle_forward(envelope),
            Some("client_result") => self.handle_client_result(&envelope),
            Some("read" | "write" | "cas") => self.handle_client(envelope),
            Some(other) => eprintln!("ignoring unsupported Maelstrom message type {other:?}"),
            None => eprintln!("ignoring Maelstrom message without body.type"),
        }
    }

    fn drive_membership(&mut self) {
        let Some(target) = self.membership_target.clone() else {
            return;
        };
        if self.node.role() != Role::Leader {
            return;
        }

        let effective = self.node.effective_membership();
        let committed = self.node.committed_membership();
        match membership_drive_action(&effective, &committed, &target) {
            MembershipDriveAction::EnterJoint => {
                eprintln!(
                    "rafter-maelstrom membership plan={:?} action=enter-joint target={:?}",
                    self.membership_plan, target
                );
                self.step(Input::change_membership(target, Vec::new()));
            }
            MembershipDriveAction::LeaveJoint => {
                eprintln!(
                    "rafter-maelstrom membership plan={:?} action=leave-joint target={:?}",
                    self.membership_plan, target
                );
                self.step(Input::change_membership(target, Vec::new()));
            }
            MembershipDriveAction::Complete => {
                if !self.membership_reported_complete {
                    self.membership_reported_complete = true;
                    eprintln!(
                        "rafter-maelstrom membership plan={:?} complete target={:?}",
                        self.membership_plan, target
                    );
                }
            }
            MembershipDriveAction::Wait => {}
        }
    }

    fn send_to_node(&mut self, to: NodeId, body: Value) {
        if let Some(dest) = self.id_to_name.get(&to).cloned() {
            self.emit(&dest, body);
        }
    }

    fn emit(&mut self, dest: &str, body: Value) {
        let envelope = Envelope {
            src: self.name.clone(),
            dest: dest.to_string(),
            body,
        };
        #[cfg(test)]
        self.emitted.push(envelope.clone());
        println!(
            "{}",
            serde_json::to_string(&envelope).expect("envelope serializes")
        );
        std::io::stdout().flush().expect("flush Maelstrom message");
    }
}
