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
    read_index: LogIndex,
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
    completed_replies: BTreeSet<(String, u64)>,
    next_msg_id: u64,
    next_read_id: u64,
    snapshot_every: u64,
    last_snapshot_index: LogIndex,
    last_reported_role: Role,
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

    fn emit(&self, dest: &str, body: Value) {
        let envelope = Envelope {
            src: self.name.clone(),
            dest: dest.to_string(),
            body,
        };
        println!(
            "{}",
            serde_json::to_string(&envelope).expect("envelope serializes")
        );
        std::io::stdout().flush().expect("flush Maelstrom message");
    }
}
