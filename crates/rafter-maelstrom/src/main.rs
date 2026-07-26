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
//! change that; [`answers`] is the ledger of those obligations and the deadline
//! that makes paying them total; [`app`] holds the durability boundary and the
//! crash points that test it. Run it through `scripts/maelstrom-lin-kv*` rather
//! than directly.

use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::path::PathBuf;

use rafter::{Input, LogIndex, MembershipSet, NodeId, Role};
use serde_json::Value;

mod answers;
mod app;
mod client;
mod membership;
mod protocol;
mod raft;
mod raft_node;
mod runtime;

use answers::OwedAnswers;
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
    /// Every client request this node has accepted and not yet answered: who
    /// each answer is addressed to, and the tick by which it goes out.
    ///
    /// This is the record that makes this node the answerer, and it is written
    /// in one place — the funnel every client request passes through — for
    /// every request of every kind, whether this node forwards it, proposes it
    /// or opens a read barrier for it. A forward writes one at both ends, and
    /// either end may turn out to be the only party left who can answer, so
    /// neither may rely on the other. See the `answers` module for why every
    /// record carries a deadline and why acting on a request requires one, and
    /// the `client` module header for who is owed what.
    ///
    /// A request the client sent here directly leaves a second mark, in the
    /// command itself: `origin == self.name`, which every replica applies but
    /// only this node matches. That mark is the one that survives a restart.
    ///
    /// This ledger deliberately does not. Every obligation in it is to somebody
    /// waiting on *this process* — a peer, or a client — and a restart ends
    /// both waits, so a recovered node replaying the entry owes nothing and
    /// stays silent. A proposer field in the payload would survive recovery and
    /// re-mail the answer instead.
    owed_answers: OwedAnswers,
    /// Every request this node has already put an answer on the wire for, by
    /// either arm: a reply to the client or a `client_result` to a peer.
    ///
    /// Written in one place, immediately before the emit, so membership is
    /// exactly "an answer for this request has left this node". That makes the
    /// suppression in `deliver_result` an already-delivered case rather than a
    /// dropped one, which is what lets `flush_reads` retire a waiter without
    /// checking and what stops one request being answered twice.
    ///
    /// # Why this one is not pruned
    ///
    /// Unlike `owed_answers`, nothing takes entries out of this set, and that is
    /// deliberate rather than overlooked. It holds two properties that the
    /// `client` module header calls the at-most-once half: a duplicate delivery
    /// of an already-answered request is refused before it can be proposed a
    /// second time, and a second answer for one request is suppressed. Both are
    /// questions about a request that arrived *late*, so every pruning rule is
    /// an assumption about how late the network can be — and paying for memory
    /// with an unproved bound on delay is the exact shape of reasoning this
    /// reply path has now had to undo four times. Forgetting a request one tick
    /// too early re-applies a `cas` and can roll back another client's committed
    /// write, which is a linearizability failure; keeping it costs bytes.
    ///
    /// What bounds it in practice is the workload, not time. One entry appears
    /// per *distinct* `(client, msg_id)` this node answers: repeats add nothing,
    /// idle ticks add nothing, and a swept deadline adds the one entry for the
    /// request it answered. A Maelstrom run is a fixed op budget — the default
    /// `scripts/maelstrom-lin-kv` is `--rate 100 --time-limit 20`, so about two
    /// thousand client operations across the cluster — and the set dies with the
    /// process, which is also what makes it volatile in the first place.
    /// `reply_tests::obligation` pins that growth law.
    completed_replies: BTreeSet<(String, u64)>,
    /// Ticks of Raft time this process has driven.
    ///
    /// The only clock the harness has below `runtime`, which chooses the
    /// interval, and the one the answer deadlines are measured against.
    ticks: u64,
    /// How long a request this node accepted waits for a committed outcome
    /// before it is answered with an indefinite error anyway.
    answer_deadline_ticks: u64,
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
            Some("read" | "write" | "cas") => self.handle_client(&envelope),
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
