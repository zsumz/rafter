//! Same-process replicas with real files, application workers, and TLS sockets.
//! This is public API/package qualification, not OS-process fault isolation.
mod application;
mod link;
mod node;
mod snapshot;
pub use application::gate;
use application::Gate;
use node::Replica;
use rafter::{Input, LocalProposalId, LogIndex, ReadId, Role};
use rafter_app::state_machine::ReplicatedStateMachine;
use rafter_reference_ledger::{
    AccountId, Command, LedgerConfig, LedgerQueryResult, LedgerStateMachine,
};
pub use snapshot::seed_genesis;
use std::{
    path::Path,
    time::{Duration, Instant},
};
pub fn config() -> LedgerConfig {
    crate::support::config(1, 8192)
}
pub const ACCOUNT: AccountId = AccountId::new(19);
#[derive(Debug)]
pub struct Cluster {
    pub nodes: Vec<Replica>,
}
impl Cluster {
    pub fn open(root: &Path, fresh: bool, gate: Option<Gate>) -> Self {
        Self::open_count(root, fresh, gate, 3)
    }
    pub fn reopen_majority(root: &Path) -> Self {
        Self::open_count(root, false, None, 2)
    }
    fn open_count(root: &Path, fresh: bool, mut gate: Option<Gate>, count: u64) -> Self {
        let nodes: Vec<_> = (1..=count)
            .map(|id| Replica::open(&root.join(format!("node-{id}")), id, fresh, gate.take()))
            .collect();
        for node in &nodes {
            for other in &nodes {
                if node.id != other.id {
                    link::endpoint(&node.endpoints, other.id, other.link.local_addr());
                }
            }
        }
        for node in &nodes {
            node.link.start().unwrap();
        }
        let mut cluster = Self { nodes };
        cluster.until(|c| c.nodes.iter().all(Replica::ready));
        cluster
    }
    pub fn stop_last(&mut self) {
        assert_eq!(self.nodes.len(), 3);
        let node = self.nodes.pop().unwrap();
        assert_eq!(node.id, 3);
        node.close();
    }
    pub fn restart_last(&mut self, root: &Path) {
        assert_eq!(self.nodes.len(), 2);
        let mut node = Replica::open(&root.join("node-3"), 3, false, None);
        for other in &self.nodes {
            link::endpoint(&other.endpoints, node.id, node.link.local_addr());
            link::endpoint(&node.endpoints, other.id, other.link.local_addr());
        }
        // Finish local recovery before activating TLS; callers can inspect the
        // recovered partial transfer before any new frame reaches the owner.
        let deadline = Instant::now() + Duration::from_secs(15);
        while !node.ready() {
            node.pump();
            assert!(Instant::now() < deadline);
            std::thread::sleep(Duration::from_millis(1));
        }
        node.link.start().unwrap();
        self.nodes.push(node);
    }
    pub fn until(&mut self, mut done: impl FnMut(&Self) -> bool) {
        let deadline = Instant::now() + Duration::from_secs(15);
        while !done(self) {
            for node in &mut self.nodes {
                node.pump();
            }
            assert!(
                Instant::now() < deadline,
                "TLS owner cluster stalled: {:?}",
                self.nodes
                    .iter()
                    .map(|n| (
                        n.id,
                        n.owner.state(),
                        n.owner.durable_through(),
                        n.owner.ready_node().map(|r| (r.role(), r.commit_index())),
                        n.link.diagnostics()
                    ))
                    .collect::<Vec<_>>()
            );
            std::thread::sleep(Duration::from_millis(1));
        }
    }
    pub fn elect(&mut self, index: usize) {
        for _ in 0..20 {
            self.until(|c| c.nodes[index].ready());
            if self.nodes[index].owner.ready_node().unwrap().role() == Role::Leader {
                break;
            }
            self.nodes[index].input(Input::Tick);
            self.until(|c| c.nodes[index].ready());
        }
        self.until(|c| {
            c.nodes[index].ready()
                && c.nodes[index].owner.ready_node().is_some_and(|node| {
                    node.role() == Role::Leader
                        && node.term_at_index(node.commit_index()) == Some(node.current_term())
                })
        });
    }
    pub fn propose(&mut self, index: usize, id: u64, command: &Command) {
        self.until(|c| c.nodes[index].ready());
        self.nodes[index].input(Input::TrackedClientProposal {
            proposal_id: LocalProposalId(id),
            payload: LedgerStateMachine::new(config())
                .encode_command(command)
                .unwrap(),
        });
    }
    pub fn write(&mut self, index: usize, id: u64, command: &Command) {
        self.propose(index, id, command);
        self.until(|c| {
            c.nodes[index].writes.contains(&LocalProposalId(id)) && c.nodes[index].ready()
        });
        let floor = self.nodes[index].owner.durable_through().unwrap();
        self.converge(index, floor);
    }
    pub fn converge(&mut self, leader: usize, floor: LogIndex) {
        // Advance only the chosen leader's logical clock. Followers still
        // receive real authenticated frames, including commit notifications.
        for _ in 0..10 {
            self.until(|c| c.nodes[leader].ready());
            self.nodes[leader].input(Input::Tick);
        }
        self.until(|c| {
            c.nodes
                .iter()
                .all(|n| n.ready() && n.owner.durable_through() == Some(floor))
        });
    }
    pub fn read(&mut self, leader: usize, id: u64, balance: Option<u64>) {
        self.until(|c| c.nodes[leader].ready());
        self.nodes[leader].request_read(ReadId(id));
        self.until(|c| c.nodes[leader].read.is_some());
        assert_eq!(
            self.nodes[leader].read,
            Some((
                ReadId(id),
                LedgerQueryResult::Account {
                    account_id: ACCOUNT,
                    balance
                }
            ))
        );
    }
    pub fn close(self) {
        for node in self.nodes {
            let diagnostics = node.link.diagnostics();
            assert!(
                diagnostics.tls_handshakes > 0
                    && diagnostics.frames_received > 0
                    && diagnostics.frames_sent > 0
            );
            assert_eq!(diagnostics.identity_mismatches, 0);
            assert_eq!(diagnostics.unknown_certificates, 0);
            assert_eq!(diagnostics.session_store_failures, 0);
            node.close();
        }
    }
}
