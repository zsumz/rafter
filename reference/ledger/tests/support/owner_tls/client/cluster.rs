//! Cooperatively drive independent owners through the existing TLS transport.
use super::super::{application::Gate, link};
use super::replica::Replica;
use rafter::{Input, LogIndex, Role};
use rafter_service::DriverFuture;
use std::{
    path::Path,
    task::Poll,
    time::{Duration, Instant},
};

pub(super) struct Cluster(pub(super) Vec<Replica>);
impl Cluster {
    pub(super) fn open(root: &Path, fresh: bool, mut gate: Option<Gate>) -> Self {
        let nodes: Vec<_> = (1..=3)
            .map(|id| Replica::open(&root.join(format!("node-{id}")), id, fresh, gate.take()))
            .collect();
        for node in &nodes {
            for other in &nodes {
                if node.id != other.id {
                    link::endpoint(&node.endpoints, other.id, other.link.local_addr());
                }
            }
            node.link.start().unwrap();
        }
        let mut cluster = Self(nodes);
        cluster.until(|c| c.0.iter().all(Replica::ready));
        cluster
    }
    pub(super) fn until(&mut self, mut done: impl FnMut(&Self) -> bool) {
        let deadline = Instant::now() + Duration::from_secs(15);
        while !done(self) {
            for node in &mut self.0 {
                node.pump();
            }
            assert!(Instant::now() < deadline, "managed TLS cluster stalled");
            std::thread::sleep(Duration::from_millis(1));
        }
    }
    pub(super) fn tick(&mut self, leader: usize) {
        self.until(|c| c.0[leader].ready());
        self.0[leader].owner.try_step(Input::Tick).unwrap();
        self.until(|c| c.0[leader].ready());
    }
    pub(super) fn elect(&mut self, leader: usize) {
        for _ in 0..20 {
            if self.0[leader].owner.ready_node().unwrap().role() == Role::Leader {
                break;
            }
            self.tick(leader);
        }
        self.until(|c| {
            c.0[leader].owner.ready_node().is_some_and(|n| {
                n.role() == Role::Leader
                    && n.term_at_index(n.commit_index()) == Some(n.current_term())
            })
        });
    }
    pub(super) fn converge(&mut self, leader: usize, floor: LogIndex) {
        for _ in 0..10 {
            self.tick(leader);
        }
        self.until(|c| {
            c.0.iter()
                .all(|n| n.ready() && n.owner.durable_through() == Some(floor))
        });
    }
    pub(super) fn complete<T>(&mut self, mut future: DriverFuture<T>) -> T {
        let mut result = None;
        self.until(|_| {
            if let Poll::Ready(value) = super::poll(&mut future) {
                result = Some(value);
                true
            } else {
                false
            }
        });
        result.unwrap()
    }
    pub(super) fn close(self) {
        for node in self.0 {
            node.close();
        }
    }
}
