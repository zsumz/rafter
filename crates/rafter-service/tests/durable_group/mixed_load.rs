//! Continuously replenished client windows must both make useful progress.
use super::support::*;
use crate::write_support::Outcome;
use rafter_service::{DriverFuture, QueryReceipt, WriteReceipt};
use std::{collections::BTreeMap, time::Instant};

type Write = DriverFuture<Result<WriteReceipt<Outcome>, WriteError>>;
type Read = DriverFuture<Result<QueryReceipt<u64, u8>, ReadError>>;

#[derive(Default)]
struct Load {
    writes: Vec<(u8, Write)>,
    reads: Vec<(bool, LogIndex, Read)>,
    issued_writes: usize,
    issued_reads: usize,
    completed_reads: usize,
    history: BTreeMap<LogIndex, u8>,
    observations: Vec<(LogIndex, u8)>,
    writes_without_read: usize,
    reads_without_write: usize,
}
impl Load {
    const REQUESTS: usize = 128;
    const WINDOW: usize = 4;

    fn collect(&mut self) {
        self.writes.retain_mut(|(value, write)| {
            let Poll::Ready(result) = poll(write) else {
                return true;
            };
            let receipt = result.unwrap();
            assert_eq!(receipt.result.value, *value);
            assert!(self.history.insert(receipt.index, *value).is_none());
            self.writes_without_read += 1;
            self.reads_without_write = 0;
            false
        });
        self.reads.retain_mut(|(linearizable, minimum, read)| {
            let Poll::Ready(result) = poll(read) else {
                return true;
            };
            let receipt = result.unwrap();
            if *linearizable {
                let proof = receipt.proof.unwrap();
                assert_eq!(proof.group_id, GROUP);
                assert_eq!(proof.issued_by, NodeId(1));
                assert!(proof.required_applied_index >= *minimum);
                assert!(proof.local_applied_index >= proof.required_applied_index);
                self.observations
                    .push((proof.local_applied_index, receipt.result));
            } else {
                assert!(receipt.proof.is_none());
            }
            self.completed_reads += 1;
            self.reads_without_write += 1;
            self.writes_without_read = 0;
            false
        });
        // Use useful completions instead of a latency threshold. While both
        // streams remain offered, neither may monopolize repeated full windows.
        if self.completed_reads < Self::REQUESTS && self.history.len() < Self::REQUESTS {
            assert!(self.writes_without_read < 64, "read stream starved");
            assert!(self.reads_without_write < 64, "write stream starved");
        }
    }

    fn replenish(&mut self, node: &Replica) {
        let sender = node.driver.sender();
        while self.writes.len() < Self::WINDOW && self.issued_writes < Self::REQUESTS {
            let value = u8::try_from(self.issued_writes + 1).unwrap();
            self.writes.push((
                value,
                sender.write(GROUP, vec![value], WriteOptions::default()),
            ));
            self.issued_writes += 1;
        }
        while self.reads.len() < Self::WINDOW && self.issued_reads < Self::REQUESTS {
            let linearizable = self.issued_reads % 2 == 0;
            let minimum = self
                .history
                .last_key_value()
                .map_or(LogIndex::ZERO, |(i, _)| *i);
            self.reads.push((
                linearizable,
                minimum,
                sender.read(
                    GROUP,
                    vec![0],
                    if linearizable {
                        ReadConsistency::Linearizable
                    } else {
                        ReadConsistency::Local
                    },
                    ReadOptions::default().with_min_applied_index(minimum),
                ),
            ));
            self.issued_reads += 1;
        }
        assert!(node.writes.retained().writes <= Self::WINDOW);
        assert!(node.reads.retained().reads <= Self::WINDOW);
        assert!(node.writes.retained().payload_bytes <= 64);
        assert!(node.reads.retained().payload_bytes <= 64);
    }

    fn complete(&self) -> bool {
        self.history.len() == Self::REQUESTS && self.completed_reads == Self::REQUESTS
    }
}

#[test]
fn sustained_mixed_windows_preserve_progress_bounds_and_quorum_views() {
    // A proposal turn cannot consume the whole client window: both queues
    // remain backlogged, instead of taking turns because a batch emptied one.
    let mut options = rafter_runtime::service::ServiceOptions::new();
    options.max_proposals = 1;
    let mut cluster = Cluster::attach(crate::reads_support::Cluster::with_options(
        4, false, options,
    ));
    let mut load = Load::default();
    let deadline = Instant::now() + std::time::Duration::from_secs(10);
    while !load.complete() {
        load.collect();
        load.replenish(&cluster.nodes[0]);
        cluster.turn();
        assert!(Instant::now() < deadline, "mixed client streams stalled");
        std::thread::yield_now();
    }
    assert_eq!(load.observations.len(), Load::REQUESTS / 2);
    for (floor, value) in load.observations {
        let expected = if floor == LogIndex::ZERO {
            0
        } else {
            load.history[&floor]
        };
        assert_eq!(
            value, expected,
            "query did not observe its reported durable view"
        );
    }
    assert_eq!(cluster.nodes[0].writes.retained().writes, 0);
    assert_eq!(cluster.nodes[0].reads.retained().reads, 0);
    let last = *load.history.last_key_value().unwrap().0;
    cluster.until(|c| {
        c.nodes
            .iter()
            .all(|n| n.driver.durable_through() == Some(last))
    });
}
