//! Sustained bounded client windows over real stores, application journals and TLS.
use super::*;
use rafter_app::state_machine::ApplyResult;
use rafter_reference_ledger::{ApplyDisposition, ApplyOutcome, LedgerSummary};
use rafter_service::{DriverFuture, QueryReceipt, WriteReceipt};
use std::collections::BTreeMap;

type Write = DriverFuture<Result<WriteReceipt<ApplyResult<ApplyOutcome>>, WriteError>>;
type Read = DriverFuture<Result<QueryReceipt<u64, LedgerQueryResult>, ReadError>>;
const REQUESTS: usize = 128;
const WINDOW: usize = 2;

#[derive(Default)]
struct Load {
    writes: Vec<(u128, Write)>,
    reads: Vec<(bool, LogIndex, Read)>,
    issued_writes: usize,
    issued_reads: usize,
    completed_reads: usize,
    history: BTreeMap<LogIndex, u128>,
    observations: Vec<(LogIndex, u128)>,
    writes_without_read: usize,
    reads_without_write: usize,
    overlapping_turns: usize,
}
impl Load {
    fn collect(&mut self) {
        self.writes.retain_mut(|(balance, future)| {
            let Poll::Ready(result) = poll(future) else {
                return true;
            };
            let receipt = result.unwrap();
            assert_eq!(receipt.result.index, receipt.index);
            assert_eq!(receipt.result.term, receipt.term);
            assert_eq!(receipt.result.result.disposition, ApplyDisposition::Applied);
            assert!(self.history.insert(receipt.index, *balance).is_none());
            self.writes_without_read += 1;
            self.reads_without_write = 0;
            false
        });
        self.reads.retain_mut(|(linearizable, minimum, future)| {
            let Poll::Ready(result) = poll(future) else {
                return true;
            };
            let receipt = result.unwrap();
            let LedgerQueryResult::Summary(summary) = receipt.result else {
                panic!("expected ledger summary");
            };
            assert_eq!(summary.open_accounts, 1);
            assert_eq!(summary.successful_deposits, summary.total_balance);
            let minimum_balance = *self.history.range(..=*minimum).next_back().unwrap().1;
            assert!(summary.total_balance >= minimum_balance);
            assert!(summary.total_balance <= REQUESTS as u128);
            if *linearizable {
                let proof = receipt.proof.unwrap();
                assert_eq!(proof.group_id, link::GROUP);
                assert_eq!(proof.issued_by, NodeId(1));
                assert!(proof.required_applied_index >= *minimum);
                assert!(proof.local_applied_index >= proof.required_applied_index);
                self.observations
                    .push((proof.local_applied_index, summary.total_balance));
            } else {
                assert!(receipt.proof.is_none());
            }
            self.completed_reads += 1;
            self.reads_without_write += 1;
            self.writes_without_read = 0;
            false
        });
        if self.completed_reads < REQUESTS && self.history.len() <= REQUESTS {
            assert!(self.writes_without_read < 64, "TLS read stream starved");
            assert!(self.reads_without_write < 64, "TLS write stream starved");
        }
    }
    fn replenish(&mut self, driver: &Driver) {
        let sender = driver.sender();
        while self.writes.len() < WINDOW && self.issued_writes < REQUESTS {
            let sequence = u64::try_from(self.issued_writes + 2).unwrap();
            self.writes.push((
                (self.issued_writes + 1) as u128,
                sender.write(link::GROUP, deposit(sequence), WriteOptions::default()),
            ));
            self.issued_writes += 1;
        }
        while self.reads.len() < WINDOW && self.issued_reads < REQUESTS {
            let linearizable = self.issued_reads % 2 == 0;
            let minimum = *self.history.last_key_value().unwrap().0;
            self.reads.push((
                linearizable,
                minimum,
                sender.read(
                    link::GROUP,
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
        if self.writes.len() == WINDOW && self.reads.len() == WINDOW {
            self.overlapping_turns += 1;
            // This extra request must be refused before it can mutate state.
            assert!(matches!(
                poll(&mut sender.write(link::GROUP, deposit(999), WriteOptions::default())),
                Poll::Ready(Err(WriteError::Backpressure))
            ));
            assert!(matches!(
                poll(&mut sender.read(
                    link::GROUP,
                    vec![0],
                    ReadConsistency::Linearizable,
                    ReadOptions::default(),
                )),
                Poll::Ready(Err(ReadError::Backpressure))
            ));
        }
    }
    fn done(&self) -> bool {
        self.history.len() == REQUESTS + 1 && self.completed_reads == REQUESTS
    }
}

fn deposit(sequence: u64) -> Vec<u8> {
    LedgerStateMachine::new(config())
        .encode_command(&crate::support::execute(
            0,
            1,
            sequence,
            Mutation::Deposit {
                account_id: super::super::super::ACCOUNT,
                amount: crate::support::amount(1),
            },
        ))
        .unwrap()
}

#[test]
fn sustained_tls_windows_preserve_receipts_quorum_views_and_reopened_state() {
    let scratch = crate::scratch::ScratchDir::new("managed-mixed-tls");
    let mut cluster = Managed::open(scratch.path(), true, 0);
    let sender = cluster.nodes[0].0.sender();
    let mut floor = LogIndex::ZERO;
    for command in [
        crate::support::open_session(0, 1),
        crate::support::execute(
            0,
            1,
            1,
            Mutation::OpenAccount {
                account_id: super::super::super::ACCOUNT,
            },
        ),
    ] {
        let payload = LedgerStateMachine::new(config())
            .encode_command(&command)
            .unwrap();
        floor = cluster
            .complete(sender.write(link::GROUP, payload, WriteOptions::default()))
            .unwrap()
            .index;
    }
    let mut load = Load::default();
    load.history.insert(floor, 0);
    let deadline = Instant::now() + Duration::from_secs(60);
    while !load.done() {
        load.collect();
        load.replenish(&cluster.nodes[0].0);
        // Replenish before each owner turn so both client streams stay offered
        // while file I/O, application workers and TLS make independent progress.
        cluster.turn(1);
        assert!(Instant::now() < deadline, "mixed TLS workload stalled");
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(load.overlapping_turns >= REQUESTS);
    assert_eq!(load.observations.len(), REQUESTS / 2);
    for (observed_at, balance) in &load.observations {
        let expected = load.history.range(..=*observed_at).next_back().unwrap().1;
        assert_eq!(balance, expected, "TLS query differs from its durable view");
    }
    floor = *load.history.last_key_value().unwrap().0;
    cluster.converge(0, floor);
    let metrics = cluster.nodes[0].0.handle().metrics().unwrap().current();
    assert_eq!(metrics.pending_proposals, 0);
    assert_eq!(metrics.reserved_reads, 0);
    cluster.compact_all(floor);
    cluster.close();

    let mut cluster = Managed::open(scratch.path(), false, 1);
    let sender = cluster.nodes[1].0.sender();
    let last = u64::try_from(REQUESTS + 1).unwrap();
    let receipt = cluster
        .complete(sender.write(link::GROUP, deposit(last), WriteOptions::default()))
        .unwrap();
    assert_eq!(
        receipt.result.result.disposition,
        ApplyDisposition::Replayed
    );
    let queried = cluster
        .complete(sender.read(
            link::GROUP,
            vec![0],
            ReadConsistency::Linearizable,
            ReadOptions::default().with_min_applied_index(receipt.index),
        ))
        .unwrap();
    assert_eq!(
        queried.result,
        LedgerQueryResult::Summary(LedgerSummary {
            open_accounts: 1,
            total_balance: REQUESTS as u128,
            successful_deposits: REQUESTS as u128,
        })
    );
    let proof = queried.proof.unwrap();
    assert_eq!(proof.group_id, link::GROUP);
    assert_eq!(proof.issued_by, NodeId(2));
    assert!(proof.required_applied_index >= receipt.index);
    assert!(proof.local_applied_index >= proof.required_applied_index);
    cluster.converge(1, receipt.index);
    cluster.close();
}
