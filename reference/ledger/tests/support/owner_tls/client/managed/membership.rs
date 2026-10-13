//! Membership receipts and committed role changes through real files and TLS.
use super::*;
use rafter::{JointMembership, MembershipConfig, MembershipSet};
use rafter_app::membership::MembershipChange;
use rafter_reference_ledger::Command;
use rafter_service::durable::DurableMembershipReceipt;
use rafter_storage::{FileRaftNodeStores, RaftHardStateStore, RaftLogSegment};

fn members(voters: &[u64], learners: &[u64]) -> MembershipSet {
    MembershipSet::new(
        voters.iter().copied().map(NodeId).collect(),
        learners.iter().copied().map(NodeId).collect(),
    )
    .unwrap()
}

impl Managed {
    fn membership_commit(
        &mut self,
        leader: usize,
        change: MembershipChange,
        expected: &MembershipConfig,
    ) -> DurableMembershipReceipt {
        let sender = self.nodes[leader].0.sender();
        let receipt = self
            .complete(sender.propose_membership(&link::GROUP, change))
            .unwrap();
        let id = self.nodes[leader].0.ready_node().unwrap().id();
        self.until(|c| {
            c.commits.iter().any(|commit| {
                commit.node == id && commit.index == receipt.index && commit.term == receipt.term
            })
        });
        // Publish the updated commit floor to followers before advancing the plan.
        for _ in 0..10 {
            self.until(|c| c.nodes[leader].0.state() == ServiceState::Ready);
            self.nodes[leader].0.try_step(Input::Tick).unwrap();
        }
        self.until(|c| {
            c.nodes.iter().all(|(driver, _)| {
                driver.state() == ServiceState::Ready
                    && driver.ready_node().is_some_and(|node| {
                        node.committed_membership() == *expected
                            && node.effective_membership() == *expected
                            && c.commits.iter().any(|commit| {
                                commit.node == node.id()
                                    && commit.index == receipt.index
                                    && commit.term == receipt.term
                                    && commit.configuration == *expected
                            })
                    })
            })
        });
        receipt
    }

    fn check_replayed(&self, receipts: &[(DurableMembershipReceipt, MembershipConfig)]) {
        for (driver, _) in &self.nodes {
            let id = driver.handle().metrics().unwrap().current().node_id;
            for (receipt, expected) in receipts {
                assert!(
                    self.commits.iter().any(|commit| {
                        commit.node == id
                            && commit.index == receipt.index
                            && commit.term == receipt.term
                            && commit.configuration == *expected
                    }),
                    "missing recovered membership receipt on {id:?}: {receipt:?}"
                );
            }
        }
    }

    fn membership_write(&mut self, leader: usize, command: &Command) -> LogIndex {
        let sender = self.nodes[leader].0.sender();
        let payload = LedgerStateMachine::new(config())
            .encode_command(command)
            .unwrap();
        self.complete(sender.write(link::GROUP, payload, WriteOptions::default()))
            .unwrap()
            .index
    }

    fn membership_read(&mut self, leader: usize, floor: LogIndex, balance: u128) {
        let sender = self.nodes[leader].0.sender();
        let receipt = self
            .complete(sender.read(
                link::GROUP,
                vec![0],
                ReadConsistency::Linearizable,
                ReadOptions::default().with_min_applied_index(floor),
            ))
            .unwrap();
        let proof = receipt.proof.unwrap();
        assert_eq!(
            proof.issued_by,
            self.nodes[leader].0.ready_node().unwrap().id()
        );
        assert!(proof.required_applied_index >= floor);
        assert!(proof.local_applied_index >= proof.required_applied_index);
        let LedgerQueryResult::Summary(summary) = receipt.result else {
            panic!("expected summary")
        };
        assert_eq!(summary.open_accounts, 1);
        assert_eq!(summary.total_balance, balance);
    }
}

#[test]
fn membership_role_changes_commit_and_recover_through_tls_and_file_stores() {
    let scratch = crate::scratch::ScratchDir::new("membership-roles-tls");
    let all = members(&[1, 2, 3], &[]);
    let learners = members(&[1, 2], &[3]);
    let joint = MembershipConfig::Joint(JointMembership::new(all.clone(), learners.clone()));
    let stable = MembershipConfig::Stable(learners.clone());
    let mut cluster = Managed::open(scratch.path(), true, 0);
    let mut receipts = vec![];
    cluster.membership_write(0, &crate::support::open_session(0, 1));
    cluster.membership_write(
        0,
        &crate::support::execute(
            0,
            1,
            1,
            Mutation::OpenAccount {
                account_id: super::super::super::ACCOUNT,
            },
        ),
    );
    let deposit = |sequence| {
        crate::support::execute(
            0,
            1,
            sequence,
            Mutation::Deposit {
                account_id: super::super::super::ACCOUNT,
                amount: crate::support::amount(7),
            },
        )
    };
    let floor = cluster.membership_write(0, &deposit(2));
    cluster.membership_read(0, floor, 7);
    cluster.converge(0, floor);
    let receipt = cluster.membership_commit(
        0,
        MembershipChange::ChangeVoters {
            target: learners.clone(),
        },
        &joint,
    );
    receipts.push((receipt, joint));
    let receipt = cluster.membership_commit(0, MembershipChange::LeaveJoint, &stable);
    receipts.push((receipt, stable.clone()));
    cluster.close();

    let mut cluster = Managed::open(scratch.path(), false, 1);
    cluster.check_replayed(&receipts);
    assert!(cluster
        .nodes
        .iter()
        .all(|(driver, _)| { driver.ready_node().unwrap().committed_membership() == stable }));
    cluster.membership_read(1, floor, 7);
    let floor = cluster.membership_write(1, &deposit(3));
    cluster.membership_read(1, floor, 14);
    cluster.converge(1, floor);
    cluster.until(|c| c.nodes[1].0.state() == ServiceState::Ready);
    let barrier = cluster.nodes[1]
        .0
        .ready_node()
        .unwrap()
        .promotion_barrier(NodeId(3))
        .unwrap();
    cluster.until(|c| {
        c.nodes[1].0.ready_node().is_some_and(|node| {
            node.leader_replication_progress().iter().any(|progress| {
                progress.follower_id == NodeId(3)
                    && progress.match_index >= barrier.required_match_index
            })
        })
    });
    let joint = MembershipConfig::Joint(JointMembership::new(learners, all.clone()));
    let receipt = cluster.membership_commit(
        1,
        MembershipChange::PromoteLearner {
            node_id: NodeId(3),
            barrier,
        },
        &joint,
    );
    receipts.push((receipt, joint));
    let stable = MembershipConfig::Stable(all);
    let receipt = cluster.membership_commit(1, MembershipChange::LeaveJoint, &stable);
    receipts.push((receipt, stable.clone()));
    cluster.close();

    let mut cluster = Managed::open(scratch.path(), false, 2);
    // Earlier configurations are below the application replay floor.
    cluster.check_replayed(&receipts[2..]);
    assert!(cluster
        .nodes
        .iter()
        .all(|(driver, _)| { driver.ready_node().unwrap().committed_membership() == stable }));
    cluster.membership_read(2, floor, 14);
    cluster.close();
}

#[test]
fn durable_uncommitted_membership_receipt_can_be_replaced_after_cold_reopen() {
    let scratch = crate::scratch::ScratchDir::new("membership-uncommitted-tls");
    let mut cluster = Managed::open(scratch.path(), true, 0);
    // Election needs a quorum, but the later shutdown checks every TLS link.
    // Establish their traffic before deliberately withholding follower turns.
    cluster.until(|c| {
        c.nodes.iter().all(|(_, link)| {
            let diagnostics = link.diagnostics();
            diagnostics.tls_handshakes > 0
                && diagnostics.frames_sent > 0
                && diagnostics.frames_received > 0
        })
    });
    let target = members(&[1, 2], &[3]);
    let mut future = cluster.nodes[0].0.sender().propose_membership(
        &link::GROUP,
        MembershipChange::ChangeVoters {
            target: target.clone(),
        },
    );
    let deadline = Instant::now() + Duration::from_secs(5);
    let receipt = loop {
        if let Poll::Ready(result) = poll(&mut future) {
            break result.unwrap();
        }
        // TLS may send, but followers cannot acknowledge without owner turns.
        let output = cluster.nodes[0].0.poll_next().unwrap();
        assert!(!matches!(
            output,
            ServicePoll::Event(ServiceEvent::Output(
                rafter::Output::ConfigurationCommitted { .. }
            ))
        ));
        assert!(Instant::now() < deadline, "membership receipt stalled");
        std::thread::yield_now();
    };
    assert!(cluster.nodes[0].0.ready_node().unwrap().commit_index() < receipt.index);
    cluster.close();
    {
        let (hard, log, _snapshot) = FileRaftNodeStores::open(scratch.path().join("node-1/raft"))
            .unwrap()
            .into_parts();
        assert!(hard.current().commit_index < receipt.index);
        let entries = log.replay_entries();
        let entry = entries
            .iter()
            .find(|entry| entry.index == receipt.index)
            .unwrap();
        assert_eq!(entry.term, receipt.term);
        assert_eq!(
            entry.configuration_entry().unwrap().membership_config(),
            MembershipConfig::Joint(JointMembership::new(members(&[1, 2, 3], &[]), target))
        );
    }
    let mut cluster = Managed::open(scratch.path(), false, 1);
    // Announce the new leader's commit floor before requiring every follower
    // to observe the replacement. No application command exists yet.
    cluster.converge(1, LogIndex::ZERO);
    let original = MembershipConfig::Stable(members(&[1, 2, 3], &[]));
    cluster.until(|c| {
        c.nodes.iter().all(|(driver, _)| {
            driver.state() == ServiceState::Ready
                && driver.ready_node().is_some_and(|node| {
                    node.commit_index() >= receipt.index
                        && node.term_at_index(receipt.index) != Some(receipt.term)
                        && node.committed_membership() == original
                        && node.effective_membership() == original
                })
        })
    });
    assert!(!cluster
        .commits
        .iter()
        .any(|commit| commit.index == receipt.index && commit.term == receipt.term));
    cluster.membership_write(1, &crate::support::open_session(0, 1));
    let floor = cluster.membership_write(
        1,
        &crate::support::execute(
            0,
            1,
            1,
            Mutation::OpenAccount {
                account_id: super::super::super::ACCOUNT,
            },
        ),
    );
    cluster.membership_read(1, floor, 0);
    cluster.close();
}
