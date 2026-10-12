//! Repeated compaction must preserve the same owners and their client handle.
use super::*;

pub(super) fn run_cycles(cycles: usize) {
    assert!(cycles > 0);
    let scratch = crate::scratch::ScratchDir::new("managed-continuous-tls");
    let mut cluster = Managed::open(scratch.path(), true, 0);
    let original_sender = cluster.nodes[0].0.sender();
    let mut floor = initialize(&mut cluster);
    let mut expected_refusals = 0;
    for cycle in 0..cycles {
        let base_balance = cycle * REQUESTS;
        let mut load = Load {
            base_balance,
            ..Load::default()
        };
        load.history.insert(floor, base_balance as u128);
        floor = load.run(&mut cluster);
        expected_refusals += load.overlapping_turns;
        cluster.converge(0, floor);
        // Maintenance runs between bounded client windows; no owner is closed
        // or recreated until every cycle has served and compacted its work.
        cluster.compact_all(floor);
        for (driver, _) in &cluster.nodes {
            let node = driver.ready_node().unwrap();
            assert_eq!(node.snapshot_index(), floor);
            assert_eq!(node.last_log_index(), floor);
            assert!(node.log_entries_slice_from(LogIndex(1)).is_empty());
            Load::default().check_retained(driver);
        }
        let balance = base_balance + REQUESTS;
        let queried = cluster
            .complete(original_sender.read(
                link::GROUP,
                vec![0],
                ReadConsistency::Linearizable,
                ReadOptions::default().with_min_applied_index(floor),
            ))
            .unwrap();
        assert_eq!(
            queried.result,
            LedgerQueryResult::Summary(LedgerSummary {
                open_accounts: 1,
                total_balance: balance as u128,
                successful_deposits: balance as u128,
            })
        );
        let proof = queried.proof.unwrap();
        assert_eq!(proof.group_id, link::GROUP);
        assert_eq!(proof.issued_by, NodeId(1));
        assert!(proof.required_applied_index >= floor);
        assert!(proof.local_applied_index >= proof.required_applied_index);
        let refusals = original_sender.backpressure_refusals();
        assert_eq!(refusals.writes, expected_refusals);
        assert_eq!(refusals.reads, Some(expected_refusals));
        assert_eq!(refusals.controls, 0);
        Load::default().check_retained(&cluster.nodes[0].0);
        eprintln!(
            "continuous owner TLS cycle {}/{} complete: balance={}, retained_entries=0",
            cycle + 1,
            cycles,
            balance,
        );
    }
    cluster.close();

    let balance = cycles * REQUESTS;
    let mut recovered = Managed::open(scratch.path(), false, 1);
    verify_reopened(&mut recovered, 1, balance);
    let fresh = recovered
        .complete(recovered.nodes[1].0.sender().write(
            link::GROUP,
            deposit(u64::try_from(balance + 2).unwrap()),
            WriteOptions::default(),
        ))
        .unwrap();
    assert_eq!(fresh.result.result.disposition, ApplyDisposition::Applied);
    verify_reopened(&mut recovered, 1, balance + 1);
    recovered.close();
    eprintln!(
        "continuous owner TLS cold recovery complete: balance={}, next_balance={}",
        balance,
        balance + 1,
    );
}

#[test]
fn repeated_compaction_preserves_live_owners_and_the_original_client_handle() {
    run_cycles(3);
}
