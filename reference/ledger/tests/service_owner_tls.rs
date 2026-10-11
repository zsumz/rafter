//! The asynchronous durable owner composed with the existing production TLS
//! transport through public APIs, in source and extracted-package lanes.
#[path = "support/owner_tls/mod.rs"]
mod owner_tls;
#[path = "support/scratch.rs"]
mod scratch;
#[path = "support/snapshot_source.rs"]
mod snapshot_source;
#[allow(dead_code)]
mod support;
use owner_tls::{gate, Cluster, ACCOUNT};
use rafter::{LocalProposalId, LogIndex, ReadId};
use rafter_reference_ledger::{LedgerQueryResult, Mutation};
use support::{amount, execute, open_session};

#[test]
fn tls_quorum_reads_wait_for_application_durability_and_survive_cold_reopen() {
    let scratch = scratch::ScratchDir::new("owner-tls");
    let (pause, entered, release) = gate();
    let mut cluster = Cluster::open(scratch.path(), true, Some(pause));
    // Drop this guard before the cluster if an assertion unwinds while the
    // worker is paused; owner destruction joins its application thread.
    let release = release;
    cluster.elect(0);
    cluster.propose(0, 1, &open_session(0, 1));
    cluster.until(|c| c.nodes[0].ready() && c.nodes[0].owner.dispatched_through() == LogIndex(2));
    entered
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    assert!(cluster.nodes[0].writes.is_empty());
    assert_eq!(
        cluster.nodes[0].owner.durable_through(),
        Some(LogIndex::ZERO)
    );
    cluster.nodes[0].request_read(ReadId(1));
    cluster.until(|c| c.nodes[0].blocked_read_polls > 0);
    assert!(cluster.nodes[0].writes.is_empty());
    assert!(cluster.nodes[0].read.is_none());
    release.now();
    cluster.until(|c| c.nodes[0].writes.contains(&LocalProposalId(1)) && c.nodes[0].read.is_some());
    assert_eq!(
        cluster.nodes[0].read,
        Some((
            ReadId(1),
            LedgerQueryResult::Account {
                account_id: ACCOUNT,
                balance: None
            }
        ))
    );
    cluster.write(
        0,
        2,
        &execute(
            0,
            1,
            1,
            Mutation::OpenAccount {
                account_id: ACCOUNT,
            },
        ),
    );
    cluster.write(
        0,
        3,
        &execute(
            0,
            1,
            2,
            Mutation::Deposit {
                account_id: ACCOUNT,
                amount: amount(9),
            },
        ),
    );
    cluster.read(0, 2, Some(9));
    cluster.close();

    // Reopen the exact journals, Raft files, and connection-session stores.
    // Volatile proposal/read identities are deliberately new process state.
    let mut cluster = Cluster::open(scratch.path(), false, None);
    cluster.elect(1);
    cluster.read(1, 1, Some(9));
    assert_eq!(cluster.nodes[1].owner.durable_through(), Some(LogIndex(4)));
    assert!(cluster.nodes[1].owner.ready_node().unwrap().commit_index() > LogIndex(4));
    cluster.write(
        1,
        1,
        &execute(
            0,
            1,
            2,
            Mutation::Deposit {
                account_id: ACCOUNT,
                amount: amount(9),
            },
        ),
    );
    cluster.read(1, 2, Some(9));
    cluster.close();
}

#[test]
fn tls_snapshot_resumes_a_durable_partial_transfer_then_preserves_retry_after_reopen() {
    let scratch = scratch::ScratchDir::new("owner-tls-snapshot");
    owner_tls::seed_genesis(scratch.path());
    let mut cluster = Cluster::open(scratch.path(), true, None);
    cluster.elect(0);
    cluster.write(0, 1, &open_session(0, 2));
    cluster.write(
        0,
        2,
        &execute(
            0,
            2,
            1,
            Mutation::Deposit {
                account_id: ACCOUNT,
                amount: amount(9),
            },
        ),
    );
    cluster.stop_last();
    let deposit = execute(
        0,
        2,
        2,
        Mutation::Deposit {
            account_id: ACCOUNT,
            amount: amount(5),
        },
    );
    cluster.write(0, 3, &deposit);
    let snapshot = cluster.nodes[0].compact();
    assert_eq!(snapshot.metadata.last_included_index, LogIndex(4));
    assert!(snapshot.application_payload_len > 64 * 1024);
    // Reopen the majority to discard old transport queues. The lagging node
    // must recover from the compacted files, not an old queued append.
    cluster.close();
    let mut cluster = Cluster::reopen_majority(scratch.path());
    cluster.elect(0);
    cluster.restart_last(scratch.path());
    assert_eq!(cluster.nodes[2].owner.durable_through(), Some(LogIndex(3)));
    // One TLS frame per owner turn makes this cut deterministic: stop after
    // the first chunk is durably staged, before any second chunk can be stepped.
    cluster.until(|c| c.nodes[0].ready());
    cluster.nodes[0].input(rafter::Input::Tick);
    cluster.until(|c| {
        c.nodes[2].owner.ready_node().is_some_and(|node| {
            node.snapshot_transfer_status()
                .follower
                .is_some_and(|transfer| transfer.received_bytes == 64 * 1024)
        })
    });
    let pending = cluster.nodes[2]
        .owner
        .ready_node()
        .unwrap()
        .snapshot_transfer_status()
        .follower
        .unwrap();
    assert_eq!(pending.total_bytes, snapshot.application_payload_len);
    assert_eq!(cluster.nodes[2].snapshot_installs, 0);
    cluster.stop_last();
    cluster.restart_last(scratch.path());
    assert_eq!(
        cluster.nodes[2]
            .owner
            .ready_node()
            .unwrap()
            .snapshot_transfer_status()
            .follower,
        Some(pending)
    );
    assert_eq!(cluster.nodes[2].owner.durable_through(), Some(LogIndex(3)));
    cluster.converge(0, LogIndex(4));
    assert_eq!(cluster.nodes[2].snapshot_installs, 1);
    assert!(cluster.nodes[2].snapshot_chunks > 0);
    assert!(cluster.nodes[2].snapshot_max_offset >= 64 * 1024);
    assert_eq!(
        cluster.nodes[2]
            .owner
            .ready_node()
            .unwrap()
            .snapshot_index(),
        LogIndex(4)
    );
    cluster.read(0, 1, Some(14));
    cluster.close();

    let mut cluster = Cluster::open(scratch.path(), false, None);
    cluster.elect(2);
    cluster.read(2, 1, Some(14));
    // Snapshot-installed application identity survives another full reopen.
    cluster.write(2, 1, &deposit);
    cluster.read(2, 2, Some(14));
    cluster.close();
}
