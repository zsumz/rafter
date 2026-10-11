//! The asynchronous durable owner composed with the existing production TLS
//! transport through public APIs, in source and extracted-package lanes.
#[path = "support/owner_tls/mod.rs"]
mod owner_tls;
#[path = "support/scratch.rs"]
mod scratch;
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
