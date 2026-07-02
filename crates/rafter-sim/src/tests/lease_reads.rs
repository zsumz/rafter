//! Lease reads under isolation: a leader that stops hearing from its quorum
//! stops granting barriers from the lease.

use super::helpers::config;
use super::*;

const ELECTION_TIMEOUT_TICKS: u64 = 8;
const LEASE_WINDOW_TICKS: u64 = ELECTION_TIMEOUT_TICKS / 2;
const LEADER: NodeId = NodeId(1);

fn lease_config(id: u64, peers: &[u64]) -> rafter::NodeConfig {
    // The default posture already carries the pre-vote and check-quorum
    // foundation the lease requires.
    config(id, peers, ELECTION_TIMEOUT_TICKS).with_lease_reads(true)
}

fn lease_cluster() -> Cluster {
    Cluster::new(vec![
        lease_config(1, &[2, 3]),
        lease_config(2, &[1, 3]),
        lease_config(3, &[1, 2]),
    ])
}

/// Elects node 1 through the pre-vote round: only node 1 is ever ticked, so
/// no other node campaigns.
fn elect_node_one_with_pre_vote(cluster: &mut Cluster) {
    for _ in 0..ELECTION_TIMEOUT_TICKS {
        cluster.tick(LEADER);
    }
    cluster.deliver_all();
    assert_eq!(cluster.role(LEADER), Role::Leader);
}

#[test]
fn an_isolated_leaders_lease_lapses_and_immediate_grants_stop() {
    let mut cluster = lease_cluster();
    elect_node_one_with_pre_vote(&mut cluster);

    // A committed current-term entry makes barriers serveable, and the
    // quorum acknowledgements that commit it confirm the lease.
    cluster.propose(LEADER, b"open-for-reads".to_vec());
    cluster.deliver_all();
    assert_eq!(cluster.commit_index(LEADER), cluster.last_log_index(LEADER));
    assert!(cluster.read_lease_active(LEADER));

    // Inside the window the barrier grants immediately: the grant is
    // recorded without a single message being delivered.
    cluster.read_index(LEADER, 7);
    assert!(
        cluster
            .read_grants()
            .iter()
            .any(|grant| grant.node_id == LEADER && grant.request_id == 7),
        "a held lease grants without a round trip"
    );

    // Isolation: the leader keeps ticking but everything it sends is lost
    // and nothing arrives. The lease lapses after the window.
    for _ in 0..LEASE_WINDOW_TICKS {
        cluster.tick(LEADER);
        cluster.drop_matching(|envelope| envelope.from == LEADER || envelope.to == LEADER);
    }
    assert!(!cluster.read_lease_active(LEADER));
    assert_eq!(cluster.role(LEADER), Role::Leader, "not yet stepped down");

    // A barrier requested behind the lapsed lease is registered, not
    // granted: no new grant appears while the leader stays isolated.
    let grants_before = cluster.read_grants().len();
    cluster.read_index(LEADER, 8);
    assert_eq!(
        cluster.read_grants().len(),
        grants_before,
        "a lapsed lease falls back to the read-index round trip"
    );

    // Check-quorum eventually deposes the isolated leader entirely;
    // acknowledgements recorded before the isolation satisfy the first
    // evaluation, so deposal takes up to two evaluation periods.
    for _ in 0..(2 * ELECTION_TIMEOUT_TICKS) {
        cluster.tick(LEADER);
        cluster.drop_matching(|envelope| envelope.from == LEADER || envelope.to == LEADER);
        if cluster.role(LEADER) != Role::Leader {
            break;
        }
    }
    assert_ne!(cluster.role(LEADER), Role::Leader);
}

#[test]
fn a_reconnected_leader_confirms_a_fresh_round_before_granting_again() {
    let mut cluster = lease_cluster();
    elect_node_one_with_pre_vote(&mut cluster);
    cluster.propose(LEADER, b"open-for-reads".to_vec());
    cluster.deliver_all();
    assert!(cluster.read_lease_active(LEADER));

    // Lapse the lease under isolation, short of the check-quorum deadline.
    for _ in 0..LEASE_WINDOW_TICKS {
        cluster.tick(LEADER);
        cluster.drop_matching(|envelope| envelope.from == LEADER || envelope.to == LEADER);
    }
    assert!(!cluster.read_lease_active(LEADER));

    // Reconnect: one round trip re-confirms leadership and re-arms the
    // lease; immediate grants resume.
    cluster.tick(LEADER);
    cluster.deliver_all();
    assert!(cluster.read_lease_active(LEADER));

    let grants_before = cluster.read_grants().len();
    cluster.read_index(LEADER, 9);
    assert_eq!(cluster.read_grants().len(), grants_before + 1);
}
