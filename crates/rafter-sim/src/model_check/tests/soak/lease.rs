use rafter::NodeId;

use super::super::super::helpers::three_node_lease_configs;
use super::super::super::{run_raft_random_soak, SoakActionKind, SoakConfig};
use crate::SimSeed;

#[test]
fn randomized_lease_soak_exercises_read_fault_and_timing_actions() {
    let config = SoakConfig::new(SimSeed(0x6c35_ea5e), 320)
        .with_max_proposals(24)
        .with_max_restarts(12)
        .with_max_read_indexes(4)
        .with_max_membership_changes(8)
        .with_max_transfers(2)
        .with_max_partitions(2)
        .with_max_lossy_restarts(2)
        .with_tick_skew(NodeId(1), 3);
    let summary = run_raft_random_soak(three_node_lease_configs(), config)
        .expect("lease-enabled production soak should preserve Raft invariants");

    for kind in [
        SoakActionKind::Tick,
        SoakActionKind::Restart,
        SoakActionKind::ReadIndex,
        SoakActionKind::Partition,
    ] {
        assert!(
            summary.observed_actions().contains(&kind),
            "lease soak should observe {kind:?}"
        );
    }
}
