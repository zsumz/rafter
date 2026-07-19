//! Canonical simulator execution contracts for each scheduled profile.

use super::SimulatorExecutionContract;

pub(crate) fn expected_execution_contract(
    profile: &str,
    canonical_check: &str,
) -> Result<SimulatorExecutionContract, String> {
    let (profile_id, steps, maxima, tick_skew_weight) = match profile {
        "pr" => ("raft-soak", 320, [24, 12, 4, 8, 2, 2, 2], 3),
        "nightly" => ("raft-nightly-soak", 1024, [64, 32, 4, 16, 2, 2, 2], 3),
        "weekly" => ("raft-weekly-soak", 4096, [192, 96, 16, 48, 8, 8, 8], 5),
        value => return Err(format!("unsupported simulator profile `{value}`")),
    };
    let (suffix, check_kind, node_config_id, node_count) = match canonical_check {
        "raft-soak" => ("", "standard", "three-node-standard-v1", 3),
        "raft-soak-lease" => ("-lease", "lease", "three-node-lease-v1", 3),
        "raft-soak-membership" => (
            "-membership",
            "membership",
            "four-node-future-learner-v1",
            4,
        ),
        value => return Err(format!("unsupported canonical soak check `{value}`")),
    };
    Ok(SimulatorExecutionContract {
        contract_id: "rafter-soak-execution-v1".to_owned(),
        profile_id: profile_id.to_owned(),
        check_id: format!("{profile_id}{suffix}"),
        check_kind: check_kind.to_owned(),
        node_config_id: node_config_id.to_owned(),
        node_count,
        steps,
        max_proposals: maxima[0],
        max_restarts: maxima[1],
        max_read_indexes: maxima[2],
        max_membership_changes: maxima[3],
        max_transfers: maxima[4],
        max_partitions: maxima[5],
        max_lossy_restarts: maxima[6],
        snapshot_catchup_probe: true,
        tick_skew_node_id: Some(1),
        tick_skew_weight: Some(tick_skew_weight),
    })
}
