//! Runtime compatibility between simulator output and independent verification.

use std::collections::BTreeMap;

use rafter::{NodeConfig, NodeId};
use rafter_sim::{
    model_check::{run_raft_random_soak, SoakConfig},
    SimSeed,
};

use super::fixture::fixture;

#[test]
fn canonical_binding_bytes_have_stable_digests() {
    let (identity, contracts, events) = fixture();
    let binding = super::fixture::derive(&identity, &contracts, &events)
        .expect("valid reports produce a binding");
    assert_eq!(
        binding.contract_sha256,
        "581a23cd820e7a1b5711d40e32c8737d2e9ff7b46d639039d1b469aeaa60adc5"
    );
    assert_eq!(
        binding.reports_sha256,
        "611bdfd44c316264366d920b96c69ee207dc30d0b72bf4132011e0cc3f978557"
    );
    assert_eq!(
        binding.reports[0].execution_contract_sha256,
        "ade8090f0755cdcb44af0dda59b971bd7d704e520475a700d19021fc3cd4a642"
    );
    assert_eq!(
        binding.reports[0].report_sha256,
        "01574a1514896a804bd1d9cf74ba14436c8ffe75acb416582b77bedcf9317c96"
    );
}
use crate::{
    contract::profile::expected_execution_contract,
    verification::simulator::validate_liveness_report,
};

#[test]
fn actual_simulator_liveness_json_satisfies_the_independent_v3_validator() {
    let configs = [1_u64, 2, 3]
        .into_iter()
        .map(|id| {
            NodeConfig::new(
                NodeId(id),
                [1_u64, 2, 3]
                    .into_iter()
                    .filter(|peer| *peer != id)
                    .map(NodeId)
                    .collect(),
                3,
            )
            .expect("three-node simulator config is valid")
        })
        .collect();
    let config = SoakConfig::new(SimSeed(0x9103), 0)
        .with_max_proposals(24)
        .with_max_restarts(12)
        .with_max_read_indexes(4)
        .with_max_membership_changes(8)
        .with_max_transfers(2)
        .with_max_partitions(2)
        .with_max_lossy_restarts(2)
        .with_snapshot_catchup_probe()
        .with_tick_skew(NodeId(1), 3);
    let summary = run_raft_random_soak(configs, config)
        .expect("actual simulator liveness reports should be produced");
    let (_, contracts, _) = fixture();
    let contracts = contracts
        .iter()
        .map(|contract| (contract.feature_id.as_str(), contract))
        .collect::<BTreeMap<_, _>>();
    let execution =
        expected_execution_contract("pr", "raft-soak").expect("PR soak execution contract exists");
    let reports = summary.liveness_reports_json();
    assert_eq!(reports.len(), contracts.len());
    for report in reports {
        let feature = report["feature_id"]
            .as_str()
            .expect("actual report has feature identity");
        validate_liveness_report(contracts[feature], &execution, &report)
            .unwrap_or_else(|error| panic!("actual {feature} report failed validation: {error}"));
    }
}
