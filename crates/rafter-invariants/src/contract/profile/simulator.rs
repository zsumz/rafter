//! Profile-owned simulator execution bounds, floors, and observation keys.

use std::collections::{BTreeMap, BTreeSet};

use serde::Deserialize;

use super::{RunnerContract, SimulatorCheckContract};
use crate::contract::catalog::Catalog;

mod identity;
mod serde_support;

pub(crate) use identity::{canonical_simulator_check_id, scheduled_simulator_seeds};
use serde_support::{optional_string_u64, state_floors, string_u64};

const PR_SOAK_STEPS: u64 = 320;
const NIGHTLY_SOAK_STEPS: u64 = 1_024;
const NIGHTLY_SEED_COUNT: u64 = 6;
const NIGHTLY_STATE_FLOOR: u64 = 13_000_000;
const WEEKLY_SOAK_STEPS: u64 = 4_096;
const WEEKLY_SEED_COUNT: u64 = 10;
const WEEKLY_STATE_FLOOR: u64 = 250_000_000;

pub(crate) const PR_FAST_CHECK_IDS: [&str; 13] = [
    "raft-election",
    "raft-commit",
    "raft-commit-window1",
    "raft-commit-production",
    "raft-membership",
    "raft-membership-restart-snapshot",
    "raft-commit-seeded",
    "raft-leadership-noop-seeded",
    "raft-restart-snapshot",
    "raft-election-prevote",
    "raft-semantic-witnesses",
    "raft-read-index",
    "raft-lease-read",
];

const PR_SPECIALIZED_OBSERVATIONS: [(&str, &str); 4] = [
    (
        "raft-commit-production",
        "production_config_commit_observed",
    ),
    ("raft-commit-window1", "window_one_backpressure_observed"),
    ("raft-lease-read", "lease_fast_path_read_granted"),
    (
        "raft-membership-restart-snapshot",
        "joint_config_restart_snapshot_recovered",
    ),
];

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SimulatorStateFloors {
    PerEvidence,
    Aggregate { protocol: u64, verifier: u64 },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct SimulatorRunnerConfiguration {
    pub build: String,
    pub compile_timeout: String,
    pub completion: String,
    pub detector_proof: String,
    pub execution_contract: String,
    pub finalization_reserve: String,
    pub kill_confirmation_timeout: String,
    pub layer_timeout: String,
    pub liveness_report_binding: String,
    pub model_profile: String,
    pub model_timeout_policy: String,
    pub receipt_finalization_allowance: String,
    pub seed_policy: String,
    #[serde(default, deserialize_with = "optional_string_u64")]
    pub seed_count: Option<u64>,
    #[serde(default)]
    pub snapshot_catchup_probe: Option<String>,
    #[serde(deserialize_with = "string_u64")]
    pub soak_steps: u64,
    #[serde(deserialize_with = "state_floors")]
    pub state_floors: SimulatorStateFloors,
    pub termination_grace: String,
    #[serde(default)]
    pub canonical_check_binding: Option<String>,
}

impl SimulatorRunnerConfiguration {
    pub(crate) fn validate_profile(&self, profile: &str) -> Result<(), &'static str> {
        if self.build != "release-and-test-locked"
            || self.compile_timeout != "10m"
            || self.detector_proof != "inherited-descriptor-pre-body-secret-v3"
            || self.execution_contract != "rafter-soak-execution-v1"
            || self.kill_confirmation_timeout != "5s"
            || self.liveness_report_binding != "typed-canonical-json-sha256-v3"
            || self.model_timeout_policy != "remaining-layer-budget"
            || self.receipt_finalization_allowance != "5s"
            || self.soak_steps == 0
            || self.termination_grace != "30s"
        {
            return Err("shared build, execution, liveness, or step policy is unsupported");
        }
        match profile {
            "pr" if self.completion == "frontier-and-semantic-floor"
                && self.model_profile == "fast+raft-soak"
                && self.seed_policy == "curated-0x9103-through-0x9106"
                && self.seed_count.is_none()
                && self.layer_timeout == "40m"
                && self.finalization_reserve == "3m"
                && self.soak_steps == PR_SOAK_STEPS
                && self.snapshot_catchup_probe.as_deref() == Some("required")
                && self.canonical_check_binding.is_none()
                && self.state_floors == SimulatorStateFloors::PerEvidence =>
            {
                Ok(())
            }
            "nightly"
                if self.matches_scheduled_profile(
                    "raft-nightly",
                    NIGHTLY_SOAK_STEPS,
                    NIGHTLY_SEED_COUNT,
                    NIGHTLY_STATE_FLOOR,
                    "170m",
                ) =>
            {
                Ok(())
            }
            "weekly"
                if self.matches_scheduled_profile(
                    "raft-weekly",
                    WEEKLY_SOAK_STEPS,
                    WEEKLY_SEED_COUNT,
                    WEEKLY_STATE_FLOOR,
                    "340m",
                ) =>
            {
                Ok(())
            }
            "pr" | "nightly" | "weekly" => Err("profile-specific simulator policy is inconsistent"),
            _ => Err("simulator profile is unsupported"),
        }
    }

    fn matches_scheduled_profile(
        &self,
        model_profile: &str,
        soak_steps: u64,
        seed_count: u64,
        state_floor: u64,
        layer_timeout: &str,
    ) -> bool {
        self.completion == "frontier-and-aggregate-state-floor"
            && self.model_profile == model_profile
            && self.seed_policy == "source-derived-sha256-v1"
            && self.seed_count == Some(seed_count)
            && self.layer_timeout == layer_timeout
            && self.finalization_reserve == "10m"
            && self.soak_steps == soak_steps
            && self.snapshot_catchup_probe.is_none()
            && self.canonical_check_binding.as_deref() == Some("scheduled-suffix-v1")
            && self.state_floors
                == SimulatorStateFloors::Aggregate {
                    protocol: state_floor,
                    verifier: state_floor,
                }
    }
}

pub(crate) fn validate_check_contracts(
    profile: &str,
    layer: &str,
    contracts: &BTreeMap<String, SimulatorCheckContract>,
    catalog: &Catalog,
) -> Result<(), String> {
    if layer != "simulator" {
        return if contracts.is_empty() {
            Ok(())
        } else {
            Err("simulator check contracts may only appear on the simulator runner".to_owned())
        };
    }
    if profile != "pr" {
        return if contracts.is_empty() {
            Ok(())
        } else {
            Err("per-check simulator contracts are owned by the PR fast profile".to_owned())
        };
    }

    let expected = PR_FAST_CHECK_IDS.into_iter().collect::<BTreeSet<_>>();
    let actual = contracts
        .keys()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    if actual != expected {
        let missing = expected.difference(&actual).copied().collect::<Vec<_>>();
        let extra = actual.difference(&expected).copied().collect::<Vec<_>>();
        return Err(format!(
            "PR simulator check inventory must exactly match the fast profile (missing: {}; extra: {})",
            missing.join(", "),
            extra.join(", ")
        ));
    }

    for (check_id, contract) in contracts {
        if contract.minimum_protocol_states == 0 || contract.minimum_verifier_states == 0 {
            return Err(format!(
                "simulator check {check_id} must have positive protocol and verifier state floors"
            ));
        }
        let observations = contract
            .required_observations
            .iter()
            .map(|observation| observation.trim())
            .collect::<Vec<_>>();
        if observations.is_empty()
            || observations
                .iter()
                .any(|observation| observation.is_empty())
            || contract
                .required_observations
                .iter()
                .any(|observation| observation != observation.trim())
            || observations.iter().collect::<BTreeSet<_>>().len() != observations.len()
        {
            return Err(format!(
                "simulator check {check_id} must have unique nonempty required observations"
            ));
        }
    }

    for (check_id, observation) in PR_SPECIALIZED_OBSERVATIONS {
        if !contracts[check_id]
            .required_observations
            .iter()
            .any(|required| required == observation)
        {
            return Err(format!(
                "simulator check {check_id} must require specialized purpose observation {observation}"
            ));
        }
    }

    let claimed = catalog
        .evidence
        .iter()
        .filter_map(|descriptor| descriptor.simulator.as_ref())
        .flat_map(|identity| identity.checks.iter().map(String::as_str))
        .collect::<BTreeSet<_>>();
    let unclaimed = expected.difference(&claimed).copied().collect::<Vec<_>>();
    if !unclaimed.is_empty() {
        return Err(format!(
            "PR simulator checks are not claimed by registry evidence: {}",
            unclaimed.join(", ")
        ));
    }
    Ok(())
}

pub(crate) fn per_check_protocol_states_key(check_id: &str) -> String {
    format!("unique_protocol_states:{check_id}")
}

pub(crate) fn per_check_verifier_states_key(check_id: &str) -> String {
    format!("unique_verifier_states:{check_id}")
}

pub(crate) fn per_check_observation_key(check_id: &str, observation: &str) -> String {
    format!("observed:{check_id}:{observation}")
}

#[cfg(test)]
#[path = "simulator/tests.rs"]
mod tests;

impl RunnerContract {
    pub(crate) fn simulator_configuration(
        &self,
    ) -> Result<SimulatorRunnerConfiguration, serde_json::Error> {
        serde_json::from_value(serde_json::to_value(&self.configuration)?)
    }
}
