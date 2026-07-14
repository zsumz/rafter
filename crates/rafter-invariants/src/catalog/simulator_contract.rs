use serde::{de, Deserialize, Deserializer};

const PR_SOAK_STEPS: u64 = 320;
const NIGHTLY_SOAK_STEPS: u64 = 1_024;
const NIGHTLY_SEED_COUNT: u64 = 6;
const NIGHTLY_STATE_FLOOR: u64 = 100_000_000;
const WEEKLY_SOAK_STEPS: u64 = 4_096;
const WEEKLY_SEED_COUNT: u64 = 10;
const WEEKLY_STATE_FLOOR: u64 = 250_000_000;

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
                && self.layer_timeout == "25m"
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

fn string_u64<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    String::deserialize(deserializer)?
        .parse()
        .map_err(de::Error::custom)
}

fn optional_string_u64<'de, D>(deserializer: D) -> Result<Option<u64>, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<String>::deserialize(deserializer)?
        .map(|value| value.parse().map_err(de::Error::custom))
        .transpose()
}

fn state_floors<'de, D>(deserializer: D) -> Result<SimulatorStateFloors, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    if value == "per-evidence" {
        return Ok(SimulatorStateFloors::PerEvidence);
    }
    let count = value
        .strip_suffix("-protocol-and-verifier")
        .ok_or_else(|| de::Error::custom("unsupported simulator state-floor policy"))?
        .parse::<u64>()
        .map_err(de::Error::custom)?;
    Ok(SimulatorStateFloors::Aggregate {
        protocol: count,
        verifier: count,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{SimulatorRunnerConfiguration, SimulatorStateFloors};

    #[test]
    fn simulator_contract_deserializes_numeric_and_floor_policy() {
        let configuration = BTreeMap::from([
            ("build", "release-and-test-locked"),
            ("compile_timeout", "10m"),
            ("completion", "frontier-and-aggregate-state-floor"),
            ("execution_contract", "rafter-soak-execution-v1"),
            ("finalization_reserve", "10m"),
            ("kill_confirmation_timeout", "5s"),
            ("layer_timeout", "170m"),
            ("liveness_report_binding", "typed-canonical-json-sha256-v3"),
            ("model_profile", "raft-nightly"),
            ("model_timeout_policy", "remaining-layer-budget"),
            ("receipt_finalization_allowance", "5s"),
            ("seed_count", "6"),
            ("seed_policy", "source-derived-sha256-v1"),
            ("soak_steps", "1024"),
            ("state_floors", "100000000-protocol-and-verifier"),
            ("termination_grace", "30s"),
            ("canonical_check_binding", "scheduled-suffix-v1"),
        ]);
        let contract: SimulatorRunnerConfiguration = serde_json::from_value(
            serde_json::to_value(configuration).expect("configuration serializes"),
        )
        .expect("typed contract deserializes");

        assert_eq!(contract.seed_count, Some(6));
        assert_eq!(contract.soak_steps, 1024);
        assert_eq!(
            contract.state_floors,
            SimulatorStateFloors::Aggregate {
                protocol: 100_000_000,
                verifier: 100_000_000,
            }
        );
        contract
            .validate_profile("nightly")
            .expect("nightly contract");
    }

    #[test]
    fn simulator_contract_rejects_unknown_and_misplaced_fields() {
        let unknown = serde_json::json!({
            "build": "release-and-test-locked",
            "compile_timeout": "10m",
            "completion": "frontier-and-semantic-floor",
            "execution_contract": "rafter-soak-execution-v1",
            "finalization_reserve": "3m",
            "kill_confirmation_timeout": "5s",
            "layer_timeout": "25m",
            "liveness_report_binding": "typed-canonical-json-sha256-v3",
            "model_profile": "fast+raft-soak",
            "model_timeout_policy": "remaining-layer-budget",
            "receipt_finalization_allowance": "5s",
            "seed_policy": "curated-0x9103-through-0x9106",
            "snapshot_catchup_probe": "required",
            "soak_steps": "320",
            "state_floors": "per-evidence",
            "termination_grace": "30s",
            "unreviewed": "true"
        });
        assert!(serde_json::from_value::<SimulatorRunnerConfiguration>(unknown).is_err());
    }

    #[test]
    fn simulator_contract_rejects_weakened_pr_thresholds() {
        let mut contract = reviewed_pr_contract();
        contract
            .validate_profile("pr")
            .expect("reviewed PR thresholds");

        contract.soak_steps = 319;
        assert!(contract.validate_profile("pr").is_err());

        let mut contract = reviewed_pr_contract();
        contract.state_floors = SimulatorStateFloors::Aggregate {
            protocol: 1,
            verifier: 1,
        };
        assert!(contract.validate_profile("pr").is_err());
    }

    #[test]
    fn simulator_contract_rejects_weakened_nightly_thresholds() {
        assert_weakened_scheduled_thresholds_are_rejected(
            "nightly",
            reviewed_scheduled_contract("nightly", 1_024, 6, 100_000_000),
        );
    }

    #[test]
    fn simulator_contract_rejects_weakened_weekly_thresholds() {
        assert_weakened_scheduled_thresholds_are_rejected(
            "weekly",
            reviewed_scheduled_contract("weekly", 4_096, 10, 250_000_000),
        );
    }

    fn assert_weakened_scheduled_thresholds_are_rejected(
        profile: &str,
        contract: SimulatorRunnerConfiguration,
    ) {
        contract
            .validate_profile(profile)
            .expect("reviewed scheduled thresholds");

        let mut weakened = contract.clone();
        weakened.soak_steps -= 1;
        assert!(weakened.validate_profile(profile).is_err());

        let mut weakened = contract.clone();
        weakened.seed_count = weakened.seed_count.map(|count| count - 1);
        assert!(weakened.validate_profile(profile).is_err());

        let mut weakened = contract.clone();
        let SimulatorStateFloors::Aggregate {
            protocol,
            verifier: _,
        } = &mut weakened.state_floors
        else {
            panic!("scheduled fixture uses aggregate floors");
        };
        *protocol -= 1;
        assert!(weakened.validate_profile(profile).is_err());

        let mut weakened = contract;
        let SimulatorStateFloors::Aggregate {
            protocol: _,
            verifier,
        } = &mut weakened.state_floors
        else {
            panic!("scheduled fixture uses aggregate floors");
        };
        *verifier -= 1;
        assert!(weakened.validate_profile(profile).is_err());
    }

    fn reviewed_pr_contract() -> SimulatorRunnerConfiguration {
        SimulatorRunnerConfiguration {
            build: "release-and-test-locked".to_owned(),
            compile_timeout: "10m".to_owned(),
            completion: "frontier-and-semantic-floor".to_owned(),
            execution_contract: "rafter-soak-execution-v1".to_owned(),
            finalization_reserve: "3m".to_owned(),
            kill_confirmation_timeout: "5s".to_owned(),
            layer_timeout: "25m".to_owned(),
            liveness_report_binding: "typed-canonical-json-sha256-v3".to_owned(),
            model_profile: "fast+raft-soak".to_owned(),
            model_timeout_policy: "remaining-layer-budget".to_owned(),
            receipt_finalization_allowance: "5s".to_owned(),
            seed_policy: "curated-0x9103-through-0x9106".to_owned(),
            seed_count: None,
            snapshot_catchup_probe: Some("required".to_owned()),
            soak_steps: 320,
            state_floors: SimulatorStateFloors::PerEvidence,
            termination_grace: "30s".to_owned(),
            canonical_check_binding: None,
        }
    }

    fn reviewed_scheduled_contract(
        profile: &str,
        soak_steps: u64,
        seed_count: u64,
        state_floor: u64,
    ) -> SimulatorRunnerConfiguration {
        SimulatorRunnerConfiguration {
            build: "release-and-test-locked".to_owned(),
            compile_timeout: "10m".to_owned(),
            completion: "frontier-and-aggregate-state-floor".to_owned(),
            execution_contract: "rafter-soak-execution-v1".to_owned(),
            finalization_reserve: "10m".to_owned(),
            kill_confirmation_timeout: "5s".to_owned(),
            layer_timeout: if profile == "nightly" { "170m" } else { "340m" }.to_owned(),
            liveness_report_binding: "typed-canonical-json-sha256-v3".to_owned(),
            model_profile: format!("raft-{profile}"),
            model_timeout_policy: "remaining-layer-budget".to_owned(),
            receipt_finalization_allowance: "5s".to_owned(),
            seed_policy: "source-derived-sha256-v1".to_owned(),
            seed_count: Some(seed_count),
            snapshot_catchup_probe: None,
            soak_steps,
            state_floors: SimulatorStateFloors::Aggregate {
                protocol: state_floor,
                verifier: state_floor,
            },
            termination_grace: "30s".to_owned(),
            canonical_check_binding: Some("scheduled-suffix-v1".to_owned()),
        }
    }
}
