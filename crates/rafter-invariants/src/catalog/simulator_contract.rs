use serde::{de, Deserialize, Deserializer};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SimulatorStateFloors {
    PerEvidence,
    Aggregate { protocol: u64, verifier: u64 },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct SimulatorRunnerConfiguration {
    pub build: String,
    pub completion: String,
    pub execution_contract: String,
    pub liveness_report_binding: String,
    pub model_profile: String,
    pub seed_policy: String,
    #[serde(default, deserialize_with = "optional_string_u64")]
    pub seed_count: Option<u64>,
    #[serde(default)]
    pub snapshot_catchup_probe: Option<String>,
    #[serde(deserialize_with = "string_u64")]
    pub soak_steps: u64,
    #[serde(deserialize_with = "state_floors")]
    pub state_floors: SimulatorStateFloors,
    #[serde(default)]
    pub canonical_check_binding: Option<String>,
}

impl SimulatorRunnerConfiguration {
    pub(crate) fn validate_profile(&self, profile: &str) -> Result<(), &'static str> {
        if self.build != "release-and-test-locked"
            || self.execution_contract != "rafter-soak-execution-v1"
            || self.liveness_report_binding != "typed-canonical-json-sha256-v2"
            || self.soak_steps == 0
        {
            return Err("shared build, execution, liveness, or step policy is unsupported");
        }
        match profile {
            "pr" if self.completion == "frontier-and-semantic-floor"
                && self.model_profile == "fast+raft-soak"
                && self.seed_policy == "curated-0x9103-through-0x9106"
                && self.seed_count.is_none()
                && self.snapshot_catchup_probe.as_deref() == Some("required")
                && self.canonical_check_binding.is_none()
                && self.state_floors == SimulatorStateFloors::PerEvidence =>
            {
                Ok(())
            }
            "nightly" | "weekly"
                if self.completion == "frontier-and-aggregate-state-floor"
                    && self.model_profile == format!("raft-{profile}")
                    && self.seed_policy == "source-derived-sha256-v1"
                    && self.seed_count.is_some_and(|count| count > 0)
                    && self.snapshot_catchup_probe.is_none()
                    && self.canonical_check_binding.as_deref() == Some("scheduled-suffix-v1")
                    && matches!(
                        self.state_floors,
                        SimulatorStateFloors::Aggregate {
                            protocol: 1..,
                            verifier: 1..
                        }
                    ) =>
            {
                Ok(())
            }
            "pr" | "nightly" | "weekly" => Err("profile-specific simulator policy is inconsistent"),
            _ => Err("simulator profile is unsupported"),
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
            ("completion", "frontier-and-aggregate-state-floor"),
            ("execution_contract", "rafter-soak-execution-v1"),
            ("liveness_report_binding", "typed-canonical-json-sha256-v2"),
            ("model_profile", "raft-nightly"),
            ("seed_count", "6"),
            ("seed_policy", "source-derived-sha256-v1"),
            ("soak_steps", "1024"),
            ("state_floors", "100000000-protocol-and-verifier"),
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
            "completion": "frontier-and-semantic-floor",
            "execution_contract": "rafter-soak-execution-v1",
            "liveness_report_binding": "typed-canonical-json-sha256-v2",
            "model_profile": "fast+raft-soak",
            "seed_policy": "curated-0x9103-through-0x9106",
            "snapshot_catchup_probe": "required",
            "soak_steps": "320",
            "state_floors": "per-evidence",
            "unreviewed": "true"
        });
        assert!(serde_json::from_value::<SimulatorRunnerConfiguration>(unknown).is_err());
    }
}
