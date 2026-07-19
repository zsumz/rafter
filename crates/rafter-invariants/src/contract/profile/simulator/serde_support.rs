//! Strict decoding for string-valued simulator profile fields.

use serde::{de, Deserialize, Deserializer};

use super::SimulatorStateFloors;

pub(super) fn string_u64<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    String::deserialize(deserializer)?
        .parse()
        .map_err(de::Error::custom)
}

pub(super) fn optional_string_u64<'de, D>(deserializer: D) -> Result<Option<u64>, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<String>::deserialize(deserializer)?
        .map(|value| value.parse().map_err(de::Error::custom))
        .transpose()
}

pub(super) fn state_floors<'de, D>(deserializer: D) -> Result<SimulatorStateFloors, D::Error>
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
