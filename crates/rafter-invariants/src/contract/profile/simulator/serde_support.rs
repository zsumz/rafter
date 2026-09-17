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
    if let Some(count) = value.strip_suffix("-protocol-and-verifier") {
        let count = count.parse::<u64>().map_err(de::Error::custom)?;
        return Ok(SimulatorStateFloors::Aggregate {
            protocol: count,
            verifier: count,
        });
    }
    let (protocol, verifier) = value
        .split_once("-protocol-")
        .and_then(|(protocol, verifier)| {
            verifier
                .strip_suffix("-verifier")
                .map(|verifier| (protocol, verifier))
        })
        .ok_or_else(|| de::Error::custom("unsupported simulator state-floor policy"))?;
    Ok(SimulatorStateFloors::Aggregate {
        protocol: protocol.parse().map_err(de::Error::custom)?,
        verifier: verifier.parse().map_err(de::Error::custom)?,
    })
}

#[cfg(test)]
#[path = "serde_support_test.rs"]
mod tests;
