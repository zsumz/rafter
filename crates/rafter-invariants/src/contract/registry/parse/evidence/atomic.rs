//! Reviewed atomic multi-clause evidence bindings.

use std::collections::BTreeMap;

use crate::contract::registry::RegistryParseError;

pub(super) fn validate_atomic_group(
    index: usize,
    record: &BTreeMap<String, String>,
    invariant_id: &str,
    clause_ids: &[String],
    layer: &str,
    strength: &str,
    atomic_group: Option<&str>,
) -> Result<(), RegistryParseError> {
    let direct_simulator = layer == "simulator" && strength == "direct";
    if direct_simulator && clause_ids.len() > 1 && atomic_group.is_none() {
        return Err(RegistryParseError(format!(
            "direct simulator evidence record {} spans multiple clauses without a reviewed atomic_group",
            index + 1
        )));
    }
    let Some(group) = atomic_group else {
        return Ok(());
    };
    if !direct_simulator || clause_ids.len() < 2 {
        return Err(RegistryParseError(format!(
            "evidence record {} declares atomic_group outside multi-clause direct simulator evidence",
            index + 1
        )));
    }
    if group.trim().is_empty() || !group.starts_with(&format!("{invariant_id}/")) {
        return Err(RegistryParseError(format!(
            "evidence record {} atomic_group must be a nonempty stable ID prefixed with {invariant_id}/",
            index + 1
        )));
    }
    let reviewed =
        group == "CM-03/current-term-commit-point" && clause_ids == ["CM-03.a", "CM-03.b"];
    if !reviewed {
        return Err(RegistryParseError(format!(
            "evidence record {} atomic_group `{group}` is not a reviewed atomic clause set",
            index + 1
        )));
    }
    if !record.contains_key("negative_fixture") || !record.contains_key("negative_fixture_detector")
    {
        return Err(RegistryParseError(format!(
            "evidence record {} atomic_group must bind a detector-level negative fixture",
            index + 1
        )));
    }
    Ok(())
}
