//! Registry and safety binding for focused TLA+ proof obligation configs.

use std::{collections::BTreeSet, error::Error, fs, path::Path};

use crate::contract::profile::ProofObligationContract;

use super::spec::{
    configured_invariants, validate_safety_only_boundary, validate_symmetry_contract, SPEC,
};

/// Binds each obligation configuration to the same registry the primary
/// configuration answers to.
///
/// Without this an obligation could "discharge" while checking nothing: TLC
/// exits cleanly on a config with no invariants, and the state floors alone
/// would happily be met. Every obligation must therefore configure exactly the
/// registered predicates plus `TypeOK`, and must stay inside the safety-only
/// boundary the production specification is held to.
pub(in crate::producer::tla) fn validate_obligation_specs(
    obligations: &[ProofObligationContract],
    symbols: &BTreeSet<String>,
) -> Result<(), Box<dyn Error>> {
    if obligations.is_empty() {
        return Ok(());
    }
    let spec = fs::read_to_string(SPEC)?;
    for obligation in obligations {
        let path = Path::new("specs/tla/raft").join(&obligation.config);
        let source = fs::read_to_string(&path).map_err(|error| {
            format!(
                "read TLA proof obligation config {}: {error}",
                obligation.id
            )
        })?;
        validate_obligation_config_sources(
            &obligation.id,
            &obligation.config,
            &spec,
            &source,
            symbols,
        )?;
    }
    Ok(())
}

pub(super) fn validate_obligation_config_sources(
    id: &str,
    config_name: &str,
    spec: &str,
    config: &str,
    symbols: &BTreeSet<String>,
) -> Result<(), Box<dyn Error>> {
    let mut expected = symbols.clone();
    expected.insert("TypeOK".to_owned());
    let configured = configured_invariants(config)
        .into_iter()
        .collect::<BTreeSet<_>>();
    if configured != expected {
        return Err(format!(
            "TLA proof obligation {id} must configure exactly the registry predicates"
        )
        .into());
    }
    validate_safety_only_boundary(spec, config)?;
    validate_symmetry_contract(config_name, config)
}
