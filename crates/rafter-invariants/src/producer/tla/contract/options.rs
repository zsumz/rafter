//! Profile-specific TLA+ runner option constraints.

use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
};

use crate::contract::profile::{ObligationCompletion, ProofObligationContract};

use super::{super::checkpoint, tool::required_configuration};

/// Configurations that some profile owns as its primary model. The producer
/// re-derives this locally rather than importing it: this allow-list is an
/// independent gate, and sharing a constant with the contract layer would make
/// one edit weaken both.
const PRIMARY_CONFIGS: [&str; 3] = ["RaftCi.cfg", "RaftNightly.cfg", "Raft.cfg"];

/// Producer-side gate on the obligation list.
///
/// This duplicates, on purpose, the shape the profile contract already
/// enforces. The runner must not execute an obligation set it cannot itself
/// justify: it refuses duplicate identities, primary configurations wearing an
/// obligation's name, non-exhaustion completions, and budgets or floors that
/// could let a vacuous run report success.
pub(in crate::producer::tla) fn validate_obligation_options(
    obligations: &[ProofObligationContract],
) -> Result<(), Box<dyn Error>> {
    let identities = obligations
        .iter()
        .map(|obligation| obligation.id.as_str())
        .collect::<BTreeSet<_>>();
    if identities.len() != obligations.len() {
        return Err("TLA runner requires unique proof obligation identities".into());
    }
    for obligation in obligations {
        if obligation.id.is_empty() {
            return Err("TLA runner requires a named proof obligation".into());
        }
        if obligation.completion != ObligationCompletion::FrontierExhausted {
            return Err(format!(
                "TLA proof obligation {} must require frontier exhaustion",
                obligation.id
            )
            .into());
        }
        if PRIMARY_CONFIGS.contains(&obligation.config.as_str())
            || !obligation.config.ends_with(".cfg")
            || obligation.config.contains('/')
        {
            return Err(format!(
                "TLA proof obligation {} must name a non-primary configuration file",
                obligation.id
            )
            .into());
        }
        if obligation.minimum_generated_states == 0 || obligation.minimum_distinct_states == 0 {
            return Err(format!(
                "TLA proof obligation {} must carry positive state floors",
                obligation.id
            )
            .into());
        }
        super::parse_timeout(&obligation.soft_timeout)?;
    }
    Ok(())
}

pub(in crate::producer::tla) fn validate_runner_options(
    configuration: &BTreeMap<String, String>,
) -> Result<(), Box<dyn Error>> {
    for (name, expected) in [
        ("module", "Raft.tla"),
        ("fp", "0"),
        ("tool_mode", "required"),
        ("trace_sample", "required"),
        ("detector_negative", "required"),
    ] {
        if required_configuration(configuration, name)? != expected {
            return Err(format!("TLA runner requires {name}={expected}").into());
        }
    }
    if matches!(
        configuration.get("config").map(String::as_str),
        Some("RaftCi.cfg" | "RaftNightly.cfg")
    ) && required_configuration(configuration, "symmetry")?
        != "nodes-values-read-requests-product"
    {
        return Err("bounded TLA runner requires the complete model-value symmetry".into());
    }
    match (
        configuration.get("config").map(String::as_str),
        checkpoint::enabled(configuration),
    ) {
        (Some("RaftCi.cfg"), false) => {
            for (name, expected) in [
                ("workers", "4"),
                ("soft_timeout", "325m"),
                ("max_heap", "8g"),
                ("fp_mem", "0.45"),
            ] {
                if required_configuration(configuration, name)? != expected {
                    return Err(format!("PR TLA runner requires {name}={expected}").into());
                }
            }
        }
        (Some("RaftNightly.cfg"), true) => {
            for (name, expected) in [
                ("workers", "auto"),
                ("soft_timeout", "250m"),
                ("checkpoint_minutes", "30"),
                ("checkpoint_gzip", "required"),
                ("max_heap", "8g"),
                ("fp_mem", "0.45"),
                ("checkpoint_recovery", "strict-compatible-if-present"),
            ] {
                if required_configuration(configuration, name)? != expected {
                    return Err(format!(
                        "checkpointed nightly TLA runner requires {name}={expected}"
                    )
                    .into());
                }
            }
        }
        _ => {}
    }
    if checkpoint::enabled(configuration) {
        match required_configuration(configuration, "config")? {
            "Raft.cfg" => {
                for (name, expected) in [
                    ("workers", "auto"),
                    ("soft_timeout", "200m"),
                    ("checkpoint_minutes", "30"),
                    ("checkpoint_gzip", "required"),
                    ("max_heap", "4g"),
                    ("fp_mem", "0.45"),
                    ("checkpoint_recovery", "strict-compatible-if-present"),
                    ("unsymmetrized_exploration", "required"),
                ] {
                    if required_configuration(configuration, name)? != expected {
                        return Err(format!(
                            "checkpointed weekly TLA runner requires {name}={expected}"
                        )
                        .into());
                    }
                }
            }
            "RaftNightly.cfg" => {}
            other => {
                return Err(
                    format!("checkpointed TLA runner does not support config={other}").into(),
                )
            }
        }
    }
    Ok(())
}
