//! Profile-specific TLA+ runner option constraints.

use std::{collections::BTreeMap, error::Error};

use super::{super::checkpoint, tool::required_configuration};

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
                ("soft_timeout", "265m"),
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
                    ("soft_timeout", "265m"),
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
