//! Producer-owned source policy, tool capture, and receipt construction.

use std::{
    collections::BTreeSet,
    error::Error,
    fs,
    path::{Component, Path, PathBuf},
};

use crate::{
    evidence::{SourceMaterializationReceipt, SourceReceipt},
    provenance::source::{
        observe_checkout_with, CheckoutCommandRunner, CheckoutObservation, CommandOutput,
        GeneratedOutputPolicy,
    },
};

use super::process;

mod artifact_names;
mod tools;

use artifact_names::reviewed_tla_evidence_artifact;
#[cfg(test)]
use tools::{bind_adjacent_tool_inputs, tool_identity_arguments, tool_version_output};
use tools::{capture_tool, find_tool};

#[derive(Clone, Copy)]
struct LayerSourceContract {
    build_profile: &'static str,
    features: &'static [&'static str],
    tools: &'static [&'static str],
    script_runtime: bool,
}

#[derive(Clone, Copy)]
enum CaptureBudget {
    Execution,
    Total,
}

#[derive(Clone, Copy)]
struct ProducerCommandRunner(CaptureBudget);

impl CheckoutCommandRunner for ProducerCommandRunner {
    fn run(
        &self,
        program: &str,
        arguments: &[&str],
        current_dir: &Path,
    ) -> Result<CommandOutput, Box<dyn Error>> {
        let output = match self.0 {
            CaptureBudget::Execution => {
                process::identity_command_in(program, arguments, current_dir)?
            }
            CaptureBudget::Total => {
                process::identity_command_in_total_budget(program, arguments, current_dir)?
            }
        };
        Ok(CommandOutput {
            stdout: output.stdout,
            stderr: output.stderr,
        })
    }
}

struct ProducerGeneratedOutputs;

impl GeneratedOutputPolicy for ProducerGeneratedOutputs {
    fn permits(&self, path: &Path) -> bool {
        reviewed_generated_output(path)
    }
}

pub(super) fn capture_for_layer(layer: &str) -> Result<SourceReceipt, Box<dyn Error>> {
    capture_at(
        layer_contract(layer)?,
        Path::new("."),
        CaptureBudget::Execution,
    )
}

#[cfg(test)]
pub(crate) fn capture_for_layer_at(
    layer: &str,
    root: &Path,
) -> Result<SourceReceipt, Box<dyn Error>> {
    capture_at(layer_contract(layer)?, root, CaptureBudget::Execution)
}

fn capture_at(
    contract: LayerSourceContract,
    root: &Path,
    budget: CaptureBudget,
) -> Result<SourceReceipt, Box<dyn Error>> {
    let runner = ProducerCommandRunner(budget);
    let checkout = observe_checkout_with(root, &runner, &ProducerGeneratedOutputs)?;
    build_receipt(contract, checkout, root, runner)
}

fn build_receipt(
    contract: LayerSourceContract,
    checkout: CheckoutObservation,
    root: &Path,
    runner: ProducerCommandRunner,
) -> Result<SourceReceipt, Box<dyn Error>> {
    let environment = process::base_environment();
    let process_runtime = process::capture_runtime_receipts(&environment, contract.script_runtime)?;
    let tools = contract
        .tools
        .iter()
        .map(|name| Ok(((*name).to_owned(), capture_tool(name, root, runner)?)))
        .collect::<Result<_, Box<dyn Error>>>()?;
    let environment_sha256 = crate::provenance::source::source_environment_sha256()?;
    Ok(SourceReceipt {
        commit: checkout.commit,
        tree: checkout.tree,
        materialization: SourceMaterializationReceipt {
            contract: checkout.materialization.contract,
            sha256: checkout.materialization.sha256,
            tracked_entries: checkout.materialization.tracked_entries,
            submodules: checkout.materialization.submodules,
        },
        cargo_lock_sha256: checkout.cargo_lock_sha256,
        cargo: checkout.cargo,
        cargo_sha256: checkout.cargo_sha256,
        cargo_config_sha256: checkout.cargo_config_sha256,
        rustc: checkout.rustc,
        rustc_sha256: checkout.rustc_sha256,
        target: checkout.target,
        build_profile: contract.build_profile.to_owned(),
        features: contract
            .features
            .iter()
            .map(|value| (*value).to_owned())
            .collect(),
        tools,
        process_runtime,
        environment_sha256,
        clean: true,
    })
}

pub(super) fn verify(expected: &SourceReceipt) -> Result<(), Box<dyn Error>> {
    let contract = contract_for_receipt(expected)?;
    let observed = capture_at(contract, Path::new("."), CaptureBudget::Total)?;
    if &observed != expected {
        return Err("source or toolchain identity changed during evidence execution".into());
    }
    Ok(())
}

pub(crate) fn verify_layer_contract(
    layer: &str,
    receipt: &SourceReceipt,
) -> Result<(), Box<dyn Error>> {
    let expected = layer_contract(layer)?;
    let expected_features = expected
        .features
        .iter()
        .map(|value| (*value).to_owned())
        .collect::<Vec<_>>();
    let expected_tools = expected.tools.iter().copied().collect::<BTreeSet<_>>();
    let observed_tools = receipt
        .tools
        .keys()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let expected_runtime = if expected.script_runtime {
        ["bash", "perl", "ps", "time"].as_slice()
    } else {
        ["perl", "ps", "time"].as_slice()
    };
    let observed_runtime = receipt
        .process_runtime
        .keys()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    if receipt.build_profile != expected.build_profile
        || receipt.features != expected_features
        || observed_tools != expected_tools
        || observed_runtime != expected_runtime.iter().copied().collect()
    {
        return Err(format!(
            "{layer} source receipt does not match its exact build profile, features, and tools contract"
        )
        .into());
    }
    Ok(())
}

fn contract_for_receipt(receipt: &SourceReceipt) -> Result<LayerSourceContract, Box<dyn Error>> {
    ["tests", "simulator", "tla", "maelstrom"]
        .into_iter()
        .find_map(|layer| {
            let contract = layer_contract(layer).ok()?;
            verify_layer_contract(layer, receipt).ok().map(|()| contract)
        })
        .ok_or_else(|| {
            "source receipt does not match any reviewed layer build profile, features, and tools contract"
                .into()
        })
}

fn layer_contract(layer: &str) -> Result<LayerSourceContract, Box<dyn Error>> {
    match layer {
        "tests" => Ok(LayerSourceContract {
            build_profile: "test",
            features: &["no-default-features"],
            tools: &[],
            script_runtime: false,
        }),
        "simulator" => Ok(LayerSourceContract {
            build_profile: "release-and-test",
            features: &["internal-test-hooks"],
            tools: &[],
            script_runtime: false,
        }),
        "tla" => Ok(LayerSourceContract {
            build_profile: "tla",
            features: &[],
            tools: &["java"],
            script_runtime: false,
        }),
        "maelstrom" => Ok(LayerSourceContract {
            build_profile: "maelstrom-debug",
            features: &[],
            tools: &["java", "maelstrom", "dot", "gnuplot"],
            script_runtime: true,
        }),
        _ => Err(format!("unsupported source profile for layer {layer}").into()),
    }
}

pub(super) fn maelstrom_jar_path(executable: &Path) -> Result<PathBuf, Box<dyn Error>> {
    let executable = fs::canonicalize(executable)?;
    Ok(executable
        .parent()
        .ok_or("Maelstrom launcher has no installation directory")?
        .join("lib/maelstrom.jar"))
}

pub(super) fn tool_path(name: &str) -> Option<PathBuf> {
    find_tool(name)
}

fn reviewed_generated_output(path: &Path) -> bool {
    let components = path
        .components()
        .filter_map(|component| match component {
            Component::Normal(value) => Some(value.to_string_lossy()),
            _ => None,
        })
        .collect::<Vec<_>>();
    matches!(components.as_slice(), [first, ..] if first == "target" || first == "store")
        || matches!(components.as_slice(), [first, second, ..]
            if (first == "artifacts"
                && (second == "invariants" || reviewed_tla_evidence_artifact(second)))
                || (first == "bench-compare" && second == "target")
                || (first == "fuzz" && second == "target")
                || (first == "tools" && second == "cache"))
        || matches!(components.as_slice(), [first, second, third, ..]
            if first == "crates" && second == "rafter-invariants" && third == "target")
        || matches!(components.as_slice(), [first, second, rest @ ..]
            if first == "specs" && second == "tla" && rest.iter().any(|value| value == "states"))
        || components.iter().any(|value| value == "__pycache__")
        || path.extension().is_some_and(|extension| extension == "pyc")
}

#[cfg(test)]
#[path = "source_identity_tests.rs"]
mod identity_tests;
