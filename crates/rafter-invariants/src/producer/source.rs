//! Producer-owned source policy, tool capture, and receipt construction.

use std::{
    collections::BTreeSet,
    env,
    error::Error,
    fs,
    path::{Component, Path, PathBuf},
};

use sha2::{Digest, Sha256};

use crate::{
    evidence::{SourceMaterializationReceipt, SourceReceipt, ToolReceipt},
    provenance::source::{
        observe_checkout_with, CheckoutCommandRunner, CheckoutObservation, CommandOutput,
        GeneratedOutputPolicy,
    },
};

use super::process;

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

const TOOL_IDENTITY_PROBES: &[(&str, &[&str])] = &[
    ("java", &["-version"]),
    ("maelstrom", &["serve", "--help"]),
    ("dot", &["-V"]),
    ("gnuplot", &["--version"]),
];

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
    let environment_sha256 = crate::provenance::invocation::digest_environment(&environment)?;
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

fn capture_tool(
    name: &str,
    root: &Path,
    runner: ProducerCommandRunner,
) -> Result<ToolReceipt, Box<dyn Error>> {
    let executable = find_tool(name).ok_or_else(|| format!("{name} is not present on PATH"))?;
    let arguments = tool_identity_arguments(name)?;
    let output = runner.run(name, arguments, root)?;
    let version = bind_adjacent_tool_inputs(
        name,
        tool_version_output(name, &output.stdout, &output.stderr)?,
        &executable,
    )?;
    Ok(ToolReceipt {
        version,
        sha256: file_sha256(&executable)?,
    })
}

fn bind_adjacent_tool_inputs(
    name: &str,
    version: String,
    executable: &Path,
) -> Result<String, Box<dyn Error>> {
    if name != "maelstrom" {
        return Ok(version);
    }
    let executable = fs::canonicalize(executable)?;
    let jar = maelstrom_jar_path(&executable)?;
    let jar_sha256 = file_sha256(&jar).map_err(|error| {
        format!(
            "bind Maelstrom launcher {} to adjacent {}: {error}",
            executable.display(),
            jar.display()
        )
    })?;
    Ok(format!(
        "{version}\nrafter-adjacent-lib/maelstrom.jar-sha256: {jar_sha256}"
    ))
}

pub(super) fn maelstrom_jar_path(executable: &Path) -> Result<PathBuf, Box<dyn Error>> {
    let executable = fs::canonicalize(executable)?;
    Ok(executable
        .parent()
        .ok_or("Maelstrom launcher has no installation directory")?
        .join("lib/maelstrom.jar"))
}

fn tool_identity_arguments(name: &str) -> Result<&'static [&'static str], Box<dyn Error>> {
    TOOL_IDENTITY_PROBES
        .iter()
        .find_map(|(tool, arguments)| (*tool == name).then_some(*arguments))
        .ok_or_else(|| format!("no reviewed identity probe is registered for {name}").into())
}

fn tool_version_output(name: &str, stdout: &str, stderr: &str) -> Result<String, Box<dyn Error>> {
    let value = format!("{stdout}{stderr}").trim().to_owned();
    if value.is_empty() {
        return Err(format!("{name} produced empty identity output").into());
    }
    Ok(value)
}

fn find_tool(name: &str) -> Option<PathBuf> {
    env::split_paths(&env::var_os("PATH")?)
        .map(|directory| directory.join(name))
        .find(|candidate| candidate.is_file())
}

pub(super) fn tool_path(name: &str) -> Option<PathBuf> {
    find_tool(name)
}

fn file_sha256(path: &Path) -> Result<String, Box<dyn Error>> {
    Ok(format!("{:x}", Sha256::digest(fs::read(path)?)))
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

fn reviewed_tla_evidence_artifact(name: &str) -> bool {
    matches!(
        name,
        "tla-log"
            | "tla.log"
            | "tla-trace-log"
            | "tla-tool"
            | "tla-spec"
            | "tla-trace-spec"
            | "tla-detector-spec"
            | "tla-runner"
            | "tla-tool-asset-id"
            | "tla-tool-checksums"
            | "tla-config"
            | "tla-trace-config"
            | "tla-detector-config"
            | "tla-mutation-log"
            | "tla-producer"
            | "tla-checkpoint-contract"
            | "tla-checkpoint-inventory"
            | "tla-checkpoint-recovered-contract"
            | "tla-checkpoint-recovered-inventory"
            | "tla-checkpoint-recovery-report"
    ) || crate::producer::tla_output::DETECTOR_PROBES
        .into_iter()
        .any(|probe| {
            crate::producer::tla_output::detector_log_kind(probe)
                .is_some_and(|kind| normalize_fixture_artifact_name(&kind) == name)
                || crate::producer::tla_output::detector_config_kind(probe)
                    .is_some_and(|kind| normalize_fixture_artifact_name(&kind) == name)
        })
}

fn normalize_fixture_artifact_name(kind: &str) -> String {
    kind.replace(':', "-")
}

#[cfg(test)]
#[path = "source_identity_tests.rs"]
mod identity_tests;
