//! Deterministic evidence producers and their runtime adapters.

pub(crate) mod artifact;
mod maelstrom;
mod maelstrom_binding;
pub(crate) use crate::evidence::format::maelstrom as maelstrom_edn;
mod maelstrom_exec;
mod maelstrom_scenario;
mod maelstrom_tool;
pub(crate) mod process;
mod simulator;
mod simulator_model;
pub(crate) mod source;
mod test_compile;
pub(crate) mod test_exec;
mod test_runner;
mod tla;
pub(crate) mod tla_checkpoint;
mod tla_contract;
mod tla_exec;
pub(crate) use crate::evidence::format::tla as tla_output;
#[cfg(test)]
mod unit_tests;

#[cfg(test)]
mod filesystem_integration_tests;

#[cfg(test)]
pub(crate) use simulator::evaluate_model_fixture;
pub(crate) use simulator_model::{
    canonical_check_id, expected_scheduled_seeds, expected_scheduled_seeds_with_count,
};
#[cfg(all(test, unix))]
pub(crate) use simulator_model::{
    later_launch_error_fixture_at, timed_out_zero_exit_fixture_at, SimulatorFixtureInvocation,
};
#[cfg(test)]
pub(crate) use tla_contract::fetch_tool_at as fetch_tla_tool_at;
pub(crate) use tla_contract::java_major;

use std::collections::BTreeSet;
use std::{error::Error, io::Write, path::PathBuf};

use crate::{
    evidence::{ExecutionPlanReceipt, InvocationReceipt, ProducerBindingReceipt, ResultBundle},
    execution::filesystem,
    plan::{capture_invocation, CapturedInvocation, ExecutionPlan, PlanOptions},
};

#[derive(Clone, Debug)]
/// Input paths and selected contract for one deterministic evidence producer.
pub struct ProducerOptions {
    pub profile: String,
    pub layer: String,
    pub registry: PathBuf,
    pub manifest: PathBuf,
    pub output_dir: PathBuf,
}

#[derive(Clone, Debug)]
/// Written producer receipt and whether every evidence result passed.
pub struct ProducerOutcome {
    pub path: PathBuf,
    pub all_passed: bool,
}

pub(super) struct ProducerContext<'a> {
    pub plan: &'a ExecutionPlanReceipt,
    pub invocation: &'a InvocationReceipt,
    pub producer: &'a ProducerBindingReceipt,
}

/// Executes one profile layer and writes its strict result bundle.
///
/// # Errors
///
/// Returns an error when the repository is dirty, the producer contract is
/// invalid, the selected layer is unsupported, or the receipt cannot be
/// written. Individual check failures are represented inside the receipt.
pub fn produce(options: &ProducerOptions) -> Result<ProducerOutcome, Box<dyn Error>> {
    let plan = ExecutionPlan::load(&PlanOptions {
        profile: options.profile.clone(),
        registry: options.registry.clone(),
        manifest: options.manifest.clone(),
    })?;
    let invocation = capture_invocation()?;
    produce_with_plan(&plan, &options.layer, &options.output_dir, &invocation)
}

/// Executes one layer from an already loaded immutable plan.
///
/// # Errors
///
/// Returns an error when the selected layer cannot produce a complete receipt.
pub(crate) fn produce_with_plan(
    plan: &ExecutionPlan,
    layer: &str,
    output_dir: &std::path::Path,
    invocation: &CapturedInvocation,
) -> Result<ProducerOutcome, Box<dyn Error>> {
    artifact::validate_output_dir(output_dir)?;
    crate::plan::verify_plan_input(&plan.receipt.registry, std::path::Path::new("."))?;
    crate::plan::verify_plan_input(&plan.receipt.manifest, std::path::Path::new("."))?;
    let contract = plan.contract();
    if !contract
        .required_layers
        .iter()
        .any(|required| required.as_str() == layer)
    {
        return Err(format!(
            "layer {} is not required by profile {}",
            layer, plan.receipt.profile
        )
        .into());
    }
    let runner = contract
        .runners
        .get(layer)
        .ok_or_else(|| format!("profile {} omitted runner {layer}", plan.receipt.profile))?;
    let _process_budget = process::LayerBudgetGuard::enter(&plan.receipt.profile, layer, runner)?;
    let path = output_dir.join(format!("{}-{layer}.json", plan.receipt.profile));
    filesystem::HeldDirectory::workspace()?.remove_file_if_exists(&path)?;
    let executable = artifact::capture_bytes(
        output_dir,
        std::path::Path::new(&format!("{}-{layer}/inputs", plan.receipt.profile)),
        &invocation.program_bytes,
        "producer-binary",
    )?;
    let producer = ProducerBindingReceipt {
        binding: crate::provenance::image::PRODUCER_BINDING.to_owned(),
        executable,
    };
    let source = source::capture_for_layer(layer)?;
    let context = ProducerContext {
        plan: &plan.receipt,
        invocation: &invocation.receipt,
        producer: &producer,
    };
    let mut bundle = match layer {
        "tests" => test_runner::run(
            &plan.catalog,
            contract,
            &plan.receipt.profile,
            source,
            output_dir,
            &context,
        )?,
        "simulator" => simulator::run(
            &plan.catalog,
            contract,
            &plan.receipt.profile,
            source,
            output_dir,
            &context,
        )?,
        "tla" => tla::run(
            &plan.catalog,
            contract,
            &plan.receipt.profile,
            source,
            output_dir,
            &context,
        )?,
        "maelstrom" => maelstrom::run(
            &plan.catalog,
            contract,
            &plan.receipt.profile,
            source,
            output_dir,
            &context,
        )?,
        layer => return Err(format!("producer for layer {layer} is not implemented").into()),
    };
    bundle.execution.artifacts.push(producer.executable.clone());
    let expected_ids = plan
        .catalog
        .required_evidence(contract)
        .into_values()
        .flatten()
        .filter(|descriptor| descriptor.layer == layer)
        .map(|descriptor| descriptor.evidence_id())
        .collect::<BTreeSet<_>>();
    let result_ids = bundle
        .results
        .iter()
        .map(|result| result.evidence_id.clone())
        .collect::<BTreeSet<_>>();
    let all_passed = !expected_ids.is_empty()
        && result_ids == expected_ids
        && bundle.results.len() == result_ids.len()
        && bundle.execution.checks.len() >= contract.runners[layer].minimum_observed_checks
        && bundle
            .results
            .iter()
            .all(|result| result.status == crate::EvidenceStatus::Pass);
    let path = publish_bundle(&bundle, output_dir, &plan.receipt.profile, layer)?;
    Ok(ProducerOutcome { path, all_passed })
}

fn publish_bundle(
    bundle: &ResultBundle,
    output_dir: &std::path::Path,
    profile: &str,
    layer: &str,
) -> Result<PathBuf, Box<dyn Error>> {
    if layer == "tla" {
        process::ensure_total_deadline(profile, layer, "TLA receipt serialization", true)?;
    }
    let staged = stage_bundle(bundle, output_dir)?;
    if layer == "tla" {
        process::ensure_total_deadline(profile, layer, "TLA receipt publication", false)?;
    }
    staged.publish()
}

struct StagedBundle {
    workspace: filesystem::HeldDirectory,
    temporary: PathBuf,
    path: PathBuf,
    published: bool,
}

impl StagedBundle {
    fn publish(mut self) -> Result<PathBuf, Box<dyn Error>> {
        self.workspace.rename(&self.temporary, &self.path)?;
        self.published = true;
        Ok(self.path.clone())
    }
}

impl Drop for StagedBundle {
    fn drop(&mut self) {
        if !self.published {
            let _ = self.workspace.remove_file_if_exists(&self.temporary);
        }
    }
}

fn stage_bundle(
    bundle: &ResultBundle,
    output_dir: &std::path::Path,
) -> Result<StagedBundle, Box<dyn Error>> {
    crate::evidence::validate_result_bundle(bundle)?;
    let workspace = filesystem::HeldDirectory::workspace()?;
    workspace.create_dir_all(output_dir)?;
    let path = output_dir.join(format!("{}-{}.json", bundle.profile, bundle.runner));
    let temporary = output_dir.join(format!(
        ".{}-{}.json.tmp-{}",
        bundle.profile,
        bundle.runner,
        std::process::id()
    ));
    workspace.remove_file_if_exists(&temporary)?;
    let mut file = workspace.create_new_file(&temporary)?;
    file.write_all(format!("{}\n", serde_json::to_string_pretty(bundle)?).as_bytes())?;
    file.sync_all()?;
    Ok(StagedBundle {
        workspace,
        temporary,
        path,
        published: false,
    })
}
