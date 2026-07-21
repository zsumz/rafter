//! Plan, producer invocation, and source-provenance acceptance.

use std::{collections::BTreeMap, path::Path};

use crate::{
    contract::{
        catalog::EvidenceDescriptor,
        profile::{EvidenceLayer, ProfileContract, RunnerContract},
    },
    evidence::ResultBundle,
};

use super::{checks, runner, structure};

pub(super) fn validate(
    bundle: &ResultBundle,
    layer: EvidenceLayer,
    profile_contract: &ProfileContract,
    runner_contract: &RunnerContract,
    expected: &BTreeMap<String, &EvidenceDescriptor>,
) -> Result<(), &'static str> {
    validate_provenance(bundle, profile_contract)?;
    checks::validate(bundle, runner_contract)?;
    runner::validate(layer, bundle, runner_contract, expected)
}

fn validate_provenance(
    bundle: &ResultBundle,
    profile_contract: &ProfileContract,
) -> Result<(), &'static str> {
    if bundle.execution.plan.schema_version != crate::evidence::PLAN_SCHEMA_VERSION
        || bundle.execution.plan.profile != bundle.profile
        || bundle.execution.plan.contract != *profile_contract
        || !structure::valid_plan_input(&bundle.execution.plan.registry)
        || !structure::valid_plan_input(&bundle.execution.plan.manifest)
        || !structure::valid_plan_input(&bundle.execution.plan.result_schema)
        || !structure::valid_plan_input(&bundle.execution.plan.verdict_schema)
    {
        return Err("hashed execution plan does not match profile contract");
    }
    if bundle.execution.invocation.program.trim().is_empty()
        || !structure::is_sha256(&bundle.execution.invocation.program_sha256)
        || bundle.execution.invocation.arguments.is_empty()
        || !bundle.execution.invocation.launchers.is_empty()
        || !Path::new(&bundle.execution.invocation.program).is_absolute()
        || !Path::new(&bundle.execution.invocation.current_dir).is_absolute()
        || Path::new(&bundle.execution.invocation.program)
            != crate::provenance::image::image_path(
                Path::new(&bundle.execution.invocation.current_dir),
                &bundle.execution.invocation.program_sha256,
            )
        || !crate::provenance::invocation::environment_matches_digest(
            &bundle.execution.invocation.environment,
            &bundle.execution.invocation.environment_sha256,
        )
        || !structure::is_sha256(&bundle.execution.invocation.environment_sha256)
    {
        return Err("actual producer invocation provenance is incomplete");
    }
    validate_producer_invocation(bundle)?;
    if bundle.execution.source.commit != bundle.source_ref
        || !bundle.execution.source.clean
        || bundle.execution.source.tree.trim().is_empty()
        || !structure::is_sha256(&bundle.execution.source.cargo_lock_sha256)
        || bundle.execution.source.cargo.trim().is_empty()
        || !structure::is_sha256(&bundle.execution.source.cargo_sha256)
        || !structure::is_sha256(&bundle.execution.source.cargo_config_sha256)
        || bundle.execution.source.rustc.trim().is_empty()
        || !structure::is_sha256(&bundle.execution.source.rustc_sha256)
        || bundle.execution.source.target.trim().is_empty()
        || bundle.execution.source.build_profile.trim().is_empty()
        || bundle
            .execution
            .source
            .tools
            .values()
            .any(|tool| tool.version.trim().is_empty() || !structure::is_sha256(&tool.sha256))
        || bundle.execution.source.process_runtime.is_empty()
        || bundle
            .execution
            .source
            .process_runtime
            .values()
            .any(|executable| {
                !Path::new(&executable.program).is_absolute()
                    || !structure::is_sha256(&executable.sha256)
            })
        || !structure::is_sha256(&bundle.execution.source.environment_sha256)
    {
        return Err("source/toolchain provenance is incomplete or does not match source_ref");
    }
    Ok(())
}

fn validate_producer_invocation(bundle: &ResultBundle) -> Result<(), &'static str> {
    let binaries = bundle
        .execution
        .artifacts
        .iter()
        .filter(|artifact| artifact.kind == "producer-binary")
        .collect::<Vec<_>>();
    let [binary] = binaries.as_slice() else {
        return Err("producer invocation requires exactly one binary artifact");
    };
    if bundle.execution.producer.binding != crate::provenance::image::PRODUCER_BINDING
        || bundle.execution.producer.executable.kind != "producer-binary"
        || &bundle.execution.producer.executable != *binary
        || binary.sha256 != bundle.execution.invocation.program_sha256
    {
        return Err("producer invocation binary does not match its artifact");
    }
    let arguments = &bundle.execution.invocation.arguments;
    let profile = unique_argument(arguments, "--profile");
    let layer = unique_argument(arguments, "--layer");
    let command_matches = match arguments.first().map(String::as_str) {
        Some("run") => {
            profile == Some(bundle.profile.as_str()) && layer == Some(bundle.runner.as_str())
        }
        Some("run-all") => profile == Some(bundle.profile.as_str()) && layer.is_none(),
        _ => false,
    };
    if !command_matches {
        return Err("producer invocation does not select this profile and layer");
    }
    Ok(())
}

fn unique_argument<'a>(arguments: &'a [String], name: &str) -> Option<&'a str> {
    let values = arguments
        .windows(2)
        .filter(|pair| pair[0] == name)
        .map(|pair| pair[1].as_str())
        .collect::<Vec<_>>();
    match values.as_slice() {
        [value] => Some(*value),
        _ => None,
    }
}
