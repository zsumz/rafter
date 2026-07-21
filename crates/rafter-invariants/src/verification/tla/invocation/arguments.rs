//! Reconstruction of exact TLC arguments, including held directory descriptors.

use std::path::Path;

use crate::{
    evidence::{
        format::tla::checkpoint::{RecoveryReport, RECOVERY_REPORT_KIND},
        CheckReceipt, ResultBundle,
    },
    verification::{AggregateError, AuthenticatedArtifacts},
};

use super::super::{artifact::read_json_kind, source::configuration};

pub(super) struct InvocationTarget<'a> {
    pub(super) config: String,
    pub(super) module: &'a str,
    pub(super) workers: &'a str,
}

pub(super) fn expected(
    bundle: &ResultBundle,
    check: &CheckReceipt,
    label: &str,
    producer_repository: &Path,
    target: InvocationTarget<'_>,
    authenticated: &AuthenticatedArtifacts,
) -> Result<Vec<String>, AggregateError> {
    let source_prefix = bundle.source_ref.get(..12).unwrap_or(&bundle.source_ref);
    let checkpointed = label == "model-check"
        && bundle.execution.plan.contract.runners["tla"]
            .configuration
            .contains_key("checkpoint_minutes");
    let state_dir = if checkpointed {
        producer_repository
            .join("target/rafter-invariants/tla-checkpoint")
            .join(&bundle.profile)
            .join("states")
    } else {
        producer_repository
            .join("target/rafter-invariants/tla")
            .join(source_prefix)
            .join(&bundle.profile)
            .join(label)
    };
    let mut arguments = Vec::new();
    if label == "model-check" {
        if let Some(max_heap) = bundle.execution.plan.contract.runners["tla"]
            .configuration
            .get("max_heap")
        {
            arguments.push(format!("-Xmx{max_heap}"));
        }
    }
    arguments.extend([
        "-XX:+UseParallelGC".to_owned(),
        "-cp".to_owned(),
        producer_repository
            .join("tools/cache/tla2tools.jar")
            .to_string_lossy()
            .into_owned(),
        "tlc2.TLC".to_owned(),
        "-tool".to_owned(),
        "-workers".to_owned(),
        target.workers.to_owned(),
        "-seed".to_owned(),
        configuration(bundle, "seed")?.to_owned(),
        "-fp".to_owned(),
        "0".to_owned(),
    ]);
    if label == "model-check" {
        if let Some(fp_mem) = bundle.execution.plan.contract.runners["tla"]
            .configuration
            .get("fp_mem")
        {
            arguments.extend(["-fpmem".to_owned(), fp_mem.clone()]);
        }
    }
    arguments.extend([
        "-metadir".to_owned(),
        state_dir.to_string_lossy().into_owned(),
    ]);
    if checkpointed {
        arguments.extend([
            "-checkpoint".to_owned(),
            configuration(bundle, "checkpoint_minutes")?.to_owned(),
            "-gzip".to_owned(),
        ]);
        let report: RecoveryReport = read_json_kind(check, RECOVERY_REPORT_KIND, authenticated)?;
        if let Some(checkpoint) = report.recovered_checkpoint {
            arguments.extend([
                "-recover".to_owned(),
                state_dir.join(checkpoint).to_string_lossy().into_owned(),
            ]);
        }
    }
    arguments.extend([
        "-config".to_owned(),
        target.config,
        target.module.to_owned(),
    ]);
    Ok(arguments)
}

pub(super) fn matches(expected: &[String], observed: &[String]) -> bool {
    if expected.len() != observed.len() {
        return false;
    }

    let mut descriptors = Vec::new();
    for (index, (expected_argument, observed_argument)) in expected.iter().zip(observed).enumerate()
    {
        let descriptor_argument =
            index > 0 && matches!(expected[index - 1].as_str(), "-metadir" | "-recover");
        if descriptor_argument {
            let Some(descriptor) = linux_descriptor(observed_argument) else {
                return false;
            };
            if descriptors.contains(&descriptor) {
                return false;
            }
            descriptors.push(descriptor);
        } else if expected_argument != observed_argument {
            return false;
        }
    }
    true
}

fn linux_descriptor(argument: &str) -> Option<u32> {
    let descriptor = argument.strip_prefix("/proc/self/fd/")?;
    if descriptor.is_empty()
        || (descriptor.len() > 1 && descriptor.starts_with('0'))
        || !descriptor.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    descriptor.parse::<u32>().ok().filter(|fd| *fd >= 3)
}

#[cfg(test)]
#[path = "arguments_tests.rs"]
mod tests;
