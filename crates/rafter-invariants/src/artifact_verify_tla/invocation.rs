use std::{fs, path::Path};

use crate::producer::tla_checkpoint::{RecoveryReport, RECOVERY_REPORT_KIND};
use crate::producer::tla_output::{
    detector_config_kind, detector_label, probe_slug, DETECTOR_PROBES,
};
use crate::{aggregate::AggregateError, ResultBundle};

use super::{configuration, has_kind, read_json_kind, read_kind, unique_artifact};

pub(super) fn read_process_log(
    bundle: &ResultBundle,
    check: &crate::CheckReceipt,
    kind: &str,
    label: &str,
    root: &Path,
) -> Result<crate::producer::ProcessLog, AggregateError> {
    let log = read_bound_process_log(check, kind, label, root)?;
    verify_tla_invocation(bundle, check, label, &log.invocation, root)?;
    Ok(log)
}

pub(super) fn read_bound_process_log(
    check: &crate::CheckReceipt,
    kind: &str,
    label: &str,
    root: &Path,
) -> Result<crate::producer::ProcessLog, AggregateError> {
    let source = read_kind(check, kind, root)?;
    let log: crate::producer::ProcessLog = serde_json::from_str(&source)
        .map_err(|error| AggregateError::new(format!("parse TLA process log: {error}")))?;
    let valid_termination = log.termination.as_ref().is_some_and(|termination| {
        termination.process_group
            && termination.grace_ms == 30_000
            && ((!log.timed_out && !termination.term_signal_sent && !termination.kill_signal_sent)
                || (log.timed_out && termination.term_signal_sent))
    });
    if log.schema_version != 3
        || log.label != label
        || !log.has_complete_invocation()
        || !valid_termination
    {
        return Err(AggregateError::new(format!(
            "TLA process log {kind} has the wrong schema, label, invocation, or group termination receipt"
        )));
    }
    Ok(log)
}

pub(super) fn optional_process_log(
    bundle: &ResultBundle,
    check: &crate::CheckReceipt,
    kind: &str,
    label: &str,
    root: &Path,
) -> Result<Option<crate::producer::ProcessLog>, AggregateError> {
    has_kind(check, kind)?
        .then(|| read_process_log(bundle, check, kind, label, root))
        .transpose()
}

struct InvocationTarget<'a> {
    config: String,
    module: &'a str,
    workers: &'a str,
}

fn verify_tla_invocation(
    bundle: &ResultBundle,
    check: &crate::CheckReceipt,
    label: &str,
    observed: &crate::InvocationReceipt,
    root: &Path,
) -> Result<(), AggregateError> {
    let repository = fs::canonicalize(root)
        .map_err(|error| AggregateError::new(format!("canonicalize TLA root: {error}")))?;
    let target = match label {
        "model-check" => InvocationTarget {
            config: configuration(bundle, "config")?.to_owned(),
            module: "Raft.tla",
            workers: configuration(bundle, "workers")?,
        },
        "trace-sample" => InvocationTarget {
            config: "RaftMembershipTraceSample.cfg".to_owned(),
            module: "RaftMembershipTraceSample.tla",
            workers: "1",
        },
        _ => {
            let probe = DETECTOR_PROBES
                .iter()
                .find(|probe| detector_label(**probe).as_deref() == Some(label))
                .ok_or_else(|| AggregateError::new(format!("unknown TLA log label {label}")))?;
            let config_kind = detector_config_kind(*probe).ok_or_else(|| {
                AggregateError::new(format!(
                    "unregistered TLA detector probe {}",
                    probe_slug(*probe)
                ))
            })?;
            let artifact = unique_artifact(check, &config_kind)?;
            let config = fs::canonicalize(root.join(&artifact.path))
                .map_err(|error| {
                    AggregateError::new(format!(
                        "canonicalize TLA detector config {}: {error}",
                        artifact.path
                    ))
                })?
                .to_string_lossy()
                .into_owned();
            InvocationTarget {
                config,
                module: "RafterInvariantDetectorNegative.tla",
                workers: "1",
            }
        }
    };
    let current_dir = repository.join("specs/tla/raft");
    let arguments = expected_tla_arguments(bundle, check, label, root, &repository, target)?;
    let java_sha = bundle
        .execution
        .source
        .tools
        .get("java")
        .map(|tool| tool.sha256.as_str());
    if observed.program != "java"
        || java_sha != Some(observed.program_sha256.as_str())
        || observed.arguments != arguments
        || observed.current_dir != current_dir.to_string_lossy()
        || observed.environment_sha256 != bundle.execution.source.environment_sha256
    {
        return Err(AggregateError::new(format!(
            "TLA process log {label} does not match the exact invocation plan"
        )));
    }
    Ok(())
}

fn expected_tla_arguments(
    bundle: &ResultBundle,
    check: &crate::CheckReceipt,
    label: &str,
    root: &Path,
    repository: &Path,
    target: InvocationTarget<'_>,
) -> Result<Vec<String>, AggregateError> {
    let source_prefix = bundle.source_ref.get(..12).unwrap_or(&bundle.source_ref);
    let checkpointed = label == "model-check"
        && bundle.execution.plan.contract.runners["tla"]
            .configuration
            .contains_key("checkpoint_minutes");
    let state_dir = if checkpointed {
        repository
            .join("target/rafter-invariants/tla-checkpoint")
            .join(&bundle.profile)
            .join("states")
    } else {
        repository
            .join("target/rafter-invariants/tla")
            .join(source_prefix)
            .join(&bundle.profile)
            .join(label)
    };
    let mut arguments = Vec::new();
    if checkpointed {
        arguments.push(format!("-Xmx{}", configuration(bundle, "max_heap")?));
    }
    arguments.extend([
        "-XX:+UseParallelGC".to_owned(),
        "-cp".to_owned(),
        repository
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
        "-metadir".to_owned(),
        state_dir.to_string_lossy().into_owned(),
    ]);
    if checkpointed {
        arguments.extend([
            "-checkpoint".to_owned(),
            configuration(bundle, "checkpoint_minutes")?.to_owned(),
            "-gzip".to_owned(),
        ]);
        let report: RecoveryReport = read_json_kind(check, RECOVERY_REPORT_KIND, root)?;
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
