//! Exact TLA+ process invocation and source-checkout binding.

use std::{
    fs,
    path::{Path, PathBuf},
};

use crate::{
    evidence::{
        format::tla::{detector_config_kind, detector_label, probe_slug, DETECTOR_PROBES},
        CheckReceipt, InvocationReceipt, ResultBundle,
    },
    verification::{AggregateError, AuthenticatedArtifacts},
};

use super::{
    artifact::{has_kind, read_kind, unique_artifact},
    source::configuration,
};

mod arguments;
mod repository;

pub(super) fn obligation_id(label: &str) -> Option<&str> {
    label.strip_prefix("obligation-").filter(|id| !id.is_empty())
}

fn contract_obligation<'a>(
    bundle: &'a ResultBundle,
    id: &str,
) -> Result<&'a crate::contract::profile::ProofObligationContract, AggregateError> {
    bundle
        .execution
        .plan
        .contract
        .runners
        .get(&bundle.runner)
        .ok_or_else(|| {
            AggregateError::new(format!("execution plan omitted runner {}", bundle.runner))
        })?
        .obligations
        .iter()
        .find(|obligation| obligation.id == id)
        .ok_or_else(|| {
            AggregateError::new(format!("TLA log names unpinned proof obligation {id}"))
        })
}

pub(super) fn read_process_log(
    bundle: &ResultBundle,
    check: &CheckReceipt,
    kind: &str,
    label: &str,
    root: &Path,
    producer_repository: &Path,
    authenticated: &AuthenticatedArtifacts,
) -> Result<crate::evidence::format::process::ProcessLog, AggregateError> {
    let log = read_bound_process_log(check, kind, label, authenticated)?;
    verify_tla_invocation(
        bundle,
        check,
        label,
        &log.invocation,
        root,
        producer_repository,
        authenticated,
    )?;
    Ok(log)
}

pub(super) fn read_initial_process_log(
    bundle: &ResultBundle,
    check: &CheckReceipt,
    kind: &str,
    label: &str,
    root: &Path,
    authenticated: &AuthenticatedArtifacts,
) -> Result<(crate::evidence::format::process::ProcessLog, PathBuf), AggregateError> {
    let log = read_bound_process_log(check, kind, label, authenticated)?;
    let producer_repository = repository::from_current_dir(&log.invocation.current_dir)?;
    verify_tla_invocation(
        bundle,
        check,
        label,
        &log.invocation,
        root,
        &producer_repository,
        authenticated,
    )?;
    Ok((log, producer_repository))
}

pub(super) fn read_bound_process_log(
    check: &CheckReceipt,
    kind: &str,
    label: &str,
    authenticated: &AuthenticatedArtifacts,
) -> Result<crate::evidence::format::process::ProcessLog, AggregateError> {
    let source = read_kind(check, kind, authenticated)?;
    let log = crate::evidence::format::process::parse_tla_v4(&source)
        .map_err(|error| AggregateError::new(format!("parse TLA process log: {error}")))?;
    let valid_termination = log.termination.as_ref().is_some_and(|termination| {
        termination.process_group
            && termination.grace_ms == 30_000
            && ((!log.timed_out && !termination.term_signal_sent && !termination.kill_signal_sent)
                || (log.timed_out && termination.term_signal_sent))
    });
    if log.label != label
        || !crate::verification::process_invocation_is_complete(&log.invocation)
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
    check: &CheckReceipt,
    kind: &str,
    label: &str,
    root: &Path,
    producer_repository: &Path,
    authenticated: &AuthenticatedArtifacts,
) -> Result<Option<crate::evidence::format::process::ProcessLog>, AggregateError> {
    has_kind(check, kind)?
        .then(|| {
            read_process_log(
                bundle,
                check,
                kind,
                label,
                root,
                producer_repository,
                authenticated,
            )
        })
        .transpose()
}

fn verify_tla_invocation(
    bundle: &ResultBundle,
    check: &CheckReceipt,
    label: &str,
    observed: &InvocationReceipt,
    root: &Path,
    producer_repository: &Path,
    authenticated: &AuthenticatedArtifacts,
) -> Result<(), AggregateError> {
    if !crate::verification::process_invocation_matches_source(observed, &bundle.execution.source) {
        return Err(AggregateError::new(format!(
            "TLA process log {label} does not match the source-bound process runtime"
        )));
    }
    let repository = fs::canonicalize(root)
        .map_err(|error| AggregateError::new(format!("canonicalize TLA root: {error}")))?;
    let target = match label {
        "model-check" => arguments::InvocationTarget {
            config: configuration(bundle, "config")?.to_owned(),
            module: "Raft.tla",
            workers: configuration(bundle, "workers")?,
            seed: configuration(bundle, "seed")?.to_owned(),
            memory_profile: true,
        },
        "trace-sample" => arguments::InvocationTarget {
            config: "RaftMembershipTraceSample.cfg".to_owned(),
            module: "RaftMembershipTraceSample.tla",
            workers: "1",
            seed: configuration(bundle, "seed")?.to_owned(),
            memory_profile: false,
        },
        // The obligation's own configuration, seed, and inherited worker and
        // memory profile are read back from the pinned profile contract, never
        // from the observed argv. A receipt cannot vouch for an obligation the
        // profile never asked for.
        _ if obligation_id(label).is_some() => {
            let obligation = obligation_id(label)
                .map(|id| contract_obligation(bundle, id))
                .transpose()?
                .ok_or_else(|| AggregateError::new("TLA obligation label vanished".to_owned()))?;
            arguments::InvocationTarget {
                config: obligation.config.clone(),
                module: "Raft.tla",
                workers: configuration(bundle, "workers")?,
                seed: obligation.seed.clone(),
                memory_profile: true,
            }
        }
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
            let config = fs::canonicalize(root.join(&artifact.path)).map_err(|error| {
                AggregateError::new(format!(
                    "canonicalize TLA detector config {}: {error}",
                    artifact.path
                ))
            })?;
            let config = producer_repository
                .join(config.strip_prefix(&repository).map_err(|_| {
                    AggregateError::new(format!(
                        "TLA detector config escaped the aggregate checkout: {}",
                        artifact.path
                    ))
                })?)
                .to_string_lossy()
                .into_owned();
            arguments::InvocationTarget {
                config,
                module: "RafterInvariantDetectorNegative.tla",
                workers: "1",
                seed: configuration(bundle, "seed")?.to_owned(),
                memory_profile: false,
            }
        }
    };
    let current_dir = producer_repository.join("specs/tla/raft");
    let expected = arguments::expected(
        bundle,
        check,
        label,
        producer_repository,
        target,
        authenticated,
    )?;
    let java_sha = bundle
        .execution
        .source
        .tools
        .get("java")
        .map(|tool| tool.sha256.as_str());
    let mut mismatches = Vec::new();
    if observed.program != "java" {
        mismatches.push("program");
    }
    if java_sha != Some(observed.program_sha256.as_str()) {
        mismatches.push("program_sha256");
    }
    if !arguments::matches(&expected, &observed.arguments) {
        mismatches.push("arguments");
    }
    if observed.current_dir != current_dir.to_string_lossy() {
        mismatches.push("current_dir");
    }
    if observed.environment != bundle.execution.invocation.environment {
        mismatches.push("environment");
    }
    if observed.environment_sha256 != bundle.execution.invocation.environment_sha256 {
        mismatches.push("environment_sha256");
    }
    if !crate::provenance::invocation::environment_matches_digest(
        &observed.environment,
        &observed.environment_sha256,
    ) {
        mismatches.push("environment_digest");
    }
    if !mismatches.is_empty() {
        return Err(AggregateError::new(format!(
            "TLA process log {label} does not match the exact invocation plan: {}",
            mismatches.join(", ")
        )));
    }
    Ok(())
}
