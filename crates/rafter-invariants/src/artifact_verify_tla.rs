//! Acceptance of TLA+ model-check, trace, and detector evidence.

use std::{collections::BTreeMap, fs, path::Path};

use crate::producer::tla_checkpoint::{RecoveryReport, RecoveryStatus};
use crate::producer::tla_output::{
    detector_config_kind, detector_invariant, detector_label, detector_log_kind,
    detector_observation, mutation_suite_passed, parse_latest_progress, probe_slug,
    render_detector_config, DETECTOR_PROBES, MEMBERSHIP_TRACE_MIN_DEPTH,
    MEMBERSHIP_TRACE_MIN_DISTINCT_STATES, MUTATION_SUITE_ARTIFACT_KIND, MUTATION_SUITE_LABEL,
    REQUIRED_MODEL_TRANSITIONS,
};
use crate::{aggregate::AggregateError, CheckCompletion, EvidenceStatus, ResultBundle};

mod checkpoint;
mod invocation;

use checkpoint::verify_checkpoint;
use invocation::{
    optional_process_log, read_bound_process_log, read_initial_process_log, read_process_log,
};

pub(super) fn verify(bundle: &ResultBundle, root: &Path) -> Result<Vec<String>, AggregateError> {
    let check = bundle
        .execution
        .checks
        .first()
        .ok_or_else(|| AggregateError::new("TLA receipt has no check".to_owned()))?;
    let (trace, producer_repository) =
        read_initial_process_log(bundle, check, "tla-trace-log", "trace-sample", root)?;
    let config = read_kind(check, "tla-config", root)?;
    let detector_template = read_kind(check, "tla-detector-config", root)?;
    verify_source_binding(bundle, check, root)?;
    verify_tool_pin(bundle, check, root)?;
    let trace_summary = crate::producer::tla_output::parse(trace.stdout.as_bytes()).ok();
    let trace_passed = trace_summary.as_ref().is_some_and(|summary| {
        successful_log(&trace)
            && successful_summary(summary)
            && summary.distinct_states >= MEMBERSHIP_TRACE_MIN_DISTINCT_STATES
            && summary.search_depth >= MEMBERSHIP_TRACE_MIN_DEPTH
    });
    let (detector_observations, detectors_passed) = verify_detectors(
        bundle,
        check,
        root,
        &producer_repository,
        &detector_template,
    )?;
    if !trace_passed && has_kind(check, "tla-log")? {
        return Err(AggregateError::new(
            "TLA main log exists after a failed trace probe".to_owned(),
        ));
    }
    let main = optional_process_log(
        bundle,
        check,
        "tla-log",
        "model-check",
        root,
        &producer_repository,
    )?;
    let (main_summary, main_parse_diagnostic) = match main.as_ref() {
        Some(log) => match crate::producer::tla_output::parse(log.stdout.as_bytes()) {
            Ok(summary) => (Some(summary), None),
            Err(error) => {
                match crate::producer::tla_output::parse_complete_prefix(log.stdout.as_bytes()) {
                    Ok(summary) if summary.violated_invariant.is_some() => (
                        Some(summary),
                        Some(format!("parse TLA main output: {error}")),
                    ),
                    _ => (None, None),
                }
            }
        },
        None => (None, None),
    };
    let checkpoint = verify_checkpoint(
        bundle,
        check,
        root,
        main_summary
            .as_ref()
            .is_some_and(|summary| summary.violated_invariant.is_some()),
    )?;
    let (main_progress, progress_diagnostic) = timeout_progress(
        main.as_ref(),
        main_summary
            .as_ref()
            .is_some_and(|summary| summary.violated_invariant.is_some()),
    )?;
    let symbols = configured_invariants(&config);
    let derived = derive_observations(
        &symbols,
        trace_passed,
        detector_observations,
        checkpoint.as_ref(),
        main_progress,
        main.as_ref(),
        main_summary.as_ref(),
    );
    if check.observations != derived {
        return Err(AggregateError::new(
            "TLA receipt observations disagree with framed proof logs".to_owned(),
        ));
    }
    let violated = main_summary
        .as_ref()
        .and_then(|summary| summary.violated_invariant.as_deref());
    verify_counterexample_binding(bundle, violated)?;
    verify_completion(
        bundle,
        trace_passed,
        detectors_passed,
        checkpoint.as_ref(),
        main.as_ref(),
        main_summary.as_ref(),
    )?;
    Ok(main_parse_diagnostic
        .into_iter()
        .chain(progress_diagnostic)
        .collect())
}

fn derive_observations(
    symbols: &[String],
    trace_passed: bool,
    detector_observations: BTreeMap<String, u64>,
    checkpoint: Option<&RecoveryReport>,
    main_progress: Option<crate::producer::tla_output::TlcProgress>,
    main: Option<&crate::evidence::format::process::ProcessLog>,
    main_summary: Option<&crate::producer::tla_output::TlcSummary>,
) -> BTreeMap<String, u64> {
    let mut derived = BTreeMap::from([
        ("configured_invariants".to_owned(), symbols.len() as u64),
        ("tool_pin_verified".to_owned(), 1),
        ("trace_sample_passed".to_owned(), u64::from(trace_passed)),
    ]);
    if trace_passed {
        derived.extend(
            REQUIRED_MODEL_TRANSITIONS
                .into_iter()
                .map(|transition| (format!("transition_covered:{transition}"), 1)),
        );
    }
    derived.extend(detector_observations);
    if let Some(checkpoint) = checkpoint {
        derived.extend([
            ("checkpoint_enabled".to_owned(), 1),
            (
                "checkpoint_candidate_present".to_owned(),
                u64::from(checkpoint.candidate_present),
            ),
            (
                "checkpoint_compatible".to_owned(),
                u64::from(checkpoint.status != RecoveryStatus::Incompatible),
            ),
            (
                "checkpoint_recovery_attempted".to_owned(),
                u64::from(checkpoint.recovery_attempted),
            ),
        ]);
    }
    if main.is_some_and(|log| log.timed_out) {
        if let Some(progress) = main_progress {
            derived.extend([
                (
                    "progress_generated_states".to_owned(),
                    progress.generated_states,
                ),
                (
                    "progress_distinct_states".to_owned(),
                    progress.distinct_states,
                ),
                ("progress_states_left".to_owned(), progress.states_left),
                ("progress_depth".to_owned(), progress.depth),
            ]);
        }
    } else if let Some(summary) = main_summary {
        derived.extend([
            ("generated_states".to_owned(), summary.generated_states),
            ("distinct_states".to_owned(), summary.distinct_states),
            ("states_left_on_queue".to_owned(), summary.states_left),
            ("search_depth".to_owned(), summary.search_depth),
        ]);
        if main.is_some_and(|log| successful_log(log) && successful_summary(summary)) {
            for symbol in symbols.iter().filter(|symbol| symbol.as_str() != "TypeOK") {
                derived.insert(format!("checked:{symbol}"), 1);
            }
        }
    }
    derived
}

fn timeout_progress(
    main: Option<&crate::evidence::format::process::ProcessLog>,
    main_has_violation: bool,
) -> Result<
    (
        Option<crate::producer::tla_output::TlcProgress>,
        Option<String>,
    ),
    AggregateError,
> {
    let Some(log) = main.filter(|log| log.timed_out) else {
        return Ok((None, None));
    };
    let progress = match parse_latest_progress(log.stdout.as_bytes()) {
        Ok(progress) => progress,
        Err(error) if main_has_violation => {
            return Ok((None, Some(format!("parse timed-out TLA progress: {error}"))));
        }
        Err(error) => {
            return Err(AggregateError::new(format!(
                "parse timed-out TLA progress: {error}"
            )));
        }
    };
    if main_has_violation || progress.is_some() {
        return Ok((progress, None));
    }
    Err(AggregateError::new(
        "timed-out TLA log omitted a complete progress frame".to_owned(),
    ))
}

fn verify_detectors(
    bundle: &ResultBundle,
    check: &crate::CheckReceipt,
    root: &Path,
    producer_repository: &Path,
    template: &str,
) -> Result<(BTreeMap<String, u64>, bool), AggregateError> {
    let mut observations = BTreeMap::new();
    let mut all_passed = true;
    for probe in DETECTOR_PROBES {
        let identity = probe_slug(probe);
        let config_kind = detector_config_kind(probe).ok_or_else(|| {
            AggregateError::new(format!("unregistered TLA detector probe {identity}"))
        })?;
        let log_kind = detector_log_kind(probe).ok_or_else(|| {
            AggregateError::new(format!("unregistered TLA detector probe {identity}"))
        })?;
        let observation = detector_observation(probe.predicate).ok_or_else(|| {
            AggregateError::new(format!(
                "unregistered TLA detector predicate {}",
                probe.predicate
            ))
        })?;
        let has_config = has_kind(check, &config_kind)?;
        let has_log = has_kind(check, &log_kind)?;
        if has_config != has_log {
            return Err(AggregateError::new(format!(
                "TLA detector artifacts for {identity} are incomplete"
            )));
        }
        if !has_config {
            all_passed = false;
            observations.insert(observation, 0);
            continue;
        }
        let realized_config = read_kind(check, &config_kind, root)?;
        let expected_config =
            render_detector_config(template, probe).map_err(AggregateError::new)?;
        if realized_config != expected_config {
            return Err(AggregateError::new(format!(
                "TLA detector config does not bind probe {identity} exactly"
            )));
        }
        let label = detector_label(probe).ok_or_else(|| {
            AggregateError::new(format!("unregistered TLA detector probe {identity}"))
        })?;
        let detector =
            read_process_log(bundle, check, &log_kind, &label, root, producer_repository)?;
        let summary = crate::producer::tla_output::parse(detector.stdout.as_bytes()).ok();
        let expected = detector_invariant(probe).ok_or_else(|| {
            AggregateError::new(format!("unregistered TLA detector probe {identity}"))
        })?;
        let qualified = summary
            .as_ref()
            .is_some_and(|summary| successful_detector(&detector, summary, &expected));
        all_passed &= qualified;
        let predicate_qualified = observations.entry(observation).or_insert(1);
        *predicate_qualified &= u64::from(qualified);
    }
    if has_kind(check, MUTATION_SUITE_ARTIFACT_KIND)? {
        let mutation = read_bound_process_log(
            check,
            MUTATION_SUITE_ARTIFACT_KIND,
            MUTATION_SUITE_LABEL,
            root,
        )?;
        verify_mutation_invocation(bundle, &mutation, producer_repository)?;
        all_passed &=
            mutation_suite_passed(mutation.exit_code, mutation.timed_out, &mutation.stdout);
    } else {
        all_passed = false;
    }
    Ok((observations, all_passed))
}

fn verify_mutation_invocation(
    bundle: &ResultBundle,
    log: &crate::evidence::format::process::ProcessLog,
    producer_repository: &Path,
) -> Result<(), AggregateError> {
    let expected_arguments = [
        "test",
        "--locked",
        "-p",
        "rafter-invariants",
        "producer::tla_exec::mutation_tests",
        "--",
        "--ignored",
        "--test-threads=1",
    ];
    let current_dir = producer_repository.to_string_lossy();
    if log.invocation.program != "cargo"
        || log.invocation.program_sha256 != bundle.execution.source.cargo_sha256
        || log.invocation.arguments != expected_arguments
        || log.invocation.current_dir != current_dir
        || log.invocation.environment_sha256 != bundle.execution.source.environment_sha256
    {
        return Err(AggregateError::new(
            "TLA mutation suite invocation is not source-bound to the exact Cargo test command"
                .to_owned(),
        ));
    }
    Ok(())
}

fn verify_source_binding(
    bundle: &ResultBundle,
    check: &crate::CheckReceipt,
    root: &Path,
) -> Result<(), AggregateError> {
    let config = configuration(bundle, "config")?;
    for (kind, source) in [
        ("tla-spec", "specs/tla/raft/Raft.tla".to_owned()),
        (
            "tla-trace-spec",
            "specs/tla/raft/RaftMembershipTraceSample.tla".to_owned(),
        ),
        (
            "tla-detector-spec",
            "specs/tla/raft/RafterInvariantDetectorNegative.tla".to_owned(),
        ),
        (
            "tla-detector-config",
            "specs/tla/raft/RafterInvariantDetectorNegative.cfg".to_owned(),
        ),
        ("tla-runner", "scripts/tla-model-check".to_owned()),
        ("tla-tool-asset-id", "tools/tla/ASSET_ID".to_owned()),
        ("tla-tool-checksums", "tools/tla/SHA256SUMS".to_owned()),
        ("tla-config", format!("specs/tla/raft/{config}")),
        (
            "tla-trace-config",
            "specs/tla/raft/RaftMembershipTraceSample.cfg".to_owned(),
        ),
    ] {
        let artifact = read_kind(check, kind, root)?;
        let source = fs::read_to_string(root.join(&source)).map_err(|error| {
            AggregateError::new(format!("read TLA source binding {source}: {error}"))
        })?;
        if artifact != source {
            return Err(AggregateError::new(format!(
                "TLA artifact {kind} does not match its bound source"
            )));
        }
    }
    Ok(())
}

fn verify_tool_pin(
    bundle: &ResultBundle,
    check: &crate::CheckReceipt,
    root: &Path,
) -> Result<(), AggregateError> {
    let expected_sha = configuration(bundle, "tool_sha256")?;
    let tool = unique_artifact(check, "tla-tool")?;
    if tool.sha256 != expected_sha {
        return Err(AggregateError::new(
            "TLA tool artifact does not match the profile digest".to_owned(),
        ));
    }
    let asset_id = read_kind(check, "tla-tool-asset-id", root)?;
    if asset_id.trim() != configuration(bundle, "tool_asset_id")? {
        return Err(AggregateError::new(
            "TLA tool asset ID does not match the profile contract".to_owned(),
        ));
    }
    let checksums = read_kind(check, "tla-tool-checksums", root)?;
    if !checksum_matches(&checksums, expected_sha) {
        return Err(AggregateError::new(
            "TLA checksum manifest does not contain the exact profile digest".to_owned(),
        ));
    }
    Ok(())
}

fn checksum_matches(checksums: &str, expected_sha: &str) -> bool {
    let declared = checksums
        .lines()
        .filter(|line| !line.trim_start().starts_with('#'))
        .filter_map(|line| {
            let mut fields = line.split_whitespace();
            let sha = fields.next()?;
            let file = fields.next()?;
            (file == "tla2tools.jar" && fields.next().is_none()).then_some(sha)
        })
        .collect::<Vec<_>>();
    declared.as_slice() == [expected_sha]
}

fn configuration<'a>(bundle: &'a ResultBundle, name: &str) -> Result<&'a str, AggregateError> {
    bundle
        .execution
        .plan
        .contract
        .runners
        .get(&bundle.runner)
        .ok_or_else(|| {
            AggregateError::new(format!("execution plan omitted runner {}", bundle.runner))
        })?
        .configuration
        .get(name)
        .map(String::as_str)
        .ok_or_else(|| AggregateError::new(format!("TLA configuration omitted {name}")))
}

fn verify_counterexample_binding(
    bundle: &ResultBundle,
    violated: Option<&str>,
) -> Result<(), AggregateError> {
    match violated {
        None if bundle
            .results
            .iter()
            .all(|result| result.status != EvidenceStatus::Fail) =>
        {
            Ok(())
        }
        Some("TypeOK")
            if bundle.results.iter().all(|result| {
                result.status == EvidenceStatus::Error
                    && result.classification == Some(crate::FailureClassification::HarnessError)
            }) =>
        {
            Ok(())
        }
        Some(symbol) => {
            let bound = bundle
                .results
                .iter()
                .filter(|result| evidence_symbol(&result.evidence_id) == Some(symbol))
                .collect::<Vec<_>>();
            if !bound.is_empty()
                && bound
                    .iter()
                    .all(|result| result.status == EvidenceStatus::Fail)
                && bundle.results.iter().all(|result| {
                    (evidence_symbol(&result.evidence_id) == Some(symbol))
                        == (result.status == EvidenceStatus::Fail)
                })
            {
                return Ok(());
            }
            Err(AggregateError::new(
                "TLA counterexample frame does not match the failed evidence result".to_owned(),
            ))
        }
        _ => Err(AggregateError::new(
            "TLA counterexample frame does not match the failed evidence result".to_owned(),
        )),
    }
}

fn evidence_symbol(evidence_id: &str) -> Option<&str> {
    evidence_id.rsplit_once('#').map(|(_, symbol)| symbol)
}

fn read_kind(
    check: &crate::CheckReceipt,
    kind: &str,
    root: &Path,
) -> Result<String, AggregateError> {
    let artifact = unique_artifact(check, kind)?;
    fs::read_to_string(root.join(&artifact.path)).map_err(|error| {
        AggregateError::new(format!("read TLA artifact {}: {error}", artifact.path))
    })
}

fn read_json_kind<T: for<'de> serde::Deserialize<'de>>(
    check: &crate::CheckReceipt,
    kind: &str,
    root: &Path,
) -> Result<T, AggregateError> {
    let source = read_kind(check, kind, root)?;
    serde_json::from_str(&source)
        .map_err(|error| AggregateError::new(format!("parse TLA artifact {kind}: {error}")))
}

fn has_kind(check: &crate::CheckReceipt, kind: &str) -> Result<bool, AggregateError> {
    let count = check
        .artifacts
        .iter()
        .filter(|artifact| artifact.kind == kind)
        .count();
    match count {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(AggregateError::new(format!(
            "TLA artifact {kind} is ambiguous"
        ))),
    }
}

fn unique_artifact<'a>(
    check: &'a crate::CheckReceipt,
    kind: &str,
) -> Result<&'a crate::ArtifactRef, AggregateError> {
    let matching = check
        .artifacts
        .iter()
        .filter(|artifact| artifact.kind == kind)
        .collect::<Vec<_>>();
    match matching.as_slice() {
        [artifact] => Ok(artifact),
        [] => Err(AggregateError::new(format!(
            "TLA artifact {kind} is missing"
        ))),
        _ => Err(AggregateError::new(format!(
            "TLA artifact {kind} is ambiguous"
        ))),
    }
}

fn verify_completion(
    bundle: &ResultBundle,
    trace_passed: bool,
    detectors_passed: bool,
    checkpoint: Option<&RecoveryReport>,
    main: Option<&crate::evidence::format::process::ProcessLog>,
    summary: Option<&crate::producer::tla_output::TlcSummary>,
) -> Result<(), AggregateError> {
    let expected = if !trace_passed
        || !detectors_passed
        || checkpoint.is_some_and(|report| report.status == RecoveryStatus::Incompatible)
    {
        CheckCompletion::HarnessError
    } else if let Some(violated) = summary.and_then(|summary| summary.violated_invariant.as_deref())
    {
        if violated == "TypeOK" {
            CheckCompletion::HarnessError
        } else {
            CheckCompletion::Counterexample
        }
    } else if main.is_some_and(|log| log.timed_out) {
        CheckCompletion::Timeout
    } else if let (Some(main), Some(summary)) = (main, summary) {
        if successful_log(main) && successful_summary(summary) {
            let minimum_generated = configuration(bundle, "minimum_generated_states")?
                .parse::<u64>()
                .map_err(|_| AggregateError::new("invalid TLA generated-state floor".to_owned()))?;
            let minimum_distinct = configuration(bundle, "minimum_distinct_states")?
                .parse::<u64>()
                .map_err(|_| AggregateError::new("invalid TLA distinct-state floor".to_owned()))?;
            if summary.generated_states >= minimum_generated
                && summary.distinct_states >= minimum_distinct
            {
                CheckCompletion::FrontierExhausted
            } else {
                CheckCompletion::CoverageNotReached
            }
        } else {
            CheckCompletion::HarnessError
        }
    } else {
        CheckCompletion::HarnessError
    };
    let observed = bundle
        .execution
        .checks
        .first()
        .map(|check| check.completion)
        .ok_or_else(|| AggregateError::new("TLA receipt has no check".to_owned()))?;
    if observed != expected {
        return Err(AggregateError::new(format!(
            "TLA receipt completion {observed:?} disagrees with proof artifacts ({expected:?})"
        )));
    }
    Ok(())
}

fn configured_invariants(source: &str) -> Vec<String> {
    let mut invariants = Vec::new();
    let mut collecting = false;
    for line in source.lines() {
        let line = line.trim();
        if line == "INVARIANT" || line == "INVARIANTS" {
            collecting = true;
        } else if let Some(symbol) = line.strip_prefix("INVARIANT ") {
            invariants.push(symbol.trim().to_owned());
            collecting = false;
        } else if collecting && line.is_empty() {
            collecting = false;
        } else if collecting {
            invariants.push(line.to_owned());
        }
    }
    invariants
}

fn successful_log(log: &crate::evidence::format::process::ProcessLog) -> bool {
    log.exit_code == Some(0) && !log.timed_out
}

fn successful_summary(summary: &crate::producer::tla_output::TlcSummary) -> bool {
    summary.completed_without_error
        && summary.process_finished
        && summary.states_left == 0
        && summary.search_depth > 0
}

fn successful_detector(
    log: &crate::evidence::format::process::ProcessLog,
    summary: &crate::producer::tla_output::TlcSummary,
    expected_invariant: &str,
) -> bool {
    log.exit_code == Some(12)
        && !log.timed_out
        && !summary.completed_without_error
        && summary.process_finished
        && summary.violated_invariant.as_deref() == Some(expected_invariant)
        && summary.distinct_states >= 2
        && summary.states_left == 0
        && summary.search_depth >= 2
}

#[cfg(test)]
#[path = "artifact_verify_tla/unit_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "artifact_verify_tla_tests.rs"]
mod full_bundle_tests;
