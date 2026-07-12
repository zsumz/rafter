use std::{collections::BTreeMap, fs, path::Path};

use crate::producer::tla_output::{
    detector_config_kind, detector_invariant, detector_label, detector_log_kind,
    detector_observation, render_detector_config, REGISTERED_PREDICATES,
};
use crate::{aggregate::AggregateError, EvidenceStatus, ResultBundle};

pub(super) fn verify(bundle: &ResultBundle, root: &Path) -> Result<(), AggregateError> {
    if bundle.results.iter().all(|result| {
        matches!(
            result.status,
            EvidenceStatus::Incomplete | EvidenceStatus::Error
        )
    }) {
        return Ok(());
    }
    let check = bundle
        .execution
        .checks
        .first()
        .ok_or_else(|| AggregateError::new("TLA receipt has no check".to_owned()))?;
    let main = read_process_log(bundle, check, "tla-log", "model-check", root)?;
    let trace = read_process_log(bundle, check, "tla-trace-log", "trace-sample", root)?;
    let config = read_kind(check, "tla-config", root)?;
    let detector_template = read_kind(check, "tla-detector-config", root)?;
    verify_source_binding(bundle, check, root)?;
    verify_tool_pin(bundle, check, root)?;
    let main_summary = crate::producer::tla_output::parse(main.stdout.as_bytes())
        .map_err(|error| AggregateError::new(format!("parse TLA proof log: {error}")))?;
    let trace_summary = crate::producer::tla_output::parse(trace.stdout.as_bytes())
        .map_err(|error| AggregateError::new(format!("parse TLA trace proof log: {error}")))?;
    let mut detector_observations = BTreeMap::new();
    for predicate in REGISTERED_PREDICATES {
        let config_kind = detector_config_kind(predicate).expect("registered detector predicate");
        let realized_config = read_kind(check, &config_kind, root)?;
        let expected_config =
            render_detector_config(&detector_template, predicate).map_err(AggregateError::new)?;
        if realized_config != expected_config {
            return Err(AggregateError::new(format!(
                "TLA detector config does not bind predicate {predicate} exactly"
            )));
        }
        let label = detector_label(predicate).expect("registered detector predicate");
        let log_kind = detector_log_kind(predicate).expect("registered detector predicate");
        let detector = read_process_log(bundle, check, &log_kind, &label, root)?;
        let summary =
            crate::producer::tla_output::parse(detector.stdout.as_bytes()).map_err(|error| {
                AggregateError::new(format!(
                    "parse TLA detector proof log for {predicate}: {error}"
                ))
            })?;
        let expected_invariant =
            detector_invariant(predicate).expect("registered detector predicate");
        detector_observations.insert(
            detector_observation(predicate).expect("registered detector predicate"),
            u64::from(successful_detector(
                &detector,
                &summary,
                &expected_invariant,
            )),
        );
    }
    let symbols = configured_invariants(&config);
    let mut derived = BTreeMap::from([
        ("configured_invariants".to_owned(), symbols.len() as u64),
        ("tool_pin_verified".to_owned(), 1),
        (
            "trace_sample_passed".to_owned(),
            u64::from(successful_log(&trace) && successful_summary(&trace_summary)),
        ),
        ("generated_states".to_owned(), main_summary.generated_states),
        ("distinct_states".to_owned(), main_summary.distinct_states),
        ("states_left_on_queue".to_owned(), main_summary.states_left),
        ("search_depth".to_owned(), main_summary.search_depth),
    ]);
    derived.extend(detector_observations);
    if successful_log(&main) && successful_summary(&main_summary) {
        for symbol in symbols.iter().filter(|symbol| symbol.as_str() != "TypeOK") {
            derived.insert(format!("checked:{symbol}"), 1);
        }
    }
    if check.observations != derived {
        return Err(AggregateError::new(
            "TLA receipt observations disagree with framed proof logs".to_owned(),
        ));
    }
    verify_counterexample_binding(bundle, main_summary.violated_invariant.as_deref())
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
            "tla-detector-spec",
            "specs/tla/raft/RafterInvariantDetectorNegative.tla".to_owned(),
        ),
        (
            "tla-detector-config",
            "specs/tla/raft/RafterInvariantDetectorNegative.cfg".to_owned(),
        ),
        ("tla-config", format!("specs/tla/raft/{config}")),
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
    let failed = bundle
        .results
        .iter()
        .filter(|result| result.status == EvidenceStatus::Fail)
        .collect::<Vec<_>>();
    match violated {
        None if failed.is_empty() => Ok(()),
        Some(symbol)
            if failed.len() == 1
                && failed[0]
                    .evidence_id
                    .strip_suffix(symbol)
                    .is_some_and(|prefix| prefix.ends_with('#')) =>
        {
            Ok(())
        }
        _ => Err(AggregateError::new(
            "TLA counterexample frame does not match the failed evidence result".to_owned(),
        )),
    }
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

fn read_process_log(
    bundle: &ResultBundle,
    check: &crate::CheckReceipt,
    kind: &str,
    label: &str,
    root: &Path,
) -> Result<crate::producer::ProcessLog, AggregateError> {
    let source = read_kind(check, kind, root)?;
    let log: crate::producer::ProcessLog = serde_json::from_str(&source)
        .map_err(|error| AggregateError::new(format!("parse TLA process log: {error}")))?;
    if log.schema_version != 2 || log.label != label || !log.has_complete_invocation() {
        return Err(AggregateError::new(format!(
            "TLA process log {kind} has the wrong schema, label, or exact invocation"
        )));
    }
    verify_tla_invocation(bundle, check, label, &log.invocation, root)?;
    Ok(log)
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
    let (config, module, workers) = match label {
        "model-check" => (
            configuration(bundle, "config")?.to_owned(),
            "Raft.tla",
            configuration(bundle, "workers")?,
        ),
        "trace-sample" => ("RaftTraceSample.cfg".to_owned(), "RaftTraceSample.tla", "1"),
        _ => {
            let predicate = REGISTERED_PREDICATES
                .iter()
                .find(|predicate| detector_label(predicate).as_deref() == Some(label))
                .ok_or_else(|| AggregateError::new(format!("unknown TLA log label {label}")))?;
            let config_kind =
                detector_config_kind(predicate).expect("registered detector predicate");
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
            (config, "RafterInvariantDetectorNegative.tla", "1")
        }
    };
    let current_dir = repository.join("specs/tla/raft");
    let source_prefix = bundle.source_ref.get(..12).unwrap_or(&bundle.source_ref);
    let state_dir = repository
        .join("target/rafter-invariants/tla")
        .join(source_prefix)
        .join(&bundle.profile)
        .join(label);
    let arguments = vec![
        "-XX:+UseParallelGC".to_owned(),
        "-cp".to_owned(),
        repository
            .join("tools/cache/tla2tools.jar")
            .to_string_lossy()
            .into_owned(),
        "tlc2.TLC".to_owned(),
        "-tool".to_owned(),
        "-workers".to_owned(),
        workers.to_owned(),
        "-seed".to_owned(),
        configuration(bundle, "seed")?.to_owned(),
        "-fp".to_owned(),
        "0".to_owned(),
        "-metadir".to_owned(),
        state_dir.to_string_lossy().into_owned(),
        "-config".to_owned(),
        config,
        module.to_owned(),
    ];
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

fn successful_log(log: &crate::producer::ProcessLog) -> bool {
    log.exit_code == Some(0) && !log.timed_out
}

fn successful_summary(summary: &crate::producer::tla_output::TlcSummary) -> bool {
    summary.completed_without_error
        && summary.process_finished
        && summary.states_left == 0
        && summary.search_depth > 0
}

fn successful_detector(
    log: &crate::producer::ProcessLog,
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
mod tests {
    use super::{checksum_matches, successful_detector};
    use crate::producer::{tla_output::TlcSummary, ProcessLog};
    use crate::InvocationReceipt;
    use std::collections::BTreeMap;

    const SHA: &str = "33de7da9ce1b7fffb9d1c184021178dbb051747be48504e65c584c423721a32e";

    #[test]
    fn tool_checksum_binding_is_exact_and_unique() {
        assert!(checksum_matches(
            &format!("# pinned\n{SHA}  tla2tools.jar\n"),
            SHA
        ));
        assert!(!checksum_matches(
            &format!("{SHA}  tla2tools.jar\n{SHA}  tla2tools.jar\n"),
            SHA
        ));
        assert!(!checksum_matches(
            &format!("{}  tla2tools.jar\n", "0".repeat(64)),
            SHA
        ));
    }

    #[test]
    fn detector_counterexample_identity_must_match_its_predicate() {
        let log = ProcessLog {
            schema_version: 2,
            label: "detector-negative-ElectionSafety".to_owned(),
            invocation: InvocationReceipt {
                program: "java".to_owned(),
                program_sha256: "0".repeat(64),
                arguments: Vec::new(),
                current_dir: ".".to_owned(),
                environment: BTreeMap::new(),
                environment_sha256: "0".repeat(64),
            },
            exit_code: Some(12),
            timed_out: false,
            duration_ms: 1,
            peak_rss_kib: 1,
            stdout: String::new(),
            stderr: String::new(),
        };
        let mut summary = TlcSummary {
            distinct_states: 2,
            states_left: 0,
            search_depth: 2,
            process_finished: true,
            violated_invariant: Some("ElectionSafety".to_owned()),
            ..TlcSummary::default()
        };
        assert!(successful_detector(&log, &summary, "ElectionSafety"));
        assert!(!successful_detector(&log, &summary, "LogMatching"));
        summary.violated_invariant = Some("ExpectedViolation".to_owned());
        assert!(!successful_detector(&log, &summary, "ElectionSafety"));
    }
}

#[cfg(test)]
#[path = "artifact_verify_tla_tests.rs"]
mod full_bundle_tests;
