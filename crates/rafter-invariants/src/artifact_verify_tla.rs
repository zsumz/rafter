use std::{collections::BTreeMap, fs, path::Path};

use crate::producer::tla_checkpoint::{
    expected_contract, CheckpointContract, CheckpointInventory, RecoveryReport, RecoveryStatus,
    CONTRACT_KIND, INVENTORY_KIND, RECOVERED_CONTRACT_KIND, RECOVERED_INVENTORY_KIND,
    RECOVERY_REPORT_KIND,
};
use crate::producer::tla_output::{
    detector_config_kind, detector_invariant, detector_label, detector_log_kind,
    detector_observation, probe_slug, render_detector_config, DETECTOR_PROBES,
};
use crate::{aggregate::AggregateError, CheckCompletion, EvidenceStatus, ResultBundle};

pub(super) fn verify(bundle: &ResultBundle, root: &Path) -> Result<(), AggregateError> {
    let check = bundle
        .execution
        .checks
        .first()
        .ok_or_else(|| AggregateError::new("TLA receipt has no check".to_owned()))?;
    let trace = read_process_log(bundle, check, "tla-trace-log", "trace-sample", root)?;
    let config = read_kind(check, "tla-config", root)?;
    let detector_template = read_kind(check, "tla-detector-config", root)?;
    verify_source_binding(bundle, check, root)?;
    verify_tool_pin(bundle, check, root)?;
    let checkpoint = verify_checkpoint(bundle, check, root)?;
    let trace_summary = crate::producer::tla_output::parse(trace.stdout.as_bytes()).ok();
    let trace_passed = trace_summary
        .as_ref()
        .is_some_and(|summary| successful_log(&trace) && successful_summary(summary));
    let (detector_observations, detectors_passed) =
        verify_detectors(bundle, check, root, &detector_template)?;
    if !trace_passed && has_kind(check, "tla-log")? {
        return Err(AggregateError::new(
            "TLA main log exists after a failed trace probe".to_owned(),
        ));
    }
    let main = optional_process_log(bundle, check, "tla-log", "model-check", root)?;
    let main_summary = main
        .as_ref()
        .and_then(|log| crate::producer::tla_output::parse(log.stdout.as_bytes()).ok());
    let symbols = configured_invariants(&config);
    let mut derived = BTreeMap::from([
        ("configured_invariants".to_owned(), symbols.len() as u64),
        ("tool_pin_verified".to_owned(), 1),
        ("trace_sample_passed".to_owned(), u64::from(trace_passed)),
    ]);
    derived.extend(detector_observations);
    if let Some(checkpoint) = &checkpoint {
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
    if let Some(summary) = &main_summary {
        derived.extend([
            ("generated_states".to_owned(), summary.generated_states),
            ("distinct_states".to_owned(), summary.distinct_states),
            ("states_left_on_queue".to_owned(), summary.states_left),
            ("search_depth".to_owned(), summary.search_depth),
        ]);
        if main
            .as_ref()
            .is_some_and(|log| successful_log(log) && successful_summary(summary))
        {
            for symbol in symbols.iter().filter(|symbol| symbol.as_str() != "TypeOK") {
                derived.insert(format!("checked:{symbol}"), 1);
            }
        }
    }
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
    )
}

fn verify_detectors(
    bundle: &ResultBundle,
    check: &crate::CheckReceipt,
    root: &Path,
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
        let detector = read_process_log(bundle, check, &log_kind, &label, root)?;
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
    Ok((observations, all_passed))
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
            "specs/tla/raft/RaftTraceSample.tla".to_owned(),
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
            "specs/tla/raft/RaftTraceSample.cfg".to_owned(),
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

fn verify_checkpoint(
    bundle: &ResultBundle,
    check: &crate::CheckReceipt,
    root: &Path,
) -> Result<Option<RecoveryReport>, AggregateError> {
    let checkpoint_enabled = bundle.execution.plan.contract.runners["tla"]
        .configuration
        .contains_key("checkpoint_minutes");
    if !checkpoint_enabled {
        for kind in [
            CONTRACT_KIND,
            INVENTORY_KIND,
            RECOVERED_CONTRACT_KIND,
            RECOVERED_INVENTORY_KIND,
            RECOVERY_REPORT_KIND,
        ] {
            if has_kind(check, kind)? {
                return Err(AggregateError::new(format!(
                    "non-checkpointed TLA receipt contains {kind}"
                )));
            }
        }
        return Ok(None);
    }

    let report: RecoveryReport = read_json_kind(check, RECOVERY_REPORT_KIND, root)?;
    let contract = expected_contract(
        &bundle.profile,
        &bundle.execution.plan.contract.runners["tla"].configuration,
        &check.artifacts,
    )
    .map_err(|error| AggregateError::new(format!("derive TLA checkpoint contract: {error}")))?;
    let contract_sha256 = contract
        .sha256()
        .map_err(|error| AggregateError::new(format!("digest TLA checkpoint contract: {error}")))?;
    if report.schema_version != 1 || report.contract_sha256 != contract_sha256 {
        return Err(AggregateError::new(
            "TLA checkpoint recovery report does not match the exact execution contract".to_owned(),
        ));
    }
    let report_shape_valid = match report.status {
        RecoveryStatus::Fresh => {
            !report.recovery_attempted
                && report.recovered_checkpoint.is_none()
                && report.error.is_none()
        }
        RecoveryStatus::Compatible => {
            report.candidate_present
                && report.recovery_attempted
                && report.recovered_checkpoint.is_some()
                && report.error.is_none()
        }
        RecoveryStatus::Incompatible => {
            report.candidate_present
                && !report.recovery_attempted
                && report.recovered_checkpoint.is_none()
                && report.error.as_ref().is_some_and(|error| !error.is_empty())
        }
    };
    if !report_shape_valid {
        return Err(AggregateError::new(
            "TLA checkpoint recovery report has inconsistent status fields".to_owned(),
        ));
    }

    if report.status != RecoveryStatus::Incompatible {
        let final_contract: CheckpointContract = read_json_kind(check, CONTRACT_KIND, root)?;
        let final_inventory: CheckpointInventory = read_json_kind(check, INVENTORY_KIND, root)?;
        if final_contract != contract {
            return Err(AggregateError::new(
                "TLA final checkpoint metadata does not match the execution contract".to_owned(),
            ));
        }
        validate_inventory(&final_inventory, &contract_sha256)?;
    } else if has_kind(check, CONTRACT_KIND)? || has_kind(check, INVENTORY_KIND)? {
        return Err(AggregateError::new(
            "incompatible TLA recovery must not overwrite final checkpoint metadata".to_owned(),
        ));
    }

    if report.candidate_present && report.status != RecoveryStatus::Incompatible {
        let recovered_contract: CheckpointContract =
            read_json_kind(check, RECOVERED_CONTRACT_KIND, root)?;
        let recovered_inventory: CheckpointInventory =
            read_json_kind(check, RECOVERED_INVENTORY_KIND, root)?;
        if recovered_contract != contract {
            return Err(AggregateError::new(
                "restored TLA checkpoint metadata does not match the execution contract".to_owned(),
            ));
        }
        validate_inventory(&recovered_inventory, &contract_sha256)?;
        if report.status == RecoveryStatus::Compatible
            && recovered_inventory.latest_checkpoint != report.recovered_checkpoint
        {
            return Err(AggregateError::new(
                "TLA recovery report selected a checkpoint outside the restored inventory"
                    .to_owned(),
            ));
        }
    }
    Ok(Some(report))
}

fn validate_inventory(
    inventory: &CheckpointInventory,
    contract_sha256: &str,
) -> Result<(), AggregateError> {
    let paths = inventory
        .files
        .iter()
        .map(|file| file.path.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    let top_level = inventory
        .files
        .iter()
        .filter_map(|file| file.path.split_once('/').map(|(directory, _)| directory))
        .collect::<std::collections::BTreeSet<_>>();
    let expected_latest = top_level
        .iter()
        .next()
        .map(|directory| (*directory).to_owned());
    let has_committed_checkpoint = inventory.files.iter().any(|file| {
        file.path
            .rsplit('/')
            .next()
            .is_some_and(|name| has_tlc_extension(name, "chkpt"))
    });
    let has_temporary_checkpoint = inventory.files.iter().any(|file| {
        file.path
            .rsplit('/')
            .next()
            .is_some_and(|name| has_tlc_extension(name, "tmp"))
    });
    let valid_files = inventory.files.iter().all(|file| {
        file.sha256.len() == 64
            && file
                .sha256
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            && file.path.split_once('/').is_some()
            && !file
                .path
                .split('/')
                .any(|component| component.is_empty() || component == "." || component == "..")
    });
    if inventory.schema_version != 1
        || inventory.contract_sha256 != contract_sha256
        || paths.len() != inventory.files.len()
        || !valid_files
        || top_level.len() > 1
        || inventory.latest_checkpoint != expected_latest
        || (!inventory.files.is_empty() && !has_committed_checkpoint)
        || has_temporary_checkpoint
    {
        return Err(AggregateError::new(
            "TLA checkpoint inventory is malformed or not contract-bound".to_owned(),
        ));
    }
    Ok(())
}

fn has_tlc_extension(name: &str, expected: &str) -> bool {
    let path = Path::new(name);
    path.extension()
        .is_some_and(|extension| extension.eq_ignore_ascii_case(expected))
        || path
            .file_stem()
            .and_then(|stem| Path::new(stem).extension())
            .is_some_and(|extension| extension.eq_ignore_ascii_case(expected))
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
    verify_tla_invocation(bundle, check, label, &log.invocation, root)?;
    Ok(log)
}

fn optional_process_log(
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
            config: "RaftTraceSample.cfg".to_owned(),
            module: "RaftTraceSample.tla",
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

fn verify_completion(
    bundle: &ResultBundle,
    trace_passed: bool,
    detectors_passed: bool,
    checkpoint: Option<&RecoveryReport>,
    main: Option<&crate::producer::ProcessLog>,
    summary: Option<&crate::producer::tla_output::TlcSummary>,
) -> Result<(), AggregateError> {
    let expected = if !trace_passed
        || !detectors_passed
        || checkpoint.is_some_and(|report| report.status == RecoveryStatus::Incompatible)
    {
        CheckCompletion::HarnessError
    } else if summary
        .and_then(|summary| summary.violated_invariant.as_ref())
        .is_some()
    {
        CheckCompletion::Counterexample
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
    use super::{checksum_matches, successful_detector, validate_inventory};
    use crate::producer::{
        tla_checkpoint::{CheckpointFile, CheckpointInventory},
        tla_output::TlcSummary,
        ProcessLog,
    };
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
            termination: None,
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

    #[test]
    fn checkpoint_inventory_rejects_partial_or_multiple_run_directories() {
        let contract = "1".repeat(64);
        let complete = CheckpointInventory {
            schema_version: 1,
            contract_sha256: contract.clone(),
            latest_checkpoint: Some("run-a".to_owned()),
            files: vec![CheckpointFile {
                path: "run-a/queue.chkpt".to_owned(),
                sha256: "2".repeat(64),
                size_bytes: 0,
            }],
        };
        assert!(validate_inventory(&complete, &contract).is_ok());

        let mut partial = complete.clone();
        partial.files.push(CheckpointFile {
            path: "run-a/queue.tmp".to_owned(),
            sha256: "3".repeat(64),
            size_bytes: 1,
        });
        assert!(validate_inventory(&partial, &contract).is_err());

        let mut multiple = complete;
        multiple.files.push(CheckpointFile {
            path: "run-b/queue.chkpt".to_owned(),
            sha256: "4".repeat(64),
            size_bytes: 1,
        });
        assert!(validate_inventory(&multiple, &contract).is_err());
    }
}

#[cfg(test)]
#[path = "artifact_verify_tla_tests.rs"]
mod full_bundle_tests;
