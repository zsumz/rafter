use std::{collections::BTreeMap, fs, path::Path};

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
    let main = read_process_log(check, "tla-log", "model-check", root)?;
    let trace = read_process_log(check, "tla-trace-log", "trace-sample", root)?;
    let detector = read_process_log(check, "tla-detector-log", "detector-negative", root)?;
    let config = read_kind(check, "tla-config", root)?;
    verify_tool_pin(bundle, check, root)?;
    let main_summary = crate::producer::tla_output::parse(main.stdout.as_bytes())
        .map_err(|error| AggregateError::new(format!("parse TLA proof log: {error}")))?;
    let trace_summary = crate::producer::tla_output::parse(trace.stdout.as_bytes())
        .map_err(|error| AggregateError::new(format!("parse TLA trace proof log: {error}")))?;
    let detector_summary = crate::producer::tla_output::parse(detector.stdout.as_bytes())
        .map_err(|error| AggregateError::new(format!("parse TLA detector proof log: {error}")))?;
    let symbols = configured_invariants(&config);
    let mut derived = BTreeMap::from([
        ("configured_invariants".to_owned(), symbols.len() as u64),
        ("tool_pin_verified".to_owned(), 1),
        (
            "trace_sample_passed".to_owned(),
            u64::from(successful_log(&trace) && successful_summary(&trace_summary)),
        ),
        (
            "detector_negative_passed".to_owned(),
            u64::from(successful_detector(&detector, &detector_summary)),
        ),
        ("generated_states".to_owned(), main_summary.generated_states),
        ("distinct_states".to_owned(), main_summary.distinct_states),
        ("states_left_on_queue".to_owned(), main_summary.states_left),
        ("search_depth".to_owned(), main_summary.search_depth),
    ]);
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
    check: &crate::CheckReceipt,
    kind: &str,
    label: &str,
    root: &Path,
) -> Result<crate::producer::ProcessLog, AggregateError> {
    let source = read_kind(check, kind, root)?;
    let log: crate::producer::ProcessLog = serde_json::from_str(&source)
        .map_err(|error| AggregateError::new(format!("parse TLA process log: {error}")))?;
    if log.schema_version != 1 || log.label != label {
        return Err(AggregateError::new(format!(
            "TLA process log {kind} has the wrong schema or label"
        )));
    }
    Ok(log)
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
) -> bool {
    log.exit_code == Some(12)
        && !log.timed_out
        && !summary.completed_without_error
        && summary.process_finished
        && summary.violated_invariant.as_deref() == Some("ExpectedViolation")
        && summary.distinct_states >= 2
        && summary.states_left == 0
        && summary.search_depth >= 2
}

#[cfg(test)]
mod tests {
    use super::checksum_matches;

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
}
