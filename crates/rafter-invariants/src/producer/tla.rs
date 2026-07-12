use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    path::Path,
    time::Instant,
};

use crate::types::RESULT_SCHEMA_VERSION;
use crate::{
    catalog::{Catalog, ProfileContract},
    CheckCompletion, CheckReceipt, EvidenceResult, EvidenceStatus, ExecutionReceipt,
    FailureClassification, ResultBundle, SourceReceipt,
};

use super::{
    artifact, process, source,
    tla_contract::{
        fetch_tool, parse_timeout, required_configuration, source_artifacts, validate_java,
        validate_runner_options, validate_spec_contract,
    },
    tla_exec::{execute, MainStatus, ProbeStatus, TlaExecution},
};

pub(super) fn run(
    catalog: &Catalog,
    contract: &ProfileContract,
    profile: &str,
    source: SourceReceipt,
    output_dir: &Path,
) -> Result<ResultBundle, Box<dyn Error>> {
    let started = Instant::now();
    let runner = contract.runners.get("tla").ok_or("TLA runner missing")?;
    validate_runner_options(&runner.configuration)?;
    validate_java(&source, &runner.configuration)?;
    fetch_tool()?;
    let artifacts = source_artifacts(&runner.configuration, output_dir, profile, &source.commit)?;
    let descriptors = catalog
        .required_evidence(contract)
        .into_values()
        .flatten()
        .filter(|descriptor| descriptor.layer == "tla")
        .collect::<Vec<_>>();
    let symbols = descriptors
        .iter()
        .map(|descriptor| descriptor.symbol.clone())
        .collect::<BTreeSet<_>>();
    let config_name = required_configuration(&runner.configuration, "config")?;
    let configured = validate_spec_contract(config_name, &symbols)?;
    let timeout = parse_timeout(required_configuration(
        &runner.configuration,
        "soft_timeout",
    )?)?;
    let execution = execute(
        profile,
        &source.commit,
        config_name,
        &runner.configuration,
        timeout,
        output_dir,
        artifacts,
    )?;
    let verdict = evaluate(&execution, &symbols, &runner.configuration);
    let execution_id = artifact::stable_id("tla", &format!("{profile}/{config_name}"));
    let evidence_ids = descriptors
        .iter()
        .map(crate::EvidenceDescriptor::evidence_id)
        .collect::<Vec<_>>();
    let results = descriptors
        .iter()
        .map(|descriptor| {
            evidence_result(descriptor, &execution_id, &verdict, &execution.artifacts)
        })
        .collect();
    let check = CheckReceipt {
        execution_id,
        check_id: format!("tla/{config_name}#Spec"),
        evidence_ids,
        completion: verdict.completion(),
        observations: observations(&execution, &symbols, configured.len()),
        duration_ms: execution.duration_ms,
        peak_rss_kib: execution.peak_rss_kib,
        artifacts: execution.artifacts.clone(),
    };
    source::verify(&source)?;
    Ok(ResultBundle {
        schema_version: RESULT_SCHEMA_VERSION,
        runner: "tla".to_owned(),
        profile: profile.to_owned(),
        source_ref: source.commit.clone(),
        execution: ExecutionReceipt {
            producer: runner.producer.clone(),
            command: runner.command.clone(),
            configuration: runner.configuration.clone(),
            source,
            checks: vec![check],
            duration_ms: process::duration_ms(started.elapsed()),
            peak_rss_kib: execution.peak_rss_kib,
            artifacts: execution.artifacts,
        },
        results,
    })
}

enum TlaVerdict {
    Pass,
    Violation(String),
    Incomplete(CheckCompletion, String),
    Error(String),
}

impl TlaVerdict {
    const fn completion(&self) -> CheckCompletion {
        match self {
            Self::Pass => CheckCompletion::FrontierExhausted,
            Self::Violation(_) => CheckCompletion::Counterexample,
            Self::Incomplete(completion, _) => *completion,
            Self::Error(_) => CheckCompletion::HarnessError,
        }
    }
}

fn evaluate(
    execution: &TlaExecution,
    symbols: &BTreeSet<String>,
    configuration: &BTreeMap<String, String>,
) -> TlaVerdict {
    if execution.trace_status != ProbeStatus::Passed {
        return error("TLC trace-sample harness did not complete successfully");
    }
    if execution.detector_status != ProbeStatus::Passed {
        return error("TLC negative detector did not report its named counterexample");
    }
    if execution.main_status == MainStatus::TimedOut {
        return incomplete(
            CheckCompletion::Timeout,
            "TLC exhausted its soft time budget",
        );
    }
    if let Some(parse_error) = &execution.main_parse_error {
        return error(&format!("malformed TLC tool output: {parse_error}"));
    }
    let Some(summary) = execution.main.as_ref() else {
        return error("TLC model check was not executed");
    };
    if let Some(invariant) = &summary.violated_invariant {
        if symbols.contains(invariant) {
            return TlaVerdict::Violation(invariant.clone());
        }
        return error(&format!(
            "TLC violated unregistered harness predicate {invariant}"
        ));
    }
    if execution.main_status != MainStatus::Succeeded
        || !summary.completed_without_error
        || !summary.process_finished
    {
        return error("TLC exited without a successful completion verdict");
    }
    let minimum_generated = required_configuration(configuration, "minimum_generated_states")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(u64::MAX);
    let minimum_distinct = required_configuration(configuration, "minimum_distinct_states")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(u64::MAX);
    if summary.states_left != 0
        || summary.generated_states < minimum_generated
        || summary.distinct_states < minimum_distinct
        || summary.search_depth == 0
        || symbols.is_empty()
    {
        return incomplete(
            CheckCompletion::CoverageNotReached,
            "TLC completion did not satisfy the configured state/depth floor",
        );
    }
    TlaVerdict::Pass
}

fn observations(
    execution: &TlaExecution,
    symbols: &BTreeSet<String>,
    configured_invariants: usize,
) -> BTreeMap<String, u64> {
    let mut observations = BTreeMap::from([
        (
            "configured_invariants".to_owned(),
            configured_invariants as u64,
        ),
        ("tool_pin_verified".to_owned(), 1),
        (
            "trace_sample_passed".to_owned(),
            u64::from(execution.trace_status == ProbeStatus::Passed),
        ),
        (
            "detector_negative_passed".to_owned(),
            u64::from(execution.detector_status == ProbeStatus::Passed),
        ),
    ]);
    if let Some(summary) = &execution.main {
        observations.extend([
            ("generated_states".to_owned(), summary.generated_states),
            ("distinct_states".to_owned(), summary.distinct_states),
            ("states_left_on_queue".to_owned(), summary.states_left),
            ("search_depth".to_owned(), summary.search_depth),
        ]);
        if execution.main_status == MainStatus::Succeeded && summary.completed_without_error {
            for symbol in symbols {
                observations.insert(format!("checked:{symbol}"), 1);
            }
        }
    }
    observations
}

fn evidence_result(
    descriptor: &crate::EvidenceDescriptor,
    execution_id: &str,
    verdict: &TlaVerdict,
    artifacts: &[crate::ArtifactRef],
) -> EvidenceResult {
    let (status, classification, message) = match verdict {
        TlaVerdict::Pass => (EvidenceStatus::Pass, None, None),
        TlaVerdict::Violation(symbol) if symbol == &descriptor.symbol => (
            EvidenceStatus::Fail,
            Some(FailureClassification::InvariantViolation),
            Some(format!("TLC reported a counterexample for {symbol}")),
        ),
        TlaVerdict::Violation(symbol) => (
            EvidenceStatus::Incomplete,
            Some(FailureClassification::CoverageNotReached),
            Some(format!(
                "TLC stopped at counterexample {symbol} before proving {}",
                descriptor.symbol
            )),
        ),
        TlaVerdict::Incomplete(_, message) => (
            EvidenceStatus::Incomplete,
            Some(FailureClassification::CoverageNotReached),
            Some(message.clone()),
        ),
        TlaVerdict::Error(message) => (
            EvidenceStatus::Error,
            Some(FailureClassification::HarnessError),
            Some(message.clone()),
        ),
    };
    EvidenceResult {
        invariant_id: descriptor.invariant_id.clone(),
        evidence_id: descriptor.evidence_id(),
        execution_id: execution_id.to_owned(),
        status,
        classification,
        message,
        artifacts: if status == EvidenceStatus::Pass {
            Vec::new()
        } else {
            artifacts.to_vec()
        },
    }
}

fn error(message: &str) -> TlaVerdict {
    TlaVerdict::Error(message.to_owned())
}

fn incomplete(completion: CheckCompletion, message: &str) -> TlaVerdict {
    TlaVerdict::Incomplete(completion, message.to_owned())
}

#[cfg(test)]
#[path = "tla_tests.rs"]
mod tests;
