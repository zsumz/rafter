use std::{collections::BTreeMap, error::Error, path::Path, time::Instant};

use crate::types::RESULT_SCHEMA_VERSION;
use crate::{
    catalog::{Catalog, ProfileContract},
    CheckCompletion, CheckReceipt, EvidenceDescriptor, EvidenceResult, EvidenceStatus,
    ExecutionReceipt, FailureClassification, ResultBundle, SourceReceipt,
};

use super::{
    maelstrom_binding::bind_counterexamples,
    maelstrom_edn::Validity,
    maelstrom_exec::{run_trial, Scenario, ScenarioMarkers, TrialOutcome},
    maelstrom_scenario::{required_configuration, scenario_for},
    process, source, ProducerContext,
};

pub(super) fn run(
    catalog: &Catalog,
    contract: &ProfileContract,
    profile: &str,
    source: SourceReceipt,
    output_dir: &Path,
    context: &ProducerContext<'_>,
) -> Result<ResultBundle, Box<dyn Error>> {
    let started = Instant::now();
    let runner = contract
        .runners
        .get("maelstrom")
        .ok_or("Maelstrom runner missing")?;
    let trials = required_configuration(&runner.configuration, "trials")?.parse::<u64>()?;
    let descriptors = catalog
        .required_evidence(contract)
        .into_values()
        .flatten()
        .filter(|descriptor| descriptor.layer == "maelstrom")
        .collect::<Vec<_>>();
    let mut checks = Vec::new();
    let mut results = Vec::new();
    let mut execution_artifacts = Vec::new();
    let mut peak_rss_kib = 0;
    for scenario in Scenario::ALL {
        let evidence = descriptors
            .iter()
            .filter(|descriptor| scenario_for(descriptor) == Some(scenario))
            .collect::<Vec<_>>();
        if evidence.is_empty() {
            return Err(format!(
                "Maelstrom scenario {} has no registry evidence",
                scenario.name()
            )
            .into());
        }
        let mut outcomes = Vec::new();
        for trial in 0..trials {
            let outcome = run_trial(
                scenario,
                trial,
                profile,
                &source.commit,
                &runner.configuration,
                output_dir,
            )?;
            peak_rss_kib = peak_rss_kib.max(outcome.peak_rss_kib);
            execution_artifacts.extend(outcome.artifacts.iter().cloned());
            outcomes.push(outcome);
        }
        let verdict = evaluate(scenario, &outcomes);
        let execution_id =
            super::artifact::stable_id("maelstrom", &format!("{profile}/{}", scenario.name()));
        let evidence_ids = evidence
            .iter()
            .map(|descriptor| descriptor.evidence_id())
            .collect::<Vec<_>>();
        let artifacts = outcomes
            .iter()
            .flat_map(|outcome| outcome.artifacts.iter().cloned())
            .collect::<Vec<_>>();
        results.extend(
            evidence
                .iter()
                .map(|descriptor| evidence_result(descriptor, &execution_id, &verdict, &artifacts)),
        );
        checks.push(CheckReceipt {
            execution_id,
            check_id: format!("maelstrom/{}", scenario.name()),
            evidence_ids,
            completion: verdict.completion(),
            observations: observations(&outcomes),
            duration_ms: outcomes.iter().map(|outcome| outcome.duration_ms).sum(),
            peak_rss_kib: outcomes
                .iter()
                .map(|outcome| outcome.peak_rss_kib)
                .max()
                .unwrap_or_default(),
            artifacts,
        });
    }
    bind_counterexamples(&mut checks, &mut results)?;
    source::verify(&source)?;
    Ok(ResultBundle {
        schema_version: RESULT_SCHEMA_VERSION,
        runner: "maelstrom".to_owned(),
        profile: profile.to_owned(),
        source_ref: source.commit.clone(),
        execution: ExecutionReceipt {
            plan: context.plan.clone(),
            invocation: context.invocation.clone(),
            source,
            checks,
            duration_ms: process::duration_ms(started.elapsed()),
            peak_rss_kib,
            artifacts: execution_artifacts,
        },
        results,
    })
}

enum ScenarioVerdict {
    Pass,
    Counterexample,
    Incomplete(String),
    Error(String),
}

impl ScenarioVerdict {
    const fn completion(&self) -> CheckCompletion {
        match self {
            Self::Pass => CheckCompletion::Completed,
            Self::Counterexample => CheckCompletion::Counterexample,
            Self::Incomplete(_) => CheckCompletion::CoverageNotReached,
            Self::Error(_) => CheckCompletion::HarnessError,
        }
    }
}

fn evaluate(scenario: Scenario, outcomes: &[TrialOutcome]) -> ScenarioVerdict {
    if let Some(error) = outcomes.iter().find_map(|outcome| outcome.error.as_ref()) {
        return ScenarioVerdict::Error(error.clone());
    }
    if outcomes.iter().any(|outcome| {
        outcome
            .summary
            .as_ref()
            .is_some_and(|summary| summary.linearizability == Validity::Invalid)
    }) {
        return ScenarioVerdict::Counterexample;
    }
    if outcomes.iter().any(|outcome| !outcome.process_succeeded) {
        return ScenarioVerdict::Error("Maelstrom process did not exit successfully".to_owned());
    }
    if outcomes.iter().any(|outcome| {
        outcome
            .summary
            .as_ref()
            .is_none_or(|summary| summary.validity != Validity::Valid)
    }) {
        return ScenarioVerdict::Incomplete(
            "Maelstrom did not produce a completed valid checker result".to_owned(),
        );
    }
    if outcomes.iter().any(|outcome| {
        outcome.summary.as_ref().is_none_or(|summary| {
            summary.read_ok == 0 || summary.write_ok == 0 || summary.cas_ok == 0
        }) || !markers_cover(scenario, outcome.markers)
    }) {
        return ScenarioVerdict::Incomplete(format!(
            "Maelstrom scenario {} did not reach its operation or fault marker floor",
            scenario.name()
        ));
    }
    ScenarioVerdict::Pass
}

fn markers_cover(scenario: Scenario, markers: ScenarioMarkers) -> bool {
    match scenario {
        Scenario::Base => true,
        Scenario::Membership => {
            markers.membership_enter > 0
                && markers.membership_leave > 0
                && markers.membership_complete > 0
        }
        Scenario::Restart => markers.restarts >= 3 && markers.post_restart_progress > 0,
        Scenario::AppCrash => markers.crashpoints > 0 && markers.post_crash_progress > 0,
        Scenario::Snapshot => {
            markers.restarts > 0
                && markers.snapshots_compacted > 0
                && markers.snapshots_applied > 0
                && markers.post_restart_snapshots_applied > 0
        }
    }
}

fn observations(outcomes: &[TrialOutcome]) -> BTreeMap<String, u64> {
    let mut values = BTreeMap::from([
        ("trials".to_owned(), outcomes.len() as u64),
        ("valid_trials".to_owned(), 0),
        ("operation_count".to_owned(), 0),
        ("ok_count".to_owned(), 0),
        ("read_ok".to_owned(), 0),
        ("write_ok".to_owned(), 0),
        ("cas_ok".to_owned(), 0),
        ("membership_enter".to_owned(), 0),
        ("membership_leave".to_owned(), 0),
        ("membership_complete".to_owned(), 0),
        ("restarts".to_owned(), 0),
        ("post_restart_progress".to_owned(), 0),
        ("crashpoints".to_owned(), 0),
        ("post_crash_progress".to_owned(), 0),
        ("snapshots_compacted".to_owned(), 0),
        ("snapshots_applied".to_owned(), 0),
        ("post_restart_snapshots_applied".to_owned(), 0),
    ]);
    for outcome in outcomes {
        if let Some(summary) = &outcome.summary {
            add(
                &mut values,
                "valid_trials",
                u64::from(summary.validity == Validity::Valid),
            );
            add(&mut values, "operation_count", summary.operation_count);
            add(&mut values, "ok_count", summary.ok_count);
            add(&mut values, "read_ok", summary.read_ok);
            add(&mut values, "write_ok", summary.write_ok);
            add(&mut values, "cas_ok", summary.cas_ok);
        }
        for (name, value) in marker_values(outcome.markers) {
            add(&mut values, name, value);
        }
    }
    values
}

fn add(values: &mut BTreeMap<String, u64>, name: &str, value: u64) {
    *values.entry(name.to_owned()).or_default() += value;
}

fn marker_values(markers: ScenarioMarkers) -> [(&'static str, u64); 10] {
    [
        ("membership_enter", markers.membership_enter),
        ("membership_leave", markers.membership_leave),
        ("membership_complete", markers.membership_complete),
        ("restarts", markers.restarts),
        ("post_restart_progress", markers.post_restart_progress),
        ("crashpoints", markers.crashpoints),
        ("post_crash_progress", markers.post_crash_progress),
        ("snapshots_compacted", markers.snapshots_compacted),
        ("snapshots_applied", markers.snapshots_applied),
        (
            "post_restart_snapshots_applied",
            markers.post_restart_snapshots_applied,
        ),
    ]
}

fn evidence_result(
    descriptor: &EvidenceDescriptor,
    execution_id: &str,
    verdict: &ScenarioVerdict,
    artifacts: &[crate::ArtifactRef],
) -> EvidenceResult {
    let (status, classification, message) = match verdict {
        ScenarioVerdict::Pass => (EvidenceStatus::Pass, None, None),
        ScenarioVerdict::Counterexample if descriptor.invariant_id == "RD-06" => (
            EvidenceStatus::Fail,
            Some(FailureClassification::InvariantViolation),
            Some("Maelstrom reported a non-linearizable client history".to_owned()),
        ),
        ScenarioVerdict::Counterexample => (
            EvidenceStatus::Incomplete,
            Some(FailureClassification::CoverageNotReached),
            Some("Maelstrom found a client counterexample that cannot be attributed to this supporting invariant".to_owned()),
        ),
        ScenarioVerdict::Incomplete(message) => (
            EvidenceStatus::Incomplete,
            Some(FailureClassification::CoverageNotReached),
            Some(message.clone()),
        ),
        ScenarioVerdict::Error(message) => (
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
