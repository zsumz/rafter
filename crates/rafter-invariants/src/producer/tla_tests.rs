use std::{collections::BTreeMap, path::PathBuf};

use super::{evaluate, evidence_result, MainStatus, ProbeStatus, TlaExecution, TlaVerdict};
use crate::{producer::tla_output::TlcSummary, Catalog, EvidenceStatus, FailureClassification};

fn complete_execution(exit_succeeded: bool) -> TlaExecution {
    TlaExecution {
        main: Some(TlcSummary {
            generated_states: 130_000_001,
            distinct_states: 120_000_001,
            states_left: 0,
            search_depth: 19,
            completed_without_error: true,
            process_finished: true,
            violated_invariant: None,
        }),
        main_parse_error: None,
        main_status: if exit_succeeded {
            MainStatus::Succeeded
        } else {
            MainStatus::Failed
        },
        trace_status: ProbeStatus::Passed,
        detector_status: ProbeStatus::Passed,
        peak_rss_kib: 1,
        duration_ms: 1,
        artifacts: Vec::new(),
    }
}

#[test]
fn successful_frames_with_nonzero_exit_are_a_harness_error() {
    let execution = complete_execution(false);
    let symbols = ["ElectionSafety".to_owned()].into_iter().collect();
    let configuration =
        BTreeMap::from([("minimum_distinct_states".to_owned(), "120000000".to_owned())]);

    assert!(matches!(
        evaluate(&execution, &symbols, &configuration),
        TlaVerdict::Error(_)
    ));
}

#[test]
fn named_counterexample_fails_only_its_predicate() -> Result<(), Box<dyn std::error::Error>> {
    let registry = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("verification/raft-invariants.yaml");
    let catalog = Catalog::load(&registry)?;
    let results = catalog
        .evidence
        .iter()
        .filter(|descriptor| descriptor.layer == "tla")
        .map(|descriptor| {
            evidence_result(
                descriptor,
                "tla-test",
                &TlaVerdict::Violation("ElectionSafety".to_owned()),
                &[],
            )
        })
        .collect::<Vec<_>>();

    assert_eq!(results.len(), 8);
    assert_eq!(
        results
            .iter()
            .filter(|result| result.status == EvidenceStatus::Fail)
            .count(),
        1
    );
    let failed = results
        .iter()
        .filter(|result| result.status == EvidenceStatus::Fail)
        .collect::<Vec<_>>();
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0].invariant_id, "EL-05");
    assert_eq!(
        failed[0].classification,
        Some(FailureClassification::InvariantViolation)
    );
    assert!(results
        .iter()
        .filter(|result| result.status != EvidenceStatus::Fail)
        .all(|result| {
            result.status == EvidenceStatus::Incomplete
                && result.classification == Some(FailureClassification::CoverageNotReached)
        }));
    Ok(())
}
