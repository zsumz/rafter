use crate::{CheckCompletion, CheckReceipt, EvidenceResult, EvidenceStatus, FailureClassification};

pub(super) fn bind_counterexamples(
    checks: &mut [CheckReceipt],
    results: &mut [EvidenceResult],
) -> Result<(), &'static str> {
    let counterexamples = checks
        .iter()
        .enumerate()
        .filter(|(_, check)| check.completion == CheckCompletion::Counterexample)
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    if counterexamples.is_empty() {
        return Ok(());
    }
    let rd06 = results
        .iter()
        .position(|result| result.invariant_id == "RD-06")
        .ok_or("Maelstrom registry omitted RD-06 linearizability evidence")?;
    let evidence_id = results[rd06].evidence_id.clone();
    let owner = counterexamples
        .iter()
        .copied()
        .find(|index| checks[*index].evidence_ids.contains(&evidence_id))
        .unwrap_or(counterexamples[0]);

    for check in checks.iter_mut() {
        check
            .evidence_ids
            .retain(|candidate| candidate != &evidence_id);
    }
    checks[owner].evidence_ids.push(evidence_id);
    results[rd06]
        .execution_id
        .clone_from(&checks[owner].execution_id);
    results[rd06].status = EvidenceStatus::Fail;
    results[rd06].classification = Some(FailureClassification::InvariantViolation);
    results[rd06].message = Some("Maelstrom reported a non-linearizable client history".to_owned());
    results[rd06].artifacts.clone_from(&checks[owner].artifacts);

    for index in counterexamples {
        if index != owner {
            checks[index].completion = CheckCompletion::CoverageNotReached;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::{
        CheckCompletion, CheckReceipt, EvidenceResult, EvidenceStatus, FailureClassification,
    };

    use super::bind_counterexamples;

    #[test]
    fn non_base_counterexample_owns_the_single_rd06_failure() {
        let mut checks = vec![
            check("base", CheckCompletion::Completed, &["RD-06/evidence"]),
            check(
                "restart",
                CheckCompletion::Counterexample,
                &["PS-03/evidence"],
            ),
        ];
        let mut results = vec![result(
            "RD-06",
            "RD-06/evidence",
            "base",
            EvidenceStatus::Pass,
        )];

        bind_counterexamples(&mut checks, &mut results).expect("counterexample binds");

        assert!(!checks[0]
            .evidence_ids
            .contains(&"RD-06/evidence".to_owned()));
        assert!(checks[1]
            .evidence_ids
            .contains(&"RD-06/evidence".to_owned()));
        assert_eq!(results[0].execution_id, "restart");
        assert_eq!(results[0].status, EvidenceStatus::Fail);
        assert_eq!(
            results[0].classification,
            Some(FailureClassification::InvariantViolation)
        );
    }

    fn check(id: &str, completion: CheckCompletion, evidence: &[&str]) -> CheckReceipt {
        CheckReceipt {
            execution_id: id.to_owned(),
            check_id: format!("maelstrom/{id}"),
            evidence_ids: evidence.iter().map(ToString::to_string).collect(),
            completion,
            observations: BTreeMap::new(),
            duration_ms: 1,
            peak_rss_kib: 1,
            artifacts: Vec::new(),
        }
    }

    fn result(
        invariant: &str,
        evidence: &str,
        execution: &str,
        status: EvidenceStatus,
    ) -> EvidenceResult {
        EvidenceResult {
            invariant_id: invariant.to_owned(),
            evidence_id: evidence.to_owned(),
            execution_id: execution.to_owned(),
            status,
            classification: None,
            message: None,
            artifacts: Vec::new(),
        }
    }
}
