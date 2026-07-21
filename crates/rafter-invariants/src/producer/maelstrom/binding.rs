//! Producer-owned attribution of Maelstrom counterexamples to reviewed evidence.

use crate::evidence::{
    CheckCompletion, CheckReceipt, EvidenceResult, EvidenceStatus, FailureClassification,
};

pub(in crate::producer) fn bind_counterexamples(
    checks: &mut [CheckReceipt],
    results: &mut [EvidenceResult],
) -> Result<(), &'static str> {
    let counterexamples = checks
        .iter()
        .enumerate()
        .filter(|(_, check)| {
            check.completion == CheckCompletion::Counterexample
                && check
                    .observations
                    .get("invalid_trials")
                    .copied()
                    .unwrap_or_default()
                    > 0
        })
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
        if index != owner
            && !results.iter().any(|result| {
                result.execution_id == checks[index].execution_id
                    && result.status == EvidenceStatus::Fail
            })
        {
            checks[index].completion = CheckCompletion::CoverageNotReached;
        }
    }
    Ok(())
}
