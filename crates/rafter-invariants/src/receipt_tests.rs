use std::collections::{BTreeMap, BTreeSet};

use crate::{CheckReceipt, EvidenceDescriptor, EvidenceStatus, ResultBundle};

pub(super) fn validate(
    bundle: &ResultBundle,
    expected: &BTreeMap<String, &EvidenceDescriptor>,
) -> Result<(), &'static str> {
    let mut required = BTreeMap::<String, BTreeSet<String>>::new();
    for (evidence_id, descriptor) in expected {
        if let Some(identity) = &descriptor.test {
            required
                .entry(identity.check_id())
                .or_default()
                .insert(evidence_id.clone());
        }
    }
    let observed = bundle
        .execution
        .checks
        .iter()
        .map(|check| {
            (
                check.check_id.clone(),
                check.evidence_ids.iter().cloned().collect::<BTreeSet<_>>(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    if observed.len() != bundle.execution.checks.len() || observed != required {
        return Err("tests check identities and evidence fanout must exactly match the registry");
    }
    for check in &bundle.execution.checks {
        validate_check(bundle, check)?;
    }
    Ok(())
}

fn validate_check(bundle: &ResultBundle, check: &CheckReceipt) -> Result<(), &'static str> {
    let statuses = bundle
        .results
        .iter()
        .filter(|result| result.execution_id == check.execution_id)
        .map(|result| result.status)
        .collect::<BTreeSet<_>>();
    if statuses.len() != 1 {
        return Err("one tests execution cannot report conflicting result statuses");
    }
    if statuses.contains(&EvidenceStatus::Pass)
        && (check.observations
            != BTreeMap::from([
                ("discovered".to_owned(), 1),
                ("executed".to_owned(), 1),
                ("passed".to_owned(), 1),
            ])
            || !check
                .artifacts
                .iter()
                .any(|artifact| artifact.kind == "test-log")
            || !check
                .artifacts
                .iter()
                .any(|artifact| artifact.kind == "test-binary"))
    {
        return Err("passing tests check lacks exact observations, log, or binary artifact");
    }
    Ok(())
}
