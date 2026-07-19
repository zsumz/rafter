//! Registry-to-libtest identity binding.

use std::collections::{BTreeMap, BTreeSet};

use crate::{contract::catalog::Catalog, evidence::CheckReceipt, verification::AggregateError};

pub(in crate::artifact_verify) fn registered_test_name(
    catalog: &Catalog,
    check: &CheckReceipt,
) -> Result<String, AggregateError> {
    let descriptors = catalog
        .evidence
        .iter()
        .map(|descriptor| (descriptor.evidence_id(), descriptor))
        .collect::<BTreeMap<_, _>>();
    let mut identities = BTreeSet::new();
    for evidence_id in &check.evidence_ids {
        let descriptor = descriptors.get(evidence_id).ok_or_else(|| {
            AggregateError::new(format!(
                "tests check {} references unknown registry evidence {evidence_id}",
                check.check_id
            ))
        })?;
        let identity = descriptor.test.as_ref().ok_or_else(|| {
            AggregateError::new(format!(
                "tests check {} references non-tests evidence {evidence_id}",
                check.check_id
            ))
        })?;
        if identity.check_id() != check.check_id {
            return Err(AggregateError::new(format!(
                "tests check {} does not match registered identity {}",
                check.check_id,
                identity.check_id()
            )));
        }
        identities.insert(identity.test_name.clone());
    }
    let identities = identities.into_iter().collect::<Vec<_>>();
    let [test_name] = identities.as_slice() else {
        return Err(AggregateError::new(format!(
            "tests check {} does not bind exactly one registered test identity",
            check.check_id
        )));
    };
    Ok(test_name.clone())
}
