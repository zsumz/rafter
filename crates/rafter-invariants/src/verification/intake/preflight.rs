//! Whole-profile resource preflight before any artifact path is opened.

use std::path::PathBuf;

use super::VerificationRequest;
use crate::{
    evidence::ResultBundle,
    verification::{
        bundle::{declared_artifacts, BundleBudget, ProfileBudget},
        AggregateError,
    },
};

pub(super) fn profile_artifacts(
    request: &VerificationRequest<'_>,
    decoded: &[(String, PathBuf, ResultBundle)],
    profile_budget: ProfileBudget,
) -> Result<(), AggregateError> {
    let (references, bytes) = decoded.iter().try_fold(
        (0_usize, 0_u64),
        |(references, bytes), (runner, _, bundle)| {
            let budget = BundleBudget::for_trusted(&request.active_plan.profile, runner)?;
            let declared = declared_artifacts(bundle, budget, runner)?;
            Ok::<_, AggregateError>((
                references.checked_add(declared.references).ok_or_else(|| {
                    AggregateError::new(
                        "profile artifact reference count overflowed usize".to_owned(),
                    )
                })?,
                bytes.checked_add(declared.bytes).ok_or_else(|| {
                    AggregateError::new("profile artifact size overflowed u64".to_owned())
                })?,
            ))
        },
    )?;
    if references > profile_budget.artifact_refs() {
        return Err(AggregateError::new(format!(
            "{} profile declares {references} artifact references, exceeding the {}-reference report limit",
            request.active_plan.profile,
            profile_budget.artifact_refs()
        )));
    }
    if bytes > profile_budget.artifact_bytes() {
        return Err(AggregateError::new(format!(
            "{} profile declares {bytes} artifact bytes, exceeding the {}-byte limit",
            request.active_plan.profile,
            profile_budget.artifact_bytes()
        )));
    }
    Ok(())
}
