//! Structural validation of the profile-owned proof obligation list.

use std::collections::BTreeSet;

use super::super::{ObligationCompletion, ProofObligationContract};
use super::tla::PRIMARY_CONFIGS;

/// The primary configuration's budget and gating stance, which is all the
/// obligation list needs to know about the run it shares a window with.
#[derive(Clone, Copy)]
pub(super) struct PrimaryBudget<'a> {
    pub(super) reporting: bool,
    pub(super) soft_timeout: &'a str,
    pub(super) total_timeout: Option<&'a str>,
    pub(super) finalization_reserve: Option<&'a str>,
}

/// Validates the obligation list structurally rather than by value.
///
/// The reviewed obligation set is profile data, not source: calibrated floors
/// arrive as a profiles-manifest edit. What source owns is the shape -- unique
/// sorted kebab-case identities, a resolvable non-primary configuration, a
/// positive whole-minute budget, positive ratchets, and a layer budget that
/// still leaves the primary continuation the time it was promised.
pub(super) fn validate(
    budget: PrimaryBudget<'_>,
    obligations: &[ProofObligationContract],
) -> Result<(), String> {
    // Demoting the primary continuation to reporting is only defensible when
    // something else still exhausts. A reporting profile with no obligations
    // would gate on nothing at all and call itself green.
    if budget.reporting && obligations.is_empty() {
        return Err(
            "a reporting-continuation TLA+ profile must declare at least one proof obligation"
                .to_owned(),
        );
    }
    let identities = obligations
        .iter()
        .map(|obligation| obligation.id.as_str())
        .collect::<Vec<_>>();
    if identities.iter().collect::<BTreeSet<_>>().len() != identities.len()
        || !identities.windows(2).all(|pair| pair[0] < pair[1])
    {
        return Err("TLA+ proof obligations must have unique, sorted identities".to_owned());
    }
    for obligation in obligations {
        validate_obligation(obligation)?;
    }
    validate_obligation_budget(budget, obligations)
}

fn validate_obligation(obligation: &ProofObligationContract) -> Result<(), String> {
    if !is_kebab_case(&obligation.id) {
        return Err(format!(
            "TLA+ proof obligation {} must use a kebab-case identity",
            obligation.id
        ));
    }
    if obligation.completion != ObligationCompletion::FrontierExhausted {
        return Err(format!(
            "TLA+ proof obligation {} must require frontier exhaustion",
            obligation.id
        ));
    }
    // Path::extension is deliberately case-sensitive here: the reviewed
    // configuration names are exact identities, and `Foo.CFG` is a different
    // (and unreviewed) name, not an acceptable spelling of `Foo.cfg`.
    if !std::path::Path::new(&obligation.config)
        .extension()
        .is_some_and(|extension| extension == "cfg")
        || obligation.config.contains('/')
        || obligation.config.contains('\\')
        || obligation.config.starts_with('.')
        || PRIMARY_CONFIGS.contains(&obligation.config.as_str())
    {
        return Err(format!(
            "TLA+ proof obligation {} must name a non-primary configuration under specs/tla/raft",
            obligation.id
        ));
    }
    if obligation.minimum_generated_states == 0
        || obligation.minimum_distinct_states == 0
        || obligation.minimum_generated_states < obligation.minimum_distinct_states
    {
        return Err(format!(
            "TLA+ proof obligation {} must ratchet positive, ordered state floors",
            obligation.id
        ));
    }
    if obligation.seed.is_empty() || !obligation.seed.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(format!(
            "TLA+ proof obligation {} must pin a numeric seed",
            obligation.id
        ));
    }
    match whole_minutes(&obligation.soft_timeout) {
        Some(minutes) if minutes > 0 => Ok(()),
        _ => Err(format!(
            "TLA+ proof obligation {} must budget positive whole minutes",
            obligation.id
        )),
    }
}

/// Obligations are paid out of the same layer budget as the primary run, and
/// they run first. Requiring the whole sequence to fit inside the execution
/// window keeps that ordering honest: an obligation set that would starve the
/// primary continuation is rejected at contract time rather than silently
/// truncating the monolith at runtime.
fn validate_obligation_budget(
    budget: PrimaryBudget<'_>,
    obligations: &[ProofObligationContract],
) -> Result<(), String> {
    let (Some(total), Some(reserve)) = (
        budget.total_timeout.and_then(whole_minutes),
        budget.finalization_reserve.and_then(whole_minutes),
    ) else {
        return Ok(());
    };
    let primary = whole_minutes(budget.soft_timeout)
        .ok_or_else(|| "TLA+ soft_timeout must use whole minutes".to_owned())?;
    let obligated = obligations
        .iter()
        .try_fold(0_u64, |sum, obligation| {
            sum.checked_add(whole_minutes(&obligation.soft_timeout)?)
        })
        .ok_or_else(|| "TLA+ proof obligation budget overflows".to_owned())?;
    let window = total
        .checked_sub(reserve)
        .ok_or_else(|| "TLA+ total_timeout must exceed finalization_reserve".to_owned())?;
    if obligated.saturating_add(primary) > window {
        return Err(
            "TLA+ proof obligations and the primary run must fit the execution window".to_owned(),
        );
    }
    Ok(())
}

fn whole_minutes(value: &str) -> Option<u64> {
    value.strip_suffix('m')?.parse::<u64>().ok()
}

fn is_kebab_case(value: &str) -> bool {
    !value.is_empty()
        && !value.starts_with('-')
        && !value.ends_with('-')
        && !value.contains("--")
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
}
