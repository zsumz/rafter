//! Structural validation of the profile-owned proof obligation list.

use std::collections::BTreeSet;

use super::super::{ObligationCompletion, ProofObligationContract};
use super::tla::PRIMARY_CONFIGS;

/// The primary configuration's budget and gating stance, which is all the
/// obligation list needs to know about the run it shares a window with.
#[derive(Clone, Copy)]
pub(super) struct PrimaryBudget<'a> {
    pub(super) profile: &'a str,
    pub(super) reporting: bool,
    pub(super) soft_timeout: &'a str,
    pub(super) total_timeout: Option<&'a str>,
    pub(super) finalization_reserve: Option<&'a str>,
}

/// Whole minutes the layer spends inside the execution window on work that is
/// neither an obligation nor the primary run: the trace-sample and negative
/// detector qualification probes plus the mutation suite, and the setup the
/// runner performs after the layer clock starts.
///
/// Re-derived here rather than imported, for the same reason the completion
/// vocabulary above is: `contract` sits upstream of `producer`, and this gate
/// exists to reject at review time a budget the runner would otherwise have to
/// truncate at runtime. Sharing constants would let one edit weaken both sides
/// at once. The producer owns the same inventory as `Duration`s -- twelve
/// qualification phases at a per-profile probe cap plus one mutation suite in
/// `producer/tla/execution/budget.rs`, and the setup allowance in the workflow
/// budget guard. All four numbers move together or none of them do.
const PR_QUALIFICATION_MINUTES: u64 = 7;
const PR_SETUP_MINUTES: u64 = 4;
const SCHEDULED_QUALIFICATION_MINUTES: u64 = 32;
const SCHEDULED_SETUP_MINUTES: u64 = 10;

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
    if std::path::Path::new(&obligation.config)
        .extension()
        .is_none_or(|extension| extension != "cfg")
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

/// Every phase of the layer is paid out of one execution window, and the
/// obligations run first. Requiring the *complete* inventory to fit keeps that
/// ordering honest: an obligation set that would starve the primary
/// continuation is rejected at contract time rather than silently truncating
/// the monolith at runtime.
///
/// Checking only the obligations against the window would be the same bug this
/// gate exists to prevent, one layer up. Qualification and setup draw on the
/// same `execution_deadline`, so the primary run never actually receives its
/// `soft_timeout` unless they are budgeted for too -- and under a gating policy
/// a truncated primary run reports as a timeout, which is a red merge gate for
/// a model that would have drained.
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
    let shared = shared_phase_minutes(budget.profile);
    let committed = shared
        .checked_add(obligated)
        .and_then(|sum| sum.checked_add(primary))
        .ok_or_else(|| "TLA+ phase inventory overflows".to_owned())?;
    if committed > window {
        return Err(format!(
            "TLA+ execution window of {window}m cannot fund {shared}m of qualification and \
             setup, {obligated}m of proof obligations, and a {primary}m primary run"
        ));
    }
    Ok(())
}

fn shared_phase_minutes(profile: &str) -> u64 {
    if profile == "pr" {
        PR_QUALIFICATION_MINUTES + PR_SETUP_MINUTES
    } else {
        SCHEDULED_QUALIFICATION_MINUTES + SCHEDULED_SETUP_MINUTES
    }
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
