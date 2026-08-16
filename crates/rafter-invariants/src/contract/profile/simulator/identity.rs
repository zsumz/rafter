//! Stable identities shared by simulator producers and independent verifiers.

use sha2::{Digest, Sha256};

const SCHEDULED_SEED_NAMESPACE: &str = "scheduled-simulator-seed-v1";

/// The `rafter-sim` model profile a scheduled invariants lane invokes.
///
/// Lane name and model profile were identical while every lane ran its
/// namesake profile, so producer, verifier, and liveness code each derived
/// `raft-{lane}` locally. Weekly's deep bounds have never completed on a
/// GitHub-hosted runner, so weekly now invokes nightly's proven profile, and
/// the two names have parted. Every check-id suffix, replay log line, and
/// execution contract for a lane follows the *model* profile it actually ran,
/// so they must all read this mapping rather than re-deriving from the lane.
/// Restoring weekly's deep profile is a one-line change here.
// The two scheduled arms are kept apart on purpose: they agree only because
// weekly is demoted, not because weekly is defined as nightly. Merging them
// would erase the seam that has to be reopened to promote weekly back.
#[allow(clippy::match_same_arms)]
pub(crate) fn scheduled_model_profile(profile: &str) -> Option<&'static str> {
    match profile {
        "nightly" => Some("raft-nightly"),
        // Interim: awaiting a >=32GB runner. See docs/model-checking.md.
        "weekly" => Some("raft-nightly"),
        _ => None,
    }
}

/// The check-id suffix a lane's emitted simulator events carry.
///
/// `rafter-sim` derives it from the model profile it was invoked with
/// (`raft-nightly` emits `-nightly`), never from the invariants lane.
pub(crate) fn scheduled_check_suffix(profile: &str) -> Option<&'static str> {
    scheduled_model_profile(profile).map(|model| model.trim_start_matches("raft-"))
}

pub(crate) fn canonical_simulator_check_id(profile: &str, check_id: &str) -> Option<String> {
    let suffix = scheduled_check_suffix(profile)?;
    if check_id == format!("raft-profile-total-{suffix}") {
        return None;
    }
    if check_id == format!("raft-commit-prevote-{suffix}") {
        return Some("raft-commit".to_owned());
    }
    let scheduled_soak = format!("raft-{suffix}-soak");
    if let Some(rest) = check_id.strip_prefix(&scheduled_soak) {
        return Some(format!("raft-soak{rest}"));
    }
    check_id
        .strip_suffix(&format!("-{suffix}"))
        .map(str::to_owned)
}

pub(crate) fn scheduled_simulator_seeds(
    profile: &str,
    source_ref: &str,
    count: usize,
) -> Option<String> {
    if !matches!(profile, "nightly" | "weekly") {
        return None;
    }
    Some(
        (0..count)
            .map(|index| {
                let input = format!("{SCHEDULED_SEED_NAMESPACE}\0{profile}\0{source_ref}\0{index}");
                let digest = Sha256::digest(input.as_bytes());
                let mut prefix = [0; 8];
                prefix.copy_from_slice(&digest[..8]);
                format!("0x{:x}", u64::from_be_bytes(prefix))
            })
            .collect::<Vec<_>>()
            .join(","),
    )
}

#[cfg(test)]
#[path = "identity_tests.rs"]
mod tests;
