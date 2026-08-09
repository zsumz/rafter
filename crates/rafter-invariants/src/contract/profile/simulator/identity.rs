//! Stable identities shared by simulator producers and independent verifiers.

use sha2::{Digest, Sha256};

const SCHEDULED_SEED_NAMESPACE: &str = "scheduled-simulator-seed-v1";

pub(crate) fn canonical_simulator_check_id(profile: &str, check_id: &str) -> Option<String> {
    let suffix = match profile {
        "nightly" => "nightly",
        "weekly" => "weekly",
        _ => return None,
    };
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
