//! Synthetic Maelstrom scenario observations and per-trial artifact sets.
//!
//! Each reviewed scenario publishes the counters its own contract names, and
//! its artifacts split into the tool inputs captured once per source tree and
//! the records one trial actually produced.

use super::receipts::artifact_kind;
use crate::*;

pub(super) fn maelstrom_scenario(path: &str) -> Option<&'static str> {
    match path {
        "scripts/maelstrom-lin-kv" => Some("base"),
        "scripts/maelstrom-lin-kv-membership-change" => Some("membership"),
        "scripts/maelstrom-lin-kv-repeated-restart" => Some("restart"),
        "scripts/maelstrom-lin-kv-app-persist-crash" => Some("app-crash"),
        "scripts/maelstrom-lin-kv-forced-snapshot" => Some("snapshot"),
        "scripts/maelstrom-lin-kv-lease-isolation" => Some("lease-isolation"),
        _ => None,
    }
}

pub(super) fn synthetic_maelstrom_observations(
    descriptor: &EvidenceDescriptor,
    manifest: &ProfileManifest,
    profile: &str,
) -> std::collections::BTreeMap<String, u64> {
    let trials = manifest.profiles[profile].runners["maelstrom"].configuration["trials"]
        .parse::<u64>()
        .expect("reviewed Maelstrom trial count");
    let mut observations = crate::artifact_verify_maelstrom_support::empty_observations(trials);
    observations.extend([
        ("valid_trials".to_owned(), trials),
        ("operation_count".to_owned(), 9 * trials),
        ("ok_count".to_owned(), 6 * trials),
        ("read_ok".to_owned(), 2 * trials),
        ("write_ok".to_owned(), 3 * trials),
        ("cas_ok".to_owned(), trials),
    ]);
    match maelstrom_scenario(&descriptor.path).expect("reviewed Maelstrom evidence path") {
        "base" => {}
        "membership" => {
            for name in [
                "membership_enter",
                "membership_leave",
                "membership_complete",
            ] {
                observations.insert(name.to_owned(), trials);
            }
        }
        "restart" => {
            observations.insert("restarts".to_owned(), 3 * trials);
            observations.insert("post_restart_progress".to_owned(), trials);
        }
        "app-crash" => {
            observations.insert("crashpoints".to_owned(), trials);
            observations.insert("post_crash_progress".to_owned(), trials);
        }
        "snapshot" => {
            for name in [
                "restarts",
                "snapshots_compacted",
                "snapshots_applied",
                "post_restart_snapshots_applied",
            ] {
                observations.insert(name.to_owned(), trials);
            }
        }
        "lease-isolation" => {
            for name in [
                "lease_fast_path_read_ok",
                "lease_read_buffered",
                "lease_expired_while_leader",
                "lease_post_expiry_released",
                "lease_post_expiry_handler",
                "lease_post_expiry_unavailable",
                "lease_history_probe_matches",
                "lease_sequence_complete",
            ] {
                observations.insert(name.to_owned(), trials);
            }
        }
        _ => unreachable!("reviewed Maelstrom scenario"),
    }
    observations
}

pub(super) fn synthetic_maelstrom_artifacts(
    descriptor: &EvidenceDescriptor,
    manifest: &ProfileManifest,
    profile: &str,
) -> Vec<ArtifactRef> {
    let scenario = maelstrom_scenario(&descriptor.path).expect("reviewed Maelstrom evidence path");
    let trials = manifest.profiles[profile].runners["maelstrom"].configuration["trials"]
        .parse::<u64>()
        .expect("reviewed Maelstrom trial count");
    let mut artifacts = Vec::new();
    // Tool inputs are captured once per source tree and shared across trials.
    let inputs = format!("artifacts/maelstrom/{scenario}/inputs");
    for kind in ["maelstrom-runner", "maelstrom-binary", "maelstrom-tool-jar"] {
        artifacts.push(artifact_kind(&format!("{inputs}/{kind}"), kind));
    }
    if matches!(
        scenario,
        "restart" | "app-crash" | "snapshot" | "lease-isolation"
    ) {
        artifacts.push(artifact_kind(
            &format!("{inputs}/maelstrom-proxy-binary"),
            "maelstrom-proxy-binary",
        ));
    }
    for trial in 0..trials {
        let root = format!("artifacts/maelstrom/{scenario}/trial-{trial}");
        for kind in [
            "maelstrom-results",
            "maelstrom-process-log",
            "maelstrom-node-log",
        ] {
            artifacts.push(artifact_kind(&format!("{root}/{kind}"), kind));
        }
        if matches!(scenario, "restart" | "app-crash" | "snapshot") {
            artifacts.push(artifact_kind(
                &format!("{root}/maelstrom-durable-file"),
                "maelstrom-durable-file",
            ));
        }
    }
    artifacts
}
