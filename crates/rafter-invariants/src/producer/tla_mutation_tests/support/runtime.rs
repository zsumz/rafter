//! TLC mutation probes and process execution.

use std::{fs, path::Path, process::Command, sync::OnceLock};

use crate::producer::{
    tla::contract,
    tla_output::{render_detector_config, DetectorProbe},
};

static TLA_TOOL_FETCH: OnceLock<Result<(), String>> = OnceLock::new();

pub(in crate::producer::tla_exec::mutation_tests) const ELECTION_PROBE: DetectorProbe =
    DetectorProbe {
        predicate: "ElectionSafety",
        mode: "ElectionRecorderOnly",
    };
pub(in crate::producer::tla_exec::mutation_tests) const LOG_MATCHING_PROBE: DetectorProbe =
    DetectorProbe {
        predicate: "LogMatching",
        mode: "LogMatchingRecorderOnly",
    };
pub(in crate::producer::tla_exec::mutation_tests) const SNAPSHOT_PREFIX_PROBE: DetectorProbe =
    DetectorProbe {
        predicate: "LogMatching",
        mode: "SnapshotPrefixRecorderOnly",
    };
pub(in crate::producer::tla_exec::mutation_tests) const LEADER_COMPLETENESS_PROBE: DetectorProbe =
    DetectorProbe {
        predicate: "LeaderCompleteness",
        mode: "LeaderCompletenessRecorderOnly",
    };
pub(in crate::producer::tla_exec::mutation_tests) const COMMITTED_PREFIX_PROBE: DetectorProbe =
    DetectorProbe {
        predicate: "CommittedPrefixStability",
        mode: "CommittedPrefixRecorderOnly",
    };
pub(in crate::producer::tla_exec::mutation_tests) const HIGHER_TERM_PROBE: DetectorProbe =
    DetectorProbe {
        predicate: "StaleLeaderFencing",
        mode: "HigherTermRecorderOnly",
    };
pub(in crate::producer::tla_exec::mutation_tests) const STALE_AUTHORITY_PROBE: DetectorProbe =
    DetectorProbe {
        predicate: "StaleLeaderFencing",
        mode: "StaleAuthorityRecorderOnly",
    };
pub(in crate::producer::tla_exec::mutation_tests) const APPLICATION_PROBE: DetectorProbe =
    DetectorProbe {
        predicate: "StateMachineSafety",
        mode: "ApplicationRecorderOnly",
    };
pub(in crate::producer::tla_exec::mutation_tests) const APPLICATION_EPOCH_PROBE: DetectorProbe =
    DetectorProbe {
        predicate: "StateMachineSafety",
        mode: "ApplicationEpochRecorderOnly",
    };
pub(in crate::producer::tla_exec::mutation_tests) const COMMIT_QUORUM_PROBE: DetectorProbe =
    DetectorProbe {
        predicate: "CommittedEntriesHaveQuorum",
        mode: "CommitQuorumRecorderOnly",
    };
pub(in crate::producer::tla_exec::mutation_tests) const READ_BARRIER_PROBE: DetectorProbe =
    DetectorProbe {
        predicate: "ReadBarrierLinearizability",
        mode: "ReadBarrierRecorderOnly",
    };

pub(in crate::producer::tla_exec::mutation_tests) fn workspace_root() -> std::path::PathBuf {
    fs::canonicalize(Path::new(env!("CARGO_MANIFEST_DIR")).join("../.."))
        .expect("canonicalize workspace root")
}

pub(in crate::producer::tla_exec::mutation_tests) fn replace_operator(
    source: &str,
    operator: &str,
    next: &str,
    body: &str,
) -> String {
    let start = format!("{operator} ==");
    let end = format!("{next} ==");
    let (prefix, rest) = source.split_once(&start).expect("operator exists");
    let (_, suffix) = rest.split_once(&end).expect("next operator exists");
    format!("{prefix}{operator} == {body}\n\n{end}{suffix}")
}

pub(in crate::producer::tla_exec::mutation_tests) fn replace_exactly_once(
    source: &str,
    from: &str,
    to: &str,
) -> String {
    assert_eq!(source.matches(from).count(), 1, "mutation target is exact");
    source.replacen(from, to, 1)
}

pub(in crate::producer::tla_exec::mutation_tests) fn replace_exactly_once_in_operator(
    source: &str,
    operator: &str,
    next: &str,
    from: &str,
    to: &str,
) -> String {
    let start = format!("{operator} ==");
    let end = format!("{next} ==");
    let (prefix, rest) = source.split_once(&start).expect("operator exists");
    let (body, suffix) = rest.split_once(&end).expect("next operator exists");
    assert_eq!(body.matches(from).count(), 1, "operator mutation is exact");
    let mutated = body.replacen(from, to, 1);
    format!("{prefix}{start}{mutated}{end}{suffix}")
}

pub(in crate::producer::tla_exec::mutation_tests) fn run_tlc_mutation(
    root: &Path,
    name: &str,
    raft: &str,
    detector: &str,
    probe: DetectorProbe,
) -> std::process::Output {
    let template =
        fs::read_to_string(root.join("specs/tla/raft/RafterInvariantDetectorNegative.cfg"))
            .expect("read detector config");
    let config = render_detector_config(&template, probe).expect("render detector config");
    run_tlc_with_config(root, name, raft, detector, &config)
}

pub(in crate::producer::tla_exec::mutation_tests) fn run_tlc_with_config(
    root: &Path,
    name: &str,
    raft: &str,
    detector: &str,
    config: &str,
) -> std::process::Output {
    ensure_tla_tool(root);
    let directory = root
        .join("target/rafter-invariants/tla-mutations")
        .join(format!("{}-{name}", std::process::id()));
    if directory.exists() {
        fs::remove_dir_all(&directory).expect("remove stale mutation directory");
    }
    fs::create_dir_all(&directory).expect("create mutation directory");
    fs::write(directory.join("Raft.tla"), raft).expect("write mutated Raft spec");
    fs::write(
        directory.join("RafterInvariantDetectorNegative.tla"),
        detector,
    )
    .expect("write mutated detector spec");
    fs::write(
        directory.join("RafterInvariantDetectorNegative.cfg"),
        config,
    )
    .expect("write detector config");
    Command::new("java")
        .args([
            "-XX:+UseParallelGC",
            "-cp",
            &root.join("tools/cache/tla2tools.jar").to_string_lossy(),
            "tlc2.TLC",
            "-tool",
            "-workers",
            "1",
            "-seed",
            "2026071101",
            "-fp",
            "0",
            "-metadir",
            &directory.join("states").to_string_lossy(),
            "-config",
            "RafterInvariantDetectorNegative.cfg",
            "RafterInvariantDetectorNegative.tla",
        ])
        .current_dir(&directory)
        .output()
        .expect("run TLC mutation")
}

fn ensure_tla_tool(root: &Path) {
    if let Err(error) = TLA_TOOL_FETCH.get_or_init(|| {
        contract::fetch_tool_at(root)
            .map_err(|error| format!("fetch and verify pinned TLC jar: {error}"))
    }) {
        panic!("{error}");
    }
}
