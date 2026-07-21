//! Exact held-descriptor substitution scenarios for TLC arguments.

use super::matches;

fn arguments(state: &str) -> Vec<String> {
    [
        "-XX:+UseParallelGC",
        "-cp",
        "/workspace/tools/cache/tla2tools.jar",
        "tlc2.TLC",
        "-metadir",
        state,
        "-config",
        "RaftCi.cfg",
        "Raft.tla",
    ]
    .map(str::to_owned)
    .to_vec()
}

#[test]
fn held_state_descriptor_matches_path_based_plan() {
    assert!(matches(
        &arguments("/workspace/target/rafter-invariants/tla/states"),
        &arguments("/proc/self/fd/3")
    ));
}

#[test]
fn state_path_cannot_replace_held_descriptor() {
    let arguments = arguments("/workspace/target/rafter-invariants/tla/states");
    assert!(!matches(&arguments, &arguments));
}

#[test]
fn malformed_or_standard_descriptors_are_rejected() {
    let expected = arguments("/workspace/target/rafter-invariants/tla/states");
    for observed in [
        "/proc/self/fd/0",
        "/proc/self/fd/03",
        "/proc/self/fd/-3",
        "/proc/self/fd/3/child",
        "/dev/fd/3",
    ] {
        assert!(!matches(&expected, &arguments(observed)));
    }
}

#[test]
fn checkpoint_state_and_recovery_require_distinct_descriptors() {
    let mut expected = arguments("/workspace/target/rafter-invariants/tla/states");
    expected.splice(
        6..6,
        [
            "-recover".to_owned(),
            "/workspace/target/rafter-invariants/tla/checkpoint".to_owned(),
        ],
    );
    let mut observed = expected.clone();
    observed[5] = "/proc/self/fd/3".to_owned();
    observed[7] = "/proc/self/fd/4".to_owned();
    assert!(matches(&expected, &observed));

    observed[7] = "/proc/self/fd/3".to_owned();
    assert!(!matches(&expected, &observed));
}

#[test]
fn non_descriptor_arguments_remain_exact() {
    let expected = arguments("/workspace/target/rafter-invariants/tla/states");
    let mut observed = arguments("/proc/self/fd/3");
    observed[7] = "Other.cfg".to_owned();
    assert!(!matches(&expected, &observed));
}
