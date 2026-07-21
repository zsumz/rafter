//! Tests for replay build-plan derivation.

use std::collections::BTreeMap;

use super::ReplayBuildPlan;
use crate::verification::detector_replay::{DetectorReplayPlan, ReplayFixture, ReplayTarget};

#[test]
fn compiler_selection_is_derived_in_deterministic_package_order() {
    let replay = plan([
        target("rafter-sim", "lib", "rafter_sim"),
        target("rafter", "lib", "rafter"),
    ]);

    let arguments = ReplayBuildPlan::derive(&replay)
        .expect("derive build plan")
        .cargo_arguments();

    assert_eq!(
        arguments,
        ["-p", "rafter", "-p", "rafter-sim", "--lib"]
            .into_iter()
            .map(std::ffi::OsString::from)
            .collect::<Vec<_>>()
    );
}

#[test]
fn changed_inventory_changes_compiler_selection_without_command_edits() {
    let replay = plan([target("replacement", "lib", "replacement")]);

    let arguments = ReplayBuildPlan::derive(&replay)
        .expect("derive build plan")
        .cargo_arguments();

    assert_eq!(
        arguments,
        ["-p", "replacement", "--lib"]
            .into_iter()
            .map(std::ffi::OsString::from)
            .collect::<Vec<_>>()
    );
}

#[test]
fn unsupported_target_kind_fails_closed() {
    let replay = plan([target("rafter", "bin", "tool")]);

    let error =
        ReplayBuildPlan::derive(&replay).expect_err("unsupported target kind must fail closed");

    assert!(
        error.contains("does not support Cargo target kind bin"),
        "{error}"
    );
}

fn plan<const N: usize>(targets: [ReplayTarget; N]) -> DetectorReplayPlan {
    DetectorReplayPlan::new(
        targets
            .into_iter()
            .map(|target| (target, Vec::<ReplayFixture>::new()))
            .collect::<BTreeMap<_, _>>(),
    )
}

fn target(package: &str, kind: &str, name: &str) -> ReplayTarget {
    ReplayTarget {
        package: package.to_owned(),
        kind: kind.to_owned(),
        name: name.to_owned(),
    }
}
