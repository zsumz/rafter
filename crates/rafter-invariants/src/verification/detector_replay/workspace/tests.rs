//! Adversarial private replay-workspace scenarios.

use std::{
    path::Path,
    sync::atomic::Ordering,
    time::{Duration, Instant},
};

use super::ReplayWorkspace;

#[test]
fn private_cargo_home_rejects_injected_configuration() {
    let source_ref = format!("{:016x}", std::process::id());
    let workspace =
        ReplayWorkspace::create("pr", &source_ref, Instant::now() + Duration::from_secs(30))
            .expect("create replay workspace");
    workspace
        .cargo_home
        .write_atomic(
            Path::new("config.toml"),
            b"[source.crates-io]\nreplace-with='ambient'\n",
        )
        .expect("inject Cargo configuration");

    let error = workspace
        .verify()
        .expect_err("injected Cargo configuration must fail closed");

    assert!(error.to_string().contains("forbidden configuration"));
}

#[test]
fn concurrent_replay_for_the_same_source_is_rejected_without_replacing_state() {
    let source_ref = format!("lock-{:x}", std::process::id());
    let deadline = Instant::now() + Duration::from_secs(30);
    let first = ReplayWorkspace::create("pr", &source_ref, deadline)
        .expect("create first replay workspace");
    first
        .temporary
        .write_atomic(Path::new("sentinel"), b"owned")
        .expect("write first-run sentinel");

    let Err(error) = ReplayWorkspace::create("pr", &source_ref, deadline) else {
        panic!("concurrent replay must not replace live state");
    };
    assert!(error.to_string().contains("already owns"), "{error}");
    assert_eq!(
        first
            .temporary
            .read_bounded(
                Path::new("sentinel"),
                crate::execution::filesystem::OperationDeadline::none("read sentinel"),
                16,
            )
            .expect("read first-run sentinel"),
        b"owned"
    );

    drop(first);
    ReplayWorkspace::create("pr", &source_ref, deadline)
        .expect("lock is released when the replay ends");
}

#[test]
fn consecutive_fixtures_cannot_observe_each_others_scratch_state() {
    let source_ref = format!("scratch-{:x}", std::process::id());
    let workspace =
        ReplayWorkspace::create("pr", &source_ref, Instant::now() + Duration::from_secs(30))
            .expect("create replay workspace");
    let first = workspace
        .create_fixture_temporary()
        .expect("create first fixture scratch");
    first
        .directory
        .write_atomic(Path::new("detector-state"), b"first fixture")
        .expect("write first fixture state");

    let second = workspace
        .create_fixture_temporary()
        .expect("create second fixture scratch");

    assert_ne!(
        first.directory.external_path(),
        second.directory.external_path()
    );
    assert!(!second
        .directory
        .path_exists(Path::new("detector-state"))
        .expect("inspect second fixture scratch"));
    assert!(first
        .directory
        .path_exists(Path::new("detector-state"))
        .expect("revalidate first fixture scratch"));
}

#[test]
fn fixture_temporary_sequence_exhaustion_fails_closed() {
    let source_ref = format!("scratch-limit-{:x}", std::process::id());
    let workspace =
        ReplayWorkspace::create("pr", &source_ref, Instant::now() + Duration::from_secs(30))
            .expect("create replay workspace");
    workspace.next_temporary.store(u64::MAX, Ordering::Relaxed);

    let Err(error) = workspace.create_fixture_temporary() else {
        panic!("exhausted fixture sequence must fail closed");
    };

    assert!(error.to_string().contains("sequence exhausted"), "{error}");
}
