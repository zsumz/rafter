//! Tests for verifier artifact publication and sealing.

use std::time::{Duration, Instant};

use super::{validate_archive_budget, ReplayArtifactPublisher};
use crate::evidence::limits::{MAX_VERIFIER_ARCHIVE_BYTES, MAX_VERIFIER_ARCHIVE_FILES};

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(30)
}

#[test]
fn sealed_artifacts_reject_post_capture_mutation() {
    let publisher = ReplayArtifactPublisher::create("pr", "publisher-mutation-test", deadline())
        .expect("create publisher");
    let artifact = publisher
        .capture("verifier-replay-report", b"original")
        .expect("capture artifact");
    publisher.publish_manifest().expect("publish manifest");
    std::fs::write(&artifact.path, b"mutated").expect("mutate artifact");

    let error = publisher.seal().expect_err("mutation must fail closed");
    assert!(
        error.to_string().contains("changed after publication"),
        "unexpected error: {error}"
    );
}

#[test]
fn sealed_artifacts_are_read_only_and_concurrent_publishers_fail_closed() {
    let source_ref = format!("publisher-lock-{}", std::process::id());
    let publisher = ReplayArtifactPublisher::create("pr", &source_ref, deadline())
        .expect("create first publisher");
    let artifact = publisher
        .capture("verifier-replay-report", b"sealed")
        .expect("capture artifact");
    let concurrent = ReplayArtifactPublisher::create("pr", &source_ref, deadline());
    assert!(
        concurrent
            .err()
            .is_some_and(|error| error.to_string().contains("already owns")),
        "concurrent publisher must fail closed"
    );

    publisher.publish_manifest().expect("publish manifest");
    let guard = publisher.seal().expect("seal publisher");
    assert!(
        std::fs::write(&artifact.path, b"mutated").is_err(),
        "sealed artifact must not remain writable"
    );
    guard.revalidate().expect("sealed artifacts remain valid");

    #[cfg(unix)]
    let inherited_lock = guard
        .lock
        .file
        .try_clone()
        .expect("duplicate the lock descriptor as a fork-inheritance fixture");
    drop(guard);
    ReplayArtifactPublisher::create("pr", &source_ref, deadline())
        .expect("publisher lock is released with the guard");
    #[cfg(unix)]
    drop(inherited_lock);
}

#[test]
fn sealing_rejects_files_outside_the_published_inventory() {
    let publisher = ReplayArtifactPublisher::create("pr", "publisher-inventory-test", deadline())
        .expect("create publisher");
    let artifact = publisher
        .capture("verifier-replay-report", b"sealed")
        .expect("capture artifact");
    publisher.publish_manifest().expect("publish manifest");
    let root = std::path::Path::new(&artifact.path)
        .parent()
        .expect("artifact has a parent");
    std::fs::write(root.join("unpublished"), b"unpublished").expect("write unpublished artifact");

    let error = publisher
        .seal()
        .expect_err("unpublished tree entry must fail closed");
    assert!(
        error.to_string().contains("tree inventory changed"),
        "unexpected error: {error}"
    );
}

#[test]
fn publication_rejects_an_expired_absolute_deadline() {
    let result =
        ReplayArtifactPublisher::create("pr", "publisher-expired-deadline-test", Instant::now());
    let error = result
        .err()
        .expect("expired publication deadline must fail closed");

    assert!(error.to_string().contains("deadline expired"), "{error}");
}

#[test]
fn publication_budget_includes_the_next_file_and_its_bytes() {
    validate_archive_budget(
        MAX_VERIFIER_ARCHIVE_FILES - 1,
        MAX_VERIFIER_ARCHIVE_BYTES - 1,
        1,
    )
    .expect("exact archive budget remains admissible");

    assert!(validate_archive_budget(MAX_VERIFIER_ARCHIVE_FILES, 1, 1)
        .expect_err("an additional file must exceed the inventory limit")
        .contains("file-count"));
    assert!(validate_archive_budget(1, MAX_VERIFIER_ARCHIVE_BYTES, 1)
        .expect_err("an additional byte must exceed the archive limit")
        .contains("total-byte"));
}
