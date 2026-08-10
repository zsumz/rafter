//! Artifact confinement and immutable-snapshot scenarios.

#[cfg(unix)]
use std::sync::atomic::{AtomicU64, Ordering};
use std::{collections::BTreeMap, path::Path};

use sha2::{Digest, Sha256};

use super::{
    authenticate_artifact, authenticate_artifact_at, preflight_artifacts, retain_semantic_bytes,
    ArtifactRef, AuthenticatedArtifacts, BundleBudget, VerificationRoot,
};
use crate::evidence::{limits::MAX_ARTIFACT_BYTES, ResultBundle};

fn tla_bundle_with_observed_shared_references() -> ResultBundle {
    let (catalog, manifest) = crate::tests::loaded();
    let mut bundle = crate::tests::passing_bundles(&catalog, &manifest)
        .into_iter()
        .find(|bundle| bundle.runner == "tla")
        .expect("TLA bundle");
    bundle.profile = "nightly".to_owned();
    bundle.execution.checks.truncate(1);

    let shared = (0..40)
        .map(|index| ArtifactRef {
            kind: "summary".to_owned(),
            path: format!("artifacts/tla-reference-{index}"),
            sha256: format!("{index:064x}"),
            size_bytes: 1,
        })
        .collect::<Vec<_>>();
    bundle.execution.artifacts.clone_from(&shared);
    bundle
        .execution
        .artifacts
        .push(bundle.execution.producer.executable.clone());
    bundle.execution.checks[0].artifacts.clone_from(&shared);

    let mut result = bundle.results[0].clone();
    result.artifacts = shared;
    bundle.results = vec![result; 13];
    bundle
}

#[cfg(unix)]
#[test]
fn artifact_integrity_rejects_modified_missing_and_symlinked_files() {
    use std::os::unix::fs::symlink;

    static NEXT: AtomicU64 = AtomicU64::new(0);
    let root = std::env::temp_dir().join(format!(
        "rafter-artifact-integrity-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&root).expect("create artifact scratch root");
    let repository = std::fs::canonicalize(&root).expect("canonical artifact scratch root");
    let path = repository.join("artifact");
    let bytes = b"preserved artifact";
    std::fs::write(&path, bytes).expect("write preserved artifact");
    let artifact = ArtifactRef {
        kind: "producer-binary".to_owned(),
        path: "artifact".to_owned(),
        sha256: format!("{:x}", Sha256::digest(bytes)),
        size_bytes: bytes.len() as u64,
    };

    authenticate_artifact(&artifact, &repository).expect("regular artifact verifies");
    let mut substituted = artifact.clone();
    substituted.path = path.to_string_lossy().into_owned();
    assert!(authenticate_artifact(&substituted, &repository).is_err());
    substituted.path = "../artifact".to_owned();
    assert!(authenticate_artifact(&substituted, &repository).is_err());
    std::fs::write(&path, b"modified artifact").expect("modify artifact");
    assert!(authenticate_artifact(&artifact, &repository).is_err());
    std::fs::remove_file(&path).expect("remove modified artifact");
    assert!(authenticate_artifact(&artifact, &repository).is_err());

    let target = repository.join("target");
    std::fs::write(&target, bytes).expect("write symlink target");
    symlink(&target, &path).expect("create artifact symlink");
    assert!(authenticate_artifact(&artifact, &repository).is_err());
    std::fs::remove_dir_all(root).expect("remove artifact scratch root");
}

#[test]
fn authenticated_snapshot_is_immutable_after_path_replacement() {
    let root = std::env::temp_dir().join(format!(
        "rafter-authenticated-artifact-snapshot-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("create artifact snapshot root");
    let repository = std::fs::canonicalize(&root).expect("canonical artifact snapshot root");
    let path = repository.join("artifact");
    let original = b"authenticated bytes\n";
    std::fs::write(&path, original).expect("write original artifact");
    let artifact = ArtifactRef {
        kind: "test-log".to_owned(),
        path: "artifact".to_owned(),
        sha256: format!("{:x}", Sha256::digest(original)),
        size_bytes: original.len() as u64,
    };
    let directory = VerificationRoot::open(&repository).expect("open artifact root");
    let read = authenticate_artifact_at(&artifact, &directory, true)
        .expect("authenticate original artifact");
    let authenticated = AuthenticatedArtifacts::new(
        BTreeMap::from([(
            artifact.clone(),
            read.bytes.expect("retained artifact bytes"),
        )]),
        vec![read.file],
    );

    std::fs::write(&path, b"substituted bytes\n").expect("replace artifact path");

    assert_eq!(authenticated.bytes(&artifact).unwrap(), original);
    assert!(authenticated.revalidate_paths().is_err());
    assert!(authenticate_artifact(&artifact, &repository).is_err());
    std::fs::remove_dir_all(root).expect("remove artifact snapshot root");
}

#[test]
fn held_artifact_rejects_whole_root_replacement() {
    let parent = std::env::temp_dir().join(format!(
        "rafter-authenticated-root-replacement-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&parent);
    let root = parent.join("repository");
    let moved = parent.join("original-repository");
    std::fs::create_dir_all(&root).expect("create original artifact root");
    std::fs::write(root.join("artifact"), b"authenticated bytes\n")
        .expect("write original artifact");

    let directory = VerificationRoot::open(&root).expect("open original artifact root");
    let held = directory
        .hold_file(Path::new("artifact"))
        .expect("hold original artifact");
    std::fs::rename(&root, &moved).expect("move original artifact root");
    std::fs::create_dir_all(&root).expect("create replacement artifact root");
    std::fs::write(root.join("artifact"), b"authenticated bytes\n")
        .expect("write replacement artifact");

    let error = held
        .verify_path_binding()
        .expect_err("root replacement must invalidate held artifact");
    assert!(error.to_string().contains("root changed"), "{error}");
    std::fs::remove_dir_all(parent).expect("remove root-replacement fixture");
}

#[test]
fn artifact_preflight_rejects_oversize_and_conflicting_paths_before_open() {
    let (catalog, manifest) = crate::tests::loaded();
    let mut bundle = crate::tests::passing_bundles(&catalog, &manifest)
        .into_iter()
        .find(|bundle| bundle.runner == "tests")
        .expect("tests bundle");
    bundle.execution.artifacts[0].size_bytes = MAX_ARTIFACT_BYTES + 1;
    let budget = BundleBudget::for_trusted("pr", "tests").expect("tests budget");
    assert!(preflight_artifacts(&bundle, budget, "tests")
        .unwrap_err()
        .to_string()
        .contains("exceeding"));

    let mut conflicting = crate::tests::passing_bundles(&catalog, &manifest)
        .into_iter()
        .find(|bundle| bundle.runner == "tests")
        .expect("tests bundle");
    let mut alias = conflicting.execution.artifacts[0].clone();
    alias.sha256 = "f".repeat(64);
    conflicting.execution.checks[0].artifacts.push(alias);
    let error = preflight_artifacts(&conflicting, budget, "tests").unwrap_err();
    assert!(
        error.to_string().contains("conflicting declarations"),
        "{error}"
    );

    let mut too_many = crate::tests::passing_bundles(&catalog, &manifest)
        .into_iter()
        .find(|bundle| bundle.runner == "tests")
        .expect("tests bundle");
    too_many.execution.checks.clear();
    too_many.results.clear();
    too_many.execution.artifacts = (0..=crate::evidence::limits::MAX_ARTIFACT_REFS_PER_BUNDLE)
        .map(|index| ArtifactRef {
            kind: "summary".to_owned(),
            path: format!("artifacts/reference-{index}"),
            sha256: format!("{index:064x}"),
            size_bytes: 1,
        })
        .collect();
    let error = preflight_artifacts(&too_many, budget, "tests").unwrap_err();
    assert!(error.to_string().contains("artifact references"), "{error}");
}

#[test]
fn artifact_reference_budget_accepts_its_boundary_and_rejects_the_next_reference() {
    let (catalog, manifest) = crate::tests::loaded();
    let mut bundle = crate::tests::passing_bundles(&catalog, &manifest)
        .into_iter()
        .find(|bundle| bundle.runner == "simulator")
        .expect("simulator bundle");
    bundle.execution.checks.clear();
    bundle.results.clear();
    bundle.execution.artifacts = (0..crate::evidence::limits::MAX_ARTIFACT_REFS_PER_BUNDLE - 1)
        .map(|index| ArtifactRef {
            kind: "summary".to_owned(),
            path: format!("artifacts/reference-{index}"),
            sha256: format!("{index:064x}"),
            size_bytes: 1,
        })
        .collect();
    let budget = BundleBudget::for_trusted("pr", "simulator").expect("simulator budget");

    preflight_artifacts(&bundle, budget, "simulator")
        .expect("producer executable plus the bounded artifact inventory is accepted");
    bundle.execution.artifacts.push(ArtifactRef {
        kind: "summary".to_owned(),
        path: "artifacts/reference-over-limit".to_owned(),
        sha256: "f".repeat(64),
        size_bytes: 1,
    });

    let error = preflight_artifacts(&bundle, budget, "simulator")
        .expect_err("one reference beyond the bounded inventory is rejected");
    assert!(error.to_string().contains("artifact references"), "{error}");
}

#[test]
fn nightly_maelstrom_budget_accepts_the_observed_shared_reference_shape() {
    let (catalog, manifest) = crate::tests::loaded();
    let mut bundle = crate::tests::passing_bundles(&catalog, &manifest)
        .into_iter()
        .find(|bundle| bundle.runner == "tests")
        .expect("tests bundle");
    bundle.runner = "maelstrom".to_owned();
    bundle.profile = "nightly".to_owned();
    bundle.results.clear();
    bundle.execution.checks.truncate(1);
    let artifacts = (0..821)
        .map(|index| ArtifactRef {
            kind: "summary".to_owned(),
            path: format!("artifacts/maelstrom-reference-{index}"),
            sha256: format!("{index:064x}"),
            size_bytes: 1,
        })
        .collect::<Vec<_>>();
    bundle.execution.artifacts.clone_from(&artifacts);
    bundle.execution.checks[0].artifacts = artifacts[..820].to_vec();
    let budget =
        BundleBudget::for_trusted("nightly", "maelstrom").expect("nightly Maelstrom budget");

    let accepted = preflight_artifacts(&bundle, budget, "maelstrom")
        .expect("the observed 1,642-declaration receipt shape is bounded");
    assert_eq!(accepted.len(), 822);
}

#[test]
fn tla_budget_accepts_the_observed_shared_reference_shape() {
    let bundle = tla_bundle_with_observed_shared_references();
    let budget = BundleBudget::for_trusted("nightly", "tla").expect("nightly TLA budget");

    let accepted = preflight_artifacts(&bundle, budget, "tla")
        .expect("the observed 602-declaration receipt shape is bounded");
    assert_eq!(accepted.len(), 41);
}

#[test]
fn tla_declaration_budget_accepts_its_boundary_and_rejects_the_next_declaration() {
    let mut bundle = tla_bundle_with_observed_shared_references();
    let budget = BundleBudget::for_trusted("nightly", "tla").expect("nightly TLA budget");
    let shared = bundle.results[0].artifacts[0].clone();
    bundle.results[0]
        .artifacts
        .extend(std::iter::repeat_n(shared.clone(), 422));

    preflight_artifacts(&bundle, budget, "tla")
        .expect("1,024 shared artifact declarations are accepted");
    bundle.results[0].artifacts.push(shared);

    let error = preflight_artifacts(&bundle, budget, "tla")
        .expect_err("the 1,025th artifact declaration is rejected");
    assert!(
        error.to_string().contains("1024-declaration limit"),
        "{error}"
    );
}

#[test]
fn tla_distinct_reference_budget_remains_bounded() {
    let mut bundle = tla_bundle_with_observed_shared_references();
    bundle.execution.checks.clear();
    bundle.results.clear();
    bundle.execution.artifacts = (0..crate::evidence::limits::MAX_ARTIFACT_REFS_PER_BUNDLE)
        .map(|index| ArtifactRef {
            kind: "summary".to_owned(),
            path: format!("artifacts/tla-distinct-reference-{index}"),
            sha256: format!("{index:064x}"),
            size_bytes: 1,
        })
        .collect();
    let budget = BundleBudget::for_trusted("nightly", "tla").expect("nightly TLA budget");

    let error = preflight_artifacts(&bundle, budget, "tla")
        .expect_err("513 distinct artifact references are rejected");
    assert!(error.to_string().contains("512-reference limit"), "{error}");
}

#[test]
fn tla_shared_references_still_reject_conflicting_declarations() {
    let mut bundle = tla_bundle_with_observed_shared_references();
    let mut conflicting = bundle.results[0].artifacts[0].clone();
    conflicting.sha256 = "f".repeat(64);
    bundle.results[0].artifacts.push(conflicting);
    let budget = BundleBudget::for_trusted("nightly", "tla").expect("nightly TLA budget");

    let error = preflight_artifacts(&bundle, budget, "tla")
        .expect_err("conflicting shared artifact declarations are rejected");
    assert!(
        error.to_string().contains("conflicting declarations"),
        "{error}"
    );
}

#[test]
fn noncanonical_and_hard_link_alias_paths_are_rejected() {
    let (catalog, manifest) = crate::tests::loaded();
    let mut bundle = crate::tests::passing_bundles(&catalog, &manifest)
        .into_iter()
        .find(|bundle| bundle.runner == "tests")
        .expect("tests bundle");
    bundle.execution.artifacts[0].path = "artifacts//summary.log".to_owned();
    let budget = BundleBudget::for_trusted("pr", "tests").expect("tests budget");
    assert!(preflight_artifacts(&bundle, budget, "tests").is_err());

    let root =
        std::env::temp_dir().join(format!("rafter-artifact-hard-link-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("create hard-link root");
    let original = root.join("original");
    let alias = root.join("alias");
    let bytes = b"same inode";
    std::fs::write(&original, bytes).expect("write original");
    std::fs::hard_link(&original, &alias).expect("create hard link");
    let directory = VerificationRoot::open(&root).expect("open hard-link root");
    let reference = |path: &str| ArtifactRef {
        kind: "test-log".to_owned(),
        path: path.to_owned(),
        sha256: format!("{:x}", Sha256::digest(bytes)),
        size_bytes: bytes.len() as u64,
    };
    let first = authenticate_artifact_at(&reference("original"), &directory, false)
        .expect("authenticate original");
    let second = authenticate_artifact_at(&reference("alias"), &directory, false)
        .expect("authenticate hard link");
    let error = super::reject_file_alias(&[first.file], &second.file)
        .expect_err("hard-link alias rejected");
    assert!(error.to_string().contains("aliases"));
    std::fs::remove_dir_all(root).expect("remove hard-link root");
}

#[test]
fn semantic_snapshot_budget_counts_unique_content_without_retaining_replay_binaries() {
    assert!(!retain_semantic_bytes("tests", "producer-binary").unwrap());
    assert!(!retain_semantic_bytes("tests", "test-binary").unwrap());
    assert!(!retain_semantic_bytes("simulator", "simulator-binary").unwrap());
    assert!(!retain_semantic_bytes("tla", "tla-tool").unwrap());
    assert!(!retain_semantic_bytes("maelstrom", "maelstrom-tool-jar").unwrap());
    assert!(!retain_semantic_bytes("maelstrom", "maelstrom-durable-file").unwrap());
    assert!(retain_semantic_bytes("tests", "test-log").unwrap());
    assert!(retain_semantic_bytes("tests", "unknown-kind").is_err());

    let (catalog, manifest) = crate::tests::loaded();
    let mut bundle = crate::tests::passing_bundles(&catalog, &manifest)
        .into_iter()
        .find(|bundle| bundle.runner == "tests")
        .expect("tests bundle");
    let budget = BundleBudget::for_trusted("pr", "tests").expect("tests budget");
    for index in 0..3 {
        bundle.execution.artifacts.push(ArtifactRef {
            kind: "test-log".to_owned(),
            path: format!("artifacts/oversize-retained-{index}"),
            sha256: format!("{index:064x}"),
            size_bytes: 200 * 1024 * 1024,
        });
    }
    let error = preflight_artifacts(&bundle, budget, "tests").unwrap_err();
    assert!(error.to_string().contains("semantic snapshot"), "{error}");
}
