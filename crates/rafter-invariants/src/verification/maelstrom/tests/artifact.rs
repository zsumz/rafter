//! Scenarios: multi-trial checks resolve exactly one of each shared input.

use super::{group_trials, unique};
use crate::{ArtifactRef, CheckCompletion, CheckReceipt};

fn artifact(path: &str, kind: &str) -> ArtifactRef {
    ArtifactRef {
        kind: kind.to_owned(),
        path: format!("artifacts/invariants/evidence/{path}"),
        sha256: format!("{:064x}", path.len()),
        size_bytes: 1,
    }
}

/// Shared tool inputs live outside any `trial-N` directory; per-trial evidence
/// lives inside one. `trials` is how many trial directories the check carries.
fn check(trials: u64, shared_repeats: usize) -> CheckReceipt {
    let mut artifacts = Vec::new();
    for _ in 0..shared_repeats {
        for (path, kind) in [
            ("inputs/runner", "maelstrom-runner"),
            ("inputs/binary", "maelstrom-binary"),
            ("inputs/maelstrom.jar", "maelstrom-tool-jar"),
        ] {
            artifacts.push(artifact(path, kind));
        }
    }
    for trial in 0..trials {
        for (name, kind) in [
            ("results.edn", "maelstrom-results"),
            ("process.json", "maelstrom-process-log"),
        ] {
            artifacts.push(artifact(&format!("trial-{trial}/{name}"), kind));
        }
    }
    CheckReceipt {
        execution_id: "maelstrom-execution-0".to_owned(),
        check_id: "maelstrom/base".to_owned(),
        evidence_ids: vec!["RD-06/test".to_owned()],
        completion: CheckCompletion::Completed,
        observations: std::collections::BTreeMap::new(),
        simulator_liveness: None,
        tla_continuation: None,
        duration_ms: 1,
        peak_rss_kib: 1,
        artifacts,
    }
}

/// The weekly profile runs three trials against one captured runner script.
/// Grouping must put the shared inputs aside once, not once per trial, and each
/// trial must keep its own evidence.
#[test]
fn a_multi_trial_check_resolves_one_shared_input_per_kind() {
    for trials in 1..=3 {
        let check = check(trials, 1);
        let grouped = group_trials(&check).expect("multi-trial check groups");

        assert_eq!(
            grouped.trials.len(),
            usize::try_from(trials).expect("small")
        );
        for kind in ["maelstrom-runner", "maelstrom-binary", "maelstrom-tool-jar"] {
            unique(&grouped.shared, kind)
                .unwrap_or_else(|error| panic!("{trials} trials, {kind}: {error}"));
        }
        for artifacts in grouped.trials.values() {
            unique(artifacts, "maelstrom-results").expect("one result set per trial");
            unique(artifacts, "maelstrom-process-log").expect("one process log per trial");
        }
    }
}

/// The pre-fix producer listed each shared input once per trial, describing one
/// file as three artifacts. The verifier's invariant is right to refuse that,
/// and keeps refusing it: the fix belongs in the claim, not in this check.
#[test]
fn a_shared_input_repeated_per_trial_is_still_ambiguous() {
    let repeated = check(3, 3);
    let grouped = group_trials(&repeated).expect("check groups");
    let error = unique(&grouped.shared, "maelstrom-runner")
        .expect_err("a shared input listed once per trial is ambiguous");
    assert!(error.to_string().contains("ambiguous"), "{error}");
}

mod binding_class {
    use std::path::PathBuf;
    use std::sync::atomic::AtomicU64;

    static SEQUENCE: AtomicU64 = AtomicU64::new(0);

    use crate::verification::maelstrom::binding::{verify_input_binding_for_test, InputBinding};
    use crate::verification::VerificationContext;
    use crate::ArtifactRef;

    /// Builds an authenticated snapshot holding `bytes` under `artifact`.
    fn authenticated(
        artifact: &ArtifactRef,
        bytes: &[u8],
    ) -> crate::verification::AuthenticatedArtifacts {
        crate::verification::AuthenticatedArtifacts::for_test(std::collections::BTreeMap::from([(
            artifact.clone(),
            std::sync::Arc::from(bytes.to_vec().into_boxed_slice()),
        )]))
    }

    fn artifact(kind: &str, bytes: &[u8]) -> ArtifactRef {
        use sha2::{Digest, Sha256};
        ArtifactRef {
            kind: kind.to_owned(),
            path: format!("artifacts/invariants/evidence/inputs/{kind}"),
            sha256: format!("{:x}", Sha256::digest(bytes)),
            size_bytes: bytes.len() as u64,
        }
    }

    fn temp_file(name: &str, bytes: &[u8]) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "rafter-binding-{}-{}-{name}",
            std::process::id(),
            SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        std::fs::write(&path, bytes).expect("write binding fixture");
        path
    }

    /// The producing job holds the binary it built, so it still binds by
    /// byte-equality and a missing file is still fatal there.
    #[test]
    fn a_producer_context_build_output_still_requires_its_file() {
        let bytes = b"built-binary".as_slice();
        let reference = artifact("maelstrom-binary", bytes);
        let snapshot = authenticated(&reference, bytes);
        let absent = std::env::temp_dir().join("rafter-binding-absent-file");
        let _ = std::fs::remove_file(&absent);

        let error = verify_input_binding_for_test(
            &reference,
            &InputBinding::BuildOutput(absent),
            VerificationContext::ProducingJob,
            &snapshot,
        )
        .expect_err("the producing job must still read the file it built");
        assert!(error.to_string().contains("read source-bound"), "{error}");
    }

    /// A producing job whose on-disk binary disagrees with what it published
    /// is exactly as fatal as before.
    #[test]
    fn a_producer_context_build_output_still_rejects_a_mismatched_file() {
        let bytes = b"built-binary".as_slice();
        let reference = artifact("maelstrom-binary", bytes);
        let snapshot = authenticated(&reference, bytes);
        let path = temp_file("mismatch", b"a different binary");

        let error = verify_input_binding_for_test(
            &reference,
            &InputBinding::BuildOutput(path.clone()),
            VerificationContext::ProducingJob,
            &snapshot,
        )
        .expect_err("published bytes must match the file they were captured from");
        assert!(
            error
                .to_string()
                .contains("does not match the source-bound"),
            "{error}"
        );
        let _ = std::fs::remove_file(path);
    }

    /// The aggregate has the repository but nothing anyone else built, so a
    /// build output binds by the presence of the bytes the bundle authenticated.
    #[test]
    fn an_aggregate_context_build_output_verifies_without_the_file() {
        let bytes = b"built-binary".as_slice();
        let reference = artifact("maelstrom-binary", bytes);
        let snapshot = authenticated(&reference, bytes);

        verify_input_binding_for_test(
            &reference,
            &InputBinding::BuildOutput(PathBuf::from("target/debug/never-built-here")),
            VerificationContext::Aggregate,
            &snapshot,
        )
        .expect("retained published bytes verify without the producing job's build tree");
    }

    /// Relaxing the file comparison does not relax the failure mode: an
    /// artifact the bundle never retained has nothing to assert, and the
    /// aggregate refuses rather than treating absence as agreement.
    ///
    /// This is deliberately not a tampered-bytes test. There used to be one,
    /// and it could only be written by building an `AuthenticatedArtifacts`
    /// whose retained bytes disagree with the declaration they were retained
    /// under -- a state the production constructor cannot produce, because
    /// `bundle::integrity::file::read_declared` compares digest and length
    /// before retaining and drops the bytes if they disagree. The check it
    /// exercised was a second hash of already-hashed bytes with one possible
    /// answer. Tampering is caught at bundle authentication, which has the file
    /// and the declaration; the tests for that live beside it in
    /// `verification::bundle::integrity`.
    #[test]
    fn an_aggregate_context_build_output_rejects_an_unretained_artifact() {
        let reference = artifact("maelstrom-binary", b"built-binary");
        let unrelated = artifact("maelstrom-proxy-binary", b"something-else");
        let snapshot = authenticated(&unrelated, b"something-else");

        let error = verify_input_binding_for_test(
            &reference,
            &InputBinding::BuildOutput(PathBuf::from("target/debug/never-built-here")),
            VerificationContext::Aggregate,
            &snapshot,
        )
        .expect_err("an artifact the bundle never retained cannot be asserted present");
        assert!(error.to_string().contains("was not retained"), "{error}");
    }

    /// Source bindings are unchanged everywhere: the runner script is in the
    /// checkout, so the aggregate still re-derives it from the checkout.
    #[test]
    fn a_source_binding_still_requires_the_checkout_file_in_the_aggregate() {
        let bytes = b"#!/bin/sh\n".as_slice();
        let reference = artifact("maelstrom-runner", bytes);
        let snapshot = authenticated(&reference, bytes);
        let absent = std::env::temp_dir().join("rafter-binding-absent-script");
        let _ = std::fs::remove_file(&absent);

        let error = verify_input_binding_for_test(
            &reference,
            &InputBinding::Checkout(absent),
            VerificationContext::Aggregate,
            &snapshot,
        )
        .expect_err("a version-controlled input is re-derivable in every context");
        assert!(error.to_string().contains("read source-bound"), "{error}");
    }
}

/// Two places decide which inputs are shared across trials: the trial grouping
/// that sets them aside, and the receipt validator that counts them. They
/// disagreed -- grouping treated them as shared while the validator still
/// demanded one copy per trial -- and a single-trial profile cannot tell the
/// two rules apart, so nightly stayed green while weekly's three trials were
/// refused. This pins them together.
#[test]
fn the_shared_input_kinds_are_exactly_those_counted_once_per_check() {
    use super::super::artifact::SHARED_INPUT_KINDS;

    let counted_once = [
        "maelstrom-runner",
        "maelstrom-binary",
        "maelstrom-tool-jar",
        "maelstrom-proxy-binary",
    ];
    let mut shared = SHARED_INPUT_KINDS.to_vec();
    shared.sort_unstable();
    let mut expected = counted_once.to_vec();
    expected.sort_unstable();
    assert_eq!(
        shared, expected,
        "a kind is shared exactly when the receipt names it once per check"
    );

    let completion = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src/verification/maelstrom/receipt/completion.rs"),
    )
    .expect("read the receipt validator");
    for kind in counted_once {
        assert!(
            completion.contains(&format!("artifact_count(check, \"{kind}\") != 1")),
            "{kind} is shared, so the receipt validator must require exactly one"
        );
    }
}
