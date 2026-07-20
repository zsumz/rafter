//! Scenarios: verifier-owned source policy rejects cross-layer producer claims.

#[test]
fn every_reviewed_layer_has_one_exact_independent_source_contract() {
    let (catalog, manifest) = crate::tests::loaded();
    let bundles = crate::tests::passing_bundles(&catalog, &manifest);

    for bundle in &bundles {
        super::verify_layer_contract(&bundle.runner, &bundle.execution.source)
            .unwrap_or_else(|error| panic!("{} source contract: {error}", bundle.runner));
    }
}

#[test]
fn cross_layer_and_mutated_source_contracts_fail_closed() {
    let (catalog, manifest) = crate::tests::loaded();
    let bundles = crate::tests::passing_bundles(&catalog, &manifest);
    let tests = bundles
        .iter()
        .find(|bundle| bundle.runner == "tests")
        .expect("tests bundle");

    assert!(super::verify_layer_contract("simulator", &tests.execution.source).is_err());
    let mut mutated = tests.execution.source.clone();
    mutated.process_runtime.remove("ps");
    assert!(super::verify_layer_contract("tests", &mutated).is_err());
}

#[test]
fn every_checkout_identity_field_is_independently_compared() {
    let (catalog, manifest) = crate::tests::loaded();
    let source = crate::tests::passing_bundles(&catalog, &manifest)
        .into_iter()
        .find(|bundle| bundle.runner == "tests")
        .expect("tests bundle")
        .execution
        .source;
    let observed = checkout_observation(&source);

    macro_rules! assert_stale {
        ($mutation:expr) => {{
            let mut mutated = source.clone();
            $mutation(&mut mutated);
            assert!(matches!(
                super::verify_checkout_identity(&mutated, &observed),
                Err(super::SourceAuthenticationError::Stale(_))
            ));
        }};
    }

    assert_stale!(|value: &mut crate::SourceReceipt| value.commit.push('x'));
    assert_stale!(|value: &mut crate::SourceReceipt| value.tree.push('x'));
    assert_stale!(|value: &mut crate::SourceReceipt| value.materialization.contract.push('x'));
    assert_stale!(|value: &mut crate::SourceReceipt| value.materialization.sha256 = "f".repeat(64));
    assert_stale!(|value: &mut crate::SourceReceipt| value.materialization.tracked_entries += 1);
    assert_stale!(|value: &mut crate::SourceReceipt| value.materialization.submodules += 1);
    assert_stale!(|value: &mut crate::SourceReceipt| value.cargo_lock_sha256 = "f".repeat(64));
    assert_stale!(|value: &mut crate::SourceReceipt| value.cargo.push('x'));
    assert_stale!(|value: &mut crate::SourceReceipt| value.cargo_sha256 = "f".repeat(64));
    assert_stale!(|value: &mut crate::SourceReceipt| value.cargo_config_sha256 = "f".repeat(64));
    assert_stale!(|value: &mut crate::SourceReceipt| value.rustc.push('x'));
    assert_stale!(|value: &mut crate::SourceReceipt| value.rustc_sha256 = "f".repeat(64));
    assert_stale!(|value: &mut crate::SourceReceipt| value.target.push('x'));
}

#[test]
fn environment_and_clean_claims_fail_closed() {
    let (catalog, manifest) = crate::tests::loaded();
    let source = crate::tests::passing_bundles(&catalog, &manifest)
        .into_iter()
        .find(|bundle| bundle.runner == "tests")
        .expect("tests bundle")
        .execution
        .source;

    assert!(super::verify_receipt_flags(&source, &source.environment_sha256).is_ok());
    assert!(matches!(
        super::verify_receipt_flags(&source, &"f".repeat(64)),
        Err(super::SourceAuthenticationError::Stale(_))
    ));
    let mut not_clean = source;
    not_clean.clean = false;
    assert!(matches!(
        super::verify_receipt_flags(&not_clean, &not_clean.environment_sha256),
        Err(super::SourceAuthenticationError::Unverifiable(_))
    ));
}

fn checkout_observation(
    source: &crate::SourceReceipt,
) -> crate::provenance::source::CheckoutObservation {
    crate::provenance::source::CheckoutObservation {
        commit: source.commit.clone(),
        tree: source.tree.clone(),
        materialization: crate::provenance::source::MaterializationObservation {
            contract: source.materialization.contract.clone(),
            sha256: source.materialization.sha256.clone(),
            tracked_entries: source.materialization.tracked_entries,
            submodules: source.materialization.submodules,
        },
        cargo_lock_sha256: source.cargo_lock_sha256.clone(),
        cargo: source.cargo.clone(),
        cargo_sha256: source.cargo_sha256.clone(),
        cargo_config_sha256: source.cargo_config_sha256.clone(),
        rustc: source.rustc.clone(),
        rustc_sha256: source.rustc_sha256.clone(),
        target: source.target.clone(),
    }
}
