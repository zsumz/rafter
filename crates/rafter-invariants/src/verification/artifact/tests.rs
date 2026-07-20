//! Scenarios for trusted artifact-verification identity.

use std::path::Path;

use super::verify_bundle;

#[test]
fn separately_supplied_profile_and_runner_cannot_override_the_receipt() {
    let (catalog, manifest) = crate::tests::loaded();
    let bundle = crate::tests::passing_bundles(&catalog, &manifest)
        .into_iter()
        .find(|bundle| bundle.runner == "tests")
        .expect("tests bundle");

    for (profile, runner) in [("nightly", "tests"), ("pr", "simulator")] {
        let error = verify_bundle(
            &bundle,
            Path::new("."),
            Path::new("."),
            &catalog,
            profile,
            runner,
        )
        .expect_err("trusted identity mismatch must fail before artifact access");
        assert!(error.to_string().contains("identity mismatch"), "{error}");
    }
}
