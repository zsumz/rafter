//! Cross-cutting producer publication tests.

use super::stage_bundle;

#[test]
fn staged_bundle_is_invisible_until_atomic_publication() {
    let (catalog, manifest) = crate::tests::loaded();
    let bundle = crate::tests::passing_bundles(&catalog, &manifest)
        .into_iter()
        .next()
        .expect("passing fixture bundle");
    let output_dir = std::path::Path::new("target/rafter-invariants").join(format!(
        "rafter-staged-bundle-{}-{}",
        std::process::id(),
        bundle.runner
    ));
    let _ = std::fs::remove_dir_all(&output_dir);

    let staged = stage_bundle(&bundle, &output_dir).expect("stage bundle");
    let temporary = staged.temporary.clone();
    let authoritative = staged.path.clone();
    assert!(temporary.is_file());
    assert!(!authoritative.exists());
    drop(staged);
    assert!(!temporary.exists());
    assert!(!authoritative.exists());

    let published = stage_bundle(&bundle, &output_dir)
        .expect("restage bundle")
        .publish()
        .expect("publish bundle");
    assert_eq!(published, authoritative);
    assert!(authoritative.is_file());
    let _ = std::fs::remove_dir_all(output_dir);
}
