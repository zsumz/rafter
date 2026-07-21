//! Reviewed registry fixture coverage for source-bound detector contracts.

use super::*;

#[test]
fn reviewed_registry_fixtures_have_source_bound_invocation_contracts() {
    let (catalog, _) = crate::tests::loaded();
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let root = fs::canonicalize(root).expect("canonical workspace root");
    let mut failures = Vec::new();
    let mut batch = DetectorFixtureAnalysis::default();
    let mut verified = 0;
    let mut unique_sources = BTreeSet::new();
    let mut fixtures_per_target = BTreeMap::new();

    for descriptor in catalog
        .evidence
        .iter()
        .filter(|evidence| evidence.layer == "simulator" && evidence.strength == "direct")
    {
        let Some(fixture) = descriptor.negative_fixture.as_deref() else {
            continue;
        };
        let Some(fixture_path) = descriptor.negative_fixture_path.as_deref() else {
            continue;
        };
        let Some(detector) = descriptor.negative_fixture_detector.as_deref() else {
            continue;
        };
        let fixture_path =
            fs::canonicalize(root.join(fixture_path)).expect("canonical registered fixture source");
        let detector_path = fs::canonicalize(root.join(descriptor.negative_detector_path()))
            .expect("canonical registered detector source");
        let fixture_source =
            fs::read_to_string(&fixture_path).expect("read registered fixture source");
        let detector_source =
            fs::read_to_string(&detector_path).expect("read registered detector source");
        unique_sources.insert(fixture_path.clone());
        unique_sources.insert(detector_path.clone());
        let identity = descriptor
            .simulator
            .as_ref()
            .and_then(|identity| identity.negative_test.as_ref())
            .expect("registered direct simulator fixture identity");
        *fixtures_per_target
            .entry((
                identity.package.clone(),
                identity.target_kind.clone(),
                identity.target.clone(),
            ))
            .or_insert(0usize) += 1;

        verified += 1;
        if let Err(error) = batch.validate(&crate::DetectorFixtureSourceBinding {
            fixture_source: &fixture_source,
            detector_source: &detector_source,
            source_root: &root,
            fixture_path: &fixture_path,
            detector_path: &detector_path,
            test_identity: identity,
            fixture,
            detector,
        }) {
            failures.push(format!(
                "{} {fixture} -> {detector}: {error}",
                descriptor.invariant_id
            ));
        }
    }

    assert!(
        failures.is_empty(),
        "registered detector invocation contracts failed:\n{}",
        failures.join("\n")
    );
    assert!(
        verified > 1,
        "registry batch must exercise multiple fixtures"
    );
    assert!(
        fixtures_per_target.values().any(|count| *count > 1),
        "registry batch must exercise many fixtures from one target"
    );
    assert_eq!(
        batch.target_analysis_count(),
        fixtures_per_target.len(),
        "each shared target must be analyzed once across the registry batch"
    );
    assert_eq!(
        batch.source_parse_count(),
        unique_sources.len(),
        "fixture and detector syntax trees must be cached by canonical path"
    );
}
