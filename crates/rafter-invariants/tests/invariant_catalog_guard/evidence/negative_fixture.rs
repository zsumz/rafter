//! Declared negative fixtures and the detector each one must actually drive.
//!
//! A fixture name must resolve to a real declaration, a direct simulator row
//! must bind an executable detector-level control, and an exemption may never
//! stand alongside the fixture it excuses.

use std::{fs, path::Path};

use super::call_closure::assert_detector_relates_to_registered_checker;
use super::identity::{test_identity_matches_source, test_identity_uses_typed_oracle};
use super::symbols::source_declares_symbol;
use super::Evidence;

pub(super) fn assert_negative_fixture_policy(
    workspace: &Path,
    record: &Evidence,
    source: &str,
    detector_sources: &mut rafter_invariants::DetectorFixtureAnalysis,
) {
    assert_declared_negative_fixture(workspace, record, source, detector_sources);

    if let Some(exemption) = &record.negative_fixture_exemption {
        assert!(
            !exemption.trim().is_empty(),
            "{} {} {} negative fixture exemption must explain the reviewed exception",
            record.id,
            record.layer,
            record.strength,
        );
        assert!(
            record.negative_fixture.is_none(),
            "{} {} {} must not declare both negative_fixture and negative_fixture_exemption",
            record.id,
            record.layer,
            record.strength,
        );
    }

    assert_direct_simulator_fixture_policy(record);

    if record.negative_fixture_path.is_some() {
        assert!(
            record.negative_fixture.is_some(),
            "{} {} {} must not declare negative_fixture_path without negative_fixture",
            record.id,
            record.layer,
            record.strength,
        );
    }

    if record.negative_fixture_detector.is_some() {
        assert!(
            record.layer == "simulator" && record.strength == "direct",
            "{} {} {} negative_fixture_detector is only meaningful for simulator direct evidence",
            record.id,
            record.layer,
            record.strength,
        );
        assert!(
            record.negative_fixture.is_some(),
            "{} {} {} must not declare negative_fixture_detector without negative_fixture",
            record.id,
            record.layer,
            record.strength,
        );
    }
}

pub(super) fn assert_declared_negative_fixture(
    workspace: &Path,
    record: &Evidence,
    source: &str,
    detector_sources: &mut rafter_invariants::DetectorFixtureAnalysis,
) {
    let Some(negative_fixture) = &record.negative_fixture else {
        return;
    };
    let fixture_source = record.negative_fixture_path.as_ref().map_or_else(
        || source.to_owned(),
        |path| {
            let fixture_path = workspace.join(path);
            fs::read_to_string(&fixture_path).unwrap_or_else(|error| {
                panic!(
                    "read negative fixture path {}: {error}",
                    fixture_path.display()
                )
            })
        },
    );
    let fixture_path = record
        .negative_fixture_path
        .as_deref()
        .map_or_else(|| workspace.join(&record.path), |path| workspace.join(path));
    assert!(
        source_declares_symbol(&fixture_path, &fixture_source, negative_fixture),
        "{} {} {} negative fixture `{}` was not found in {}",
        record.id,
        record.layer,
        record.strength,
        negative_fixture,
        record
            .negative_fixture_path
            .as_deref()
            .unwrap_or(record.path.as_str()),
    );
    if record.layer == "simulator" && record.strength == "direct" {
        assert_simulator_detector_fixture(
            workspace,
            record,
            negative_fixture,
            &fixture_path,
            &fixture_source,
            detector_sources,
        );
    }
}

pub(super) fn assert_simulator_detector_fixture(
    workspace: &Path,
    record: &Evidence,
    negative_fixture: &str,
    fixture_path: &Path,
    fixture_source: &str,
    detector_sources: &mut rafter_invariants::DetectorFixtureAnalysis,
) {
    let detector = record
        .negative_fixture_detector
        .as_ref()
        .unwrap_or_else(|| {
            panic!(
                "{} simulator direct negative fixture `{negative_fixture}` must name negative_fixture_detector",
                record.id,
            )
        });
    let fixture_path_text = record
        .negative_fixture_path
        .as_deref()
        .unwrap_or(record.path.as_str());
    let detector_path_text = record
        .negative_fixture_detector_path
        .as_deref()
        .unwrap_or(record.path.as_str());
    let detector_path = workspace.join(detector_path_text);
    let detector_source = fs::read_to_string(&detector_path).unwrap_or_else(|error| {
        panic!(
            "read simulator detector source {}: {error}",
            detector_path.display()
        )
    });
    let identity = record
        .simulator
        .as_ref()
        .and_then(|identity| identity.negative_test.as_ref())
        .expect("direct simulator fixture has executable identity");
    let source_contract =
        detector_sources.validate(&rafter_invariants::DetectorFixtureSourceBinding {
            fixture_source,
            detector_source: &detector_source,
            source_root: workspace,
            fixture_path,
            detector_path: &detector_path,
            test_identity: identity,
            fixture: negative_fixture,
            detector,
        });
    assert!(
        source_contract.is_ok(),
        "{} simulator fixture `{negative_fixture}` has no invocation-bound detector contract: {}",
        record.id,
        source_contract.expect_err("failed source contract has an error"),
    );
    assert!(
        test_identity_matches_source(
            workspace,
            fixture_path_text,
            fixture_source,
            negative_fixture,
            identity,
        ),
        "{} simulator fixture `{negative_fixture}` execution identity `{}` does not match its analyzed module",
        record.id,
        identity.test_name,
    );
    assert!(
        test_identity_uses_typed_oracle(
            workspace,
            fixture_path_text,
            fixture_source,
            negative_fixture,
            identity,
        ),
        "{} simulator fixture `{negative_fixture}` must execute an explicit typed oracle macro",
        record.id,
    );
    assert_detector_relates_to_registered_checker(workspace, record, detector, &detector_source);
}

pub(super) fn assert_direct_simulator_fixture_policy(record: &Evidence) {
    if record.layer != "simulator" || record.strength != "direct" {
        return;
    }
    assert!(
        record.negative_fixture_exemption.is_none(),
        "{} simulator direct evidence may not use negative_fixture_exemption",
        record.id,
    );
    assert!(
        record.negative_fixture.is_some()
            && record.negative_fixture_detector.is_some()
            && record
                .simulator
                .as_ref()
                .and_then(|identity| identity.negative_test.as_ref())
                .is_some(),
        "{} simulator direct evidence must bind an executable detector-level negative fixture",
        record.id,
    );
}
