//! Detector replay inventory digest scenarios.

use super::*;

#[test]
fn inventory_digest_has_a_fixed_width_known_answer() {
    let target = ReplayTarget {
        package: "fixture-package".to_owned(),
        kind: "test".to_owned(),
        name: "fixture-target".to_owned(),
    };
    let fixture = ReplayFixture {
        identity: TestIdentity {
            package: "fixture-package".to_owned(),
            target_kind: "test".to_owned(),
            target: "fixture-target".to_owned(),
            test_name: "fixture::rejects".to_owned(),
        },
        fixture: "rejects".to_owned(),
        fixture_path: PathBuf::from("tests/fixture.rs"),
        fixture_sha256: "1".repeat(64),
        detector: "detect".to_owned(),
        detector_path: PathBuf::from("src/detector.rs"),
        detector_sha256: "2".repeat(64),
        registered_identity: "fixture::detect()".to_owned(),
        source_graph_sha256: "5".repeat(64),
        expected_witnesses: BTreeMap::from([("expect-err:fixture::detect()".to_owned(), 2)]),
        evidence: vec![ReplayEvidence {
            invariant_id: "ST-01".to_owned(),
            evidence_id: "ST-01/direct".to_owned(),
        }],
    };
    let inventory = DetectorReplayPlan::new(BTreeMap::from([(target, vec![fixture])]));

    assert_eq!(
        inventory
            .inventory_sha256()
            .expect("digest fixed inventory"),
        "75ed0048ab51af1804d64c579c69f45ecce3526610a6d927017533341d1d2a11"
    );
}

#[test]
fn transitive_source_receipt_changes_the_reviewed_inventory_identity() {
    let first = fixture();
    let mut second = first.clone();
    second.source_graph_sha256 = "6".repeat(64);
    let before = DetectorReplayPlan::new(BTreeMap::from([(
        ReplayTarget {
            package: "fixture".to_owned(),
            kind: "lib".to_owned(),
            name: "fixture".to_owned(),
        },
        vec![first],
    )]));
    let after = DetectorReplayPlan::new(BTreeMap::from([(
        ReplayTarget {
            package: "fixture".to_owned(),
            kind: "lib".to_owned(),
            name: "fixture".to_owned(),
        },
        vec![second],
    )]));

    assert_ne!(
        before.inventory_sha256().expect("hash initial inventory"),
        after.inventory_sha256().expect("hash changed inventory")
    );
}

fn fixture() -> ReplayFixture {
    ReplayFixture {
        identity: TestIdentity {
            package: "fixture".to_owned(),
            target_kind: "lib".to_owned(),
            target: "fixture".to_owned(),
            test_name: "tests::rejects".to_owned(),
        },
        fixture: "rejects".to_owned(),
        fixture_path: PathBuf::from("src/tests.rs"),
        fixture_sha256: "1".repeat(64),
        detector: "detect".to_owned(),
        detector_path: PathBuf::from("src/detector.rs"),
        detector_sha256: "2".repeat(64),
        registered_identity: "fixture::detector::detect".to_owned(),
        source_graph_sha256: "5".repeat(64),
        expected_witnesses: BTreeMap::from([(
            "expect-err:fixture::detector::detect".to_owned(),
            1,
        )]),
        evidence: vec![ReplayEvidence {
            invariant_id: "ST-01".to_owned(),
            evidence_id: "ST-01/direct".to_owned(),
        }],
    }
}
