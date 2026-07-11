use std::{collections::BTreeSet, fs, path::Path};

use super::{Entry, Evidence, COVERAGE_LAYERS, VALID_EVIDENCE_STRENGTHS};

pub(super) fn assert_evidence_is_machine_checkable(
    workspace: &Path,
    entries: &[Entry],
    evidence: &[Evidence],
) {
    assert!(
        !evidence.is_empty(),
        "registry must declare machine-checkable evidence records",
    );

    let ids = entries
        .iter()
        .map(|entry| entry.id.as_str())
        .collect::<BTreeSet<_>>();
    let mut seen_records = BTreeSet::new();

    for record in evidence {
        assert!(
            ids.contains(record.id.as_str()),
            "evidence record references unknown invariant ID {}",
            record.id,
        );
        assert!(
            COVERAGE_LAYERS.contains(&record.layer.as_str()),
            "{} evidence has unknown layer {}",
            record.id,
            record.layer,
        );
        assert!(
            VALID_EVIDENCE_STRENGTHS.contains(&record.strength.as_str()),
            "{} evidence has unknown strength {}",
            record.id,
            record.strength,
        );
        assert!(
            seen_records.insert((
                record.id.as_str(),
                record.layer.as_str(),
                record.strength.as_str(),
                record.path.as_str(),
                record.symbol.as_str(),
                record.negative_fixture.as_deref(),
                record.negative_fixture_path.as_deref(),
                record.negative_fixture_detector.as_deref(),
                record.negative_fixture_exemption.as_deref(),
            )),
            "{} {} {} evidence record for {}#{} is duplicated",
            record.id,
            record.layer,
            record.strength,
            record.path,
            record.symbol,
        );
        assert!(
            !record.path.trim().is_empty() && !record.symbol.trim().is_empty(),
            "{} {} {} evidence must name path and symbol",
            record.id,
            record.layer,
            record.strength,
        );
        let path = workspace.join(&record.path);
        let source = fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("read evidence path {}: {error}", path.display()));
        assert!(
            source.contains(&record.symbol),
            "{} {} {} evidence symbol `{}` was not found in {}",
            record.id,
            record.layer,
            record.strength,
            record.symbol,
            record.path,
        );
        assert_negative_fixture_policy(workspace, record, &source);
    }

    for entry in entries {
        for layer in COVERAGE_LAYERS {
            let coverage = entry
                .current_coverage
                .get(*layer)
                .unwrap_or_else(|| panic!("{} missing current_coverage.{layer}", entry.id));
            let Some(strength) = evidence_strength_for_coverage(coverage) else {
                continue;
            };
            assert!(
                evidence.iter().any(|record| {
                    record.id == entry.id && record.layer == *layer && record.strength == strength
                }),
                "{} current_coverage.{} declares {} evidence but has no machine-checkable evidence record",
                entry.id,
                layer,
                coverage,
            );
        }
    }
}

fn assert_negative_fixture_policy(workspace: &Path, record: &Evidence, source: &str) {
    if let Some(negative_fixture) = &record.negative_fixture {
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
        assert!(
            fixture_source.contains(negative_fixture),
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
            let Some(detector) = record.negative_fixture_detector.as_ref() else {
                panic!(
                    "{} simulator direct negative fixture `{}` must name negative_fixture_detector",
                    record.id, negative_fixture
                )
            };
            assert!(
                fixture_source.contains(detector),
                "{} simulator direct negative fixture `{}` must exercise detector `{}` in {}",
                record.id,
                negative_fixture,
                detector,
                record
                    .negative_fixture_path
                    .as_deref()
                    .unwrap_or(record.path.as_str()),
            );
        }
    }

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

    if record.layer == "simulator" && record.strength == "direct" {
        assert!(
            record.negative_fixture.is_some() || record.negative_fixture_exemption.is_some(),
            "{} simulator direct evidence must name a negative_fixture or reviewed negative_fixture_exemption",
            record.id,
        );
    }

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

fn evidence_strength_for_coverage(coverage: &str) -> Option<&'static str> {
    let coverage = coverage.trim();
    if coverage == "D" || coverage.starts_with("D:") {
        Some("direct")
    } else if coverage == "E2E" || coverage.starts_with("E2E:") {
        Some("e2e")
    } else {
        None
    }
}
