//! Adversarial semantic readback scenarios for the published report set.

use std::fs;

use super::verify_report_set;

#[test]
fn exact_canonical_report_set_passes_semantic_readback() {
    let fixture = fixture();
    verify_report_set(&fixture.output, "pr", &fixture.catalog, &fixture.manifest)
        .expect("canonical report set verifies");
}

#[test]
fn malformed_or_internally_inconsistent_json_is_rejected() {
    let fixture = fixture();
    let path = fixture.output.join("pr.json");
    let mut value: serde_json::Value =
        serde_json::from_slice(&fs::read(&path).expect("read report")).expect("decode report");
    value["summary"]["green"] = serde_json::json!(44);
    fs::write(
        &path,
        format!("{}\n", serde_json::to_string_pretty(&value).unwrap()),
    )
    .expect("write inconsistent report");

    let error = verify_report_set(&fixture.output, "pr", &fixture.catalog, &fixture.manifest)
        .expect_err("inconsistent JSON must fail closed")
        .to_string();
    assert!(error.contains("summary does not match"));
}

#[test]
fn markdown_and_junit_status_changes_are_rejected() {
    for (name, from, to) in [
        ("pr.md", "RED", "GREEN"),
        ("pr.xml", "failures=\"44\"", "failures=\"0\""),
    ] {
        let fixture = fixture();
        let path = fixture.output.join(name);
        let source = fs::read_to_string(&path).expect("read projection");
        assert!(source.contains(from), "fixture omitted {from}");
        fs::write(&path, source.replacen(from, to, 1)).expect("write changed projection");

        let error = verify_report_set(&fixture.output, "pr", &fixture.catalog, &fixture.manifest)
            .expect_err("changed projection must fail closed")
            .to_string();
        assert!(error.contains("canonical verdict projection"));
    }
}

struct Fixture {
    _temp: tempfile::TempDir,
    output: std::path::PathBuf,
    catalog: crate::Catalog,
    manifest: crate::ProfileManifest,
}

fn fixture() -> Fixture {
    let (catalog, manifest) = crate::tests::loaded();
    let report = crate::tests::aggregate_unverified(&catalog, &manifest, "pr", "abc123", &[])
        .expect("build complete red report");
    let temp = tempfile::tempdir().expect("report-set fixture");
    let output = temp.path().join("reports");
    super::super::report::write(&report, &catalog, &manifest, &output)
        .expect("write canonical report set");
    Fixture {
        _temp: temp,
        output,
        catalog,
        manifest,
    }
}
