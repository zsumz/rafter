use std::{
    fs,
    path::{Path, PathBuf},
};

const FORBIDDEN_POLICY_MARKERS: &[(&str, &str)] = &[
    ("countercommand", "counter command schema"),
    ("counterresult", "counter result schema"),
    ("counter_", "counter product identifier"),
    ("shardid", "shard identity policy"),
    ("shard_id", "shard identity policy"),
    ("tombstone", "group-incarnation retention policy"),
    ("sessionid", "application session identity"),
    ("session_id", "application session identity"),
    ("dedup", "application deduplication policy"),
    ("fencingtoken", "application fencing-token policy"),
    ("fencing_token", "application fencing-token policy"),
    ("authentication", "transport authentication policy"),
    ("certificate", "certificate policy"),
    ("rustls", "concrete secure-transport policy"),
    ("internal-test-hooks", "unpublished test hooks"),
    ("rafter_sim", "unpublished simulation hooks"),
    ("#[cfg(test)]", "test-only observation surface"),
    ("reference::", "reference-consumer dependency"),
];

#[test]
fn managed_scheduler_sources_remain_product_and_transport_neutral() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/managed");
    let mut files = rust_sources(&root);
    files.sort();

    let violations = files
        .iter()
        .flat_map(|path| policy_violations(path))
        .collect::<Vec<_>>();

    assert!(
        violations.is_empty(),
        "managed scheduler policy boundary failed:\n{}",
        violations.join("\n")
    );
}

#[test]
fn policy_boundary_detector_rejects_each_forbidden_shape() {
    for (marker, reason) in FORBIDDEN_POLICY_MARKERS {
        let source = format!("pub struct BoundaryProbe {{ value: {marker:?} }}");
        let violations = source_violations("fixture.rs", &source);
        assert_eq!(
            violations.len(),
            1,
            "marker {marker:?} ({reason}) must be detected"
        );
    }
}

fn rust_sources(root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    let entries = fs::read_dir(root).expect("managed scheduler source directory exists");
    for entry in entries {
        let path = entry
            .expect("managed scheduler source entry is readable")
            .path();
        if path.is_dir() {
            files.extend(rust_sources(&path));
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            files.push(path);
        }
    }
    files
}

fn policy_violations(path: &Path) -> Vec<String> {
    let source = fs::read_to_string(path).expect("managed scheduler source is readable");
    source_violations(&path.display().to_string(), &source)
}

fn source_violations(path: &str, source: &str) -> Vec<String> {
    let source = source.to_ascii_lowercase();
    FORBIDDEN_POLICY_MARKERS
        .iter()
        .filter(|(marker, _)| source.contains(marker))
        .map(|(marker, reason)| format!("{path}: {reason} ({marker})"))
        .collect()
}
