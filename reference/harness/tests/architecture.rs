use std::{
    fs,
    path::{Path, PathBuf},
};

const MANIFEST: &str = include_str!("../Cargo.toml");
const FORBIDDEN_TERMS: &[&str] = &[
    "ledger", "account", "balance", "lock", "fencing", "token", "counter", "shard", "session",
    "dedup",
];

#[test]
fn production_sources_contain_no_product_vocabulary() {
    let source_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    collect_files(&source_root, &mut files);
    assert!(
        !files.is_empty(),
        "the architecture scan must inspect source"
    );

    for path in files {
        let relative = path
            .strip_prefix(env!("CARGO_MANIFEST_DIR"))
            .expect("source paths stay below the manifest directory");
        let text = fs::read_to_string(&path).expect("production source is UTF-8");
        let inspected = format!("{}\n{}", relative.display(), text).to_ascii_lowercase();
        for term in FORBIDDEN_TERMS {
            assert!(
                !inspected.contains(term),
                "{} contains forbidden product term {term:?}",
                relative.display()
            );
        }
    }
}

#[test]
fn manifest_is_unpublished_and_has_no_dependencies() {
    assert!(MANIFEST.contains("publish = false"));
    assert!(
        !MANIFEST.contains("[dependencies]"),
        "the neutral harness must not depend on a reference consumer or Rafter crate"
    );
    for consumer in [
        "rafter-reference-ledger",
        "rafter-reference-fenced-lock",
        "rafter-reference-sharded-counter",
    ] {
        assert!(
            !MANIFEST.contains(consumer),
            "the harness must not depend on {consumer}"
        );
    }
}

fn collect_files(root: &Path, files: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(root).expect("source directory is readable") {
        let path = entry.expect("source entry is readable").path();
        if path.is_dir() {
            collect_files(&path, files);
        } else {
            files.push(path);
        }
    }
}
