//! Scenarios for authenticated snapshot isolation and target analysis.

use std::{fs, path::PathBuf};

use super::SourceSnapshot;
use crate::provenance::source::CapturedSourceFile;

#[test]
fn snapshot_is_materialized_inside_the_bound_workspace() {
    let snapshot = snapshot(vec![file("src/lib.rs", b"pub fn value() {}\n")]);
    let workspace = fs::canonicalize(".").expect("canonical workspace");
    let parent = workspace.join("target/rafter-invariants/verified-source");

    assert!(snapshot.root().starts_with(parent));
}

#[test]
fn semantic_source_is_detached_from_later_ambient_path_replacement() {
    let ambient = tempfile::tempdir().expect("create ambient source root");
    let relative = PathBuf::from("src/lib.rs");
    let ambient_path = ambient.path().join(&relative);
    fs::create_dir_all(ambient_path.parent().expect("source parent"))
        .expect("create ambient source parent");
    fs::write(&ambient_path, b"authenticated source\n").expect("write ambient source");

    let snapshot = snapshot(vec![file(
        &relative,
        fs::read(&ambient_path).expect("capture ambient source"),
    )]);
    fs::write(&ambient_path, b"substituted source\n").expect("replace ambient source");

    assert_eq!(
        fs::read(snapshot.root().join(relative)).expect("read semantic source"),
        b"authenticated source\n"
    );
    snapshot.revalidate().expect("snapshot remains authentic");
}

#[test]
fn detached_snapshot_constructs_target_graph_without_git_metadata() {
    let snapshot = snapshot(vec![
        file(
            "Cargo.toml",
            br#"[workspace]
members = ["sample"]
resolver = "2"
"#,
        ),
        file(
            "sample/Cargo.toml",
            br#"[package]
name = "sample"
version = "0.0.0"
edition = "2021"
"#,
        ),
        file("sample/src/lib.rs", b"mod child;\n"),
        file("sample/src/child.rs", b"pub fn detector() {}\n"),
    ]);

    assert!(!snapshot.root().join(".git").exists());
    let graph = crate::verification::target::target_source_graph(
        snapshot.root(),
        "sample",
        "lib",
        "sample",
        &[],
    )
    .expect("construct target graph from authenticated inventory");

    assert!(graph
        .declaration_identities()
        .get("detector")
        .is_some_and(|identities| identities == &["sample::child::detector"]));
}

#[test]
fn same_byte_snapshot_file_replacement_is_rejected() {
    let snapshot = snapshot(vec![file("src/lib.rs", b"authenticated source\n")]);
    let source = snapshot.root().join("src/lib.rs");
    let displaced = snapshot.root().with_extension("displaced-source");
    make_directory_writable(source.parent().expect("source parent"));
    fs::rename(&source, &displaced).expect("displace authenticated file");
    fs::write(&source, b"authenticated source\n").expect("replace with identical bytes");
    make_directory_read_only(source.parent().expect("source parent"));

    let error = snapshot
        .revalidate()
        .expect_err("replacement must change retained file identity");
    assert!(error.contains("identity changed"), "{error}");
    fs::remove_file(displaced).expect("remove displaced authenticated file");
}

#[cfg(unix)]
#[test]
fn snapshot_directories_are_not_owner_writable() {
    use std::os::unix::fs::PermissionsExt;

    let snapshot = snapshot(vec![file("src/lib.rs", b"pub fn value() {}\n")]);
    let source_directory = snapshot.root().join("src");
    for directory in [snapshot.root(), source_directory.as_path()] {
        let mode = fs::metadata(directory)
            .expect("snapshot directory metadata")
            .permissions()
            .mode();
        assert_eq!(mode & 0o200, 0, "{} remained writable", directory.display());
    }
}

fn snapshot(files: Vec<CapturedSourceFile>) -> SourceSnapshot {
    SourceSnapshot::materialize(files).expect("materialize authenticated source snapshot")
}

fn file(path: impl Into<PathBuf>, bytes: impl AsRef<[u8]>) -> CapturedSourceFile {
    CapturedSourceFile {
        path: path.into(),
        executable: false,
        bytes: bytes.as_ref().to_vec(),
    }
}

#[cfg(unix)]
fn make_directory_writable(path: &std::path::Path) {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .expect("make snapshot directory writable for adversarial replacement");
}

#[cfg(not(unix))]
fn make_directory_writable(_path: &std::path::Path) {}

#[cfg(unix)]
fn make_directory_read_only(path: &std::path::Path) {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o500))
        .expect("restore hardened snapshot directory permissions");
}

#[cfg(not(unix))]
fn make_directory_read_only(_path: &std::path::Path) {}
