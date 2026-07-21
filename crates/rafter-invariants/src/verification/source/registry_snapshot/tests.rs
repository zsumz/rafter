//! Adversarial registry archive and lockfile scenarios.

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use flate2::{write::GzEncoder, Compression};
use sha2::Digest as _;

use crate::execution::filesystem::{HeldDirectory, OperationDeadline};

use super::{cache, extract, lock, permissions};
use crate::verification::source::sealed::SealedTree;

#[test]
#[cfg(target_pointer_width = "64")]
fn sealed_tree_node_accounting_rejects_u64_overflow() {
    assert_eq!(
        crate::verification::source::sealed::checked_node_count(usize::MAX, 1),
        Err("sealed tree node count overflow".to_owned())
    );
}

#[path = "tests/archive_path_scenarios.rs"]
mod archive_path_scenarios;

#[test]
#[ignore = "full 247-package registry materialization is exercised by aggregate replay"]
fn current_registry_cache_materializes_and_revalidates() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let snapshot = super::RegistrySnapshot::materialize(&root, registry_policy())
        .expect("materialize current authenticated registry source");

    assert_eq!(snapshot.receipt().package_count, 247);
    assert!(snapshot.receipt().archive_bytes > 0);
    assert!(snapshot.receipt().expanded_bytes > snapshot.receipt().archive_bytes);
    assert!(snapshot.receipt().entries > 247);
    assert!(snapshot.vendor_root().is_dir());
    snapshot
        .revalidate()
        .expect("registry snapshot remains sealed");
}

#[test]
fn current_lock_has_only_unique_checksummed_crates_io_packages() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let locked = lock::parse(&root.join("Cargo.lock")).expect("parse authenticated lock fixture");

    assert_eq!(locked.packages.len(), 247);
    assert!(locked
        .packages
        .iter()
        .all(|package| package.source == "registry+https://github.com/rust-lang/crates.io-index"));
}

#[test]
fn regular_archive_materializes_a_cargo_directory_source() {
    let scratch = Scratch::new();
    let package = package();
    let archive = archive(&[
        Entry::file(
            "sample-1.2.3/Cargo.toml",
            b"[package]\nname = \"sample\"\nversion = \"1.2.3\"\n",
        ),
        Entry::file("sample-1.2.3/src/lib.rs", b"pub fn sample() {}\n"),
    ]);

    let extracted = extract::package(&scratch.root, &package, &archive, extraction_budget())
        .expect("extract authenticated crate archive");
    permissions::harden_directories(&scratch.path).expect("harden extracted package");
    let integrity = SealedTree::capture("registry extraction test", &scratch.path, extracted.plans)
        .expect("seal extracted package");

    let checksum = std::fs::read(
        scratch
            .path
            .join("vendor/sample-1.2.3/.cargo-checksum.json"),
    )
    .expect("read generated Cargo checksum");
    let checksum: serde_json::Value =
        serde_json::from_slice(&checksum).expect("parse generated Cargo checksum");
    assert_eq!(checksum["package"], package.checksum);
    assert_eq!(
        checksum["files"].as_object().map(serde_json::Map::len),
        Some(2)
    );
    integrity
        .revalidate()
        .expect("registry tree remains sealed");
}

#[test]
fn extraction_obeys_aggregate_resource_and_deadline_budgets() {
    let archive = archive(&[Entry::file(
        "sample-1.2.3/src/lib.rs",
        b"pub fn sample() {}\n",
    )]);
    let scratch = Scratch::new();
    let error = extract::package(
        &scratch.root,
        &package(),
        &archive,
        extract::ExtractionBudget {
            expanded_bytes: 1,
            entries: 32,
            deadline: Instant::now() + Duration::from_secs(30),
        },
    )
    .expect_err("aggregate byte budget must fail closed");
    assert!(error.contains("expanded size limit"), "{error}");

    let scratch = Scratch::new();
    let error = extract::package(
        &scratch.root,
        &package(),
        &archive,
        extract::ExtractionBudget {
            expanded_bytes: 1024,
            entries: 32,
            deadline: Instant::now(),
        },
    )
    .expect_err("expired extraction deadline must fail closed");
    assert!(error.contains("deadline expired"), "{error}");
}

#[test]
fn generated_checksum_is_charged_to_the_entry_budget() {
    let scratch = Scratch::new();
    let archive = regular_archive();
    let error = extract::package(
        &scratch.root,
        &package(),
        &archive,
        extract::ExtractionBudget {
            expanded_bytes: 1024,
            entries: 4,
            deadline: Instant::now() + Duration::from_secs(30),
        },
    )
    .expect_err("generated checksum must consume an entry");

    assert!(error.contains("entry limit"), "{error}");
    assert!(!scratch
        .path
        .join("vendor/sample-1.2.3/.cargo-checksum.json")
        .exists());
}

#[test]
fn implicit_parent_directories_are_charged_to_the_entry_budget() {
    let scratch = Scratch::new();
    let archive = archive(&[
        Entry::file(
            "sample-1.2.3/Cargo.toml",
            b"[package]\nname = \"sample\"\nversion = \"1.2.3\"\n",
        ),
        Entry::file("sample-1.2.3/deep/nested/lib.rs", b"pub fn sample() {}\n"),
    ]);
    let error = extract::package(
        &scratch.root,
        &package(),
        &archive,
        extract::ExtractionBudget {
            expanded_bytes: 1024,
            entries: 4,
            deadline: Instant::now() + Duration::from_secs(30),
        },
    )
    .expect_err("implicit parent directories must consume entries");

    assert!(error.contains("entry limit"), "{error}");
    assert!(!scratch
        .path
        .join("vendor/sample-1.2.3/deep/nested/lib.rs")
        .exists());
}

#[test]
fn registry_hardening_and_sealing_obey_node_and_deadline_budgets() {
    let scratch = Scratch::new();
    let extracted = extract::package(
        &scratch.root,
        &package(),
        &regular_archive(),
        extraction_budget(),
    )
    .expect("extract regular archive");
    let deadline = OperationDeadline::at(
        Instant::now() + Duration::from_secs(30),
        "registry sealing fixture",
    );
    let error = SealedTree::capture_bounded(
        "registry sealing fixture",
        &scratch.path,
        extracted.plans,
        deadline,
        1,
    )
    .expect_err("sealed inventory must reject a node budget below its plan");
    assert!(error.contains("node limit"), "{error}");

    let expired = permissions::harden_directories_bounded(
        &scratch.path,
        OperationDeadline::at(Instant::now(), "expired registry hardening fixture"),
        64,
    )
    .expect_err("expired hardening must fail before traversal");
    assert!(
        expired.to_string().contains("deadline expired"),
        "{expired}"
    );
}

#[test]
fn generated_checksum_is_charged_to_the_expanded_byte_budget() {
    let archive = regular_archive();
    let generous = Scratch::new();
    let extracted = extract::package(&generous.root, &package(), &archive, extraction_budget())
        .expect("measure complete extraction");
    let checksum_bytes = std::fs::metadata(
        generous
            .path
            .join("vendor/sample-1.2.3/.cargo-checksum.json"),
    )
    .expect("stat generated checksum")
    .len();

    let constrained = Scratch::new();
    let error = extract::package(
        &constrained.root,
        &package(),
        &archive,
        extract::ExtractionBudget {
            expanded_bytes: extracted.expanded_bytes - checksum_bytes,
            entries: 32,
            deadline: Instant::now() + Duration::from_secs(30),
        },
    )
    .expect_err("generated checksum must consume expanded bytes");

    assert!(error.contains("expanded size limit"), "{error}");
    assert!(!constrained
        .path
        .join("vendor/sample-1.2.3/.cargo-checksum.json")
        .exists());
}

#[test]
fn archive_links_are_rejected() {
    let scratch = Scratch::new();
    let archive = archive(&[
        Entry::file(
            "sample-1.2.3/Cargo.toml",
            b"[package]\nname = \"sample\"\nversion = \"1.2.3\"\n",
        ),
        Entry::symlink("sample-1.2.3/src/lib.rs", "../Cargo.toml"),
    ]);

    let error = extract::package(&scratch.root, &package(), &archive, extraction_budget())
        .expect_err("archive symlink must fail closed");

    assert!(error.contains("non-regular archive entry"), "{error}");
}

#[test]
fn archive_cannot_supply_its_own_checksum_policy() {
    let scratch = Scratch::new();
    let archive = archive(&[
        Entry::file(
            "sample-1.2.3/Cargo.toml",
            b"[package]\nname = \"sample\"\nversion = \"1.2.3\"\n",
        ),
        Entry::file("sample-1.2.3/.cargo-checksum.json", b"{}"),
    ]);

    let error = extract::package(&scratch.root, &package(), &archive, extraction_budget())
        .expect_err("archive checksum metadata must fail closed");

    assert!(error.contains("preexisting Cargo checksum"), "{error}");
}

#[test]
fn cache_requires_at_least_one_authenticated_archive_candidate() {
    let scratch = Scratch::new();
    let first = scratch.path.join("first.crate");
    let second = scratch.path.join("second.crate");
    std::fs::write(&first, b"first archive").expect("write first archive");
    std::fs::write(&second, b"second archive").expect("write second archive");

    let missing = cache::ArchiveInventory::fixture(BTreeMap::new());
    let error = missing
        .acquire(&package(), Instant::now() + Duration::from_secs(30))
        .expect_err("missing archive must fail closed");
    assert!(error.contains("0 cache candidates"), "{error}");

    let duplicate = cache::ArchiveInventory::fixture(BTreeMap::from([(
        package().archive_name(),
        vec![first, second],
    )]));
    let error = duplicate
        .acquire(&package(), Instant::now() + Duration::from_secs(30))
        .expect_err("duplicate archive must fail closed");
    assert!(
        error.contains("does not match its authenticated lock checksum"),
        "{error}"
    );
}

#[test]
fn cache_accepts_byte_identical_authenticated_archive_copies() {
    let scratch = Scratch::new();
    let bytes = regular_archive();
    let first = scratch.path.join("first.crate");
    let second = scratch.path.join("second.crate");
    std::fs::write(&first, &bytes).expect("write first archive");
    std::fs::write(&second, &bytes).expect("write second archive");
    let mut package = package();
    package.checksum = format!("{:x}", sha2::Sha256::digest(&bytes));
    let inventory = cache::ArchiveInventory::fixture(BTreeMap::from([(
        package.archive_name(),
        vec![second, first],
    )]));

    let authenticated = inventory
        .acquire(&package, Instant::now() + Duration::from_secs(30))
        .expect("identical authenticated cache copies are one semantic archive");
    assert_eq!(authenticated.bytes, bytes);
}

#[test]
fn cache_rejects_conflicting_archive_copies_even_if_one_matches_the_lock() {
    let scratch = Scratch::new();
    let bytes = regular_archive();
    let first = scratch.path.join("first.crate");
    let second = scratch.path.join("second.crate");
    std::fs::write(&first, &bytes).expect("write authenticated archive");
    std::fs::write(&second, b"conflicting archive").expect("write conflicting archive");
    let mut package = package();
    package.checksum = format!("{:x}", sha2::Sha256::digest(&bytes));
    let inventory = cache::ArchiveInventory::fixture(BTreeMap::from([(
        package.archive_name(),
        vec![first, second],
    )]));

    assert!(inventory
        .acquire(&package, Instant::now() + Duration::from_secs(30))
        .is_err());
}

#[test]
fn cache_rejects_archive_bytes_not_authenticated_by_the_lock() {
    let scratch = Scratch::new();
    let path = scratch.path.join("sample-1.2.3.crate");
    std::fs::write(&path, b"altered archive bytes").expect("write altered archive");
    let inventory =
        cache::ArchiveInventory::fixture(BTreeMap::from([(package().archive_name(), vec![path])]));

    let error = inventory
        .acquire(&package(), Instant::now() + Duration::from_secs(30))
        .expect_err("altered archive must fail closed");

    assert!(error.contains("authenticated lock checksum"), "{error}");
}

#[cfg(unix)]
#[test]
fn cache_rejects_symlink_archive_candidate() {
    use std::os::unix::fs::symlink;

    let scratch = Scratch::new();
    let target = scratch.path.join("target.crate");
    let link = scratch.path.join("sample-1.2.3.crate");
    std::fs::write(&target, b"archive target").expect("write archive target");
    symlink(&target, &link).expect("create archive symlink");
    let inventory =
        cache::ArchiveInventory::fixture(BTreeMap::from([(package().archive_name(), vec![link])]));

    let error = inventory
        .acquire(&package(), Instant::now() + Duration::from_secs(30))
        .expect_err("symlink archive must fail closed");

    assert!(error.contains("without following links"), "{error}");
}

#[cfg(unix)]
#[test]
fn cache_discovery_rejects_symlink_registry_namespace() {
    use std::os::unix::fs::symlink;

    let scratch = Scratch::new();
    let cache = scratch.path.join("cargo/registry/cache");
    let target = scratch.path.join("registry-target");
    std::fs::create_dir_all(&cache).expect("create cache root");
    std::fs::create_dir_all(&target).expect("create namespace target");
    symlink(&target, cache.join("symlinked-namespace")).expect("create namespace symlink");

    let error = cache::ArchiveInventory::discover_fixture(&scratch.path.join("cargo"))
        .expect_err("symlink namespace must fail closed");

    assert!(error.contains("is not a direct directory"), "{error}");
}

#[test]
fn cache_discovery_obeys_its_entry_limit_and_absolute_deadline() {
    let scratch = Scratch::new();
    let cache = scratch.path.join("cargo/registry/cache/crates-io");
    std::fs::create_dir_all(&cache).expect("create cache namespace");
    std::fs::write(cache.join("first.crate"), b"first").expect("write first archive");
    std::fs::write(cache.join("second.crate"), b"second").expect("write second archive");
    let home = scratch.path.join("cargo");

    let overflow = cache::ArchiveInventory::discover_fixture_bounded(
        &home,
        Instant::now() + Duration::from_secs(30),
        2,
    )
    .expect_err("namespace and archives must share the discovery entry budget");
    assert!(overflow.contains("entry limit"), "{overflow}");

    let expired = cache::ArchiveInventory::discover_fixture_bounded(&home, Instant::now(), 16)
        .expect_err("expired discovery must fail before traversal");
    assert!(expired.contains("deadline expired"), "{expired}");
}

#[cfg(unix)]
#[test]
fn sealed_registry_vendor_rejects_same_byte_file_replacement() {
    use std::os::unix::fs::PermissionsExt;

    let scratch = Scratch::new();
    let (integrity, source) = sealed_sample(&scratch);
    let _retained_identity = std::fs::File::open(&source).expect("retain sealed source identity");
    let parent = source.parent().expect("source parent");
    std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700))
        .expect("make source directory writable");
    std::fs::remove_file(&source).expect("remove sealed source");
    std::fs::write(&source, b"pub fn sample() {}\n").expect("replace with identical bytes");
    std::fs::set_permissions(&source, std::fs::Permissions::from_mode(0o400))
        .expect("restore source mode");
    std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o500))
        .expect("restore source directory mode");

    let error = integrity
        .revalidate()
        .expect_err("same-byte replacement must change file identity");

    assert!(error.contains("file identity changed"), "{error}");
}

#[cfg(unix)]
#[test]
fn sealed_registry_vendor_rejects_permission_mutation() {
    use std::os::unix::fs::PermissionsExt;

    let scratch = Scratch::new();
    let (integrity, source) = sealed_sample(&scratch);
    std::fs::set_permissions(&source, std::fs::Permissions::from_mode(0o600))
        .expect("make sealed source writable");

    let error = integrity
        .revalidate()
        .expect_err("permission mutation must fail closed");

    assert!(error.contains("permissions changed"), "{error}");
}

fn sealed_sample(scratch: &Scratch) -> (SealedTree, PathBuf) {
    let package = package();
    let archive = archive(&[
        Entry::file(
            "sample-1.2.3/Cargo.toml",
            b"[package]\nname = \"sample\"\nversion = \"1.2.3\"\n",
        ),
        Entry::file("sample-1.2.3/src/lib.rs", b"pub fn sample() {}\n"),
    ]);
    let extracted = extract::package(&scratch.root, &package, &archive, extraction_budget())
        .expect("extract authenticated crate archive");
    permissions::harden_directories(&scratch.path).expect("harden extracted package");
    let integrity = SealedTree::capture("registry sealing test", &scratch.path, extracted.plans)
        .expect("seal extracted package");
    let source = scratch.path.join("vendor/sample-1.2.3/src/lib.rs");
    (integrity, source)
}

fn package() -> lock::LockedPackage {
    lock::LockedPackage {
        name: "sample".to_owned(),
        version: "1.2.3".to_owned(),
        source: "registry+https://github.com/rust-lang/crates.io-index".to_owned(),
        checksum: "a".repeat(64),
    }
}

fn regular_archive() -> Vec<u8> {
    archive(&[
        Entry::file(
            "sample-1.2.3/Cargo.toml",
            b"[package]\nname = \"sample\"\nversion = \"1.2.3\"\n",
        ),
        Entry::file("sample-1.2.3/src/lib.rs", b"pub fn sample() {}\n"),
    ])
}

fn extraction_budget() -> extract::ExtractionBudget {
    extract::ExtractionBudget {
        expanded_bytes: 256 * 1024 * 1024,
        entries: 32 * 1024,
        deadline: Instant::now() + Duration::from_secs(30),
    }
}

fn registry_policy() -> crate::verification::source::RegistryMaterializationPolicy {
    crate::verification::source::RegistryMaterializationPolicy {
        required_packages: 247,
        maximum_archive_bytes: 268_435_456,
        maximum_expanded_bytes: 2_147_483_648,
        maximum_entries: 250_000,
        deadline: Instant::now() + Duration::from_secs(300),
    }
}

struct Scratch {
    _parent: HeldDirectory,
    _directory: tempfile::TempDir,
    root: HeldDirectory,
    path: PathBuf,
}

impl Scratch {
    fn new() -> Self {
        let parent = HeldDirectory::create_all(Path::new(
            "target/rafter-invariants/registry-extraction-tests",
        ))
        .expect("create registry test parent");
        let directory = tempfile::Builder::new()
            .prefix("registry-")
            .tempdir_in(parent.external_path())
            .expect("create registry test root");
        let path = std::fs::canonicalize(directory.path()).expect("canonical registry test root");
        let root = HeldDirectory::open(&path).expect("hold registry test root");
        Self {
            _parent: parent,
            _directory: directory,
            root,
            path,
        }
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        permissions::restore_tree(&self.path);
    }
}

enum Entry<'a> {
    File { path: &'a str, bytes: &'a [u8] },
    Symlink { path: &'a str, target: &'a str },
}

impl<'a> Entry<'a> {
    fn file(path: &'a str, bytes: &'a [u8]) -> Self {
        Self::File { path, bytes }
    }

    fn symlink(path: &'a str, target: &'a str) -> Self {
        Self::Symlink { path, target }
    }
}

fn archive(entries: &[Entry<'_>]) -> Vec<u8> {
    let encoder = GzEncoder::new(Vec::new(), Compression::default());
    let mut archive = tar::Builder::new(encoder);
    for entry in entries {
        let mut header = tar::Header::new_gnu();
        header.set_mode(0o644);
        header.set_mtime(0);
        match entry {
            Entry::File { path, bytes } => {
                header.set_entry_type(tar::EntryType::Regular);
                header.set_size(bytes.len() as u64);
                header.set_cksum();
                archive
                    .append_data(&mut header, path, *bytes)
                    .expect("append regular tar fixture");
            }
            Entry::Symlink { path, target } => {
                header.set_entry_type(tar::EntryType::Symlink);
                header.set_size(0);
                header.set_link_name(target).expect("set tar link target");
                header.set_cksum();
                archive
                    .append_data(&mut header, path, std::io::empty())
                    .expect("append symlink tar fixture");
            }
        }
    }
    let encoder = archive.into_inner().expect("finish tar fixture");
    encoder.finish().expect("finish gzip fixture")
}
