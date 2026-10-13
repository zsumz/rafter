//! Real directory topology, synchronization failures, and safe retry ordering.

use std::{
    fs, io,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use super::{create_durable_directory, provision};

static NEXT: AtomicU64 = AtomicU64::new(0);

struct Scratch(PathBuf);

impl Scratch {
    fn new(parent: &Path) -> Self {
        let directory = parent.join(format!(
            "rafter-directory-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&directory).expect("exclusive scratch directory");
        Self(directory)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        if std::thread::panicking() {
            eprintln!("retained directory test inputs: {}", self.0.display());
        } else {
            fs::remove_dir_all(&self.0).expect("owned scratch directory removes");
        }
    }
}

fn sync(parent: &Path) -> io::Result<()> {
    fs::File::open(parent)?.sync_all()
}

#[test]
fn each_new_parent_entry_is_synced_before_creating_children() {
    let scratch = Scratch::new(&std::env::temp_dir());
    let first = scratch.0.join("first");
    let second = first.join("second");
    let final_dir = second.join("database");
    let mut visited = Vec::new();
    provision(&final_dir, |parent| {
        if parent == scratch.0 {
            assert!(first.is_dir());
            assert!(
                !second.exists(),
                "a child cannot precede its parent's durability"
            );
        } else if parent == first {
            assert!(second.is_dir());
            assert!(!final_dir.exists());
        }
        visited.push(parent.to_path_buf());
        sync(parent)
    })
    .expect("nested directories become durable");
    assert_eq!(
        visited,
        [scratch.0.parent().unwrap(), &scratch.0, &first, &second]
    );
    assert!(final_dir.is_dir());
}

#[test]
fn failed_parent_sync_stops_creation_and_retry_resynchronizes_the_residue() {
    let scratch = Scratch::new(&std::env::temp_dir());
    let first = scratch.0.join("first");
    let second = first.join("second");
    let final_dir = second.join("database");
    let error = provision(&final_dir, |parent| {
        if parent == scratch.0 {
            assert!(first.is_dir());
            return Err(io::Error::other("injected parent synchronization failure"));
        }
        sync(parent)
    })
    .expect_err("failed synchronization must refuse successful provisioning");
    assert_eq!(error.kind(), io::ErrorKind::Other);
    assert!(first.is_dir());
    assert!(
        !second.exists(),
        "failure must leave at most one unsynchronized entry"
    );
    let mut retried = Vec::new();
    provision(&final_dir, |parent| {
        if retried.is_empty() {
            assert_eq!(parent, scratch.0);
            assert!(
                !second.exists(),
                "residue must be synchronized before continuing"
            );
        }
        retried.push(parent.to_path_buf());
        sync(parent)
    })
    .expect("retry provisions the remaining hierarchy");
    assert_eq!(retried, [scratch.0.as_path(), &first, &second]);
    assert!(final_dir.is_dir());
}

#[test]
fn existing_target_still_requires_its_parent_sync_and_preserves_data() {
    let scratch = Scratch::new(&std::env::temp_dir());
    let directory = scratch.0.join("database");
    fs::create_dir(&directory).unwrap();
    fs::write(directory.join("evidence"), b"keep this state").unwrap();
    let mut calls = 0;
    let error = provision(&directory, |parent| {
        calls += 1;
        assert_eq!(parent, scratch.0);
        Err(io::Error::other("injected synchronization failure"))
    })
    .expect_err("existing does not mean synchronized");
    assert_eq!(error.kind(), io::ErrorKind::Other);
    assert_eq!(calls, 1);
    create_durable_directory(&directory).expect("existing target may be retried");
    assert_eq!(
        fs::read(directory.join("evidence")).unwrap(),
        b"keep this state"
    );
}

#[test]
fn a_regular_file_component_is_refused_without_modification() {
    let scratch = Scratch::new(&std::env::temp_dir());
    let file = scratch.0.join("database");
    fs::write(&file, b"not a directory").unwrap();
    for target in [&file, &file.join("child")] {
        let error = provision(target, |_| {
            panic!("invalid hierarchy cannot reach synchronization")
        })
        .expect_err("file component refuses provisioning");
        assert_eq!(error.kind(), io::ErrorKind::NotADirectory);
        assert_eq!(fs::read(&file).unwrap(), b"not a directory");
    }
}

#[test]
fn a_directory_created_concurrently_is_synchronized_before_use() {
    let scratch = Scratch::new(&std::env::temp_dir());
    let directory = scratch.0.join("database");
    let mut calls = Vec::new();
    provision(&directory, |parent| {
        if calls.is_empty() {
            fs::create_dir(&directory).expect("another creator wins the mkdir race");
        }
        calls.push(parent.to_path_buf());
        sync(parent)
    })
    .expect("existing directory from another creator is accepted and synchronized");
    assert_eq!(calls, [scratch.0.parent().unwrap(), &scratch.0]);
}

#[test]
fn relative_paths_sync_the_same_parents_as_absolute_paths() {
    let working = std::env::current_dir().unwrap();
    let scratch = Scratch::new(&working);
    let relative = Path::new(scratch.0.file_name().unwrap()).join("database");
    let mut calls = Vec::new();
    provision(&relative, |parent| {
        assert!(parent.is_absolute());
        calls.push(parent.to_path_buf());
        sync(parent)
    })
    .expect("relative directory provisions");
    assert_eq!(calls, [working, scratch.0.clone()]);
    assert!(scratch.0.join("database").is_dir());
}
