//! Scenarios for confinement, durability, traversal bounds, and consumer deadlines.

use std::{
    fs,
    io::Write,
    path::PathBuf,
    time::{Duration, Instant},
};

use super::{HeldDirectory, OperationDeadline, TreeLimits, TREE_LIMITS};

fn test_path(label: &str) -> PathBuf {
    PathBuf::from("target/rafter-invariants/filesystem-tests")
        .join(format!("{label}-{}", std::process::id()))
}

fn limits_with(nodes: usize, depth: usize) -> TreeLimits {
    TREE_LIMITS.with_nodes(nodes).with_depth(depth)
}

#[test]
fn remove_file_if_exists_accepts_a_missing_parent_without_creating_it() {
    let root = test_path("remove-missing-parent");
    let _ = fs::remove_dir_all(&root);

    HeldDirectory::workspace()
        .expect("open workspace")
        .remove_file_if_exists(&root.join("missing/result.json"))
        .expect("a file below a missing parent is already absent");

    assert!(!root.exists(), "absence check must not create parents");
}

#[test]
fn nested_atomic_publication_syncs_the_real_directory_descriptor() {
    let root = test_path("atomic-directory-sync");
    let artifact = root.join("nested/result.json");
    let _ = fs::remove_dir_all(&root);

    HeldDirectory::workspace()
        .expect("open workspace")
        .write_atomic(&artifact, br#"{"green":44}"#)
        .expect("publish and sync nested artifact");

    assert_eq!(
        fs::read(&artifact).expect("read published artifact"),
        br#"{"green":44}"#
    );
    fs::remove_dir_all(root).expect("remove atomic publication fixture");
}

#[cfg(target_os = "linux")]
#[test]
fn unsupported_directory_fsync_uses_and_propagates_filesystem_sync() {
    use std::cell::Cell;

    let called = Cell::new(false);
    super::complete_directory_sync(Err(rustix::io::Errno::BADF), || {
        called.set(true);
        Ok(())
    })
    .expect("filesystem-wide sync substitutes for unsupported directory fsync");
    assert!(called.get());

    let error = super::complete_directory_sync(
        Err(rustix::io::Errno::BADF),
        || -> Result<(), Box<dyn std::error::Error>> { Err(Box::new(rustix::io::Errno::IO)) },
    )
    .expect_err("filesystem sync failure remains fatal");
    assert_eq!(error.to_string(), rustix::io::Errno::IO.to_string());

    let called = Cell::new(false);
    let error = super::complete_directory_sync(Err(rustix::io::Errno::PERM), || {
        called.set(true);
        Ok(())
    })
    .expect_err("unrelated directory fsync failures remain fatal");
    assert_eq!(error.to_string(), rustix::io::Errno::PERM.to_string());
    assert!(!called.get());
}

#[cfg(target_os = "linux")]
#[test]
fn unsupported_filesystem_sync_uses_global_sync_without_masking_other_errors() {
    use std::cell::Cell;

    let called = Cell::new(false);
    super::complete_filesystem_sync(Err(rustix::io::Errno::BADF), || {
        called.set(true);
    })
    .expect("global sync substitutes for unsupported filesystem sync");
    assert!(called.get());

    let called = Cell::new(false);
    let error = super::complete_filesystem_sync(Err(rustix::io::Errno::PERM), || {
        called.set(true);
    })
    .expect_err("unrelated filesystem sync failures remain fatal");
    assert_eq!(error.to_string(), rustix::io::Errno::PERM.to_string());
    assert!(!called.get());
}

#[test]
fn bounded_cleanup_rejects_node_overflow_before_removing_anything() {
    let root = test_path("node-limit");
    let _ = fs::remove_dir_all(&root);
    fs::create_dir_all(&root).expect("create node-limit fixture");
    for name in ["one", "two", "three"] {
        fs::write(root.join(name), name).expect("write node-limit fixture");
    }

    let held = HeldDirectory::open(&root).expect("hold node-limit fixture");
    let error = held
        .remove_contents(
            limits_with(2, TREE_LIMITS.depth()),
            OperationDeadline::none("node-limit test"),
        )
        .expect_err("cleanup must reject an oversized tree");
    assert!(error.to_string().contains("node limit of 2"));
    for name in ["one", "two", "three"] {
        assert!(root.join(name).is_file());
    }

    fs::remove_dir_all(root).expect("remove node-limit fixture");
}

#[test]
fn bounded_cleanup_rejects_deep_trees_before_removing_anything() {
    let root = test_path("depth-limit");
    let leaf = root.join("one/two/three");
    let _ = fs::remove_dir_all(&root);
    fs::create_dir_all(&leaf).expect("create depth-limit fixture");
    fs::write(leaf.join("sentinel"), b"inside").expect("write depth-limit fixture");

    let held = HeldDirectory::open(&root).expect("hold depth-limit fixture");
    let error = held
        .remove_contents(
            limits_with(TREE_LIMITS.nodes(), 2),
            OperationDeadline::none("depth-limit test"),
        )
        .expect_err("cleanup must reject a deep tree");
    assert!(error.to_string().contains("depth limit of 2"));
    assert!(leaf.join("sentinel").is_file());

    fs::remove_dir_all(root).expect("remove depth-limit fixture");
}

#[cfg(unix)]
#[test]
fn cleanup_unlinks_symlinks_without_touching_external_sentinels() {
    use std::os::unix::fs::symlink;

    let root = test_path("external-sentinel");
    let external = test_path("external-target");
    let _ = fs::remove_dir_all(&root);
    let _ = fs::remove_dir_all(&external);
    fs::create_dir_all(&root).expect("create cleanup fixture");
    fs::create_dir_all(&external).expect("create external target");
    fs::write(external.join("sentinel"), b"outside").expect("write external sentinel");
    symlink(&external, root.join("linked-outside")).expect("link cleanup fixture outside");

    let replacement = HeldDirectory::replace_tree(
        &root,
        TREE_LIMITS,
        OperationDeadline::none("external-sentinel cleanup"),
    )
    .expect("replace scratch tree without following symlink");
    assert!(replacement
        .entries(OperationDeadline::none("inspect replacement"))
        .expect("read replacement")
        .is_empty());
    assert_eq!(
        fs::read(external.join("sentinel")).expect("read external sentinel"),
        b"outside"
    );

    fs::remove_dir_all(root).expect("remove replacement fixture");
    fs::remove_dir_all(external).expect("remove external fixture");
}

#[cfg(unix)]
#[test]
fn create_rejects_symlinked_ancestors() {
    use std::os::unix::fs::symlink;

    let root = test_path("ancestor-symlink");
    let external = test_path("ancestor-external");
    let _ = fs::remove_dir_all(&root);
    let _ = fs::remove_dir_all(&external);
    fs::create_dir_all(&root).expect("create ancestor fixture");
    fs::create_dir_all(&external).expect("create ancestor external target");
    fs::write(external.join("sentinel"), b"outside").expect("write ancestor sentinel");
    symlink(&external, root.join("linked-parent")).expect("create ancestor symlink");

    assert!(HeldDirectory::create_all(&root.join("linked-parent/created")).is_err());
    assert!(!external.join("created").exists());
    assert_eq!(
        fs::read(external.join("sentinel")).expect("read ancestor sentinel"),
        b"outside"
    );

    fs::remove_dir_all(root).expect("remove ancestor fixture");
    fs::remove_dir_all(external).expect("remove ancestor external target");
}

#[cfg(unix)]
#[test]
fn leaf_swap_during_cleanup_is_detected_without_following_replacement() {
    use std::os::unix::fs::symlink;

    let root = test_path("leaf-swap");
    let moved = test_path("leaf-swap-moved");
    let external = test_path("leaf-swap-external");
    let _ = fs::remove_file(&root);
    let _ = fs::remove_dir_all(&root);
    let _ = fs::remove_dir_all(&moved);
    let _ = fs::remove_dir_all(&external);
    fs::create_dir_all(&root).expect("create leaf-swap fixture");
    fs::write(root.join("inside"), b"scratch").expect("write leaf-swap fixture");
    fs::create_dir_all(&external).expect("create leaf-swap external target");
    fs::write(external.join("sentinel"), b"outside").expect("write leaf-swap sentinel");

    let error = HeldDirectory::replace_tree_with_hook(
        &root,
        TREE_LIMITS,
        OperationDeadline::none("leaf-swap cleanup"),
        || {
            fs::rename(&root, &moved).expect("move held scratch leaf");
            symlink(&external, &root).expect("replace scratch leaf with symlink");
        },
    )
    .expect_err("leaf replacement must invalidate cleanup publication");
    assert!(!error.to_string().is_empty());
    assert_eq!(
        fs::read(external.join("sentinel")).expect("read leaf-swap sentinel"),
        b"outside"
    );

    fs::remove_file(root).expect("remove replacement symlink");
    fs::remove_dir_all(moved).expect("remove moved fixture");
    fs::remove_dir_all(external).expect("remove leaf-swap external target");
}

#[cfg(unix)]
#[test]
fn held_file_detects_leaf_replacement_before_external_launch() {
    use std::os::unix::fs::symlink;

    let root = test_path("held-file-swap");
    let external = test_path("held-file-external");
    let file = root.join("state.chkpt");
    let moved = root.join("state.original");
    let _ = fs::remove_dir_all(&root);
    let _ = fs::remove_dir_all(&external);
    fs::create_dir_all(&root).expect("create held-file fixture");
    fs::write(&file, b"checkpoint").expect("write held checkpoint");
    fs::create_dir_all(&external).expect("create held-file external target");
    fs::write(external.join("sentinel"), b"outside").expect("write held-file sentinel");

    let held = HeldDirectory::workspace()
        .expect("open workspace")
        .hold_file(&file)
        .expect("hold checkpoint file");
    fs::rename(&file, &moved).expect("move held checkpoint");
    symlink(external.join("sentinel"), &file).expect("replace checkpoint with symlink");

    assert!(held.verify_path_binding().is_err());
    assert_eq!(
        fs::read(&moved).expect("read original checkpoint"),
        b"checkpoint"
    );
    assert_eq!(
        fs::read(external.join("sentinel")).expect("read held-file sentinel"),
        b"outside"
    );

    fs::remove_dir_all(root).expect("remove held-file fixture");
    fs::remove_dir_all(external).expect("remove held-file external target");
}

#[cfg(unix)]
#[test]
fn held_file_reads_do_not_reopen_a_replacement_fifo() {
    let root = test_path("held-file-fifo");
    let file = root.join("receipt");
    let moved = root.join("receipt.original");
    let _ = fs::remove_dir_all(&root);
    fs::create_dir_all(&root).expect("create held-file FIFO fixture");

    let held = HeldDirectory::workspace()
        .expect("open workspace")
        .create_new_held_file(&file)
        .expect("create held receipt");
    held.try_clone_std()
        .expect("clone held receipt")
        .write_all(b"immutable receipt")
        .expect("write held receipt");
    fs::rename(&file, &moved).expect("move held receipt");
    let status = std::process::Command::new("/usr/bin/mkfifo")
        .arg(&file)
        .status()
        .expect("create replacement FIFO");
    assert!(status.success());

    let started = Instant::now();
    let bytes = held
        .read_bounded(
            OperationDeadline::at(Instant::now() + Duration::from_secs(1), "held receipt read"),
            1024,
        )
        .expect("read through the original capability");
    assert_eq!(bytes, b"immutable receipt");
    assert!(started.elapsed() < Duration::from_millis(250));
    assert!(held.verify_path_binding().is_err());

    fs::remove_file(file).expect("remove replacement FIFO");
    fs::remove_dir_all(root).expect("remove held-file FIFO fixture");
}

#[cfg(unix)]
#[test]
fn child_directory_capabilities_are_close_on_exec_until_explicitly_mapped() {
    let root = test_path("child-directory-cloexec");
    let _ = fs::remove_dir_all(&root);
    fs::create_dir_all(&root).expect("create child-directory fixture");
    let held = HeldDirectory::open(&root).expect("hold child-directory fixture");
    let child = held.bind_for_child().expect("bind child-directory fixture");

    let flags = rustix::io::fcntl_getfd(child.descriptor()).expect("inspect descriptor flags");
    assert!(flags.contains(rustix::io::FdFlags::CLOEXEC));

    drop(child);
    fs::remove_dir_all(root).expect("remove child-directory fixture");
}
