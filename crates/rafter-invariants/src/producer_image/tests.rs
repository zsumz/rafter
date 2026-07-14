use std::{
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    thread,
    time::{Duration, Instant},
};

use super::{publish_content_addressed, sha256, verify_managed_image};

fn scratch(label: &str) -> std::path::PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let path = std::env::temp_dir().join(format!(
        "rafter-producer-image-{label}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&path).expect("create producer-image scratch directory");
    path
}

#[test]
fn poisoned_content_addressed_image_is_rejected_without_overwrite() {
    let root = scratch("poisoned");
    let path = root.join("image");
    std::fs::write(&path, b"poisoned").expect("write poisoned image");

    let error = publish_content_addressed(&path, b"expected", false)
        .expect_err("poisoned image must fail closed");
    assert!(error.to_string().contains("conflicting content"));
    assert_eq!(std::fs::read(&path).expect("read poison"), b"poisoned");
    std::fs::remove_dir_all(root).expect("remove scratch directory");
}

#[test]
fn concurrent_publishers_never_expose_partial_content() {
    let root = scratch("concurrent");
    let path = Arc::new(root.join("image"));
    let bytes = Arc::<[u8]>::from(&b"complete producer image"[..]);
    let done = Arc::new(AtomicBool::new(false));
    let observed = Arc::new(AtomicBool::new(false));
    let reader_path = Arc::clone(&path);
    let reader_bytes = Arc::clone(&bytes);
    let reader_done = Arc::clone(&done);
    let reader_observed = Arc::clone(&observed);
    let reader = thread::spawn(move || {
        while !reader_done.load(Ordering::Acquire) {
            match std::fs::read(&*reader_path) {
                Ok(actual) => {
                    assert_eq!(actual, &*reader_bytes);
                    reader_observed.store(true, Ordering::Release);
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => panic!("read concurrent publication: {error}"),
            }
            thread::yield_now();
        }
    });
    let mut workers = Vec::new();
    for _ in 0..8 {
        let path = Arc::clone(&path);
        let bytes = Arc::clone(&bytes);
        workers.push(thread::spawn(move || {
            publish_content_addressed(&path, &bytes, false)
                .expect("concurrent publication succeeds")
        }));
    }
    for worker in workers {
        assert_eq!(worker.join().expect("publisher joins"), &*bytes);
    }
    let deadline = Instant::now() + Duration::from_secs(1);
    while !observed.load(Ordering::Acquire) && Instant::now() < deadline {
        thread::yield_now();
    }
    assert!(observed.load(Ordering::Acquire));
    done.store(true, Ordering::Release);
    reader.join().expect("concurrent reader joins");
    assert_eq!(
        std::fs::read(&*path).expect("read published image"),
        &*bytes
    );
    std::fs::remove_dir_all(root).expect("remove scratch directory");
}

#[cfg(unix)]
#[test]
fn symlink_destination_is_rejected_and_writable_file_is_normalized() {
    use std::os::unix::fs::{symlink, PermissionsExt};

    let root = scratch("file-type");
    let target = root.join("target");
    let link = root.join("link");
    std::fs::write(&target, b"expected").expect("write symlink target");
    symlink(&target, &link).expect("create destination symlink");
    assert!(publish_content_addressed(&link, b"expected", false).is_err());

    let writable = root.join("writable");
    std::fs::write(&writable, b"expected").expect("write existing content");
    std::fs::set_permissions(&writable, std::fs::Permissions::from_mode(0o600))
        .expect("make existing content writable");
    publish_content_addressed(&writable, b"expected", false)
        .expect("matching existing content is normalized");
    assert_eq!(
        std::fs::symlink_metadata(&writable)
            .expect("existing content metadata")
            .permissions()
            .mode()
            & 0o777,
        0o400
    );
    std::fs::remove_dir_all(root).expect("remove scratch directory");
}

#[test]
fn managed_image_rejects_forged_digest_and_wrong_path() {
    let root = scratch("binding");
    let bytes = b"managed image";
    let digest = sha256(bytes);
    let expected = root
        .join("target/rafter-invariants/producer-images")
        .join(&digest)
        .join("rafter-invariants");
    publish_content_addressed(&expected, bytes, true).expect("publish managed image");
    verify_managed_image(&root, &expected, bytes, &digest).expect("exact binding verifies");
    assert!(verify_managed_image(&root, &expected, bytes, &"f".repeat(64)).is_err());
    let wrong = root.join("target/debug/rafter-invariants");
    std::fs::create_dir_all(wrong.parent().expect("wrong parent")).expect("create wrong parent");
    std::fs::write(&wrong, bytes).expect("write wrong image path");
    assert!(verify_managed_image(&root, &wrong, bytes, &digest).is_err());
    std::fs::remove_dir_all(root).expect("remove scratch directory");
}
