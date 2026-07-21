//! Clone, equality, and source-retention scenarios for storage I/O errors.

use std::{error::Error as _, io};

use super::StorageIoError;

#[test]
fn clones_share_the_original_io_error() {
    let error = StorageIoError::new(io::Error::new(
        io::ErrorKind::PermissionDenied,
        "injected permission failure",
    ));
    let clone = error.clone();

    assert_eq!(clone, error);
    assert_eq!(clone.kind(), io::ErrorKind::PermissionDenied);
    assert_eq!(clone.to_string(), "injected permission failure");
    assert!(std::ptr::eq(clone.as_io_error(), error.as_io_error()));
}

#[test]
fn wrapper_preserves_a_nested_io_error_source() {
    #[derive(Debug)]
    struct Cause;

    impl std::fmt::Display for Cause {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("nested cause")
        }
    }

    impl std::error::Error for Cause {}

    let error = StorageIoError::new(io::Error::other(Cause));

    let io_source = error
        .source()
        .expect("the original I/O error is exposed")
        .downcast_ref::<io::Error>()
        .expect("source is std::io::Error");
    assert!(std::ptr::eq(io_source, error.as_io_error()));
    assert_eq!(
        io_source
            .get_ref()
            .expect("nested source is retained")
            .to_string(),
        "nested cause"
    );
}
