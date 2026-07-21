//! Cloneable access to the original filesystem error behind storage failures.
//!
//! Operational storage errors are cloned into runtime poison state and compared
//! in deterministic tests. `std::io::Error` is neither `Clone` nor `Eq`, so this
//! wrapper keeps the original error behind an `Arc` while defining equality over
//! its stable diagnostic projection.

use std::{error::Error, fmt, io, sync::Arc};

/// Original filesystem error retained by a storage operation.
///
/// Clones share the same [`io::Error`] allocation. Equality compares the error
/// kind, raw operating-system code, and rendered message so storage and runtime
/// error enums can remain cloneable and equatable without discarding the source.
#[derive(Clone)]
pub struct StorageIoError {
    source: Arc<io::Error>,
}

impl StorageIoError {
    /// Retains `source` for later inspection through [`Error::source`].
    #[must_use]
    pub fn new(source: io::Error) -> Self {
        Self {
            source: Arc::new(source),
        }
    }

    /// Returns the underlying I/O error.
    #[must_use]
    pub fn as_io_error(&self) -> &io::Error {
        self.source.as_ref()
    }

    /// Returns the portable I/O error category.
    #[must_use]
    pub fn kind(&self) -> io::ErrorKind {
        self.source.kind()
    }

    /// Returns the operating-system error code, when one exists.
    #[must_use]
    pub fn raw_os_error(&self) -> Option<i32> {
        self.source.raw_os_error()
    }
}

impl From<io::Error> for StorageIoError {
    fn from(source: io::Error) -> Self {
        Self::new(source)
    }
}

impl fmt::Display for StorageIoError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.source.fmt(formatter)
    }
}

impl fmt::Debug for StorageIoError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageIoError")
            .field("kind", &self.kind())
            .field("raw_os_error", &self.raw_os_error())
            .field("message", &self.to_string())
            .finish()
    }
}

impl PartialEq for StorageIoError {
    fn eq(&self, other: &Self) -> bool {
        self.kind() == other.kind()
            && self.raw_os_error() == other.raw_os_error()
            && self.to_string() == other.to_string()
    }
}

impl Eq for StorageIoError {}

impl Error for StorageIoError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.as_io_error())
    }
}

#[cfg(test)]
#[path = "io_error_test.rs"]
mod tests;
