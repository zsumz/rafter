//! Typed public errors for construction, admission, receive, and shutdown.

mod build;
mod lifecycle;
mod transport;

use std::error::Error;

/// Boxed caller or store failure preserved at a public boundary.
pub type BoxError = Box<dyn Error + Send + Sync + 'static>;

pub use build::TlsTransportBuildError;
pub use lifecycle::{TlsInboundError, TlsTransportJoinError, TlsTransportStartError};
pub use transport::TlsTransportError;
