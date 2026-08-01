//! Rustls connection-construction failures.

use std::{error::Error, fmt};

use crate::TlsServerName;

/// Failure while creating one rustls connection object.
#[derive(Debug)]
#[non_exhaustive]
pub enum TlsConnectionError {
    /// A canonical transport name could not be represented by rustls.
    InvalidServerName {
        /// Canonical rejected name.
        name: TlsServerName,
    },
    /// Rustls refused the connection configuration.
    Rustls(rustls::Error),
}

impl fmt::Display for TlsConnectionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidServerName { name } => {
                write!(formatter, "TLS server name {name} is not representable")
            }
            Self::Rustls(source) => write!(formatter, "could not create TLS connection: {source}"),
        }
    }
}

impl Error for TlsConnectionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Rustls(source) => Some(source),
            Self::InvalidServerName { .. } => None,
        }
    }
}
