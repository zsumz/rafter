//! Caller-managed resolved endpoints and canonical TLS server names.

mod book;
mod error;
mod name;

pub use book::{EndpointBook, EndpointGeneration, EndpointSnapshot, PeerEndpoint};
pub use error::EndpointBookError;
pub use name::{TlsServerName, TlsServerNameError, MAX_TLS_SERVER_NAME_BYTES};
