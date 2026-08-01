//! Caller-owned snapshot payload resolution outside the managed driver lock.

mod handle;
mod request;
mod resolver;

pub use request::SnapshotChunkResolveRequest;
pub use resolver::{SnapshotChunkResolver, SnapshotChunkSourceResolver};

pub(crate) use handle::SnapshotResolverHandle;
