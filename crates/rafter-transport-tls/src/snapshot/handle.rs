//! Type-erased shared resolver retained by builders and sender workers.

use std::{fmt, sync::Arc};

use crate::BoxError;

use super::{SnapshotChunkResolveRequest, SnapshotChunkResolver};

trait ErasedSnapshotChunkResolver<G>: Send + Sync {
    fn resolve(
        &self,
        request: SnapshotChunkResolveRequest<'_, G>,
    ) -> Result<Option<Vec<u8>>, BoxError>;
}

struct ResolverAdapter<R>(R);

impl<G, R> ErasedSnapshotChunkResolver<G> for ResolverAdapter<R>
where
    R: SnapshotChunkResolver<G>,
{
    fn resolve(
        &self,
        request: SnapshotChunkResolveRequest<'_, G>,
    ) -> Result<Option<Vec<u8>>, BoxError> {
        self.0
            .resolve(request)
            .map_err(|error| Box::new(error) as BoxError)
    }
}

pub(crate) struct SnapshotResolverHandle<G> {
    resolver: Arc<dyn ErasedSnapshotChunkResolver<G>>,
}

impl<G> SnapshotResolverHandle<G> {
    pub(crate) fn new<R>(resolver: R) -> Self
    where
        R: SnapshotChunkResolver<G>,
    {
        Self {
            resolver: Arc::new(ResolverAdapter(resolver)),
        }
    }

    pub(crate) fn resolve(
        &self,
        request: SnapshotChunkResolveRequest<'_, G>,
    ) -> Result<Option<Vec<u8>>, BoxError> {
        self.resolver.resolve(request)
    }
}

impl<G> Clone for SnapshotResolverHandle<G> {
    fn clone(&self) -> Self {
        Self {
            resolver: Arc::clone(&self.resolver),
        }
    }
}

impl<G> fmt::Debug for SnapshotResolverHandle<G> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SnapshotResolverHandle")
            .finish_non_exhaustive()
    }
}
