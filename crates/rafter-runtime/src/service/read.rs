//! Lifecycle admission for nonblocking verified application reads.

use super::{driver::Terminal, DurableService, ServiceState};
use crate::application::{
    ApplicationEntry, ApplicationRead, ApplicationReadRejection, DurableApplication,
};
use rafter::{LogIndex, SnapshotChunkSource};
use rafter_storage::{RaftHardStateStore, RaftLogSegment, RaftSnapshotStore};
use std::{error::Error, fmt};

/// Why an application query was returned without being invoked.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ServiceReadRejection {
    /// Poll outstanding work or finish the lifecycle transition before retrying.
    /// Failed and stopped owners cannot resume admission.
    NotReady(ServiceState),
    /// The application view was busy, behind the requested floor, or stopped.
    /// A stopped application terminally fences the service for recovery.
    Application(ApplicationReadRejection),
}

/// An uninvoked callback and the reason its application read was refused.
pub struct ServiceReadError<Q> {
    query: Q,
    rejection: ServiceReadRejection,
}

impl<Q> ServiceReadError<Q> {
    /// Returns the lifecycle or application refusal.
    #[must_use]
    pub const fn rejection(&self) -> ServiceReadRejection {
        self.rejection
    }

    /// Returns ownership of the original callback and its captured query inputs.
    #[must_use]
    pub fn into_query(self) -> Q {
        self.query
    }
}
impl<Q> fmt::Debug for ServiceReadError<Q> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServiceReadError")
            .field("rejection", &self.rejection)
            .finish_non_exhaustive()
    }
}
impl<Q> fmt::Display for ServiceReadError<Q> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "durable service read was refused: {:?}", self.rejection)
    }
}
impl<Q> Error for ServiceReadError<Q> {}

impl<H, L, S, T, A> DurableService<H, L, S, T, A>
where
    H: RaftHardStateStore + Send + 'static,
    L: RaftLogSegment + Send + 'static,
    S: RaftSnapshotStore + SnapshotChunkSource + Send + 'static,
    T: ApplicationEntry,
    A: DurableApplication<T>,
{
    /// Queries a verified application view while this owner admits live work.
    ///
    /// This performs a local read; it does not establish a Raft read proof. For
    /// a linearizable query, first consume the matching `ReadIndexGranted`
    /// output and derive `required` with the ready runtime's
    /// `committed_application_index_through(read_index)`. Capture that required
    /// application floor before admitting more input. Protocol-only positions
    /// need no phantom application update. The returned view may be newer.
    ///
    /// The callback runs on the caller's thread under an immutable application
    /// borrow, excluding updates. It must be bounded, read-only (including
    /// interior state), and nonreentrant. No query is retained or queued here;
    /// the result cannot borrow the application. A callback's returned error
    /// value is its own query result and does not poison the owner.
    ///
    /// # Errors
    /// Returns the original callback unless lifecycle state is `Ready` and a
    /// verified application view has reached `required`. Ready may still have
    /// application work in flight, yielding `Application(Busy)`. Poll outstanding
    /// work before retrying; no additional wake obligation is registered here.
    /// A stopped application is terminal and later operations require recovery.
    ///
    /// # Panics
    /// A callback panic propagates and terminally fences this owner, even when
    /// the caller catches it. Reopen both stores before continuing service.
    pub fn try_read_application<Q, R>(
        &mut self,
        required: LogIndex,
        query: Q,
    ) -> Result<ApplicationRead<R>, ServiceReadError<Q>>
    where
        Q: FnOnce(&A) -> R,
    {
        let state = self.state();
        if state != ServiceState::Ready {
            return Err(ServiceReadError {
                query,
                rejection: ServiceReadRejection::NotReady(state),
            });
        }
        // Restore admission only on an ordinary, nonterminal return. A caught
        // callback panic must not leave the service reusable.
        self.terminal = Some(Terminal::Failed);
        let Some(application) = self.application.as_mut() else {
            return Err(ServiceReadError {
                query,
                rejection: ServiceReadRejection::Application(ApplicationReadRejection::Stopped),
            });
        };
        match application.try_read(required, query) {
            Ok(read) => {
                self.terminal = None;
                Ok(read)
            }
            Err(error) => {
                let rejection = error.rejection();
                if rejection != ApplicationReadRejection::Stopped {
                    self.terminal = None;
                }
                Err(ServiceReadError {
                    query: error.into_query(),
                    rejection: ServiceReadRejection::Application(rejection),
                })
            }
        }
    }
}
