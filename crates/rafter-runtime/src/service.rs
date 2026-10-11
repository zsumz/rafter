//! Poll-driven durable service composition over the existing runtime workers.
//!
//! [`crate::service::DurableService`] owns consensus persistence and ordered application work.
//! It accepts no input mailbox: a refusal returns the original input. It holds
//! one runtime output batch, at most one converted application item, and the
//! workers' bounded outstanding work. A runtime/recovery output batch itself
//! can be large; this is not a fixed bound on the kernel log or replay memory.
//!
//! Polling never waits for persistence or application work to finish. An idle
//! application thread is joined at snapshot handoff. Ordinary consensus steps
//! still use the synchronous durability fence; eligible proposal batches use
//! the persistence thread. Callers choose their executor; `recover_with_wake`
//! and `poll_next` support notification-driven scheduling without timer polling.
//!
//! Peer sends go to the caller's existing authenticated transport. Only
//! [`crate::service::ServiceEvent::Applied`] establishes client write success. Appended IDs are
//! correlation, not success or deduplication; dropped IDs have unknown outcome.
//! A read grant is held until its application prefix is durable. The embedding
//! can use [`crate::service::DurableService::try_read_application`] to query a
//! verified application view while respecting the owner's lifecycle.
//!
//! Recovery and snapshot installation close admission until application state
//! catches up. Snapshot bytes remain in the runtime's snapshot store; the
//! embedding installs them atomically with the required floor before returning
//! its store. Any fatal error requires reopening both stores. Dropping a busy
//! service joins workers but abandons outcomes; use explicit draining shutdown
//! to preserve completion delivery. Shutdown drains owned durable work, not
//! outstanding quorum decisions. The embedding closes unresolved client waiters
//! with unknown outcome. Returned `StageSnapshotChunk` outputs are observations
//! of work already persisted by the runtime, not requests to persist it again.

mod command;
mod compaction;
mod contract;
mod driver;
mod error;
mod lifecycle;
mod poll;
mod read;
mod step;

pub use command::CommittedCommand;
pub use contract::{ServiceEvent, ServiceInput, ServiceOptions, ServicePoll, ServiceState};
pub use driver::DurableService;
pub use error::{ServiceAdapterError, ServiceError, ServiceStepError};
pub use read::{ServiceReadError, ServiceReadRejection};

#[cfg(test)]
mod tests;
