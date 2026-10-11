//! Client ownership for the asynchronous durable path (feature `durable`).
//!
//! Encoded writes enter a bounded queue without borrowing the application
//! worker. Only a durable application completion resolves a write successfully.
//! Dropping a caller does not cancel an accepted Raft command or release its
//! client credit while the owner still tracks it. Completed, uncollected
//! outcomes also retain their credit. Application outcome sizes and the kernel
//! log are outside the encoded-payload byte bound.
//!
//! The owner drives the existing service and returns its transport and snapshot
//! events. It opens no sockets. Drive it until `Pending` before waiting on the
//! supplied wake callback. Consensus inputs still have the service's synchronous
//! I/O contract. After a fatal error, discard the owner and recover both stores;
//! discarding it joins workers before resolving remaining client outcomes.
//!
//! `recover_with_reads` adds independently bounded encoded queries. Linearizable
//! reads obtain a real grant, preserve its term, translate the application floor,
//! and honor the caller's freshness minimum. Canceled quorum rounds retain their
//! credits until protocol completion or backend release. Local reads carry no
//! quorum proof; lease reads are refused. Query results need an application bound.
//! [`DurableRaftDriver`](crate::durable::DurableRaftDriver) attaches a group identity and an existing transport,
//! exposing the managed `RaftHandle` through [`DurableCommandSender`](crate::durable::DurableCommandSender). The
//! embedding owns authenticated inbound routing, membership policy, snapshot
//! installation, and transport lifecycle.

mod budget;
mod group;
mod owner;
mod read;
mod reply;
mod sender;
mod shared;

pub use group::{
    DurableCommandSender, DurableDriverAttachError, DurableRaftDriver, DurableRaftHandle,
};
pub use owner::{DurableClientOwner, DurableClientStepError};
pub use read::{DurableClientOptions, DurableReadLimits, DurableReadSender, DurableReadStats};
pub use sender::DurableWriteSender;
pub use shared::{DurableWriteLimits, DurableWriteStats};

use std::sync::{Mutex, MutexGuard};

fn lock<T>(value: &Mutex<T>) -> MutexGuard<'_, T> {
    value
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
