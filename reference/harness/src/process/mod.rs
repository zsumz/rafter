//! Process-test mechanisms shared by independent consumers.
//!
//! Callers own every command-line argument, protocol line, parser, and recovery
//! decision. This module only supplies bounded waits, line exchanges, retained
//! child output, reaping, and scratch-space lifetime.

mod child;
mod connection;
mod lines;
mod scratch;
mod wait;

pub use child::{ChildProcess, ChildWaitError};
pub use connection::{
    ConnectionTimeouts, ExchangeError, LineConnection, ReconnectingClient, RequestError,
};
pub use scratch::ScratchSpace;
pub use wait::{Wait, WaitError};
