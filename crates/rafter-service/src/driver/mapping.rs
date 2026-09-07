//! How a managed failure is said, and where each vocabulary lives.
//!
//! The driver's failures reach three different readers, so they are declared in
//! three places and gathered here. [`driver_error`] and [`checkpoint_error`] hold
//! what an embedder matches on; [`super::routing`] holds what a stage carries
//! before a client hears anything; [`super::group_error`] holds the projections
//! into the client surfaces. This file re-exports all of them under their
//! original names so nothing outside the driver has to know that.

#![allow(
    clippy::wildcard_imports,
    reason = "the driver is one state machine deliberately split across focused files, each opening the shared driver namespace"
)]

use std::{error::Error, fmt};

use super::*;

mod checkpoint_error;
mod driver_error;

pub use checkpoint_error::ControlPlaneCheckpointError;
pub use driver_error::ManagedDriverError;

pub(super) use super::group_error::{
    read_error_from_group, terminal_read_error, transfer_error_from_group, write_error_from_group,
};
pub(super) use super::routing::{DriverRoutingError, ManagedOperationError};

#[cfg(test)]
#[path = "mapping_test.rs"]
mod tests;
