//! Top-level protocol scenario map for the deterministic node kernel.

pub(super) use crate::*;

mod bootstrap;
mod config;
mod derived_state;
mod dispatch;
mod election;
mod helpers;
mod membership;
mod pre_vote;
mod read;
mod replication;
mod snapshot;
mod transfer;
