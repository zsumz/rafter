//! Evidence for the pipelining and flow-control discipline: the
//! per-follower Progress/Inflights window turns catch-up from ack-paced to
//! wire-paced, duplicated rejection storms collapse to probing without
//! livelock or window rewind, and the leader reports each follower's send
//! discipline through [`rafter::ReplicationState`].
//!
//! Catch-up is measured in explicit rounds. One round models one network
//! hop: every message in flight at the start of the round is delivered
//! exactly once (responses it provokes belong to the next round), and the
//! leader ticks only when a round finds the network idle.

mod catch_up;
mod fixtures;
mod rejection_storm;
