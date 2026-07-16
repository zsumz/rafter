//! Version 1 peer-message payload grammar facade.
//!
//! Existing tags and field order are wire-stable. New tags may be allocated,
//! but an existing tag must never be reassigned.

mod append;
mod membership;
mod message;
mod snapshot;
mod tags;

#[cfg(test)]
pub(crate) use append::{append_entries_entry_capacity, MIN_ENCODED_LOG_ENTRY_BYTES};
#[cfg(test)]
pub(crate) use membership::{membership_node_capacity, NODE_ID_BYTES};
pub(crate) use message::{decode_payload, encode_payload};
#[cfg(test)]
pub(crate) use tags::{LogEntryTag, MembershipTag, MessageTag};
