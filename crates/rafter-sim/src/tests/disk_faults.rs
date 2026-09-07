//! Recovery scenarios for dirty disk images.
//!
//! Groups the crash-prefix, torn-tail, and hard-state-reorder suites. Each
//! proves a running cluster survives reopening a damaged replica, or refuses
//! the image loudly.

mod dirty_recovery;
mod fixtures;
mod hard_state;
