//! Randomized soak scenarios across cluster shapes.
//!
//! Groups the core, lease, membership, and liveness soak suites. These prove
//! the soak driver reaches the coverage its configuration promises, not that
//! any single seed is interesting.

mod core;
mod lease;
mod liveness;
mod membership;
