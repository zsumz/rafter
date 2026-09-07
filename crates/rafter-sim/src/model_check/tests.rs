//! Model-check suites over the bounded, seeded, and soak drivers.
//!
//! The root of the checker's own tests: bounded exploration, linearizability,
//! purpose witnesses, replay, seeded starts, soaks, TLA projection, and
//! transition boundaries. These prove the checker, not the kernel it checks.

mod bounded;
mod linearizability;
mod purpose_witnesses;
mod replay;
mod seeded;
mod soak;
mod tla;
mod transition_boundary;
