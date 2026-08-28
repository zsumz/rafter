//! Durability of term, vote, and commit metadata before dependent output.
//!
//! The children divide by what the write is guarding: elections and vote
//! responses, the final commit write standing behind an apply, and the inputs
//! both script. Log entry durability is the persistence-ordering module's.

mod commit_failure;
mod fixtures;
mod voting;
