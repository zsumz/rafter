//! Scenario coverage for the A2 fault primitives: sustained partitions that
//! hold across elections, lossy restarts in both their legal and
//! assumption-violating shapes, and typed wire-corruption injection.

mod corruption;
mod fixtures;
mod partition;
mod restarts;
