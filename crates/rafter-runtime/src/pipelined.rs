//! One outstanding persistence operation with explicitly independent replication sends.
//!
//! The owner prepares a bounded proposal batch, releases only eligible leader
//! appends, and hands the owned work to an I/O executor. No consensus input can
//! run until its generation/operation completion returns. Thus local unstable
//! entries cannot contribute to a durable quorum. Followers and term/vote,
//! membership, conflict, and snapshot transitions retain synchronous fences.
//! Applications still acknowledge clients only after their own durable apply.
mod driver;
mod eligibility;
mod error;
mod work;

pub use driver::{PipelineProgress, PipelinedRaftNode};
pub use error::PipelineError;
pub use work::{PersistenceCompletion, PersistenceOperation, PersistenceWork, PreparedProposals};

#[cfg(test)]
mod tests;
