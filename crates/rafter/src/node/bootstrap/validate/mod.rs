//! Validation and materialization of a durable bootstrap image.

mod log;

use crate::{LogIndex, RaftSnapshotMetadata};

use super::super::NodeConfig;
use super::error::BootstrapValidationError;
use super::state::{BootstrapParts, BootstrapState};
use log::validate_log;

impl BootstrapState {
    pub(in crate::node) fn into_parts(
        self,
        config: &NodeConfig,
    ) -> Result<BootstrapParts, BootstrapValidationError> {
        self.validate_vote()?;
        self.validate_snapshot(config)?;

        let log = validate_log(
            self.log,
            self.snapshot.as_ref().map(|snapshot| &snapshot.metadata),
            self.current_term,
            self.commit_index,
            self.committed_configuration,
        )?;
        let snapshot_index = self.snapshot.as_ref().map_or(LogIndex::ZERO, |snapshot| {
            snapshot.metadata.last_included_index
        });

        Ok(BootstrapParts {
            current_term: self.current_term,
            voted_for: self.voted_for,
            commit_index: self.commit_index.max(snapshot_index),
            committed_configuration: self.committed_configuration,
            snapshot: self.snapshot,
            log,
        })
    }

    fn validate_vote(&self) -> Result<(), BootstrapValidationError> {
        let Some(voted_for) = self.voted_for else {
            return Ok(());
        };
        if self.current_term.is_zero() {
            return Err(BootstrapValidationError::VoteInZeroTerm { voted_for });
        }
        Ok(())
    }

    fn validate_snapshot(&self, config: &NodeConfig) -> Result<(), BootstrapValidationError> {
        let Some(snapshot) = self.snapshot.as_ref() else {
            return Ok(());
        };
        let metadata = &snapshot.metadata;

        if metadata.hard_state_term > self.current_term {
            return Err(
                BootstrapValidationError::SnapshotHardStateTermAheadOfCurrentTerm {
                    snapshot_hard_state_term: metadata.hard_state_term,
                    current_term: self.current_term,
                },
            );
        }

        if snapshot_writer_is_replica(metadata, config) {
            Ok(())
        } else {
            Err(BootstrapValidationError::SnapshotWriterNotReplica {
                writer_id: metadata.writer_id,
            })
        }
    }
}

/// The author rule, in the one place that sees only a durable image.
///
/// Replica rather than voter: a learner replicates the same committed prefix a
/// voter does, and the shipped compaction API stamps the boundary membership
/// into the descriptor a node writes about itself — so demanding a voter here
/// made a learner's own snapshot unhydratable, which is to say it made the
/// learner unbootable. The receive path and
/// [`Node::install_local_snapshot`](crate::Node::install_local_snapshot) apply
/// the same rule, so nothing can be installed that this refuses to recover.
fn snapshot_writer_is_replica(metadata: &RaftSnapshotMetadata, config: &NodeConfig) -> bool {
    metadata.committed_membership().map_or_else(
        || {
            config
                .static_membership_ref()
                .contains_replica(metadata.writer_id)
        },
        |membership| membership.contains_replica(metadata.writer_id),
    )
}
