//! Cross-component validation for canonical and recomputable node state.

use std::collections::BTreeSet;

use crate::{LogIndex, MembershipConfig};

use super::super::super::{read_index, Node, Role};

impl Node {
    #[doc(hidden)]
    pub fn validate_derived_state(&self) -> Result<(), String> {
        self.validate_index_geometry()?;
        validate_membership_shape(&self.effective_membership(), "effective membership")?;
        validate_membership_shape(&self.committed_membership(), "committed membership")?;
        self.derived.validate(&self.persistent.log)?;
        self.validate_pending_reads()?;
        self.validate_incoming_snapshot()
    }

    fn validate_index_geometry(&self) -> Result<(), String> {
        let retained_len = u64::try_from(self.persistent.log.len())
            .map_err(|_| "retained log length is not representable as a Raft index".to_owned())?;
        let logical_last_index = self
            .snapshot_index()
            .0
            .checked_add(retained_len)
            .map(LogIndex)
            .ok_or_else(|| {
                format!(
                    "snapshot boundary {} plus {} retained entries overflows LogIndex",
                    self.snapshot_index(),
                    retained_len
                )
            })?;
        if self.volatile.commit_index > logical_last_index {
            return Err(format!(
                "commit index {} exceeds logical last index {logical_last_index}",
                self.volatile.commit_index
            ));
        }
        if self.volatile.applied_index > self.volatile.commit_index {
            return Err(format!(
                "applied index {} exceeds commit index {}",
                self.volatile.applied_index, self.volatile.commit_index
            ));
        }
        if self.volatile.applied_index < self.snapshot_index() {
            return Err(format!(
                "applied index {} is behind installed snapshot boundary {}",
                self.volatile.applied_index,
                self.snapshot_index()
            ));
        }
        if let Some(committed) = self.persistent.committed_configuration {
            if committed.index > self.volatile.commit_index {
                return Err(format!(
                    "committed configuration index {} exceeds commit index {}",
                    committed.index, self.volatile.commit_index
                ));
            }
        }
        Ok(())
    }

    fn validate_pending_reads(&self) -> Result<(), String> {
        if !self.leader.pending_reads.is_empty() && self.role() != Role::Leader {
            return Err("non-leader retains pending read-index rounds".to_owned());
        }
        if self.leader.pending_transfer.is_some() && !self.leader.pending_reads.is_empty() {
            return Err("leadership transfer retains pending read-index rounds".to_owned());
        }
        let pending_read_count = self
            .leader
            .pending_reads
            .iter()
            .map(|pending| pending.read_ids.len())
            .sum::<usize>();
        if pending_read_count > read_index::MAX_PENDING_READS {
            return Err(format!(
                "pending read count {pending_read_count} exceeds limit {}",
                read_index::MAX_PENDING_READS
            ));
        }

        let mut read_ids = BTreeSet::new();
        let mut previous_read_index = LogIndex::ZERO;
        let mut previous_sequence = 0_u64;
        for pending in &self.leader.pending_reads {
            if pending.read_ids.is_empty() {
                return Err("pending read-index round has no read IDs".to_owned());
            }
            if pending.read_index > self.volatile.commit_index {
                return Err(format!(
                    "pending read index {} exceeds commit index {}",
                    pending.read_index, self.volatile.commit_index
                ));
            }
            if pending.read_index < previous_read_index
                || pending.registered_sequence < previous_sequence
            {
                return Err("pending read-index rounds are not monotone".to_owned());
            }
            if pending.registered_sequence == 0
                || pending.registered_sequence > self.leader.heartbeat_sequence
            {
                return Err(format!(
                    "pending read round sequence {} is outside 1..={} ",
                    pending.registered_sequence, self.leader.heartbeat_sequence
                ));
            }
            for read_id in &pending.read_ids {
                if !read_ids.insert(*read_id) {
                    return Err(format!("pending read ID {read_id:?} is duplicated"));
                }
            }
            previous_read_index = pending.read_index;
            previous_sequence = pending.registered_sequence;
        }
        Ok(())
    }

    fn validate_incoming_snapshot(&self) -> Result<(), String> {
        if let Some(incoming) = &self.volatile.incoming_snapshot {
            if incoming.received_len > incoming.total_payload_len {
                return Err(format!(
                    "incoming snapshot received length {} exceeds total {}",
                    incoming.received_len, incoming.total_payload_len
                ));
            }
        }
        Ok(())
    }
}

fn validate_membership_shape(membership: &MembershipConfig, label: &str) -> Result<(), String> {
    let validate_set = |set: &crate::MembershipSet| {
        crate::MembershipSet::new(set.voters().to_vec(), set.learners().to_vec())
            .map_err(|error| format!("{label} is invalid: {error}"))
            .and_then(|rebuilt| {
                (rebuilt == *set)
                    .then_some(())
                    .ok_or_else(|| format!("{label} is not in canonical identity order"))
            })
    };
    match membership {
        MembershipConfig::Stable(set) => validate_set(set),
        MembershipConfig::Joint(joint) => {
            validate_set(joint.old())?;
            validate_set(joint.new_membership())
        }
    }
}
