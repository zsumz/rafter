//! Cross-component validation for canonical and recomputable node state.
//!
//! Every other method on [`Node`] answers a question about the protocol. This
//! one answers a question about the node itself: are the pieces of its own
//! representation still consistent with each other? A kernel that keeps
//! recomputable indexes beside their canonical source, and volatile cursors
//! beside the durable log they point into, can be wrong in ways no protocol
//! query reveals — a stale configuration offset still returns *an* entry, and a
//! commit index past the log still compares.
//!
//! The check exists as public API because the state it reads is private by
//! design and no consumer can reconstruct it. A deterministic-testing consumer
//! that explores state spaces needs a well-formedness oracle it can assert
//! after every transition; without one it can only detect the corruption that
//! happens to change an observable protocol answer later, in a different
//! transition, with the originating step already off the trace.

use std::{collections::BTreeSet, error::Error, fmt};

use crate::{LogIndex, MembershipConfig};

use super::{read_index, Node, Role};

/// A node's internal representation contradicts itself.
///
/// Returned by [`Node::validate_derived_state`], which reports the first rule
/// it finds violated. Reaching this type at all means a Rafter bug or memory
/// corruption, not a protocol event or caller mistake: no sequence of
/// [`Input`](crate::Input) values can produce it.
///
/// The error is deliberately opaque. The rules it reports on are an internal
/// consistency set that grows whenever the kernel gains state worth
/// cross-checking, and a caller must not branch on which one failed — the only
/// two useful answers are "well formed" and "this node is broken, here is
/// what". Enumerating the rules as public variants would make every new check
/// either a breaking change or a variant that existing callers silently
/// ignore, in exchange for a discrimination nothing needs. A future typed
/// classification remains additive.
///
/// It is a named type rather than a `String` so it composes: `?` into
/// `Box<dyn Error>`, and a name for the failure domain in a caller's own error
/// enum.
///
/// [`Display`](fmt::Display) renders a diagnostic describing the violated rule
/// and the values that violated it. That text is for humans and logs. It is not
/// a stable parse target and may be reworded whenever a clearer diagnosis is
/// available.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct StateValidationError {
    detail: String,
}

impl fmt::Display for StateValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.detail)
    }
}

impl Error for StateValidationError {}

impl Node {
    /// Checks that this node's internal state is structurally well formed, and
    /// returns the first violation found.
    ///
    /// Six families of rule are checked, all within this one node:
    ///
    /// - **Index geometry.** The retained log length is representable as a Raft
    ///   index and does not overflow the snapshot boundary; the commit index
    ///   does not exceed the logical last index; the applied index lies between
    ///   the installed snapshot boundary and the commit index; a recorded
    ///   committed configuration is at or below the commit index.
    /// - **Effective membership shape.** The voter and learner sets rebuild to
    ///   themselves through [`MembershipSet::new`](crate::MembershipSet::new)
    ///   and are in canonical identity order.
    /// - **Committed membership shape.** The same, for the committed
    ///   configuration.
    /// - **Derived indexes.** Every recomputable index equals what it would be
    ///   if rebuilt from its canonical source right now, so a mutation path
    ///   that forgot to maintain one is caught at the step that forgot rather
    ///   than at the later query that reads a stale answer.
    /// - **Pending read-index rounds.** Only a leader holds them, and not
    ///   during a leadership transfer; the total is within the kernel's pending
    ///   bound; each round is non-empty, sits at or below the commit index,
    ///   follows its predecessor in both read index and heartbeat sequence,
    ///   carries a sequence this leader has actually reached, and no
    ///   [`ReadId`](crate::ReadId) appears in two rounds.
    /// - **Incoming snapshot transfer.** A partial transfer has not received
    ///   more bytes than the payload it is receiving.
    ///
    /// This is a single-node structural check, not a Raft safety check. It says
    /// nothing about agreement between nodes — election safety, log matching,
    /// leader completeness and state-machine safety are properties of a set of
    /// nodes, and a cluster of individually well-formed nodes can still violate
    /// every one of them. Nor does it validate application payloads, which the
    /// kernel treats as opaque bytes.
    ///
    /// # Cost
    ///
    /// Linear in the retained log: the derived-index rules rebuild each index
    /// from the log and compare. This is an assertion for tests, simulators,
    /// model checkers, fuzz harnesses, and debug builds — not a step-loop call.
    ///
    /// # Errors
    ///
    /// Returns [`StateValidationError`] describing the first violated rule.
    /// A node that has only ever been driven through this crate's public API
    /// cannot produce one; see that type for what an error means.
    pub fn validate_derived_state(&self) -> Result<(), StateValidationError> {
        self.validate_state_rules()
            .map_err(|detail| StateValidationError { detail })
    }

    /// Runs every rule, reporting the first violation as a diagnostic string.
    ///
    /// The rules are written against `String` and wrapped once at the public
    /// boundary above, so adding a rule never touches the public type.
    fn validate_state_rules(&self) -> Result<(), String> {
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
