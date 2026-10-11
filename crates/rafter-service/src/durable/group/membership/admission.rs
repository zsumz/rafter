//! Normalize retained allocations before reserving the shared control credit.
use super::{DurableMembershipError, DurableMembershipReceipt, Pending, ProposalReply};
use crate::{durable::lock, durable::reply::Reply, DriverFuture};
use rafter::{
    ConfigurationProposalRejection, MembershipSet, NodeId, PromotionBarrier, ProposalRejection,
};
use rafter_app::membership::{MembershipChange, NodeInfo};
use std::{mem::size_of, sync::Arc};

impl<G: Eq, O, R> super::super::DurableCommandSender<G, O, R> {
    /// Queues one membership proposal on an explicitly enabled driver.
    ///
    /// Translation uses effective membership at dispatch, not at enqueue. The
    /// result confirms a durable append and drained immediate effects, not a
    /// committed change. Continue observing configuration commits to execute a
    /// membership plan. Dropping the future abandons observation; submitted work
    /// may still commit. Metadata in `NodeInfo` is discarded, as in the shared
    /// input translation. Authorization and transport policy remain with the
    /// embedding. New replica IDs must satisfy the kernel's fresh-identity
    /// precondition; this endpoint does not track or fence retired identities.
    ///
    /// # Errors
    /// Refuses foreign groups, disabled or closed admission, and excess retained
    /// controls or configuration bytes. Kernel refusals retain their exact reason.
    /// A submitted request may be unconfirmed when its backend is released.
    #[must_use]
    pub fn propose_membership(
        &self,
        group: &G,
        change: MembershipChange,
    ) -> DriverFuture<Result<DurableMembershipReceipt, DurableMembershipError>> {
        if group != &self.shared.group {
            return Box::pin(std::future::ready(Err(DurableMembershipError::WrongGroup)));
        }
        // Metadata destruction does not hold the driver state lock.
        let change = match change {
            MembershipChange::AddLearner { node_id, info } => {
                drop(info);
                MembershipChange::AddLearner {
                    node_id,
                    info: NodeInfo::default(),
                }
            }
            other => other,
        };
        match self.admit_membership(change) {
            Ok(reply) => {
                let future = reply.future();
                (self.shared.wake)();
                future
            }
            Err(error) => Box::pin(std::future::ready(Err(error))),
        }
    }

    fn admit_membership(
        &self,
        change: MembershipChange,
    ) -> Result<ProposalReply, DurableMembershipError> {
        let mut state = lock(&self.shared.state);
        if state.closed {
            return Err(DurableMembershipError::ShuttingDown);
        }
        let limits = self
            .shared
            .membership_limits
            .ok_or(DurableMembershipError::NotEnabled)?;
        if self.shared.budget.stats().0 != 0 {
            self.shared.budget.record_refusal();
            return Err(DurableMembershipError::Backpressure);
        }
        let (change, bytes) =
            normalize(change, limits.request_bytes.get()).inspect_err(|error| {
                if matches!(error, DurableMembershipError::Backpressure) {
                    self.shared.budget.record_refusal();
                }
            })?;
        let credit = self
            .shared
            .budget
            .reserve(bytes, 1, limits.request_bytes.get())
            .ok_or(DurableMembershipError::Backpressure)?;
        let reply = Reply::new(credit, Arc::clone(&self.shared.wake));
        state.membership = Some(Pending {
            change: Some(change),
            submitted: false,
            receipt: None,
            rejection: None,
            leader_hint: None,
            reply: Arc::clone(&reply),
        });
        Ok(reply)
    }
}

fn charge(members: usize, barriers: usize, max: usize) -> Result<usize, DurableMembershipError> {
    members
        .checked_mul(size_of::<NodeId>())
        .and_then(|n| {
            barriers
                .checked_mul(size_of::<PromotionBarrier>())
                .and_then(|b| n.checked_add(b))
        })
        .filter(|bytes| *bytes <= max)
        .ok_or(DurableMembershipError::Backpressure)
}

fn target_copy(
    target: &MembershipSet,
    barriers: usize,
    max: usize,
) -> Result<(MembershipSet, usize), DurableMembershipError> {
    let members = target
        .voters()
        .len()
        .checked_add(target.learners().len())
        .ok_or(DurableMembershipError::Backpressure)?;
    charge(members, barriers, max)?;
    let voters = target.voters().to_vec();
    let learners = target.learners().to_vec();
    let members = voters
        .capacity()
        .checked_add(learners.capacity())
        .ok_or(DurableMembershipError::Backpressure)?;
    let bytes = charge(members, 0, max)?;
    let target =
        MembershipSet::new(voters, learners).map_err(|error| DurableMembershipError::Rejected {
            reason: ProposalRejection::Configuration(
                ConfigurationProposalRejection::InvalidMembership { error },
            ),
            leader_hint: None,
        })?;
    Ok((target, bytes))
}

fn normalize(
    change: MembershipChange,
    max: usize,
) -> Result<(MembershipChange, usize), DurableMembershipError> {
    match change {
        MembershipChange::ChangeVoters { target } => {
            let (target, bytes) = target_copy(&target, 0, max)?;
            Ok((MembershipChange::ChangeVoters { target }, bytes))
        }
        MembershipChange::EnterJoint {
            target,
            promotion_barriers,
        } => {
            let (target, bytes) = target_copy(&target, promotion_barriers.len(), max)?;
            let promotion_barriers = promotion_barriers.as_slice().to_vec();
            let barrier_bytes = charge(0, promotion_barriers.capacity(), max)?;
            let bytes = bytes
                .checked_add(barrier_bytes)
                .filter(|n| *n <= max)
                .ok_or(DurableMembershipError::Backpressure)?;
            Ok((
                MembershipChange::EnterJoint {
                    target,
                    promotion_barriers,
                },
                bytes,
            ))
        }
        other @ (MembershipChange::AddLearner { .. }
        | MembershipChange::PromoteLearner { .. }
        | MembershipChange::RemoveNode { .. }
        | MembershipChange::LeaveJoint) => Ok((other, 0)),
    }
}
