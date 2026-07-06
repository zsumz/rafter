#![allow(clippy::wildcard_imports)]

use super::*;

impl<G, A, R> InMemoryRaftState<G, A, R>
where
    G: Clone + Ord + Debug + Send + Sync + 'static,
    A: ReplicatedStateMachine + Send + 'static,
    A::Command: Send + 'static,
    A::CommandResult: Clone + Send + 'static,
    A::Query: Clone + Send + 'static,
    A::QueryResult: Send + 'static,
    A::Error: Debug + Send + 'static,
    R: PersistedRaftRuntime + Send + 'static,
    R::Error: Debug + Send + 'static,
{
    pub(super) fn transfer_leadership(
        &mut self,
        group_id: &G,
        target: NodeId,
    ) -> ManagedResult<A, R, ()> {
        self.reject_for_operation(group_id)?;
        let report = self
            .primary_group_mut()?
            .step(GroupInput::TransferLeadership { target })?;
        let rejection = report
            .leadership_transfer_events
            .iter()
            .find_map(|event| match event {
                LeadershipTransferEvent::Rejected {
                    target: event_target,
                    reason,
                    leader_hint,
                } if *event_target == target => Some((*reason, *leader_hint)),
                _ => None,
            });
        self.route_report(report);
        if let Err(error) = self.drain_network() {
            self.publish_primary_metrics();
            return Err(error);
        }
        self.publish_primary_metrics();
        if let Some((reason, leader_hint)) = rejection {
            return Err(ManagedOperationError::Transfer(
                TransferLeadershipError::Rejected {
                    reason,
                    leader_hint,
                },
            ));
        }
        Ok(())
    }
}
