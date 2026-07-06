use super::{
    Debug, GroupError, GroupResult, PeerEnvelope, PersistedRaftRuntime, RaftGroup,
    ReadBarrierRequest, ReplicatedStateMachine,
};

impl<G, A, R> RaftGroup<G, A, R>
where
    G: Clone + Ord + Debug + Send + Sync + 'static,
    A: ReplicatedStateMachine,
    A::CommandResult: Clone,
    R: PersistedRaftRuntime,
{
    pub(super) fn validate_peer_envelope(
        &self,
        envelope: &PeerEnvelope<G>,
    ) -> GroupResult<A, R, ()> {
        if envelope.group_id != self.group_id {
            return Err(GroupError::WrongGroup);
        }
        if envelope.to != self.node_id {
            return Err(GroupError::WrongRecipient {
                expected: self.node_id,
                actual: envelope.to,
            });
        }
        Ok(())
    }

    pub(super) fn validate_read_barrier_request(
        &self,
        request: &ReadBarrierRequest<G>,
    ) -> GroupResult<A, R, ()> {
        if request.group_id != self.group_id {
            return Err(GroupError::WrongGroup);
        }
        Ok(())
    }
}
