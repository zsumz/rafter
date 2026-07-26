#![allow(clippy::wildcard_imports)]

use super::*;

impl<G, A, R> InMemoryRaftDriver<G, A, R>
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
    /// Builds an in-memory driver from already configured Raft groups.
    ///
    /// The driver may adopt groups that have already consumed local proposal
    /// or read IDs, but only when their local waiter state is quiescent.
    /// Generated IDs start above the highest adopted group watermarks.
    ///
    /// # Errors
    ///
    /// Returns [`ManagedDriverError`] when no groups are supplied, the primary
    /// node is missing, the supplied groups do not all belong to the same
    /// group ID, a group is poisoned, a group has unresolved local
    /// proposal/read waiters, or an adopted local-ID watermark is exhausted.
    pub fn new(
        primary_node_id: NodeId,
        groups: impl IntoIterator<Item = RaftGroup<G, A, R>>,
    ) -> Result<Self, ManagedDriverError> {
        let mut group_id = None;
        let mut by_node = BTreeMap::new();
        let mut max_local_proposal_id = None::<(NodeId, LocalProposalId)>;
        let mut max_read_id = None::<(NodeId, ReadId)>;
        for group in groups {
            match &group_id {
                Some(expected) if expected != group.group_id() => {
                    return Err(ManagedDriverError::MixedGroups);
                }
                None => {
                    group_id = Some(group.group_id().clone());
                }
                _ => {}
            }
            let node_id = group.node_id();
            if by_node.contains_key(&node_id) {
                return Err(ManagedDriverError::DuplicateNode { node_id });
            }
            let metrics = group.metrics();
            match group.fatal_state() {
                GroupFatalState::Poisoned { reason } => {
                    return Err(ManagedDriverError::PoisonedGroup {
                        node_id,
                        reason: reason.clone(),
                    });
                }
                GroupFatalState::Healthy if !group.poisoned_waiters().is_empty() => {
                    return Err(ManagedDriverError::PoisonedGroup {
                        node_id,
                        reason: "group has undrained poisoned waiters".to_owned(),
                    });
                }
                GroupFatalState::Healthy => {}
            }
            if metrics.pending_proposals != 0 || metrics.reserved_reads != 0 {
                return Err(ManagedDriverError::NonQuiescentGroup {
                    node_id,
                    pending_proposals: metrics.pending_proposals,
                    reserved_reads: metrics.reserved_reads,
                });
            }
            if let Some(watermark) = group.local_proposal_id_watermark() {
                if max_local_proposal_id.is_none_or(|(_, current)| watermark > current) {
                    max_local_proposal_id = Some((node_id, watermark));
                }
            }
            if let Some(watermark) = group.read_id_watermark() {
                if max_read_id.is_none_or(|(_, current)| watermark > current) {
                    max_read_id = Some((node_id, watermark));
                }
            }
            by_node.insert(node_id, group);
        }
        let group_id = group_id.ok_or(ManagedDriverError::EmptyCluster)?;
        if !by_node.contains_key(&primary_node_id) {
            return Err(ManagedDriverError::MissingPrimary {
                node_id: primary_node_id,
            });
        }
        let metrics = by_node
            .get(&primary_node_id)
            .ok_or(ManagedDriverError::MissingPrimary {
                node_id: primary_node_id,
            })?
            .metrics();
        let next_proposal_id = match max_local_proposal_id {
            Some((node_id, last_seen_local_proposal_id)) => {
                Some(last_seen_local_proposal_id.0.checked_add(1).ok_or(
                    ManagedDriverError::LocalProposalIdExhausted {
                        node_id,
                        last_seen_local_proposal_id,
                    },
                )?)
            }
            None => Some(1),
        };
        let next_read_id = match max_read_id {
            Some((node_id, last_seen_read_id)) => Some(last_seen_read_id.0.checked_add(1).ok_or(
                ManagedDriverError::ReadIdExhausted {
                    node_id,
                    last_seen_read_id,
                },
            )?),
            None => Some(1),
        };
        Ok(Self {
            inner: Arc::new(Mutex::new(InMemoryRaftState {
                group_id,
                primary_node_id,
                groups: by_node,
                network: VecDeque::new(),
                metrics: MetricsPublisher::new(metrics),
                next_proposal_id,
                next_read_id,
                routed_read_outcome: None,
                max_drive_steps: 1024,
                shutting_down: false,
            })),
        })
    }
}
