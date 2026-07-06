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
    pub(super) fn read(
        &mut self,
        group_id: &G,
        query: &A::Query,
        consistency: ReadConsistency,
    ) -> ManagedQueryResult<G, A, R> {
        self.reject_for_operation(group_id)?;
        let read_id = self.read_id_for_consistency(consistency)?;
        for _ in 0..self.max_drive_steps {
            let request = self.read_request(query, consistency, read_id)?;
            let outcome = match self.primary_group_mut()?.read(request) {
                Ok(outcome) => outcome,
                Err(error) => {
                    if let Some(read_id) = read_id {
                        if let Some(read_error) = self.poisoned_read_error_from_primary(read_id) {
                            self.publish_primary_metrics();
                            return Err(ManagedOperationError::Read(read_error));
                        }
                        self.abandon_read(read_id);
                    }
                    return Err(error.into());
                }
            };
            if let Some(receipt) = self.handle_read_outcome(outcome, read_id)? {
                return Ok(receipt);
            }
            if let Err(error) = self.drain_network() {
                if let Some(read_id) = read_id {
                    if let Some(read_error) = self.poisoned_read_error_from_primary(read_id) {
                        self.publish_primary_metrics();
                        return Err(ManagedOperationError::Read(read_error));
                    }
                    self.abandon_read(read_id);
                }
                return Err(error);
            }
        }
        if let Some(read_id) = read_id {
            self.abandon_read(read_id);
        }
        Err(ManagedOperationError::Read(ReadError::Transport {
            message: format!("managed read stalled after {} steps", self.max_drive_steps),
        }))
    }

    pub(super) fn handle_read_outcome(
        &mut self,
        outcome: ReadOutcome<G, A::QueryResult>,
        managed_read_id: Option<ReadId>,
    ) -> ManagedReadResult<G, A, R> {
        match outcome {
            ReadOutcome::Ready { result, proof } => {
                self.publish_primary_metrics();
                Ok(Some(QueryReceipt { result, proof }))
            }
            ReadOutcome::Pending { peer_messages, .. } => {
                self.network.extend(peer_messages);
                Ok(None)
            }
            ReadOutcome::Rejected {
                read_id,
                reason,
                leader_hint,
            } => {
                self.publish_primary_metrics();
                Err(ManagedOperationError::Read(ReadError::Rejected {
                    read_id: Some(read_id),
                    reason,
                    leader_hint,
                }))
            }
            ReadOutcome::Canceled {
                read_id,
                reason,
                leader_hint,
            } => {
                self.publish_primary_metrics();
                Err(ManagedOperationError::Read(ReadError::Canceled {
                    read_id,
                    reason,
                    leader_hint,
                }))
            }
            ReadOutcome::LinearizableFreshnessUnavailable {
                read_id,
                required_applied_index,
                local_applied_index,
            } => self.handle_linearizable_freshness_gap(
                read_id,
                required_applied_index,
                local_applied_index,
            ),
            ReadOutcome::LocalFreshnessUnavailable {
                required_applied_index,
                local_applied_index,
            } => {
                self.publish_primary_metrics();
                Err(ManagedOperationError::Read(
                    ReadError::FreshnessUnavailable {
                        read_id: None,
                        required_applied_index,
                        local_applied_index,
                    },
                ))
            }
            _ => {
                if let Some(read_id) = managed_read_id {
                    self.abandon_read(read_id);
                }
                self.publish_primary_metrics();
                Err(ManagedOperationError::Read(
                    ReadError::ManagedInvariantViolation {
                        message:
                            "managed driver received unsupported app-layer read outcome variant"
                                .to_owned(),
                    },
                ))
            }
        }
    }

    pub(super) fn handle_linearizable_freshness_gap(
        &mut self,
        read_id: ReadId,
        required_applied_index: LogIndex,
        local_applied_index: LogIndex,
    ) -> ManagedReadResult<G, A, R> {
        if self.network.is_empty() {
            self.abandon_read(read_id);
            return Err(ManagedOperationError::Read(
                ReadError::FreshnessUnavailable {
                    read_id: Some(read_id),
                    required_applied_index,
                    local_applied_index,
                },
            ));
        }
        Ok(None)
    }

    pub(super) fn read_id_for_consistency(
        &mut self,
        consistency: ReadConsistency,
    ) -> ManagedResult<A, R, Option<ReadId>> {
        match consistency {
            ReadConsistency::Linearizable => self
                .next_read_id()
                .map(Some)
                .map_err(ManagedOperationError::Read),
            ReadConsistency::LeaseRead | ReadConsistency::Local => Ok(None),
            _ => Err(ManagedOperationError::Read(
                ReadError::UnsupportedConsistency { consistency },
            )),
        }
    }

    pub(super) fn read_request(
        &self,
        query: &A::Query,
        consistency: ReadConsistency,
        read_id: Option<ReadId>,
    ) -> ReadRequestResult<G, A, R> {
        match (consistency, read_id) {
            (ReadConsistency::Linearizable, Some(read_id)) => Ok(ReadRequest::Linearizable {
                group_id: self.group_id.clone(),
                read_id,
                query: query.clone(),
                min_applied_index: None,
                context: Vec::new(),
            }),
            (ReadConsistency::Local, None) => Ok(ReadRequest::Local {
                group_id: self.group_id.clone(),
                query: query.clone(),
                min_applied_index: None,
            }),
            (ReadConsistency::LeaseRead, None) => Ok(ReadRequest::Lease {
                group_id: self.group_id.clone(),
                query: query.clone(),
                min_applied_index: None,
            }),
            (ReadConsistency::Linearizable, None)
            | (ReadConsistency::LeaseRead | ReadConsistency::Local, Some(_)) => {
                unreachable!("managed read id allocation follows requested consistency")
            }
            _ => Err(ManagedOperationError::Read(
                ReadError::UnsupportedConsistency { consistency },
            )),
        }
    }
}
