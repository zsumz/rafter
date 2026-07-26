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
        options: ReadOptions,
    ) -> ManagedQueryResult<G, A, R> {
        self.reject_for_operation(group_id)?;
        let read_id = self.read_id_for_consistency(consistency)?;
        for _ in 0..self.max_drive_steps {
            // Routing may have ended this barrier since the last attempt — a
            // leadership change is observed by a delivered frame, not by the
            // read call. That answer is the whole answer, and asking the group
            // again would submit a spent `ReadId`.
            if let Some(error) = self.take_routed_read_outcome(read_id) {
                self.publish_primary_metrics();
                return Err(ManagedOperationError::Read(error));
            }
            let request = self.read_request(query, consistency, read_id, options)?;
            let read = match self.primary_group_mut()?.read(request) {
                Ok(read) => read,
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
            // The report is routed, never dropped: it carries the read-index
            // frames the barrier's quorum round needs, and a barrier whose
            // round was never sent can only wait until the drive bound.
            // `ReadOutcome::Pending` duplicates the same frames, so exactly one
            // of the two lists is routed — routing both sends every frame
            // twice.
            self.route_report(read.report);
            if let Some(receipt) = self.handle_read_outcome(read.outcome, read_id)? {
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
        // The driver stopped waiting; nothing refused anything. The barrier is
        // cancelled first, so the group's `reserved_reads` returns to its
        // previous value before this error is seen.
        if let Some(read_id) = read_id {
            self.abandon_read(read_id);
            return Err(ManagedOperationError::Read(ReadError::Abandoned {
                read_id,
                reason: ReadAbandonReason::DriveBoundReached,
            }));
        }
        // A consistency level that reserves no read ID has no barrier to
        // abandon, so the bound is the driver's own routing bound.
        Err(ManagedOperationError::DriveBoundReached {
            max_steps: self.max_drive_steps,
        })
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
            // Peer messages reached the network through the routed report.
            ReadOutcome::Pending { .. } => Ok(None),
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

    /// Abandons a read whose freshness gap cannot close on its own.
    ///
    /// The gap this handles is a real one: the state machine has not yet
    /// applied the committed application entries at or below the granted read
    /// index. It closes as those applies drain, so the driver keeps driving
    /// while anything is still in flight and gives up only once the network is
    /// quiet and nothing can advance the applied index.
    ///
    /// This path used to be reached after every election. The barrier required
    /// the state machine to reach the read index itself, which named the new
    /// leader's `Noop`, and the network drains promptly when nothing else is
    /// happening — so a linearizable read failed until an unrelated write
    /// committed. The barrier now requires only the application floor below the
    /// read index, so the common post-election read answers here instead.
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

    /// Builds one read request, carrying the caller's freshness floor verbatim.
    ///
    /// The floor is not capped, lowered, or substituted: the app layer honors
    /// what it is given, and a driver that quietly weakened it would turn a
    /// read-your-writes request into an ordinary one.
    pub(super) fn read_request(
        &self,
        query: &A::Query,
        consistency: ReadConsistency,
        read_id: Option<ReadId>,
        options: ReadOptions,
    ) -> ReadRequestResult<G, A, R> {
        let min_applied_index = options.min_applied_index;
        match (consistency, read_id) {
            (ReadConsistency::Linearizable, Some(read_id)) => Ok(ReadRequest::Linearizable {
                group_id: self.group_id.clone(),
                read_id,
                query: query.clone(),
                min_applied_index,
                context: Vec::new(),
            }),
            (ReadConsistency::Local, None) => Ok(ReadRequest::Local {
                group_id: self.group_id.clone(),
                query: query.clone(),
                min_applied_index,
            }),
            (ReadConsistency::LeaseRead, None) => Ok(ReadRequest::Lease {
                group_id: self.group_id.clone(),
                query: query.clone(),
                min_applied_index,
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
