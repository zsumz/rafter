use std::collections::BTreeMap;

use rafter::NodeId;
use rafter_multiraft::managed::RemoveError;

use crate::{
    GroupId, GroupIncarnation, GroupLifecycle, LifecycleOutcome, LifecycleRejection,
    LifecycleRequest, LifecycleTransition,
};

use super::{AdapterError, GroupSlot, ManagedCounterCluster};

impl ManagedCounterCluster {
    /// Applies one complete consumer-owned lifecycle transition.
    ///
    /// Every state/request cell is either applied, idempotent, or returned as
    /// a typed conflict. Physical managed-host ownership changes only on
    /// `Create` and `Remove`; tombstones and incarnation retention remain here.
    ///
    /// # Errors
    ///
    /// Returns a real driver open, scheduler admission, or ownership error.
    /// Policy conflicts are values inside [`LifecycleTransition`].
    pub fn lifecycle(
        &mut self,
        group_id: GroupId,
        request: LifecycleRequest,
    ) -> Result<LifecycleTransition, AdapterError> {
        let outcome = match request {
            LifecycleRequest::Create { quota } => self.create(group_id, quota)?,
            LifecycleRequest::Recover => self.recover(group_id)?,
            LifecycleRequest::Serve => self.serve(group_id)?,
            LifecycleRequest::Drain => self.drain(group_id),
            LifecycleRequest::Remove => self.remove(group_id),
            LifecycleRequest::Tombstone => self.tombstone(group_id),
        };
        Ok(LifecycleTransition {
            outcome,
            failed: Vec::new(),
        })
    }

    fn create(
        &mut self,
        group_id: GroupId,
        quota: crate::WorkQuota,
    ) -> Result<LifecycleOutcome, AdapterError> {
        let Some(current) = self.groups.get(&group_id).cloned() else {
            self.open_physical_group(group_id, quota)?;
            self.groups.insert(
                group_id,
                GroupSlot {
                    incarnation: GroupIncarnation::first(),
                    lifecycle: GroupLifecycle::Creating,
                    quota,
                    applied_index: rafter::LogIndex::ZERO,
                    value: 0,
                    sessions: BTreeMap::default(),
                },
            );
            return Ok(LifecycleOutcome::Created {
                incarnation: GroupIncarnation::first(),
            });
        };
        match current.lifecycle {
            GroupLifecycle::Creating if current.quota == quota => {
                Ok(LifecycleOutcome::Idempotent {
                    state: GroupLifecycle::Creating,
                    incarnation: current.incarnation,
                })
            }
            GroupLifecycle::Creating => Ok(LifecycleOutcome::Rejected(
                LifecycleRejection::QuotaConflict {
                    current: current.quota,
                },
            )),
            GroupLifecycle::Removed => {
                let Some(incarnation) = current.incarnation.successor() else {
                    return Ok(LifecycleOutcome::Rejected(
                        LifecycleRejection::IncarnationExhausted,
                    ));
                };
                self.open_physical_group(group_id, quota)?;
                self.poisoned.remove(&group_id);
                self.groups.insert(
                    group_id,
                    GroupSlot {
                        incarnation,
                        lifecycle: GroupLifecycle::Creating,
                        quota,
                        applied_index: rafter::LogIndex::ZERO,
                        value: 0,
                        sessions: BTreeMap::default(),
                    },
                );
                Ok(LifecycleOutcome::Created { incarnation })
            }
            GroupLifecycle::Tombstoned => Ok(LifecycleOutcome::Rejected(
                LifecycleRejection::GroupTombstoned,
            )),
            current_state => Ok(LifecycleOutcome::Rejected(LifecycleRejection::Conflict {
                current: current_state,
                requested: GroupLifecycle::Creating,
            })),
        }
    }

    fn recover(&mut self, group_id: GroupId) -> Result<LifecycleOutcome, AdapterError> {
        let Some(slot) = self.groups.get(&group_id) else {
            return Ok(LifecycleOutcome::Rejected(LifecycleRejection::GroupUnknown));
        };
        let incarnation = slot.incarnation;
        match slot.lifecycle {
            GroupLifecycle::Recovering => Ok(LifecycleOutcome::Idempotent {
                state: GroupLifecycle::Recovering,
                incarnation,
            }),
            GroupLifecycle::Tombstoned => Ok(LifecycleOutcome::Rejected(
                LifecycleRejection::GroupTombstoned,
            )),
            GroupLifecycle::Creating => {
                self.recover_group(group_id)?;
                Ok(LifecycleOutcome::Applied {
                    from: GroupLifecycle::Creating,
                    to: GroupLifecycle::Recovering,
                    incarnation,
                })
            }
            current => Ok(LifecycleOutcome::Rejected(LifecycleRejection::Conflict {
                current,
                requested: GroupLifecycle::Recovering,
            })),
        }
    }

    fn serve(&mut self, group_id: GroupId) -> Result<LifecycleOutcome, AdapterError> {
        let Some(slot) = self.groups.get(&group_id) else {
            return Ok(LifecycleOutcome::Rejected(LifecycleRejection::GroupUnknown));
        };
        let incarnation = slot.incarnation;
        match slot.lifecycle {
            GroupLifecycle::Serving => Ok(LifecycleOutcome::Idempotent {
                state: GroupLifecycle::Serving,
                incarnation,
            }),
            GroupLifecycle::Tombstoned => Ok(LifecycleOutcome::Rejected(
                LifecycleRejection::GroupTombstoned,
            )),
            GroupLifecycle::Recovering => {
                self.serve_group(group_id)?;
                Ok(LifecycleOutcome::Applied {
                    from: GroupLifecycle::Recovering,
                    to: GroupLifecycle::Serving,
                    incarnation,
                })
            }
            current => Ok(LifecycleOutcome::Rejected(LifecycleRejection::Conflict {
                current,
                requested: GroupLifecycle::Serving,
            })),
        }
    }

    fn drain(&mut self, group_id: GroupId) -> LifecycleOutcome {
        let Some(slot) = self.groups.get_mut(&group_id) else {
            return LifecycleOutcome::Rejected(LifecycleRejection::GroupUnknown);
        };
        match slot.lifecycle {
            GroupLifecycle::Draining => LifecycleOutcome::Idempotent {
                state: GroupLifecycle::Draining,
                incarnation: slot.incarnation,
            },
            GroupLifecycle::Tombstoned => {
                LifecycleOutcome::Rejected(LifecycleRejection::GroupTombstoned)
            }
            current @ (GroupLifecycle::Creating
            | GroupLifecycle::Recovering
            | GroupLifecycle::Serving) => {
                slot.lifecycle = GroupLifecycle::Draining;
                LifecycleOutcome::Applied {
                    from: current,
                    to: GroupLifecycle::Draining,
                    incarnation: slot.incarnation,
                }
            }
            current => LifecycleOutcome::Rejected(LifecycleRejection::Conflict {
                current,
                requested: GroupLifecycle::Draining,
            }),
        }
    }

    fn remove(&mut self, group_id: GroupId) -> LifecycleOutcome {
        let Some(slot) = self.groups.get(&group_id) else {
            return LifecycleOutcome::Rejected(LifecycleRejection::GroupUnknown);
        };
        if slot.lifecycle == GroupLifecycle::Removed {
            return LifecycleOutcome::Idempotent {
                state: GroupLifecycle::Removed,
                incarnation: slot.incarnation,
            };
        }
        if slot.lifecycle == GroupLifecycle::Tombstoned {
            return LifecycleOutcome::Rejected(LifecycleRejection::GroupTombstoned);
        }
        if slot.lifecycle != GroupLifecycle::Draining {
            return LifecycleOutcome::Rejected(LifecycleRejection::Conflict {
                current: slot.lifecycle,
                requested: GroupLifecycle::Removed,
            });
        }
        let incarnation = slot.incarnation;
        match self.host.remove_group(&group_id) {
            Err(RemoveError::Queued { items, .. }) => {
                let pending = u32::try_from(items).unwrap_or(u32::MAX);
                LifecycleOutcome::Rejected(LifecycleRejection::QueueNotDrained { pending })
            }
            Err(RemoveError::InFlight(_)) => {
                LifecycleOutcome::Rejected(LifecycleRejection::QueueNotDrained { pending: 1 })
            }
            Ok(_) => {
                self.peers.remove(&(group_id, NodeId(2)));
                self.peers.remove(&(group_id, NodeId(3)));
                self.service_delays.remove(&group_id);
                if let Some(slot) = self.groups.get_mut(&group_id) {
                    slot.lifecycle = GroupLifecycle::Removed;
                    slot.sessions.clear();
                }
                LifecycleOutcome::Applied {
                    from: GroupLifecycle::Draining,
                    to: GroupLifecycle::Removed,
                    incarnation,
                }
            }
        }
    }

    fn tombstone(&mut self, group_id: GroupId) -> LifecycleOutcome {
        let Some(slot) = self.groups.get_mut(&group_id) else {
            return LifecycleOutcome::Rejected(LifecycleRejection::GroupUnknown);
        };
        match slot.lifecycle {
            GroupLifecycle::Tombstoned => LifecycleOutcome::Idempotent {
                state: GroupLifecycle::Tombstoned,
                incarnation: slot.incarnation,
            },
            GroupLifecycle::Removed => {
                slot.lifecycle = GroupLifecycle::Tombstoned;
                LifecycleOutcome::Applied {
                    from: GroupLifecycle::Removed,
                    to: GroupLifecycle::Tombstoned,
                    incarnation: slot.incarnation,
                }
            }
            current => LifecycleOutcome::Rejected(LifecycleRejection::Conflict {
                current,
                requested: GroupLifecycle::Tombstoned,
            }),
        }
    }
}
