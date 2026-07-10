use rafter::{BootstrapLogEntry, BootstrapState, LogIndex};

/// Simulated disk recovery image after a crash at a modeled persistence point.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirtyRecovery {
    pub fault: DiskFault,
    pub bootstrap: BootstrapState,
}

/// Disk-fault shape applied to a node's captured bootstrap state.
///
/// This enum is exhaustive because the simulator models a closed set of
/// disk-recovery shapes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DiskFault {
    CrashAfterIo {
        completed_io: usize,
        durable_through: LogIndex,
    },
    TornTail {
        torn_index: LogIndex,
    },
    LostUnfsyncedSuffix {
        durable_through: LogIndex,
    },
    HardStateLogReorder {
        durable_log_through: LogIndex,
    },
}

/// Fault-injecting durable image for the deterministic simulator.
///
/// The model works at the simulator's restart boundary: it captures the
/// node's clean [`BootstrapState`] and emits alternate bootstrap states that
/// represent dirty disk recovery shapes. It deliberately does not add a
/// storage dependency to `rafter-sim`; tests re-open the simulated node through
/// [`crate::Cluster::restart_node_from_bootstrap_losing_application_state`]
/// and keep driving the running protocol, because these dirty images model
/// storage repair after application state may have been lost with the damaged
/// durable protocol state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FaultInjectingDisk {
    clean: BootstrapState,
}

impl FaultInjectingDisk {
    /// Captures a clean durable image for later fault injection.
    #[must_use]
    pub fn new(clean: BootstrapState) -> Self {
        Self { clean }
    }

    /// Returns recovery images for a crash after each modeled disk I/O.
    ///
    /// The write order is hard state, snapshot descriptor when present, log
    /// entries in order, then committed-floor metadata. Each prefix is a state
    /// the simulator can reopen and drive.
    #[must_use]
    pub fn crash_after_each_io(&self) -> Vec<DirtyRecovery> {
        let mut recoveries = Vec::new();
        let mut completed_io = 1;
        let mut image = BootstrapState {
            current_term: self.clean.current_term,
            voted_for: self.clean.voted_for,
            commit_index: LogIndex::ZERO,
            committed_configuration: None,
            snapshot: None,
            log: Vec::new(),
        };
        recoveries.push(DirtyRecovery {
            fault: DiskFault::CrashAfterIo {
                completed_io,
                durable_through: LogIndex::ZERO,
            },
            bootstrap: image.clone(),
        });

        if let Some(snapshot) = self.clean.snapshot.clone() {
            completed_io += 1;
            let durable_through = snapshot.metadata.last_included_index;
            image.snapshot = Some(snapshot);
            recoveries.push(DirtyRecovery {
                fault: DiskFault::CrashAfterIo {
                    completed_io,
                    durable_through,
                },
                bootstrap: image.clone(),
            });
        }

        for entry in &self.clean.log {
            completed_io += 1;
            image.log.push(entry.clone());
            recoveries.push(DirtyRecovery {
                fault: DiskFault::CrashAfterIo {
                    completed_io,
                    durable_through: entry.index,
                },
                bootstrap: image.clone(),
            });
        }

        if self.clean.commit_index != LogIndex::ZERO || self.clean.committed_configuration.is_some()
        {
            completed_io += 1;
            image.commit_index = self.clean.commit_index;
            image.committed_configuration = self.clean.committed_configuration;
            recoveries.push(DirtyRecovery {
                fault: DiskFault::CrashAfterIo {
                    completed_io,
                    durable_through: self.last_durable_index(),
                },
                bootstrap: image,
            });
        }

        recoveries
    }

    /// Drops the last durable log entry, modeling a torn or rejected tail
    /// record found during recovery.
    #[must_use]
    pub fn torn_tail(&self) -> Option<DirtyRecovery> {
        let last = self.clean.log.last()?;
        Some(DirtyRecovery {
            fault: DiskFault::TornTail {
                torn_index: last.index,
            },
            bootstrap: self.with_log_through(LogIndex(last.index.0.saturating_sub(1))),
        })
    }

    /// Drops every log entry above `durable_through`.
    #[must_use]
    pub fn lost_unfsynced_suffix(&self, durable_through: LogIndex) -> DirtyRecovery {
        DirtyRecovery {
            fault: DiskFault::LostUnfsyncedSuffix { durable_through },
            bootstrap: self.with_log_through(durable_through),
        }
    }

    /// Persists hard state from the clean image while the log only survives
    /// through `durable_log_through`.
    ///
    /// This preserves term, vote, committed index, and committed
    /// configuration. If the committed hard-state fields point beyond the
    /// retained log or snapshot, reopening the image should fail with a typed
    /// bootstrap validation error.
    #[must_use]
    pub fn hard_state_log_reorder(&self, durable_log_through: LogIndex) -> DirtyRecovery {
        DirtyRecovery {
            fault: DiskFault::HardStateLogReorder {
                durable_log_through,
            },
            bootstrap: self.with_log_through_and_hard_state(durable_log_through),
        }
    }

    fn with_log_through_and_hard_state(&self, durable_through: LogIndex) -> BootstrapState {
        let mut bootstrap = self.with_log_through(durable_through);
        bootstrap.commit_index = self.clean.commit_index;
        bootstrap.committed_configuration = self.clean.committed_configuration;
        bootstrap
    }

    fn with_log_through(&self, durable_through: LogIndex) -> BootstrapState {
        let snapshot_floor = self
            .clean
            .snapshot
            .as_ref()
            .map_or(LogIndex::ZERO, |snapshot| {
                snapshot.metadata.last_included_index
            });
        let floor = durable_through.max(snapshot_floor);
        BootstrapState {
            current_term: self.clean.current_term,
            voted_for: self.clean.voted_for,
            commit_index: LogIndex::ZERO,
            committed_configuration: None,
            snapshot: self.clean.snapshot.clone(),
            log: self.log_through(floor),
        }
    }

    fn log_through(&self, durable_through: LogIndex) -> Vec<BootstrapLogEntry> {
        self.clean
            .log
            .iter()
            .filter(|entry| entry.index <= durable_through)
            .cloned()
            .collect()
    }

    fn last_durable_index(&self) -> LogIndex {
        self.clean.log.last().map_or_else(
            || {
                self.clean
                    .snapshot
                    .as_ref()
                    .map_or(LogIndex::ZERO, |snapshot| {
                        snapshot.metadata.last_included_index
                    })
            },
            |entry| entry.index,
        )
    }
}
