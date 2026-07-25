//! Durable lock applications for the deterministic three-node driver.
//!
//! Each replica owns a directory under one scratch root, so a restart reopens
//! that replica's own slot files and nothing else. Arming a fault plan here is
//! how a replication test interrupts one replica's transaction: the plan
//! travels with the store the next opening builds, so the other two replicas
//! are unaffected and the injection stays deterministic.

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use rafter::NodeId;
use rafter_reference_fenced_lock::{
    store::{FaultPlan, LockStore},
    DurableLockStateMachine, LockConfig,
};

use crate::cluster::LockApps;

/// Applications whose lock services live in one pair of slot files per replica.
#[derive(Clone, Debug)]
pub struct DurableLockApps {
    root: PathBuf,
    config: LockConfig,
    armed: BTreeMap<NodeId, FaultPlan>,
}

impl DurableLockApps {
    /// Builds a factory that keeps every replica's store under `root`.
    pub fn new(root: &Path, config: LockConfig) -> Self {
        Self {
            root: root.to_path_buf(),
            config,
            armed: BTreeMap::new(),
        }
    }

    /// Arms `node_id`'s next store with `plan`.
    ///
    /// The plan applies from the next opening onward, which for a running
    /// replica means its next transaction.
    pub fn arm(&mut self, node_id: NodeId, plan: FaultPlan) {
        self.armed.insert(node_id, plan);
    }

    /// Returns the directory holding one replica's slot files.
    pub fn directory(&self, node_id: NodeId) -> PathBuf {
        self.root.join(format!("node-{}", node_id.0))
    }

    fn open_store(&mut self, node_id: NodeId) -> DurableLockStateMachine {
        let plan = self.armed.remove(&node_id).unwrap_or_else(FaultPlan::none);
        let directory = self.directory(node_id);
        let store = LockStore::open_with_faults(&directory, self.config, plan.clone())
            .unwrap_or_else(|error| {
                panic!(
                    "replica {} could not open its lock store at {} under {plan}: {error}",
                    node_id.0,
                    directory.display()
                )
            });
        DurableLockStateMachine::new(store)
    }
}

impl LockApps for DurableLockApps {
    type App = DurableLockStateMachine;

    /// A durable replica can fail a step, and a crash test's whole purpose is
    /// to make it. The failure is recorded rather than fatal so the surviving
    /// quorum keeps running and the crashed replica can be restarted.
    const APPLICATIONS_CAN_FAIL: bool = true;

    fn open(&mut self, node_id: NodeId) -> Self::App {
        self.open_store(node_id)
    }

    /// A restarting replica drops its open store and recovers from the files.
    ///
    /// Dropping `retired` is the whole point: everything the new incarnation
    /// knows — every lock, every session, and every fencing high-water mark —
    /// it read back from disk.
    fn reopen(&mut self, node_id: NodeId, retired: Self::App) -> Self::App {
        drop(retired);
        self.open_store(node_id)
    }
}
