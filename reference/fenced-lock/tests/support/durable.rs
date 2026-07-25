//! Durable lock applications for the deterministic three-node driver.
//!
//! Each replica owns a directory under one scratch root, so a restart reopens
//! that replica's own slot files and nothing else. Arming a fault plan here is
//! how a replication test interrupts one replica's transaction: the plan
//! travels with the store the next opening builds, so the other two replicas
//! are unaffected and the injection stays deterministic.

use std::{
    collections::{BTreeMap, BTreeSet},
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
    /// Replicas a scenario has deliberately interrupted at some point.
    ///
    /// A recovery report is only evidence if something reads it, and the
    /// strongest thing this factory can say is that a replica nobody
    /// interrupted came back with nothing to report. Remembering which replicas
    /// were armed is what makes that assertion possible without also failing
    /// the crash tests, whose whole purpose is to produce residue.
    interrupted: BTreeSet<NodeId>,
}

impl DurableLockApps {
    /// Builds a factory that keeps every replica's store under `root`.
    pub fn new(root: &Path, config: LockConfig) -> Self {
        Self {
            root: root.to_path_buf(),
            config,
            armed: BTreeMap::new(),
            interrupted: BTreeSet::new(),
        }
    }

    /// Arms `node_id`'s next store with `plan`.
    ///
    /// The plan applies from the next opening onward, which for a running
    /// replica means its next transaction.
    pub fn arm(&mut self, node_id: NodeId, plan: FaultPlan) {
        self.interrupted.insert(node_id);
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

        // The recovery report is asserted on rather than dropped. A replica no
        // scenario ever interrupted has no business finding a damaged slot, and
        // a driver that let one through would hide exactly the class of bug the
        // report exists to expose — including a one-generation rollback, which
        // costs a fencing high-water mark.
        let recovery = *store.recovery();
        assert!(
            recovery.is_clean() || self.interrupted.contains(&node_id),
            "replica {} recovered from a damaged slot no scenario put there: {recovery:?}",
            node_id.0
        );
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
