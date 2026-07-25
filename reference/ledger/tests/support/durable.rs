//! Durable ledger applications for the deterministic three-node driver.
//!
//! Each replica owns a directory under one scratch root, so a restart reopens
//! that replica's own journal and nothing else. Arming a fault plan here is how
//! a replication test interrupts one replica's transaction: the plan travels
//! with the store the next opening builds, so the other two replicas are
//! unaffected and the injection stays deterministic.

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use rafter::NodeId;
use rafter_reference_ledger::{
    store::{FaultPlan, LedgerStore},
    DurableLedgerStateMachine, LedgerConfig,
};

use crate::cluster::LedgerApps;

/// Applications whose ledgers live in one journal per replica.
#[derive(Clone, Debug)]
pub struct DurableLedgerApps {
    root: PathBuf,
    config: LedgerConfig,
    armed: BTreeMap<NodeId, FaultPlan>,
}

impl DurableLedgerApps {
    /// Builds a factory that keeps every replica's journal under `root`.
    pub fn new(root: &Path, config: LedgerConfig) -> Self {
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

    /// Returns the directory holding one replica's journal.
    pub fn directory(&self, node_id: NodeId) -> PathBuf {
        self.root.join(format!("node-{}", node_id.0))
    }

    fn open_store(&mut self, node_id: NodeId) -> DurableLedgerStateMachine {
        let plan = self.armed.remove(&node_id).unwrap_or_else(FaultPlan::none);
        let directory = self.directory(node_id);
        let store = LedgerStore::open_with_faults(&directory, self.config, plan.clone())
            .unwrap_or_else(|error| {
                panic!(
                    "replica {} could not open its ledger store at {} under {plan}: {error}",
                    node_id.0,
                    directory.display()
                )
            });
        DurableLedgerStateMachine::new(store)
    }
}

impl LedgerApps for DurableLedgerApps {
    type App = DurableLedgerStateMachine;

    fn open(&mut self, node_id: NodeId) -> Self::App {
        self.open_store(node_id)
    }

    /// A restarting replica drops its open journal and recovers from the file.
    ///
    /// Dropping `retired` is the whole point: everything the new incarnation
    /// knows, it read back from disk.
    fn reopen(&mut self, node_id: NodeId, retired: Self::App) -> Self::App {
        drop(retired);
        self.open_store(node_id)
    }
}
