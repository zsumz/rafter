//! Durable ledger applications for the deterministic three-node driver.
//!
//! Each replica owns a directory under one scratch root, so a restart reopens
//! that replica's own journal and nothing else. Arming a fault plan here is how
//! a replication test interrupts one replica's transaction: the plan travels
//! with the store the next opening builds, so the other two replicas are
//! unaffected and the injection stays deterministic.

use std::{
    collections::{BTreeMap, BTreeSet},
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
    /// Replicas a scenario has deliberately interrupted at some point.
    ///
    /// A recovery report is only evidence if something reads it, and the
    /// strongest thing this factory can say is that a replica nobody
    /// interrupted came back with nothing to report. Remembering which replicas
    /// were armed is what makes that assertion possible without also failing
    /// the crash tests, whose whole purpose is to produce residue.
    interrupted: BTreeSet<NodeId>,
}

impl DurableLedgerApps {
    /// Builds a factory that keeps every replica's journal under `root`.
    pub fn new(root: &Path, config: LedgerConfig) -> Self {
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

        // The recovery report is asserted on rather than dropped. A replica no
        // scenario ever interrupted has no business finding residue in its own
        // journal, and a driver that let one through would hide exactly the
        // class of bug the report exists to expose.
        let recovery = *store.recovery();
        assert!(
            recovery.repair().is_none(),
            "replica {} repaired its journal, which `open_with_faults` must never do: {recovery:?}",
            node_id.0
        );
        assert!(
            recovery.is_clean() || self.interrupted.contains(&node_id),
            "replica {} recovered from residue no scenario put there: {recovery:?}",
            node_id.0
        );
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
