//! Structurally append-only storage for execution witnesses.
//!
//! Production code reaches this ledger only through `push`. The test-only
//! corruption hooks bump a revision so the incremental AP-02 verifier can
//! detect rewritten consumed history without rescanning the whole prefix.

use std::hash::{Hash, Hasher};

use super::ExecutionWitness;

/// Structurally append-only owner of execution witnesses.
///
/// Production code can only append. Test-only corruption hooks advance a
/// revision so the incremental verifier can detect rewritten consumed history
/// without rescanning every payload-rich prefix after each transition.
#[derive(Clone, Debug, Default)]
pub(crate) struct ExecutionLedger {
    witnesses: Vec<ExecutionWitness>,
    rewrite_revision: u64,
}

impl Hash for ExecutionLedger {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.witnesses.hash(state);
        self.rewrite_revision.hash(state);
    }
}

impl ExecutionLedger {
    pub(crate) fn push(&mut self, witness: ExecutionWitness) {
        self.witnesses.push(witness);
    }

    pub(crate) fn as_slice(&self) -> &[ExecutionWitness] {
        &self.witnesses
    }

    pub(crate) const fn rewrite_revision(&self) -> u64 {
        self.rewrite_revision
    }

    #[cfg(test)]
    pub(crate) fn from_witnesses(witnesses: Vec<ExecutionWitness>) -> Self {
        let mut ledger = Self::default();
        for witness in witnesses {
            ledger.push(witness);
        }
        ledger
    }

    #[cfg(test)]
    pub(crate) fn rewrite(&mut self, index: usize, witness: ExecutionWitness) {
        self.witnesses[index] = witness;
        self.rewrite_revision = self.rewrite_revision.saturating_add(1);
    }

    #[cfg(test)]
    pub(crate) fn swap(&mut self, first: usize, second: usize) {
        self.witnesses.swap(first, second);
        self.rewrite_revision = self.rewrite_revision.saturating_add(1);
    }
}
