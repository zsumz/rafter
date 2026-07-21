//! Cargo target and compiler-message vocabulary.

use std::{collections::BTreeSet, path::PathBuf};

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct CargoTargetKey {
    pub(crate) package: String,
    pub(crate) kind: String,
    pub(crate) target: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct PreservedTestBinary {
    pub(super) sha256: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct EmittedTestExecutable {
    pub(crate) package_id: String,
    pub(crate) target: CargoTargetKey,
    pub(crate) executable: PathBuf,
    pub(crate) sha256: String,
}

pub(super) struct ParsedCompilerArtifact {
    pub(super) package_id: String,
    pub(super) executable: PathBuf,
}

#[derive(Default)]
pub(crate) struct CompilationEvidence {
    failed_execution_ids: BTreeSet<String>,
}

impl CompilationEvidence {
    pub(crate) fn record_failures(&mut self, execution_ids: BTreeSet<String>) {
        self.failed_execution_ids.extend(execution_ids);
    }

    pub(crate) fn failed_for(&self, execution_id: &str) -> bool {
        self.failed_execution_ids.contains(execution_id)
    }
}
