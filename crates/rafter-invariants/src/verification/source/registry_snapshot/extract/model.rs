//! Aggregate extraction budgets and completed package observations.

use std::{collections::BTreeMap, path::PathBuf, time::Instant};

use super::super::FilePlan;

#[derive(Clone, Copy)]
pub(in crate::verification::source::registry_snapshot) struct ExtractionBudget {
    pub(in crate::verification::source::registry_snapshot) expanded_bytes: u64,
    pub(in crate::verification::source::registry_snapshot) entries: u64,
    pub(in crate::verification::source::registry_snapshot) deadline: Instant,
}

impl ExtractionBudget {
    pub(super) fn check(self) -> Result<(), String> {
        if Instant::now() >= self.deadline {
            return Err("authenticated registry extraction deadline expired".to_owned());
        }
        Ok(())
    }
}

#[derive(Debug)]
pub(in crate::verification::source::registry_snapshot) struct ExtractedPackage {
    pub(in crate::verification::source::registry_snapshot) plans: BTreeMap<PathBuf, FilePlan>,
    pub(in crate::verification::source::registry_snapshot) expanded_bytes: u64,
    pub(in crate::verification::source::registry_snapshot) entries: u64,
}
