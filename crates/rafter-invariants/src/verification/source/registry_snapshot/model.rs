//! Registry snapshot receipts and intermediate materialization facts.

use std::{collections::BTreeMap, path::PathBuf};

use serde::{Deserialize, Serialize};

use super::FilePlan;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(in crate::verification) struct RegistryReceipt {
    pub(in crate::verification) lock_sha256: String,
    pub(in crate::verification) package_count: usize,
    pub(in crate::verification) archive_bytes: u64,
    pub(in crate::verification) expanded_bytes: u64,
    pub(in crate::verification) entries: u64,
    pub(in crate::verification) materialization_sha256: String,
}

pub(super) struct RegistryMaterialization {
    pub(super) plans: BTreeMap<PathBuf, FilePlan>,
    pub(super) archive_bytes: u64,
    pub(super) expanded_bytes: u64,
    pub(super) entries: u64,
    pub(super) materialization_sha256: String,
}
