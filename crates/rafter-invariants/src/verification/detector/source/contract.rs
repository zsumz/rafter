//! Invocation-bound detector contract derived from authenticated source.

use std::collections::BTreeMap;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DetectorInvocationContract {
    pub(super) witnesses: BTreeMap<String, usize>,
    pub(super) registered_identity: String,
    source_graph_sha256: String,
}

impl DetectorInvocationContract {
    pub(super) fn new(registered_identity: String, source_graph_sha256: String) -> Self {
        Self {
            witnesses: BTreeMap::new(),
            registered_identity,
            source_graph_sha256,
        }
    }

    pub(crate) fn witnesses(&self) -> &BTreeMap<String, usize> {
        &self.witnesses
    }

    pub(crate) fn registered_identity(&self) -> &str {
        &self.registered_identity
    }

    pub(crate) fn source_graph_sha256(&self) -> &str {
        &self.source_graph_sha256
    }
}
