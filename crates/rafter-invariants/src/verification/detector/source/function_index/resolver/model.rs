//! Indexed declarations and imports used to resolve same-source calls.

use std::collections::{BTreeMap, BTreeSet};

use super::super::FunctionId;

#[derive(Clone, Default)]
pub(in crate::verification::detector::source) struct LocalCallResolver {
    pub(super) explicit: BTreeMap<(Vec<String>, String), Vec<FunctionId>>,
    pub(super) globs: BTreeMap<Vec<String>, Vec<Vec<String>>>,
    pub(super) module_aliases: BTreeMap<(Vec<String>, String), Vec<Vec<String>>>,
    pub(super) crate_aliases: BTreeSet<String>,
    pub(super) local_functions: BTreeSet<FunctionId>,
    pub(super) local_methods: BTreeSet<FunctionId>,
    pub(super) local_trait_methods: BTreeSet<FunctionId>,
    pub(super) deref_targets: BTreeMap<Vec<String>, Vec<Vec<String>>>,
    pub(super) struct_fields: BTreeMap<Vec<String>, BTreeMap<String, Vec<String>>>,
    pub(super) out_of_line_modules: BTreeSet<Vec<String>>,
    pub(super) target_functions: BTreeSet<String>,
    pub(super) target_modules: BTreeSet<Vec<String>>,
}
