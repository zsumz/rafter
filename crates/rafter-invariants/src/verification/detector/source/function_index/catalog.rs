//! Function and value declarations indexed by exact source identity.

use std::collections::{BTreeMap, BTreeSet};

use super::{CallTarget, FunctionId};
use crate::verification::detector::source::FunctionFacts;

#[derive(Default)]
pub(in crate::verification::detector::source) struct FunctionIndex {
    pub(in crate::verification::detector::source) functions:
        BTreeMap<FunctionId, Vec<FunctionFacts>>,
    pub(in crate::verification::detector::source) values: BTreeSet<FunctionId>,
}

impl FunctionIndex {
    pub(in crate::verification::detector::source) fn extend(&mut self, other: Self) {
        for (id, mut functions) in other.functions {
            self.functions.entry(id).or_default().append(&mut functions);
        }
        self.values.extend(other.values);
    }

    pub(in crate::verification::detector::source) fn contains(&self, id: &FunctionId) -> bool {
        self.functions.contains_key(id)
    }

    pub(in crate::verification::detector::source) fn unique_exact(
        &self,
        id: &FunctionId,
    ) -> Result<Option<&FunctionFacts>, String> {
        match self.functions.get(id).map(Vec::as_slice) {
            None => Ok(None),
            Some([function]) => Ok(Some(function)),
            Some(functions) => Err(format!(
                "function `{id}` resolves to {} declarations",
                functions.len()
            )),
        }
    }

    pub(in crate::verification::detector::source) fn resolve_call(
        &self,
        target: &CallTarget,
    ) -> Result<Option<FunctionId>, String> {
        self.require_function_namespace(target)?;
        let matches = self.matching_functions(target);
        match matches.len() {
            0 => Ok(None),
            1 => Ok(matches.into_iter().next()),
            count => Err(format!(
                "call `{}` resolves to {count} same-source function declarations",
                target.name
            )),
        }
    }

    pub(in crate::verification::detector::source) fn require_function_namespace(
        &self,
        target: &CallTarget,
    ) -> Result<(), String> {
        let value_matches = target
            .candidates
            .iter()
            .filter(|candidate| self.values.contains(candidate))
            .collect::<Vec<_>>();
        if !value_matches.is_empty() {
            return Err(format!(
                "call `{}` can resolve to {} non-function value declarations",
                target.name,
                value_matches.len()
            ));
        }
        Ok(())
    }

    pub(in crate::verification::detector::source) fn matching_functions(
        &self,
        target: &CallTarget,
    ) -> BTreeSet<FunctionId> {
        target
            .candidates
            .iter()
            .filter(|candidate| self.contains(candidate))
            .cloned()
            .collect()
    }
}
