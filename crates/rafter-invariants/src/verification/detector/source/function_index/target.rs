//! Display and composition behavior for function call targets.

use super::{CallTarget, FunctionId};

impl std::fmt::Display for FunctionId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for segment in &self.module {
            write!(formatter, "{segment}::")?;
        }
        formatter.write_str(&self.name)
    }
}

impl CallTarget {
    pub(in crate::verification::detector::source) fn merge(mut self, other: Self) -> Self {
        self.candidates.extend(other.candidates);
        self.candidates.sort();
        self.candidates.dedup();
        self.opaque_local_module |= other.opaque_local_module;
        self.imprecise_dispatch |= other.imprecise_dispatch;
        self
    }

    pub(in crate::verification::detector::source) fn candidates(&self) -> &[FunctionId] {
        &self.candidates
    }

    pub(in crate::verification::detector::source) fn matches_any_name(
        &self,
        names: &[&str],
    ) -> bool {
        names.contains(&self.name.as_str())
            || self
                .candidates
                .iter()
                .any(|candidate| names.contains(&candidate.name.as_str()))
    }
}
