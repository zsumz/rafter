//! Conservative conversion from Rust paths to same-source call candidates.

use std::collections::BTreeSet;

use syn::Expr;

use super::LocalCallResolver;
use crate::verification::detector::source::function_index::{
    path_syntax::{expression_function_id, expression_path},
    CallTarget, FunctionId,
};

impl LocalCallResolver {
    pub(in crate::verification::detector::source) fn self_target(
        &self,
        expression: &Expr,
        self_type: Option<&[String]>,
    ) -> Option<CallTarget> {
        let self_type = self_type?;
        let path = expression_path(expression)?;
        if path.qself.is_some() || path.path.leading_colon.is_some() {
            return None;
        }
        let segments = path
            .path
            .segments
            .iter()
            .map(|segment| segment.ident.to_string())
            .collect::<Vec<_>>();
        let [first, name] = segments.as_slice() else {
            return None;
        };
        (first == "Self").then(|| {
            self.classify_target(CallTarget {
                candidates: vec![FunctionId {
                    module: self_type.to_vec(),
                    name: name.clone(),
                }],
                name: name.clone(),
                opaque_local_module: false,
                imprecise_dispatch: false,
            })
        })
    }

    pub(in crate::verification::detector::source) fn call_target(
        &self,
        expression: &Expr,
        current_module: &[String],
    ) -> Option<CallTarget> {
        let path = expression_path(expression)?;
        if path.path.segments.is_empty() {
            return None;
        }
        let name = path.path.segments.last()?.ident.to_string();
        if path.qself.is_some() {
            return Some(CallTarget {
                candidates: Vec::new(),
                name,
                opaque_local_module: true,
                imprecise_dispatch: false,
            });
        }
        if path.path.leading_colon.is_some() {
            let segments = path
                .path
                .segments
                .iter()
                .map(|segment| segment.ident.to_string())
                .collect::<Vec<_>>();
            if segments
                .first()
                .is_some_and(|root| self.crate_aliases.contains(root))
            {
                let mut local_segments = vec!["crate".to_owned()];
                local_segments.extend(segments.into_iter().skip(1));
                let mut target = self.segments_target(&local_segments, current_module, name);
                target.opaque_local_module = true;
                return Some(target);
            }
            return Some(CallTarget {
                candidates: Vec::new(),
                name,
                opaque_local_module: false,
                imprecise_dispatch: false,
            });
        }
        let segments = path
            .path
            .segments
            .iter()
            .map(|segment| segment.ident.to_string())
            .collect::<Vec<_>>();
        let name = segments.last()?.clone();
        Some(self.segments_target(&segments, current_module, name))
    }

    pub(in crate::verification::detector::source) fn named_target(
        &self,
        name: &str,
        current_module: &[String],
    ) -> CallTarget {
        self.segments_target(&[name.to_owned()], current_module, name.to_owned())
    }

    pub(in crate::verification::detector::source) fn explicit_target(
        &self,
        expression: &Expr,
        current_module: &[String],
    ) -> Option<CallTarget> {
        let path = expression_path(expression)?;
        if path.qself.is_some()
            || path.path.leading_colon.is_some()
            || path.path.segments.len() != 1
        {
            return None;
        }
        let name = path.path.segments.first()?.ident.to_string();
        let candidates = self
            .explicit
            .get(&(current_module.to_vec(), name.clone()))?
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        Some(
            self.classify_target(CallTarget {
                candidates: self
                    .expand_import_candidates(candidates)
                    .into_iter()
                    .collect(),
                name,
                opaque_local_module: false,
                imprecise_dispatch: false,
            }),
        )
    }

    pub(in crate::verification::detector::source) fn classify_target(
        &self,
        mut target: CallTarget,
    ) -> CallTarget {
        target.opaque_local_module |= target.candidates.iter().any(|candidate| {
            self.out_of_line_modules
                .iter()
                .any(|module| candidate.module.starts_with(module))
                || (self.target_modules.contains(&candidate.module)
                    && self.target_functions.contains(&candidate.name))
        });
        target
    }

    pub(in crate::verification::detector::source) fn can_name_reviewed_function(
        &self,
        target: &CallTarget,
    ) -> bool {
        target
            .candidates
            .iter()
            .any(|candidate| self.target_functions.contains(&candidate.name))
    }

    pub(super) fn segments_target(
        &self,
        segments: &[String],
        current_module: &[String],
        name: String,
    ) -> CallTarget {
        let mut candidates = BTreeSet::new();
        if segments.len() == 1 {
            let local = FunctionId {
                module: current_module.to_vec(),
                name: name.clone(),
            };
            if let Some(imports) = self.explicit.get(&(current_module.to_vec(), name.clone())) {
                candidates.extend(imports.iter().cloned());
            } else if self.local_functions.contains(&local) {
                candidates.insert(local);
            } else if let Some(globs) = self.globs.get(current_module) {
                candidates.extend(globs.iter().cloned().map(|module| FunctionId {
                    module,
                    name: name.clone(),
                }));
            } else {
                candidates.insert(local);
            }
        } else {
            if let Some(candidate) = expression_function_id(segments, current_module) {
                candidates.insert(candidate);
            }
            if let Some((alias, remainder)) = segments.split_first() {
                if let Some(modules) = self
                    .module_aliases
                    .get(&(current_module.to_vec(), alias.clone()))
                {
                    if let Some((function, nested)) = remainder.split_last() {
                        candidates.extend(modules.iter().cloned().map(|mut module| {
                            module.extend(nested.iter().cloned());
                            FunctionId {
                                module,
                                name: function.clone(),
                            }
                        }));
                    }
                }
            }
        }
        let candidates = self
            .expand_import_candidates(candidates)
            .into_iter()
            .collect::<Vec<_>>();
        self.classify_target(CallTarget {
            candidates,
            name,
            opaque_local_module: false,
            imprecise_dispatch: false,
        })
    }

    fn expand_import_candidates(&self, candidates: BTreeSet<FunctionId>) -> BTreeSet<FunctionId> {
        let mut expanded = BTreeSet::new();
        let mut pending = candidates.into_iter().collect::<Vec<_>>();
        while let Some(candidate) = pending.pop() {
            if !expanded.insert(candidate.clone()) {
                continue;
            }
            if let Some(imports) = self
                .explicit
                .get(&(candidate.module.clone(), candidate.name.clone()))
            {
                pending.extend(imports.iter().cloned());
            }
            if let Some(globs) = self.globs.get(&candidate.module) {
                pending.extend(globs.iter().cloned().map(|module| FunctionId {
                    module,
                    name: candidate.name.clone(),
                }));
            }
        }
        expanded
    }
}
