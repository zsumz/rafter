//! Local method dispatch, including conservative trait, factory, and Deref handling.

use std::collections::BTreeSet;

use syn::{Expr, ItemImpl};

use super::LocalCallResolver;
use crate::verification::detector::source::function_index::{
    path_syntax::expression_path, CallTarget, FunctionId,
};

impl LocalCallResolver {
    pub(in crate::verification::detector::source) fn method_target(
        &self,
        receiver: &Expr,
        method: &str,
        current_module: &[String],
        known_receiver_type: Option<&[String]>,
    ) -> CallTarget {
        let receiver_has_path_syntax = expression_path(receiver).is_some();
        let mut target = if let Some(receiver_type) = known_receiver_type {
            CallTarget {
                candidates: self.method_candidates_for_type(receiver_type, method),
                name: method.to_owned(),
                opaque_local_module: false,
                imprecise_dispatch: false,
            }
        } else {
            expression_path(receiver).map_or_else(
                || CallTarget {
                    candidates: Vec::new(),
                    name: method.to_owned(),
                    opaque_local_module: false,
                    imprecise_dispatch: false,
                },
                |path| {
                    let mut segments = path
                        .path
                        .segments
                        .iter()
                        .map(|segment| segment.ident.to_string())
                        .collect::<Vec<_>>();
                    segments.push(method.to_owned());
                    self.segments_target(&segments, current_module, method.to_owned())
                },
            )
        };
        let plausible_local_methods = self
            .local_methods
            .iter()
            .filter(|candidate| {
                candidate.name == method
                    && candidate.module.split_last().is_some_and(|(_, parent)| {
                        parent == current_module || current_module.starts_with(parent)
                    })
            })
            .cloned()
            .collect::<BTreeSet<_>>();
        let plausible_trait_methods = self
            .local_trait_methods
            .iter()
            .filter(|candidate| {
                candidate.name == method
                    && (candidate.module == current_module
                        || current_module.starts_with(&candidate.module))
            })
            .cloned()
            .collect::<BTreeSet<_>>();
        let plausible_factory_method =
            self.factory_receiver_may_dispatch_locally(receiver, method, current_module);
        let mut resolves_local_method = target
            .candidates
            .iter()
            .any(|candidate| self.local_methods.contains(candidate));
        if !resolves_local_method
            && known_receiver_type.is_none()
            && receiver_has_path_syntax
            && plausible_trait_methods.is_empty()
            && !plausible_local_methods.is_empty()
        {
            target
                .candidates
                .extend(plausible_local_methods.iter().cloned());
            target.imprecise_dispatch = true;
            resolves_local_method = true;
        }
        if !resolves_local_method
            && receiver_has_path_syntax
            && (!plausible_local_methods.is_empty() || !plausible_trait_methods.is_empty())
        {
            target.opaque_local_module = true;
        }
        if plausible_factory_method && !resolves_local_method {
            target.opaque_local_module = true;
        }
        target
    }

    fn method_candidates_for_type(
        &self,
        receiver_type: &[String],
        method: &str,
    ) -> Vec<FunctionId> {
        let mut candidates = BTreeSet::new();
        let mut visited = BTreeSet::new();
        let mut pending = vec![receiver_type.to_vec()];
        while let Some(module) = pending.pop() {
            if !visited.insert(module.clone()) {
                continue;
            }
            candidates.insert(FunctionId {
                module: module.clone(),
                name: method.to_owned(),
            });
            if let Some(targets) = self.deref_targets.get(&module) {
                pending.extend(targets.iter().cloned());
            }
        }
        candidates.into_iter().collect()
    }

    pub(super) fn is_deref_impl(&self, item: &ItemImpl, current_module: &[String]) -> bool {
        let Some((_, path, _)) = &item.trait_ else {
            return false;
        };
        let Some(name) = path
            .segments
            .last()
            .map(|segment| segment.ident.to_string())
        else {
            return false;
        };
        if name == "Deref" {
            return true;
        }
        path.leading_colon.is_none()
            && path.segments.len() == 1
            && self
                .explicit
                .get(&(current_module.to_vec(), name))
                .into_iter()
                .flatten()
                .any(|candidate| candidate.name == "Deref")
    }

    fn factory_receiver_may_dispatch_locally(
        &self,
        receiver: &Expr,
        method: &str,
        current_module: &[String],
    ) -> bool {
        let Expr::Call(call) = receiver else {
            return false;
        };
        let Some(factory) = self.call_target(&call.func, current_module) else {
            return false;
        };
        self.local_methods
            .iter()
            .chain(&self.local_trait_methods)
            .any(|candidate| {
                candidate.name == method
                    && candidate.module.split_last().is_some_and(|(_, parent)| {
                        factory
                            .candidates
                            .iter()
                            .any(|function| function.module == parent)
                    })
            })
    }
}
