//! Declared and inferred local type identities used for receiver resolution.

use std::collections::{BTreeMap, BTreeSet};

use syn::{Expr, Member, Type};

use super::LocalCallResolver;
use crate::verification::detector::source::{
    function_index::path_syntax::{impl_type_module, peel_type},
    syntax::unqualified_expression_name,
};

impl LocalCallResolver {
    pub(in crate::verification::detector::source) fn value_type_module(
        &self,
        ty: &Type,
        current_module: &[String],
    ) -> Option<Vec<String>> {
        let Type::Path(path) = peel_type(ty) else {
            return impl_type_module(ty, current_module);
        };
        if let Some(module) = self.absolute_crate_alias_type_module(path) {
            return Some(module);
        }
        if path.qself.is_some()
            || path.path.leading_colon.is_some()
            || path.path.segments.len() != 1
        {
            return impl_type_module(ty, current_module);
        }
        let name = path.path.segments.first()?.ident.to_string();
        let candidates = self
            .named_target(&name, current_module)
            .candidates
            .iter()
            .map(|candidate| {
                let mut module = candidate.module.clone();
                module.push(candidate.name.clone());
                module
            })
            .filter(|candidate| {
                self.local_methods
                    .iter()
                    .any(|method| method.module == *candidate)
            })
            .collect::<BTreeSet<_>>();
        match candidates.into_iter().collect::<Vec<_>>().as_slice() {
            [candidate] => Some(candidate.clone()),
            _ => impl_type_module(ty, current_module),
        }
    }

    pub(in crate::verification::detector::source) fn value_expression_type_module(
        &self,
        expression: &Expr,
        current_module: &[String],
    ) -> Option<Vec<String>> {
        let target = self.call_target(expression, current_module)?;
        let candidates = target
            .candidates
            .iter()
            .map(|candidate| {
                let mut module = candidate.module.clone();
                module.push(candidate.name.clone());
                module
            })
            .filter(|candidate| {
                self.local_methods
                    .iter()
                    .any(|method| method.module == *candidate)
            })
            .collect::<BTreeSet<_>>();
        match candidates.into_iter().collect::<Vec<_>>().as_slice() {
            [candidate] => Some(candidate.clone()),
            _ => None,
        }
    }

    pub(in crate::verification::detector::source) fn declared_type_module(
        &self,
        ty: &Type,
        current_module: &[String],
    ) -> Option<Vec<String>> {
        let Type::Path(path) = peel_type(ty) else {
            return impl_type_module(ty, current_module);
        };
        if let Some(module) = self.absolute_crate_alias_type_module(path) {
            return Some(module);
        }
        if path.qself.is_some()
            || path.path.leading_colon.is_some()
            || path.path.segments.len() != 1
        {
            return impl_type_module(ty, current_module);
        }
        let name = path.path.segments.first()?.ident.to_string();
        let imported = self
            .explicit
            .get(&(current_module.to_vec(), name))
            .into_iter()
            .flatten()
            .cloned()
            .collect::<BTreeSet<_>>();
        match imported.into_iter().collect::<Vec<_>>().as_slice() {
            [candidate] => {
                let mut module = candidate.module.clone();
                module.push(candidate.name.clone());
                Some(module)
            }
            _ => impl_type_module(ty, current_module),
        }
    }

    pub(in crate::verification::detector::source) fn field_expression_type_module(
        &self,
        expression: &Expr,
        current_module: &[String],
        self_type: Option<&[String]>,
        value_types: &BTreeMap<String, Vec<String>>,
    ) -> Option<Vec<String>> {
        match expression {
            Expr::Field(field) => {
                let base = self.field_expression_type_module(
                    &field.base,
                    current_module,
                    self_type,
                    value_types,
                )?;
                let Member::Named(member) = &field.member else {
                    return None;
                };
                self.struct_fields
                    .get(&base)?
                    .get(&member.to_string())
                    .cloned()
            }
            Expr::Group(group) => self.field_expression_type_module(
                &group.expr,
                current_module,
                self_type,
                value_types,
            ),
            Expr::Paren(paren) => self.field_expression_type_module(
                &paren.expr,
                current_module,
                self_type,
                value_types,
            ),
            Expr::Reference(reference) => self.field_expression_type_module(
                &reference.expr,
                current_module,
                self_type,
                value_types,
            ),
            Expr::Path(_) => {
                let name = unqualified_expression_name(expression)?;
                if name == "self" {
                    return self_type.map(<[String]>::to_vec);
                }
                value_types
                    .get(&name)
                    .cloned()
                    .or_else(|| self.value_expression_type_module(expression, current_module))
            }
            _ => None,
        }
    }

    fn absolute_crate_alias_type_module(&self, path: &syn::TypePath) -> Option<Vec<String>> {
        if path.qself.is_some() || path.path.leading_colon.is_none() {
            return None;
        }
        let mut segments = path
            .path
            .segments
            .iter()
            .map(|segment| segment.ident.to_string())
            .collect::<Vec<_>>();
        if segments.len() < 2 || !self.crate_aliases.contains(segments.first()?) {
            return None;
        }
        segments.remove(0);
        Some(segments)
    }
}
