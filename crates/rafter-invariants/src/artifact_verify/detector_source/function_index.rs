use std::collections::{BTreeMap, BTreeSet};

use syn::{
    visit::Visit, Expr, File, ImplItem, ItemExternCrate, ItemFn, ItemImpl, ItemMod, ItemStruct,
    ItemTrait, ItemUse, Member, TraitItem, Type,
};

use super::FunctionFacts;

mod path_syntax;

use path_syntax::{
    collect_use_tree, expression_function_id, expression_path, impl_type_module, peel_type,
};

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct FunctionId {
    pub(super) module: Vec<String>,
    pub(super) name: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct CallTarget {
    candidates: Vec<FunctionId>,
    pub(super) name: String,
    pub(super) opaque_local_module: bool,
    pub(super) imprecise_dispatch: bool,
}

impl std::fmt::Display for FunctionId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for segment in &self.module {
            write!(formatter, "{segment}::")?;
        }
        formatter.write_str(&self.name)
    }
}

#[derive(Default)]
pub(super) struct FunctionIndex {
    pub(super) functions: BTreeMap<FunctionId, Vec<FunctionFacts>>,
    pub(super) values: BTreeSet<FunctionId>,
}

impl FunctionIndex {
    pub(super) fn extend(&mut self, other: Self) {
        for (id, mut functions) in other.functions {
            self.functions.entry(id).or_default().append(&mut functions);
        }
        self.values.extend(other.values);
    }

    pub(super) fn contains(&self, id: &FunctionId) -> bool {
        self.functions.contains_key(id)
    }

    pub(super) fn ids_named(&self, name: &str) -> Vec<FunctionId> {
        self.functions
            .iter()
            .filter(|(id, _)| id.name == name)
            .flat_map(|(id, functions)| std::iter::repeat_n(id, functions.len()))
            .cloned()
            .collect()
    }

    pub(super) fn unique_exact(&self, id: &FunctionId) -> Result<Option<&FunctionFacts>, String> {
        match self.functions.get(id).map(Vec::as_slice) {
            None => Ok(None),
            Some([function]) => Ok(Some(function)),
            Some(functions) => Err(format!(
                "function `{id}` resolves to {} declarations",
                functions.len()
            )),
        }
    }

    pub(super) fn resolve_call(&self, target: &CallTarget) -> Result<Option<FunctionId>, String> {
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

    pub(super) fn require_function_namespace(&self, target: &CallTarget) -> Result<(), String> {
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

    pub(super) fn matching_functions(&self, target: &CallTarget) -> BTreeSet<FunctionId> {
        target
            .candidates
            .iter()
            .filter(|candidate| self.contains(candidate))
            .cloned()
            .collect()
    }
}

impl CallTarget {
    pub(super) fn merge(mut self, other: Self) -> Self {
        self.candidates.extend(other.candidates);
        self.candidates.sort();
        self.candidates.dedup();
        self.opaque_local_module |= other.opaque_local_module;
        self.imprecise_dispatch |= other.imprecise_dispatch;
        self
    }

    pub(super) fn candidates(&self) -> &[FunctionId] {
        &self.candidates
    }

    pub(super) fn matches_any_name(&self, names: &[&str]) -> bool {
        names.contains(&self.name.as_str())
            || self
                .candidates
                .iter()
                .any(|candidate| names.contains(&candidate.name.as_str()))
    }
}

#[derive(Clone, Default)]
pub(super) struct LocalCallResolver {
    explicit: BTreeMap<(Vec<String>, String), Vec<FunctionId>>,
    globs: BTreeMap<Vec<String>, Vec<Vec<String>>>,
    module_aliases: BTreeMap<(Vec<String>, String), Vec<Vec<String>>>,
    crate_aliases: BTreeSet<String>,
    local_functions: BTreeSet<FunctionId>,
    local_methods: BTreeSet<FunctionId>,
    local_trait_methods: BTreeSet<FunctionId>,
    deref_targets: BTreeMap<Vec<String>, Vec<Vec<String>>>,
    struct_fields: BTreeMap<Vec<String>, BTreeMap<String, Vec<String>>>,
    out_of_line_modules: BTreeSet<Vec<String>>,
    target_functions: BTreeSet<String>,
    target_modules: BTreeSet<Vec<String>>,
}

impl LocalCallResolver {
    pub(super) fn collect(
        file: &File,
        module: &[String],
        target_modules: &BTreeSet<Vec<String>>,
        target_functions: &BTreeSet<String>,
    ) -> Self {
        let mut collector = LocalImportCollector {
            module: module.to_vec(),
            resolver: Self {
                target_functions: target_functions.clone(),
                target_modules: target_modules.clone(),
                ..Self::default()
            },
        };
        collector.visit_file(file);
        collector.resolver
    }

    pub(super) fn scoped(&self) -> Self {
        Self {
            local_functions: self.local_functions.clone(),
            local_methods: self.local_methods.clone(),
            local_trait_methods: self.local_trait_methods.clone(),
            deref_targets: self.deref_targets.clone(),
            struct_fields: self.struct_fields.clone(),
            crate_aliases: self.crate_aliases.clone(),
            out_of_line_modules: self.out_of_line_modules.clone(),
            target_functions: self.target_functions.clone(),
            target_modules: self.target_modules.clone(),
            ..Self::default()
        }
    }

    pub(super) fn extend(&mut self, other: Self) {
        for (key, mut values) in other.explicit {
            self.explicit.entry(key).or_default().append(&mut values);
        }
        for (key, mut values) in other.globs {
            self.globs.entry(key).or_default().append(&mut values);
        }
        for (key, mut values) in other.module_aliases {
            self.module_aliases
                .entry(key)
                .or_default()
                .append(&mut values);
        }
        self.local_methods.extend(other.local_methods);
        self.local_functions.extend(other.local_functions);
        self.local_trait_methods.extend(other.local_trait_methods);
        for (key, mut values) in other.deref_targets {
            self.deref_targets
                .entry(key)
                .or_default()
                .append(&mut values);
        }
        for (key, values) in other.struct_fields {
            self.struct_fields.entry(key).or_default().extend(values);
        }
        self.crate_aliases.extend(other.crate_aliases);
        self.out_of_line_modules.extend(other.out_of_line_modules);
        self.target_functions.extend(other.target_functions);
        self.target_modules.extend(other.target_modules);
    }

    pub(super) fn complete_target_graph(&mut self) {
        self.out_of_line_modules.clear();
    }

    pub(super) fn self_target(
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

    pub(super) fn call_target(
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

    pub(super) fn named_target(&self, name: &str, current_module: &[String]) -> CallTarget {
        self.segments_target(&[name.to_owned()], current_module, name.to_owned())
    }

    pub(super) fn value_type_module(
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

    pub(super) fn value_expression_type_module(
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

    pub(super) fn declared_type_module(
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

    pub(super) fn explicit_target(
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

    pub(super) fn method_target(
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

    pub(super) fn field_expression_type_module(
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
                let name = super::syntax::unqualified_expression_name(expression)?;
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

    fn is_deref_impl(&self, item: &ItemImpl, current_module: &[String]) -> bool {
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

    pub(super) fn add_use(&mut self, item: &ItemUse, module: &[String]) {
        collect_use_tree(&item.tree, &mut Vec::new(), module, self);
    }

    pub(super) fn classify_target(&self, mut target: CallTarget) -> CallTarget {
        target.opaque_local_module |= target.candidates.iter().any(|candidate| {
            self.out_of_line_modules
                .iter()
                .any(|module| candidate.module.starts_with(module))
                || (self.target_modules.contains(&candidate.module)
                    && self.target_functions.contains(&candidate.name))
        });
        target
    }

    pub(super) fn can_name_reviewed_function(&self, target: &CallTarget) -> bool {
        target
            .candidates
            .iter()
            .any(|candidate| self.target_functions.contains(&candidate.name))
    }

    fn segments_target(
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

struct LocalImportCollector {
    module: Vec<String>,
    resolver: LocalCallResolver,
}

impl<'ast> Visit<'ast> for LocalImportCollector {
    fn visit_item_extern_crate(&mut self, item: &'ast ItemExternCrate) {
        if !crate::verification::target::module_active_for_test(&item.attrs).unwrap_or(false)
            || !self.module.is_empty()
            || item.ident != "self"
        {
            return;
        }
        if let Some((_, alias)) = &item.rename {
            self.resolver.crate_aliases.insert(alias.to_string());
        }
    }

    fn visit_item_use(&mut self, item: &'ast ItemUse) {
        if !crate::verification::target::module_active_for_test(&item.attrs).unwrap_or(false) {
            return;
        }
        collect_use_tree(
            &item.tree,
            &mut Vec::new(),
            &self.module,
            &mut self.resolver,
        );
    }

    fn visit_item_mod(&mut self, item: &'ast ItemMod) {
        if !crate::verification::target::module_active_for_test(&item.attrs).unwrap_or(false) {
            return;
        }
        self.module.push(item.ident.to_string());
        let Some((_, items)) = &item.content else {
            self.resolver
                .out_of_line_modules
                .insert(self.module.clone());
            self.module.pop();
            return;
        };
        for item in items {
            self.visit_item(item);
        }
        self.module.pop();
    }

    fn visit_item_fn(&mut self, item: &'ast ItemFn) {
        if !crate::verification::target::module_active_for_test(&item.attrs).unwrap_or(false) {
            return;
        }
        self.resolver.local_functions.insert(FunctionId {
            module: self.module.clone(),
            name: item.sig.ident.to_string(),
        });
    }

    fn visit_item_struct(&mut self, item: &'ast ItemStruct) {
        if !crate::verification::target::module_active_for_test(&item.attrs).unwrap_or(false) {
            return;
        }
        let mut type_module = self.module.clone();
        type_module.push(item.ident.to_string());
        let fields = item
            .fields
            .iter()
            .filter_map(|field| {
                let name = field.ident.as_ref()?.to_string();
                let module = self
                    .resolver
                    .declared_type_module(&field.ty, &self.module)?;
                Some((name, module))
            })
            .collect::<BTreeMap<_, _>>();
        if !fields.is_empty() {
            self.resolver.struct_fields.insert(type_module, fields);
        }
    }

    fn visit_item_impl(&mut self, item: &'ast ItemImpl) {
        if !crate::verification::target::module_active_for_test(&item.attrs).unwrap_or(false) {
            return;
        }
        let Some(type_module) = self
            .resolver
            .declared_type_module(&item.self_ty, &self.module)
        else {
            return;
        };
        self.resolver
            .local_methods
            .extend(item.items.iter().filter_map(|item| {
                match item {
                    ImplItem::Fn(method)
                        if crate::verification::target::module_active_for_test(&method.attrs)
                            .unwrap_or(false) =>
                    {
                        Some(FunctionId {
                            module: type_module.clone(),
                            name: method.sig.ident.to_string(),
                        })
                    }
                    _ => None,
                }
            }));
        if self.resolver.is_deref_impl(item, &self.module) {
            let targets = item
                .items
                .iter()
                .filter_map(|item| match item {
                    ImplItem::Type(item)
                        if item.ident == "Target"
                            && crate::verification::target::module_active_for_test(&item.attrs)
                                .unwrap_or(false) =>
                    {
                        self.resolver.declared_type_module(&item.ty, &self.module)
                    }
                    _ => None,
                })
                .collect::<Vec<_>>();
            self.resolver
                .deref_targets
                .entry(type_module)
                .or_default()
                .extend(targets);
        }
    }

    fn visit_item_trait(&mut self, item: &'ast ItemTrait) {
        if !crate::verification::target::module_active_for_test(&item.attrs).unwrap_or(false) {
            return;
        }
        self.resolver
            .local_trait_methods
            .extend(item.items.iter().filter_map(|item| {
                match item {
                    TraitItem::Fn(method)
                        if crate::verification::target::module_active_for_test(&method.attrs)
                            .unwrap_or(false) =>
                    {
                        Some(FunctionId {
                            module: self.module.clone(),
                            name: method.sig.ident.to_string(),
                        })
                    }
                    _ => None,
                }
            }));
    }
}
