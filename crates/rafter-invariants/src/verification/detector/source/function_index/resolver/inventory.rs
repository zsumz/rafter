//! AST inventory collection and composition for local call resolution.

use std::collections::{BTreeMap, BTreeSet};

use syn::{
    visit::Visit, File, ImplItem, ItemExternCrate, ItemFn, ItemImpl, ItemMod, ItemStruct,
    ItemTrait, ItemUse, TraitItem,
};

use super::{imports::collect_use_tree, LocalCallResolver};
use crate::verification::detector::source::function_index::FunctionId;

impl LocalCallResolver {
    pub(in crate::verification::detector::source) fn collect(
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

    pub(in crate::verification::detector::source) fn scoped(&self) -> Self {
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

    pub(in crate::verification::detector::source) fn extend(&mut self, other: Self) {
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

    pub(in crate::verification::detector::source) fn complete_target_graph(&mut self) {
        self.out_of_line_modules.clear();
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
