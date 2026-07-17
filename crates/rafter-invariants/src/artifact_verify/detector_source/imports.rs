use std::collections::{BTreeMap, BTreeSet};

use syn::{
    visit::{self, Visit},
    File, Item, ItemConst, ItemExternCrate, ItemFn, ItemMacro, ItemMod, ItemStatic, ItemUse,
    UseTree,
};

use super::{FORBIDDEN_WITNESS_HELPERS, ORACLE_MACROS, SAFE_BUILTIN_MACROS};

#[derive(Clone, Default)]
pub(super) struct ImportedPaths {
    explicit: BTreeMap<String, Vec<Vec<String>>>,
    globs: Vec<Vec<String>>,
    aliases: BTreeSet<String>,
    pub(super) local_macros: BTreeSet<String>,
    local_oracle_macros: BTreeSet<String>,
    local_rafter_invariant_test: bool,
    pub(super) local_value_bindings: BTreeSet<String>,
    forbidden_witness_import: bool,
}

pub(super) fn collect_imports(file: &File) -> ImportedPaths {
    collect_item_imports(&file.items)
}

pub(super) fn collect_item_imports(items: &[Item]) -> ImportedPaths {
    #[derive(Default)]
    struct ImportVisitor {
        imports: ImportedPaths,
    }

    impl<'ast> Visit<'ast> for ImportVisitor {
        fn visit_item_use(&mut self, item: &'ast ItemUse) {
            if !crate::rust_target::module_active_for_test(&item.attrs).unwrap_or(false) {
                return;
            }
            collect_use_tree(&item.tree, &mut Vec::new(), &mut self.imports);
        }

        fn visit_item_fn(&mut self, _item: &'ast ItemFn) {}

        fn visit_item_macro(&mut self, item: &'ast ItemMacro) {
            visit::visit_item_macro(self, item);
        }

        fn visit_item_const(&mut self, item: &'ast ItemConst) {
            if !crate::rust_target::module_active_for_test(&item.attrs).unwrap_or(false) {
                return;
            }
            self.imports
                .local_value_bindings
                .insert(item.ident.to_string());
            visit::visit_item_const(self, item);
        }

        fn visit_item_static(&mut self, item: &'ast ItemStatic) {
            if !crate::rust_target::module_active_for_test(&item.attrs).unwrap_or(false) {
                return;
            }
            self.imports
                .local_value_bindings
                .insert(item.ident.to_string());
            visit::visit_item_static(self, item);
        }

        fn visit_item_mod(&mut self, item: &'ast ItemMod) {
            if !crate::rust_target::module_active_for_test(&item.attrs).unwrap_or(false) {
                return;
            }
            if item.ident == "rafter_invariant_test" {
                self.imports.local_rafter_invariant_test = true;
            }
        }

        fn visit_item_extern_crate(&mut self, item: &'ast ItemExternCrate) {
            if !crate::rust_target::module_active_for_test(&item.attrs).unwrap_or(false) {
                return;
            }
            let effective = item
                .rename
                .as_ref()
                .map_or(&item.ident, |(_, rename)| rename);
            if effective == "rafter_invariant_test" && item.ident != "rafter_invariant_test" {
                self.imports.local_rafter_invariant_test = true;
            }
            visit::visit_item_extern_crate(self, item);
        }
    }

    let mut visitor = ImportVisitor::default();
    for item in items {
        visitor.visit_item(item);
    }
    visitor.imports
}

impl ImportedPaths {
    pub(super) fn add_macro_declaration(&mut self, item: &ItemMacro) {
        if !crate::rust_target::module_active_for_test(&item.attrs).unwrap_or(false) {
            return;
        }
        let Some(name) = &item.ident else {
            return;
        };
        let name = name.to_string();
        self.local_macros.insert(name.clone());
        if ORACLE_MACROS.contains(&name.as_str())
            || matches!(
                name.as_str(),
                "oracle_detector_witness" | "oracle_fabricated_detector_witness"
            )
        {
            self.local_oracle_macros.insert(name);
        }
    }

    pub(super) fn add_item(&mut self, item: &Item) {
        let imports = collect_item_imports(std::slice::from_ref(item));
        self.merge(imports);
    }

    pub(super) fn inherits_parent_glob(&self) -> bool {
        self.globs
            .iter()
            .any(|path| path.iter().map(String::as_str).eq(["super"]))
    }

    pub(super) fn inherit(&mut self, parent: &Self) {
        self.merge(parent.clone());
    }

    pub(super) fn inherit_parent_macros(&mut self, parent: &Self) {
        self.local_macros
            .extend(parent.local_macros.iter().cloned());
        self.local_oracle_macros
            .extend(parent.local_oracle_macros.iter().cloned());
    }

    fn merge(&mut self, other: Self) {
        for (name, mut paths) in other.explicit {
            self.explicit.entry(name).or_default().append(&mut paths);
        }
        self.globs.extend(other.globs);
        self.aliases.extend(other.aliases);
        self.local_macros.extend(other.local_macros);
        self.local_oracle_macros.extend(other.local_oracle_macros);
        self.local_rafter_invariant_test |= other.local_rafter_invariant_test;
        self.local_value_bindings.extend(other.local_value_bindings);
        self.forbidden_witness_import |= other.forbidden_witness_import;
    }
}

fn collect_use_tree(tree: &UseTree, prefix: &mut Vec<String>, imports: &mut ImportedPaths) {
    match tree {
        UseTree::Path(path) => {
            prefix.push(path.ident.to_string());
            collect_use_tree(&path.tree, prefix, imports);
            prefix.pop();
        }
        UseTree::Name(name) => {
            let mut path = prefix.clone();
            path.push(name.ident.to_string());
            if name.ident == "rafter_invariant_test" && path != ["rafter_invariant_test".to_owned()]
            {
                imports.local_rafter_invariant_test = true;
            }
            if path
                .last()
                .is_some_and(|part| FORBIDDEN_WITNESS_HELPERS.contains(&part.as_str()))
            {
                imports.forbidden_witness_import = true;
            }
            imports
                .explicit
                .entry(name.ident.to_string())
                .or_default()
                .push(path);
        }
        UseTree::Rename(rename) => {
            let mut path = prefix.clone();
            path.push(rename.ident.to_string());
            if path
                .last()
                .is_some_and(|part| FORBIDDEN_WITNESS_HELPERS.contains(&part.as_str()))
            {
                imports.forbidden_witness_import = true;
            }
            imports
                .explicit
                .entry(rename.rename.to_string())
                .or_default()
                .push(path);
            imports.aliases.insert(rename.rename.to_string());
            if rename.rename == "rafter_invariant_test" {
                imports.local_rafter_invariant_test = true;
            }
        }
        UseTree::Glob(_) => imports.globs.push(prefix.clone()),
        UseTree::Group(group) => {
            for item in &group.items {
                collect_use_tree(item, prefix, imports);
            }
        }
    }
}

pub(super) fn validate_oracle_provenance(imports: &ImportedPaths) -> Result<(), String> {
    if imports.local_rafter_invariant_test {
        return Err("fixture source shadows the rafter_invariant_test crate".to_owned());
    }
    if imports.forbidden_witness_import {
        return Err("fixture source imports the arbitrary detector witness helper".to_owned());
    }
    Ok(())
}

pub(super) fn trusted_oracle_macro(path: &syn::Path, imports: &ImportedPaths) -> bool {
    let parts = path
        .segments
        .iter()
        .map(|segment| segment.ident.to_string())
        .collect::<Vec<_>>();
    let Some(name) = parts.last() else {
        return false;
    };
    if !ORACLE_MACROS.contains(&name.as_str()) {
        return false;
    }
    if imports.local_oracle_macros.contains(name) {
        return false;
    }
    match parts.as_slice() {
        [unqualified] => {
            !imports.aliases.contains(unqualified)
                && imports.explicit.get(unqualified).is_some_and(|paths| {
                    paths.len() == 1
                        && paths[0] == ["rafter_invariant_test".to_owned(), unqualified.clone()]
                })
        }
        [krate, qualified] => {
            krate == "rafter_invariant_test"
                && qualified == name
                && trusted_rafter_invariant_test_path(path, imports)
        }
        _ => false,
    }
}

pub(super) fn trusted_detector_test_attribute(path: &syn::Path, imports: &ImportedPaths) -> bool {
    path.segments
        .iter()
        .map(|segment| segment.ident.to_string())
        .eq(["rafter_invariant_test", "detector_test"].map(str::to_owned))
        && trusted_rafter_invariant_test_path(path, imports)
}

fn trusted_rafter_invariant_test_path(path: &syn::Path, imports: &ImportedPaths) -> bool {
    path.leading_colon.is_some()
        || imports.globs.is_empty()
            && !imports.local_rafter_invariant_test
            && !imports.aliases.contains("rafter_invariant_test")
}

pub(super) fn safe_builtin_macro(path: &syn::Path, imports: &ImportedPaths) -> bool {
    let parts = path
        .segments
        .iter()
        .map(|segment| segment.ident.to_string())
        .collect::<Vec<_>>();
    let [name] = parts.as_slice() else {
        return false;
    };
    SAFE_BUILTIN_MACROS.contains(&name.as_str())
        && !imports.local_macros.contains(name)
        && !imports.explicit.contains_key(name)
        && !imports.aliases.contains(name)
}
