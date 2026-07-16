use std::collections::{BTreeMap, BTreeSet};

use syn::{
    visit::{self, Visit},
    File, ItemConst, ItemExternCrate, ItemMacro, ItemMod, ItemStatic, ItemUse, UseTree,
};

use super::{FORBIDDEN_WITNESS_HELPERS, ORACLE_MACROS, SAFE_BUILTIN_MACROS};

#[derive(Default)]
pub(super) struct ImportedPaths {
    explicit: BTreeMap<String, Vec<Vec<String>>>,
    globs: Vec<Vec<String>>,
    aliases: BTreeSet<String>,
    pub(super) local_macros: BTreeSet<String>,
    local_oracle_macros: BTreeSet<String>,
    local_rafter_invariant_test: bool,
    local_value_bindings: BTreeSet<String>,
    forbidden_witness_import: bool,
}

pub(super) fn collect_imports(file: &File) -> ImportedPaths {
    #[derive(Default)]
    struct ImportVisitor {
        imports: ImportedPaths,
    }

    impl<'ast> Visit<'ast> for ImportVisitor {
        fn visit_item_use(&mut self, item: &'ast ItemUse) {
            collect_use_tree(&item.tree, &mut Vec::new(), &mut self.imports);
        }

        fn visit_item_macro(&mut self, item: &'ast ItemMacro) {
            if let Some(name) = &item.ident {
                let name = name.to_string();
                self.imports.local_macros.insert(name.clone());
                if ORACLE_MACROS.contains(&name.as_str())
                    || matches!(
                        name.as_str(),
                        "oracle_detector_witness" | "oracle_fabricated_detector_witness"
                    )
                {
                    self.imports.local_oracle_macros.insert(name);
                }
            }
            visit::visit_item_macro(self, item);
        }

        fn visit_item_const(&mut self, item: &'ast ItemConst) {
            self.imports
                .local_value_bindings
                .insert(item.ident.to_string());
            visit::visit_item_const(self, item);
        }

        fn visit_item_static(&mut self, item: &'ast ItemStatic) {
            self.imports
                .local_value_bindings
                .insert(item.ident.to_string());
            visit::visit_item_static(self, item);
        }

        fn visit_item_mod(&mut self, item: &'ast ItemMod) {
            if item.ident == "rafter_invariant_test" {
                self.imports.local_rafter_invariant_test = true;
            }
            visit::visit_item_mod(self, item);
        }

        fn visit_item_extern_crate(&mut self, item: &'ast ItemExternCrate) {
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
    visitor.visit_file(file);
    visitor.imports
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
                && !imports.aliases.contains("rafter_invariant_test")
        }
        _ => false,
    }
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

pub(super) fn verify_detector_resolution(
    imports: &ImportedPaths,
    fixture_module: &[String],
    detector_module: Option<&[String]>,
    detector: &str,
    declared_locally: bool,
) -> Result<(), String> {
    if imports.local_value_bindings.contains(detector) {
        return Err(format!(
            "registered detector `{detector}` is shadowed by a module value binding"
        ));
    }
    if declared_locally {
        if imports.explicit.contains_key(detector) || imports.aliases.contains(detector) {
            return Err(format!(
                "registered detector `{detector}` is ambiguous between a local declaration and an import"
            ));
        }
        return Ok(());
    }

    let detector_module = detector_module.ok_or_else(|| {
        format!("registered detector `{detector}` source is outside its executable Cargo target")
    })?;
    let explicit = imports.explicit.get(detector).is_some_and(|paths| {
        !paths.is_empty()
            && paths
                .iter()
                .all(|path| trusted_import_path(path, detector_module, fixture_module))
    });
    let glob = imports
        .globs
        .iter()
        .any(|path| trusted_import_path(path, detector_module, fixture_module));
    if imports.aliases.contains(detector) || !(explicit || glob) {
        return Err(format!(
            "registered detector `{detector}` is not imported from its bound detector source"
        ));
    }
    Ok(())
}

fn trusted_import_path(
    path: &[String],
    detector_module: &[String],
    fixture_module: &[String],
) -> bool {
    let mut exact = vec!["crate".to_owned()];
    exact.extend_from_slice(detector_module);
    if path == exact || path.len() == exact.len() + 1 && path.starts_with(&exact) {
        return true;
    }
    resolve_relative_module(path, fixture_module).is_some_and(|resolved| {
        resolved == detector_module
            || resolved.len() == detector_module.len() + 1 && resolved.starts_with(detector_module)
    })
}

fn resolve_relative_module(path: &[String], fixture_module: &[String]) -> Option<Vec<String>> {
    let first = path.first()?;
    if first != "self" && first != "super" {
        return None;
    }
    let mut resolved = fixture_module.to_vec();
    let mut position = usize::from(first == "self");
    while path.get(position).is_some_and(|segment| segment == "super") {
        resolved.pop()?;
        position += 1;
    }
    resolved.extend_from_slice(&path[position..]);
    Some(resolved)
}
