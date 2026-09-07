//! Imported paths, and which oracle macros a file may be trusting.
//!
//! An oracle macro counts only when it resolves to the helper crate, so both
//! the explicit imports and any local alias of `rafter_invariant_test` are
//! collected before a macro invocation is judged.

use std::collections::{BTreeMap, BTreeSet};

use syn::{visit::Visit, File, ItemMacro, ItemUse, UseTree};

#[derive(Default)]
pub(super) struct ImportedPaths {
    explicit: BTreeMap<String, Vec<Vec<String>>>,
    pub(super) local_rafter_invariant_test: bool,
}

pub(super) fn imported_paths(file: &File) -> ImportedPaths {
    #[derive(Default)]
    struct ImportVisitor {
        imports: ImportedPaths,
    }

    impl<'ast> Visit<'ast> for ImportVisitor {
        fn visit_item_use(&mut self, item: &'ast ItemUse) {
            collect_use_tree(&item.tree, &mut Vec::new(), &mut self.imports);
        }
    }

    let mut visitor = ImportVisitor::default();
    visitor.visit_file(file);
    visitor.imports
}

pub(super) fn collect_use_tree(
    tree: &UseTree,
    prefix: &mut Vec<String>,
    imports: &mut ImportedPaths,
) {
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
            imports
                .explicit
                .entry(name.ident.to_string())
                .or_default()
                .push(path);
        }
        UseTree::Rename(rename) => {
            let mut path = prefix.clone();
            path.push(rename.ident.to_string());
            imports
                .explicit
                .entry(rename.rename.to_string())
                .or_default()
                .push(path);
            if rename.rename == "rafter_invariant_test" {
                imports.local_rafter_invariant_test = true;
            }
        }
        UseTree::Glob(_) => {}
        UseTree::Group(group) => {
            for item in &group.items {
                collect_use_tree(item, prefix, imports);
            }
        }
    }
}

pub(super) fn trusted_oracle_imports(file: &File) -> BTreeSet<String> {
    let imports = imported_paths(file);
    if imports.local_rafter_invariant_test {
        return BTreeSet::new();
    }
    imports
        .explicit
        .into_iter()
        .filter_map(|(name, paths)| {
            (is_oracle_macro(&name)
                && paths.len() == 1
                && paths[0].as_slice() == ["rafter_invariant_test", name.as_str()])
            .then_some(name)
        })
        .collect()
}

pub(super) fn declares_local_oracle_macro(file: &File) -> bool {
    #[derive(Default)]
    struct Visitor {
        found: bool,
    }

    impl Visit<'_> for Visitor {
        fn visit_item_macro(&mut self, item: &ItemMacro) {
            self.found |= item
                .ident
                .as_ref()
                .is_some_and(|ident| is_oracle_macro(&ident.to_string()));
            syn::visit::visit_item_macro(self, item);
        }
    }

    let mut visitor = Visitor::default();
    visitor.visit_file(file);
    visitor.found
}

pub(super) fn is_oracle_macro(name: &str) -> bool {
    matches!(
        name,
        "oracle_assert"
            | "oracle_assert_eq"
            | "oracle_assert_ne"
            | "oracle_expect_err"
            | "oracle_invoke_recorder"
            | "oracle_violation"
            | "oracle_prop_assert"
            | "oracle_prop_assert_eq"
    )
}
