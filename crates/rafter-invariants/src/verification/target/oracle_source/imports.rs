//! Exact import provenance for public oracle and property-test macros.

use std::collections::{BTreeMap, BTreeSet};

use syn::{
    visit::{self, Visit},
    Item, ItemFn, ItemMacro, ItemMod, ItemUse, Macro, UseTree,
};

use crate::verification::target::ORACLE_MACROS;

#[derive(Default)]
pub(super) struct Imports {
    explicit: BTreeMap<String, Vec<Vec<String>>>,
    globs: Vec<Vec<String>>,
    aliases: BTreeSet<String>,
    local_macros: BTreeSet<String>,
    local_rafter_invariant_test: bool,
}

impl Imports {
    pub(super) fn collect(items: &[Item], parent: Option<&Self>) -> Self {
        let mut collector = ImportCollector::default();
        for item in items {
            collector.visit_item(item);
        }
        let mut imports = collector.imports;
        if imports
            .globs
            .iter()
            .any(|path| path == &["super".to_owned()])
        {
            if let Some(parent) = parent {
                imports.inherit_parent(parent);
            }
        }
        imports
    }

    pub(super) fn validate(&self) -> Result<(), String> {
        if self.local_rafter_invariant_test {
            return Err("source shadows the rafter_invariant_test crate".to_owned());
        }
        if self
            .local_macros
            .iter()
            .any(|name| ORACLE_MACROS.contains(&name.as_str()))
        {
            return Err("source declares a local oracle macro".to_owned());
        }
        Ok(())
    }

    pub(super) fn trusted_oracle(&self, path: &syn::Path) -> bool {
        let parts = path
            .segments
            .iter()
            .map(|segment| segment.ident.to_string())
            .collect::<Vec<_>>();
        let Some(name) = parts.last() else {
            return false;
        };
        if !ORACLE_MACROS.contains(&name.as_str()) || self.local_macros.contains(name) {
            return false;
        }
        match parts.as_slice() {
            [unqualified] => {
                !self.aliases.contains(unqualified)
                    && self.explicit.get(unqualified).is_some_and(|paths| {
                        paths.len() == 1
                            && paths[0] == ["rafter_invariant_test".to_owned(), unqualified.clone()]
                    })
            }
            [krate, qualified] => {
                krate == "rafter_invariant_test"
                    && qualified == name
                    && (path.leading_colon.is_some()
                        || self.globs.is_empty() && !self.aliases.contains(krate))
            }
            _ => false,
        }
    }

    pub(super) fn trusted_proptest(&self, invocation: &Macro) -> bool {
        if self.local_macros.contains("proptest") {
            return false;
        }
        let parts = invocation
            .path
            .segments
            .iter()
            .map(|segment| segment.ident.to_string())
            .collect::<Vec<_>>();
        match parts.as_slice() {
            [name] if name == "proptest" => {
                self.explicit.get(name).is_some_and(|paths| {
                    paths.len() == 1 && paths[0] == ["proptest".to_owned(), "proptest".to_owned()]
                }) || self
                    .globs
                    .iter()
                    .any(|path| path == &["proptest".to_owned(), "prelude".to_owned()])
            }
            [krate, name] => krate == "proptest" && name == "proptest",
            _ => false,
        }
    }

    pub(super) fn trusted_detector_test(&self, path: &syn::Path) -> bool {
        let parts = path
            .segments
            .iter()
            .map(|segment| segment.ident.to_string())
            .collect::<Vec<_>>();
        match parts.as_slice() {
            [name] if name == "detector_test" => {
                !self.aliases.contains(name)
                    && self.explicit.get(name).is_some_and(|paths| {
                        paths.len() == 1
                            && paths[0]
                                == [
                                    "rafter_invariant_test".to_owned(),
                                    "detector_test".to_owned(),
                                ]
                    })
            }
            [krate, name] => {
                krate == "rafter_invariant_test"
                    && name == "detector_test"
                    && (path.leading_colon.is_some()
                        || self.globs.is_empty() && !self.aliases.contains(krate))
            }
            _ => false,
        }
    }

    fn inherit_parent(&mut self, parent: &Self) {
        for (name, paths) in &parent.explicit {
            self.explicit
                .entry(name.clone())
                .or_default()
                .extend(paths.iter().cloned());
        }
        self.aliases.extend(parent.aliases.iter().cloned());
        self.local_macros
            .extend(parent.local_macros.iter().cloned());
        self.local_rafter_invariant_test |= parent.local_rafter_invariant_test;
    }
}

#[derive(Default)]
struct ImportCollector {
    imports: Imports,
}

impl Visit<'_> for ImportCollector {
    fn visit_item_use(&mut self, item: &ItemUse) {
        collect_use_tree(&item.tree, &mut Vec::new(), &mut self.imports);
    }

    fn visit_item_macro(&mut self, item: &ItemMacro) {
        if let Some(name) = &item.ident {
            self.imports.local_macros.insert(name.to_string());
        }
        visit::visit_item_macro(self, item);
    }

    fn visit_item_mod(&mut self, item: &ItemMod) {
        if item.ident == "rafter_invariant_test" {
            self.imports.local_rafter_invariant_test = true;
        }
    }

    fn visit_item_fn(&mut self, _function: &ItemFn) {}
}

fn collect_use_tree(tree: &UseTree, prefix: &mut Vec<String>, imports: &mut Imports) {
    match tree {
        UseTree::Path(path) => {
            prefix.push(path.ident.to_string());
            collect_use_tree(&path.tree, prefix, imports);
            prefix.pop();
        }
        UseTree::Name(name) => {
            let mut path = prefix.clone();
            path.push(name.ident.to_string());
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
            imports.aliases.insert(rename.rename.to_string());
            if rename.rename == "rafter_invariant_test" {
                imports.local_rafter_invariant_test = true;
            }
        }
        UseTree::Glob(_) => imports.globs.push(prefix.clone()),
        UseTree::Group(group) => {
            for tree in &group.items {
                collect_use_tree(tree, prefix, imports);
            }
        }
    }
}
