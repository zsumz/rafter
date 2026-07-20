//! Use-tree indexing for explicit names, globs, and module aliases.

use syn::{ItemUse, UseTree};

use super::LocalCallResolver;
use crate::verification::detector::source::function_index::FunctionId;

impl LocalCallResolver {
    pub(in crate::verification::detector::source) fn add_use(
        &mut self,
        item: &ItemUse,
        module: &[String],
    ) {
        collect_use_tree(&item.tree, &mut Vec::new(), module, self);
    }
}

pub(super) fn collect_use_tree(
    tree: &UseTree,
    prefix: &mut Vec<String>,
    module: &[String],
    resolver: &mut LocalCallResolver,
) {
    match tree {
        UseTree::Path(path) => {
            prefix.push(path.ident.to_string());
            collect_use_tree(&path.tree, prefix, module, resolver);
            prefix.pop();
        }
        UseTree::Name(name) => {
            if name.ident == "self" {
                if let (Some(alias), Some(target)) =
                    (prefix.last(), imported_module_path(prefix, module))
                {
                    resolver
                        .module_aliases
                        .entry((module.to_vec(), alias.clone()))
                        .or_default()
                        .push(target);
                }
                return;
            }
            let mut path = prefix.clone();
            path.push(name.ident.to_string());
            if let Some(function) = imported_function_id(&path, module) {
                resolver
                    .explicit
                    .entry((module.to_vec(), name.ident.to_string()))
                    .or_default()
                    .push(function);
            }
            if let Some(target) = imported_module_path(&path, module) {
                resolver
                    .module_aliases
                    .entry((module.to_vec(), name.ident.to_string()))
                    .or_default()
                    .push(target);
            }
        }
        UseTree::Rename(rename) => {
            let mut path = prefix.clone();
            let renamed_self = rename.ident == "self";
            if !renamed_self {
                path.push(rename.ident.to_string());
                if let Some(function) = imported_function_id(&path, module) {
                    resolver
                        .explicit
                        .entry((module.to_vec(), rename.rename.to_string()))
                        .or_default()
                        .push(function);
                }
            }
            if let Some(target) = imported_module_path(&path, module) {
                resolver
                    .module_aliases
                    .entry((module.to_vec(), rename.rename.to_string()))
                    .or_default()
                    .push(target);
            }
        }
        UseTree::Glob(_) => {
            if let Some(target) = imported_module_path(prefix, module) {
                resolver
                    .globs
                    .entry(module.to_vec())
                    .or_default()
                    .push(target);
            }
        }
        UseTree::Group(group) => {
            for item in &group.items {
                collect_use_tree(item, prefix, module, resolver);
            }
        }
    }
}

fn imported_function_id(path: &[String], current_module: &[String]) -> Option<FunctionId> {
    let (name, module) = path.split_last()?;
    Some(FunctionId {
        module: imported_module_path(module, current_module)?,
        name: name.clone(),
    })
}

fn imported_module_path(path: &[String], current_module: &[String]) -> Option<Vec<String>> {
    let mut module = Vec::new();
    let mut cursor = 0;
    match path.first().map(String::as_str) {
        Some("crate") => cursor = 1,
        Some("self") => {
            module.extend_from_slice(current_module);
            cursor = 1;
        }
        Some("super") => {
            module.extend_from_slice(current_module);
            while path.get(cursor).is_some_and(|segment| segment == "super") {
                module.pop()?;
                cursor += 1;
            }
        }
        Some(_) => module.extend_from_slice(current_module),
        None => {
            module.extend_from_slice(current_module);
            return Some(module);
        }
    }
    module.extend_from_slice(&path[cursor..]);
    Some(module)
}
