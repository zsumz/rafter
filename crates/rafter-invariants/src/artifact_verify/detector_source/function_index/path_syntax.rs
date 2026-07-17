use syn::{Expr, Type, UseTree};

use super::{FunctionId, LocalCallResolver};

pub(super) fn expression_path(expression: &Expr) -> Option<&syn::ExprPath> {
    match expression {
        Expr::Path(path) => Some(path),
        Expr::Group(group) => expression_path(&group.expr),
        Expr::Paren(paren) => expression_path(&paren.expr),
        _ => None,
    }
}

pub(super) fn expression_function_id(
    segments: &[String],
    current_module: &[String],
) -> Option<FunctionId> {
    let mut cursor = 0;
    let mut module = current_module.to_vec();
    match segments.first().map(String::as_str) {
        Some("crate") => {
            module.clear();
            cursor = 1;
        }
        Some("self") => cursor = 1,
        Some("super") => {
            while segments
                .get(cursor)
                .is_some_and(|segment| segment == "super")
            {
                module.pop()?;
                cursor += 1;
            }
        }
        Some(_) => {}
        None => return None,
    }
    let (name, path) = segments.get(cursor..)?.split_last()?;
    if matches!(name.as_str(), "crate" | "self" | "super") {
        return None;
    }
    module.extend(path.iter().cloned());
    Some(FunctionId {
        module,
        name: name.clone(),
    })
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

pub(super) fn impl_type_module(ty: &Type, current_module: &[String]) -> Option<Vec<String>> {
    let path = match ty {
        Type::Path(path) => path,
        Type::Group(group) => return impl_type_module(&group.elem, current_module),
        Type::Paren(paren) => return impl_type_module(&paren.elem, current_module),
        Type::Reference(reference) => return impl_type_module(&reference.elem, current_module),
        _ => return None,
    };
    if path.qself.is_some() || path.path.leading_colon.is_some() {
        return None;
    }
    let segments = path
        .path
        .segments
        .iter()
        .map(|segment| segment.ident.to_string())
        .collect::<Vec<_>>();
    let function = expression_function_id(&segments, current_module)?;
    let mut module = function.module;
    module.push(function.name);
    Some(module)
}

pub(super) fn peel_type(ty: &Type) -> &Type {
    match ty {
        Type::Group(group) => peel_type(&group.elem),
        Type::Paren(paren) => peel_type(&paren.elem),
        Type::Reference(reference) => peel_type(&reference.elem),
        _ => ty,
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
