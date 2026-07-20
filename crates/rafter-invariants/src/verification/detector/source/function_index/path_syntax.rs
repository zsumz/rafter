//! Rust syntax helpers for paths, types, receivers, and imports.

use syn::{Expr, Type};

use super::FunctionId;

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
