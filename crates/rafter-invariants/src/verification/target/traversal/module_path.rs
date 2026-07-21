//! External Rust module path resolution under test-active attributes.

use std::path::{Path, PathBuf};

use syn::{parse::Parser, punctuated::Punctuated, Attribute, ItemMod, Meta, Token};

pub(super) fn resolve_external_module(
    item: &ItemMod,
    path_base: &Path,
    module_dir: &Path,
) -> Result<PathBuf, String> {
    if let Some(path) = effective_module_path(&item.attrs)? {
        return Ok(path_base.join(path));
    }
    let name = item.ident.to_string();
    let candidates = [
        module_dir.join(format!("{name}.rs")),
        module_dir.join(&name).join("mod.rs"),
    ];
    let existing = candidates
        .into_iter()
        .filter(|candidate| candidate.is_file())
        .collect::<Vec<_>>();
    let [path] = existing.as_slice() else {
        return Err(format!(
            "module {name} resolves to {} source files",
            existing.len()
        ));
    };
    Ok(path.clone())
}

fn effective_module_path(attributes: &[Attribute]) -> Result<Option<PathBuf>, String> {
    let mut paths = Vec::new();
    for attribute in attributes {
        collect_effective_module_paths(&attribute.meta, &mut paths)?;
    }
    match paths.as_slice() {
        [] => Ok(None),
        [path] => Ok(Some(path.clone())),
        _ => Err("module has more than one effective #[path] attribute".to_owned()),
    }
}

fn collect_effective_module_paths(meta: &Meta, paths: &mut Vec<PathBuf>) -> Result<(), String> {
    if let Some(path) = module_path_meta(meta) {
        paths.push(path);
        return Ok(());
    }
    let Meta::List(list) = meta else {
        return Ok(());
    };
    if !list.path.is_ident("cfg_attr") {
        return Ok(());
    }
    let arguments = Punctuated::<Meta, Token![,]>::parse_terminated
        .parse2(list.tokens.clone())
        .map_err(|error| format!("parse cfg_attr arguments: {error}"))?;
    let mut arguments = arguments.iter();
    let predicate = arguments.next().ok_or("cfg_attr requires a predicate")?;
    if super::super::cfg::cfg_predicate_active_for_test(predicate)? {
        for attribute in arguments {
            collect_effective_module_paths(attribute, paths)?;
        }
    }
    Ok(())
}

fn module_path_meta(meta: &Meta) -> Option<PathBuf> {
    let Meta::NameValue(name_value) = meta else {
        return None;
    };
    if !name_value.path.is_ident("path") {
        return None;
    }
    let syn::Expr::Lit(expression) = &name_value.value else {
        return None;
    };
    match &expression.lit {
        syn::Lit::Str(path) => Some(PathBuf::from(path.value())),
        _ => None,
    }
}
