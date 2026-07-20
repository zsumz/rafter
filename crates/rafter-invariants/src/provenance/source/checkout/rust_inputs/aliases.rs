//! Lexical alias tracking for Rust source-input macros.

use std::collections::{HashMap, HashSet};

use syn::UseTree;

use super::{cfg_eval::item_is_definitively_inactive, is_include_name, path_is_ident, unraw_ident};

pub(super) type AliasEdges = HashMap<String, HashSet<String>>;
pub(super) type IncludedAliasMap = HashMap<String, Option<String>>;
pub(super) type QualifiedAliasMap = HashMap<String, String>;

pub(super) fn resolve_include_aliases(edges: &AliasEdges) -> HashMap<String, String> {
    let mut aliases = HashMap::new();
    loop {
        let mut changed = false;
        for (alias, targets) in edges {
            for target in targets {
                let canonical = if is_include_name(target) {
                    Some(target.clone())
                } else {
                    aliases.get(target).cloned()
                };
                if let Some(canonical) = canonical {
                    changed |= record_include_alias(&mut aliases, alias.clone(), canonical);
                }
            }
        }
        if !changed {
            return aliases;
        }
    }
}

fn record_include_alias(
    aliases: &mut HashMap<String, String>,
    alias: String,
    canonical: String,
) -> bool {
    match aliases.get_mut(&alias) {
        Some(existing) if existing != "include" && canonical == "include" => {
            existing.clone_from(&canonical);
            true
        }
        Some(_) => false,
        None => {
            aliases.insert(alias, canonical);
            true
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub(super) enum ScopedAlias {
    Include(String),
    Shadowed,
    Unbound,
}

#[derive(Default)]
pub(super) struct AliasScope {
    bindings: HashMap<String, Option<String>>,
    qualified_bindings: HashMap<String, Option<String>>,
}

pub(super) fn collect_alias_scope<'a>(
    items: impl IntoIterator<Item = &'a syn::Item>,
) -> AliasScope {
    let mut edges = AliasEdges::new();
    let mut bound = HashSet::new();
    let mut macro_shadows = HashSet::new();
    let items = items.into_iter().collect::<Vec<_>>();
    for item in &items {
        if item_is_definitively_inactive(item).unwrap_or(false) {
            continue;
        }
        match item {
            syn::Item::Use(item) => collect_use_tree(&item.tree, &mut edges, &mut bound),
            syn::Item::Macro(item) if path_is_ident(&item.mac.path, "macro_rules") => {
                if let Some(ident) = &item.ident {
                    macro_shadows.insert(unraw_ident(ident));
                }
            }
            _ => {}
        }
    }

    let resolved = resolve_include_aliases(&edges);
    let mut bindings = bound
        .into_iter()
        .map(|name| {
            let canonical = resolved.get(&name).cloned();
            (name, canonical)
        })
        .collect::<HashMap<_, _>>();
    for name in macro_shadows {
        bindings.insert(name, None);
    }
    let mut qualified_bindings = HashMap::new();
    for item in items {
        if item_is_definitively_inactive(item).unwrap_or(false) {
            continue;
        }
        let syn::Item::Mod(item) = item else {
            continue;
        };
        let Some((_, items)) = &item.content else {
            continue;
        };
        collect_qualified_scope_bindings(
            items.iter(),
            &[unraw_ident(&item.ident)],
            &mut qualified_bindings,
        );
    }
    AliasScope {
        bindings,
        qualified_bindings,
    }
}

pub(super) fn resolve_scoped_alias(scopes: &[AliasScope], name: &str) -> ScopedAlias {
    for scope in scopes.iter().rev() {
        if let Some(binding) = scope.bindings.get(name) {
            return match binding {
                Some(canonical) => ScopedAlias::Include(canonical.clone()),
                None => ScopedAlias::Shadowed,
            };
        }
    }
    ScopedAlias::Unbound
}

pub(super) fn resolve_scoped_qualified_alias(scopes: &[AliasScope], path: &str) -> ScopedAlias {
    for scope in scopes.iter().rev() {
        if let Some(binding) = scope.qualified_bindings.get(path) {
            return match binding {
                Some(canonical) => ScopedAlias::Include(canonical.clone()),
                None => ScopedAlias::Shadowed,
            };
        }
    }
    ScopedAlias::Unbound
}

pub(super) fn visible_include_aliases(scopes: &[AliasScope]) -> HashMap<String, String> {
    let mut visible = HashMap::new();
    for scope in scopes {
        for (name, canonical) in &scope.bindings {
            if let Some(canonical) = canonical {
                visible.insert(name.clone(), canonical.clone());
            } else {
                visible.remove(name);
            }
        }
    }
    visible
}

pub(super) fn alias_path_key(module_path: &[String], name: &str) -> String {
    let mut path = module_path.to_vec();
    path.push(name.to_owned());
    path.join("::")
}

pub(super) fn collect_included_alias_scope<'a>(
    items: impl IntoIterator<Item = &'a syn::Item>,
    module_path: &[String],
    aliases: &mut IncludedAliasMap,
) {
    let scope = collect_alias_scope(items);
    for (name, canonical) in scope.bindings {
        aliases.insert(alias_path_key(module_path, &name), canonical);
    }
}

pub(super) fn collect_qualified_include_aliases<'a>(
    items: impl IntoIterator<Item = &'a syn::Item>,
    module_path: &[String],
    aliases: &mut QualifiedAliasMap,
) {
    let items = items.into_iter().collect::<Vec<_>>();
    let scope = collect_alias_scope(items.iter().copied());
    for (name, canonical) in &scope.bindings {
        if let Some(canonical) = canonical {
            aliases.insert(alias_path_key(module_path, name), canonical.clone());
        }
    }
    for item in items {
        if item_is_definitively_inactive(item).unwrap_or(false) {
            continue;
        }
        let syn::Item::Mod(item) = item else {
            continue;
        };
        let Some((_, items)) = &item.content else {
            continue;
        };
        let mut child_path = module_path.to_vec();
        child_path.push(unraw_ident(&item.ident));
        collect_qualified_include_aliases(items.iter(), &child_path, aliases);
    }
}

fn collect_qualified_scope_bindings<'a>(
    items: impl IntoIterator<Item = &'a syn::Item>,
    module_path: &[String],
    bindings: &mut HashMap<String, Option<String>>,
) {
    let items = items.into_iter().collect::<Vec<_>>();
    let mut edges = AliasEdges::new();
    let mut bound = HashSet::new();
    let mut macro_shadows = HashSet::new();
    for item in &items {
        if item_is_definitively_inactive(item).unwrap_or(false) {
            continue;
        }
        match item {
            syn::Item::Use(item) => collect_use_tree(&item.tree, &mut edges, &mut bound),
            syn::Item::Macro(item) if path_is_ident(&item.mac.path, "macro_rules") => {
                if let Some(ident) = &item.ident {
                    macro_shadows.insert(unraw_ident(ident));
                }
            }
            _ => {}
        }
    }
    let resolved = resolve_include_aliases(&edges);
    for name in bound {
        if let Some(canonical) = resolved.get(&name) {
            bindings.insert(alias_path_key(module_path, &name), Some(canonical.clone()));
        }
    }
    for name in macro_shadows {
        bindings.insert(alias_path_key(module_path, &name), None);
    }
    for item in items {
        if item_is_definitively_inactive(item).unwrap_or(false) {
            continue;
        }
        let syn::Item::Mod(item) = item else {
            continue;
        };
        let Some((_, items)) = &item.content else {
            continue;
        };
        let mut child_path = module_path.to_vec();
        child_path.push(unraw_ident(&item.ident));
        collect_qualified_scope_bindings(items.iter(), &child_path, bindings);
    }
}

fn collect_use_tree(tree: &UseTree, aliases: &mut AliasEdges, bound: &mut HashSet<String>) {
    match tree {
        UseTree::Path(path) => collect_use_tree(&path.tree, aliases, bound),
        UseTree::Name(name) => {
            let original = unraw_ident(&name.ident);
            bound.insert(original.clone());
            if is_include_name(&original) {
                aliases
                    .entry(original.clone())
                    .or_default()
                    .insert(original);
            }
        }
        UseTree::Rename(rename) => {
            let alias = unraw_ident(&rename.rename);
            bound.insert(alias.clone());
            aliases
                .entry(alias)
                .or_default()
                .insert(unraw_ident(&rename.ident));
        }
        UseTree::Group(group) => {
            for item in &group.items {
                collect_use_tree(item, aliases, bound);
            }
        }
        UseTree::Glob(_) => {}
    }
}
