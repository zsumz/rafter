//! The declared module graph of one Cargo target, walked from its root.
//!
//! A registered test identity is only meaningful against the module path the
//! compiler would give the file, so the graph is built by following `mod`
//! declarations and `#[path]` attributes from the target root rather than
//! guessing a path from the file's location.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    sync::{Mutex, OnceLock},
};

use syn::ItemMod;

pub(super) fn test_source_module_path(
    workspace: &Path,
    path: &Path,
    identity: &rafter_invariants::TestIdentity,
) -> Option<Vec<String>> {
    type CacheKey = (PathBuf, String, String, String);
    type ModuleGraph = BTreeMap<PathBuf, BTreeSet<String>>;
    static MODULE_GRAPHS: OnceLock<Mutex<BTreeMap<CacheKey, ModuleGraph>>> = OnceLock::new();

    let workspace = fs::canonicalize(workspace).ok()?;
    let target = fs::canonicalize(workspace.join(path)).ok()?;
    let key = (
        workspace.clone(),
        identity.package.clone(),
        identity.target_kind.clone(),
        identity.target.clone(),
    );
    let mut cache = MODULE_GRAPHS
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .ok()?;
    let graph = cache
        .entry(key)
        .or_insert_with(|| build_test_target_module_graph(&workspace, identity));
    let matches = graph.get(&target)?.iter().collect::<Vec<_>>();
    let [module] = matches.as_slice() else {
        return None;
    };
    Some(
        module
            .split("::")
            .filter(|part| !part.is_empty())
            .map(str::to_owned)
            .collect(),
    )
}

pub(super) fn build_test_target_module_graph(
    workspace: &Path,
    identity: &rafter_invariants::TestIdentity,
) -> BTreeMap<PathBuf, BTreeSet<String>> {
    let mut graph = BTreeMap::new();
    for root in test_target_roots(workspace, identity) {
        let Ok(root) = fs::canonicalize(root) else {
            continue;
        };
        let Some(parent) = root.parent() else {
            continue;
        };
        collect_module_graph(&root, &[], parent, parent, &mut BTreeSet::new(), &mut graph);
    }
    graph
}

pub(super) fn test_target_roots(
    workspace: &Path,
    identity: &rafter_invariants::TestIdentity,
) -> Vec<PathBuf> {
    let package = workspace.join("crates").join(&identity.package);
    let candidates = match identity.target_kind.as_str() {
        "lib" => vec![package.join("src/lib.rs")],
        "test" => vec![package
            .join("tests")
            .join(format!("{}.rs", identity.target))],
        "bin" => {
            let mut roots = vec![
                package
                    .join("src/bin")
                    .join(format!("{}.rs", identity.target)),
                package
                    .join("src/bin")
                    .join(&identity.target)
                    .join("main.rs"),
            ];
            if identity.target == identity.package {
                roots.push(package.join("src/main.rs"));
            }
            roots
        }
        _ => Vec::new(),
    };
    candidates
        .into_iter()
        .filter(|candidate| candidate.is_file())
        .collect()
}

#[allow(clippy::too_many_arguments)]
pub(super) fn collect_module_graph(
    source_file: &Path,
    module: &[String],
    path_base: &Path,
    module_dir: &Path,
    visited: &mut BTreeSet<(PathBuf, String)>,
    graph: &mut BTreeMap<PathBuf, BTreeSet<String>>,
) {
    let key = (source_file.to_owned(), module.join("::"));
    if !visited.insert(key) {
        return;
    }
    graph
        .entry(source_file.to_owned())
        .or_default()
        .insert(module.join("::"));
    let Ok(source) = fs::read_to_string(source_file) else {
        return;
    };
    let Ok(file) = syn::parse_file(&source) else {
        return;
    };
    collect_module_graph_from_items(&file.items, module, path_base, module_dir, visited, graph);
}

pub(super) fn collect_module_graph_from_items(
    items: &[syn::Item],
    module: &[String],
    path_base: &Path,
    module_dir: &Path,
    visited: &mut BTreeSet<(PathBuf, String)>,
    graph: &mut BTreeMap<PathBuf, BTreeSet<String>>,
) {
    for item in items {
        let syn::Item::Mod(item) = item else {
            continue;
        };
        let mut child_module = module.to_vec();
        child_module.push(item.ident.to_string());
        if let Some((_, inline_items)) = &item.content {
            let inline_dir = module_dir.join(item.ident.to_string());
            collect_module_graph_from_items(
                inline_items,
                &child_module,
                &inline_dir,
                &inline_dir,
                visited,
                graph,
            );
            continue;
        }
        let Some(child_file) = resolve_external_module(item, path_base, module_dir) else {
            continue;
        };
        let Ok(child_file) = fs::canonicalize(child_file) else {
            continue;
        };
        let child_dir =
            if child_file.file_name().and_then(std::ffi::OsStr::to_str) == Some("mod.rs") {
                child_file.parent().unwrap_or(module_dir).to_owned()
            } else {
                module_dir.join(item.ident.to_string())
            };
        let child_path_base = child_file.parent().unwrap_or(path_base);
        collect_module_graph(
            &child_file,
            &child_module,
            child_path_base,
            &child_dir,
            visited,
            graph,
        );
    }
}

pub(super) fn resolve_external_module(
    item: &ItemMod,
    path_base: &Path,
    module_dir: &Path,
) -> Option<PathBuf> {
    if let Some(path) = item.attrs.iter().find_map(module_path_attribute) {
        return Some(path_base.join(path));
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
        return None;
    };
    Some(path.clone())
}

pub(super) fn module_path_attribute(attribute: &syn::Attribute) -> Option<PathBuf> {
    let syn::Meta::NameValue(name_value) = &attribute.meta else {
        return None;
    };
    if !name_value.path.is_ident("path") {
        return None;
    }
    let syn::Expr::Lit(expression) = &name_value.value else {
        return None;
    };
    let syn::Lit::Str(path) = &expression.lit else {
        return None;
    };
    Some(PathBuf::from(path.value()))
}
