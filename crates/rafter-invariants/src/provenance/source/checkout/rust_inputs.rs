//! Complete resolution of tracked Rust compiler inputs.

use std::{
    collections::{HashMap, HashSet},
    error::Error,
    fs,
    path::{Path, PathBuf},
};

use syn::{ext::IdentExt, visit::Visit};

use super::path_validation::validate_tracked_source_path;

mod aliases;
mod cfg_eval;
mod discovery;
mod macro_scan;
mod validator;
mod visit;

use aliases::{
    alias_path_key, collect_alias_scope, collect_included_alias_scope,
    collect_qualified_include_aliases, resolve_scoped_alias, resolve_scoped_qualified_alias,
    visible_include_aliases, AliasScope, IncludedAliasMap, QualifiedAliasMap, ScopedAlias,
};
use cfg_eval::{item_is_definitively_inactive, walk_effective_metas, CfgValue};
use macro_scan::{
    contains_dynamic_macro_invocation, contains_generated_out_of_line_module,
    contains_include_invocation, contains_include_reference, contains_out_of_line_module_argument,
};

#[cfg(test)]
pub(super) fn validate_tracked_rust_inputs(
    root: &Path,
    tracked: &HashSet<PathBuf>,
) -> Result<(), Box<dyn Error>> {
    validate_tracked_rust_input_paths(
        root,
        tracked,
        tracked.iter().filter(|path| is_rust_path(path)).cloned(),
    )
}

pub(super) fn validate_resolved_tracked_rust_inputs(
    root: &Path,
    tracked: &HashSet<PathBuf>,
    metadata: &str,
) -> Result<(), Box<dyn Error>> {
    let root = fs::canonicalize(root)?;
    let metadata: serde_json::Value = serde_json::from_str(metadata)?;
    let packages = metadata
        .get("packages")
        .and_then(serde_json::Value::as_array)
        .ok_or("cargo metadata omitted its package inventory")?;
    let mut target_sources = HashSet::new();
    for package in packages {
        if !package
            .get("source")
            .is_some_and(serde_json::Value::is_null)
        {
            continue;
        }
        for target in package
            .get("targets")
            .and_then(serde_json::Value::as_array)
            .ok_or("path package omitted its target inventory")?
        {
            let source = target
                .get("src_path")
                .and_then(serde_json::Value::as_str)
                .ok_or("Cargo target omitted its src_path")?;
            let source = fs::canonicalize(source)?;
            let relative = source.strip_prefix(&root).map_err(|_| {
                format!(
                    "Cargo target source is outside the workspace: {}",
                    source.display()
                )
            })?;
            target_sources.insert(relative.to_owned());
        }
    }
    validate_tracked_rust_input_paths(&root, tracked, target_sources)
}

#[cfg(test)]
fn is_rust_path(path: &Path) -> bool {
    path.extension().is_some_and(|extension| extension == "rs")
}

fn validate_tracked_rust_input_paths(
    root: &Path,
    tracked: &HashSet<PathBuf>,
    initial: impl IntoIterator<Item = PathBuf>,
) -> Result<(), Box<dyn Error>> {
    let root = fs::canonicalize(root)?;
    let mut pending = initial
        .into_iter()
        .map(RustSourceContext::target_root)
        .collect::<Vec<_>>();
    let mut parsed = HashMap::new();
    let mut contexts = HashSet::new();
    loop {
        while let Some(context) = pending.pop() {
            if !contexts.insert(context.clone()) {
                continue;
            }
            if parsed.contains_key(&context.relative) {
                continue;
            }
            let source_path = root.join(&context.relative);
            let source = fs::read_to_string(&source_path)?;
            let file = syn::parse_file(&source).map_err(|error| {
                format!(
                    "parse tracked Rust source {}: {error}",
                    source_path.display()
                )
            })?;
            parsed.insert(context.relative, file);
        }

        let mut qualified_aliases = QualifiedAliasMap::new();
        let mut included_aliases = IncludedAliasMap::new();
        for context in &contexts {
            let source_path = root.join(&context.relative);
            let Some(file) = parsed.get(&context.relative) else {
                return Err(format!(
                    "tracked Rust source context was not parsed: {}",
                    source_path.display()
                )
                .into());
            };
            collect_qualified_include_aliases(
                file.items.iter(),
                &context.module_path,
                &mut qualified_aliases,
            );
            if context.kind == RustSourceKind::Include {
                collect_included_alias_scope(
                    file.items.iter(),
                    &context.module_path,
                    &mut included_aliases,
                );
            }
        }
        let mut discovered = HashSet::new();
        for context in &contexts {
            let source_path = root.join(&context.relative);
            let Some(file) = parsed.get(&context.relative) else {
                return Err(format!(
                    "tracked Rust source context was not parsed: {}",
                    source_path.display()
                )
                .into());
            };
            let mut validator = RustIncludeValidator {
                root: &root,
                source_path: &source_path,
                tracked,
                included_aliases: included_aliases.clone(),
                qualified_aliases: qualified_aliases.clone(),
                alias_scopes: vec![collect_alias_scope(file.items.iter())],
                discovered: HashSet::new(),
                module_dir: context.module_dir.clone(),
                module_path: context.module_path.clone(),
                inline_module_depth: 0,
                error: None,
            };
            validator.visit_file(file);
            if let Some(error) = validator.error {
                return Err(error.into());
            }
            discovered.extend(validator.discovered);
        }
        pending.extend(
            discovered
                .into_iter()
                .filter(|context| !contexts.contains(context)),
        );
        if pending.is_empty() {
            return Ok(());
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct RustSourceContext {
    relative: PathBuf,
    module_dir: PathBuf,
    module_path: Vec<String>,
    kind: RustSourceKind,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum RustSourceKind {
    Target,
    Include,
    Module,
}

impl RustSourceContext {
    fn target_root(relative: PathBuf) -> Self {
        let module_dir = relative.parent().unwrap_or(Path::new("")).to_owned();
        Self {
            relative,
            module_dir,
            module_path: Vec::new(),
            kind: RustSourceKind::Target,
        }
    }
}

fn is_include_name(name: &str) -> bool {
    matches!(name, "include" | "include_str" | "include_bytes")
}

fn unraw_ident(ident: &syn::Ident) -> String {
    ident.unraw().to_string()
}

fn path_is_ident(path: &syn::Path, expected: &str) -> bool {
    path.get_ident()
        .is_some_and(|ident| unraw_ident(ident) == expected)
}

struct RustIncludeValidator<'a> {
    root: &'a Path,
    source_path: &'a Path,
    tracked: &'a HashSet<PathBuf>,
    included_aliases: IncludedAliasMap,
    qualified_aliases: QualifiedAliasMap,
    alias_scopes: Vec<AliasScope>,
    discovered: HashSet<RustSourceContext>,
    module_dir: PathBuf,
    module_path: Vec<String>,
    inline_module_depth: usize,
    error: Option<String>,
}

#[cfg(test)]
mod tests;
