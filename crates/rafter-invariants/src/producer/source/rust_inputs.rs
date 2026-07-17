use std::{
    collections::{HashMap, HashSet},
    error::Error,
    fs,
    path::{Path, PathBuf},
};

use syn::{
    ext::IdentExt, parse::Parser, punctuated::Punctuated, visit::Visit, Attribute, Expr, Macro,
    Meta, Token,
};

use super::path_validation::validate_tracked_source_path;

mod aliases;
mod cfg_eval;
mod macro_scan;

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

impl RustIncludeValidator<'_> {
    fn included_alias(&self, name: &str) -> ScopedAlias {
        match self
            .included_aliases
            .get(&alias_path_key(&self.module_path, name))
        {
            Some(Some(canonical)) => ScopedAlias::Include(canonical.clone()),
            Some(None) => ScopedAlias::Shadowed,
            None => ScopedAlias::Unbound,
        }
    }

    fn qualified_alias(&self, invocation: &Macro) -> Option<String> {
        if invocation.path.get_ident().is_some() {
            return None;
        }
        let mut path = Vec::new();
        let mut segments = invocation
            .path
            .segments
            .iter()
            .map(|segment| unraw_ident(&segment.ident))
            .peekable();
        let original_segments = invocation
            .path
            .segments
            .iter()
            .map(|segment| unraw_ident(&segment.ident))
            .collect::<Vec<_>>();
        match resolve_scoped_qualified_alias(&self.alias_scopes, &original_segments.join("::")) {
            ScopedAlias::Include(canonical) => return Some(canonical),
            ScopedAlias::Shadowed => return None,
            ScopedAlias::Unbound => {}
        }
        match segments.peek().map(String::as_str) {
            Some("crate") => {
                segments.next();
            }
            Some("self") => {
                path.extend(self.module_path.iter().cloned());
                segments.next();
            }
            Some("super") => {
                path.extend(self.module_path.iter().cloned());
                while matches!(segments.peek().map(String::as_str), Some("super")) {
                    segments.next();
                    path.pop();
                }
            }
            Some(_) => path.extend(self.module_path.iter().cloned()),
            None => return None,
        }
        path.extend(segments);
        self.qualified_aliases.get(&path.join("::")).cloned()
    }

    fn validate_include(&mut self, invocation: &Macro, name: &str) -> Result<(), String> {
        let arguments = Punctuated::<Expr, Token![,]>::parse_terminated
            .parse2(invocation.tokens.clone())
            .map_err(|error| format!("parse {name}! input: {error}"))?
            .into_iter()
            .collect::<Vec<_>>();
        let [Expr::Lit(expression)] = arguments.as_slice() else {
            return Err(format!(
                "{name}! in {} must use one literal tracked path",
                self.source_path.display()
            ));
        };
        let syn::Lit::Str(path) = &expression.lit else {
            return Err(format!(
                "{name}! in {} must use one string literal tracked path",
                self.source_path.display()
            ));
        };
        let parent = self.source_path.parent().ok_or_else(|| {
            format!(
                "tracked source has no parent: {}",
                self.source_path.display()
            )
        })?;
        let input = parent.join(path.value());
        validate_tracked_source_path(self.root, &input, self.tracked, &format!("{name}! input"))
            .map_err(|error| error.to_string())?;
        if name == "include" {
            self.record_discovered_rust_source(
                &input,
                self.module_dir.clone(),
                self.module_path.clone(),
                RustSourceKind::Include,
            )?;
        }
        Ok(())
    }

    fn validate_path_meta(&self, meta: &Meta) -> Result<PathBuf, String> {
        let Meta::NameValue(value) = meta else {
            return Err(format!(
                "#[path] in {} must use one literal tracked path",
                self.source_path.display()
            ));
        };
        let Expr::Lit(expression) = &value.value else {
            return Err(format!(
                "#[path] in {} must use one literal tracked path",
                self.source_path.display()
            ));
        };
        let syn::Lit::Str(path) = &expression.lit else {
            return Err(format!(
                "#[path] in {} must use one string literal tracked path",
                self.source_path.display()
            ));
        };
        let parent = self.source_path.parent().ok_or_else(|| {
            format!(
                "tracked source has no parent: {}",
                self.source_path.display()
            )
        })?;
        let input = parent.join(path.value());
        validate_tracked_source_path(self.root, &input, self.tracked, "#[path] module input")
            .map_err(|error| error.to_string())?;
        Ok(input)
    }

    fn effective_path_metas(&self, attributes: &[Attribute]) -> Result<Vec<Meta>, String> {
        let mut paths = Vec::new();
        for attribute in attributes {
            walk_effective_metas(&attribute.meta, CfgValue::True, &mut |meta, guard| {
                if path_is_ident(meta.path(), "path") {
                    match guard {
                        CfgValue::True => paths.push(meta.clone()),
                        CfgValue::Unknown => {
                            return Err(format!(
                                "target-conditional #[path] in {} is outside the source binding contract",
                                self.source_path.display()
                            ));
                        }
                        CfgValue::False => {}
                    }
                }
                Ok(())
            })?;
        }
        Ok(paths)
    }

    fn record_discovered_rust_source(
        &mut self,
        path: &Path,
        module_dir: PathBuf,
        module_path: Vec<String>,
        kind: RustSourceKind,
    ) -> Result<(), String> {
        let canonical = fs::canonicalize(path).map_err(|error| {
            format!(
                "canonicalize discovered Rust input {}: {error}",
                path.display()
            )
        })?;
        let relative = canonical.strip_prefix(self.root).map_err(|_| {
            format!(
                "discovered Rust input is outside source root: {}",
                canonical.display()
            )
        })?;
        self.discovered.insert(RustSourceContext {
            relative: relative.to_owned(),
            module_dir,
            module_path,
            kind,
        });
        Ok(())
    }

    fn discover_default_module(&mut self, item: &syn::ItemMod) -> Result<(), String> {
        let name = unraw_ident(&item.ident);
        let candidates = [
            self.root.join(&self.module_dir).join(format!("{name}.rs")),
            self.root.join(&self.module_dir).join(&name).join("mod.rs"),
        ];
        let existing = candidates
            .into_iter()
            .filter(|candidate| candidate.is_file())
            .collect::<Vec<_>>();
        let path = match existing.as_slice() {
            [] => return Ok(()),
            [path] => path,
            _ => {
                return Err(format!(
                    "module {name} in {} resolves to more than one source file",
                    self.source_path.display()
                ));
            }
        };
        validate_tracked_source_path(self.root, path, self.tracked, "module input")
            .map_err(|error| error.to_string())?;
        let child_module_dir =
            if path.file_name().and_then(std::ffi::OsStr::to_str) == Some("mod.rs") {
                path.strip_prefix(self.root)
                    .ok()
                    .and_then(Path::parent)
                    .unwrap_or(Path::new(""))
                    .to_owned()
            } else {
                self.module_dir.join(&name)
            };
        let mut child_module_path = self.module_path.clone();
        child_module_path.push(name);
        self.record_discovered_rust_source(
            path,
            child_module_dir,
            child_module_path,
            RustSourceKind::Module,
        )
    }

    fn discover_path_module(
        &mut self,
        item: &syn::ItemMod,
        path_meta: &Meta,
    ) -> Result<(), String> {
        let input = self.validate_path_meta(path_meta)?;
        let canonical = fs::canonicalize(&input).map_err(|error| {
            format!(
                "canonicalize discovered Rust input {}: {error}",
                input.display()
            )
        })?;
        let child_module_dir =
            if canonical.file_name().and_then(std::ffi::OsStr::to_str) == Some("mod.rs") {
                canonical
                    .strip_prefix(self.root)
                    .ok()
                    .and_then(Path::parent)
                    .unwrap_or(Path::new(""))
                    .to_owned()
            } else {
                self.module_dir.join(unraw_ident(&item.ident))
            };
        let mut child_module_path = self.module_path.clone();
        child_module_path.push(unraw_ident(&item.ident));
        self.record_discovered_rust_source(
            &input,
            child_module_dir,
            child_module_path,
            RustSourceKind::Module,
        )
    }
}

impl<'ast> Visit<'ast> for RustIncludeValidator<'_> {
    fn visit_item(&mut self, item: &'ast syn::Item) {
        if self.error.is_some() {
            return;
        }
        match item_is_definitively_inactive(item) {
            Ok(true) => return,
            Ok(false) => {}
            Err(error) => {
                self.error = Some(format!(
                    "analyze cfg attributes in {}: {error}",
                    self.source_path.display()
                ));
                return;
            }
        }
        syn::visit::visit_item(self, item);
    }

    fn visit_macro(&mut self, invocation: &'ast Macro) {
        let unqualified = invocation.path.get_ident().map(unraw_ident);
        let visible_aliases = visible_include_aliases(&self.alias_scopes);
        let canonical = unqualified
            .as_ref()
            .and_then(
                |name| match resolve_scoped_alias(&self.alias_scopes, name) {
                    ScopedAlias::Include(canonical) => Some(canonical),
                    ScopedAlias::Shadowed => None,
                    ScopedAlias::Unbound => match self.included_alias(name) {
                        ScopedAlias::Include(canonical) => Some(canonical),
                        ScopedAlias::Unbound if is_include_name(name) => Some(name.clone()),
                        ScopedAlias::Shadowed | ScopedAlias::Unbound => None,
                    },
                },
            )
            .or_else(|| self.qualified_alias(invocation));
        let qualified_include = invocation
            .path
            .segments
            .last()
            .is_some_and(|segment| is_include_name(&unraw_ident(&segment.ident)))
            && unqualified.is_none();
        if self.error.is_none() && qualified_include {
            self.error = Some(format!(
                "qualified include macros in {} are outside the source binding contract",
                self.source_path.display()
            ));
        } else if self.error.is_none() {
            if let Some(name) = canonical.as_deref() {
                self.error = self.validate_include(invocation, name).err();
            }
        }
        let macro_generated_input = path_is_ident(&invocation.path, "macro_rules")
            && (contains_include_reference(invocation, &visible_aliases)
                || contains_dynamic_macro_invocation(invocation)
                || contains_generated_out_of_line_module(invocation))
            || canonical.is_none() && contains_out_of_line_module_argument(invocation);
        if self.error.is_none() && macro_generated_input {
            self.error = Some(format!(
                "macro-generated compiler inputs in {} are outside the source binding contract",
                self.source_path.display()
            ));
        } else if self.error.is_none()
            && canonical.is_none()
            && contains_include_invocation(invocation, &visible_aliases)
        {
            match Punctuated::<Expr, Token![,]>::parse_terminated.parse2(invocation.tokens.clone())
            {
                Ok(expressions) => {
                    for expression in &expressions {
                        self.visit_expr(expression);
                        if self.error.is_some() {
                            break;
                        }
                    }
                    return;
                }
                Err(_) => {
                    self.error = Some(format!(
                        "include macros nested in opaque macro input in {} are outside the source binding contract",
                        self.source_path.display()
                    ));
                }
            }
        }
        if self.error.is_none() {
            syn::visit::visit_macro(self, invocation);
        }
    }

    fn visit_block(&mut self, block: &'ast syn::Block) {
        let scope = collect_alias_scope(block.stmts.iter().filter_map(|statement| {
            if let syn::Stmt::Item(item) = statement {
                Some(item)
            } else {
                None
            }
        }));
        self.alias_scopes.push(scope);
        syn::visit::visit_block(self, block);
        self.alias_scopes.pop();
    }

    fn visit_attribute(&mut self, attribute: &'ast Attribute) {
        if self.error.is_none() {
            match self.effective_path_metas(std::slice::from_ref(attribute)) {
                Ok(paths) if self.inline_module_depth > 0 && !paths.is_empty() => {
                    self.error = Some(format!(
                        "#[path] inside an inline module in {} is outside the portable source binding contract",
                        self.source_path.display()
                    ));
                }
                Ok(paths) => {
                    for path_meta in paths {
                        let result = self.validate_path_meta(&path_meta).and_then(|input| {
                            self.record_discovered_rust_source(
                                &input,
                                self.module_dir.clone(),
                                self.module_path.clone(),
                                RustSourceKind::Module,
                            )
                        });
                        if let Err(error) = result {
                            self.error = Some(error);
                            break;
                        }
                    }
                }
                Err(error) => self.error = Some(error),
            }
        }
        if self.error.is_none() {
            syn::visit::visit_attribute(self, attribute);
        }
    }

    fn visit_item_mod(&mut self, item: &'ast syn::ItemMod) {
        let path_metas = match self.effective_path_metas(&item.attrs) {
            Ok(paths) => paths,
            Err(error) => {
                self.error = Some(error);
                return;
            }
        };
        if path_metas.len() > 1 {
            self.error = Some(format!(
                "module {} in {} has more than one #[path] attribute",
                unraw_ident(&item.ident),
                self.source_path.display()
            ));
            return;
        }
        if self.inline_module_depth > 0 && !path_metas.is_empty() {
            self.error = Some(format!(
                "#[path] inside an inline module in {} is outside the portable source binding contract",
                self.source_path.display()
            ));
            return;
        }
        if let Some((_, items)) = &item.content {
            let previous_module_dir = self.module_dir.clone();
            let previous_module_path = self.module_path.clone();
            self.module_dir.push(unraw_ident(&item.ident));
            self.module_path.push(unraw_ident(&item.ident));
            self.inline_module_depth += 1;
            self.alias_scopes.push(collect_alias_scope(items.iter()));
            for item in items {
                self.visit_item(item);
                if self.error.is_some() {
                    break;
                }
            }
            self.alias_scopes.pop();
            self.inline_module_depth -= 1;
            self.module_dir = previous_module_dir;
            self.module_path = previous_module_path;
        } else if let Some(path_meta) = path_metas.first() {
            self.error = self.discover_path_module(item, path_meta).err();
        } else {
            self.error = self.discover_default_module(item).err();
        }
    }
}

#[cfg(test)]
mod tests;
