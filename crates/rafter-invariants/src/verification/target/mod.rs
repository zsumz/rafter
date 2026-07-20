//! Independent Cargo target source identity and protected compiler-artifact policy.

use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    fs,
    path::{Path, PathBuf},
};

use syn::{parse::Parser, punctuated::Punctuated, Attribute, ItemMod, Meta, Token};

mod cfg;
mod protected_compiler;
#[cfg(test)]
mod tests;

pub(crate) use cfg::module_active_for_test;
pub(crate) use protected_compiler::verify_protected_compiler_artifacts;

type ModuleMap = BTreeMap<PathBuf, BTreeSet<Vec<String>>>;
type DeclarationMap = BTreeMap<String, BTreeSet<String>>;
type DeclarationSourceMap = BTreeMap<String, BTreeSet<PathBuf>>;
type OracleShadowMap = BTreeMap<String, BTreeSet<PathBuf>>;

const ORACLE_MACRO_SOURCE: &str = "crates/rafter-invariant-test/src/oracle/macros.rs";
const ORACLE_CALL_SOURCE: &str = "crates/rafter-invariant-test/src/oracle/call.rs";
const DETECTOR_SESSION_SOURCE: &str = "crates/rafter-invariant-test/src/detector/session.rs";

struct OracleShadowImplMethod {
    module: Vec<String>,
    self_ty: syn::Type,
    name: String,
    source: PathBuf,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SourceModule {
    pub(crate) crate_name: String,
    pub(crate) module: Vec<String>,
}

pub(crate) struct TargetSourceGraph {
    crate_name: String,
    modules: ModuleMap,
    declarations: DeclarationMap,
    declaration_sources: DeclarationSourceMap,
    oracle_shadow_sources: OracleShadowMap,
    oracle_shadow_impl_methods: Vec<OracleShadowImplMethod>,
}

impl TargetSourceGraph {
    pub(crate) fn source_module(&self, source: &Path) -> Result<SourceModule, String> {
        let source = fs::canonicalize(source)
            .map_err(|error| format!("canonicalize source {}: {error}", source.display()))?;
        let modules = self
            .modules
            .get(&source)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .collect::<Vec<_>>();
        let [module] = modules.as_slice() else {
            return Err(format!(
                "source {} resolves to {} modules in the registered Cargo target",
                source.display(),
                modules.len()
            ));
        };
        Ok(SourceModule {
            crate_name: self.crate_name.clone(),
            module: module.clone(),
        })
    }

    pub(crate) fn declaration_identities(&self) -> BTreeMap<String, Vec<String>> {
        self.declarations
            .iter()
            .map(|(name, identities)| (name.clone(), identities.iter().cloned().collect()))
            .collect()
    }

    pub(crate) fn module_paths(&self) -> BTreeSet<Vec<String>> {
        self.modules
            .values()
            .flat_map(BTreeSet::iter)
            .cloned()
            .collect()
    }

    pub(crate) fn source_modules(&self) -> Vec<(PathBuf, Vec<String>)> {
        self.modules
            .iter()
            .flat_map(|(source, modules)| {
                modules
                    .iter()
                    .cloned()
                    .map(|module| (source.clone(), module))
            })
            .collect()
    }

    pub(crate) fn require_declaration_source(
        &self,
        identity: &str,
        source: &Path,
    ) -> Result<(), String> {
        let source = fs::canonicalize(source)
            .map_err(|error| format!("canonicalize source {}: {error}", source.display()))?;
        let sources = self
            .declaration_sources
            .get(identity)
            .map(BTreeSet::iter)
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        let [registered_source] = sources.as_slice() else {
            return Err(format!(
                "registered test identity `{identity}` resolves to {} declarations in its Cargo target",
                sources.len()
            ));
        };
        if **registered_source != source {
            return Err(format!(
                "registered test identity `{identity}` is declared in {}, not its bound fixture source {}",
                registered_source.display(),
                source.display()
            ));
        }
        self.require_unshadowed_oracle_macros(identity)
    }

    pub(crate) fn require_unshadowed_oracle_macros(&self, identity: &str) -> Result<(), String> {
        if let Some(sources) = self.oracle_shadow_sources.get(identity) {
            let locations = sources
                .iter()
                .map(|source| source.display().to_string())
                .collect::<Vec<_>>()
                .join(", ");
            return Err(format!(
                "registered Cargo target declares a macro that shadows a trusted oracle macro for function `{identity}` in {locations}"
            ));
        }
        Ok(())
    }

    pub(crate) fn resolve_oracle_shadowed_impl_methods(
        &mut self,
        resolve_type: impl Fn(&syn::Type, &[String]) -> Option<Vec<String>>,
    ) {
        for method in &self.oracle_shadow_impl_methods {
            let Some(mut identity) = resolve_type(&method.self_ty, &method.module) else {
                continue;
            };
            identity.push(method.name.clone());
            self.oracle_shadow_sources
                .entry(identity.join("::"))
                .or_default()
                .insert(method.source.clone());
        }
    }
}

pub(crate) fn target_source_graph(
    workspace: &Path,
    package_name: &str,
    target_kind: &str,
    target_name: &str,
    reserved_macros: &[&str],
) -> Result<TargetSourceGraph, String> {
    let workspace =
        fs::canonicalize(workspace).map_err(|error| format!("canonicalize workspace: {error}"))?;
    let package = package_manifest(&workspace, package_name)?;
    let tracked = crate::provenance::source::tracked_source_paths_at(&workspace)?;
    let manifest_source = fs::read_to_string(&package.manifest)
        .map_err(|error| format!("read {}: {error}", package.manifest.display()))?;
    let manifest = manifest_source
        .parse::<toml::Value>()
        .map_err(|error| format!("parse {}: {error}", package.manifest.display()))?;
    let target = target_root(&package.root, &manifest, target_kind, target_name)?;
    let mut collector =
        ModuleGraphCollector::new(&target.crate_name, &workspace, &tracked, reserved_macros);
    let target_path = collector.bound_source_path(&target.path)?;
    let target_parent = target_path
        .parent()
        .ok_or_else(|| format!("target root has no parent: {}", target_path.display()))?;
    collector.collect_file(
        &target_path,
        &[],
        target_parent,
        target_parent,
        &BTreeSet::new(),
    )?;
    let (
        modules,
        declarations,
        declaration_sources,
        oracle_shadow_sources,
        oracle_shadow_impl_methods,
    ) = collector.finish();
    Ok(TargetSourceGraph {
        crate_name: target.crate_name,
        modules,
        declarations,
        declaration_sources,
        oracle_shadow_sources,
        oracle_shadow_impl_methods,
    })
}

struct PackageManifest {
    root: PathBuf,
    manifest: PathBuf,
}

fn package_manifest(workspace: &Path, package: &str) -> Result<PackageManifest, String> {
    let workspace_manifest = workspace.join("Cargo.toml");
    let source = fs::read_to_string(&workspace_manifest)
        .map_err(|error| format!("read {}: {error}", workspace_manifest.display()))?;
    let manifest = source
        .parse::<toml::Value>()
        .map_err(|error| format!("parse {}: {error}", workspace_manifest.display()))?;
    let mut candidates = vec![workspace.to_owned()];
    if let Some(members) = manifest
        .get("workspace")
        .and_then(|workspace| workspace.get("members"))
        .and_then(toml::Value::as_array)
    {
        for member in members {
            let member = member.as_str().ok_or("workspace member must be a string")?;
            if member.contains(['*', '?', '[', ']']) {
                return Err(format!(
                    "workspace member glob is unsupported for detector identity resolution: {member}"
                ));
            }
            candidates.push(workspace.join(member));
        }
    }
    let mut matches = Vec::new();
    for root in candidates {
        let candidate = root.join("Cargo.toml");
        let Ok(source) = fs::read_to_string(&candidate) else {
            continue;
        };
        let Ok(value) = source.parse::<toml::Value>() else {
            continue;
        };
        if value
            .get("package")
            .and_then(|package| package.get("name"))
            .and_then(toml::Value::as_str)
            == Some(package)
        {
            matches.push(PackageManifest {
                root,
                manifest: candidate,
            });
        }
    }
    let [found] = matches.as_slice() else {
        return Err(format!(
            "registered package {package} resolves to {} workspace manifests",
            matches.len()
        ));
    };
    Ok(PackageManifest {
        root: found.root.clone(),
        manifest: found.manifest.clone(),
    })
}

struct TargetRoot {
    crate_name: String,
    path: PathBuf,
}

fn target_root(
    package: &Path,
    manifest: &toml::Value,
    target_kind: &str,
    target_name: &str,
) -> Result<TargetRoot, String> {
    let package_name = manifest
        .get("package")
        .and_then(|package| package.get("name"))
        .and_then(toml::Value::as_str)
        .ok_or("package manifest omits package.name")?;
    let normalized_package = package_name.replace('-', "_");
    let (crate_name, relative) = match target_kind {
        "lib" => {
            let table = manifest.get("lib").and_then(toml::Value::as_table);
            let name = table
                .and_then(|table| table.get("name"))
                .and_then(toml::Value::as_str)
                .unwrap_or(&normalized_package)
                .to_owned();
            let path = table
                .and_then(|table| table.get("path"))
                .and_then(toml::Value::as_str)
                .unwrap_or("src/lib.rs");
            (name, PathBuf::from(path))
        }
        "bin" | "test" => {
            let table_name = if target_kind == "bin" { "bin" } else { "test" };
            let configured = manifest
                .get(table_name)
                .and_then(toml::Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(toml::Value::as_table)
                .find(|table| table.get("name").and_then(toml::Value::as_str) == Some(target_name));
            let default = if target_kind == "bin" {
                if target_name == package_name {
                    PathBuf::from("src/main.rs")
                } else {
                    PathBuf::from("src/bin").join(format!("{target_name}.rs"))
                }
            } else {
                PathBuf::from("tests").join(format!("{target_name}.rs"))
            };
            let path = configured
                .and_then(|table| table.get("path"))
                .and_then(toml::Value::as_str)
                .map_or(default, PathBuf::from);
            (target_name.replace('-', "_"), path)
        }
        kind => return Err(format!("unsupported registered target kind {kind}")),
    };
    if crate_name != target_name.replace('-', "_") {
        return Err(format!(
            "registered target {target_name} disagrees with manifest crate name {crate_name}"
        ));
    }
    let path = package.join(relative);
    if !path.is_file() {
        return Err(format!(
            "registered target root does not exist: {}",
            path.display()
        ));
    }
    Ok(TargetRoot { crate_name, path })
}

struct ModuleGraphCollector<'a> {
    crate_name: &'a str,
    workspace: &'a Path,
    tracked: &'a HashSet<PathBuf>,
    reserved_macros: BTreeSet<String>,
    visited: BTreeSet<(PathBuf, Vec<String>)>,
    modules: ModuleMap,
    declarations: DeclarationMap,
    declaration_sources: DeclarationSourceMap,
    oracle_shadow_sources: OracleShadowMap,
    oracle_shadow_impl_methods: Vec<OracleShadowImplMethod>,
}

impl<'a> ModuleGraphCollector<'a> {
    fn new(
        crate_name: &'a str,
        workspace: &'a Path,
        tracked: &'a HashSet<PathBuf>,
        reserved_macros: &[&str],
    ) -> Self {
        Self {
            crate_name,
            workspace,
            tracked,
            reserved_macros: reserved_macros
                .iter()
                .map(|name| (*name).to_owned())
                .collect(),
            visited: BTreeSet::new(),
            modules: BTreeMap::new(),
            declarations: BTreeMap::new(),
            declaration_sources: BTreeMap::new(),
            oracle_shadow_sources: BTreeMap::new(),
            oracle_shadow_impl_methods: Vec::new(),
        }
    }

    fn finish(
        self,
    ) -> (
        ModuleMap,
        DeclarationMap,
        DeclarationSourceMap,
        OracleShadowMap,
        Vec<OracleShadowImplMethod>,
    ) {
        (
            self.modules,
            self.declarations,
            self.declaration_sources,
            self.oracle_shadow_sources,
            self.oracle_shadow_impl_methods,
        )
    }

    fn collect_file(
        &mut self,
        source_file: &Path,
        module: &[String],
        path_base: &Path,
        module_dir: &Path,
        visible_oracle_macros: &BTreeSet<String>,
    ) -> Result<(), String> {
        let source_file = self.bound_source_path(source_file)?;
        let key = (source_file.clone(), module.to_vec());
        if !self.visited.insert(key) {
            return Ok(());
        }
        self.modules
            .entry(source_file.clone())
            .or_default()
            .insert(module.to_vec());
        let source = fs::read_to_string(&source_file)
            .map_err(|error| format!("read module source {}: {error}", source_file.display()))?;
        let file = syn::parse_file(&source)
            .map_err(|error| format!("parse module source {}: {error}", source_file.display()))?;
        self.collect_items(
            &file.items,
            module,
            path_base,
            module_dir,
            &source_file,
            visible_oracle_macros,
        )
    }

    fn collect_items(
        &mut self,
        items: &[syn::Item],
        module: &[String],
        path_base: &Path,
        module_dir: &Path,
        source_file: &Path,
        inherited_oracle_macros: &BTreeSet<String>,
    ) -> Result<(), String> {
        let mut visible_oracle_macros = inherited_oracle_macros.clone();
        for item in items {
            match item {
                syn::Item::Fn(function) => {
                    self.collect_function_item(
                        function,
                        module,
                        source_file,
                        &visible_oracle_macros,
                    )?;
                }
                syn::Item::Mod(item) => {
                    self.collect_module_item(
                        item,
                        module,
                        path_base,
                        module_dir,
                        source_file,
                        &visible_oracle_macros,
                    )?;
                }
                syn::Item::Macro(item) => {
                    self.collect_macro_item(item, module, source_file, &mut visible_oracle_macros)?;
                }
                syn::Item::Use(item) => {
                    module_active_for_test(&item.attrs)?;
                }
                syn::Item::ExternCrate(item) => {
                    module_active_for_test(&item.attrs)?;
                    Self::reject_macro_use(&item.attrs, source_file)?;
                }
                syn::Item::Impl(item) => {
                    self.collect_impl_item(item, module, source_file, &visible_oracle_macros)?;
                }
                syn::Item::Trait(item) => Self::validate_trait_item(item, source_file)?,
                _ => {}
            }
        }
        Ok(())
    }

    fn collect_function_item(
        &mut self,
        function: &syn::ItemFn,
        module: &[String],
        source_file: &Path,
        visible_oracle_macros: &BTreeSet<String>,
    ) -> Result<(), String> {
        if !module_active_for_test(&function.attrs)? {
            return Ok(());
        }
        let name = function.sig.ident.to_string();
        let identity = std::iter::once(self.crate_name.to_owned())
            .chain(module.iter().cloned())
            .chain(std::iter::once(name.clone()))
            .collect::<Vec<_>>()
            .join("::");
        let test_identity = module
            .iter()
            .cloned()
            .chain(std::iter::once(name.clone()))
            .collect::<Vec<_>>()
            .join("::");
        self.declarations
            .entry(name.clone())
            .or_default()
            .insert(identity);
        self.declaration_sources
            .entry(test_identity.clone())
            .or_default()
            .insert(source_file.to_owned());
        if !visible_oracle_macros.is_empty() {
            self.oracle_shadow_sources
                .entry(test_identity)
                .or_default()
                .insert(source_file.to_owned());
        }
        Ok(())
    }

    fn collect_module_item(
        &mut self,
        item: &ItemMod,
        module: &[String],
        path_base: &Path,
        module_dir: &Path,
        source_file: &Path,
        visible_oracle_macros: &BTreeSet<String>,
    ) -> Result<(), String> {
        if !module_active_for_test(&item.attrs)? {
            return Ok(());
        }
        Self::reject_macro_use(&item.attrs, source_file)?;
        let mut child_module = module.to_vec();
        child_module.push(item.ident.to_string());
        if let Some((_, inline_items)) = &item.content {
            let inline_dir = module_dir.join(item.ident.to_string());
            return self.collect_items(
                inline_items,
                &child_module,
                &inline_dir,
                &inline_dir,
                source_file,
                visible_oracle_macros,
            );
        }
        let child_file =
            self.bound_source_path(&resolve_external_module(item, path_base, module_dir)?)?;
        let child_dir =
            if child_file.file_name().and_then(std::ffi::OsStr::to_str) == Some("mod.rs") {
                child_file.parent().unwrap_or(module_dir).to_owned()
            } else {
                module_dir.join(item.ident.to_string())
            };
        let child_path_base = child_file.parent().unwrap_or(path_base);
        self.collect_file(
            &child_file,
            &child_module,
            child_path_base,
            &child_dir,
            visible_oracle_macros,
        )
    }

    fn collect_macro_item(
        &self,
        item: &syn::ItemMacro,
        module: &[String],
        source_file: &Path,
        visible_oracle_macros: &mut BTreeSet<String>,
    ) -> Result<(), String> {
        if !module_active_for_test(&item.attrs)? {
            return Ok(());
        }
        if let Some(name) = item
            .ident
            .as_ref()
            .filter(|name| self.reserved_macros.contains(&name.to_string()))
        {
            let canonical_oracle_definition =
                self.canonical_oracle_macro_definition(module, source_file);
            if !canonical_oracle_definition {
                visible_oracle_macros.insert(name.to_string());
            }
        }
        if item.ident.is_none() && !self.reviewed_support_item_macro(item, source_file) {
            return Err(format!(
                "registered Cargo target uses an unexpanded item macro outside the reviewed module graph in {}",
                source_file.display()
            ));
        }
        Ok(())
    }

    fn reject_macro_use(attributes: &[Attribute], source_file: &Path) -> Result<(), String> {
        if attributes
            .iter()
            .any(|attribute| attribute.path().is_ident("macro_use"))
        {
            return Err(format!(
                "registered Cargo target uses #[macro_use] outside the reviewed lexical macro graph in {}",
                source_file.display()
            ));
        }
        Ok(())
    }

    fn collect_impl_item(
        &mut self,
        item: &syn::ItemImpl,
        module: &[String],
        source_file: &Path,
        visible_oracle_macros: &BTreeSet<String>,
    ) -> Result<(), String> {
        if !module_active_for_test(&item.attrs)? {
            return Ok(());
        }
        for member in &item.items {
            let attributes = match member {
                syn::ImplItem::Fn(method) => {
                    if module_active_for_test(&method.attrs)? && !visible_oracle_macros.is_empty() {
                        self.oracle_shadow_impl_methods
                            .push(OracleShadowImplMethod {
                                module: module.to_vec(),
                                self_ty: (*item.self_ty).clone(),
                                name: method.sig.ident.to_string(),
                                source: source_file.to_owned(),
                            });
                    }
                    continue;
                }
                syn::ImplItem::Const(item) => &item.attrs,
                syn::ImplItem::Type(item) => &item.attrs,
                syn::ImplItem::Macro(item) => {
                    module_active_for_test(&item.attrs)?;
                    return Err(format!(
                        "registered Cargo target uses an unexpanded impl macro outside the reviewed module graph in {}",
                        source_file.display()
                    ));
                }
                _ => continue,
            };
            module_active_for_test(attributes)?;
        }
        Ok(())
    }

    fn validate_trait_item(item: &syn::ItemTrait, source_file: &Path) -> Result<(), String> {
        if !module_active_for_test(&item.attrs)? {
            return Ok(());
        }
        for member in &item.items {
            let attributes = match member {
                syn::TraitItem::Fn(item) => &item.attrs,
                syn::TraitItem::Const(item) => &item.attrs,
                syn::TraitItem::Type(item) => &item.attrs,
                syn::TraitItem::Macro(item) => {
                    module_active_for_test(&item.attrs)?;
                    return Err(format!(
                        "registered Cargo target uses an unexpanded trait macro outside the reviewed module graph in {}",
                        source_file.display()
                    ));
                }
                _ => continue,
            };
            module_active_for_test(attributes)?;
        }
        Ok(())
    }

    fn reviewed_support_item_macro(&self, item: &syn::ItemMacro, source_file: &Path) -> bool {
        if self.crate_name != "rafter_invariant_test" {
            return false;
        }
        let Ok(source) = source_file.strip_prefix(self.workspace) else {
            return false;
        };
        let path = item
            .mac
            .path
            .segments
            .iter()
            .map(|segment| segment.ident.to_string())
            .collect::<Vec<_>>();
        match path.as_slice() {
            [name] if name == "impl_oracle_call" => source == Path::new(ORACLE_CALL_SOURCE),
            [krate, name] if krate == "std" && name == "thread_local" => {
                source == Path::new(DETECTOR_SESSION_SOURCE)
            }
            _ => false,
        }
    }

    fn canonical_oracle_macro_definition(&self, module: &[String], source_file: &Path) -> bool {
        self.crate_name == "rafter_invariant_test"
            && module.iter().map(String::as_str).eq(["oracle", "macros"])
            && source_file.strip_prefix(self.workspace).ok() == Some(Path::new(ORACLE_MACRO_SOURCE))
    }

    fn bound_source_path(&self, path: &Path) -> Result<PathBuf, String> {
        let metadata = fs::symlink_metadata(path)
            .map_err(|error| format!("inspect module source {}: {error}", path.display()))?;
        if !metadata.file_type().is_file() {
            return Err(format!(
                "module source is not a regular file: {}",
                path.display()
            ));
        }
        let canonical = fs::canonicalize(path)
            .map_err(|error| format!("canonicalize module source {}: {error}", path.display()))?;
        if canonical != path {
            return Err(format!(
                "module source traverses a filesystem alias or noncanonical path: {}",
                path.display()
            ));
        }
        let relative = canonical.strip_prefix(self.workspace).map_err(|_| {
            format!(
                "module source is outside the bound source tree: {}",
                canonical.display()
            )
        })?;
        if !self.tracked.contains(relative) {
            return Err(format!(
                "module source is not tracked by the bound source tree: {}",
                canonical.display()
            ));
        }
        Ok(canonical)
    }
}

fn resolve_external_module(
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
    if cfg::cfg_predicate_active_for_test(predicate)? {
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
