use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
};

use syn::{parse::Parser, punctuated::Punctuated, Attribute, ItemMod, Meta, Token};

use crate::TestIdentity;

type ModuleMap = BTreeMap<PathBuf, BTreeSet<Vec<String>>>;
type DeclarationMap = BTreeMap<String, BTreeSet<String>>;
type DeclarationSourceMap = BTreeMap<String, BTreeSet<PathBuf>>;

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
        Ok(())
    }
}

pub(crate) fn target_source_graph(
    workspace: &Path,
    identity: &TestIdentity,
) -> Result<TargetSourceGraph, String> {
    let workspace =
        fs::canonicalize(workspace).map_err(|error| format!("canonicalize workspace: {error}"))?;
    let package = package_manifest(&workspace, &identity.package)?;
    let manifest_source = fs::read_to_string(&package.manifest)
        .map_err(|error| format!("read {}: {error}", package.manifest.display()))?;
    let manifest = manifest_source
        .parse::<toml::Value>()
        .map_err(|error| format!("parse {}: {error}", package.manifest.display()))?;
    let target = target_root(&package.root, &manifest, identity)?;
    let mut collector = ModuleGraphCollector::new(&target.crate_name);
    collector.collect_file(
        &fs::canonicalize(&target.path).map_err(|error| {
            format!(
                "canonicalize target root {}: {error}",
                target.path.display()
            )
        })?,
        &[],
        target
            .path
            .parent()
            .ok_or_else(|| format!("target root has no parent: {}", target.path.display()))?,
        target
            .path
            .parent()
            .ok_or_else(|| format!("target root has no parent: {}", target.path.display()))?,
    )?;
    let (modules, declarations, declaration_sources) = collector.finish();
    Ok(TargetSourceGraph {
        crate_name: target.crate_name,
        modules,
        declarations,
        declaration_sources,
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
    identity: &TestIdentity,
) -> Result<TargetRoot, String> {
    let package_name = manifest
        .get("package")
        .and_then(|package| package.get("name"))
        .and_then(toml::Value::as_str)
        .ok_or("package manifest omits package.name")?;
    let normalized_package = package_name.replace('-', "_");
    let (crate_name, relative) = match identity.target_kind.as_str() {
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
            let table_name = if identity.target_kind == "bin" {
                "bin"
            } else {
                "test"
            };
            let configured = manifest
                .get(table_name)
                .and_then(toml::Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(toml::Value::as_table)
                .find(|table| {
                    table.get("name").and_then(toml::Value::as_str)
                        == Some(identity.target.as_str())
                });
            let default = if identity.target_kind == "bin" {
                if identity.target == package_name {
                    PathBuf::from("src/main.rs")
                } else {
                    PathBuf::from("src/bin").join(format!("{}.rs", identity.target))
                }
            } else {
                PathBuf::from("tests").join(format!("{}.rs", identity.target))
            };
            let path = configured
                .and_then(|table| table.get("path"))
                .and_then(toml::Value::as_str)
                .map_or(default, PathBuf::from);
            (identity.target.replace('-', "_"), path)
        }
        kind => return Err(format!("unsupported registered target kind {kind}")),
    };
    if crate_name != identity.target.replace('-', "_") {
        return Err(format!(
            "registered target {} disagrees with manifest crate name {crate_name}",
            identity.target
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
    visited: BTreeSet<(PathBuf, Vec<String>)>,
    modules: ModuleMap,
    declarations: DeclarationMap,
    declaration_sources: DeclarationSourceMap,
}

impl<'a> ModuleGraphCollector<'a> {
    fn new(crate_name: &'a str) -> Self {
        Self {
            crate_name,
            visited: BTreeSet::new(),
            modules: BTreeMap::new(),
            declarations: BTreeMap::new(),
            declaration_sources: BTreeMap::new(),
        }
    }

    fn finish(self) -> (ModuleMap, DeclarationMap, DeclarationSourceMap) {
        (self.modules, self.declarations, self.declaration_sources)
    }

    fn collect_file(
        &mut self,
        source_file: &Path,
        module: &[String],
        path_base: &Path,
        module_dir: &Path,
    ) -> Result<(), String> {
        let key = (source_file.to_owned(), module.to_vec());
        if !self.visited.insert(key) {
            return Ok(());
        }
        self.modules
            .entry(source_file.to_owned())
            .or_default()
            .insert(module.to_vec());
        let source = fs::read_to_string(source_file)
            .map_err(|error| format!("read module source {}: {error}", source_file.display()))?;
        let file = syn::parse_file(&source)
            .map_err(|error| format!("parse module source {}: {error}", source_file.display()))?;
        self.collect_items(&file.items, module, path_base, module_dir, source_file)
    }

    fn collect_items(
        &mut self,
        items: &[syn::Item],
        module: &[String],
        path_base: &Path,
        module_dir: &Path,
        source_file: &Path,
    ) -> Result<(), String> {
        for item in items {
            match item {
                syn::Item::Fn(function) => {
                    if !module_active_for_test(&function.attrs)? {
                        continue;
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
                    self.declarations.entry(name).or_default().insert(identity);
                    self.declaration_sources
                        .entry(test_identity)
                        .or_default()
                        .insert(source_file.to_owned());
                }
                syn::Item::Mod(item) => {
                    if !module_active_for_test(&item.attrs)? {
                        continue;
                    }
                    let mut child_module = module.to_vec();
                    child_module.push(item.ident.to_string());
                    if let Some((_, inline_items)) = &item.content {
                        let inline_dir = module_dir.join(item.ident.to_string());
                        self.collect_items(
                            inline_items,
                            &child_module,
                            &inline_dir,
                            &inline_dir,
                            source_file,
                        )?;
                        continue;
                    }
                    let child_file = resolve_external_module(item, path_base, module_dir)?;
                    let child_file = fs::canonicalize(&child_file).map_err(|error| {
                        format!("canonicalize module {}: {error}", child_file.display())
                    })?;
                    let child_dir = if child_file.file_name().and_then(std::ffi::OsStr::to_str)
                        == Some("mod.rs")
                    {
                        child_file.parent().unwrap_or(module_dir).to_owned()
                    } else {
                        module_dir.join(item.ident.to_string())
                    };
                    let child_path_base = child_file.parent().unwrap_or(path_base);
                    self.collect_file(&child_file, &child_module, child_path_base, &child_dir)?;
                }
                _ => {}
            }
        }
        Ok(())
    }
}

fn module_active_for_test(attributes: &[Attribute]) -> Result<bool, String> {
    attributes
        .iter()
        .map(|attribute| meta_keeps_item_active(&attribute.meta))
        .try_fold(true, |active, keep| keep.map(|keep| active && keep))
}

fn meta_keeps_item_active(meta: &Meta) -> Result<bool, String> {
    let Meta::List(list) = meta else {
        return Ok(true);
    };
    if list.path.is_ident("cfg") {
        let predicate = syn::parse2::<Meta>(list.tokens.clone())
            .map_err(|error| format!("parse cfg predicate: {error}"))?;
        return cfg_value_for_test(&predicate).into_result();
    }
    if !list.path.is_ident("cfg_attr") {
        return Ok(true);
    }
    let arguments = Punctuated::<Meta, Token![,]>::parse_terminated
        .parse2(list.tokens.clone())
        .map_err(|error| format!("parse cfg_attr arguments: {error}"))?;
    let mut arguments = arguments.iter();
    let predicate = arguments.next().ok_or("cfg_attr requires a predicate")?;
    if !cfg_value_for_test(predicate).into_result()? {
        return Ok(true);
    }
    arguments
        .map(meta_keeps_item_active)
        .try_fold(true, |active, keep| keep.map(|keep| active && keep))
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum CfgValue {
    False,
    True,
    Unknown,
}

impl CfgValue {
    fn into_result(self) -> Result<bool, String> {
        match self {
            Self::False => Ok(false),
            Self::True => Ok(true),
            Self::Unknown => Err(
                "registered Cargo target uses a cfg predicate outside the reviewed test context"
                    .to_owned(),
            ),
        }
    }
}

fn cfg_value_for_test(predicate: &Meta) -> CfgValue {
    match predicate {
        Meta::Path(path) => cfg_path_value_for_test(path),
        Meta::List(list) if list.path.is_ident("any") || list.path.is_ident("all") => {
            let Ok(items) =
                Punctuated::<Meta, Token![,]>::parse_terminated.parse2(list.tokens.clone())
            else {
                return CfgValue::Unknown;
            };
            if list.path.is_ident("any") {
                if items
                    .iter()
                    .any(|item| cfg_value_for_test(item) == CfgValue::True)
                {
                    CfgValue::True
                } else if items
                    .iter()
                    .all(|item| cfg_value_for_test(item) == CfgValue::False)
                {
                    CfgValue::False
                } else {
                    CfgValue::Unknown
                }
            } else if items
                .iter()
                .any(|item| cfg_value_for_test(item) == CfgValue::False)
            {
                CfgValue::False
            } else if items
                .iter()
                .all(|item| cfg_value_for_test(item) == CfgValue::True)
            {
                CfgValue::True
            } else {
                CfgValue::Unknown
            }
        }
        Meta::List(list) if list.path.is_ident("not") => {
            match syn::parse2::<Meta>(list.tokens.clone()).map(|item| cfg_value_for_test(&item)) {
                Ok(CfgValue::True) => CfgValue::False,
                Ok(CfgValue::False) => CfgValue::True,
                Ok(CfgValue::Unknown) | Err(_) => CfgValue::Unknown,
            }
        }
        Meta::NameValue(value) => cfg_name_value_for_test(value),
        Meta::List(_) => CfgValue::Unknown,
    }
}

fn cfg_path_value_for_test(path: &syn::Path) -> CfgValue {
    if path.is_ident("test") || path.is_ident("debug_assertions") {
        CfgValue::True
    } else if path.is_ident("unix") {
        bool_cfg(cfg!(unix))
    } else if path.is_ident("windows") {
        bool_cfg(cfg!(windows))
    } else if path.is_ident("doctest") || path.is_ident("miri") {
        CfgValue::False
    } else {
        CfgValue::Unknown
    }
}

fn cfg_name_value_for_test(value: &syn::MetaNameValue) -> CfgValue {
    let syn::Expr::Lit(expression) = &value.value else {
        return CfgValue::Unknown;
    };
    let syn::Lit::Str(expected) = &expression.lit else {
        return CfgValue::Unknown;
    };
    let expected = expected.value();
    if value.path.is_ident("feature") {
        return CfgValue::False;
    }
    let actual = if value.path.is_ident("target_arch") {
        Some(std::env::consts::ARCH)
    } else if value.path.is_ident("target_os") {
        Some(std::env::consts::OS)
    } else if value.path.is_ident("target_family") {
        if cfg!(unix) {
            Some("unix")
        } else if cfg!(windows) {
            Some("windows")
        } else {
            None
        }
    } else if value.path.is_ident("target_endian") {
        if cfg!(target_endian = "little") {
            Some("little")
        } else {
            Some("big")
        }
    } else if value.path.is_ident("target_pointer_width") {
        return bool_cfg(expected.parse::<u32>() == Ok(usize::BITS));
    } else if value.path.is_ident("panic") {
        if cfg!(panic = "unwind") {
            Some("unwind")
        } else {
            Some("abort")
        }
    } else {
        None
    };
    actual.map_or(CfgValue::Unknown, |actual| bool_cfg(actual == expected))
}

const fn bool_cfg(value: bool) -> CfgValue {
    if value {
        CfgValue::True
    } else {
        CfgValue::False
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
    if cfg_value_for_test(predicate).into_result()? {
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
    let syn::Lit::Str(path) = &expression.lit else {
        return None;
    };
    Some(PathBuf::from(path.value()))
}
