//! Recursive Rust module traversal for authenticated Cargo targets.

mod items;
mod module_path;

use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    fs,
    path::{Path, PathBuf},
};

use super::policy::OracleSourcePolicy;

type ModuleMap = BTreeMap<PathBuf, BTreeSet<Vec<String>>>;
type DeclarationMap = BTreeMap<String, BTreeSet<String>>;
type DeclarationSourceMap = BTreeMap<String, BTreeSet<PathBuf>>;
type OracleShadowMap = BTreeMap<String, BTreeSet<PathBuf>>;

pub(super) struct OracleShadowImplMethod {
    pub(super) module: Vec<String>,
    pub(super) self_ty: syn::Type,
    pub(super) name: String,
    pub(super) source: PathBuf,
}

pub(super) struct CollectedModuleGraph {
    pub(super) modules: ModuleMap,
    pub(super) declarations: DeclarationMap,
    pub(super) declaration_sources: DeclarationSourceMap,
    pub(super) oracle_shadow_sources: OracleShadowMap,
    pub(super) oracle_shadow_impl_methods: Vec<OracleShadowImplMethod>,
}

pub(super) struct ModuleGraphCollector<'a> {
    pub(super) crate_name: &'a str,
    workspace: &'a Path,
    tracked: &'a HashSet<PathBuf>,
    pub(super) policy: OracleSourcePolicy<'a>,
    visited: BTreeSet<(PathBuf, Vec<String>)>,
    modules: ModuleMap,
    pub(super) declarations: DeclarationMap,
    pub(super) declaration_sources: DeclarationSourceMap,
    pub(super) oracle_shadow_sources: OracleShadowMap,
    pub(super) oracle_shadow_impl_methods: Vec<OracleShadowImplMethod>,
}

impl<'a> ModuleGraphCollector<'a> {
    pub(super) fn new(
        crate_name: &'a str,
        workspace: &'a Path,
        tracked: &'a HashSet<PathBuf>,
        reserved_macros: &[&str],
    ) -> Self {
        Self {
            crate_name,
            workspace,
            tracked,
            policy: OracleSourcePolicy::new(crate_name, workspace, reserved_macros),
            visited: BTreeSet::new(),
            modules: BTreeMap::new(),
            declarations: BTreeMap::new(),
            declaration_sources: BTreeMap::new(),
            oracle_shadow_sources: BTreeMap::new(),
            oracle_shadow_impl_methods: Vec::new(),
        }
    }

    pub(super) fn finish(self) -> CollectedModuleGraph {
        CollectedModuleGraph {
            modules: self.modules,
            declarations: self.declarations,
            declaration_sources: self.declaration_sources,
            oracle_shadow_sources: self.oracle_shadow_sources,
            oracle_shadow_impl_methods: self.oracle_shadow_impl_methods,
        }
    }

    pub(super) fn collect_file(
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

    pub(super) fn bound_source_path(&self, path: &Path) -> Result<PathBuf, String> {
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
