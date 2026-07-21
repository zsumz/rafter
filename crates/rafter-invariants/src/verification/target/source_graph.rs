//! Authenticated source graph construction and query model.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
};

use super::{
    cargo_target::resolve_registered_target,
    traversal::{CollectedModuleGraph, ModuleGraphCollector},
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SourceModule {
    pub(crate) crate_name: String,
    pub(crate) module: Vec<String>,
}

pub(crate) struct TargetSourceGraph {
    crate_name: String,
    modules: BTreeMap<PathBuf, BTreeSet<Vec<String>>>,
    declarations: BTreeMap<String, BTreeSet<String>>,
    declaration_sources: BTreeMap<String, BTreeSet<PathBuf>>,
    oracle_shadow_sources: BTreeMap<String, BTreeSet<PathBuf>>,
    oracle_shadow_impl_methods: Vec<super::traversal::OracleShadowImplMethod>,
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
    let tracked = match crate::verification::source::authenticated_snapshot_paths(&workspace)? {
        Some(paths) => paths,
        None => crate::provenance::source::tracked_source_paths_at(&workspace)?,
    };
    let target = resolve_registered_target(&workspace, package_name, target_kind, target_name)?;
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
    let CollectedModuleGraph {
        modules,
        declarations,
        declaration_sources,
        oracle_shadow_sources,
        oracle_shadow_impl_methods,
    } = collector.finish();
    let graph = TargetSourceGraph {
        crate_name: target.crate_name,
        modules,
        declarations,
        declaration_sources,
        oracle_shadow_sources,
        oracle_shadow_impl_methods,
    };
    crate::verification::source::revalidate_authenticated_snapshot(&workspace)?;
    Ok(graph)
}
