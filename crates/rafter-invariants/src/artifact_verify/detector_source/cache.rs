use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    rc::Rc,
};

use syn::File;

use super::{
    collect_functions,
    function_index::{FunctionIndex, LocalCallResolver},
    imports::collect_imports,
};

#[derive(Default)]
pub(crate) struct DetectorSourceCache {
    targets: BTreeMap<TargetCacheKey, CachedTargetAnalysis>,
    sources: BTreeMap<PathBuf, CachedSource>,
    target_analysis_count: usize,
    source_parse_count: usize,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct TargetCacheKey {
    source_root: PathBuf,
    package: String,
    target_kind: String,
    target: String,
}

pub(super) struct CachedTargetAnalysis {
    pub(super) graph: crate::rust_target::TargetSourceGraph,
    pub(super) resolver: LocalCallResolver,
    pub(super) functions: FunctionIndex,
}

struct CachedSource {
    source: String,
    file: Rc<File>,
}

impl DetectorSourceCache {
    pub(super) fn source(
        &mut self,
        path: &Path,
        expected: &str,
        label: &str,
    ) -> Result<Rc<File>, String> {
        let canonical = std::fs::canonicalize(path).map_err(|error| {
            format!(
                "canonicalize bound {label} source {}: {error}",
                path.display()
            )
        })?;
        let actual = std::fs::read_to_string(&canonical).map_err(|error| {
            format!("read bound {label} source {}: {error}", canonical.display())
        })?;
        if actual != expected {
            return Err(format!(
                "provided {label} source does not match bound path {}",
                canonical.display()
            ));
        }
        if let Some(cached) = self.sources.get(&canonical) {
            if cached.source == actual {
                return Ok(Rc::clone(&cached.file));
            }
        }
        let file = Rc::new(
            syn::parse_file(&actual)
                .map_err(|error| format!("parse registered {label} source: {error}"))?,
        );
        self.source_parse_count += 1;
        self.sources.insert(
            canonical,
            CachedSource {
                source: actual,
                file: Rc::clone(&file),
            },
        );
        Ok(file)
    }

    pub(super) fn target(
        &mut self,
        binding: &crate::DetectorFixtureSourceBinding<'_>,
    ) -> Result<&CachedTargetAnalysis, String> {
        let key = TargetCacheKey {
            source_root: binding.source_root.to_owned(),
            package: binding.test_identity.package.clone(),
            target_kind: binding.test_identity.target_kind.clone(),
            target: binding.test_identity.target.clone(),
        };
        if !self.targets.contains_key(&key) {
            let mut graph = crate::rust_target::target_source_graph(
                &key.source_root,
                &key.package,
                &key.target_kind,
                &key.target,
            )?;
            let target_modules = graph.module_paths();
            let target_functions = graph
                .declaration_identities()
                .into_keys()
                .collect::<BTreeSet<_>>();
            let (resolver, functions) =
                collect_target_analysis(&graph, &target_modules, &target_functions)?;
            graph.resolve_oracle_shadowed_impl_methods(|ty, module| {
                resolver.declared_type_module(ty, module)
            });
            self.target_analysis_count += 1;
            self.targets.insert(
                key.clone(),
                CachedTargetAnalysis {
                    graph,
                    resolver,
                    functions,
                },
            );
        }
        self.targets
            .get(&key)
            .ok_or_else(|| "cached detector target analysis disappeared".to_owned())
    }

    #[cfg(test)]
    pub(crate) fn target_analysis_count(&self) -> usize {
        self.target_analysis_count
    }

    #[cfg(test)]
    pub(crate) fn source_parse_count(&self) -> usize {
        self.source_parse_count
    }
}

fn collect_target_analysis(
    graph: &crate::rust_target::TargetSourceGraph,
    target_modules: &BTreeSet<Vec<String>>,
    target_function_names: &BTreeSet<String>,
) -> Result<(LocalCallResolver, FunctionIndex), String> {
    let mut parsed_sources = Vec::new();
    for (source, module) in graph.source_modules() {
        let source_text = std::fs::read_to_string(&source)
            .map_err(|error| format!("read target source {}: {error}", source.display()))?;
        let file = syn::parse_file(&source_text)
            .map_err(|error| format!("parse target source {}: {error}", source.display()))?;
        parsed_sources.push((file, module));
    }
    let mut resolver = LocalCallResolver::default();
    for (file, module) in &parsed_sources {
        resolver.extend(LocalCallResolver::collect(
            file,
            module,
            target_modules,
            target_function_names,
        ));
    }
    resolver.complete_target_graph();
    let mut functions = FunctionIndex::default();
    for (file, module) in &parsed_sources {
        let imports = collect_imports(file);
        functions.extend(collect_functions(file, &imports, &resolver, module, true));
    }
    Ok((resolver, functions))
}
