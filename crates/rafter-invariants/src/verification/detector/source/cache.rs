//! Content-sensitive source and target-analysis cache.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    rc::Rc,
};

use sha2::{Digest, Sha256};
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
    pub(super) graph: crate::verification::target::TargetSourceGraph,
    pub(super) resolver: LocalCallResolver,
    pub(super) functions: FunctionIndex,
    source_fingerprint: TargetSourceFingerprint,
}

#[derive(Eq, PartialEq)]
struct TargetSourceFingerprint {
    files: Vec<(PathBuf, String)>,
    sha256: String,
}

impl CachedTargetAnalysis {
    pub(super) fn source_graph_sha256(&self) -> &str {
        &self.source_fingerprint.sha256
    }
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
        binding: &crate::verification::detector::DetectorFixtureSourceBinding<'_>,
    ) -> Result<&CachedTargetAnalysis, String> {
        let source_root = std::fs::canonicalize(binding.source_root).map_err(|error| {
            format!(
                "canonicalize detector source root {}: {error}",
                binding.source_root.display()
            )
        })?;
        let key = TargetCacheKey {
            source_root,
            package: binding.test_identity.package.clone(),
            target_kind: binding.test_identity.target_kind.clone(),
            target: binding.test_identity.target.clone(),
        };
        let stale = self
            .targets
            .get(&key)
            .map(|cached| {
                target_source_fingerprint(&cached.graph, &key.source_root)
                    .map(|fingerprint| fingerprint != cached.source_fingerprint)
            })
            .transpose()?
            .unwrap_or(false);
        if stale {
            self.targets.remove(&key);
        }
        if !self.targets.contains_key(&key) {
            let mut graph = crate::verification::target::target_source_graph(
                &key.source_root,
                &key.package,
                &key.target_kind,
                &key.target,
                super::ORACLE_MACROS,
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
            let source_fingerprint = target_source_fingerprint(&graph, &key.source_root)?;
            self.target_analysis_count += 1;
            self.targets.insert(
                key.clone(),
                CachedTargetAnalysis {
                    graph,
                    resolver,
                    functions,
                    source_fingerprint,
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

fn target_source_fingerprint(
    graph: &crate::verification::target::TargetSourceGraph,
    source_root: &Path,
) -> Result<TargetSourceFingerprint, String> {
    let files = graph
        .source_modules()
        .into_iter()
        .map(|(path, _)| path)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(|path| {
            let bytes = std::fs::read(&path)
                .map_err(|error| format!("read target source {}: {error}", path.display()))?;
            let relative = path.strip_prefix(source_root).map_err(|_| {
                format!(
                    "target source {} escapes authenticated source root {}",
                    path.display(),
                    source_root.display()
                )
            })?;
            Ok((relative.to_owned(), format!("{:x}", Sha256::digest(bytes))))
        })
        .collect::<Result<Vec<_>, String>>()?;
    let mut digest = Sha256::new();
    for (path, sha256) in &files {
        fingerprint_frame(
            &mut digest,
            path.to_str()
                .ok_or_else(|| format!("target source path is not UTF-8: {}", path.display()))?,
        )?;
        fingerprint_frame(&mut digest, sha256)?;
    }
    Ok(TargetSourceFingerprint {
        files,
        sha256: format!("{:x}", digest.finalize()),
    })
}

fn fingerprint_frame(digest: &mut Sha256, value: &str) -> Result<(), String> {
    let length = u64::try_from(value.len())
        .map_err(|_| "target source fingerprint value exceeds u64".to_owned())?;
    digest.update(length.to_be_bytes());
    digest.update(value.as_bytes());
    Ok(())
}

fn collect_target_analysis(
    graph: &crate::verification::target::TargetSourceGraph,
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
