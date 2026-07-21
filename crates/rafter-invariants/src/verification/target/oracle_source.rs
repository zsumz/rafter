//! Authenticated source qualification for ordinary test-oracle evidence.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
};

use syn::{visit::Visit, ItemFn, ItemMacro, ItemMod};

use super::{target_source_graph, ORACLE_MACROS};
use crate::contract::TestIdentity;

mod body;
mod imports;
mod proptest;

use body::{analyze_block, analyze_token_block, scan_reserved_channels};
use imports::Imports;
use proptest::generated_functions;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct RegisteredTestBinding {
    pub(crate) identity: TestIdentity,
    pub(crate) path: String,
    pub(crate) symbol: String,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct TargetKey {
    package: String,
    kind: String,
    name: String,
}

pub(crate) fn verify_registered_oracle_sources(
    workspace: &Path,
    bindings: &BTreeSet<RegisteredTestBinding>,
) -> Result<(), String> {
    let mut targets = BTreeMap::<TargetKey, Vec<&RegisteredTestBinding>>::new();
    for binding in bindings {
        targets
            .entry(TargetKey {
                package: binding.identity.package.clone(),
                kind: binding.identity.target_kind.clone(),
                name: binding.identity.target.clone(),
            })
            .or_default()
            .push(binding);
    }

    for (target, bindings) in targets {
        let graph = target_source_graph(
            workspace,
            &target.package,
            &target.kind,
            &target.name,
            ORACLE_MACROS,
        )?;
        verify_target_reserved_channels(&graph)?;
        for binding in bindings {
            verify_binding(workspace, &graph, binding)?;
        }
    }
    crate::verification::source::revalidate_authenticated_snapshot(workspace)
}

fn verify_target_reserved_channels(graph: &super::TargetSourceGraph) -> Result<(), String> {
    let sources = graph
        .source_modules()
        .into_iter()
        .map(|(path, _)| path)
        .collect::<BTreeSet<_>>();
    for source in sources {
        let text = fs::read_to_string(&source)
            .map_err(|error| format!("read target source {}: {error}", source.display()))?;
        let file = syn::parse_file(&text)
            .map_err(|error| format!("parse target source {}: {error}", source.display()))?;
        scan_reserved_channels(&file).map_err(|error| {
            format!(
                "target source {} accesses the reserved oracle channel: {error}",
                source.display()
            )
        })?;
    }
    Ok(())
}

fn verify_binding(
    workspace: &Path,
    graph: &super::TargetSourceGraph,
    binding: &RegisteredTestBinding,
) -> Result<(), String> {
    if binding.identity.test_name.rsplit("::").next() != Some(binding.symbol.as_str()) {
        return Err(format!(
            "registered test identity `{}` does not name source symbol `{}`",
            binding.identity.test_name, binding.symbol
        ));
    }
    let source_path = workspace.join(&binding.path);
    graph.require_declaration_source(&binding.identity.test_name, &source_path)?;
    let module = graph.source_module(&source_path)?.module;
    let source = fs::read_to_string(&source_path).map_err(|error| {
        format!(
            "read registered oracle source {}: {error}",
            source_path.display()
        )
    })?;
    qualify_source_text(&source, &module, binding).map_err(|error| {
        format!(
            "registered test `{}` in {} is not a qualified oracle source: {error}",
            binding.identity.test_name,
            source_path.display()
        )
    })
}

fn qualify_source_text(
    source: &str,
    source_module: &[String],
    binding: &RegisteredTestBinding,
) -> Result<(), String> {
    let file = syn::parse_file(source).map_err(|error| format!("parse source: {error}"))?;
    scan_reserved_channels(&file)?;
    let imports = Imports::collect(&file.items, None);
    let import_error = imports.validate().err();

    let mut visitor = RegisteredOracleVisitor {
        desired: binding
            .identity
            .test_name
            .split("::")
            .map(str::to_owned)
            .collect(),
        module: source_module.to_vec(),
        imports,
        import_error,
        matches: Vec::new(),
    };
    visitor.visit_file(&file);
    let [result] = visitor.matches.as_slice() else {
        return Err(format!(
            "identity resolves to {} source declarations",
            visitor.matches.len()
        ));
    };
    result.clone()
}

struct RegisteredOracleVisitor {
    desired: Vec<String>,
    module: Vec<String>,
    imports: Imports,
    import_error: Option<String>,
    matches: Vec<Result<(), String>>,
}

impl Visit<'_> for RegisteredOracleVisitor {
    fn visit_item_fn(&mut self, function: &ItemFn) {
        let mut identity = self.module.clone();
        identity.push(function.sig.ident.to_string());
        if identity != self.desired {
            return;
        }
        if let Some(error) = &self.import_error {
            self.matches.push(Err(error.clone()));
            return;
        }
        if !function.attrs.iter().any(|attribute| {
            attribute.path().is_ident("test")
                || self.imports.trusted_detector_test(attribute.path())
        }) {
            self.matches.push(Err(
                "declaration is not an exact #[test] function".to_owned()
            ));
            return;
        }
        if function
            .attrs
            .iter()
            .any(|attribute| attribute.path().is_ident("should_panic"))
        {
            self.matches.push(Err(
                "#[should_panic] cannot qualify oracle evidence".to_owned()
            ));
            return;
        }
        self.matches
            .push(analyze_block(&function.block, &self.imports));
    }

    fn visit_item_mod(&mut self, item: &ItemMod) {
        if !super::module_active_for_test(&item.attrs).unwrap_or(false) {
            return;
        }
        let Some((_, items)) = &item.content else {
            return;
        };
        self.module.push(item.ident.to_string());
        if !self.desired.starts_with(&self.module) {
            self.module.pop();
            return;
        }
        let imports = Imports::collect(items, Some(&self.imports));
        let import_error = imports.validate().err();
        let previous_imports = std::mem::replace(&mut self.imports, imports);
        let previous_error = std::mem::replace(&mut self.import_error, import_error);
        for item in items {
            self.visit_item(item);
        }
        self.imports = previous_imports;
        self.import_error = previous_error;
        self.module.pop();
    }

    fn visit_item_macro(&mut self, item: &ItemMacro) {
        if !super::policy::proptest_invocation(item) || !self.imports.trusted_proptest(&item.mac) {
            return;
        }
        for generated in generated_functions(&item.mac.tokens) {
            let mut identity = self.module.clone();
            identity.push(generated.name.clone());
            if identity != self.desired {
                continue;
            }
            if !generated.test_attribute {
                self.matches
                    .push(Err("proptest declaration is missing #[test]".to_owned()));
            } else if generated.should_panic {
                self.matches.push(Err(
                    "#[should_panic] cannot qualify oracle evidence".to_owned()
                ));
            } else {
                self.matches
                    .push(analyze_token_block(&generated.body, &self.imports));
            }
        }
    }
}

#[cfg(test)]
#[path = "oracle_source/tests.rs"]
mod tests;
