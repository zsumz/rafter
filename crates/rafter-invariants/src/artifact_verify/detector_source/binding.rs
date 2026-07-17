use std::collections::BTreeMap;

use syn::File;

use super::{
    collect_functions,
    function_index::{FunctionId, FunctionIndex, LocalCallResolver},
    imports::ImportedPaths,
};

pub(super) struct TargetDetectorContract {
    pub(super) declarations: BTreeMap<String, Vec<String>>,
    pub(super) registered_function: FunctionId,
    pub(super) registered_identity: String,
}

pub(super) fn bind_target_detector(
    binding: &crate::DetectorFixtureSourceBinding<'_>,
    fixture_functions: &FunctionIndex,
    fixture_resolver: &LocalCallResolver,
    fixture_id: &FunctionId,
    detector_file: &File,
    target_graph: &crate::rust_target::TargetSourceGraph,
    fixture_module: &crate::rust_target::SourceModule,
) -> Result<TargetDetectorContract, String> {
    require_fixture_declaration(binding, target_graph)?;
    let same_source = binding.fixture_path == binding.detector_path;
    let detector_module_result = if same_source {
        Ok(fixture_module.clone())
    } else {
        target_graph.source_module(binding.detector_path)
    };
    let detector_functions = if same_source {
        FunctionIndex::default()
    } else {
        let detector_module: &[String] = detector_module_result
            .as_ref()
            .map_or(&[][..], |module| module.module.as_slice());
        collect_functions(
            detector_file,
            &ImportedPaths::default(),
            fixture_resolver,
            detector_module,
            false,
        )
    };
    let detector_target = fixture_resolver.named_target(binding.detector, &fixture_id.module);
    let local_id = fixture_functions.resolve_call(&detector_target)?;
    let external_id = if same_source {
        None
    } else {
        detector_functions.resolve_call(&detector_target)?
    };
    let external_named = detector_functions.ids_named(binding.detector);
    if local_id.is_none() {
        detector_module_result.as_ref().map_err(|error| {
            format!(
                "resolve bound detector source {} in registered Cargo target: {error}",
                binding.detector_path.display()
            )
        })?;
    }
    let (detector_id, detector_facts) = match (local_id, external_id) {
        (Some(id), None) => {
            if !same_source && !external_named.is_empty() {
                return Err(format!(
                    "registered detector `{}` has ambiguous declarations in both bound source paths",
                    binding.detector
                ));
            }
            let facts = fixture_functions
                .unique_exact(&id)?
                .ok_or_else(|| format!("registered detector `{}` disappeared", binding.detector))?;
            (id, facts)
        }
        (None, Some(id)) => {
            let facts = detector_functions
                .unique_exact(&id)?
                .ok_or_else(|| format!("registered detector `{}` disappeared", binding.detector))?;
            (id, facts)
        }
        (None, None) => {
            return Err(format!(
                "registered detector `{}` has no function declaration in its bound source paths",
                binding.detector
            ))
        }
        (Some(_), Some(_)) => {
            return Err(format!(
                "registered detector `{}` has ambiguous declarations in both bound source paths",
                binding.detector
            ))
        }
    };
    if detector_facts.conditional_compilation || detector_facts.untrusted_attributes {
        return Err(format!(
            "registered detector `{}` has conditional or untrusted semantic attributes",
            binding.detector
        ));
    }
    if detector_id.name != binding.detector {
        return Err(format!(
            "registered detector `{}` resolves through an alias to `{detector_id}`",
            binding.detector
        ));
    }
    let registered_identity = compiler_identity(&fixture_module.crate_name, &detector_id);
    let declarations = target_graph.declaration_identities();
    if !declarations
        .get(binding.detector)
        .is_some_and(|identities| identities.contains(&registered_identity))
    {
        return Err(format!(
            "registered detector `{}` has no declaration at `{registered_identity}`",
            binding.detector
        ));
    }
    Ok(TargetDetectorContract {
        declarations,
        registered_function: detector_id,
        registered_identity,
    })
}

fn require_fixture_declaration(
    binding: &crate::DetectorFixtureSourceBinding<'_>,
    target_graph: &crate::rust_target::TargetSourceGraph,
) -> Result<(), String> {
    if binding.test_identity.test_name.rsplit("::").next() != Some(binding.fixture) {
        return Err(format!(
            "registered test identity `{}` does not name fixture `{}`",
            binding.test_identity.test_name, binding.fixture
        ));
    }
    target_graph.require_declaration_source(&binding.test_identity.test_name, binding.fixture_path)
}

pub(super) fn compiler_identity(crate_name: &str, function: &FunctionId) -> String {
    std::iter::once(crate_name.to_owned())
        .chain(function.module.iter().cloned())
        .chain(std::iter::once(function.name.clone()))
        .collect::<Vec<_>>()
        .join("::")
}

pub(super) fn registered_fixture_id(
    identity: &crate::TestIdentity,
    fixture: &str,
) -> Result<FunctionId, String> {
    let mut segments = identity
        .test_name
        .split("::")
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let Some(name) = segments.pop() else {
        return Err("registered test identity is empty".to_owned());
    };
    if name != fixture {
        return Err(format!(
            "registered test identity `{}` does not name fixture `{fixture}`",
            identity.test_name
        ));
    }
    Ok(FunctionId {
        module: segments,
        name,
    })
}

pub(super) fn require_registered_fixture(
    functions: &FunctionIndex,
    id: &FunctionId,
    fixture: &str,
) -> Result<(), String> {
    let facts = functions
        .unique_exact(id)?
        .ok_or_else(|| format!("registered negative fixture `{fixture}` has no declaration"))?;
    if facts.detector_test_attributes != 1 {
        return Err(format!(
            "registered negative fixture `{fixture}` must have exactly one #[rafter_invariant_test::detector_test] attribute"
        ));
    }
    if facts.conditional_compilation {
        return Err(format!(
            "registered negative fixture `{fixture}` has conditional compilation attributes"
        ));
    }
    if facts.untrusted_attributes {
        return Err(format!(
            "registered negative fixture `{fixture}` has an untrusted semantic attribute"
        ));
    }
    Ok(())
}
