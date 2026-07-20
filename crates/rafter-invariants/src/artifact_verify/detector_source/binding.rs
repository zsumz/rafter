use std::collections::BTreeMap;

use super::function_index::{FunctionId, FunctionIndex, LocalCallResolver};

pub(super) struct TargetDetectorContract {
    pub(super) declarations: BTreeMap<String, Vec<String>>,
    pub(super) registered_function: FunctionId,
    pub(super) registered_identity: String,
}

pub(super) fn bind_target_detector(
    binding: &crate::DetectorFixtureSourceBinding<'_>,
    target_functions: &FunctionIndex,
    target_resolver: &LocalCallResolver,
    target_graph: &crate::verification::target::TargetSourceGraph,
    fixture_module: &crate::verification::target::SourceModule,
) -> Result<TargetDetectorContract, String> {
    require_fixture_declaration(binding, target_graph)?;
    let target = target_resolver.named_target(binding.detector, &fixture_module.module);
    let detector_id = target_functions.resolve_call(&target)?.ok_or_else(|| {
        format!(
            "registered detector `{}` does not resolve to a fixture-visible target declaration",
            binding.detector
        )
    })?;
    target_graph.require_declaration_source(&detector_id.to_string(), binding.detector_path)?;
    let detector_facts = target_functions
        .unique_exact(&detector_id)?
        .ok_or_else(|| format!("registered detector `{}` disappeared", binding.detector))?;
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
    target_graph: &crate::verification::target::TargetSourceGraph,
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
