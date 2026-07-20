//! Exact fixture binding and invocation-contract derivation.

use super::{
    binding::{bind_target_detector, registered_fixture_id, require_registered_fixture},
    cache::DetectorSourceCache,
    contract::DetectorInvocationContract,
    function_collection::collect_functions,
    imports::{collect_imports, validate_oracle_provenance},
    reachability::expand_reachable_fixture,
};

#[cfg(test)]
pub(in crate::verification::detector) fn verify_invocation_bound_detector(
    binding: &crate::verification::detector::DetectorFixtureSourceBinding<'_>,
) -> Result<DetectorInvocationContract, String> {
    verify_invocation_bound_detector_cached(binding, &mut DetectorSourceCache::default())
}

pub(in crate::verification::detector) fn verify_invocation_bound_detector_cached(
    binding: &crate::verification::detector::DetectorFixtureSourceBinding<'_>,
    cache: &mut DetectorSourceCache,
) -> Result<DetectorInvocationContract, String> {
    let fixture_source = binding.fixture_source;
    let fixture_path = binding.fixture_path;
    let detector_path = binding.detector_path;
    let fixture = binding.fixture;
    let detector = binding.detector;
    let fixture_file = cache.source(fixture_path, fixture_source, "fixture")?;
    cache.source(detector_path, binding.detector_source, "detector")?;
    let target_analysis = cache.target(binding)?;
    let target_graph = &target_analysis.graph;
    let fixture_module = target_graph.source_module(binding.fixture_path)?;
    let imports = collect_imports(&fixture_file);
    validate_oracle_provenance(&imports)?;
    if imports.local_value_bindings.contains(detector) {
        return Err(format!(
            "registered detector `{detector}` is shadowed by a module value binding"
        ));
    }
    let target_resolver = &target_analysis.resolver;
    let fixture_functions = collect_functions(
        &fixture_file,
        &imports,
        target_resolver,
        &fixture_module.module,
        false,
    );
    let fixture_id = registered_fixture_id(binding.test_identity, fixture)?;
    require_registered_fixture(&fixture_functions, &fixture_id, fixture)?;
    let target = bind_target_detector(
        binding,
        &target_analysis.functions,
        target_resolver,
        target_graph,
        &fixture_module,
    )?;
    let declarations = target.declarations;
    let registered_function = target.registered_function;
    let target_functions = &target_analysis.functions;

    let mut contract = DetectorInvocationContract::new(target.registered_identity);
    expand_reachable_fixture(
        target_functions,
        target_graph,
        &fixture_id,
        &registered_function,
        &fixture_module.crate_name,
        &declarations,
        &mut contract,
    )?;
    let rejecting_witness = format!("expect-err:{}", contract.registered_identity);
    if !contract.witnesses.contains_key(&rejecting_witness) {
        return Err(format!(
            "negative fixture `{fixture}` does not invoke registered detector `{detector}` through an invocation-bound rejecting oracle; observed witnesses: {:?}",
            contract.witnesses
        ));
    }
    Ok(contract)
}
