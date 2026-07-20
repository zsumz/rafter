//! Target-module graph, cfg, alias, and cache-invalidation scenarios.

use std::fs;

use super::super::DetectorFixtureAnalysis;
use super::{
    tests::{
        detector_fixture, synthetic_identity, synthetic_workspace, track_workspace_paths,
        DETECTOR_PATH, DETECTOR_SOURCE, FIXTURE_PATH,
    },
    verify_invocation_bound_detector,
};

fn verify_module_graph(source: &str, root: &std::path::Path) -> Result<(), String> {
    verify_invocation_bound_detector(&crate::DetectorFixtureSourceBinding {
        fixture_source: source,
        detector_source: DETECTOR_SOURCE,
        source_root: root,
        fixture_path: &root.join(FIXTURE_PATH),
        detector_path: &root.join(DETECTOR_PATH),
        test_identity: &synthetic_identity(),
        fixture: "fixture",
        detector: "detector",
    })
    .map(|_| ())
}

#[test]
fn source_root_stripping_ignores_ancestor_src_components() {
    let source = detector_fixture(
        r#"use crate::detector::detector; use rafter_invariant_test::oracle_expect_err; fn fixture() { oracle_expect_err!(detector(), "reject"); }"#,
    );
    let root = synthetic_workspace(&source, DETECTOR_SOURCE);
    let contract = verify_invocation_bound_detector(&crate::DetectorFixtureSourceBinding {
        fixture_source: &source,
        detector_source: DETECTOR_SOURCE,
        source_root: &root,
        fixture_path: &root.join(FIXTURE_PATH),
        detector_path: &root.join(DETECTOR_PATH),
        test_identity: &synthetic_identity(),
        fixture: "fixture",
        detector: "detector",
    })
    .expect("workspace-relative module inference ignores host path components");
    assert_eq!(
        contract.registered_identity(),
        "fixture::detector::detector"
    );
}

#[test]
fn cargo_test_cfg_and_cfg_attr_path_select_the_actual_fixture_module() {
    let source = detector_fixture(
        r#"use crate::detector::detector; use rafter_invariant_test::oracle_expect_err; fn fixture() { oracle_expect_err!(detector(), "reject"); }"#,
    );
    let root = synthetic_workspace(&source, DETECTOR_SOURCE);
    let alternate_fixture = root.join("crates/fixture/src/alternate_tests.rs");
    fs::write(&alternate_fixture, &source).expect("write cfg_attr-selected fixture source");
    track_workspace_paths(&root, &["crates/fixture/src/alternate_tests.rs"]);
    fs::write(
        root.join("crates/fixture/src/lib.rs"),
        r#"#[path = "mapped_detector.rs"]
mod detector;
mod other;
#[cfg(any(test, feature = "disabled"))]
#[cfg_attr(test, path = "alternate_tests.rs")]
mod tests;
"#,
    )
    .expect("write cfg-selected target root");

    verify_invocation_bound_detector(&crate::DetectorFixtureSourceBinding {
        fixture_source: &source,
        detector_source: DETECTOR_SOURCE,
        source_root: &root,
        fixture_path: &alternate_fixture,
        detector_path: &root.join(DETECTOR_PATH),
        test_identity: &synthetic_identity(),
        fixture: "fixture",
        detector: "detector",
    })
    .expect("ordinary Cargo test cfg and active cfg_attr path must resolve exactly");
}

#[test]
fn host_target_cfg_matches_the_detector_compile_context() {
    let source = detector_fixture(
        r#"use crate::detector::detector; use rafter_invariant_test::oracle_expect_err; fn fixture() { oracle_expect_err!(detector(), "reject"); }"#,
    );
    let root = synthetic_workspace(&source, DETECTOR_SOURCE);
    let family = if cfg!(unix) { "unix" } else { "windows" };
    fs::write(
        root.join("crates/fixture/src/lib.rs"),
        format!(
            "#[path = \"mapped_detector.rs\"]\nmod detector;\nmod other;\n#[cfg(all({family}, target_os = \"{}\", target_arch = \"{}\", target_pointer_width = \"{}\"))]\nmod tests;\n",
            std::env::consts::OS,
            std::env::consts::ARCH,
            usize::BITS,
        ),
    )
    .expect("write target-sensitive target root");

    verify_module_graph(&source, &root)
        .expect("host target cfg is bound by the host-targeted detector compile contract");
}

#[test]
fn item_include_source_fails_closed_outside_the_reviewed_module_graph() {
    let source = detector_fixture(
        r#"include!("included.rs"); use crate::detector::detector; use rafter_invariant_test::oracle_expect_err; fn fixture() { helper(); oracle_expect_err!(detector(), "reject"); }"#,
    );
    let root = synthetic_workspace(&source, DETECTOR_SOURCE);
    fs::write(
        root.join("crates/fixture/src/included.rs"),
        "fn helper() {}\n",
    )
    .expect("write included helper source");
    track_workspace_paths(&root, &["crates/fixture/src/included.rs"]);

    let error = verify_module_graph(&source, &root)
        .expect_err("item include expansion is not represented by the module graph");
    assert!(error.contains("unexpanded item macro"), "{error}");
}

#[test]
fn impl_include_source_fails_closed_outside_the_reviewed_module_graph() {
    let source = detector_fixture(
        r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
struct Local;
impl Local { include!("included_impl.rs"); }
fn fixture() { oracle_expect_err!(detector(), "reject"); }
"#,
    );
    let root = synthetic_workspace(&source, DETECTOR_SOURCE);
    fs::write(
        root.join("crates/fixture/src/included_impl.rs"),
        "fn hidden(&self) {}\n",
    )
    .expect("write included impl source");
    track_workspace_paths(&root, &["crates/fixture/src/included_impl.rs"]);

    let error = verify_module_graph(&source, &root)
        .expect_err("impl include expansion is not represented by the module graph");
    assert!(error.contains("unexpanded impl macro"), "{error}");
}

#[test]
fn cfg_selection_is_consistent_for_imports_and_methods() {
    let source = detector_fixture(
        r#"
#[cfg(not(test))]
use external_crate::detector;
use crate::detector::*;
use rafter_invariant_test::oracle_expect_err;
struct Local;
impl Local {
    #[cfg(not(test))]
    fn check(&self) { std::process::exit(0); }
    #[cfg(test)]
    fn check(&self) {}
}
fn fixture() { Local.check(); oracle_expect_err!(detector(), "reject"); }
"#,
    );
    let root = synthetic_workspace(&source, DETECTOR_SOURCE);

    verify_module_graph(&source, &root)
        .expect("inactive imports and methods must not enter the reviewed test graph");

    let family = if cfg!(unix) { "unix" } else { "windows" };
    let active_host_import = detector_fixture(
        &r#"
#[cfg(__HOST_FAMILY__)]
use external_crate::detector;
use crate::detector::*;
use rafter_invariant_test::oracle_expect_err;
fn fixture() { oracle_expect_err!(detector(), "reject"); }
"#
        .replace("__HOST_FAMILY__", family),
    );
    let active_root = synthetic_workspace(&active_host_import, DETECTOR_SOURCE);
    verify_module_graph(&active_host_import, &active_root)
        .expect_err("an active host-target import must participate in detector resolution");
}

#[test]
fn inferred_receiver_resolves_a_sibling_module_method() {
    let source = detector_fixture(
        r#"
use crate::detector::detector;
use crate::other as helpers;
use rafter_invariant_test::oracle_expect_err;
fn fixture() {
    let value = helpers::Local;
    value.forge();
    oracle_expect_err!(detector(), "reject");
}
"#,
    );
    let root = synthetic_workspace(&source, DETECTOR_SOURCE);
    fs::write(
        root.join("crates/fixture/src/other.rs"),
        r"
pub(crate) struct Local;
impl Local { pub(crate) fn forge(&self) { std::process::exit(0); } }
",
    )
    .expect("write sibling receiver implementation");

    let error = verify_module_graph(&source, &root)
        .expect_err("an inferred sibling receiver method must be recursively inspected");
    assert!(error.contains("arbitrary detector witness"), "{error}");
}

#[test]
fn deref_receiver_methods_resolve_to_the_reviewed_target_type() {
    let source = detector_fixture(
        r#"
use std::ops::Deref;
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
struct Cluster;
impl Cluster { fn forge(&self) { std::process::exit(0); } }
struct Wrapper(Cluster);
impl Deref for Wrapper {
    type Target = Cluster;
    fn deref(&self) -> &Self::Target { &self.0 }
}
fn fixture() {
    let wrapper = Wrapper(Cluster);
    wrapper.forge();
    oracle_expect_err!(detector(), "reject");
}
"#,
    );
    let root = synthetic_workspace(&source, DETECTOR_SOURCE);

    let error = verify_module_graph(&source, &root)
        .expect_err("a reviewed Deref target method must be recursively inspected");
    assert!(error.contains("arbitrary detector witness"), "{error}");
}

#[test]
fn aliased_deref_receiver_methods_resolve_to_the_reviewed_target_type() {
    let source = detector_fixture(
        r#"
use std::ops::Deref as D;
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
struct Cluster;
impl Cluster { fn forge(&self) { std::process::exit(0); } }
struct Wrapper(Cluster);
impl D for Wrapper {
    type Target = Cluster;
    fn deref(&self) -> &Self::Target { &self.0 }
}
fn fixture() {
    let wrapper = Wrapper(Cluster);
    wrapper.forge();
    oracle_expect_err!(detector(), "reject");
}
"#,
    );
    let root = synthetic_workspace(&source, DETECTOR_SOURCE);

    let error = verify_module_graph(&source, &root)
        .expect_err("an aliased Deref target method must be recursively inspected");
    assert!(error.contains("arbitrary detector witness"), "{error}");
}

#[path = "source_module_graph_tests/macro_provenance.rs"]
mod macro_provenance;

#[path = "source_module_graph_tests/compiler_boundaries.rs"]
mod compiler_boundaries;

#[test]
fn parent_oracle_macro_shadowing_is_inherited_by_child_modules() {
    macro_provenance::parent_oracle_macro_shadowing_is_inherited_by_child_modules();
}

#[test]
fn inherited_oracle_macro_shadowing_is_rejected_for_reachable_helper_identity() {
    macro_provenance::inherited_oracle_macro_shadowing_is_rejected_for_reachable_helper_identity();
}

#[test]
fn inherited_oracle_macro_shadowing_tracks_absolute_alias_impl_method_identity() {
    macro_provenance::inherited_oracle_macro_shadowing_tracks_absolute_alias_impl_method_identity();
}

#[test]
fn inherited_oracle_macro_shadowing_tracks_associated_function_identity() {
    macro_provenance::inherited_oracle_macro_shadowing_tracks_associated_function_identity();
}

#[test]
fn inherited_oracle_macro_shadowing_tracks_receiver_method_identity() {
    macro_provenance::inherited_oracle_macro_shadowing_tracks_receiver_method_identity();
}

#[test]
fn oracle_macro_shadowing_respects_lexical_fixture_scope() {
    macro_provenance::oracle_macro_shadowing_respects_lexical_fixture_scope();
}

#[test]
fn cached_target_analysis_revalidates_oracle_shadows_for_each_fixture_in_any_order() {
    macro_provenance::cached_target_analysis_revalidates_oracle_shadows_for_each_fixture_in_any_order();
}

#[test]
fn no_default_features_contract_disables_unrequested_feature_cfg() {
    compiler_boundaries::no_default_features_contract_disables_unrequested_feature_cfg();
}

#[test]
fn unknown_custom_cfg_fails_closed_instead_of_omitting_the_module() {
    compiler_boundaries::unknown_custom_cfg_fails_closed_instead_of_omitting_the_module();
}

#[test]
fn profile_sensitive_cfg_paths_fail_closed_without_compiler_profile_binding() {
    compiler_boundaries::profile_sensitive_cfg_paths_fail_closed_without_compiler_profile_binding();
}

#[test]
fn ignored_transitive_module_source_is_rejected() {
    compiler_boundaries::ignored_transitive_module_source_is_rejected();
}

#[test]
fn absolute_out_of_tree_transitive_module_source_is_rejected() {
    compiler_boundaries::absolute_out_of_tree_transitive_module_source_is_rejected();
}

#[cfg(unix)]
#[test]
fn symlinked_transitive_module_source_is_rejected() {
    compiler_boundaries::symlinked_transitive_module_source_is_rejected();
}
