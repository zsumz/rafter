use std::fs;

use super::{
    tests::{
        detector_fixture, synthetic_identity, synthetic_workspace, track_workspace_paths,
        DETECTOR_PATH, DETECTOR_SOURCE, FIXTURE_PATH,
    },
    verify_invocation_bound_detector,
};
use crate::artifact_verify::DetectorFixtureSourceBatchVerifier;

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

#[test]
fn parent_oracle_macro_shadowing_is_inherited_by_child_modules() {
    let source = detector_fixture(
        r#"use crate::detector::detector; use rafter_invariant_test::oracle_expect_err; fn fixture() { oracle_expect_err!(detector(), "reject"); }"#,
    );
    let root = synthetic_workspace(&source, DETECTOR_SOURCE);
    fs::write(
        root.join("crates/fixture/src/lib.rs"),
        r#"#[path = "mapped_detector.rs"]
mod detector;
mod other;
macro_rules! oracle_expect_err { ($call:expr, $message:expr) => {}; }
#[cfg(test)]
mod tests;
"#,
    )
    .expect("write parent-scoped macro shadow");

    let error = verify_module_graph(&source, &root)
        .expect_err("parent lexical macros must shadow child imports during analysis");
    assert!(error.contains("shadows a trusted oracle macro"), "{error}");
}

#[test]
fn inherited_oracle_macro_shadowing_is_rejected_for_reachable_helper_identity() {
    let source = detector_fixture(
        r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
fn fixture() {
    crate::shadowed_helpers::helper();
    oracle_expect_err!(detector(), "reject");
}
"#,
    );
    let root = synthetic_workspace(&source, DETECTOR_SOURCE);
    fs::write(
        root.join("crates/fixture/src/lib.rs"),
        r#"#[path = "mapped_detector.rs"]
mod detector;
#[cfg(test)]
mod tests;
macro_rules! oracle_expect_err { ($call:expr, $message:expr) => {}; }
mod shadowed_helpers;
"#,
    )
    .expect("write helper-only inherited oracle shadow");
    fs::write(
        root.join("crates/fixture/src/shadowed_helpers.rs"),
        r#"use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
pub(crate) fn helper() {
    oracle_expect_err!(detector(), "shadowed");
}
"#,
    )
    .expect("write shadowed reachable helper");
    track_workspace_paths(&root, &["crates/fixture/src/shadowed_helpers.rs"]);

    let error = verify_module_graph(&source, &root)
        .expect_err("reachable helper identities must inherit oracle macro shadows");
    assert!(error.contains("shadows a trusted oracle macro"), "{error}");
    assert!(error.contains("shadowed_helpers::helper"), "{error}");
}

fn verify_shadowed_impl_method(call: &str, method: &str) {
    let source = detector_fixture(&format!(
        r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
fn fixture() {{
    {call}
    oracle_expect_err!(detector(), "reject");
}}
"#,
    ));
    let root = synthetic_workspace(&source, DETECTOR_SOURCE);
    fs::write(
        root.join("crates/fixture/src/lib.rs"),
        r#"#[path = "mapped_detector.rs"]
mod detector;
#[cfg(test)]
mod tests;
macro_rules! oracle_expect_err { ($call:expr, $message:expr) => {}; }
mod shadowed_methods;
"#,
    )
    .expect("write impl-only inherited oracle shadow");
    fs::write(
        root.join("crates/fixture/src/shadowed_methods.rs"),
        r#"use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;

pub(crate) struct Helper;

impl Helper {
    pub(crate) fn associated() {
        oracle_expect_err!(detector(), "shadowed associated method");
    }

    pub(crate) fn receiver(&self) {
        oracle_expect_err!(detector(), "shadowed receiver method");
    }
}
"#,
    )
    .expect("write shadowed impl methods");
    track_workspace_paths(&root, &["crates/fixture/src/shadowed_methods.rs"]);

    let error = verify_module_graph(&source, &root)
        .expect_err("reachable impl methods must inherit oracle macro shadows");
    assert!(error.contains("shadows a trusted oracle macro"), "{error}");
    assert!(
        error.contains(&format!("shadowed_methods::Helper::{method}")),
        "{error}"
    );
}

#[test]
fn inherited_oracle_macro_shadowing_tracks_absolute_alias_impl_method_identity() {
    let source = detector_fixture(
        r#"
extern crate self as fixture_alias;
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
use crate::shadowed_methods::Local;
fn fixture() {
    Local::forge();
    oracle_expect_err!(detector(), "reject");
}
"#,
    );
    let root = synthetic_workspace(&source, DETECTOR_SOURCE);
    fs::write(
        root.join("crates/fixture/src/lib.rs"),
        r#"#[path = "mapped_detector.rs"]
mod detector;
#[cfg(test)]
mod tests;
extern crate self as fixture_alias;
macro_rules! oracle_expect_err { ($call:expr, $message:expr) => {}; }
mod shadowed_methods;
"#,
    )
    .expect("write absolute-alias impl shadow target");
    fs::write(
        root.join("crates/fixture/src/shadowed_methods.rs"),
        r#"use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
pub(crate) struct Local;
impl ::fixture_alias::shadowed_methods::Local {
    pub(crate) fn forge() {
        oracle_expect_err!(detector(), "shadowed");
    }
}
"#,
    )
    .expect("write absolute-alias impl method");
    track_workspace_paths(&root, &["crates/fixture/src/shadowed_methods.rs"]);

    let error = verify_module_graph(&source, &root)
        .expect_err("absolute self-crate aliases must preserve impl method shadow identities");
    assert!(error.contains("shadows a trusted oracle macro"), "{error}");
    assert!(error.contains("shadowed_methods::Local::forge"), "{error}");
}

#[test]
fn inherited_oracle_macro_shadowing_tracks_associated_function_identity() {
    verify_shadowed_impl_method(
        "crate::shadowed_methods::Helper::associated();",
        "associated",
    );
}

#[test]
fn inherited_oracle_macro_shadowing_tracks_receiver_method_identity() {
    verify_shadowed_impl_method(
        "let helper = crate::shadowed_methods::Helper; helper.receiver();",
        "receiver",
    );
}

#[test]
fn oracle_macro_shadowing_respects_lexical_fixture_scope() {
    let source = detector_fixture(
        r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
fn fixture() { oracle_expect_err!(detector(), "reject"); }
macro_rules! oracle_expect_err { ($call:expr, $message:expr) => {}; }
"#,
    );
    let root = synthetic_workspace(&source, DETECTOR_SOURCE);
    fs::write(
        root.join("crates/fixture/src/lib.rs"),
        r#"#[path = "mapped_detector.rs"]
mod detector;
mod other;
mod unrelated {
    macro_rules! oracle_expect_err { ($call:expr, $message:expr) => {}; }
    fn unreachable_helper() { oracle_expect_err!((), "unreachable"); }
}
#[cfg(test)]
mod tests;
macro_rules! oracle_expect_err { ($call:expr, $message:expr) => {}; }
"#,
    )
    .expect("write lexically unrelated macro declarations");

    verify_module_graph(&source, &root)
        .expect("unrelated and later macro declarations cannot shadow the fixture invocation");
}

#[test]
fn cached_target_analysis_revalidates_oracle_shadows_for_each_fixture_in_any_order() {
    let clean_source = r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
#[::rafter_invariant_test::detector_test]
fn clean_fixture() { oracle_expect_err!(detector(), "reject"); }
"#;
    let shadowed_source = r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
#[::rafter_invariant_test::detector_test]
fn shadowed_fixture() { oracle_expect_err!(detector(), "reject"); }
"#;
    let root = synthetic_workspace(clean_source, DETECTOR_SOURCE);
    let clean_path = root.join("crates/fixture/src/clean_tests.rs");
    let shadowed_path = root.join("crates/fixture/src/shadowed_tests.rs");
    fs::write(&clean_path, clean_source).expect("write clean fixture module");
    fs::write(&shadowed_path, shadowed_source).expect("write shadowed fixture module");
    fs::write(
        root.join("crates/fixture/src/lib.rs"),
        r#"#[path = "mapped_detector.rs"]
mod detector;
#[cfg(test)]
mod clean_tests;
macro_rules! oracle_expect_err { ($call:expr, $message:expr) => {}; }
#[cfg(test)]
mod shadowed_tests;
"#,
    )
    .expect("write ordered fixture modules");
    track_workspace_paths(
        &root,
        &[
            "crates/fixture/src/lib.rs",
            "crates/fixture/src/clean_tests.rs",
            "crates/fixture/src/shadowed_tests.rs",
        ],
    );

    let clean_identity = crate::TestIdentity {
        test_name: "clean_tests::clean_fixture".to_owned(),
        ..synthetic_identity()
    };
    let shadowed_identity = crate::TestIdentity {
        test_name: "shadowed_tests::shadowed_fixture".to_owned(),
        ..synthetic_identity()
    };
    let cases = [
        (
            clean_source,
            clean_path.as_path(),
            &clean_identity,
            "clean_fixture",
            false,
        ),
        (
            shadowed_source,
            shadowed_path.as_path(),
            &shadowed_identity,
            "shadowed_fixture",
            true,
        ),
    ];

    for order in [[0, 1], [1, 0]] {
        let mut batch = DetectorFixtureSourceBatchVerifier::default();
        for index in order {
            let (source, fixture_path, identity, fixture, should_reject) = cases[index];
            let result = batch.validate(&crate::DetectorFixtureSourceBinding {
                fixture_source: source,
                detector_source: DETECTOR_SOURCE,
                source_root: &root,
                fixture_path,
                detector_path: &root.join(DETECTOR_PATH),
                test_identity: identity,
                fixture,
                detector: "detector",
            });
            if should_reject {
                let error = result.expect_err("oracle-shadowed fixture must be rejected");
                assert!(error.contains("shadows a trusted oracle macro"), "{error}");
            } else {
                result.expect("clean fixture must remain accepted");
            }
        }
        assert_eq!(
            batch.target_analysis_count(),
            1,
            "both fixtures must reuse one Cargo-target analysis"
        );
    }
}

#[test]
fn no_default_features_contract_disables_unrequested_feature_cfg() {
    let source = detector_fixture(
        r#"use crate::detector::detector; use rafter_invariant_test::oracle_expect_err; fn fixture() { oracle_expect_err!(detector(), "reject"); }"#,
    );
    let root = synthetic_workspace(&source, DETECTOR_SOURCE);
    fs::write(root.join("crates/fixture/src/alternate_tests.rs"), &source)
        .expect("write feature-selected alternate fixture");
    fs::write(
        root.join("crates/fixture/src/lib.rs"),
        r#"#[path = "mapped_detector.rs"]
mod detector;
mod other;
#[cfg_attr(feature = "build-selected", path = "alternate_tests.rs")]
mod tests;
"#,
    )
    .expect("write feature-cfg target root");
    verify_module_graph(&source, &root)
        .expect("the reviewed detector compiler passes --no-default-features without --features");
}

#[test]
fn unknown_custom_cfg_fails_closed_instead_of_omitting_the_module() {
    let source = detector_fixture(
        r#"use crate::detector::detector; use rafter_invariant_test::oracle_expect_err; fn fixture() { oracle_expect_err!(detector(), "reject"); }"#,
    );
    let root = synthetic_workspace(&source, DETECTOR_SOURCE);
    fs::write(root.join("crates/fixture/src/alternate_tests.rs"), &source)
        .expect("write custom-cfg-selected alternate fixture");
    fs::write(
        root.join("crates/fixture/src/lib.rs"),
        "#[path = \"mapped_detector.rs\"]\nmod detector;\nmod other;\n#[cfg_attr(custom_build_flag, path = \"alternate_tests.rs\")]\nmod tests;\n",
    )
    .expect("write custom-cfg target root");
    let error =
        verify_module_graph(&source, &root).expect_err("unbound custom cfg must fail closed");
    assert!(
        error.contains("outside the reviewed test context"),
        "{error}"
    );
}

#[test]
fn profile_sensitive_cfg_paths_fail_closed_without_compiler_profile_binding() {
    for predicate in ["debug_assertions", "panic = \"unwind\""] {
        let source = detector_fixture(
            r#"use crate::detector::detector; use rafter_invariant_test::oracle_expect_err; fn fixture() { oracle_expect_err!(detector(), "reject"); }"#,
        );
        let root = synthetic_workspace(&source, DETECTOR_SOURCE);
        fs::write(root.join("crates/fixture/src/alternate_tests.rs"), &source)
            .expect("write profile-selected alternate fixture");
        fs::write(
            root.join("crates/fixture/src/lib.rs"),
            format!(
                "#[path = \"mapped_detector.rs\"]\nmod detector;\nmod other;\n#[cfg_attr({predicate}, path = \"alternate_tests.rs\")]\nmod tests;\n"
            ),
        )
        .expect("write profile-cfg target root");
        let error = verify_module_graph(&source, &root)
            .expect_err("unbound profile-sensitive cfg must fail closed");
        assert!(
            error.contains("outside the reviewed test context"),
            "{predicate}: {error}"
        );
    }
}

#[test]
fn ignored_transitive_module_source_is_rejected() {
    let source = detector_fixture(
        r#"use crate::detector::detector; use rafter_invariant_test::oracle_expect_err; fn fixture() { oracle_expect_err!(detector(), "reject"); }"#,
    );
    let root = synthetic_workspace(&source, DETECTOR_SOURCE);
    fs::write(root.join(".gitignore"), "crates/fixture/src/ignored.rs\n")
        .expect("write synthetic ignore rule");
    fs::write(
        root.join("crates/fixture/src/ignored.rs"),
        "fn hidden() {}\n",
    )
    .expect("write ignored transitive module");
    fs::write(
        root.join("crates/fixture/src/lib.rs"),
        "#[path = \"mapped_detector.rs\"]\nmod detector;\nmod other;\nmod ignored;\n#[cfg(test)]\nmod tests;\n",
    )
    .expect("bind ignored transitive module");

    let error = verify_module_graph(&source, &root)
        .expect_err("compiler-visible ignored modules must fail closed");
    assert!(error.contains("not tracked"), "{error}");
}

#[test]
fn absolute_out_of_tree_transitive_module_source_is_rejected() {
    let source = detector_fixture(
        r#"use crate::detector::detector; use rafter_invariant_test::oracle_expect_err; fn fixture() { oracle_expect_err!(detector(), "reject"); }"#,
    );
    let root = synthetic_workspace(&source, DETECTOR_SOURCE);
    let escaped = root
        .parent()
        .expect("synthetic checkout parent")
        .join(format!(
            "outside-module-{}",
            root.file_name().unwrap().to_string_lossy()
        ));
    fs::write(&escaped, "fn hidden() {}\n").expect("write out-of-tree transitive module");
    let escaped = fs::canonicalize(escaped).expect("canonical out-of-tree module path");
    fs::write(
        root.join("crates/fixture/src/lib.rs"),
        format!(
            "#[path = \"mapped_detector.rs\"]\nmod detector;\nmod other;\n#[path = {:?}]\nmod escaped;\n#[cfg(test)]\nmod tests;\n",
            escaped.to_string_lossy()
        ),
    )
    .expect("bind out-of-tree transitive module");

    let error = verify_module_graph(&source, &root)
        .expect_err("out-of-tree compiler module paths must fail closed");
    assert!(error.contains("outside the bound source tree"), "{error}");
}

#[cfg(unix)]
#[test]
fn symlinked_transitive_module_source_is_rejected() {
    let source = detector_fixture(
        r#"use crate::detector::detector; use rafter_invariant_test::oracle_expect_err; fn fixture() { oracle_expect_err!(detector(), "reject"); }"#,
    );
    let root = synthetic_workspace(&source, DETECTOR_SOURCE);
    std::os::unix::fs::symlink(
        root.join("crates/fixture/src/other.rs"),
        root.join("crates/fixture/src/linked.rs"),
    )
    .expect("create symlinked transitive module");
    fs::write(
        root.join("crates/fixture/src/lib.rs"),
        "#[path = \"mapped_detector.rs\"]\nmod detector;\nmod other;\nmod linked;\n#[cfg(test)]\nmod tests;\n",
    )
    .expect("bind symlinked transitive module");

    let error = verify_module_graph(&source, &root)
        .expect_err("symlinked compiler module paths must fail closed");
    assert!(error.contains("not a regular file"), "{error}");
}
