//! Compiler cfg and tracked-source boundary scenarios for the reviewed module graph.

use super::*;

pub(super) fn no_default_features_contract_disables_unrequested_feature_cfg() {
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

pub(super) fn unknown_custom_cfg_fails_closed_instead_of_omitting_the_module() {
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

pub(super) fn profile_sensitive_cfg_paths_fail_closed_without_compiler_profile_binding() {
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

pub(super) fn ignored_transitive_module_source_is_rejected() {
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

pub(super) fn absolute_out_of_tree_transitive_module_source_is_rejected() {
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
pub(super) fn symlinked_transitive_module_source_is_rejected() {
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
