//! Lexical import, out-of-line module, and process-primitive boundary scenarios.

use super::*;

pub(super) fn block_scoped_imports_are_resolved_without_cross_function_contamination() {
    let rejecting = r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
fn fixture() {
    { use self::forge as local; local(); }
    oracle_expect_err!(detector(), "reject");
}
fn forge() { std::process::exit(0); }
"#;
    assert!(verify(rejecting)
        .expect_err("a reachable block import must resolve")
        .contains("arbitrary detector witness"));

    let accepted = r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
fn fixture() { safe(); oracle_expect_err!(detector(), "reject"); }
fn safe() {}
fn forge() { std::process::exit(0); }
fn unrelated() { use self::forge as safe; safe(); }
"#;
    verify(accepted).expect("an unreachable block import must not contaminate fixture scope");
}

pub(super) fn out_of_line_local_helper_calls_fail_closed() {
    let source = detector_fixture(
        r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
mod forged;
use self::forged as helpers;
fn fixture() { helpers::run(); oracle_expect_err!(detector(), "reject"); }
"#,
    );
    let root = synthetic_workspace(&source, DETECTOR_SOURCE);
    fs::create_dir_all(root.join("crates/fixture/src/tests"))
        .expect("create out-of-line module directory");
    fs::write(
        root.join("crates/fixture/src/tests/forged.rs"),
        "pub(super) fn run() { std::process::exit(0); }\n",
    )
    .expect("write out-of-line helper");
    track_workspace_paths(&root, &["crates/fixture/src/tests/forged.rs"]);
    let error = verify_invocation_bound_detector(&crate::DetectorFixtureSourceBinding {
        fixture_source: &source,
        detector_source: DETECTOR_SOURCE,
        source_root: &root,
        fixture_path: &root.join(FIXTURE_PATH),
        detector_path: &root.join(DETECTOR_PATH),
        test_identity: &synthetic_identity(),
        fixture: "fixture",
        detector: "detector",
    })
    .expect_err("out-of-line helper calls must fail closed");
    assert!(error.contains("arbitrary detector witness"), "{error}");
}

pub(super) fn benign_custom_exec_method_does_not_trigger_process_exit_guard() {
    let source = r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
struct LocalCommand;
impl LocalCommand { fn exec(&self) {} }
fn fixture() {
    LocalCommand.exec();
    oracle_expect_err!(detector(), "reject");
}
"#;
    verify(source).expect("an ordinary method named exec is not a process exit primitive");
}
