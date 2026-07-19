//! Cross-toolchain compiler contracts for invocation type boundaries.

#[path = "support/compile_fixture.rs"]
mod compile_fixture;

use compile_fixture::{runtime_dependency, CargoFixture};

#[test]
fn nine_argument_invocations_fail_without_an_adapter() {
    let fixture = CargoFixture::new("arity-nine-contract", &runtime_dependency("runtime"));
    fixture.write_source(
        r#"
use runtime::{oracle_expect_err, oracle_invoke_recorder};

fn reject(_: u8, _: u8, _: u8, _: u8, _: u8, _: u8, _: u8, _: u8, _: u8) -> Result<(), ()> { Err(()) }
fn record(_: u8, _: u8, _: u8, _: u8, _: u8, _: u8, _: u8, _: u8, _: u8) {}

fn main() {
    let _ = oracle_expect_err!(reject(0, 1, 2, 3, 4, 5, 6, 7, 8), "nine");
    oracle_invoke_recorder!(record(0, 1, 2, 3, 4, 5, 6, 7, 8));
}
"#,
    );
    assert_compile_failure_mentions(&fixture, &["__OracleCall", "reject", "record"]);
}

#[test]
fn invocation_output_types_are_enforced_by_the_adapter() {
    let fixture = CargoFixture::new("output-type-contract", &runtime_dependency("runtime"));
    fixture.write_source(
        r#"
use runtime::{oracle_expect_err, oracle_invoke_recorder};

fn not_a_detector() -> bool { false }
fn not_a_recorder() -> usize { 0 }

fn main() {
    let _ = oracle_expect_err!(not_a_detector(), "result required");
    oracle_invoke_recorder!(not_a_recorder());
}
"#,
    );
    assert_compile_failure_mentions(&fixture, &["not_a_detector", "not_a_recorder", "Result"]);
}

fn assert_compile_failure_mentions(fixture: &CargoFixture, expected: &[&str]) {
    let output = fixture.compile();
    assert!(!output.status.success(), "fixture unexpectedly compiled");
    let stderr = String::from_utf8_lossy(&output.stderr);
    for expected in expected {
        assert!(
            stderr.contains(expected),
            "compiler failure omitted `{expected}`: {stderr}"
        );
    }
}
