//! Compiler-facing contracts for the detector attribute and invocation macros.

#[test]
fn detector_macro_compile_contracts() {
    let cases = trybuild::TestCases::new();
    cases.pass("tests/ui/pass/*.rs");
    cases.compile_fail("tests/ui/fail/*.rs");
}
