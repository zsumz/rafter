use super::test_exec::{exact_pass, listed_tests};

#[test]
fn exact_pass_rejects_zero_test_success() {
    let zero = b"running 0 tests\ntest result: ok. 0 passed; 0 failed; 0 ignored";
    assert!(!exact_pass(zero, "module::test"));
}

#[test]
fn terse_discovery_uses_exact_identity() {
    let tests = listed_tests(b"module::one: test\nmodule::two: test\n");
    assert!(tests.contains("module::one"));
    assert!(!tests.contains("one"));
}
