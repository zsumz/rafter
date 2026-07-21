//! Exact libtest discovery tests.

use crate::evidence::format::libtest::listed_tests;

#[test]
fn terse_discovery_uses_exact_identity() {
    let tests = listed_tests(b"module::one: test\nmodule::two: test\n");
    assert!(tests.iter().any(|test| test == "module::one"));
    assert!(!tests.iter().any(|test| test == "one"));

    let duplicate = listed_tests(b"module::one: test\nmodule::one: test\n");
    assert_eq!(
        duplicate
            .iter()
            .filter(|test| *test == "module::one")
            .count(),
        2
    );
}
