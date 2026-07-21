//! Verifier-owned TLA+ mutation-suite qualification scenarios.

use std::fmt::Write as _;

use super::{mutation_suite_qualified, REQUIRED_MUTATION_TESTS};

#[test]
fn verifier_mutation_policy_requires_exact_complete_inventory() {
    let complete = transcript(REQUIRED_MUTATION_TESTS.iter().copied());
    assert!(mutation_suite_qualified(Some(0), false, &complete));

    let missing = transcript(REQUIRED_MUTATION_TESTS[..33].iter().copied());
    assert!(!mutation_suite_qualified(Some(0), false, &missing));

    let duplicate = format!(
        "{complete}test producer::tla_exec::mutation_tests::{} ... ok\n",
        REQUIRED_MUTATION_TESTS[0]
    );
    assert!(!mutation_suite_qualified(Some(0), false, &duplicate));
}

#[test]
fn verifier_mutation_policy_requires_successful_process_completion() {
    let complete = transcript(REQUIRED_MUTATION_TESTS.iter().copied());
    assert!(!mutation_suite_qualified(Some(1), false, &complete));
    assert!(!mutation_suite_qualified(Some(0), true, &complete));
}

#[test]
fn verifier_mutation_policy_rejects_noncanonical_cargo_counts() {
    let complete = transcript(REQUIRED_MUTATION_TESTS.iter().copied());
    assert!(!mutation_suite_qualified(
        Some(0),
        false,
        &complete.replacen("running 34 tests", "running 034 tests", 1),
    ));
    assert!(!mutation_suite_qualified(
        Some(0),
        false,
        &complete.replacen("34 passed;", "034 passed;", 1),
    ));
    assert!(!mutation_suite_qualified(
        Some(0),
        false,
        &complete.replacen("34 passed;", "34  passed;", 1),
    ));
}

fn transcript<'a>(tests: impl Iterator<Item = &'a str>) -> String {
    let mut output = format!("running {} tests\n", REQUIRED_MUTATION_TESTS.len());
    for test in tests {
        writeln!(
            output,
            "test producer::tla_exec::mutation_tests::{test} ... ok"
        )
        .expect("write mutation transcript fixture");
    }
    writeln!(
        output,
        "test result: ok. {} passed; 0 failed; 0 ignored; 0 measured; finished",
        REQUIRED_MUTATION_TESTS.len()
    )
    .expect("write mutation summary fixture");
    output
}
