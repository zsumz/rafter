use rafter_invariant_test::detector_test;

#[detector_test]
#[should_panic]
fn panic_fixture() {}

#[detector_test]
#[ignore(reason)]
fn malformed_ignore_fixture() {}

#[detector_test]
#[ignore = 7]
fn non_string_ignore_fixture() {}

fn main() {}
