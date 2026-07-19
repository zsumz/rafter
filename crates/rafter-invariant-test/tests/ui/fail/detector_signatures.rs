use rafter_invariant_test::detector_test;

#[detector_test]
async fn async_fixture() {}

#[detector_test]
const fn const_fixture() {}

#[detector_test]
unsafe fn unsafe_fixture() {}

#[detector_test]
extern "C" fn extern_fixture() {}

#[detector_test]
fn generic_fixture<T>() {}

#[detector_test]
fn parameter_fixture(_: usize) {}

#[detector_test]
fn return_fixture() -> () {}

fn main() {}
