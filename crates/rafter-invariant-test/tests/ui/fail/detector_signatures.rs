//! Proves the signature a detector test is allowed to have.
//!
//! Async, const, unsafe, extern, generic, parameterized, and returning forms
//! are each refused at compile time, leaving exactly the plain zero-argument
//! function the gate can invoke unambiguously. The concrete `where` clause
//! that stays legal is the passing fixtures' half.

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
