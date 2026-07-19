//! Trusted wrapper for detector-level negative fixtures.

use proc_macro::TokenStream;

mod detector_test;

/// Turn a zero-argument fixture into an exact libtest whose successful return
/// carries the invocation proofs accumulated by `rafter-invariant-test`.
#[proc_macro_attribute]
pub fn detector_test(arguments: TokenStream, item: TokenStream) -> TokenStream {
    let arguments = arguments.into();
    detector_test::expand(&arguments, item.into()).into()
}
