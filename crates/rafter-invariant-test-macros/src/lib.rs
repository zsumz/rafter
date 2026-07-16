//! Trusted wrapper for detector-level negative fixtures.

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, ItemFn, ReturnType};

/// Turn a zero-argument fixture into an exact libtest whose successful return
/// carries the invocation proofs accumulated by `rafter-invariant-test`.
#[proc_macro_attribute]
pub fn detector_test(arguments: TokenStream, item: TokenStream) -> TokenStream {
    if !arguments.is_empty() {
        return syn::Error::new(
            proc_macro2::Span::call_site(),
            "detector_test does not accept arguments",
        )
        .into_compile_error()
        .into();
    }

    let mut function = parse_macro_input!(item as ItemFn);
    if function.sig.asyncness.is_some()
        || function.sig.unsafety.is_some()
        || function.sig.abi.is_some()
        || !function.sig.generics.params.is_empty()
        || !function.sig.inputs.is_empty()
        || !matches!(function.sig.output, ReturnType::Default)
    {
        return syn::Error::new_spanned(
            &function.sig,
            "detector_test requires a safe, synchronous, zero-argument function returning ()",
        )
        .into_compile_error()
        .into();
    }
    if function
        .attrs
        .iter()
        .any(|attribute| !attribute.path().is_ident("ignore"))
    {
        return syn::Error::new_spanned(
            &function,
            "detector_test permits only the inert #[ignore] attribute",
        )
        .into_compile_error()
        .into();
    }

    let body = function.block;
    function.sig.output = syn::parse_quote!(-> ::rafter_invariant_test::DetectorTestOutcome);
    function.attrs.push(syn::parse_quote!(#[test]));
    function.block = Box::new(syn::parse_quote!({
        ::rafter_invariant_test::__begin_detector_test();
        (|| #body)();
        ::rafter_invariant_test::__detector_test_outcome()
    }));

    quote!(#function).into()
}
