//! Parser, validator, and expansion for the detector-test attribute.

use proc_macro2::TokenStream;
use quote::quote;
use syn::{ItemFn, ReturnType};

pub(super) fn expand(arguments: &TokenStream, item: TokenStream) -> TokenStream {
    expand_checked(arguments, item).unwrap_or_else(syn::Error::into_compile_error)
}

fn expand_checked(arguments: &TokenStream, item: TokenStream) -> syn::Result<TokenStream> {
    if !arguments.is_empty() {
        return Err(syn::Error::new(
            proc_macro2::Span::call_site(),
            "detector_test does not accept arguments",
        ));
    }

    let mut function = syn::parse2::<ItemFn>(item)?;
    validate_signature(&function)?;
    validate_attributes(&function)?;

    let body = function.block;
    function.sig.output = syn::parse_quote!(-> ::rafter_invariant_test::DetectorTestOutcome);
    function.attrs.push(syn::parse_quote!(#[test]));
    function.block = Box::new(syn::parse_quote!({
        ::rafter_invariant_test::__begin_detector_test();
        (|| #body)();
        ::rafter_invariant_test::__detector_test_outcome()
    }));

    Ok(quote!(#function))
}

fn validate_signature(function: &ItemFn) -> syn::Result<()> {
    let signature = &function.sig;
    if signature.asyncness.is_some()
        || signature.constness.is_some()
        || signature.unsafety.is_some()
        || signature.abi.is_some()
        || !signature.generics.params.is_empty()
        || !signature.inputs.is_empty()
        || !matches!(signature.output, ReturnType::Default)
    {
        return Err(syn::Error::new_spanned(
            signature,
            "detector_test requires a safe, synchronous, zero-argument function returning ()",
        ));
    }
    Ok(())
}

fn validate_attributes(function: &ItemFn) -> syn::Result<()> {
    let attributes_are_inert = function.attrs.iter().all(|attribute| {
        if !attribute.path().is_ident("ignore") {
            return false;
        }
        match &attribute.meta {
            syn::Meta::Path(_) => true,
            syn::Meta::NameValue(value) => matches!(
                &value.value,
                syn::Expr::Lit(expression) if matches!(expression.lit, syn::Lit::Str(_))
            ),
            syn::Meta::List(_) => false,
        }
    });
    if !attributes_are_inert {
        return Err(syn::Error::new_spanned(
            function,
            "detector_test permits only the inert #[ignore] attribute",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
