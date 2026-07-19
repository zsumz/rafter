//! Parser and expansion contracts for the detector-test attribute.

use quote::quote;

use super::expand_checked;

const SIGNATURE_ERROR: &str =
    "detector_test requires a safe, synchronous, zero-argument function returning ()";

#[test]
fn expands_the_exact_session_protocol() {
    let expanded = expand_checked(
        &quote!(),
        quote!(
            fn fixture() {}
        ),
    )
    .unwrap();
    let source = expanded.to_string();
    assert!(source.contains(":: rafter_invariant_test :: __begin_detector_test"));
    assert!(source.contains(":: rafter_invariant_test :: __detector_test_outcome"));
    assert!(source.contains("-> :: rafter_invariant_test :: DetectorTestOutcome"));
}

#[test]
fn preserves_the_inert_ignore_attribute() {
    let expanded = expand_checked(
        &quote!(),
        quote!(
            #[ignore = "runner fixture"]
            fn fixture() {}
        ),
    )
    .unwrap();
    let source = expanded.to_string();
    assert!(source.contains("ignore = \"runner fixture\""));
    assert!(source.contains("# [test]"));
}

#[test]
fn rejects_attribute_arguments() {
    let error = expand_checked(
        &quote!(unexpected),
        quote!(
            fn fixture() {}
        ),
    )
    .unwrap_err();
    assert_eq!(error.to_string(), "detector_test does not accept arguments");
}

#[test]
fn rejects_every_unsupported_signature_shape() {
    for source in [
        quote!(
            async fn fixture() {}
        ),
        quote!(
            const fn fixture() {}
        ),
        quote!(
            unsafe fn fixture() {}
        ),
        quote!(
            extern "C" fn fixture() {}
        ),
        quote!(
            fn fixture<T>() {}
        ),
        quote!(
            fn fixture(value: usize) {
                let _ = value;
            }
        ),
        quote!(
            fn fixture() -> () {}
        ),
    ] {
        let error = expand_checked(&quote!(), source).unwrap_err();
        assert_eq!(error.to_string(), SIGNATURE_ERROR);
    }
}

#[test]
fn preserves_a_concrete_where_clause() {
    let expanded = expand_checked(
        &quote!(),
        quote!(
            fn fixture()
            where
                String: Clone,
            {
            }
        ),
    )
    .unwrap();
    assert!(expanded.to_string().contains("where String : Clone"));
}

#[test]
fn rejects_attributes_other_than_ignore() {
    for source in [
        quote!(
            #[should_panic]
            fn fixture() {}
        ),
        quote!(
            #[ignore(reason)]
            fn fixture() {}
        ),
        quote!(
            #[ignore = 7]
            fn fixture() {}
        ),
    ] {
        let error = expand_checked(&quote!(), source).unwrap_err();
        assert_eq!(
            error.to_string(),
            "detector_test permits only the inert #[ignore] attribute"
        );
    }
}
