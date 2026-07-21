//! Detector-oracle trust vocabulary and source-policy predicates.

pub(super) use crate::verification::target::ORACLE_MACROS;

pub(super) const INVOCATION_MACROS: &[&str] = &["oracle_expect_err", "oracle_invoke_recorder"];
pub(super) const SAFE_BUILTIN_MACROS: &[&str] = &[
    "assert",
    "assert_eq",
    "assert_ne",
    "cfg",
    "concat",
    "debug_assert",
    "debug_assert_eq",
    "debug_assert_ne",
    "file",
    "format",
    "format_args",
    "line",
    "matches",
    "module_path",
    "panic",
    "stringify",
    "unreachable",
    "vec",
];
pub(super) const TOKEN_ONLY_MACROS: &[&str] =
    &["cfg", "concat", "file", "line", "module_path", "stringify"];
pub(super) const FORBIDDEN_WITNESS_HELPERS: &[&str] = &[
    "__oracle_detector_witness",
    "__oracle_fabricated_detector_witness",
];
pub(super) const FORBIDDEN_CALLS: &[&str] = &[
    "__oracle_detector_witness",
    "__oracle_fabricated_detector_witness",
    "exit",
    "_exit",
    "abort",
    "write",
    "write_all",
    "write_fmt",
];

pub(super) fn is_detector_test_attribute(attribute: &syn::Attribute) -> bool {
    attribute
        .path()
        .segments
        .iter()
        .map(|segment| segment.ident.to_string())
        .eq(["rafter_invariant_test", "detector_test"].map(str::to_owned))
}

pub(super) fn is_trusted_function_attribute(
    attribute: &syn::Attribute,
    imports: &ImportedPaths,
) -> bool {
    trusted_detector_test_attribute(attribute.path(), imports)
        || [
            "allow",
            "cold",
            "deny",
            "deprecated",
            "doc",
            "expect",
            "forbid",
            "ignore",
            "inline",
            "must_use",
            "track_caller",
            "warn",
        ]
        .iter()
        .any(|name| attribute.path().is_ident(name))
        || attribute.path().is_ident("cfg")
        || attribute.path().is_ident("cfg_attr")
}
use super::imports::{trusted_detector_test_attribute, ImportedPaths};
