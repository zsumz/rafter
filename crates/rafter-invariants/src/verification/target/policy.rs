//! Trusted oracle vocabulary and exact support-source policy.

use std::{collections::BTreeSet, path::Path};

pub(super) const ORACLE_MACROS: &[&str] = &[
    "oracle_assert",
    "oracle_assert_eq",
    "oracle_assert_ne",
    "oracle_expect_err",
    "oracle_invoke_recorder",
    "oracle_prop_assert",
    "oracle_prop_assert_eq",
    "oracle_violation",
];

const ORACLE_MACRO_SOURCE: &str = "crates/rafter-invariant-test/src/oracle/macros.rs";
const ORACLE_CALL_SOURCE: &str = "crates/rafter-invariant-test/src/oracle/call.rs";
const DETECTOR_SESSION_SOURCE: &str = "crates/rafter-invariant-test/src/detector/session.rs";

pub(super) struct OracleSourcePolicy<'a> {
    crate_name: &'a str,
    workspace: &'a Path,
    reserved_macros: BTreeSet<String>,
}

impl<'a> OracleSourcePolicy<'a> {
    pub(super) fn new(crate_name: &'a str, workspace: &'a Path, reserved_macros: &[&str]) -> Self {
        Self {
            crate_name,
            workspace,
            reserved_macros: reserved_macros
                .iter()
                .map(|name| (*name).to_owned())
                .collect(),
        }
    }

    pub(super) fn reserves(&self, name: &str) -> bool {
        self.reserved_macros.contains(name)
    }

    pub(super) fn reviewed_support_item_macro(
        &self,
        item: &syn::ItemMacro,
        source_file: &Path,
    ) -> bool {
        if self.crate_name != "rafter_invariant_test" {
            return false;
        }
        let Ok(source) = source_file.strip_prefix(self.workspace) else {
            return false;
        };
        let path = item
            .mac
            .path
            .segments
            .iter()
            .map(|segment| segment.ident.to_string())
            .collect::<Vec<_>>();
        match path.as_slice() {
            [name] if name == "impl_oracle_call" => source == Path::new(ORACLE_CALL_SOURCE),
            [krate, name] if krate == "std" && name == "thread_local" => {
                source == Path::new(DETECTOR_SESSION_SOURCE)
            }
            _ => false,
        }
    }

    pub(super) fn canonical_oracle_macro_definition(
        &self,
        module: &[String],
        source_file: &Path,
    ) -> bool {
        self.crate_name == "rafter_invariant_test"
            && module.iter().map(String::as_str).eq(["oracle", "macros"])
            && source_file.strip_prefix(self.workspace).ok() == Some(Path::new(ORACLE_MACRO_SOURCE))
    }
}

pub(super) fn proptest_invocation(item: &syn::ItemMacro) -> bool {
    if item.ident.is_some() {
        return false;
    }
    let path = item
        .mac
        .path
        .segments
        .iter()
        .map(|segment| segment.ident.to_string())
        .collect::<Vec<_>>();
    match path.as_slice() {
        [name] => name == "proptest",
        [krate, name] => krate == "proptest" && name == "proptest",
        _ => false,
    }
}

pub(super) fn proptest_declarations(tokens: &proc_macro2::TokenStream) -> Vec<String> {
    let tokens = tokens.clone().into_iter().collect::<Vec<_>>();
    tokens
        .windows(2)
        .filter_map(|pair| match (&pair[0], &pair[1]) {
            (proc_macro2::TokenTree::Ident(keyword), proc_macro2::TokenTree::Ident(name))
                if keyword == "fn" =>
            {
                Some(name.to_string())
            }
            _ => None,
        })
        .collect()
}
