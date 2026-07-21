//! Rust source inventories used by architecture ownership scenarios.

use std::collections::{BTreeMap, BTreeSet};

use syn::{parse::Parser, visit::Visit, Expr, Item, Lit, Visibility};

pub(crate) fn public_free_functions(source: &str) -> BTreeSet<String> {
    syn::parse_file(source)
        .expect("parse source for public function inventory")
        .items
        .into_iter()
        .filter_map(|item| match item {
            Item::Fn(function) if !matches!(function.vis, Visibility::Inherited) => {
                Some(function.sig.ident.to_string())
            }
            _ => None,
        })
        .collect()
}

pub(crate) fn public_associated_methods(source: &str) -> BTreeSet<String> {
    syn::parse_file(source)
        .expect("parse source for associated method inventory")
        .items
        .into_iter()
        .filter_map(|item| match item {
            Item::Impl(implementation) => Some(implementation),
            _ => None,
        })
        .flat_map(|implementation| {
            let owner = match implementation.self_ty.as_ref() {
                syn::Type::Path(path) => path
                    .path
                    .segments
                    .last()
                    .map(|segment| segment.ident.to_string()),
                _ => None,
            };
            let trait_method = implementation.trait_.is_some();
            implementation.items.into_iter().filter_map(move |item| {
                let syn::ImplItem::Fn(function) = item else {
                    return None;
                };
                if !trait_method && matches!(function.vis, Visibility::Inherited) {
                    return None;
                }
                Some(format!(
                    "{}::{}",
                    owner.as_deref().unwrap_or("<unknown>"),
                    function.sig.ident
                ))
            })
        })
        .collect()
}

pub(crate) fn declared_macros(source: &str) -> BTreeSet<String> {
    syn::parse_file(source)
        .expect("parse source for macro inventory")
        .items
        .into_iter()
        .filter_map(|item| match item {
            Item::Macro(macro_) => macro_.ident.map(|identifier| identifier.to_string()),
            _ => None,
        })
        .collect()
}

pub(crate) fn string_array_constant(source: &str, name: &str) -> Vec<String> {
    let syntax = syn::parse_file(source).expect("parse source for string-array constant");
    let constant = syntax
        .items
        .iter()
        .find_map(|item| match item {
            Item::Const(constant) if constant.ident == name => Some(constant),
            _ => None,
        })
        .unwrap_or_else(|| panic!("missing {name} constant"));
    let Expr::Array(array) = constant.expr.as_ref() else {
        panic!("{name} must be an array expression");
    };
    array
        .elems
        .iter()
        .map(|element| match element {
            Expr::Lit(literal) => match &literal.lit {
                Lit::Str(value) => value.value(),
                _ => panic!("{name} contains a non-string literal"),
            },
            _ => panic!("{name} contains a non-literal element"),
        })
        .collect()
}

pub(crate) fn macro_second_identifiers(source: &str, macro_name: &str) -> Vec<String> {
    syn::parse_file(source)
        .expect("parse source for macro invocation inventory")
        .items
        .into_iter()
        .filter_map(|item| match item {
            Item::Macro(macro_) if macro_.mac.path.is_ident(macro_name) => Some(macro_.mac.tokens),
            _ => None,
        })
        .map(|tokens| {
            let identifiers =
                syn::punctuated::Punctuated::<syn::Ident, syn::Token![,]>::parse_terminated
                    .parse2(tokens)
                    .expect("parse mutation selector arguments");
            identifiers
                .iter()
                .nth(1)
                .expect("mutation selector needs a test identity")
                .to_string()
        })
        .collect()
}

pub(crate) fn function_call_counts(source: &str, function_name: &str) -> BTreeMap<String, usize> {
    let syntax = syn::parse_file(source).expect("parse source for call-edge inventory");
    let function = syntax
        .items
        .iter()
        .find_map(|item| match item {
            Item::Fn(function) if function.sig.ident == function_name => Some(function),
            _ => None,
        })
        .unwrap_or_else(|| panic!("missing {function_name} function"));
    let mut calls = FunctionCallCollector::default();
    calls.visit_block(&function.block);
    calls
        .paths
        .into_iter()
        .fold(BTreeMap::new(), |mut counts, path| {
            *counts.entry(path).or_insert(0) += 1;
            counts
        })
}

#[derive(Default)]
struct FunctionCallCollector {
    paths: Vec<String>,
}

impl<'ast> Visit<'ast> for FunctionCallCollector {
    fn visit_expr_call(&mut self, call: &'ast syn::ExprCall) {
        if let Expr::Path(path) = call.func.as_ref() {
            self.paths.push(
                path.path
                    .segments
                    .iter()
                    .map(|segment| segment.ident.to_string())
                    .collect::<Vec<_>>()
                    .join("::"),
            );
        }
        syn::visit::visit_expr_call(self, call);
    }

    fn visit_expr_method_call(&mut self, call: &'ast syn::ExprMethodCall) {
        self.paths.push(format!("method::{}", call.method));
        syn::visit::visit_expr_method_call(self, call);
    }

    fn visit_macro(&mut self, macro_: &'ast syn::Macro) {
        self.paths.push(format!(
            "macro::{}",
            macro_
                .path
                .segments
                .iter()
                .map(|segment| segment.ident.to_string())
                .collect::<Vec<_>>()
                .join("::")
        ));
        syn::visit::visit_macro(self, macro_);
    }
}
