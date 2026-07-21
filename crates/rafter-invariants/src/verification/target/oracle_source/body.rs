//! Trusted observation and reserved-channel analysis within one test body.

use std::collections::BTreeSet;

use proc_macro2::{Group, TokenStream, TokenTree};
use syn::{
    visit::{self, Visit},
    Block, File, ItemFn, ItemMacro, LitStr, Macro,
};

use super::imports::Imports;
use crate::verification::target::ORACLE_MACROS;

pub(super) fn analyze_token_block(body: &Group, imports: &Imports) -> Result<(), String> {
    scan_forbidden_tokens(&body.stream())?;
    let block = syn::parse2::<Block>(TokenStream::from(TokenTree::Group(body.clone())))
        .map_err(|error| format!("parse proptest body: {error}"))?;
    analyze_block(&block, imports)
}

pub(super) fn analyze_block(block: &Block, imports: &Imports) -> Result<(), String> {
    let mut analyzer = OracleBodyAnalyzer {
        imports,
        trusted_observations: 0,
        defects: BTreeSet::new(),
    };
    analyzer.visit_block(block);
    if !analyzer.defects.is_empty() {
        return Err(analyzer.defects.into_iter().collect::<Vec<_>>().join("; "));
    }
    if analyzer.trusted_observations == 0 {
        return Err("declaration does not invoke a trusted oracle macro".to_owned());
    }
    Ok(())
}

pub(super) fn scan_reserved_channels(file: &File) -> Result<(), String> {
    let mut analyzer = ReservedChannelAnalyzer::default();
    analyzer.visit_file(file);
    if analyzer.defects.is_empty() {
        Ok(())
    } else {
        Err(analyzer.defects.into_iter().collect::<Vec<_>>().join("; "))
    }
}

struct OracleBodyAnalyzer<'a> {
    imports: &'a Imports,
    trusted_observations: usize,
    defects: BTreeSet<String>,
}

impl Visit<'_> for OracleBodyAnalyzer<'_> {
    fn visit_macro(&mut self, invocation: &Macro) {
        let Some(name) = invocation
            .path
            .segments
            .last()
            .map(|part| part.ident.to_string())
        else {
            return;
        };
        if ORACLE_MACROS.contains(&name.as_str()) {
            if self.imports.trusted_oracle(&invocation.path) {
                self.trusted_observations += 1;
            } else {
                self.defects
                    .insert(format!("invokes untrusted oracle macro `{name}`"));
            }
        }
        if let Err(error) = scan_forbidden_tokens(&invocation.tokens) {
            self.defects.insert(error);
        }
        visit::visit_macro(self, invocation);
    }

    fn visit_expr_path(&mut self, expression: &syn::ExprPath) {
        for segment in &expression.path.segments {
            let name = segment.ident.to_string();
            if forbidden_identifier(&name) {
                self.defects.insert(format!(
                    "references reserved oracle channel `{name}` directly"
                ));
            }
        }
        visit::visit_expr_path(self, expression);
    }

    fn visit_lit_str(&mut self, literal: &LitStr) {
        if forbidden_text(&literal.value()) {
            self.defects
                .insert("contains a reserved oracle marker or token name".to_owned());
        }
        visit::visit_lit_str(self, literal);
    }

    fn visit_item_fn(&mut self, _function: &ItemFn) {
        self.defects
            .insert("contains a nested function outside the registered oracle body".to_owned());
    }

    fn visit_item_macro(&mut self, item: &ItemMacro) {
        if item.ident.as_ref().is_some_and(|name| {
            ORACLE_MACROS.contains(&name.to_string().as_str()) || name == "proptest"
        }) {
            self.defects
                .insert("declares a local macro in the registered oracle body".to_owned());
        }
    }
}

#[derive(Default)]
struct ReservedChannelAnalyzer {
    defects: BTreeSet<String>,
}

impl Visit<'_> for ReservedChannelAnalyzer {
    fn visit_macro(&mut self, invocation: &Macro) {
        if let Err(error) = scan_forbidden_tokens(&invocation.tokens) {
            self.defects.insert(error);
        }
        visit::visit_macro(self, invocation);
    }

    fn visit_expr_path(&mut self, expression: &syn::ExprPath) {
        for segment in &expression.path.segments {
            let name = segment.ident.to_string();
            if forbidden_identifier(&name) {
                self.defects.insert(format!(
                    "references reserved oracle channel `{name}` directly"
                ));
            }
        }
        visit::visit_expr_path(self, expression);
    }

    fn visit_lit_str(&mut self, literal: &LitStr) {
        if forbidden_text(&literal.value()) {
            self.defects
                .insert("contains a reserved oracle marker or token name".to_owned());
        }
        visit::visit_lit_str(self, literal);
    }
}

fn forbidden_identifier(name: &str) -> bool {
    name.starts_with("__oracle_")
        || matches!(
            name,
            "ORACLE_TOKEN_ENV"
                | "ORACLE_OBSERVED_PREFIX"
                | "ORACLE_VIOLATION_PREFIX"
                | "TOKEN_ENV"
                | "OBSERVED_PREFIX"
                | "VIOLATION_PREFIX"
        )
}

fn forbidden_text(text: &str) -> bool {
    text.contains("RAFTER_INVARIANT_ORACLE_") || text.contains("RAFTER_INVARIANT_DETECTOR_WITNESS")
}

fn scan_forbidden_tokens(tokens: &TokenStream) -> Result<(), String> {
    for token in tokens.clone() {
        match token {
            TokenTree::Group(group) => scan_forbidden_tokens(&group.stream())?,
            TokenTree::Ident(identifier) if forbidden_identifier(&identifier.to_string()) => {
                return Err(format!(
                    "references reserved oracle channel `{identifier}` directly"
                ));
            }
            TokenTree::Literal(literal) if forbidden_text(&literal.to_string()) => {
                return Err("contains a reserved oracle marker or token name".to_owned());
            }
            _ => {}
        }
    }
    Ok(())
}
