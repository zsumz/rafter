//! Rust path normalization and syntax visitors for ownership assertions.

use proc_macro2::{TokenStream, TokenTree};
use syn::{visit::Visit, Expr, ItemMod, ItemUse, UseTree};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum PathContext {
    Import,
    Expression,
}

#[derive(Debug)]
pub(crate) struct RustPathOccurrence {
    pub context: PathContext,
    pub written: Vec<String>,
    pub normalized: Vec<String>,
}

pub(crate) struct RustPathCollector {
    module: Vec<String>,
    pub occurrences: Vec<RustPathOccurrence>,
    pub crate_root_aliases: Vec<String>,
    pub process_macro_tokens: Vec<String>,
    pub macro_identifier_groups: Vec<Vec<String>>,
}

impl RustPathCollector {
    pub(crate) fn new(module: Vec<String>) -> Self {
        Self {
            module,
            occurrences: Vec::new(),
            crate_root_aliases: Vec::new(),
            process_macro_tokens: Vec::new(),
            macro_identifier_groups: Vec::new(),
        }
    }

    fn record(&mut self, context: PathContext, written: Vec<String>) {
        if let Some(normalized) = normalize_rust_path(&self.module, &written, context) {
            self.occurrences.push(RustPathOccurrence {
                context,
                written,
                normalized,
            });
        }
    }
}

impl<'ast> Visit<'ast> for RustPathCollector {
    fn visit_visibility(&mut self, _visibility: &'ast syn::Visibility) {}

    fn visit_item_use(&mut self, item: &'ast ItemUse) {
        let mut paths = Vec::new();
        flatten_use_tree(&item.tree, Vec::new(), &mut paths);
        for path in paths {
            self.record(PathContext::Import, path);
        }
    }

    fn visit_item_extern_crate(&mut self, item: &'ast syn::ItemExternCrate) {
        if item.ident == "self" || item.ident == "rafter_invariants" {
            self.crate_root_aliases.push(
                item.rename
                    .as_ref()
                    .map_or_else(|| "self".to_owned(), |(_, alias)| alias.to_string()),
            );
        }
    }

    fn visit_item_mod(&mut self, item: &'ast ItemMod) {
        if item.content.is_none() {
            syn::visit::visit_item_mod(self, item);
            return;
        }
        self.module.push(item.ident.to_string());
        syn::visit::visit_item_mod(self, item);
        self.module.pop();
    }

    fn visit_macro(&mut self, macro_: &'ast syn::Macro) {
        let source = macro_.tokens.to_string();
        for path in rooted_paths_in_tokens(macro_.tokens.clone()) {
            self.record(PathContext::Expression, path);
        }
        let identifiers = token_identifiers(macro_.tokens.clone());
        if identifiers
            .windows(2)
            .any(|window| window == ["execution".to_owned(), "process".to_owned()])
        {
            self.process_macro_tokens.push(source);
        }
        self.macro_identifier_groups.push(identifiers);
        syn::visit::visit_macro(self, macro_);
    }

    fn visit_path(&mut self, path: &'ast syn::Path) {
        let segments = path
            .segments
            .iter()
            .map(|segment| segment.ident.to_string())
            .collect::<Vec<_>>();
        self.record(PathContext::Expression, segments);
        syn::visit::visit_path(self, path);
    }
}

#[derive(Default)]
pub(crate) struct BlockingProcessCollector {
    pub calls: Vec<String>,
}

impl<'ast> Visit<'ast> for BlockingProcessCollector {
    fn visit_expr_method_call(&mut self, call: &'ast syn::ExprMethodCall) {
        let method = call.method.to_string();
        if is_blocking_process_method(&method) {
            self.calls.push(method);
        }
        syn::visit::visit_expr_method_call(self, call);
    }

    fn visit_expr_call(&mut self, call: &'ast syn::ExprCall) {
        if let Expr::Path(path) = call.func.as_ref() {
            if let Some(method) = path
                .path
                .segments
                .last()
                .map(|segment| segment.ident.to_string())
            {
                if is_blocking_process_method(&method) {
                    self.calls.push(method);
                }
            }
        }
        syn::visit::visit_expr_call(self, call);
    }

    fn visit_macro(&mut self, macro_: &'ast syn::Macro) {
        self.calls
            .extend(blocking_process_calls_in_tokens(macro_.tokens.clone()));
        syn::visit::visit_macro(self, macro_);
    }
}

fn is_blocking_process_method(method: &str) -> bool {
    matches!(method, "wait" | "wait_with_output" | "output" | "status")
}

fn flatten_use_tree(tree: &UseTree, prefix: Vec<String>, paths: &mut Vec<Vec<String>>) {
    match tree {
        UseTree::Path(path) => {
            let mut prefix = prefix;
            prefix.push(path.ident.to_string());
            flatten_use_tree(&path.tree, prefix, paths);
        }
        UseTree::Name(name) => {
            let mut path = prefix;
            path.push(name.ident.to_string());
            paths.push(path);
        }
        UseTree::Rename(rename) => {
            let mut path = prefix;
            path.push(rename.ident.to_string());
            paths.push(path);
        }
        UseTree::Glob(_) => {
            let mut path = prefix;
            path.push("*".to_owned());
            paths.push(path);
        }
        UseTree::Group(group) => {
            for tree in &group.items {
                flatten_use_tree(tree, prefix.clone(), paths);
            }
        }
    }
}

pub(crate) fn normalize_rust_path(
    module: &[String],
    written: &[String],
    context: PathContext,
) -> Option<Vec<String>> {
    let written = written
        .iter()
        .map(|segment| segment.strip_prefix("r#").unwrap_or(segment).to_owned())
        .collect::<Vec<_>>();
    let first = written.first()?.as_str();
    let mut normalized = vec!["crate".to_owned()];
    let mut index = 0;
    match first {
        "crate" | "rafter_invariants" => index = 1,
        "self" => {
            normalized.extend_from_slice(module);
            index = 1;
        }
        "super" => {
            let mut owner = module.to_vec();
            while written.get(index).map(String::as_str) == Some("super") {
                owner.pop()?;
                index += 1;
            }
            normalized.extend(owner);
        }
        _ => return None,
    }
    normalized.extend(written[index..].iter().cloned());
    if context == PathContext::Import && normalized.last().map(String::as_str) == Some("self") {
        normalized.pop();
    }
    Some(normalized)
}

fn token_identifiers(tokens: TokenStream) -> Vec<String> {
    let mut identifiers = Vec::new();
    collect_token_identifiers(tokens, &mut identifiers);
    identifiers
}

fn collect_token_identifiers(tokens: TokenStream, identifiers: &mut Vec<String>) {
    for token in tokens {
        match token {
            TokenTree::Group(group) => collect_token_identifiers(group.stream(), identifiers),
            TokenTree::Ident(identifier) => {
                let identifier = identifier.to_string();
                identifiers.push(
                    identifier
                        .strip_prefix("r#")
                        .unwrap_or(&identifier)
                        .to_owned(),
                );
            }
            TokenTree::Literal(_) | TokenTree::Punct(_) => {}
        }
    }
}

fn rooted_paths_in_tokens(tokens: TokenStream) -> Vec<Vec<String>> {
    let tokens = tokens.into_iter().collect::<Vec<_>>();
    let mut paths = Vec::new();
    for token in &tokens {
        if let TokenTree::Group(group) = token {
            paths.extend(rooted_paths_in_tokens(group.stream()));
        }
    }
    for (index, token) in tokens.iter().enumerate() {
        let TokenTree::Ident(identifier) = token else {
            continue;
        };
        let root = normalized_identifier(identifier);
        if root != "crate" && root != "rafter_invariants" {
            continue;
        }
        let mut path = vec![root];
        let mut cursor = index + 1;
        while token_is_colon(tokens.get(cursor)) && token_is_colon(tokens.get(cursor + 1)) {
            let Some(TokenTree::Ident(segment)) = tokens.get(cursor + 2) else {
                break;
            };
            path.push(normalized_identifier(segment));
            cursor += 3;
        }
        if path.len() > 1 {
            paths.push(path);
        }
    }
    paths
}

fn normalized_identifier(identifier: &proc_macro2::Ident) -> String {
    let identifier = identifier.to_string();
    identifier
        .strip_prefix("r#")
        .unwrap_or(&identifier)
        .to_owned()
}

fn token_is_colon(token: Option<&TokenTree>) -> bool {
    matches!(token, Some(TokenTree::Punct(punctuation)) if punctuation.as_char() == ':')
}

fn blocking_process_calls_in_tokens(tokens: TokenStream) -> Vec<String> {
    let tokens = tokens.into_iter().collect::<Vec<_>>();
    let mut calls = Vec::new();
    for (index, token) in tokens.iter().enumerate() {
        if let TokenTree::Group(group) = token {
            calls.extend(blocking_process_calls_in_tokens(group.stream()));
        }
        let TokenTree::Ident(identifier) = token else {
            continue;
        };
        let method = identifier.to_string();
        if is_blocking_process_method(&method)
            && matches!(tokens.get(index + 1), Some(TokenTree::Group(group)) if group.delimiter() == proc_macro2::Delimiter::Parenthesis)
        {
            calls.push(method);
        }
    }
    calls
}
