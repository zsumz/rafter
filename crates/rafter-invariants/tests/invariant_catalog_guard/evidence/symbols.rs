//! Symbols a Rust source file actually declares, never ones it imports.
//!
//! A registry row may not point at an alias or a re-export, so the visitor
//! collects declaration sites only -- functions, associated functions,
//! constants, statics, and function declarations inside macro input.

use std::{collections::BTreeSet, path::Path};

use syn::{visit::Visit, File, ImplItemFn, ItemConst, ItemFn, ItemMacro, ItemStatic};

pub(super) fn source_declares_symbol(path: &Path, source: &str, symbol: &str) -> bool {
    if path.extension().and_then(std::ffi::OsStr::to_str) != Some("rs") {
        return source.contains(symbol);
    }
    syn::parse_file(source).is_ok_and(|file| declared_symbols(&file).contains(symbol))
}

pub(super) fn declared_symbols(file: &File) -> BTreeSet<String> {
    let mut visitor = DeclarationVisitor::default();
    visitor.visit_file(file);
    visitor.symbols
}

#[derive(Default)]
pub(super) struct DeclarationVisitor {
    symbols: BTreeSet<String>,
}

impl<'ast> Visit<'ast> for DeclarationVisitor {
    fn visit_item_fn(&mut self, function: &'ast ItemFn) {
        self.symbols.insert(function.sig.ident.to_string());
        syn::visit::visit_item_fn(self, function);
    }

    fn visit_impl_item_fn(&mut self, function: &'ast ImplItemFn) {
        self.symbols.insert(function.sig.ident.to_string());
        syn::visit::visit_impl_item_fn(self, function);
    }

    fn visit_item_const(&mut self, item: &'ast ItemConst) {
        self.symbols.insert(item.ident.to_string());
        syn::visit::visit_item_const(self, item);
    }

    fn visit_item_static(&mut self, item: &'ast ItemStatic) {
        self.symbols.insert(item.ident.to_string());
        syn::visit::visit_item_static(self, item);
    }

    fn visit_item_macro(&mut self, item: &'ast ItemMacro) {
        let tokens = item.mac.tokens.to_string();
        let mut previous = None;
        for token in tokens.split_whitespace() {
            if previous == Some("fn") {
                self.symbols.insert(
                    token
                        .trim_matches(|character: char| {
                            !character.is_alphanumeric() && character != '_'
                        })
                        .to_owned(),
                );
            }
            previous = Some(token);
        }
        syn::visit::visit_item_macro(self, item);
    }
}
