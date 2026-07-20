//! Lexical scopes, local bindings, and block-local resolver state.

use std::collections::BTreeSet;

use syn::{
    visit::{self, Visit},
    Block, Expr, ExprAssign, ItemConst, ItemMacro, ItemStatic, Local, Pat, PatIdent, PatType, Stmt,
};

use super::FunctionBodyVisitor;
use crate::verification::detector::source::{
    model::SourceDefect,
    syntax::{
        statement_may_complete_normally, statement_unconditionally_exits,
        unqualified_expression_name,
    },
};

impl FunctionBodyVisitor<'_> {
    fn inferred_value_type(&self, expression: &Expr) -> Option<Vec<String>> {
        let candidates = [
            self.resolver
                .value_expression_type_module(expression, self.module),
            self.scoped_resolver
                .value_expression_type_module(expression, self.module),
        ]
        .into_iter()
        .flatten()
        .collect::<BTreeSet<_>>();
        match candidates.into_iter().collect::<Vec<_>>().as_slice() {
            [candidate] => Some(candidate.clone()),
            _ => None,
        }
    }

    pub(super) fn analyze_block(&mut self, block: &Block) {
        let previous_guarantee = self.guaranteed;
        let previous_reachability = self.reachability;
        let previous_may_exit = self.statement.may_exit;
        let previous_may_diverge = self.statement.may_diverge;
        let previous_aliases = self.value_aliases.clone();
        let previous_bindings = self.value_bindings.clone();
        let previous_types = self.value_types.clone();
        let previous_scoped_resolver = self.scoped_resolver.clone();
        let previous_imports = self.imports.clone();
        for statement in &block.stmts {
            if let Stmt::Item(item) = statement {
                match item {
                    syn::Item::Use(item_use) => {
                        match crate::verification::target::module_active_for_test(&item_use.attrs) {
                            Ok(true) => self.scoped_resolver.add_use(item_use, self.module),
                            Ok(false) => continue,
                            Err(_) => {
                                self.facts.conditional_compilation = true;
                                self.facts.defects.insert(SourceDefect::OpaqueCallable);
                                continue;
                            }
                        }
                    }
                    syn::Item::Fn(function) => {
                        let name = function.sig.ident.to_string();
                        self.value_bindings.insert(name.clone());
                        self.facts.shadowed_values.insert(name);
                    }
                    _ => {}
                }
                self.imports.add_item(item);
            }
        }
        let mut guaranteed_reachable = previous_guarantee;
        let mut potentially_reachable = previous_reachability.is_reachable();
        let mut block_may_exit = false;
        let mut block_may_diverge = false;
        for statement in &block.stmts {
            self.guaranteed = guaranteed_reachable;
            self.reachability = potentially_reachable.into();
            self.statement.may_exit = false;
            self.statement.may_diverge = false;
            self.visit_stmt(statement);
            let statement_may_exit =
                self.statement.may_exit || statement_unconditionally_exits(statement);
            let statement_may_diverge = self.statement.may_diverge;
            block_may_exit |= statement_may_exit;
            block_may_diverge |= statement_may_diverge;
            if guaranteed_reachable && statement_may_exit {
                guaranteed_reachable = false;
            }
            if potentially_reachable && !statement_may_complete_normally(statement) {
                potentially_reachable = false;
            }
        }
        let updated_aliases = self.value_aliases.clone();
        self.value_aliases = previous_aliases;
        for binding in &previous_bindings {
            if let Some(alias) = updated_aliases.get(binding) {
                self.value_aliases.insert(binding.clone(), alias.clone());
            } else {
                self.value_aliases.remove(binding);
            }
        }
        self.value_bindings = previous_bindings;
        self.value_types = previous_types;
        self.scoped_resolver = previous_scoped_resolver;
        self.imports = previous_imports;
        self.guaranteed = previous_guarantee;
        self.reachability = previous_reachability;
        self.statement.may_exit = previous_may_exit || block_may_exit;
        self.statement.may_diverge = previous_may_diverge || block_may_diverge;
        self.function.may_diverge |= block_may_diverge;
    }

    pub(super) fn analyze_local(&mut self, local: &Local) {
        let binding = match &local.pat {
            Pat::Ident(pattern) if pattern.subpat.is_none() => Some(pattern.ident.to_string()),
            _ => None,
        };
        let alias = local
            .init
            .as_ref()
            .and_then(|init| self.alias_target(&init.expr));
        let inferred_type = local
            .init
            .as_ref()
            .and_then(|init| self.inferred_value_type(&init.expr));
        for attribute in &local.attrs {
            self.visit_attribute(attribute);
        }
        if let Some(init) = &local.init {
            self.visit_expr(&init.expr);
            if let Some((_, diverge)) = &init.diverge {
                self.with_guarantee(false, |visitor| visitor.visit_expr(diverge));
                self.statement.may_exit = true;
            }
        }
        self.visit_pat(&local.pat);
        if let Some(binding) = binding {
            self.value_bindings.insert(binding.clone());
            if let Some(inferred_type) = inferred_type {
                self.value_types.insert(binding.clone(), inferred_type);
            } else {
                self.value_types.remove(&binding);
            }
            if let Some(alias) = alias {
                self.value_aliases.insert(binding, alias);
            } else {
                self.value_aliases.remove(&binding);
            }
        }
    }

    pub(super) fn analyze_assignment(&mut self, expression: &ExprAssign) {
        self.visit_expr(&expression.right);
        if let Some(binding) = unqualified_expression_name(&expression.left) {
            if let Some(alias) = self.alias_target(&expression.right) {
                if self.guaranteed {
                    self.value_aliases.insert(binding, alias);
                } else {
                    self.value_aliases.remove(&binding);
                    self.facts.defects.insert(SourceDefect::OpaqueCallable);
                }
            } else {
                self.value_aliases.remove(&binding);
            }
        } else {
            self.visit_expr(&expression.left);
        }
    }

    pub(super) fn analyze_pattern_binding(&mut self, pattern: &PatIdent) {
        let name = pattern.ident.to_string();
        self.value_bindings.insert(name.clone());
        self.facts.shadowed_values.insert(name);
        visit::visit_pat_ident(self, pattern);
    }

    pub(super) fn analyze_typed_pattern(&mut self, pattern: &PatType) {
        if let Pat::Ident(binding) = pattern.pat.as_ref() {
            if let Some(module) = self.resolver.value_type_module(&pattern.ty, self.module) {
                self.value_types.insert(binding.ident.to_string(), module);
            }
        }
        visit::visit_pat_type(self, pattern);
    }

    pub(super) fn analyze_const(&mut self, item: &ItemConst) {
        self.facts.shadowed_values.insert(item.ident.to_string());
        visit::visit_item_const(self, item);
    }

    pub(super) fn analyze_static(&mut self, item: &ItemStatic) {
        self.facts.shadowed_values.insert(item.ident.to_string());
        visit::visit_item_static(self, item);
    }

    pub(super) fn record_macro_declaration(&mut self, item: &ItemMacro) {
        self.imports.add_macro_declaration(item);
    }
}
