//! Syntax traversal that drives include and module acceptance.
//!
//! Walks one parsed source, skipping definitively inactive items, and stops at
//! the first rejection so the reported error names the construct that left the
//! source binding contract.

use syn::{parse::Parser, punctuated::Punctuated, visit::Visit, Attribute, Expr, Macro, Token};

use super::{
    collect_alias_scope, contains_dynamic_macro_invocation, contains_generated_out_of_line_module,
    contains_include_invocation, contains_include_reference, contains_out_of_line_module_argument,
    is_include_name, item_is_definitively_inactive, path_is_ident, resolve_scoped_alias,
    unraw_ident, visible_include_aliases, RustIncludeValidator, RustSourceKind, ScopedAlias,
};

impl<'ast> Visit<'ast> for RustIncludeValidator<'_> {
    fn visit_item(&mut self, item: &'ast syn::Item) {
        if self.error.is_some() {
            return;
        }
        match item_is_definitively_inactive(item) {
            Ok(true) => return,
            Ok(false) => {}
            Err(error) => {
                self.error = Some(format!(
                    "analyze cfg attributes in {}: {error}",
                    self.source_path.display()
                ));
                return;
            }
        }
        syn::visit::visit_item(self, item);
    }

    fn visit_macro(&mut self, invocation: &'ast Macro) {
        let unqualified = invocation.path.get_ident().map(unraw_ident);
        let visible_aliases = visible_include_aliases(&self.alias_scopes);
        let canonical = unqualified
            .as_ref()
            .and_then(
                |name| match resolve_scoped_alias(&self.alias_scopes, name) {
                    ScopedAlias::Include(canonical) => Some(canonical),
                    ScopedAlias::Shadowed => None,
                    ScopedAlias::Unbound => match self.included_alias(name) {
                        ScopedAlias::Include(canonical) => Some(canonical),
                        ScopedAlias::Unbound if is_include_name(name) => Some(name.clone()),
                        ScopedAlias::Shadowed | ScopedAlias::Unbound => None,
                    },
                },
            )
            .or_else(|| self.qualified_alias(invocation));
        let qualified_include = invocation
            .path
            .segments
            .last()
            .is_some_and(|segment| is_include_name(&unraw_ident(&segment.ident)))
            && unqualified.is_none();
        if self.error.is_none() && qualified_include {
            self.error = Some(format!(
                "qualified include macros in {} are outside the source binding contract",
                self.source_path.display()
            ));
        } else if self.error.is_none() {
            if let Some(name) = canonical.as_deref() {
                self.error = self.validate_include(invocation, name).err();
            }
        }
        let macro_generated_input = path_is_ident(&invocation.path, "macro_rules")
            && (contains_include_reference(invocation, &visible_aliases)
                || contains_dynamic_macro_invocation(invocation)
                || contains_generated_out_of_line_module(invocation))
            || canonical.is_none() && contains_out_of_line_module_argument(invocation);
        if self.error.is_none() && macro_generated_input {
            self.error = Some(format!(
                "macro-generated compiler inputs in {} are outside the source binding contract",
                self.source_path.display()
            ));
        } else if self.error.is_none()
            && canonical.is_none()
            && contains_include_invocation(invocation, &visible_aliases)
        {
            match Punctuated::<Expr, Token![,]>::parse_terminated.parse2(invocation.tokens.clone())
            {
                Ok(expressions) => {
                    for expression in &expressions {
                        self.visit_expr(expression);
                        if self.error.is_some() {
                            break;
                        }
                    }
                    return;
                }
                Err(_) => {
                    self.error = Some(format!(
                        "include macros nested in opaque macro input in {} are outside the source binding contract",
                        self.source_path.display()
                    ));
                }
            }
        }
        if self.error.is_none() {
            syn::visit::visit_macro(self, invocation);
        }
    }

    fn visit_block(&mut self, block: &'ast syn::Block) {
        let scope = collect_alias_scope(block.stmts.iter().filter_map(|statement| {
            if let syn::Stmt::Item(item) = statement {
                Some(item)
            } else {
                None
            }
        }));
        self.alias_scopes.push(scope);
        syn::visit::visit_block(self, block);
        self.alias_scopes.pop();
    }

    fn visit_attribute(&mut self, attribute: &'ast Attribute) {
        if self.error.is_none() {
            match self.effective_path_metas(std::slice::from_ref(attribute)) {
                Ok(paths) if self.inline_module_depth > 0 && !paths.is_empty() => {
                    self.error = Some(format!(
                        "#[path] inside an inline module in {} is outside the portable source binding contract",
                        self.source_path.display()
                    ));
                }
                Ok(paths) => {
                    for path_meta in paths {
                        let result = self.validate_path_meta(&path_meta).and_then(|input| {
                            self.record_discovered_rust_source(
                                &input,
                                self.module_dir.clone(),
                                self.module_path.clone(),
                                RustSourceKind::Module,
                            )
                        });
                        if let Err(error) = result {
                            self.error = Some(error);
                            break;
                        }
                    }
                }
                Err(error) => self.error = Some(error),
            }
        }
        if self.error.is_none() {
            syn::visit::visit_attribute(self, attribute);
        }
    }

    fn visit_item_mod(&mut self, item: &'ast syn::ItemMod) {
        let path_metas = match self.effective_path_metas(&item.attrs) {
            Ok(paths) => paths,
            Err(error) => {
                self.error = Some(error);
                return;
            }
        };
        if path_metas.len() > 1 {
            self.error = Some(format!(
                "module {} in {} has more than one #[path] attribute",
                unraw_ident(&item.ident),
                self.source_path.display()
            ));
            return;
        }
        if self.inline_module_depth > 0 && !path_metas.is_empty() {
            self.error = Some(format!(
                "#[path] inside an inline module in {} is outside the portable source binding contract",
                self.source_path.display()
            ));
            return;
        }
        if let Some((_, items)) = &item.content {
            let previous_module_dir = self.module_dir.clone();
            let previous_module_path = self.module_path.clone();
            self.module_dir.push(unraw_ident(&item.ident));
            self.module_path.push(unraw_ident(&item.ident));
            self.inline_module_depth += 1;
            self.alias_scopes.push(collect_alias_scope(items.iter()));
            for item in items {
                self.visit_item(item);
                if self.error.is_some() {
                    break;
                }
            }
            self.alias_scopes.pop();
            self.inline_module_depth -= 1;
            self.module_dir = previous_module_dir;
            self.module_path = previous_module_path;
        } else if let Some(path_meta) = path_metas.first() {
            self.error = self.discover_path_module(item, path_meta).err();
        } else {
            self.error = self.discover_default_module(item).err();
        }
    }
}
