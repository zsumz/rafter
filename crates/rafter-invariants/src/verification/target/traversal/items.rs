//! Test-active Rust item collection and oracle-shadow accounting.

use std::{collections::BTreeSet, path::Path};

use syn::{Attribute, ItemMod};

use super::{module_path::resolve_external_module, ModuleGraphCollector, OracleShadowImplMethod};
use crate::verification::target::{
    cfg::module_active_for_test,
    policy::{proptest_declarations, proptest_invocation},
};

impl ModuleGraphCollector<'_> {
    pub(super) fn collect_items(
        &mut self,
        items: &[syn::Item],
        module: &[String],
        path_base: &Path,
        module_dir: &Path,
        source_file: &Path,
        inherited_oracle_macros: &BTreeSet<String>,
    ) -> Result<(), String> {
        let mut visible_oracle_macros = inherited_oracle_macros.clone();
        for item in items {
            match item {
                syn::Item::Fn(function) => {
                    self.collect_function_item(
                        function,
                        module,
                        source_file,
                        &visible_oracle_macros,
                    )?;
                }
                syn::Item::Mod(item) => {
                    self.collect_module_item(
                        item,
                        module,
                        path_base,
                        module_dir,
                        source_file,
                        &visible_oracle_macros,
                    )?;
                }
                syn::Item::Macro(item) => {
                    self.collect_macro_item(item, module, source_file, &mut visible_oracle_macros)?;
                }
                syn::Item::Use(item) => {
                    module_active_for_test(&item.attrs)?;
                }
                syn::Item::ExternCrate(item) => {
                    module_active_for_test(&item.attrs)?;
                    Self::reject_macro_use(&item.attrs, source_file)?;
                }
                syn::Item::Impl(item) => {
                    self.collect_impl_item(item, module, source_file, &visible_oracle_macros)?;
                }
                syn::Item::Trait(item) => Self::validate_trait_item(item, source_file)?,
                _ => {}
            }
        }
        Ok(())
    }

    fn collect_function_item(
        &mut self,
        function: &syn::ItemFn,
        module: &[String],
        source_file: &Path,
        visible_oracle_macros: &BTreeSet<String>,
    ) -> Result<(), String> {
        if !module_active_for_test(&function.attrs)? {
            return Ok(());
        }
        self.record_declaration(
            &function.sig.ident.to_string(),
            module,
            source_file,
            visible_oracle_macros,
        );
        Ok(())
    }

    fn collect_module_item(
        &mut self,
        item: &ItemMod,
        module: &[String],
        path_base: &Path,
        module_dir: &Path,
        source_file: &Path,
        visible_oracle_macros: &BTreeSet<String>,
    ) -> Result<(), String> {
        if !module_active_for_test(&item.attrs)? {
            return Ok(());
        }
        Self::reject_macro_use(&item.attrs, source_file)?;
        let mut child_module = module.to_vec();
        child_module.push(item.ident.to_string());
        if let Some((_, inline_items)) = &item.content {
            let inline_dir = module_dir.join(item.ident.to_string());
            return self.collect_items(
                inline_items,
                &child_module,
                &inline_dir,
                &inline_dir,
                source_file,
                visible_oracle_macros,
            );
        }
        let child_file =
            self.bound_source_path(&resolve_external_module(item, path_base, module_dir)?)?;
        let child_dir =
            if child_file.file_name().and_then(std::ffi::OsStr::to_str) == Some("mod.rs") {
                child_file.parent().unwrap_or(module_dir).to_owned()
            } else {
                module_dir.join(item.ident.to_string())
            };
        let child_path_base = child_file.parent().unwrap_or(path_base);
        self.collect_file(
            &child_file,
            &child_module,
            child_path_base,
            &child_dir,
            visible_oracle_macros,
        )
    }

    fn collect_macro_item(
        &mut self,
        item: &syn::ItemMacro,
        module: &[String],
        source_file: &Path,
        visible_oracle_macros: &mut BTreeSet<String>,
    ) -> Result<(), String> {
        if !module_active_for_test(&item.attrs)? {
            return Ok(());
        }
        if let Some(name) = item
            .ident
            .as_ref()
            .filter(|name| self.policy.reserves(&name.to_string()))
        {
            let canonical_oracle_definition = self
                .policy
                .canonical_oracle_macro_definition(module, source_file);
            if !canonical_oracle_definition {
                visible_oracle_macros.insert(name.to_string());
            }
        }
        if proptest_invocation(item) {
            for name in proptest_declarations(&item.mac.tokens) {
                self.record_declaration(&name, module, source_file, visible_oracle_macros);
            }
            return Ok(());
        }
        if item.ident.is_none() && !self.policy.reviewed_support_item_macro(item, source_file) {
            return Err(format!(
                "registered Cargo target uses an unexpanded item macro outside the reviewed module graph in {}",
                source_file.display()
            ));
        }
        Ok(())
    }

    fn record_declaration(
        &mut self,
        name: &str,
        module: &[String],
        source_file: &Path,
        visible_oracle_macros: &BTreeSet<String>,
    ) {
        let identity = std::iter::once(self.crate_name.to_owned())
            .chain(module.iter().cloned())
            .chain(std::iter::once(name.to_owned()))
            .collect::<Vec<_>>()
            .join("::");
        let test_identity = module
            .iter()
            .cloned()
            .chain(std::iter::once(name.to_owned()))
            .collect::<Vec<_>>()
            .join("::");
        self.declarations
            .entry(name.to_owned())
            .or_default()
            .insert(identity);
        self.declaration_sources
            .entry(test_identity.clone())
            .or_default()
            .insert(source_file.to_owned());
        if !visible_oracle_macros.is_empty() {
            self.oracle_shadow_sources
                .entry(test_identity)
                .or_default()
                .insert(source_file.to_owned());
        }
    }

    fn reject_macro_use(attributes: &[Attribute], source_file: &Path) -> Result<(), String> {
        if attributes
            .iter()
            .any(|attribute| attribute.path().is_ident("macro_use"))
        {
            return Err(format!(
                "registered Cargo target uses #[macro_use] outside the reviewed lexical macro graph in {}",
                source_file.display()
            ));
        }
        Ok(())
    }

    fn collect_impl_item(
        &mut self,
        item: &syn::ItemImpl,
        module: &[String],
        source_file: &Path,
        visible_oracle_macros: &BTreeSet<String>,
    ) -> Result<(), String> {
        if !module_active_for_test(&item.attrs)? {
            return Ok(());
        }
        for member in &item.items {
            let attributes = match member {
                syn::ImplItem::Fn(method) => {
                    if module_active_for_test(&method.attrs)? && !visible_oracle_macros.is_empty() {
                        self.oracle_shadow_impl_methods
                            .push(OracleShadowImplMethod {
                                module: module.to_vec(),
                                self_ty: (*item.self_ty).clone(),
                                name: method.sig.ident.to_string(),
                                source: source_file.to_owned(),
                            });
                    }
                    continue;
                }
                syn::ImplItem::Const(item) => &item.attrs,
                syn::ImplItem::Type(item) => &item.attrs,
                syn::ImplItem::Macro(item) => {
                    module_active_for_test(&item.attrs)?;
                    return Err(format!(
                        "registered Cargo target uses an unexpanded impl macro outside the reviewed module graph in {}",
                        source_file.display()
                    ));
                }
                _ => continue,
            };
            module_active_for_test(attributes)?;
        }
        Ok(())
    }

    fn validate_trait_item(item: &syn::ItemTrait, source_file: &Path) -> Result<(), String> {
        if !module_active_for_test(&item.attrs)? {
            return Ok(());
        }
        for member in &item.items {
            let attributes = match member {
                syn::TraitItem::Fn(item) => &item.attrs,
                syn::TraitItem::Const(item) => &item.attrs,
                syn::TraitItem::Type(item) => &item.attrs,
                syn::TraitItem::Macro(item) => {
                    module_active_for_test(&item.attrs)?;
                    return Err(format!(
                        "registered Cargo target uses an unexpanded trait macro outside the reviewed module graph in {}",
                        source_file.display()
                    ));
                }
                _ => continue,
            };
            module_active_for_test(attributes)?;
        }
        Ok(())
    }
}
