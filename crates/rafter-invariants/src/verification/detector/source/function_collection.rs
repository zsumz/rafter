//! Function discovery across the authenticated detector target graph.

use std::collections::BTreeSet;

use syn::{
    visit::{self, Visit},
    Block, File, ForeignItem, ImplItem, ItemConst, ItemFn, ItemImpl, ItemMod, ItemStatic,
    Signature,
};

use super::{
    function_body::analyze_function,
    function_index::{FunctionId, FunctionIndex, LocalCallResolver},
    imports::{collect_item_imports, ImportedPaths},
    model::{FunctionFacts, SourceDefect},
};

pub(super) fn collect_functions(
    file: &File,
    imports: &ImportedPaths,
    resolver: &LocalCallResolver,
    module: &[String],
    active_only: bool,
) -> FunctionIndex {
    let mut collector = FunctionCollector {
        imports: imports.clone(),
        resolver,
        active_only,
        module: module.to_vec(),
        functions: FunctionIndex::default(),
    };
    collector.visit_file(file);
    collector.functions
}

struct FunctionCollector<'a> {
    imports: ImportedPaths,
    resolver: &'a LocalCallResolver,
    active_only: bool,
    module: Vec<String>,
    functions: FunctionIndex,
}

impl FunctionCollector<'_> {
    fn collect_function(
        &mut self,
        attributes: &[syn::Attribute],
        signature: &Signature,
        block: &Block,
        id_module: Vec<String>,
        self_type: Option<&[String]>,
    ) {
        let facts = analyze_function(
            &self.imports,
            self.resolver,
            &self.module,
            self_type,
            attributes,
            signature,
            block,
        );
        self.functions
            .functions
            .entry(FunctionId {
                module: id_module,
                name: signature.ident.to_string(),
            })
            .or_default()
            .push(facts);
    }
}

impl<'ast> Visit<'ast> for FunctionCollector<'_> {
    fn visit_item_macro(&mut self, item: &'ast syn::ItemMacro) {
        self.imports.add_macro_declaration(item);
    }

    fn visit_item_fn(&mut self, function: &'ast ItemFn) {
        if self.active_only
            && !crate::verification::target::module_active_for_test(&function.attrs)
                .unwrap_or(false)
        {
            return;
        }
        self.collect_function(
            &function.attrs,
            &function.sig,
            &function.block,
            self.module.clone(),
            None,
        );
        visit::visit_item_fn(self, function);
    }

    fn visit_item_impl(&mut self, item: &'ast ItemImpl) {
        if self.active_only
            && !crate::verification::target::module_active_for_test(&item.attrs).unwrap_or(false)
        {
            return;
        }
        let Some(type_module) = self
            .resolver
            .declared_type_module(&item.self_ty, &self.module)
        else {
            return;
        };
        for item in &item.items {
            match item {
                ImplItem::Const(item) => {
                    if self.active_only
                        && !crate::verification::target::module_active_for_test(&item.attrs)
                            .unwrap_or(false)
                    {
                        continue;
                    }
                    self.functions.values.insert(FunctionId {
                        module: type_module.clone(),
                        name: item.ident.to_string(),
                    });
                }
                ImplItem::Fn(method) => {
                    if self.active_only
                        && !crate::verification::target::module_active_for_test(&method.attrs)
                            .unwrap_or(false)
                    {
                        continue;
                    }
                    self.collect_function(
                        &method.attrs,
                        &method.sig,
                        &method.block,
                        type_module.clone(),
                        Some(&type_module),
                    );
                }
                _ => {}
            }
        }
    }

    fn visit_item_mod(&mut self, item: &'ast ItemMod) {
        if self.active_only
            && !crate::verification::target::module_active_for_test(&item.attrs).unwrap_or(false)
        {
            return;
        }
        let Some((_, items)) = &item.content else {
            return;
        };
        let mut nested_imports = collect_item_imports(items);
        nested_imports.inherit_parent_macros(&self.imports);
        if nested_imports.inherits_parent_glob() {
            nested_imports.inherit(&self.imports);
        }
        let previous_imports = std::mem::replace(&mut self.imports, nested_imports);
        self.module.push(item.ident.to_string());
        for item in items {
            self.visit_item(item);
        }
        self.module.pop();
        self.imports = previous_imports;
    }

    fn visit_item_const(&mut self, item: &'ast ItemConst) {
        self.functions.values.insert(FunctionId {
            module: self.module.clone(),
            name: item.ident.to_string(),
        });
    }

    fn visit_item_static(&mut self, item: &'ast ItemStatic) {
        self.functions.values.insert(FunctionId {
            module: self.module.clone(),
            name: item.ident.to_string(),
        });
    }

    fn visit_item_foreign_mod(&mut self, item: &'ast syn::ItemForeignMod) {
        for foreign in &item.items {
            if let ForeignItem::Fn(function) = foreign {
                self.functions
                    .functions
                    .entry(FunctionId {
                        module: self.module.clone(),
                        name: function.sig.ident.to_string(),
                    })
                    .or_default()
                    .push(FunctionFacts {
                        defects: BTreeSet::from([SourceDefect::UnsafeCapability]),
                        ..FunctionFacts::default()
                    });
            }
        }
    }
}
