//! Alias resolution and literal-path acceptance for the include validator.
//!
//! Resolves the macro name or qualified path an invocation actually reaches,
//! and accepts only literal `include!` and `#[path]` inputs that name a
//! tracked source file.

use std::path::PathBuf;

use syn::{parse::Parser, punctuated::Punctuated, Attribute, Expr, Macro, Meta, Token};

use super::{
    alias_path_key, path_is_ident, resolve_scoped_qualified_alias, unraw_ident,
    validate_tracked_source_path, walk_effective_metas, CfgValue, RustIncludeValidator,
    RustSourceKind, ScopedAlias,
};

impl RustIncludeValidator<'_> {
    pub(super) fn included_alias(&self, name: &str) -> ScopedAlias {
        match self
            .included_aliases
            .get(&alias_path_key(&self.module_path, name))
        {
            Some(Some(canonical)) => ScopedAlias::Include(canonical.clone()),
            Some(None) => ScopedAlias::Shadowed,
            None => ScopedAlias::Unbound,
        }
    }

    pub(super) fn qualified_alias(&self, invocation: &Macro) -> Option<String> {
        if invocation.path.get_ident().is_some() {
            return None;
        }
        let mut path = Vec::new();
        let mut segments = invocation
            .path
            .segments
            .iter()
            .map(|segment| unraw_ident(&segment.ident))
            .peekable();
        let original_segments = invocation
            .path
            .segments
            .iter()
            .map(|segment| unraw_ident(&segment.ident))
            .collect::<Vec<_>>();
        match resolve_scoped_qualified_alias(&self.alias_scopes, &original_segments.join("::")) {
            ScopedAlias::Include(canonical) => return Some(canonical),
            ScopedAlias::Shadowed => return None,
            ScopedAlias::Unbound => {}
        }
        match segments.peek().map(String::as_str) {
            Some("crate") => {
                segments.next();
            }
            Some("self") => {
                path.extend(self.module_path.iter().cloned());
                segments.next();
            }
            Some("super") => {
                path.extend(self.module_path.iter().cloned());
                while matches!(segments.peek().map(String::as_str), Some("super")) {
                    segments.next();
                    path.pop();
                }
            }
            Some(_) => path.extend(self.module_path.iter().cloned()),
            None => return None,
        }
        path.extend(segments);
        self.qualified_aliases.get(&path.join("::")).cloned()
    }

    pub(super) fn validate_include(
        &mut self,
        invocation: &Macro,
        name: &str,
    ) -> Result<(), String> {
        let arguments = Punctuated::<Expr, Token![,]>::parse_terminated
            .parse2(invocation.tokens.clone())
            .map_err(|error| format!("parse {name}! input: {error}"))?
            .into_iter()
            .collect::<Vec<_>>();
        let [Expr::Lit(expression)] = arguments.as_slice() else {
            return Err(format!(
                "{name}! in {} must use one literal tracked path",
                self.source_path.display()
            ));
        };
        let syn::Lit::Str(path) = &expression.lit else {
            return Err(format!(
                "{name}! in {} must use one string literal tracked path",
                self.source_path.display()
            ));
        };
        let parent = self.source_path.parent().ok_or_else(|| {
            format!(
                "tracked source has no parent: {}",
                self.source_path.display()
            )
        })?;
        let input = parent.join(path.value());
        validate_tracked_source_path(self.root, &input, self.tracked, &format!("{name}! input"))
            .map_err(|error| error.to_string())?;
        if name == "include" {
            self.record_discovered_rust_source(
                &input,
                self.module_dir.clone(),
                self.module_path.clone(),
                RustSourceKind::Include,
            )?;
        }
        Ok(())
    }

    pub(super) fn validate_path_meta(&self, meta: &Meta) -> Result<PathBuf, String> {
        let Meta::NameValue(value) = meta else {
            return Err(format!(
                "#[path] in {} must use one literal tracked path",
                self.source_path.display()
            ));
        };
        let Expr::Lit(expression) = &value.value else {
            return Err(format!(
                "#[path] in {} must use one literal tracked path",
                self.source_path.display()
            ));
        };
        let syn::Lit::Str(path) = &expression.lit else {
            return Err(format!(
                "#[path] in {} must use one string literal tracked path",
                self.source_path.display()
            ));
        };
        let parent = self.source_path.parent().ok_or_else(|| {
            format!(
                "tracked source has no parent: {}",
                self.source_path.display()
            )
        })?;
        let input = parent.join(path.value());
        validate_tracked_source_path(self.root, &input, self.tracked, "#[path] module input")
            .map_err(|error| error.to_string())?;
        Ok(input)
    }

    pub(super) fn effective_path_metas(
        &self,
        attributes: &[Attribute],
    ) -> Result<Vec<Meta>, String> {
        let mut paths = Vec::new();
        for attribute in attributes {
            walk_effective_metas(&attribute.meta, CfgValue::True, &mut |meta, guard| {
                if path_is_ident(meta.path(), "path") {
                    match guard {
                        CfgValue::True => paths.push(meta.clone()),
                        CfgValue::Unknown => {
                            return Err(format!(
                                "target-conditional #[path] in {} is outside the source binding contract",
                                self.source_path.display()
                            ));
                        }
                        CfgValue::False => {}
                    }
                }
                Ok(())
            })?;
        }
        Ok(paths)
    }
}
