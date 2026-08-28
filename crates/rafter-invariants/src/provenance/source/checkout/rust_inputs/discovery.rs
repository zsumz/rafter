//! Discovery of the Rust sources an accepted include or module declares.
//!
//! Each accepted input becomes a tracked context with the module directory and
//! module path its children resolve against, so the sweep reaches every file
//! the compiler would.

use std::{
    fs,
    path::{Path, PathBuf},
};

use syn::Meta;

use super::{
    unraw_ident, validate_tracked_source_path, RustIncludeValidator, RustSourceContext,
    RustSourceKind,
};

impl RustIncludeValidator<'_> {
    pub(super) fn record_discovered_rust_source(
        &mut self,
        path: &Path,
        module_dir: PathBuf,
        module_path: Vec<String>,
        kind: RustSourceKind,
    ) -> Result<(), String> {
        let canonical = fs::canonicalize(path).map_err(|error| {
            format!(
                "canonicalize discovered Rust input {}: {error}",
                path.display()
            )
        })?;
        let relative = canonical.strip_prefix(self.root).map_err(|_| {
            format!(
                "discovered Rust input is outside source root: {}",
                canonical.display()
            )
        })?;
        self.discovered.insert(RustSourceContext {
            relative: relative.to_owned(),
            module_dir,
            module_path,
            kind,
        });
        Ok(())
    }

    pub(super) fn discover_default_module(&mut self, item: &syn::ItemMod) -> Result<(), String> {
        let name = unraw_ident(&item.ident);
        let candidates = [
            self.root.join(&self.module_dir).join(format!("{name}.rs")),
            self.root.join(&self.module_dir).join(&name).join("mod.rs"),
        ];
        let existing = candidates
            .into_iter()
            .filter(|candidate| candidate.is_file())
            .collect::<Vec<_>>();
        let path = match existing.as_slice() {
            [] => return Ok(()),
            [path] => path,
            _ => {
                return Err(format!(
                    "module {name} in {} resolves to more than one source file",
                    self.source_path.display()
                ));
            }
        };
        validate_tracked_source_path(self.root, path, self.tracked, "module input")
            .map_err(|error| error.to_string())?;
        let child_module_dir =
            if path.file_name().and_then(std::ffi::OsStr::to_str) == Some("mod.rs") {
                path.strip_prefix(self.root)
                    .ok()
                    .and_then(Path::parent)
                    .unwrap_or(Path::new(""))
                    .to_owned()
            } else {
                self.module_dir.join(&name)
            };
        let mut child_module_path = self.module_path.clone();
        child_module_path.push(name);
        self.record_discovered_rust_source(
            path,
            child_module_dir,
            child_module_path,
            RustSourceKind::Module,
        )
    }

    pub(super) fn discover_path_module(
        &mut self,
        item: &syn::ItemMod,
        path_meta: &Meta,
    ) -> Result<(), String> {
        let input = self.validate_path_meta(path_meta)?;
        let canonical = fs::canonicalize(&input).map_err(|error| {
            format!(
                "canonicalize discovered Rust input {}: {error}",
                input.display()
            )
        })?;
        let child_module_dir =
            if canonical.file_name().and_then(std::ffi::OsStr::to_str) == Some("mod.rs") {
                canonical
                    .strip_prefix(self.root)
                    .ok()
                    .and_then(Path::parent)
                    .unwrap_or(Path::new(""))
                    .to_owned()
            } else {
                self.module_dir.join(unraw_ident(&item.ident))
            };
        let mut child_module_path = self.module_path.clone();
        child_module_path.push(unraw_ident(&item.ident));
        self.record_discovered_rust_source(
            &input,
            child_module_dir,
            child_module_path,
            RustSourceKind::Module,
        )
    }
}
