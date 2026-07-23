//! Canonical registry-document rendering and readback adaptation.

use std::{error::Error, fs};

use crate::contract::registry::{render_registry_markdown, RegistryDocument};

use super::model::{CommandOutput, RenderDocumentOptions};

/// Render or verify the canonical registry document.
///
/// # Errors
///
/// Returns an error when the registry or output is invalid, unreadable, stale,
/// or cannot be published.
pub fn execute(options: &RenderDocumentOptions) -> Result<CommandOutput, Box<dyn Error>> {
    let registry = RegistryDocument::load(&options.registry)?;
    let rendered = render_registry_markdown(&registry);
    if options.check {
        let current = fs::read_to_string(&options.output).map_err(|error| {
            format!(
                "{} is missing or unreadable: {error}; run scripts/render-raft-invariants-doc",
                options.output.display()
            )
        })?;
        if current != rendered {
            return Err(format!(
                "{} is out of date; run scripts/render-raft-invariants-doc",
                options.output.display()
            )
            .into());
        }
        return Ok(CommandOutput::new(true, Vec::new()));
    }
    if let Some(parent) = options.output.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&options.output, rendered)?;
    Ok(CommandOutput::passed(format!(
        "wrote {}",
        options.output.display()
    )))
}

#[cfg(test)]
#[path = "document_tests.rs"]
mod tests;
