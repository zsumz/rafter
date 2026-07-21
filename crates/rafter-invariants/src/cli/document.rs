//! Registry-document rendering and canonical-output checking.

use std::{fs, path::Path};

use rafter_invariants::{render_registry_markdown, RegistryDocument};

pub(super) fn execute(
    registry_path: &Path,
    output_path: &Path,
    check: bool,
) -> Result<bool, Box<dyn std::error::Error>> {
    let registry = RegistryDocument::load(registry_path)?;
    let rendered = render_registry_markdown(&registry);
    if check {
        let current = fs::read_to_string(output_path).map_err(|error| {
            format!(
                "{} is missing or unreadable: {error}; run scripts/render-raft-invariants-doc",
                output_path.display()
            )
        })?;
        if current != rendered {
            return Err(format!(
                "{} is out of date; run scripts/render-raft-invariants-doc",
                output_path.display()
            )
            .into());
        }
        return Ok(true);
    }
    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(output_path, rendered)?;
    println!("wrote {}", output_path.display());
    Ok(true)
}

#[cfg(test)]
#[path = "document/tests.rs"]
mod tests;
