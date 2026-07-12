use std::{error::Error, path::Path};

use crate::ArtifactRef;

use super::{artifact, source};

pub(super) fn capture_jar(
    output_dir: &Path,
    namespace: &Path,
) -> Result<ArtifactRef, Box<dyn Error>> {
    let launcher =
        source::tool_path("maelstrom").ok_or("Maelstrom launcher is not present on PATH")?;
    let jar = launcher
        .parent()
        .ok_or("Maelstrom launcher has no installation directory")?
        .join("lib/maelstrom.jar");
    artifact::capture_external(
        output_dir,
        &namespace.join("inputs"),
        &jar,
        "maelstrom-tool-jar",
    )
}
