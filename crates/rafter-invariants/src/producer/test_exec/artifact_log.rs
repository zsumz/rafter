//! Durable artifact paths for incremental and completed libtest logs.

use std::{error::Error, path::Path};

use crate::evidence::ArtifactRef;

use super::super::artifact;

pub(super) fn write(
    output_dir: &Path,
    profile: &str,
    source_ref: &str,
    execution_id: &str,
    bytes: &[u8],
) -> Result<ArtifactRef, Box<dyn Error>> {
    let source_prefix = source_ref.get(..12).unwrap_or(source_ref);
    artifact::write(
        output_dir,
        Path::new(&format!(
            "{profile}-tests/{source_prefix}/checks/{execution_id}.log"
        )),
        "test-log",
        bytes,
    )
}

pub(super) fn persist(
    output_dir: &Path,
    profile: &str,
    source_ref: &str,
    execution_id: &str,
    bytes: &[u8],
) -> Result<(), Box<dyn Error>> {
    write(output_dir, profile, source_ref, execution_id, bytes).map(|_| ())
}
