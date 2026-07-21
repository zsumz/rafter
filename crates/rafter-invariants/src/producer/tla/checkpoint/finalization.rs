//! Checkpoint finalization, metadata publication, and deadline enforcement.

use std::{error::Error, fmt, path::Path, time::Instant};

use crate::evidence::ArtifactRef;

use super::{
    super::artifact,
    cache::{prune_to_latest, write_cache_valid_marker},
    inventory::{inventory, write_json_atomic},
    model::{Preparation, CONTRACT_FILE, INVENTORY_FILE},
    CONTRACT_KIND, INVENTORY_KIND,
};

impl Preparation {
    pub(in crate::producer::tla) fn abandon(self) -> Vec<ArtifactRef> {
        self.artifacts
    }

    pub(in crate::producer::tla) fn finish(
        mut self,
        output_dir: &Path,
        deadline: Instant,
    ) -> Result<Vec<ArtifactRef>, Box<dyn Error>> {
        ensure_deadline(deadline, "checkpoint finalization")?;
        if self.error.is_some() {
            return Ok(self.artifacts);
        }
        self.state_handle
            .as_ref()
            .ok_or("compatible checkpoint preparation omitted state handle")?
            .verify_path_binding()?;
        if let Some(recover_handle) = &self.recover_handle {
            recover_handle.verify_path_binding()?;
        }
        let contract_sha256 = self.contract.sha256()?;
        prune_to_latest(&self.state_dir, deadline)?;
        let inventory = inventory(&self.state_dir, &contract_sha256, deadline)?;
        let contract_path = self.root.join(CONTRACT_FILE);
        let inventory_path = self.root.join(INVENTORY_FILE);
        write_json_atomic(&contract_path, &self.contract)?;
        write_json_atomic(&inventory_path, &inventory)?;
        ensure_deadline(deadline, "checkpoint metadata publication")?;
        self.artifacts.push(artifact::capture(
            output_dir,
            &self.namespace,
            &contract_path,
            CONTRACT_KIND,
        )?);
        ensure_deadline(deadline, "checkpoint contract capture")?;
        self.artifacts.push(artifact::capture(
            output_dir,
            &self.namespace,
            &inventory_path,
            INVENTORY_KIND,
        )?);
        ensure_deadline(deadline, "checkpoint inventory capture")?;
        write_cache_valid_marker(&self.root, "checkpoint", &contract_sha256)?;
        ensure_deadline(deadline, "checkpoint validity publication")?;
        Ok(self.artifacts)
    }
}

#[derive(Debug)]
pub(super) struct CheckpointDeadlineError {
    operation: String,
}

impl fmt::Display for CheckpointDeadlineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "TLA checkpoint deadline expired during {}",
            self.operation
        )
    }
}

impl Error for CheckpointDeadlineError {}

pub(super) fn ensure_deadline(deadline: Instant, operation: &str) -> Result<(), Box<dyn Error>> {
    if Instant::now() >= deadline {
        return Err(Box::new(CheckpointDeadlineError {
            operation: operation.to_owned(),
        }));
    }
    Ok(())
}
