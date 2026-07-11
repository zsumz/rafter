mod artifact;
mod process;
mod source;
mod test_exec;
mod tests;

use std::{error::Error, fs, path::PathBuf};

use crate::{Catalog, ProfileManifest, ResultBundle};

#[derive(Clone, Debug)]
/// Input paths and selected contract for one deterministic evidence producer.
pub struct ProducerOptions {
    pub profile: String,
    pub layer: String,
    pub registry: PathBuf,
    pub manifest: PathBuf,
    pub output_dir: PathBuf,
}

#[derive(Clone, Debug)]
/// Written producer receipt and whether every evidence result passed.
pub struct ProducerOutcome {
    pub path: PathBuf,
    pub all_passed: bool,
}

/// Executes one profile layer and writes its strict result bundle.
///
/// # Errors
///
/// Returns an error when the repository is dirty, the producer contract is
/// invalid, the selected layer is unsupported, or the receipt cannot be
/// written. Individual check failures are represented inside the receipt.
pub fn produce(options: &ProducerOptions) -> Result<ProducerOutcome, Box<dyn Error>> {
    artifact::validate_output_dir(&options.output_dir)?;
    let catalog = Catalog::load(&options.registry)?;
    let manifest = ProfileManifest::load(&options.manifest)?;
    manifest.validate(&catalog)?;
    let contract = manifest
        .profiles
        .get(&options.profile)
        .ok_or_else(|| format!("unknown profile {}", options.profile))?;
    if !contract.required_layers.contains(&options.layer) {
        return Err(format!(
            "layer {} is not required by profile {}",
            options.layer, options.profile
        )
        .into());
    }
    let source = source::capture()?;
    let bundle = match options.layer.as_str() {
        "tests" => tests::run(
            &catalog,
            contract,
            &options.profile,
            source,
            &options.output_dir,
        )?,
        layer => return Err(format!("producer for layer {layer} is not implemented").into()),
    };
    let all_passed = bundle
        .results
        .iter()
        .all(|result| result.status == crate::EvidenceStatus::Pass);
    let path = write_bundle(&bundle, &options.output_dir)?;
    Ok(ProducerOutcome { path, all_passed })
}

fn write_bundle(bundle: &ResultBundle, output_dir: &PathBuf) -> Result<PathBuf, Box<dyn Error>> {
    fs::create_dir_all(output_dir)?;
    let path = output_dir.join(format!("{}-{}.json", bundle.profile, bundle.runner));
    fs::write(
        &path,
        format!("{}\n", serde_json::to_string_pretty(bundle)?),
    )?;
    Ok(path)
}
