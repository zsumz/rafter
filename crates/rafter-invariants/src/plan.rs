use std::{
    collections::BTreeMap,
    env,
    error::Error,
    ffi::OsString,
    fs,
    path::{Component, Path, PathBuf},
    process::Command,
};

use sha2::{Digest, Sha256};

use crate::{
    Catalog, ExecutionPlanReceipt, InvocationReceipt, PlanInput, ProfileManifest, ResultBundle,
    PLAN_SCHEMA_VERSION,
};

#[derive(Clone, Debug)]
/// Paths and selected profile used to construct one immutable execution plan.
pub struct PlanOptions {
    pub profile: String,
    pub registry: PathBuf,
    pub manifest: PathBuf,
}

#[derive(Clone, Debug)]
/// Parsed contracts plus their exact source-byte receipt.
pub struct ExecutionPlan {
    pub catalog: Catalog,
    pub manifest: ProfileManifest,
    pub receipt: ExecutionPlanReceipt,
}

impl ExecutionPlan {
    /// Loads, validates, and hashes the registry and profile manifest once.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid contracts, untracked or escaping paths,
    /// or an unknown profile.
    pub fn load(options: &PlanOptions) -> Result<Self, Box<dyn Error>> {
        let catalog = Catalog::load(&options.registry)?;
        let manifest = ProfileManifest::load(&options.manifest)?;
        manifest.validate(&catalog)?;
        let contract = manifest
            .profiles
            .get(&options.profile)
            .ok_or_else(|| format!("unknown profile {}", options.profile))?
            .clone();
        let receipt = ExecutionPlanReceipt {
            schema_version: PLAN_SCHEMA_VERSION,
            profile: options.profile.clone(),
            registry: plan_input(&options.registry)?,
            manifest: plan_input(&options.manifest)?,
            result_schema: plan_input(Path::new("verification/invariant-result-schema.json"))?,
            verdict_schema: plan_input(Path::new("verification/invariant-verdict-schema.json"))?,
            contract,
        };
        Ok(Self {
            catalog,
            manifest,
            receipt,
        })
    }

    #[must_use]
    /// Returns the selected validated profile contract.
    pub fn contract(&self) -> &crate::ProfileContract {
        &self.receipt.contract
    }
}

/// Requires a bundle to carry the exact active execution plan.
///
/// # Errors
///
/// Returns an error when any plan path, digest, size, profile, or selected
/// contract differs from the plan loaded by the current invocation.
pub fn verify_bundle_plan(
    bundle: &ResultBundle,
    expected: &ExecutionPlanReceipt,
) -> Result<(), Box<dyn Error>> {
    if bundle.execution.plan != *expected {
        return Err(format!(
            "runner {} execution plan does not match the active registry and manifest",
            bundle.runner
        )
        .into());
    }
    Ok(())
}

/// Captures the actual invariant binary argv, working directory, and safe
/// environment digest for a receipt.
///
/// # Errors
///
/// Returns an error when argv or the working directory is not valid UTF-8.
pub fn capture_invocation() -> Result<InvocationReceipt, Box<dyn Error>> {
    capture_invocation_from(env::args_os().collect())
}

fn capture_invocation_from(mut argv: Vec<OsString>) -> Result<InvocationReceipt, Box<dyn Error>> {
    if argv.is_empty() {
        return Err("invariant invocation omitted argv[0]".into());
    }
    let _argv_zero = argv
        .remove(0)
        .into_string()
        .map_err(|_| "invariant program path is not UTF-8")?;
    let arguments = argv
        .into_iter()
        .map(|argument| {
            argument
                .into_string()
                .map_err(|_| "invariant argument is not UTF-8")
        })
        .collect::<Result<Vec<_>, _>>()?;
    let current_dir = fs::canonicalize(".")?
        .into_os_string()
        .into_string()
        .map_err(|_| "invariant working directory is not UTF-8")?;
    let environment = safe_environment();
    let environment_sha256 = digest_environment(&environment);
    let program = fs::canonicalize(env::current_exe()?)?
        .into_os_string()
        .into_string()
        .map_err(|_| "invariant executable path is not UTF-8")?;
    let program_sha256 = format!("{:x}", Sha256::digest(fs::read(&program)?));
    Ok(InvocationReceipt {
        program,
        program_sha256,
        arguments,
        current_dir,
        environment,
        environment_sha256,
    })
}

pub(crate) fn verify_plan_input(input: &PlanInput, root: &Path) -> Result<(), Box<dyn Error>> {
    let root = fs::canonicalize(root)?;
    let path = confined_path(&PathBuf::from(&input.path), &root)?;
    let bytes = fs::read(&path)?;
    if input.size_bytes != bytes.len() as u64
        || input.sha256 != format!("{:x}", Sha256::digest(&bytes))
    {
        return Err(format!("execution-plan input changed: {}", input.path).into());
    }
    Ok(())
}

fn plan_input(path: &Path) -> Result<PlanInput, Box<dyn Error>> {
    let root = fs::canonicalize(".")?;
    let canonical = confined_path(path, &root)?;
    let relative = canonical
        .strip_prefix(&root)?
        .to_string_lossy()
        .into_owned();
    let tracked = Command::new("git")
        .args(["ls-files", "--error-unmatch", "--", &relative])
        .output()?;
    if !tracked.status.success() {
        return Err(format!("execution-plan input is not tracked: {relative}").into());
    }
    let bytes = fs::read(canonical)?;
    Ok(PlanInput {
        path: relative,
        sha256: format!("{:x}", Sha256::digest(&bytes)),
        size_bytes: bytes.len() as u64,
    })
}

fn confined_path(path: &Path, root: &Path) -> Result<PathBuf, Box<dyn Error>> {
    if path.is_absolute()
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(format!(
            "execution-plan path must be repository-relative: {}",
            path.display()
        )
        .into());
    }
    let canonical = fs::canonicalize(root.join(path))?;
    canonical
        .strip_prefix(root)
        .map_err(|_| format!("execution-plan path escapes repository: {}", path.display()))?;
    Ok(canonical)
}

fn safe_environment() -> BTreeMap<String, String> {
    const ALLOWED: &[&str] = &[
        "CARGO_HOME",
        "DEVELOPER_DIR",
        "HOME",
        "PATH",
        "RUSTUP_HOME",
        "SDKROOT",
        "SYSTEMROOT",
    ];
    ALLOWED
        .iter()
        .filter_map(|name| env::var(name).ok().map(|value| ((*name).to_owned(), value)))
        .collect()
}

fn digest_environment(environment: &BTreeMap<String, String>) -> String {
    let encoded = environment
        .iter()
        .map(|(name, value)| format!("{name}={value}"))
        .collect::<Vec<_>>()
        .join("\0");
    format!("{:x}", Sha256::digest(encoded))
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;

    use sha2::{Digest, Sha256};

    use crate::{PlanInput, ResultBundle};

    use super::{capture_invocation_from, confined_path, verify_bundle_plan, verify_plan_input};

    #[test]
    fn invocation_records_actual_argv_without_manifest_substitution() {
        let receipt = capture_invocation_from(vec![
            OsString::from("target/debug/rafter-invariants"),
            OsString::from("run"),
            OsString::from("--profile"),
            OsString::from("pr"),
        ])
        .expect("invocation captures");
        assert_eq!(
            receipt.program,
            std::fs::canonicalize(std::env::current_exe().expect("current executable"))
                .expect("current executable canonicalizes")
                .to_string_lossy()
        );
        assert_eq!(receipt.arguments, ["run", "--profile", "pr"]);
        assert_eq!(receipt.environment_sha256.len(), 64);
    }

    #[test]
    fn plan_paths_reject_absolute_and_parent_traversal() {
        let root = std::fs::canonicalize(".").expect("workspace root");
        assert!(confined_path(std::path::Path::new("/tmp/input"), &root).is_err());
        assert!(confined_path(std::path::Path::new("../input"), &root).is_err());
    }

    #[test]
    fn plan_input_digest_detects_exact_byte_drift() {
        let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
        let bytes =
            std::fs::read(root.join("verification/raft-invariants.yaml")).expect("registry reads");
        let mut changed = PlanInput {
            path: "verification/raft-invariants.yaml".to_owned(),
            sha256: format!("{:x}", Sha256::digest(&bytes)),
            size_bytes: bytes.len() as u64,
        };
        verify_plan_input(&changed, &root).expect("unchanged registry verifies");
        changed.sha256 = "0".repeat(64);
        assert!(verify_plan_input(&changed, &root).is_err());
    }

    #[test]
    fn active_plan_binding_rejects_alternate_input_paths() {
        let (_, manifest) = crate::tests::loaded();
        let expected = crate::tests::plan_receipt(&manifest, "pr");
        let mut bundle: ResultBundle =
            crate::tests::passing_bundles(&crate::tests::loaded().0, &manifest).remove(0);
        bundle.execution.plan.registry.path = "verification/alternate.yaml".to_owned();
        assert!(verify_bundle_plan(&bundle, &expected).is_err());
    }
}
