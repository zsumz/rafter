//! Deterministic execution-plan construction and source-bound input hashing.

use std::{
    collections::BTreeMap,
    env,
    error::Error,
    ffi::OsString,
    fs,
    path::{Component, Path, PathBuf},
};

use sha2::{Digest, Sha256};

#[cfg(test)]
use crate::ResultBundle;
use crate::{
    Catalog, ExecutionPlanReceipt, InvocationReceipt, PlanInput, ProfileManifest,
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

pub(crate) struct CapturedInvocation {
    pub receipt: InvocationReceipt,
    pub program_bytes: Vec<u8>,
}

pub(crate) fn current_source_ref() -> Result<String, Box<dyn Error>> {
    crate::provenance::source::head_commit_at(Path::new("."))
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
#[cfg(test)]
pub(crate) fn verify_bundle_plan(
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
pub(crate) fn capture_invocation() -> Result<CapturedInvocation, Box<dyn Error>> {
    let captured = capture_invocation_from(env::args_os().collect())?;
    crate::provenance::image::verify_capture(
        Path::new(&captured.receipt.program),
        &captured.program_bytes,
    )?;
    Ok(captured)
}

fn capture_invocation_from(argv: Vec<OsString>) -> Result<CapturedInvocation, Box<dyn Error>> {
    capture_invocation_from_program(argv, &env::current_exe()?)
}

fn capture_invocation_from_program(
    mut argv: Vec<OsString>,
    program: &Path,
) -> Result<CapturedInvocation, Box<dyn Error>> {
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
    let environment_sha256 = crate::provenance::invocation::digest_environment(&environment)?;
    let program = fs::canonicalize(program)?
        .into_os_string()
        .into_string()
        .map_err(|_| "invariant executable path is not UTF-8")?;
    let program_bytes = fs::read(&program)?;
    let program_sha256 = format!("{:x}", Sha256::digest(&program_bytes));
    Ok(CapturedInvocation {
        receipt: InvocationReceipt {
            program,
            program_sha256,
            arguments,
            current_dir,
            environment,
            environment_sha256,
            launchers: Vec::new(),
        },
        program_bytes,
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
    crate::producer::process::identity_command(
        "git",
        &["ls-files", "--error-unmatch", "--", &relative],
    )
    .map_err(|error| format!("execution-plan input is not tracked: {relative}: {error}"))?;
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

#[cfg(test)]
mod tests {
    use std::{
        ffi::OsString,
        sync::atomic::{AtomicU64, Ordering},
    };

    use sha2::{Digest, Sha256};

    use crate::{PlanInput, ResultBundle};

    use super::{
        capture_invocation_from, capture_invocation_from_program, confined_path,
        verify_bundle_plan, verify_plan_input,
    };

    #[test]
    fn invocation_records_actual_argv_without_manifest_substitution() {
        let captured = capture_invocation_from(vec![
            OsString::from("target/debug/rafter-invariants"),
            OsString::from("run"),
            OsString::from("--profile"),
            OsString::from("pr"),
        ])
        .expect("invocation captures");
        let receipt = captured.receipt;
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
    fn invocation_freezes_program_bytes_before_later_cargo_rebuilds() {
        static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);
        let directory = std::env::temp_dir().join(format!(
            "rafter-invariants-invocation-{}-{}",
            std::process::id(),
            NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&directory).expect("create temporary program directory");
        let program = directory.join("rafter-invariants");
        let initial = b"initial producer image";
        std::fs::write(&program, initial).expect("write initial producer image");

        let captured = capture_invocation_from_program(
            vec![
                OsString::from("rafter-invariants"),
                OsString::from("run-all"),
                OsString::from("--profile"),
                OsString::from("pr"),
            ],
            &program,
        )
        .expect("invocation captures immutable program bytes");
        std::fs::write(&program, b"later cargo rebuild").expect("replace producer path");

        assert_eq!(captured.program_bytes, initial);
        assert_eq!(
            captured.receipt.program_sha256,
            format!("{:x}", Sha256::digest(initial))
        );
        assert_ne!(
            captured.receipt.program_sha256,
            format!(
                "{:x}",
                Sha256::digest(std::fs::read(&program).expect("read replacement"))
            )
        );
        std::fs::remove_dir_all(directory).expect("remove temporary program directory");
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
