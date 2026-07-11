use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    ffi::OsString,
    fs,
    path::{Path, PathBuf},
    time::Instant,
};

use serde_json::Value;

use crate::types::RESULT_SCHEMA_VERSION;
use crate::{
    catalog::{Catalog, ProfileContract},
    ArtifactRef, CheckReceipt, EvidenceDescriptor, EvidenceResult, EvidenceStatus,
    ExecutionReceipt, ResultBundle, SourceReceipt, TestIdentity,
};

use super::{artifact, process, source};

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct Target {
    package: String,
    kind: String,
    name: String,
}

type TestEvidence = BTreeMap<TestIdentity, Vec<EvidenceDescriptor>>;

struct CheckResults {
    checks: Vec<CheckReceipt>,
    results: Vec<EvidenceResult>,
    peak_rss_kib: u64,
}

pub(super) struct CompiledTarget {
    pub executable: Option<PathBuf>,
    pub binary_artifact: Option<ArtifactRef>,
    pub artifact: ArtifactRef,
    pub error: Option<String>,
    pub peak_rss_kib: u64,
}

pub(super) fn run(
    catalog: &Catalog,
    contract: &ProfileContract,
    profile: &str,
    source: SourceReceipt,
    output_dir: &Path,
) -> Result<ResultBundle, Box<dyn Error>> {
    let started = Instant::now();
    let runner = contract
        .runners
        .get("tests")
        .ok_or("tests runner missing")?;
    let target_dir = prepare_target_dir(profile, &source.commit)?;
    let mut build_environment = process::base_environment();
    build_environment.insert(
        "CARGO_TARGET_DIR".to_owned(),
        target_dir.to_string_lossy().into_owned(),
    );
    let identities = test_evidence(catalog, contract);
    let targets = identities.keys().map(Target::from).collect::<BTreeSet<_>>();
    let mut compiled = BTreeMap::new();
    let mut execution_artifacts = Vec::new();
    let mut peak_rss_kib = 0;
    for target in targets {
        let outcome = compile(
            &target,
            profile,
            &source.commit,
            &build_environment,
            output_dir,
        )?;
        peak_rss_kib = peak_rss_kib.max(outcome.peak_rss_kib);
        execution_artifacts.push(outcome.artifact.clone());
        compiled.insert(target, outcome);
    }

    let check_results = run_checks(identities, &compiled, profile, &source.commit, output_dir)?;
    peak_rss_kib = peak_rss_kib.max(check_results.peak_rss_kib);
    let checks = check_results.checks;
    let results = check_results.results;
    source::verify(&source)?;
    let summary = format!(
        "profile={profile}\nproducer={}\ntargets={}\nchecks={}\nresults={}\n",
        runner.producer,
        compiled.len(),
        checks.len(),
        results.len()
    );
    execution_artifacts.push(artifact::write(
        output_dir,
        Path::new(&format!(
            "{profile}-tests/{}/summary.log",
            source.commit.get(..12).unwrap_or(&source.commit)
        )),
        "summary",
        summary.as_bytes(),
    )?);
    Ok(ResultBundle {
        schema_version: RESULT_SCHEMA_VERSION,
        runner: "tests".to_owned(),
        profile: profile.to_owned(),
        source_ref: source.commit.clone(),
        execution: ExecutionReceipt {
            producer: runner.producer.clone(),
            command: runner.command.clone(),
            configuration: runner.configuration.clone(),
            source,
            checks,
            duration_ms: process::duration_ms(started.elapsed()),
            peak_rss_kib,
            artifacts: execution_artifacts,
        },
        results,
    })
}

fn test_evidence(catalog: &Catalog, contract: &ProfileContract) -> TestEvidence {
    let required = catalog.required_evidence(contract);
    let mut identities = BTreeMap::<TestIdentity, Vec<_>>::new();
    for descriptor in required.values().flatten() {
        if let Some(identity) = &descriptor.test {
            identities
                .entry(identity.clone())
                .or_default()
                .push(descriptor.clone());
        }
    }
    identities
}

fn run_checks(
    identities: TestEvidence,
    compiled: &BTreeMap<Target, CompiledTarget>,
    profile: &str,
    source_ref: &str,
    output_dir: &Path,
) -> Result<CheckResults, Box<dyn Error>> {
    let mut checks = Vec::with_capacity(identities.len());
    let mut results = Vec::new();
    let mut peak_rss_kib = 0;
    for (identity, evidence) in identities {
        let evidence_ids = evidence
            .iter()
            .map(EvidenceDescriptor::evidence_id)
            .collect::<Vec<_>>();
        let check_id = identity.check_id();
        let execution_id = artifact::stable_id("test", &check_id);
        let target = Target::from(&identity);
        let compiled_target = compiled
            .get(&target)
            .ok_or("compiled target inventory changed during execution")?;
        let mut outcome = super::test_exec::evaluate(
            &identity,
            compiled_target,
            profile,
            source_ref,
            &execution_id,
            output_dir,
        )?;
        if let Some(binary) = &compiled_target.binary_artifact {
            outcome.artifacts.push(binary.clone());
        }
        peak_rss_kib = peak_rss_kib.max(outcome.peak_rss_kib);
        results.extend(evidence.into_iter().map(|descriptor| EvidenceResult {
            invariant_id: descriptor.invariant_id.clone(),
            evidence_id: descriptor.evidence_id(),
            execution_id: execution_id.clone(),
            status: outcome.status,
            classification: outcome.classification,
            message: outcome.message.clone(),
            artifacts: if outcome.status == EvidenceStatus::Pass {
                Vec::new()
            } else {
                outcome.artifacts.clone()
            },
        }));
        checks.push(CheckReceipt {
            execution_id,
            check_id,
            evidence_ids,
            completion: outcome.completion,
            observations: outcome.observations,
            duration_ms: outcome.duration_ms,
            peak_rss_kib: outcome.peak_rss_kib,
            artifacts: outcome.artifacts,
        });
    }
    Ok(CheckResults {
        checks,
        results,
        peak_rss_kib,
    })
}

fn compile(
    target: &Target,
    profile: &str,
    source_ref: &str,
    environment: &BTreeMap<String, String>,
    output_dir: &Path,
) -> Result<CompiledTarget, Box<dyn Error>> {
    let mut arguments = vec![
        "test".into(),
        "--locked".into(),
        "--no-default-features".into(),
        "-p".into(),
        target.package.clone().into(),
    ];
    arguments.extend(target.selector()?);
    arguments.extend([
        "--no-run".into(),
        "--message-format=json-render-diagnostics".into(),
    ]);
    let output = process::timed("cargo", &arguments, environment, Path::new("."))?;
    let artifact_id = artifact::stable_id(
        "compile",
        &format!("{profile}\0{source_ref}\0{}", target.key()),
    );
    let log = artifact::write(
        output_dir,
        Path::new(&format!("{profile}-tests/compile/{artifact_id}.log")),
        "compile-log",
        &process::combined_log(&target.key(), &output),
    )?;
    let (executable, error) = if output.status.success() {
        match executable_from_messages(&output.stdout, target) {
            Ok(executable) => (Some(executable), None),
            Err(error) => (None, Some(error)),
        }
    } else {
        (
            None,
            Some(format!("cargo test --no-run failed for {}", target.key())),
        )
    };
    let binary_artifact = executable
        .as_deref()
        .map(|path| artifact::existing(path, "test-binary"))
        .transpose()?;
    Ok(CompiledTarget {
        executable,
        binary_artifact,
        artifact: log,
        error,
        peak_rss_kib: output.peak_rss_kib,
    })
}

fn executable_from_messages(bytes: &[u8], target: &Target) -> Result<PathBuf, String> {
    let mut executables = Vec::new();
    for line in String::from_utf8_lossy(bytes).lines() {
        let Ok(message) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        if message["reason"] == "compiler-artifact"
            && message["target"]["name"] == target.name
            && message["target"]["kind"]
                .as_array()
                .is_some_and(|kinds| kinds.iter().any(|kind| kind == &target.kind))
        {
            if message["fresh"] == true {
                return Err(format!(
                    "fresh cached executable is forbidden for {}",
                    target.key()
                ));
            }
            if let Some(executable) = message["executable"].as_str() {
                executables.push(PathBuf::from(executable));
            }
        }
    }
    if executables.len() == 1 {
        Ok(executables.remove(0))
    } else {
        Err(format!(
            "expected one executable for {}, found {}",
            target.key(),
            executables.len()
        ))
    }
}

fn prepare_target_dir(profile: &str, source_ref: &str) -> Result<PathBuf, Box<dyn Error>> {
    let source_prefix = source_ref.get(..12).unwrap_or(source_ref);
    let directory = Path::new("target/rafter-invariants/build")
        .join(source_prefix)
        .join(format!("{profile}-tests"));
    if directory.exists() {
        fs::remove_dir_all(&directory)?;
    }
    fs::create_dir_all(&directory)?;
    Ok(directory)
}

impl From<&TestIdentity> for Target {
    fn from(identity: &TestIdentity) -> Self {
        Self {
            package: identity.package.clone(),
            kind: identity.target_kind.clone(),
            name: identity.target.clone(),
        }
    }
}

impl Target {
    fn key(&self) -> String {
        format!("{}/{}/{}", self.package, self.kind, self.name)
    }

    fn selector(&self) -> Result<Vec<OsString>, Box<dyn Error>> {
        match self.kind.as_str() {
            "lib" => Ok(vec!["--lib".into()]),
            "test" => Ok(vec!["--test".into(), self.name.clone().into()]),
            "bin" => Ok(vec!["--bin".into(), self.name.clone().into()]),
            kind => Err(format!("unsupported Cargo target kind {kind}").into()),
        }
    }
}
