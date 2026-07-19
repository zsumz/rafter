//! Content-addressed evidence transport fixtures and post-stage verification.

use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
    sync::atomic::{AtomicU64, Ordering},
};

static NEXT_TRANSPORT_FIXTURE_ID: AtomicU64 = AtomicU64::new(0);

const GOLDEN_RESULT_BUNDLE: &str =
    include_str!("../../../../rafter-invariants/src/evidence/fixtures/result-v14-minimal.json");
const ARTIFACT_BYTES: &[u8] = b"content-hashed fixture\n";
const ARTIFACT_SHA256: &str = "1e54fde20fa6d441854ec1f4282f714fdf83f55db754ff247151328743c3cfb8";

const VERIFY_STAGED_BUNDLE: &str = r#"
import hashlib
import json
import pathlib
import sys

result_path = pathlib.Path(sys.argv[1])
workspace = pathlib.Path(sys.argv[2]).resolve()
canonical_prefix = pathlib.PurePosixPath(sys.argv[3])
evidence_root = workspace.joinpath(*canonical_prefix.parts)

with result_path.open(encoding="utf-8") as handle:
    bundle = json.load(handle)

refs = []
def collect(value):
    if isinstance(value, dict):
        if {"kind", "path", "sha256", "size_bytes"}.issubset(value):
            refs.append(value)
        for child in value.values():
            collect(child)
    elif isinstance(value, list):
        for child in value:
            collect(child)

collect(bundle)
if len(refs) < 3:
    raise SystemExit(f"expected at least three ArtifactRef entries, found {len(refs)}")

seen = set()
for ref in refs:
    relative = pathlib.PurePosixPath(ref["path"])
    if relative.is_absolute() or ".." in relative.parts:
        raise SystemExit(f"unsafe ArtifactRef path: {relative}")
    if relative.parts[:len(canonical_prefix.parts)] != canonical_prefix.parts:
        raise SystemExit(f"ArtifactRef escaped staged evidence root: {relative}")
    if relative in seen:
        raise SystemExit(f"duplicate ArtifactRef path: {relative}")
    seen.add(relative)

    artifact = workspace.joinpath(*relative.parts)
    try:
        artifact.resolve().relative_to(workspace)
    except ValueError:
        raise SystemExit(f"ArtifactRef resolved outside workspace: {relative}")
    if not artifact.is_file():
        raise SystemExit(f"ArtifactRef does not resolve after staging: {relative}")
    payload = artifact.read_bytes()
    if len(payload) != ref["size_bytes"]:
        raise SystemExit(f"ArtifactRef size mismatch: {relative}")
    if hashlib.sha256(payload).hexdigest() != ref["sha256"]:
        raise SystemExit(f"ArtifactRef digest mismatch: {relative}")

for entry in evidence_root.rglob("*"):
    relative = entry.relative_to(evidence_root)
    if any(token in part.lower() for part in relative.parts for token in ("telemetry", "diagnostics")):
        raise SystemExit(f"diagnostics contaminated invariant evidence: {relative}")
"#;

pub(crate) struct EvidenceTransportFixture {
    root: PathBuf,
    pub(crate) workspace: PathBuf,
    runner_temp: PathBuf,
    evidence_dir: String,
    profile: String,
}

impl EvidenceTransportFixture {
    pub(crate) fn new(root: &Path, profile: &str, layers: &[&str]) -> Self {
        let id = NEXT_TRANSPORT_FIXTURE_ID.fetch_add(1, Ordering::Relaxed);
        let fixture_root = root
            .join("target/ci-contract")
            .join(format!("evidence-transport-{}-{id}", std::process::id()));
        if fixture_root.exists() {
            fs::remove_dir_all(&fixture_root).expect("remove stale evidence transport fixture");
        }
        let workspace = fixture_root.join("workspace");
        let runner_temp = fixture_root.join("runner-temp");
        let evidence_dir = format!("transport-{profile}");
        fs::create_dir_all(&workspace).expect("create evidence transport workspace");

        let fixture = Self {
            root: fixture_root,
            workspace,
            runner_temp,
            evidence_dir,
            profile: profile.to_owned(),
        };
        for layer in layers {
            fixture.write_layer(layer);
        }
        fixture
    }

    pub(crate) fn environment(&self) -> Vec<(&str, &str)> {
        vec![
            (
                "RUNNER_TEMP",
                self.runner_temp.to_str().expect("UTF-8 runner temp"),
            ),
            ("INVARIANT_EVIDENCE_DIR", self.evidence_dir.as_str()),
        ]
    }

    pub(crate) fn remove_artifact_directory(&self, layer: &str) {
        fs::remove_dir_all(self.transported_artifact_directory(layer))
            .expect("remove transported artifact directory");
    }

    pub(crate) fn remove_result(&self, layer: &str) {
        fs::remove_file(self.transported_result(layer)).expect("remove transported result");
    }

    pub(crate) fn remove_referenced_artifact(&self, layer: &str) {
        fs::remove_file(
            self.transported_artifact_directory(layer)
                .join("checks/check.log"),
        )
        .expect("remove referenced artifact");
    }

    pub(crate) fn contaminate_with_diagnostics(&self, layer: &str) {
        let diagnostics = self
            .transported_artifact_directory(layer)
            .join("telemetry/process.log");
        fs::create_dir_all(diagnostics.parent().expect("diagnostics parent"))
            .expect("create contaminating diagnostics directory");
        fs::write(diagnostics, b"diagnostic fixture\n").expect("write contaminating diagnostics");
    }

    pub(crate) fn occupy_canonical_target(&self, layer: &str) {
        let destination = self.canonical_artifact_directory(layer);
        fs::create_dir_all(&destination).expect("create stale canonical evidence target");
        fs::write(destination.join("stale.log"), "stale\n")
            .expect("write stale canonical evidence");
    }

    pub(crate) fn verify_staged_bundles(&self, layers: &[&str]) -> Result<(), String> {
        for layer in layers {
            let canonical_prefix = format!("artifacts/invariants/{}-{layer}", self.profile);
            let output = Command::new("python3")
                .args([
                    "-c",
                    VERIFY_STAGED_BUNDLE,
                    self.transported_result(layer)
                        .to_str()
                        .expect("UTF-8 result path"),
                    self.workspace.to_str().expect("UTF-8 fixture workspace"),
                    &canonical_prefix,
                ])
                .output()
                .expect("verify staged ArtifactRef entries");
            if !output.status.success() {
                return Err(format!(
                    "{} {layer} staged bundle failed verification:\n{}",
                    self.profile,
                    String::from_utf8_lossy(&output.stderr)
                ));
            }
        }
        Ok(())
    }

    fn write_layer(&self, layer: &str) {
        let transport = self.transported_artifact_directory(layer);
        for relative in [
            "producer.bin",
            "checks/check.log",
            "execution/execution.log",
        ] {
            let artifact = transport.join(relative);
            fs::create_dir_all(artifact.parent().expect("artifact parent"))
                .expect("create transported artifact directory");
            fs::write(artifact, ARTIFACT_BYTES).expect("write transported artifact");
        }

        let canonical = format!("artifacts/invariants/{}-{layer}", self.profile);
        let mut bundle = GOLDEN_RESULT_BUNDLE.to_owned();
        replace_once(
            &mut bundle,
            r#""runner": "tests""#,
            &format!(r#""runner": "{layer}""#),
        );
        replace_all_exact(
            &mut bundle,
            r#""profile": "pr""#,
            &format!(r#""profile": "{}""#, self.profile),
            2,
        );
        replace_once(
            &mut bundle,
            r#""required_layers": ["tests"]"#,
            &format!(r#""required_layers": ["{layer}"]"#),
        );
        replace_once(&mut bundle, r#""tests": {"#, &format!(r#""{layer}": {{"#));
        bundle = bundle.replace("tests/golden", &format!("{layer}/transport"));

        for (old_path, new_path) in [
            ("artifacts/producer", format!("{canonical}/producer.bin")),
            (
                "artifacts/check.log",
                format!("{canonical}/checks/check.log"),
            ),
            (
                "artifacts/execution.log",
                format!("{canonical}/execution/execution.log"),
            ),
        ] {
            bind_artifact_ref(&mut bundle, old_path, &new_path);
        }
        fs::write(self.transported_result(layer), bundle)
            .expect("write schema-valid transported result bundle");
    }

    fn transported_artifact_directory(&self, layer: &str) -> PathBuf {
        self.runner_temp
            .join(&self.evidence_dir)
            .join(layer)
            .join(format!("{}-{layer}", self.profile))
    }

    fn transported_result(&self, layer: &str) -> PathBuf {
        self.transported_artifact_directory(layer)
            .with_extension("json")
    }

    fn canonical_artifact_directory(&self, layer: &str) -> PathBuf {
        self.workspace
            .join("artifacts/invariants")
            .join(format!("{}-{layer}", self.profile))
    }
}

fn bind_artifact_ref(bundle: &mut String, old_path: &str, new_path: &str) {
    replace_once(
        bundle,
        &format!(r#""path": "{old_path}""#),
        &format!(r#""path": "{new_path}""#),
    );
    let path_offset = bundle
        .find(&format!(r#""path": "{new_path}""#))
        .expect("bound ArtifactRef path");
    replace_once_after(
        bundle,
        path_offset,
        r#""sha256": "0000000000000000000000000000000000000000000000000000000000000000""#,
        &format!(r#""sha256": "{ARTIFACT_SHA256}""#),
    );
    replace_once_after(
        bundle,
        path_offset,
        r#""size_bytes": 1"#,
        &format!(r#""size_bytes": {}"#, ARTIFACT_BYTES.len()),
    );
}

fn replace_once(source: &mut String, from: &str, to: &str) {
    replace_all_exact(source, from, to, 1);
}

fn replace_all_exact(source: &mut String, from: &str, to: &str, expected: usize) {
    assert_eq!(
        source.matches(from).count(),
        expected,
        "golden bundle replacement inventory changed for {from}"
    );
    *source = source.replace(from, to);
}

fn replace_once_after(source: &mut String, offset: usize, from: &str, to: &str) {
    let relative = source[offset..]
        .find(from)
        .unwrap_or_else(|| panic!("golden bundle omitted {from} after ArtifactRef path"));
    let start = offset + relative;
    source.replace_range(start..start + from.len(), to);
}

impl Drop for EvidenceTransportFixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}
