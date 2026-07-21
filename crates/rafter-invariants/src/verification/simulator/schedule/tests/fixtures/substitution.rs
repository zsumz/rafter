//! Adversarial rewrites of authenticated compiler provenance fixtures.

use std::{fs, path::Path};

use sha2::{Digest, Sha256};

use super::{
    io::framed_process_log,
    model::{ProvenanceSubstitution, SimulatorFixture},
};

impl SimulatorFixture {
    pub(in super::super) fn substitute_provenance(&self, substitution: ProvenanceSubstitution) {
        let mut bundle = self.serialized_bundle();
        let compile_path = bundle
            .execution
            .artifacts
            .iter()
            .find(|artifact| artifact.kind == "compile-log")
            .expect("compile artifact")
            .path
            .clone();
        let source =
            fs::read_to_string(self.root.join(&compile_path)).expect("read serialized compile log");
        let processes = crate::evidence::format::process::parse_combined_processes(&source)
            .expect("parse serialized compile log");
        let [process] = processes.as_slice() else {
            panic!("serialized compile log must contain exactly one process")
        };
        let mut invocation = process.invocation.clone();
        let stdout = if matches!(substitution, ProvenanceSubstitution::CompileRoot) {
            invocation.current_dir = self
                .producer_root
                .with_extension("substituted-root")
                .to_string_lossy()
                .into_owned();
            process.stdout.clone()
        } else {
            substitute_compiler_message(&process.stdout, &self.producer_root, substitution)
        };
        let rewritten = framed_process_log(
            "simulator compile",
            &invocation,
            process.timed_out,
            &stdout,
            &process.stderr,
        );
        fs::write(self.root.join(&compile_path), rewritten.as_bytes())
            .expect("rewrite substituted compile log");
        let artifact = bundle
            .execution
            .artifacts
            .iter_mut()
            .find(|artifact| artifact.path == compile_path)
            .expect("serialized compile artifact");
        artifact.sha256 = format!("{:x}", Sha256::digest(rewritten.as_bytes()));
        artifact.size_bytes = rewritten.len() as u64;
        fs::write(
            &self.bundle_path,
            format!(
                "{}\n",
                serde_json::to_string_pretty(&bundle)
                    .expect("serialize substituted simulator bundle")
            ),
        )
        .expect("write substituted simulator bundle");
    }
}

fn substitute_compiler_message(
    stdout: &str,
    producer_root: &Path,
    substitution: ProvenanceSubstitution,
) -> String {
    let mut replacements = 0_usize;
    let rewritten = stdout
        .lines()
        .map(|line| {
            let Ok(mut message) = serde_json::from_str::<serde_json::Value>(line) else {
                return line.to_owned();
            };
            if message["reason"] != "compiler-artifact"
                || message["target"]["name"] != "rafter-model-check-fast"
            {
                return line.to_owned();
            }
            replacements += 1;
            match substitution {
                ProvenanceSubstitution::Package => {
                    message["package_id"] = serde_json::json!(format!(
                        "path+file://{}#0.0.1",
                        producer_root.join("crates/rafter-alt").display()
                    ));
                }
                ProvenanceSubstitution::Source => {
                    message["target"]["src_path"] = serde_json::json!(producer_root
                        .join("crates/rafter-sim/src/bin/rafter-model-check-substituted.rs"));
                }
                ProvenanceSubstitution::TargetName => {
                    message["target"]["name"] = serde_json::json!("rafter-model-check-substituted");
                }
                ProvenanceSubstitution::TargetKind => {
                    message["target"]["kind"] = serde_json::json!(["bin", "test"]);
                }
                ProvenanceSubstitution::Executable => {
                    let executable = Path::new(
                        message["executable"]
                            .as_str()
                            .expect("compiler executable path"),
                    );
                    message["executable"] = serde_json::json!(executable
                        .parent()
                        .expect("compiler executable parent")
                        .join("rafter-model-check-substituted"));
                }
                ProvenanceSubstitution::CompileRoot => unreachable!("handled by caller"),
            }
            message.to_string()
        })
        .collect::<Vec<_>>()
        .join("\n");
    assert_eq!(replacements, 1, "substitute one simulator compiler message");
    format!("{rewritten}\n")
}
