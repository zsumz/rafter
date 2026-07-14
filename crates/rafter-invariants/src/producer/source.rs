use std::{env, error::Error, fs, path::PathBuf};

use sha2::{Digest, Sha256};

use crate::{SourceReceipt, ToolReceipt};

use super::process;

#[derive(Clone, Copy)]
struct LayerSourceContract {
    build_profile: &'static str,
    features: &'static [&'static str],
    tools: &'static [&'static str],
}

pub(super) fn capture_for_layer(layer: &str) -> Result<SourceReceipt, Box<dyn Error>> {
    capture(layer_contract(layer)?)
}

pub(crate) fn head_commit() -> Result<String, Box<dyn Error>> {
    git(&["rev-parse", "HEAD"])
}

fn capture(contract: LayerSourceContract) -> Result<SourceReceipt, Box<dyn Error>> {
    let status = command_output(
        "git",
        &["status", "--porcelain=v1", "--untracked-files=all"],
        true,
    )?;
    if !status.trim().is_empty() {
        return Err("evidence producers require a clean tracked and untracked worktree".into());
    }
    let commit = git(&["rev-parse", "HEAD"])?;
    let tree = git(&["rev-parse", "HEAD^{tree}"])?;
    let cargo = command_output("cargo", &["-vV"], false)?;
    let rustc = command_output("rustc", &["-vV"], false)?;
    let target = rustc
        .lines()
        .find_map(|line| line.strip_prefix("host: "))
        .ok_or("rustc -vV omitted host target")?
        .to_owned();
    let cargo_lock = fs::read("Cargo.lock")?;
    let environment = process::base_environment();
    let tools = contract
        .tools
        .iter()
        .map(|name| {
            let version = tool_version(name)?;
            Ok((
                (*name).to_owned(),
                ToolReceipt {
                    version,
                    sha256: executable_sha256(name)?,
                },
            ))
        })
        .collect::<Result<_, Box<dyn Error>>>()?;
    let encoded_environment = environment
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("\0");
    Ok(SourceReceipt {
        commit,
        tree,
        cargo_lock_sha256: format!("{:x}", Sha256::digest(cargo_lock)),
        cargo,
        cargo_sha256: executable_sha256("cargo")?,
        cargo_config_sha256: cargo_config_sha256()?,
        rustc,
        rustc_sha256: executable_sha256("rustc")?,
        target,
        build_profile: contract.build_profile.to_owned(),
        features: contract
            .features
            .iter()
            .map(|value| (*value).to_owned())
            .collect(),
        tools,
        environment_sha256: format!("{:x}", Sha256::digest(encoded_environment)),
        clean: true,
    })
}

pub(super) fn verify(expected: &SourceReceipt) -> Result<(), Box<dyn Error>> {
    let contract = contract_for_receipt(expected)?;
    let observed = capture(contract)?;
    if &observed != expected {
        return Err("source or toolchain identity changed during evidence execution".into());
    }
    Ok(())
}

pub(crate) fn verify_checkout(expected: &SourceReceipt) -> Result<(), Box<dyn Error>> {
    let contract = contract_for_receipt(expected)?;
    let observed = capture(LayerSourceContract {
        tools: &[],
        ..contract
    })?;
    if observed.commit != expected.commit
        || observed.tree != expected.tree
        || observed.cargo_lock_sha256 != expected.cargo_lock_sha256
        || observed.cargo != expected.cargo
        || observed.cargo_sha256 != expected.cargo_sha256
        || observed.cargo_config_sha256 != expected.cargo_config_sha256
        || observed.rustc != expected.rustc
        || observed.rustc_sha256 != expected.rustc_sha256
        || observed.target != expected.target
    {
        return Err("evidence source identity does not match the active checkout".into());
    }
    Ok(())
}

pub(crate) fn verify_layer_contract(
    layer: &str,
    receipt: &SourceReceipt,
) -> Result<(), Box<dyn Error>> {
    let expected = layer_contract(layer)?;
    let expected_features = expected
        .features
        .iter()
        .map(|value| (*value).to_owned())
        .collect::<Vec<_>>();
    let expected_tools = expected
        .tools
        .iter()
        .copied()
        .collect::<std::collections::BTreeSet<_>>();
    let observed_tools = receipt
        .tools
        .keys()
        .map(String::as_str)
        .collect::<std::collections::BTreeSet<_>>();
    if receipt.build_profile != expected.build_profile
        || receipt.features != expected_features
        || observed_tools != expected_tools
    {
        return Err(format!(
            "{layer} source receipt does not match its exact build profile, features, and tools contract"
        )
        .into());
    }
    Ok(())
}

fn contract_for_receipt(receipt: &SourceReceipt) -> Result<LayerSourceContract, Box<dyn Error>> {
    ["tests", "simulator", "tla", "maelstrom"]
        .into_iter()
        .find_map(|layer| {
            let contract = layer_contract(layer).ok()?;
            verify_layer_contract(layer, receipt).ok().map(|()| contract)
        })
        .ok_or_else(|| {
            "source receipt does not match any reviewed layer build profile, features, and tools contract"
                .into()
        })
}

fn layer_contract(layer: &str) -> Result<LayerSourceContract, Box<dyn Error>> {
    match layer {
        "tests" => Ok(LayerSourceContract {
            build_profile: "test",
            features: &["no-default-features"],
            tools: &[],
        }),
        "simulator" => Ok(LayerSourceContract {
            build_profile: "release-and-test",
            features: &["internal-test-hooks"],
            tools: &[],
        }),
        "tla" => Ok(LayerSourceContract {
            build_profile: "tla",
            features: &[],
            tools: &["java"],
        }),
        "maelstrom" => Ok(LayerSourceContract {
            build_profile: "maelstrom-debug",
            features: &[],
            tools: &["java", "maelstrom", "dot", "gnuplot"],
        }),
        _ => Err(format!("unsupported source profile for layer {layer}").into()),
    }
}

fn git(arguments: &[&str]) -> Result<String, Box<dyn Error>> {
    command_output("git", arguments, false)
}

fn command_output(
    program: &str,
    arguments: &[&str],
    allow_empty: bool,
) -> Result<String, Box<dyn Error>> {
    let output = process::identity_command(program, arguments)?;
    let value = output.stdout.trim().to_owned();
    if value.is_empty() && !allow_empty {
        return Err(format!("{program} produced empty identity output").into());
    }
    Ok(value)
}

fn executable_sha256(name: &str) -> Result<String, Box<dyn Error>> {
    let path = find_tool(name).ok_or_else(|| format!("{name} is not present on PATH"))?;
    Ok(format!("{:x}", Sha256::digest(fs::read(path)?)))
}

fn tool_version(name: &str) -> Result<String, Box<dyn Error>> {
    let output = process::identity_command(name, &["--version"])?;
    tool_version_output(name, &output.stdout, &output.stderr)
}

fn tool_version_output(name: &str, stdout: &str, stderr: &str) -> Result<String, Box<dyn Error>> {
    let value = format!("{stdout}{stderr}").trim().to_owned();
    if value.is_empty() {
        return Err(format!("{name} produced empty identity output").into());
    }
    Ok(value)
}

fn find_tool(name: &str) -> Option<PathBuf> {
    env::split_paths(&env::var_os("PATH")?)
        .map(|directory| directory.join(name))
        .find(|candidate| candidate.is_file())
}

pub(super) fn tool_path(name: &str) -> Option<PathBuf> {
    find_tool(name)
}

fn cargo_config_sha256() -> Result<String, Box<dyn Error>> {
    let mut paths = vec![
        PathBuf::from(".cargo/config"),
        PathBuf::from(".cargo/config.toml"),
    ];
    if let Some(home) = env::var_os("CARGO_HOME")
        .map(PathBuf::from)
        .or_else(|| env::var_os("HOME").map(|home| PathBuf::from(home).join(".cargo")))
    {
        paths.push(home.join("config"));
        paths.push(home.join("config.toml"));
    }
    let mut hasher = Sha256::new();
    for path in paths.into_iter().filter(|path| path.is_file()) {
        hasher.update(path.to_string_lossy().as_bytes());
        hasher.update([0]);
        hasher.update(fs::read(path)?);
        hasher.update([0]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

#[cfg(test)]
mod tests {
    use crate::{SourceReceipt, ToolReceipt};

    use super::{tool_version_output, verify_layer_contract};

    #[test]
    #[cfg(unix)]
    fn tool_version_rejects_empty_combined_output() {
        let error = tool_version_output("fixture-tool", "", "")
            .expect_err("empty version command must fail")
            .to_string();
        assert!(error.contains("empty identity output"));
    }

    fn source(build_profile: &str, features: &[&str], tools: &[&str]) -> SourceReceipt {
        SourceReceipt {
            commit: "commit".to_owned(),
            tree: "tree".to_owned(),
            cargo_lock_sha256: "0".repeat(64),
            cargo: "cargo".to_owned(),
            cargo_sha256: "0".repeat(64),
            cargo_config_sha256: "0".repeat(64),
            rustc: "rustc".to_owned(),
            rustc_sha256: "0".repeat(64),
            target: "target".to_owned(),
            build_profile: build_profile.to_owned(),
            features: features.iter().map(|value| (*value).to_owned()).collect(),
            tools: tools
                .iter()
                .map(|name| {
                    (
                        (*name).to_owned(),
                        ToolReceipt {
                            version: "version".to_owned(),
                            sha256: "0".repeat(64),
                        },
                    )
                })
                .collect(),
            environment_sha256: "0".repeat(64),
            clean: true,
        }
    }

    #[test]
    fn layer_contract_rejects_altered_build_profile_and_features() {
        let exact = source("test", &["no-default-features"], &[]);
        verify_layer_contract("tests", &exact).expect("exact tests contract");

        let mut altered_profile = exact.clone();
        altered_profile.build_profile = "release".to_owned();
        assert!(verify_layer_contract("tests", &altered_profile).is_err());

        let mut altered_features = exact;
        altered_features.features = vec!["internal-test-hooks".to_owned()];
        assert!(verify_layer_contract("tests", &altered_features).is_err());
    }

    #[test]
    fn layer_contract_rejects_cross_layer_receipts_and_tool_drift() {
        let simulator = source("release-and-test", &["internal-test-hooks"], &[]);
        assert!(verify_layer_contract("tests", &simulator).is_err());

        let mut tla = source("tla", &[], &["java"]);
        verify_layer_contract("tla", &tla).expect("exact TLA contract");
        tla.tools.insert(
            "curl".to_owned(),
            ToolReceipt {
                version: "version".to_owned(),
                sha256: "0".repeat(64),
            },
        );
        assert!(verify_layer_contract("tla", &tla).is_err());
    }
}
