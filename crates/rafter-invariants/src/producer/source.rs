use std::{
    env,
    error::Error,
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use sha2::{Digest, Sha256};

use crate::{SourceReceipt, ToolReceipt};

use super::process;

pub(super) fn capture_for_layer(layer: &str) -> Result<SourceReceipt, Box<dyn Error>> {
    match layer {
        "tests" => capture("test", vec!["no-default-features".to_owned()], &[]),
        "simulator" => capture(
            "release-and-test",
            vec!["internal-test-hooks".to_owned()],
            &[],
        ),
        "tla" => capture("tla", Vec::new(), &["java"]),
        "maelstrom" => capture("release", Vec::new(), &[]),
        _ => Err(format!("unsupported source profile for layer {layer}").into()),
    }
}

fn capture(
    build_profile: &str,
    features: Vec<String>,
    additional_tools: &[&str],
) -> Result<SourceReceipt, Box<dyn Error>> {
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
    let tools = additional_tools
        .iter()
        .map(|name| {
            let version = command_output(name, &["--version"], false)?;
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
        cargo_sha256: tool_sha256("cargo")?,
        cargo_config_sha256: cargo_config_sha256()?,
        rustc,
        rustc_sha256: tool_sha256("rustc")?,
        target,
        build_profile: build_profile.to_owned(),
        features,
        tools,
        environment_sha256: format!("{:x}", Sha256::digest(encoded_environment)),
        clean: true,
    })
}

pub(super) fn verify(expected: &SourceReceipt) -> Result<(), Box<dyn Error>> {
    let names = expected
        .tools
        .keys()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let observed = capture(&expected.build_profile, expected.features.clone(), &names)?;
    if &observed != expected {
        return Err("source or toolchain identity changed during evidence execution".into());
    }
    Ok(())
}

pub(crate) fn verify_checkout(expected: &SourceReceipt) -> Result<(), Box<dyn Error>> {
    let names = expected
        .tools
        .keys()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let observed = capture(&expected.build_profile, expected.features.clone(), &names)?;
    if observed.commit != expected.commit
        || observed.tree != expected.tree
        || observed.cargo_lock_sha256 != expected.cargo_lock_sha256
        || observed.cargo != expected.cargo
        || observed.cargo_sha256 != expected.cargo_sha256
        || observed.cargo_config_sha256 != expected.cargo_config_sha256
        || observed.rustc != expected.rustc
        || observed.rustc_sha256 != expected.rustc_sha256
        || observed.target != expected.target
        || observed.tools != expected.tools
        || observed.environment_sha256 != expected.environment_sha256
    {
        return Err("evidence source identity does not match the active checkout".into());
    }
    Ok(())
}

fn git(arguments: &[&str]) -> Result<String, Box<dyn Error>> {
    command_output("git", arguments, false)
}

fn command_output(
    program: &str,
    arguments: &[&str],
    allow_empty: bool,
) -> Result<String, Box<dyn Error>> {
    let output = Command::new(program).args(arguments).output()?;
    if !output.status.success() {
        return Err(format!(
            "{program} failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )
        .into());
    }
    let value = String::from_utf8(output.stdout)?.trim().to_owned();
    if value.is_empty() && !allow_empty {
        return Err(format!("{program} produced empty identity output").into());
    }
    Ok(value)
}

fn tool_sha256(name: &str) -> Result<String, Box<dyn Error>> {
    let sysroot = command_output("rustc", &["--print", "sysroot"], false)?;
    let sysroot_tool = Path::new(&sysroot).join("bin").join(name);
    let path = if sysroot_tool.is_file() {
        sysroot_tool
    } else {
        find_tool(name).ok_or_else(|| format!("{name} is not present on PATH"))?
    };
    Ok(format!("{:x}", Sha256::digest(fs::read(path)?)))
}

fn executable_sha256(name: &str) -> Result<String, Box<dyn Error>> {
    let path = find_tool(name).ok_or_else(|| format!("{name} is not present on PATH"))?;
    Ok(format!("{:x}", Sha256::digest(fs::read(path)?)))
}

fn find_tool(name: &str) -> Option<PathBuf> {
    env::split_paths(&env::var_os("PATH")?)
        .map(|directory| directory.join(name))
        .find(|candidate| candidate.is_file())
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
