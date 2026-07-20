//! Verifier-owned layer, external-tool, and process-runtime source policy.

use std::{
    collections::BTreeSet,
    error::Error,
    fmt::Write as _,
    fs,
    path::{Path, PathBuf},
};

use crate::{
    evidence::{ExecutableReceipt, SourceReceipt, ToolReceipt},
    provenance::source::{file_sha256, find_executable, identity_probe_at},
};

use super::SourceAuthenticationError;

#[cfg(test)]
mod tests;

#[derive(Clone, Copy)]
struct ToolProbe {
    name: &'static str,
    arguments: &'static [&'static str],
}

struct LayerContract {
    build_profile: &'static str,
    features: &'static [&'static str],
    tools: &'static [ToolProbe],
    runtime: &'static [&'static str],
}

const JAVA: ToolProbe = ToolProbe {
    name: "java",
    arguments: &["-version"],
};
const MAELSTROM: ToolProbe = ToolProbe {
    name: "maelstrom",
    arguments: &["serve", "--help"],
};
const DOT: ToolProbe = ToolProbe {
    name: "dot",
    arguments: &["-V"],
};
const GNUPLOT: ToolProbe = ToolProbe {
    name: "gnuplot",
    arguments: &["--version"],
};

pub(super) fn verify_layer_contract(
    layer: &str,
    receipt: &SourceReceipt,
) -> Result<(), Box<dyn Error>> {
    let expected = layer_contract(layer)?;
    let observed_tools = receipt
        .tools
        .keys()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let observed_runtime = receipt
        .process_runtime
        .keys()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    if receipt.build_profile != expected.build_profile
        || receipt.features
            != expected
                .features
                .iter()
                .map(|feature| (*feature).to_owned())
                .collect::<Vec<_>>()
        || observed_tools != expected.tools.iter().map(|tool| tool.name).collect()
        || observed_runtime != expected.runtime.iter().copied().collect()
    {
        return Err(format!(
            "{layer} source receipt does not match its exact build profile, features, and tools contract"
        )
        .into());
    }
    Ok(())
}

pub(super) fn verify_runtime_identities(
    layer: &str,
    receipt: &SourceReceipt,
    root: &Path,
) -> Result<(), SourceAuthenticationError> {
    let contract = layer_contract(layer)
        .map_err(|error| SourceAuthenticationError::Unverifiable(error.to_string()))?;
    for probe in contract.tools {
        let expected = receipt.tools.get(probe.name).ok_or_else(|| {
            SourceAuthenticationError::Unverifiable(format!("{layer} omitted tool {}", probe.name))
        })?;
        let observed = observe_tool(*probe, root)
            .map_err(|error| SourceAuthenticationError::Unverifiable(error.to_string()))?;
        require_exact_identity(
            &format!("{layer} tool identity changed for {}", probe.name),
            expected,
            &observed,
        )?;
    }
    for runtime in contract.runtime {
        let expected = receipt.process_runtime.get(*runtime).ok_or_else(|| {
            SourceAuthenticationError::Unverifiable(format!(
                "{layer} omitted process runtime {runtime}"
            ))
        })?;
        let observed = observe_runtime(runtime)
            .map_err(|error| SourceAuthenticationError::Unverifiable(error.to_string()))?;
        require_exact_identity(
            &format!("{layer} process runtime identity changed for {runtime}"),
            expected,
            &observed,
        )?;
    }
    Ok(())
}

fn require_exact_identity<T: Eq>(
    message: &str,
    expected: &T,
    observed: &T,
) -> Result<(), SourceAuthenticationError> {
    if expected == observed {
        Ok(())
    } else {
        Err(SourceAuthenticationError::Stale(message.to_owned()))
    }
}

fn observe_tool(probe: ToolProbe, root: &Path) -> Result<ToolReceipt, Box<dyn Error>> {
    let executable = find_executable(probe.name)
        .ok_or_else(|| format!("{} is not present on PATH", probe.name))?;
    let output = identity_probe_at(probe.name, probe.arguments, root)?;
    let mut version = format!("{}{}", output.stdout, output.stderr)
        .trim()
        .to_owned();
    if version.is_empty() {
        return Err(format!("{} produced empty identity output", probe.name).into());
    }
    if probe.name == "maelstrom" {
        let executable = fs::canonicalize(&executable)?;
        let jar = executable
            .parent()
            .ok_or("Maelstrom launcher has no installation directory")?
            .join("lib/maelstrom.jar");
        write!(
            version,
            "\nrafter-adjacent-lib/maelstrom.jar-sha256: {}",
            file_sha256(&jar)?
        )?;
    }
    Ok(ToolReceipt {
        version,
        sha256: file_sha256(&executable)?,
    })
}

fn observe_runtime(name: &str) -> Result<ExecutableReceipt, Box<dyn Error>> {
    let path = match name {
        "bash" => find_executable("bash").ok_or("bash is not present on PATH")?,
        "perl" => PathBuf::from("/usr/bin/perl"),
        "time" => PathBuf::from("/usr/bin/time"),
        "ps" => PathBuf::from(ps_path()),
        _ => return Err(format!("unknown reviewed process runtime {name}").into()),
    };
    let path = fs::canonicalize(path)?;
    Ok(ExecutableReceipt {
        program: path.to_string_lossy().into_owned(),
        sha256: file_sha256(&path)?,
    })
}

#[cfg(target_os = "macos")]
const fn ps_path() -> &'static str {
    "/bin/ps"
}

#[cfg(not(target_os = "macos"))]
const fn ps_path() -> &'static str {
    "/usr/bin/ps"
}

fn layer_contract(layer: &str) -> Result<LayerContract, Box<dyn Error>> {
    match layer {
        "tests" => Ok(LayerContract {
            build_profile: "test",
            features: &["no-default-features"],
            tools: &[],
            runtime: &["perl", "ps", "time"],
        }),
        "simulator" => Ok(LayerContract {
            build_profile: "release-and-test",
            features: &["internal-test-hooks"],
            tools: &[],
            runtime: &["perl", "ps", "time"],
        }),
        "tla" => Ok(LayerContract {
            build_profile: "tla",
            features: &[],
            tools: &[JAVA],
            runtime: &["perl", "ps", "time"],
        }),
        "maelstrom" => Ok(LayerContract {
            build_profile: "maelstrom-debug",
            features: &[],
            tools: &[JAVA, MAELSTROM, DOT, GNUPLOT],
            runtime: &["bash", "perl", "ps", "time"],
        }),
        _ => Err(format!("unknown evidence layer {layer}").into()),
    }
}
