//! Process invocation and launcher-chain acceptance.

use std::{collections::BTreeMap, path::Path};

use crate::evidence::{ExecutableReceipt, InvocationReceipt, SourceReceipt};

pub(crate) fn process_invocation_is_complete(invocation: &InvocationReceipt) -> bool {
    !invocation.program.trim().is_empty()
        && is_sha256(&invocation.program_sha256)
        && !invocation.arguments.is_empty()
        && Path::new(&invocation.current_dir).is_absolute()
        && crate::provenance::invocation::environment_matches_digest(
            &invocation.environment,
            &invocation.environment_sha256,
        )
        && is_sha256(&invocation.environment_sha256)
        && launcher_chain_is_complete(invocation)
}

pub(crate) fn process_invocation_matches_source(
    invocation: &InvocationReceipt,
    source: &SourceReceipt,
) -> bool {
    process_invocation_is_complete(invocation)
        && process_launchers_match_runtime(invocation, &source.process_runtime)
}

pub(crate) fn script_invocation_matches_source(
    invocation: &InvocationReceipt,
    source: &SourceReceipt,
) -> bool {
    process_invocation_matches_source(invocation, source)
        && invocation.launchers.len() == 5
        && invocation.launchers.last().is_some_and(|launcher| {
            launcher.role == "target-interpreter" && launcher.runtime == "bash"
        })
}

pub(crate) fn process_launchers_match_runtime(
    invocation: &InvocationReceipt,
    runtime: &BTreeMap<String, ExecutableReceipt>,
) -> bool {
    invocation
        .launchers
        .iter()
        .all(|launcher| runtime.get(&launcher.runtime) == Some(&launcher.executable))
}

fn launcher_chain_is_complete(invocation: &InvocationReceipt) -> bool {
    let expected = [
        ("resource-wrapper", "perl"),
        ("resource-sampler", "time"),
        ("target-group-launcher", "perl"),
        ("process-observer", "ps"),
    ];
    let core_matches = invocation.launchers.len() >= expected.len()
        && invocation
            .launchers
            .iter()
            .zip(expected)
            .all(|(launcher, (role, runtime))| {
                launcher.role == role
                    && launcher.runtime == runtime
                    && Path::new(&launcher.executable.program).is_absolute()
                    && is_sha256(&launcher.executable.sha256)
            });
    let interpreter_matches = match invocation.launchers.get(expected.len()..) {
        Some([]) => true,
        Some([launcher]) => {
            launcher.role == "target-interpreter"
                && launcher.runtime == "bash"
                && Path::new(&launcher.executable.program).is_absolute()
                && is_sha256(&launcher.executable.sha256)
        }
        _ => false,
    };
    core_matches && interpreter_matches
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}
