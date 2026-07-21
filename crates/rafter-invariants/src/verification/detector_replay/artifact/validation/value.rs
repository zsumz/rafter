//! Primitive source, toolchain, path, and digest validation.

use std::path::Component;

use crate::verification::source::ReplayToolchainProgramReceipt;

use super::super::model::ReplayReport;

pub(super) fn validate_source(report: &ReplayReport) -> Result<(), String> {
    if report.source_ref != report.source.commit {
        return Err(
            "verifier replay source reference differs from authenticated commit".to_owned(),
        );
    }
    require_git_object(&report.source.commit, "source commit")?;
    require_git_object(&report.source.tree, "source tree")?;
    let source_sha256 = crate::verification::source::canonical_sha256(
        &report.source,
        "authenticated source receipt",
    )?;
    if source_sha256 != report.source_sha256 {
        return Err("authenticated source receipt digest changed".to_owned());
    }
    let toolchain_sha256 = crate::verification::source::canonical_sha256(
        &report.toolchain,
        "replay toolchain receipt",
    )?;
    if toolchain_sha256 != report.toolchain_sha256 {
        return Err("replay toolchain receipt digest changed".to_owned());
    }
    require_digest(&report.source_sha256, "authenticated source receipt")?;
    require_digest(&report.toolchain_sha256, "replay toolchain receipt")?;
    require_nonempty(
        &report.source.materialization.contract,
        "source materialization contract",
    )?;
    require_digest(
        &report.source.materialization.sha256,
        "source materialization",
    )?;
    if report.source.materialization.tracked_entries == 0 {
        return Err("authenticated source materialization has no tracked entries".to_owned());
    }
    require_digest(&report.source.cargo_lock_sha256, "Cargo.lock")?;
    require_digest(&report.source.cargo_config_sha256, "Cargo configuration")?;
    require_digest(&report.source.environment_sha256, "source environment")?;
    require_nonempty(&report.source.target, "source target")?;
    validate_program(&report.toolchain.cargo, "Cargo")?;
    validate_program(&report.toolchain.rustc, "rustc")
}

fn validate_program(program: &ReplayToolchainProgramReceipt, name: &str) -> Result<(), String> {
    require_nonempty(&program.identity, &format!("{name} identity"))?;
    require_digest(&program.launcher_sha256, &format!("{name} launcher"))?;
    require_digest(&program.executable_sha256, &format!("{name} executable"))?;
    let path = std::path::Path::new(&program.executable_path);
    if !path.is_absolute()
        || path
            .components()
            .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
    {
        return Err(format!("{name} executable path is not canonical absolute"));
    }
    Ok(())
}

pub(super) fn require_relative_path(path: &str, label: &str) -> Result<(), String> {
    let path = std::path::Path::new(path);
    if path.as_os_str().is_empty()
        || path.is_absolute()
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(format!(
            "{label} source path is not canonical repository-relative"
        ));
    }
    Ok(())
}

fn require_git_object(value: &str, label: &str) -> Result<(), String> {
    if !matches!(value.len(), 40 | 64) || !lower_hex(value) {
        return Err(format!("{label} is not a canonical Git object ID"));
    }
    Ok(())
}

pub(super) fn require_digest(value: &str, label: &str) -> Result<(), String> {
    if value.len() != 64 || !lower_hex(value) {
        return Err(format!("{label} digest is not canonical SHA-256"));
    }
    Ok(())
}

fn lower_hex(value: &str) -> bool {
    value
        .bytes()
        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

pub(super) fn require_nonempty(value: &str, label: &str) -> Result<(), String> {
    if value.trim().is_empty() {
        Err(format!("verifier replay report has an empty {label}"))
    } else {
        Ok(())
    }
}
