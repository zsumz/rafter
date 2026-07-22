//! Descriptor-bound executable and working-directory substitution scenarios.

use std::{
    collections::BTreeMap,
    ffi::OsString,
    fs,
    path::{Path, PathBuf},
    time::Duration,
};

use super::super::{timed_with_timeout_and_policy_and_descriptors, ProcessPolicy};
use super::{
    super::{timed_with_timeout_after_bind, timed_with_timeout_after_run},
    support::unique_test_path,
};

#[test]
fn adapter_records_the_canonical_working_directory() {
    let working_directory = unique_test_path("canonical-working-directory");
    std::fs::create_dir_all(&working_directory).expect("create working directory");

    let output = timed_with_timeout_after_bind(
        "/usr/bin/perl",
        &[OsString::from("-e"), OsString::from("exit 0")],
        &super::super::base_environment(),
        &working_directory,
        Duration::from_secs(2),
        || {},
    )
    .expect("execute in descriptor-bound working directory");

    assert_eq!(
        output.invocation.current_dir,
        std::fs::canonicalize(&working_directory)
            .expect("canonicalize working directory")
            .to_string_lossy()
    );
    std::fs::remove_dir_all(working_directory).expect("remove working directory");
}

#[test]
fn launcher_digest_substitution_is_rejected_by_runtime_verification() {
    let invocation = super::super::expected_invocation(
        "/usr/bin/perl",
        &[OsString::from("-e"), OsString::from("exit 0")],
        &super::super::base_environment(),
        Path::new("."),
    )
    .expect("bind process runtime launchers");
    let mut runtime = invocation
        .launchers
        .iter()
        .map(|launcher| (launcher.runtime.clone(), launcher.executable.clone()))
        .collect::<BTreeMap<_, _>>();
    assert!(crate::receipt::process_launchers_match_runtime(
        &invocation,
        &runtime
    ));

    runtime.get_mut("perl").expect("Perl runtime").sha256 = "f".repeat(64);
    assert!(!crate::receipt::process_launchers_match_runtime(
        &invocation,
        &runtime
    ));
}

#[cfg(unix)]
#[test]
fn shebang_interpreter_is_descriptor_bound_and_source_verified() {
    use std::os::unix::fs::PermissionsExt;

    let script = unique_test_path("bound-shebang");
    if let Some(parent) = script.parent() {
        fs::create_dir_all(parent).expect("create shebang fixture directory");
    }
    fs::write(
        &script,
        b"#!/usr/bin/env bash\nprintf 'bound-interpreter'\n",
    )
    .expect("write shebang fixture");
    fs::set_permissions(&script, fs::Permissions::from_mode(0o755))
        .expect("make shebang fixture executable");

    let output = super::super::timed_with_timeout(
        script.to_str().expect("UTF-8 shebang fixture"),
        &[],
        &super::super::base_environment(),
        Path::new("."),
        Duration::from_secs(2),
    )
    .expect("execute descriptor-bound shebang interpreter");
    assert_eq!(output.stdout, b"bound-interpreter");
    let interpreter = output
        .invocation
        .launchers
        .last()
        .expect("target interpreter receipt");
    assert_eq!(interpreter.role, "target-interpreter");
    assert_eq!(interpreter.runtime, "bash");

    let mut runtime = output
        .invocation
        .launchers
        .iter()
        .map(|launcher| (launcher.runtime.clone(), launcher.executable.clone()))
        .collect::<BTreeMap<_, _>>();
    runtime.get_mut("bash").expect("Bash runtime").sha256 = "e".repeat(64);
    assert!(!crate::receipt::process_launchers_match_runtime(
        &output.invocation,
        &runtime
    ));
    fs::remove_file(script).expect("remove shebang fixture");
}

#[cfg(unix)]
#[test]
fn absolute_bash_shebang_must_name_the_source_bound_runtime() {
    use std::os::unix::fs::PermissionsExt;

    let alternate_root = unique_test_path("alternate-bash-runtime");
    let alternate_bash_path = alternate_root.join("bash");
    let script = unique_test_path("absolute-shebang");
    fs::create_dir_all(&alternate_root).expect("create absolute shebang fixture directory");
    fs::copy("/bin/bash", &alternate_bash_path).expect("copy alternate Bash runtime");
    fs::set_permissions(&alternate_bash_path, fs::Permissions::from_mode(0o755))
        .expect("make alternate Bash executable");
    let alternate_bash =
        fs::canonicalize(&alternate_bash_path).expect("canonicalize alternate Bash runtime");
    fs::write(
        &script,
        format!(
            "#!{}\nprintf 'unbound-interpreter'\n",
            alternate_bash.display()
        ),
    )
    .expect("write absolute shebang fixture");
    fs::set_permissions(&script, fs::Permissions::from_mode(0o755))
        .expect("make absolute shebang fixture executable");

    let error = super::super::timed_with_timeout(
        script.to_str().expect("UTF-8 absolute shebang fixture"),
        &[],
        &super::super::base_environment(),
        Path::new("."),
        Duration::from_secs(2),
    )
    .expect_err("unregistered absolute Bash identity must fail closed");
    let message = error.to_string();
    assert!(
        message.contains("is not the source-bound PATH runtime"),
        "{message}"
    );

    fs::remove_file(script).expect("remove absolute shebang fixture");
    fs::remove_dir_all(alternate_root).expect("remove alternate Bash runtime");
}

#[cfg(unix)]
#[test]
fn working_directory_replacement_is_rejected_after_descriptor_bound_execution() {
    let root = unique_test_path("working-directory-binding");
    let moved = root.with_extension("original");
    let _ = std::fs::remove_dir_all(&root);
    let _ = std::fs::remove_dir_all(&moved);
    std::fs::create_dir_all(&root).expect("create original working directory");

    let error = timed_with_timeout_after_run(
        "/usr/bin/perl",
        &[
            OsString::from("-e"),
            OsString::from("print qq(retained-output\\n)"),
        ],
        &super::super::base_environment(),
        &root,
        Duration::from_secs(2),
        || {
            std::fs::rename(&root, &moved).expect("move bound working directory");
            std::fs::create_dir_all(&root).expect("install replacement working directory");
        },
    )
    .expect_err("receipt must reject a working-directory path replacement");
    let error = error.to_string();
    assert!(error.contains("producer directory changed after it was opened"));
    assert!(error.contains("retained subprocess stdout"));
    assert!(error.contains("resource telemetry"));

    std::fs::remove_dir_all(root).expect("remove replacement working directory");
    std::fs::remove_dir_all(moved).expect("remove original working directory");
}

#[test]
#[cfg_attr(
    not(target_os = "linux"),
    ignore = "production descriptor-bound executable launch is Linux-only"
)]
fn executable_receipt_and_launch_share_the_same_open_file_after_path_replacement() {
    let root = std::env::temp_dir().join(format!(
        "rafter-invariants-executable-binding-{}-{}",
        std::process::id(),
        super::support::next_sequence()
    ));
    let moved = root.with_extension("original");
    let _ = std::fs::remove_file(&root);
    let _ = std::fs::remove_file(&moved);
    std::fs::copy("/usr/bin/perl", &root).expect("copy original executable");
    let arguments = [
        OsString::from("-e"),
        OsString::from("print qq(bound-executable\\n)"),
    ];

    let output = timed_with_timeout_after_bind(
        root.to_str().expect("UTF-8 executable path"),
        &arguments,
        &super::super::base_environment(),
        Path::new("."),
        Duration::from_secs(2),
        || {
            std::fs::rename(&root, &moved).expect("move bound executable");
            std::fs::copy("/usr/bin/false", &root).expect("install replacement executable");
        },
    )
    .expect("launch uses the descriptor opened before path replacement");

    assert!(
        output.status.success(),
        "stdout: {}; stderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(output.stdout, b"bound-executable\n");
    let original = super::super::expected_invocation(
        moved.to_str().expect("UTF-8 moved executable path"),
        &arguments,
        &super::super::base_environment(),
        Path::new("."),
    )
    .expect("hash moved original executable");
    assert_eq!(output.invocation.program_sha256, original.program_sha256);

    std::fs::remove_file(root).expect("remove replacement executable");
    std::fs::remove_file(moved).expect("remove original executable");
}

#[test]
#[cfg_attr(
    not(target_os = "linux"),
    ignore = "production inherited descriptor launch is Linux-only"
)]
fn inherited_directory_binding_survives_path_replacement_through_launcher_chain() {
    use std::os::unix::fs::symlink;

    let repository_test_path = |label| {
        PathBuf::from("target/rafter-invariants/process-tests").join(
            unique_test_path(label)
                .file_name()
                .expect("temporary fixture has a file name"),
        )
    };
    let root = repository_test_path("held-directory-binding");
    let moved = repository_test_path("held-directory-binding-original");
    let external = repository_test_path("held-directory-binding-external");
    let _ = std::fs::remove_file(&root);
    let _ = std::fs::remove_dir_all(&root);
    let _ = std::fs::remove_dir_all(&moved);
    let _ = std::fs::remove_dir_all(&external);
    std::fs::create_dir_all(&root).expect("create held directory fixture");
    std::fs::write(root.join("checkpoint"), b"held-inode").expect("write held checkpoint fixture");
    std::fs::create_dir_all(&external).expect("create replacement fixture");
    std::fs::write(external.join("checkpoint"), b"replacement")
        .expect("write replacement checkpoint fixture");

    let held = crate::execution::filesystem::HeldDirectory::open(&root)
        .expect("hold checkpoint directory");
    let binding = held.bind_for_child().expect("bind directory for child");
    let mut environment = super::super::base_environment();
    environment.insert(
        "BOUND_DIRECTORY".to_owned(),
        binding.path().to_string_lossy().into_owned(),
    );

    std::fs::rename(&root, &moved).expect("move original checkpoint directory");
    let external = std::fs::canonicalize(&external).expect("canonical replacement directory");
    symlink(&external, &root).expect("replace checkpoint path with external symlink");

    let output = timed_with_timeout_and_policy_and_descriptors(
        "sh",
        &[
            OsString::from("-c"),
            OsString::from("cat \"$BOUND_DIRECTORY/checkpoint\""),
        ],
        &environment,
        Path::new("."),
        Duration::from_secs(2),
        ProcessPolicy::default(),
        &[binding.descriptor()],
    )
    .expect("launch child through inherited descriptor");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(output.stdout, b"held-inode");

    std::fs::remove_file(&root).expect("remove replacement symlink");
    std::fs::remove_dir_all(&moved).expect("remove original fixture");
    std::fs::remove_dir_all(&external).expect("remove replacement fixture");
}
