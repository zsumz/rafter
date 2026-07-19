//! Canonical source-runtime and launcher receipts for verifier unit scenarios.

use std::collections::BTreeMap;

pub(crate) fn process_runtime(include_bash: bool) -> BTreeMap<String, crate::ExecutableReceipt> {
    let mut runtime = [
        ("perl", "/usr/bin/perl"),
        ("ps", "/usr/bin/ps"),
        ("time", "/usr/bin/time"),
    ]
    .into_iter()
    .map(|(name, program)| {
        (
            name.to_owned(),
            crate::ExecutableReceipt {
                program: program.to_owned(),
                sha256: "0".repeat(64),
            },
        )
    })
    .collect::<BTreeMap<_, _>>();
    if include_bash {
        runtime.insert(
            "bash".to_owned(),
            crate::ExecutableReceipt {
                program: "/bin/bash".to_owned(),
                sha256: "0".repeat(64),
            },
        );
    }
    runtime
}

pub(crate) fn launchers(include_bash: bool) -> Vec<crate::LauncherReceipt> {
    let runtime = process_runtime(include_bash);
    let mut launchers = [
        ("resource-wrapper", "perl"),
        ("resource-sampler", "time"),
        ("target-group-launcher", "perl"),
        ("process-observer", "ps"),
    ]
    .into_iter()
    .map(|(role, name)| crate::LauncherReceipt {
        role: role.to_owned(),
        runtime: name.to_owned(),
        executable: runtime[name].clone(),
    })
    .collect::<Vec<_>>();
    if include_bash {
        launchers.push(crate::LauncherReceipt {
            role: "target-interpreter".to_owned(),
            runtime: "bash".to_owned(),
            executable: runtime["bash"].clone(),
        });
    }
    launchers
}
