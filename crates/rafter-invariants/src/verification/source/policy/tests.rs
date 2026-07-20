//! Detector-level mutations for independently observed tool and runtime identities.

use crate::evidence::{ExecutableReceipt, ToolReceipt};

#[test]
fn every_tool_receipt_value_is_load_bearing() {
    let observed = ToolReceipt {
        version: "tool 1".to_owned(),
        sha256: "0".repeat(64),
    };
    super::require_exact_identity("tool", &observed, &observed).expect("exact tool identity");

    let mut version = observed.clone();
    version.version.push('x');
    assert!(super::require_exact_identity("tool", &version, &observed).is_err());
    let mut digest = observed.clone();
    digest.sha256 = "f".repeat(64);
    assert!(super::require_exact_identity("tool", &digest, &observed).is_err());
}

#[test]
fn every_runtime_receipt_value_is_load_bearing() {
    let observed = ExecutableReceipt {
        program: "/usr/bin/runtime".to_owned(),
        sha256: "0".repeat(64),
    };
    super::require_exact_identity("runtime", &observed, &observed).expect("exact runtime identity");

    let mut program = observed.clone();
    program.program.push('x');
    assert!(super::require_exact_identity("runtime", &program, &observed).is_err());
    let mut digest = observed.clone();
    digest.sha256 = "f".repeat(64);
    assert!(super::require_exact_identity("runtime", &digest, &observed).is_err());
}
