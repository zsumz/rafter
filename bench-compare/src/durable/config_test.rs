//! Configuration rejection and workload-selection scenarios.

use super::{Config, HardStateBackend, Workload};

fn parse(args: &[&str]) -> Result<Option<Config>, String> {
    Config::parse(args.iter().map(|arg| (*arg).to_owned()))
}

#[test]
fn defaults_preserve_smoke_workloads() {
    let config = parse(&[]).unwrap().unwrap();
    assert_eq!(config.workload, Workload::All);
    assert_eq!(config.hard_state, HardStateBackend::Replace);
    assert_eq!(config.proposals, 512);
    assert_eq!(config.batch_sizes, [1, 32]);
    assert_eq!(config.snapshot_bytes, 32 * 1024 * 1024);
}

#[test]
fn accepts_fixed_workloads_and_partial_final_batch() {
    let config = parse(&[
        "--workload",
        "proposals",
        "--proposals",
        "1001",
        "--batch-sizes",
        "1,8,32,128",
        "--payload-bytes",
        "512",
    ])
    .unwrap()
    .unwrap();
    assert_eq!(config.workload, Workload::Proposals);
    assert_eq!(config.proposals, 1001);
    assert_eq!(config.batch_sizes, [1, 8, 32, 128]);
    assert_eq!(config.payload_bytes, 512);
    assert!(parse(&["--help"]).unwrap().is_none());
}

#[test]
fn rejects_unbounded_empty_duplicate_or_unknown_inputs() {
    for args in [
        vec!["--proposals", "0"],
        vec!["--proposals", "1048577"],
        vec!["--proposals", "99999999999999999999999999"],
        vec!["--proposals", "1048576", "--payload-bytes", "1024"],
        vec!["--batch-sizes", ""],
        vec!["--batch-sizes", "1,1"],
        vec!["--batch-sizes", "1025"],
        vec!["--proposals", "1"],
        vec!["--snapshot-bytes", "1073741825"],
        vec!["--workload", "unknown"],
        vec!["--proposals"],
        vec!["--unknown", "1"],
    ] {
        assert!(parse(&args).is_err(), "accepted {args:?}");
    }
}

#[test]
fn hard_state_selection_is_explicit_and_bounded() {
    assert_eq!(
        parse(&["--hard-state", "journal"])
            .unwrap()
            .unwrap()
            .hard_state,
        HardStateBackend::Journal
    );
    assert!(parse(&["--hard-state", "auto"]).is_err());
}
