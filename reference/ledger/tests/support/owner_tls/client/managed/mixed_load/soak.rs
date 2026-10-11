//! The nightly profile retains the same assertions across repeated cold recovery.

#[test]
#[ignore = "96-cycle file/TLS owner soak; run scripts/reference-owner-soak"]
fn ninety_six_load_compaction_and_cold_recovery_cycles() {
    super::run_cycles(96);
}
