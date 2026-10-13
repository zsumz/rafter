//! Nightly covers repeated cold recovery and owners retained across compaction.

#[test]
#[ignore = "96-cycle file/TLS owner soak; run scripts/reference-owner-soak"]
fn ninety_six_load_compaction_and_cold_recovery_cycles() {
    super::run_cycles(96);
}

#[test]
#[ignore = "256-cycle file/TLS continuous owner soak; run scripts/reference-owner-soak"]
fn two_hundred_fifty_six_compactions_preserve_live_owners() {
    super::continuous::run_cycles(256);
}
