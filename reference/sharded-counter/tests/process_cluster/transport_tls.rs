#[test]
#[ignore = "durable TLS session state is mandatory and checksummed"]
fn transport_session_state_missing_or_corrupt_fails_closed() {
    let mut cluster = ProcessCluster::start("transport-state-refusal");
    cluster.kill(1);
    let state = cluster.scratch_path().join("host-1/transport.state");
    let original = fs::read(&state)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", state.display()));

    fs::remove_file(&state)
        .unwrap_or_else(|error| panic!("could not remove {}: {error}", state.display()));
    cluster.restart_expect_fatal(1, "transport session state is missing");

    let mut corrupt = original.clone();
    let byte = corrupt
        .get_mut(16)
        .expect("transport session fixture has a checksummed body");
    *byte ^= 1;
    fs::write(&state, corrupt)
        .unwrap_or_else(|error| panic!("could not corrupt {}: {error}", state.display()));
    cluster.restart_expect_fatal(1, "checksum");

    fs::write(&state, original)
        .unwrap_or_else(|error| panic!("could not restore {}: {error}", state.display()));
    cluster.restart(1);
    cluster.wait_ready();
}
