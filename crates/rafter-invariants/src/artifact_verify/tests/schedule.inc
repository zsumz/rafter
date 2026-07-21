#[test]
fn scheduled_simulator_log_proves_exact_source_derived_plan() {
    let source_ref = "abc123";
    let log = scheduled_log(source_ref);
    let configuration = simulator_configuration("nightly");

    assert!(validate_simulator_schedule(
        "nightly",
        source_ref,
        &configuration,
        std::slice::from_ref(&log),
    )
    .is_ok());
    assert!(validate_simulator_schedule(
        "nightly",
        "different",
        &configuration,
        std::slice::from_ref(&log),
    )
    .is_err());
    assert!(validate_simulator_schedule("nightly", source_ref, &configuration, &[]).is_err());

    let mut changed = configuration.clone();
    changed.soak_steps += 1;
    assert!(validate_simulator_schedule(
        "nightly",
        source_ref,
        &changed,
        std::slice::from_ref(&log),
    )
    .is_err());
    changed = configuration.clone();
    changed.seed_count = Some(7);
    assert!(validate_simulator_schedule(
        "nightly",
        source_ref,
        &changed,
        std::slice::from_ref(&log),
    )
    .is_err());
    changed = configuration;
    changed.state_floors = crate::contract::profile::SimulatorStateFloors::Aggregate {
        protocol: 100_000_001,
        verifier: 100_000_001,
    };
    assert!(validate_simulator_schedule("nightly", source_ref, &changed, &[log]).is_err());
}

#[test]
fn scheduled_simulator_rejects_fabricated_totals_and_executed_seeds() {
    let source_ref = "abc123";
    let log = scheduled_log(source_ref);
    let configuration = simulator_configuration("nightly");
    let fabricated_total = log.replace(
        "\"unique_protocol_states\":40000000",
        "\"unique_protocol_states\":4",
    );
    assert!(validate_simulator_schedule(
        "nightly",
        source_ref,
        &configuration,
        &[fabricated_total],
    )
    .is_err());

    let wrong_seed = log.replacen("\"seed\":", "\"seed\":999,\"ignored_seed\":", 1);
    assert!(
        validate_simulator_schedule("nightly", source_ref, &configuration, &[wrong_seed]).is_err()
    );

    let wrong_steps = log.replace("\"steps\":1024", "\"steps\":1023");
    assert!(
        validate_simulator_schedule("nightly", source_ref, &configuration, &[wrong_steps]).is_err()
    );
}

#[test]
fn scheduled_seed_banner_uses_simulator_canonical_hex() {
    let seeds = scheduled_simulator_seeds("weekly", "abc123", 10).expect("weekly seeds");
    assert!(seeds.contains("0xe00e6256b8bdd15"));
    assert!(!seeds.contains("0x0e00e6256b8bdd15"));
}

#[test]
fn pr_simulator_log_proves_exact_curated_seed_inventory() {
    let logs = pr_logs();
    let configuration = simulator_configuration("pr");
    assert!(validate_simulator_schedule("pr", "unused", &configuration, &logs).is_ok());

    let mut substituted = logs.clone();
    substituted[1] = substituted[1].replacen("\"seed\":37123", "\"seed\":999", 1);
    assert!(validate_simulator_schedule("pr", "unused", &configuration, &substituted).is_err());

    let mut extra = logs.clone();
    write!(
        extra[1],
        "\n{EVENT_PREFIX}{}",
        serde_json::to_string(&json!({
            "event": "soak-check",
            "check_id": "raft-soak",
            "status": "pass",
            "seed": 0x9107_u64,
            "steps": 320,
        }))
        .expect("serialize extra event")
    )
    .expect("append extra event");
    assert!(validate_simulator_schedule("pr", "unused", &configuration, &extra).is_err());

    let mut unknown = logs;
    write!(
        unknown[0],
        "\n{EVENT_PREFIX}{}",
        serde_json::to_string(&json!({
            "event": "soak-check",
            "check_id": "unreviewed-soak",
            "status": "pass",
            "seed": 0x9103_u64,
            "steps": 320,
        }))
        .expect("serialize unknown event")
    )
    .expect("append unknown event");
    assert!(validate_simulator_schedule("pr", "unused", &configuration, &unknown).is_err());
}

fn pr_logs() -> Vec<String> {
    let mut soak = vec![
        "label: raft-soak".to_owned(),
        "exit_code: Some(0)".to_owned(),
        "model-check raft-soak seeds source=curated seeds=0x9103,0x9104,0x9105,0x9106".to_owned(),
    ];
    for check_id in ["raft-soak", "raft-soak-lease", "raft-soak-membership"] {
        for seed in 0x9103_u64..=0x9106 {
            soak.push(event(&json!({
                "event": "soak-check",
                "check_id": check_id,
                "status": "pass",
                "seed": seed,
                "steps": 320,
            })));
        }
    }
    vec![
        "label: fast\nexit_code: Some(0)".to_owned(),
        soak.join("\n"),
    ]
}
