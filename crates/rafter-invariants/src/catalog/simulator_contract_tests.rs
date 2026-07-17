use std::collections::{BTreeMap, BTreeSet};

use syn::{visit::Visit, Expr, ExprCall, Item, Lit};

use super::{
    SimulatorCheckContract, SimulatorRunnerConfiguration, SimulatorStateFloors, PR_FAST_CHECK_IDS,
    PR_SPECIALIZED_OBSERVATIONS,
};

#[test]
fn simulator_contract_deserializes_numeric_and_floor_policy() {
    let configuration = BTreeMap::from([
        ("build", "release-and-test-locked"),
        ("compile_timeout", "10m"),
        ("completion", "frontier-and-aggregate-state-floor"),
        ("detector_proof", "post-invocation-parent-challenge-v1"),
        ("detector_source_preflight", "exact-module-call-graph-v1"),
        ("execution_contract", "rafter-soak-execution-v1"),
        ("finalization_reserve", "10m"),
        ("kill_confirmation_timeout", "5s"),
        ("layer_timeout", "170m"),
        ("liveness_report_binding", "typed-canonical-json-sha256-v3"),
        ("model_profile", "raft-nightly"),
        ("model_timeout_policy", "remaining-layer-budget"),
        ("receipt_finalization_allowance", "5s"),
        ("seed_count", "6"),
        ("seed_policy", "source-derived-sha256-v1"),
        ("soak_steps", "1024"),
        ("state_floors", "100000000-protocol-and-verifier"),
        ("termination_grace", "30s"),
        ("canonical_check_binding", "scheduled-suffix-v1"),
    ]);
    let contract: SimulatorRunnerConfiguration = serde_json::from_value(
        serde_json::to_value(configuration).expect("configuration serializes"),
    )
    .expect("typed contract deserializes");

    assert_eq!(contract.seed_count, Some(6));
    assert_eq!(contract.soak_steps, 1024);
    assert_eq!(
        contract.state_floors,
        SimulatorStateFloors::Aggregate {
            protocol: 100_000_000,
            verifier: 100_000_000,
        }
    );
    contract
        .validate_profile("nightly")
        .expect("nightly contract");
}

#[test]
fn simulator_contract_rejects_unknown_and_misplaced_fields() {
    let unknown = serde_json::json!({
        "build": "release-and-test-locked",
        "compile_timeout": "10m",
        "completion": "frontier-and-semantic-floor",
        "execution_contract": "rafter-soak-execution-v1",
        "finalization_reserve": "3m",
        "kill_confirmation_timeout": "5s",
        "layer_timeout": "25m",
        "liveness_report_binding": "typed-canonical-json-sha256-v3",
        "model_profile": "fast+raft-soak",
        "model_timeout_policy": "remaining-layer-budget",
        "receipt_finalization_allowance": "5s",
        "seed_policy": "curated-0x9103-through-0x9106",
        "snapshot_catchup_probe": "required",
        "soak_steps": "320",
        "state_floors": "per-evidence",
        "termination_grace": "30s",
        "unreviewed": "true"
    });
    assert!(serde_json::from_value::<SimulatorRunnerConfiguration>(unknown).is_err());
}

#[test]
fn simulator_check_contract_rejects_unknown_fields() {
    let unknown = serde_json::json!({
        "minimum_protocol_states": 1,
        "minimum_verifier_states": 1,
        "required_observations": ["well_formed_states_checked"],
        "unreviewed": true
    });
    assert!(serde_json::from_value::<SimulatorCheckContract>(unknown).is_err());
}

#[test]
fn checked_in_pr_contract_has_the_exact_source_fast_inventory() {
    let (catalog, manifest) = crate::tests::loaded();
    manifest
        .validate(&catalog)
        .expect("checked-in profile contract");
    let configured = manifest.profiles["pr"].runners["simulator"]
        .simulator_checks
        .keys()
        .cloned()
        .collect::<BTreeSet<_>>();
    assert_eq!(configured, source_fast_check_ids());
}

#[test]
fn pr_contract_rejects_missing_and_extra_fast_checks() {
    let (catalog, manifest) = crate::tests::loaded();
    let mut missing = manifest.clone();
    missing
        .profiles
        .get_mut("pr")
        .expect("PR profile")
        .runners
        .get_mut("simulator")
        .expect("simulator runner")
        .simulator_checks
        .remove("raft-lease-read");
    assert!(missing.validate(&catalog).is_err());

    let mut extra = manifest;
    let contract =
        extra.profiles["pr"].runners["simulator"].simulator_checks["raft-election"].clone();
    extra
        .profiles
        .get_mut("pr")
        .expect("PR profile")
        .runners
        .get_mut("simulator")
        .expect("simulator runner")
        .simulator_checks
        .insert("raft-unreviewed".to_owned(), contract);
    assert!(extra.validate(&catalog).is_err());
}

#[test]
fn pr_contract_rejects_nonpositive_floors_and_invalid_observations() {
    let (catalog, manifest) = crate::tests::loaded();
    let mutations: [fn(&mut SimulatorCheckContract); 5] = [
        |contract: &mut SimulatorCheckContract| contract.minimum_protocol_states = 0,
        |contract: &mut SimulatorCheckContract| contract.minimum_verifier_states = 0,
        |contract: &mut SimulatorCheckContract| contract.required_observations.clear(),
        |contract: &mut SimulatorCheckContract| {
            contract.required_observations = vec!["duplicate".to_owned(), "duplicate".to_owned()];
        },
        |contract: &mut SimulatorCheckContract| {
            contract.required_observations = vec!["  ".to_owned()];
        },
    ];
    for mutate in mutations {
        let mut invalid = manifest.clone();
        let contract = invalid
            .profiles
            .get_mut("pr")
            .expect("PR profile")
            .runners
            .get_mut("simulator")
            .expect("simulator runner")
            .simulator_checks
            .get_mut("raft-election")
            .expect("election contract");
        mutate(contract);
        assert!(invalid.validate(&catalog).is_err());
    }
}

#[test]
fn simulator_check_contracts_are_rejected_outside_the_pr_simulator_runner() {
    let (catalog, manifest) = crate::tests::loaded();
    let contract =
        manifest.profiles["pr"].runners["simulator"].simulator_checks["raft-election"].clone();
    for (profile, layer) in [("pr", "tests"), ("nightly", "simulator")] {
        let mut misplaced = manifest.clone();
        misplaced
            .profiles
            .get_mut(profile)
            .expect("profile")
            .runners
            .get_mut(layer)
            .expect("runner")
            .simulator_checks
            .insert("raft-election".to_owned(), contract.clone());
        assert!(misplaced.validate(&catalog).is_err());
    }
}

#[test]
fn specialized_pr_checks_cannot_drop_their_purpose_observations() {
    let (catalog, manifest) = crate::tests::loaded();
    for (check_id, observation) in PR_SPECIALIZED_OBSERVATIONS {
        let mut invalid = manifest.clone();
        invalid
            .profiles
            .get_mut("pr")
            .expect("PR profile")
            .runners
            .get_mut("simulator")
            .expect("simulator runner")
            .simulator_checks
            .get_mut(check_id)
            .expect("specialized check contract")
            .required_observations
            .retain(|required| required != observation);
        let error = invalid
            .validate(&catalog)
            .expect_err("specialized purpose observation is required")
            .to_string();
        assert!(error.contains(observation), "unexpected error: {error}");
    }
}

fn source_fast_check_ids() -> BTreeSet<String> {
    struct CheckVisitor(BTreeSet<String>);

    impl<'ast> Visit<'ast> for CheckVisitor {
        fn visit_expr_call(&mut self, call: &'ast ExprCall) {
            let Expr::Path(function) = call.func.as_ref() else {
                syn::visit::visit_expr_call(self, call);
                return;
            };
            if function
                .path
                .segments
                .last()
                .is_some_and(|segment| segment.ident == "run_raft_check")
            {
                if let Some(Expr::Lit(literal)) = call.args.first() {
                    if let Lit::Str(check_id) = &literal.lit {
                        self.0.insert(check_id.value());
                    }
                }
            }
            syn::visit::visit_expr_call(self, call);
        }
    }

    let source =
        include_str!("../../../rafter-sim/src/bin/rafter_model_check_fast/runner/checks.rs");
    let file = syn::parse_file(source).expect("fast runner parses as Rust");
    let function = file
        .items
        .iter()
        .find_map(|item| match item {
            Item::Fn(function) if function.sig.ident == "run_fast_profile" => Some(function),
            _ => None,
        })
        .expect("fast runner function");
    let mut visitor = CheckVisitor(BTreeSet::new());
    visitor.visit_block(&function.block);
    let reviewed = PR_FAST_CHECK_IDS
        .into_iter()
        .map(str::to_owned)
        .collect::<BTreeSet<_>>();
    assert_eq!(visitor.0, reviewed);
    visitor.0
}

#[test]
fn simulator_contract_rejects_weakened_pr_thresholds() {
    let mut contract = reviewed_pr_contract();
    contract
        .validate_profile("pr")
        .expect("reviewed PR thresholds");

    contract.soak_steps = 319;
    assert!(contract.validate_profile("pr").is_err());

    let mut contract = reviewed_pr_contract();
    contract.state_floors = SimulatorStateFloors::Aggregate {
        protocol: 1,
        verifier: 1,
    };
    assert!(contract.validate_profile("pr").is_err());

    let mut contract = reviewed_pr_contract();
    contract.detector_proof = "textual-witness-v0".to_owned();
    assert!(contract.validate_profile("pr").is_err());
}

#[test]
fn simulator_contract_rejects_weakened_nightly_thresholds() {
    assert_weakened_scheduled_thresholds_are_rejected(
        "nightly",
        reviewed_scheduled_contract("nightly", 1_024, 6, 100_000_000),
    );
}

#[test]
fn simulator_contract_rejects_weakened_weekly_thresholds() {
    assert_weakened_scheduled_thresholds_are_rejected(
        "weekly",
        reviewed_scheduled_contract("weekly", 4_096, 10, 250_000_000),
    );
}

fn assert_weakened_scheduled_thresholds_are_rejected(
    profile: &str,
    contract: SimulatorRunnerConfiguration,
) {
    contract
        .validate_profile(profile)
        .expect("reviewed scheduled thresholds");

    let mut weakened = contract.clone();
    weakened.soak_steps -= 1;
    assert!(weakened.validate_profile(profile).is_err());

    let mut weakened = contract.clone();
    weakened.seed_count = weakened.seed_count.map(|count| count - 1);
    assert!(weakened.validate_profile(profile).is_err());

    let mut weakened = contract.clone();
    let SimulatorStateFloors::Aggregate {
        protocol,
        verifier: _,
    } = &mut weakened.state_floors
    else {
        panic!("scheduled fixture uses aggregate floors");
    };
    *protocol -= 1;
    assert!(weakened.validate_profile(profile).is_err());

    let mut weakened = contract;
    let SimulatorStateFloors::Aggregate {
        protocol: _,
        verifier,
    } = &mut weakened.state_floors
    else {
        panic!("scheduled fixture uses aggregate floors");
    };
    *verifier -= 1;
    assert!(weakened.validate_profile(profile).is_err());
}

fn reviewed_pr_contract() -> SimulatorRunnerConfiguration {
    SimulatorRunnerConfiguration {
        build: "release-and-test-locked".to_owned(),
        compile_timeout: "10m".to_owned(),
        completion: "frontier-and-semantic-floor".to_owned(),
        detector_proof: "post-invocation-parent-challenge-v1".to_owned(),
        detector_source_preflight: "exact-module-call-graph-v1".to_owned(),
        execution_contract: "rafter-soak-execution-v1".to_owned(),
        finalization_reserve: "3m".to_owned(),
        kill_confirmation_timeout: "5s".to_owned(),
        layer_timeout: "25m".to_owned(),
        liveness_report_binding: "typed-canonical-json-sha256-v3".to_owned(),
        model_profile: "fast+raft-soak".to_owned(),
        model_timeout_policy: "remaining-layer-budget".to_owned(),
        receipt_finalization_allowance: "5s".to_owned(),
        seed_policy: "curated-0x9103-through-0x9106".to_owned(),
        seed_count: None,
        snapshot_catchup_probe: Some("required".to_owned()),
        soak_steps: 320,
        state_floors: SimulatorStateFloors::PerEvidence,
        termination_grace: "30s".to_owned(),
        canonical_check_binding: None,
    }
}

fn reviewed_scheduled_contract(
    profile: &str,
    soak_steps: u64,
    seed_count: u64,
    state_floor: u64,
) -> SimulatorRunnerConfiguration {
    SimulatorRunnerConfiguration {
        build: "release-and-test-locked".to_owned(),
        compile_timeout: "10m".to_owned(),
        completion: "frontier-and-aggregate-state-floor".to_owned(),
        detector_proof: "post-invocation-parent-challenge-v1".to_owned(),
        detector_source_preflight: "exact-module-call-graph-v1".to_owned(),
        execution_contract: "rafter-soak-execution-v1".to_owned(),
        finalization_reserve: "10m".to_owned(),
        kill_confirmation_timeout: "5s".to_owned(),
        layer_timeout: if profile == "nightly" { "170m" } else { "340m" }.to_owned(),
        liveness_report_binding: "typed-canonical-json-sha256-v3".to_owned(),
        model_profile: format!("raft-{profile}"),
        model_timeout_policy: "remaining-layer-budget".to_owned(),
        receipt_finalization_allowance: "5s".to_owned(),
        seed_policy: "source-derived-sha256-v1".to_owned(),
        seed_count: Some(seed_count),
        snapshot_catchup_probe: None,
        soak_steps,
        state_floors: SimulatorStateFloors::Aggregate {
            protocol: state_floor,
            verifier: state_floor,
        },
        termination_grace: "30s".to_owned(),
        canonical_check_binding: Some("scheduled-suffix-v1".to_owned()),
    }
}
