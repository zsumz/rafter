use rafter_sim::{model_check::FailureKind, SimSeed};

use super::{failure_timeline_lines, parse_profile, Profile, ProfileRun, ProfileSelection};

#[test]
fn default_profile_stays_fast_for_ci() {
    assert_eq!(
        parse_profile(Vec::<String>::new()).expect("default profile parses"),
        ProfileSelection::Run(ProfileRun {
            profile: Profile::Fast,
            seed_override: None,
        })
    );
}

#[test]
fn explicit_profiles_parse() {
    assert_eq!(
        parse_profile(["--profile", "raft-deep"]).expect("flag profile parses"),
        ProfileSelection::Run(ProfileRun {
            profile: Profile::RaftDeep,
            seed_override: None,
        })
    );
    assert_eq!(
        parse_profile(["raft-soak"]).expect("bare profile parses"),
        ProfileSelection::Run(ProfileRun {
            profile: Profile::RaftSoak,
            seed_override: None,
        })
    );
    assert_eq!(
        parse_profile(["--profile", "raft-nightly"]).expect("nightly profile parses"),
        ProfileSelection::Run(ProfileRun {
            profile: Profile::RaftNightly,
            seed_override: None,
        })
    );
    assert_eq!(
        parse_profile(["raft-weekly"]).expect("weekly profile parses"),
        ProfileSelection::Run(ProfileRun {
            profile: Profile::RaftWeekly,
            seed_override: None,
        })
    );
    assert_eq!(
        parse_profile(["--list-profiles"]).expect("list flag parses"),
        ProfileSelection::List
    );
}

#[test]
fn explicit_replay_seeds_parse() {
    assert_eq!(
        parse_profile(["--profile", "raft-nightly", "--seed", "0x1234,5678"])
            .expect("seeded nightly profile parses"),
        ProfileSelection::Run(ProfileRun {
            profile: Profile::RaftNightly,
            seed_override: Some(vec![SimSeed(0x1234), SimSeed(5678)]),
        })
    );
    assert_eq!(
        parse_profile(["raft-weekly", "--seed", "0xabc"]).expect("seeded weekly profile parses"),
        ProfileSelection::Run(ProfileRun {
            profile: Profile::RaftWeekly,
            seed_override: Some(vec![SimSeed(0xabc)]),
        })
    );
}

#[test]
fn replay_seed_errors_are_not_silent_noops() {
    let err = parse_profile(["--seed", "0x1234"]).expect_err("default fast profile rejects seeds");
    assert!(err
        .to_string()
        .contains("only applies to profiles with soak workloads"));

    let err = parse_profile(["raft-soak", "--seed", "0x1234,,0x5678"])
        .expect_err("empty seed item is rejected");
    assert!(err.to_string().contains("seed list contains an empty seed"));
}

#[test]
fn failure_timeline_lines_include_failure_and_trace_context() {
    let lines = failure_timeline_lines(
        "raft-commit",
        FailureKind::InvariantViolation,
        "commit_safety",
        "committed prefix diverged",
        [
            (0, "tick n1".to_string()),
            (1, "deliver n1->n2".to_string()),
        ],
    );

    assert_eq!(
        lines,
        vec![
            "ERROR test model failure name=raft-commit failure_kind=invariant-violation invariant=commit_safety error_message=\"committed prefix diverged\"",
            "DEBUG test trace step step=0 action=\"tick n1\"",
            "DEBUG test trace step step=1 action=\"deliver n1->n2\"",
        ]
    );
}
