//! Scenario: the manual TLC runner reproduces the profile CI actually runs.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
};

/// Tiers of `scripts/tla-model-check` that reproduce a wired profile, and the
/// profile each one reproduces. The unwired tiers are listed separately: they
/// are manual explorations with no profile to agree with.
const WIRED_TLA_RUNNER_TIERS: [(&str, &str); 3] = [
    ("--ci", "pr"),
    ("--nightly", "nightly"),
    ("--full", "weekly"),
];
const UNWIRED_TLA_RUNNER_TIERS: [&str; 4] = [
    "--joint-quorum",
    "--joint-quorum-focused-next",
    "--joint-quorum-focused-init",
    "--trace-sample",
];

/// Settings that decide what TLC actually explores. Anything here that differs
/// between the script and the manifest makes the documented manual runner
/// reproduce a different run than CI performs, silently.
const TLA_RUNNER_TIER_SETTINGS: [&str; 5] = ["config", "workers", "seed", "fp_mem", "max_heap"];

/// The manual runner and the profile manifest describe the same TLC
/// invocations, and the manifest is the one CI executes. Drift here does not
/// fail anything -- it just means a local reproduction is quietly not the run
/// being reproduced. The weekly tier sat at a 4g heap for a release after the
/// profile moved to 8g, which is the difference between draining the
/// unsymmetrized snapshot obligation and never draining it.
#[test]
fn tla_runner_tiers_agree_with_the_profile_manifest() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let source = fs::read_to_string(root.join("scripts/tla-model-check")).expect("read TLA runner");
    let manifest =
        crate::ProfileManifest::load(&root.join("verification/raft-invariant-profiles.json"))
            .expect("load profile manifest");

    let declared = tla_runner_tier_flags(&source);
    let reviewed = WIRED_TLA_RUNNER_TIERS
        .iter()
        .map(|(flag, _)| (*flag).to_owned())
        .chain(
            UNWIRED_TLA_RUNNER_TIERS
                .iter()
                .map(|flag| (*flag).to_owned()),
        )
        .collect::<BTreeSet<_>>();
    assert_eq!(
        declared, reviewed,
        "TLA runner tier inventory changed; classify the new tier as wired or unwired"
    );

    // No flag runs the PR tier, so the defaults are a fourth copy of `--ci`.
    let defaults = tla_runner_tier(&source, None);
    assert_eq!(
        defaults,
        tla_runner_tier(&source, Some("--ci")),
        "the TLA runner defaults drifted from its --ci tier"
    );

    for (flag, profile) in WIRED_TLA_RUNNER_TIERS {
        let tier = tla_runner_tier(&source, Some(flag));
        let configuration = &manifest
            .profiles
            .get(profile)
            .unwrap_or_else(|| panic!("{profile} profile"))
            .runners
            .get("tla")
            .unwrap_or_else(|| panic!("{profile} TLA+ runner"))
            .configuration;
        for setting in TLA_RUNNER_TIER_SETTINGS {
            assert_eq!(
                tier.get(setting).map(String::as_str),
                configuration.get(setting).map(String::as_str),
                "scripts/tla-model-check {flag} disagrees with the {profile} profile on {setting}"
            );
        }
    }
}

/// Every `--flag)` arm of the runner's option parser.
fn tla_runner_tier_flags(source: &str) -> BTreeSet<String> {
    source
        .lines()
        .filter_map(|line| line.trim().strip_suffix(')'))
        .filter(|flag| flag.starts_with("--"))
        .filter(|flag| !matches!(*flag, "--fetch-tool" | "--print-config"))
        .map(str::to_owned)
        .collect()
}

/// The `name=value` assignments a tier applies, quotes stripped. `None` reads
/// the script's defaults: the assignments above the option loop.
fn tla_runner_tier(source: &str, flag: Option<&str>) -> BTreeMap<String, String> {
    let body = if let Some(flag) = flag {
        let opened = format!("        {flag})\n");
        let start = source
            .find(&opened)
            .unwrap_or_else(|| panic!("TLA runner tier {flag} is missing"))
            + opened.len();
        let end = source[start..]
            .find("            ;;")
            .unwrap_or_else(|| panic!("TLA runner tier {flag} is unterminated"));
        &source[start..start + end]
    } else {
        let end = source
            .find("\nusage()")
            .expect("TLA runner defaults precede its usage helper");
        &source[..end]
    };
    body.lines()
        .filter_map(|line| line.trim().split_once('='))
        .filter(|(name, _)| TLA_RUNNER_TIER_SETTINGS.contains(name))
        .map(|(name, value)| (name.to_owned(), value.trim_matches('"').to_owned()))
        .collect()
}
