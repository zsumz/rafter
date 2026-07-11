use rafter_sim::SimSeed;

use super::{CliError, Profile, ProfileRun, ProfileSelection};

pub(crate) fn parse_profile<I>(args: I) -> Result<ProfileSelection, CliError>
where
    I: IntoIterator,
    I::Item: Into<String>,
{
    let args = args.into_iter().map(Into::into).collect::<Vec<_>>();
    let mut profile = None;
    let mut seeds = Vec::new();
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--list-profiles" if args.len() == 1 => return Ok(ProfileSelection::List),
            "--list-profiles" => {
                return Err(CliError(
                    "`--list-profiles` cannot be combined with other arguments".to_string(),
                ));
            }
            "--profile" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(usage());
                };
                if profile.replace(profile_by_name(value)?).is_some() {
                    return Err(CliError(
                        "model-check profile specified more than once".to_string(),
                    ));
                }
            }
            "--seed" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    return Err(usage());
                };
                seeds.extend(parse_seed_list(value)?);
            }
            value if !value.starts_with('-') => {
                if profile.replace(profile_by_name(value)?).is_some() {
                    return Err(CliError(
                        "model-check profile specified more than once".to_string(),
                    ));
                }
            }
            _ => return Err(usage()),
        }
        index += 1;
    }
    let profile = profile.unwrap_or(Profile::Fast);
    let seed_override = (!seeds.is_empty()).then_some(seeds);
    if seed_override.is_some() && profile == Profile::Fast {
        return Err(CliError(
            "`--seed` only applies to profiles with soak workloads".to_string(),
        ));
    }
    Ok(ProfileSelection::Run(ProfileRun {
        profile,
        seed_override,
    }))
}

fn profile_by_name(value: &str) -> Result<Profile, CliError> {
    match value {
        "fast" => Ok(Profile::Fast),
        "raft-deep" => Ok(Profile::RaftDeep),
        "raft-soak" => Ok(Profile::RaftSoak),
        "raft-nightly" => Ok(Profile::RaftNightly),
        "raft-weekly" => Ok(Profile::RaftWeekly),
        _ => Err(CliError(format!("unknown model-check profile `{value}`"))),
    }
}

fn parse_seed_list(value: &str) -> Result<Vec<SimSeed>, CliError> {
    value.split(',').map(parse_seed).collect()
}

fn parse_seed(value: &str) -> Result<SimSeed, CliError> {
    let value = value.trim();
    if value.is_empty() {
        return Err(CliError(
            "model-check seed list contains an empty seed".to_string(),
        ));
    }
    let parsed = if let Some(hex) = value.strip_prefix("0x") {
        u64::from_str_radix(hex, 16)
    } else {
        value.parse()
    }
    .map_err(|_| CliError(format!("invalid model-check seed `{value}`")))?;
    Ok(SimSeed(parsed))
}

fn usage() -> CliError {
    CliError(
        "usage: rafter-model-check-fast [--list-profiles | [--profile] <fast|raft-deep|raft-soak|raft-nightly|raft-weekly> [--seed <seed>[,<seed>...]]]"
            .to_string(),
    )
}
