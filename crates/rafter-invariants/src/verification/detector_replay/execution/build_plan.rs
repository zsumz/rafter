//! Cargo compilation selection derived from the reviewed replay inventory.

use std::{collections::BTreeSet, ffi::OsString};

use super::super::DetectorReplayPlan;

#[derive(Debug)]
pub(super) struct ReplayBuildPlan {
    packages: Vec<String>,
    target: CargoTargetSelection,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CargoTargetSelection {
    Library,
}

impl ReplayBuildPlan {
    pub(super) fn derive(replay: &DetectorReplayPlan) -> Result<Self, String> {
        if replay.targets().is_empty() {
            return Err("detector replay build plan has no targets".to_owned());
        }
        let mut packages = BTreeSet::new();
        let mut target = None;
        for replay_target in replay.targets().keys() {
            let selection = CargoTargetSelection::parse(&replay_target.kind)?;
            if target
                .replace(selection)
                .is_some_and(|value| value != selection)
            {
                return Err("detector replay build plan mixes Cargo target kinds".to_owned());
            }
            if !packages.insert(replay_target.package.clone()) {
                return Err(format!(
                    "detector replay build plan contains multiple {} targets for package {}",
                    replay_target.kind, replay_target.package
                ));
            }
        }
        Ok(Self {
            packages: packages.into_iter().collect(),
            target: target.ok_or("detector replay build plan has no target selector")?,
        })
    }

    pub(super) fn cargo_arguments(&self) -> Vec<OsString> {
        let mut arguments = Vec::with_capacity(self.packages.len() * 2 + 1);
        for package in &self.packages {
            arguments.push(OsString::from("-p"));
            arguments.push(OsString::from(package));
        }
        arguments.push(self.target.cargo_argument().into());
        arguments
    }
}

impl CargoTargetSelection {
    fn parse(kind: &str) -> Result<Self, String> {
        match kind {
            "lib" => Ok(Self::Library),
            unsupported => Err(format!(
                "detector replay build plan does not support Cargo target kind {unsupported}"
            )),
        }
    }

    const fn cargo_argument(self) -> &'static str {
        match self {
            Self::Library => "--lib",
        }
    }
}

#[cfg(test)]
#[path = "build_plan_tests.rs"]
mod tests;
