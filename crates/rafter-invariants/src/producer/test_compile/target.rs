//! Cargo target identity and selector construction.

use std::{error::Error, ffi::OsString};

use crate::contract::TestIdentity;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(in crate::producer) struct Target {
    pub(super) package: String,
    pub(super) kind: String,
    pub(super) name: String,
}

impl From<&TestIdentity> for Target {
    fn from(identity: &TestIdentity) -> Self {
        Self {
            package: identity.package.clone(),
            kind: identity.target_kind.clone(),
            name: identity.target.clone(),
        }
    }
}

impl Target {
    pub(super) fn key(&self) -> String {
        format!("{}/{}/{}", self.package, self.kind, self.name)
    }

    pub(super) fn selector(&self) -> Result<Vec<OsString>, Box<dyn Error>> {
        match self.kind.as_str() {
            "lib" => Ok(vec!["--lib".into()]),
            "test" => Ok(vec!["--test".into(), self.name.clone().into()]),
            "bin" => Ok(vec!["--bin".into(), self.name.clone().into()]),
            kind => Err(format!("unsupported Cargo target kind {kind}").into()),
        }
    }
}
