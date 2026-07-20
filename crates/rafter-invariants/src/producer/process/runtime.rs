//! Evidence adaptation for the neutral process-runtime inventory.

use std::{collections::BTreeMap, error::Error};

use crate::{evidence::ExecutableReceipt, execution::process::capture_runtime_identities};

pub(crate) fn capture_runtime_receipts(
    environment: &BTreeMap<String, String>,
    include_bash: bool,
) -> Result<BTreeMap<String, ExecutableReceipt>, Box<dyn Error>> {
    Ok(capture_runtime_identities(environment, include_bash)?
        .into_iter()
        .map(|(runtime, identity)| {
            (
                runtime,
                ExecutableReceipt {
                    program: identity.path.to_string_lossy().into_owned(),
                    sha256: identity.sha256,
                },
            )
        })
        .collect())
}
