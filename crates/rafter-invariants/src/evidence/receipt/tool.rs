//! Producer-image and external-tool identity receipts.

use serde::{Deserialize, Serialize};

/// Canonical path and digest for one executable in the process runtime.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutableReceipt {
    pub program: String,
    pub sha256: String,
}

/// Version and executable digest for a non-Rust evidence tool.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ToolReceipt {
    pub version: String,
    pub sha256: String,
}
