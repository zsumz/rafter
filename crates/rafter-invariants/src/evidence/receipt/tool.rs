//! Producer-image and external-tool identity receipts.

use serde::{Deserialize, Serialize};

/// Version and executable digest for a non-Rust evidence tool.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ToolReceipt {
    pub version: String,
    pub sha256: String,
}
