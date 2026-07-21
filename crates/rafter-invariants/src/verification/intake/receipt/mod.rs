//! Semantic acceptance of source-bound runner receipts.

mod checks;
mod collection;
mod execution;
mod runner;
mod structure;

pub(super) use collection::{collect_results, ReceiptExpectation};
