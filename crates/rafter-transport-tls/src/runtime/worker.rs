//! Panic-visible owned worker execution.

use std::{
    panic::{self, AssertUnwindSafe},
    sync::Arc,
};

use super::RuntimeControl;

pub(crate) fn run_guarded(control: &Arc<RuntimeControl>, role: &str, operation: impl FnOnce()) {
    match panic::catch_unwind(AssertUnwindSafe(operation)) {
        Ok(()) => {}
        Err(payload) => {
            control.fail(format!("owned transport worker {role} panicked"));
            panic::resume_unwind(payload);
        }
    }
}
