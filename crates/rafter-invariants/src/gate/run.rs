//! Execution of one source-bound invariant evidence producer.

use std::{error::Error, path::Path};

use crate::{
    plan::{CapturedInvocation, ExecutionPlan},
    producer::{produce_with_plan, ProducerOutcome},
};

pub(super) fn produce_layer(
    plan: &ExecutionPlan,
    layer: &str,
    output_dir: &Path,
    invocation: &CapturedInvocation,
) -> Result<ProducerOutcome, Box<dyn Error>> {
    produce_with_plan(plan, layer, output_dir, invocation)
}
