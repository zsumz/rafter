//! Fail-closed callable, invocation, and source-defect policy.

use std::collections::{BTreeMap, BTreeSet};

use super::super::{
    binding::compiler_identity,
    function_index::{CallTarget, FunctionId, FunctionIndex},
    CallableArgument, DetectorInvocationContract, FunctionEvent, FunctionFacts, InvocationCall,
    SourceDefect,
};

pub(super) fn resolve_callable_arguments(
    arguments: &[CallableArgument],
    bound_arguments: Option<&[CallableArgument]>,
) -> Vec<CallableArgument> {
    arguments
        .iter()
        .map(|argument| match argument {
            CallableArgument::Parameter(index) => bound_arguments
                .and_then(|arguments| arguments.get(*index))
                .cloned()
                .unwrap_or(CallableArgument::Opaque),
            argument => argument.clone(),
        })
        .collect()
}

pub(super) fn reject_unresolved_control_flow_call(
    target: &CallTarget,
    function: &FunctionId,
    call_guaranteed: bool,
) -> Result<(), String> {
    if target.opaque_local_module {
        let qualifier = if call_guaranteed {
            ""
        } else {
            "conditionally "
        };
        return Err(format!(
            "negative fixture {qualifier}reaches unresolved local call `{}` through `{function}`",
            target.name
        ));
    }
    if target.matches_any_name(&["exec"]) {
        return Err(format!(
            "negative fixture reaches unresolved process-replacement call `{}` through `{function}`",
            target.name
        ));
    }
    Ok(())
}

pub(super) fn record_invocation_witness(
    functions: &FunctionIndex,
    invocation: &InvocationCall,
    registered_function: &FunctionId,
    crate_name: &str,
    declarations: &BTreeMap<String, Vec<String>>,
    contract: &mut DetectorInvocationContract,
) -> Result<(), String> {
    let called = resolve_invocation(functions, &invocation.target, crate_name, declarations)?;
    let identity = compiler_identity(crate_name, &called);
    if &called == registered_function && identity != contract.registered_identity {
        return Err(format!(
            "registered detector `{called}` has inconsistent compiler identity `{identity}`"
        ));
    }
    *contract
        .witnesses
        .entry(format!("{}:{identity}", invocation.kind.label()))
        .or_default() += 1;
    Ok(())
}

fn resolve_invocation(
    functions: &FunctionIndex,
    target: &CallTarget,
    crate_name: &str,
    declarations: &BTreeMap<String, Vec<String>>,
) -> Result<FunctionId, String> {
    if let Some(called) = functions.resolve_call(target)? {
        return Ok(called);
    }
    let matches = target
        .candidates()
        .iter()
        .filter(|candidate| {
            declarations.get(&candidate.name).is_some_and(|identities| {
                identities.contains(&compiler_identity(crate_name, candidate))
            })
        })
        .cloned()
        .collect::<BTreeSet<_>>();
    let count = matches.len();
    let mut matches = matches.into_iter();
    match (matches.next(), matches.next()) {
        (Some(called), None) => Ok(called),
        _ => Err(format!(
            "invocation-bound call `{}` resolves to {count} bound source declarations",
            target.name
        )),
    }
}

pub(super) fn require_clean_function(
    facts: &FunctionFacts,
    function: &FunctionId,
    detector: &str,
    guaranteed_path: bool,
) -> Result<(), String> {
    if facts.untrusted_attributes {
        return Err(format!(
            "negative fixture reaches untrusted semantic attributes through `{function}`"
        ));
    }
    if facts.defects.contains(&SourceDefect::ForbiddenWitness) {
        return Err(format!(
            "negative fixture can emit an arbitrary detector witness through `{function}`"
        ));
    }
    if facts.defects.contains(&SourceDefect::UntrustedOracleMacro) {
        return Err(format!(
            "negative fixture invokes an untrusted oracle macro through `{function}`"
        ));
    }
    if facts.shadowed_values.contains(detector) {
        return Err(format!(
            "negative fixture shadows registered detector `{detector}` through `{function}`"
        ));
    }
    if facts
        .defects
        .contains(&SourceDefect::MalformedInvocationMacro)
    {
        return Err(format!(
            "negative fixture has a malformed invocation-bound oracle macro through `{function}`"
        ));
    }
    if facts.defects.contains(&SourceDefect::OpaqueMacro) {
        return Err(format!(
            "negative fixture reaches an opaque macro through `{function}`"
        ));
    }
    if facts.defects.contains(&SourceDefect::OpaqueCallable) {
        return Err(format!(
            "negative fixture reaches an unresolved local callable through `{function}`"
        ));
    }
    if facts.defects.contains(&SourceDefect::UnsafeCapability) {
        return Err(format!(
            "negative fixture reaches unsafe or foreign code through `{function}`"
        ));
    }
    let guaranteed_invocation = facts.events.iter().any(|event| {
        matches!(
            event,
            FunctionEvent::Invocation {
                guaranteed: true,
                ..
            }
        )
    });
    let conditional_invocation = facts.events.iter().any(|event| {
        matches!(
            event,
            FunctionEvent::Invocation {
                guaranteed: false,
                ..
            }
        )
    });
    if conditional_invocation || !guaranteed_path && guaranteed_invocation {
        return Err(conditional_invocation_error(function));
    }
    Ok(())
}

pub(super) fn conditional_invocation_error(function: &FunctionId) -> String {
    format!(
        "negative fixture reaches an invocation-bound oracle macro only through conditional control flow in `{function}`"
    )
}
