use std::collections::{BTreeMap, BTreeSet};

use super::{
    binding::compiler_identity,
    function_index::{CallTarget, FunctionId, FunctionIndex},
    CallableArgument, DetectorInvocationContract, FunctionCall, FunctionEvent, FunctionFacts,
    FunctionFallthrough, InvocationCall, SourceDefect,
};

pub(super) fn expand_reachable_fixture(
    functions: &FunctionIndex,
    target_graph: &crate::rust_target::TargetSourceGraph,
    fixture_function: &FunctionId,
    registered_function: &FunctionId,
    crate_name: &str,
    declarations: &BTreeMap<String, Vec<String>>,
    contract: &mut DetectorInvocationContract,
) -> Result<bool, String> {
    Reachability {
        functions,
        target_graph,
        registered_function,
        crate_name,
        declarations,
        contract,
    }
    .expand(fixture_function, true, &mut Vec::new(), None, true)
    .map(|fallthrough| fallthrough.may)
}

struct Reachability<'a> {
    functions: &'a FunctionIndex,
    target_graph: &'a crate::rust_target::TargetSourceGraph,
    registered_function: &'a FunctionId,
    crate_name: &'a str,
    declarations: &'a BTreeMap<String, Vec<String>>,
    contract: &'a mut DetectorInvocationContract,
}

#[derive(Clone, Copy)]
struct Fallthrough {
    may: bool,
    guaranteed: bool,
}

impl Fallthrough {
    const fn guaranteed() -> Self {
        Self {
            may: true,
            guaranteed: true,
        }
    }

    const fn none() -> Self {
        Self {
            may: false,
            guaranteed: false,
        }
    }

    const fn conditional() -> Self {
        Self {
            may: true,
            guaranteed: false,
        }
    }

    const fn and(self, other: Self) -> Self {
        Self {
            may: self.may && other.may,
            guaranteed: self.guaranteed && other.guaranteed,
        }
    }

    const fn from_facts(facts: &FunctionFacts) -> Self {
        match facts.fallthrough {
            FunctionFallthrough::Never => Self {
                may: false,
                guaranteed: false,
            },
            FunctionFallthrough::Conditional => Self {
                may: true,
                guaranteed: false,
            },
            FunctionFallthrough::Guaranteed => Self {
                may: true,
                guaranteed: true,
            },
        }
    }
}

impl Reachability<'_> {
    fn expand(
        &mut self,
        function: &FunctionId,
        guaranteed_path: bool,
        stack: &mut Vec<FunctionId>,
        bound_arguments: Option<&[super::CallableArgument]>,
        exact_dispatch: bool,
    ) -> Result<Fallthrough, String> {
        self.target_graph
            .require_unshadowed_oracle_macros(&function.to_string())?;
        let Some(facts) = self.functions.unique_exact(function)? else {
            return Ok(Fallthrough::guaranteed());
        };
        if let Some(falls_through) =
            self.recursive_fallthrough(function, guaranteed_path, exact_dispatch, stack)?
        {
            return Ok(falls_through);
        }
        require_clean_function(
            facts,
            function,
            &self.registered_function.name,
            guaranteed_path,
        )?;

        stack.push(function.clone());
        for target in &facts.potential_callable_arguments {
            let Some(callable) = self.functions.resolve_call(target)? else {
                continue;
            };
            self.expand(&callable, false, stack, None, true)?;
        }
        let mut path_guaranteed = guaranteed_path;
        let mut fallthrough = Fallthrough::guaranteed();
        for event in &facts.events {
            match event {
                FunctionEvent::Call { call, guaranteed } => {
                    let call_guaranteed = path_guaranteed && *guaranteed;
                    let call_falls_through =
                        self.expand_call(function, call, call_guaranteed, stack, bound_arguments)?;
                    if !call_falls_through.may {
                        if call_guaranteed {
                            fallthrough = Fallthrough::none();
                            break;
                        }
                        path_guaranteed = false;
                        fallthrough.guaranteed = false;
                    } else if !call_falls_through.guaranteed {
                        path_guaranteed = false;
                        fallthrough.guaranteed = false;
                    }
                }
                FunctionEvent::Invocation {
                    invocation,
                    guaranteed,
                } => {
                    if !path_guaranteed || !*guaranteed {
                        return Err(conditional_invocation_error(function));
                    }
                    record_invocation_witness(
                        self.functions,
                        invocation,
                        self.registered_function,
                        self.crate_name,
                        self.declarations,
                        self.contract,
                    )?;
                }
            }
        }
        stack.pop();
        Ok(fallthrough.and(Fallthrough::from_facts(facts)))
    }

    fn expand_call(
        &mut self,
        function: &FunctionId,
        call: &FunctionCall,
        call_guaranteed: bool,
        stack: &mut Vec<FunctionId>,
        bound_arguments: Option<&[CallableArgument]>,
    ) -> Result<Fallthrough, String> {
        let target = &call.target;
        self.functions.require_function_namespace(target)?;
        let called = self.functions.matching_functions(target);
        if called.is_empty() {
            reject_unresolved_control_flow_call(target, function, call_guaranteed)?;
            return Ok(Fallthrough::guaranteed());
        }

        let exact_dispatch = called.len() == 1 && !target.imprecise_dispatch;
        let arguments = resolve_callable_arguments(&call.arguments, bound_arguments);
        let mut call_falls_through = Fallthrough::guaranteed();
        for called in called {
            let arguments_fall_through =
                self.validate_callable_arguments(&called, &arguments, call_guaranteed, stack)?;
            let called_falls_through = self.expand(
                &called,
                call_guaranteed,
                stack,
                Some(&arguments),
                exact_dispatch,
            )?;
            call_falls_through = call_falls_through
                .and(arguments_fall_through)
                .and(called_falls_through);
        }
        Ok(call_falls_through)
    }

    fn recursive_fallthrough(
        &self,
        function: &FunctionId,
        guaranteed_path: bool,
        exact_dispatch: bool,
        stack: &[FunctionId],
    ) -> Result<Option<Fallthrough>, String> {
        let Some(cycle_start) = stack.iter().position(|active| active == function) else {
            return Ok(None);
        };
        if stack[cycle_start..]
            .iter()
            .any(|function| self.function_can_emit_invocation(function, &mut BTreeSet::new()))
        {
            return Err(format!(
                "negative fixture call graph is recursive through `{function}` and can emit an invocation-bound witness"
            ));
        }
        Ok(Some(if guaranteed_path && exact_dispatch {
            Fallthrough::none()
        } else {
            Fallthrough::conditional()
        }))
    }

    fn function_can_emit_invocation(
        &self,
        function: &FunctionId,
        visiting: &mut BTreeSet<FunctionId>,
    ) -> bool {
        if !visiting.insert(function.clone()) {
            return false;
        }
        let emits = self
            .functions
            .unique_exact(function)
            .ok()
            .flatten()
            .is_some_and(|facts| {
                facts
                    .events
                    .iter()
                    .any(|event| matches!(event, FunctionEvent::Invocation { .. }))
                    || facts
                        .events
                        .iter()
                        .filter_map(|event| match event {
                            FunctionEvent::Call { call, .. } => Some(&call.target),
                            FunctionEvent::Invocation { .. } => None,
                        })
                        .chain(facts.potential_callable_arguments.iter())
                        .flat_map(|target| self.functions.matching_functions(target))
                        .any(|called| self.function_can_emit_invocation(&called, visiting))
            });
        visiting.remove(function);
        emits
    }

    fn validate_callable_arguments(
        &mut self,
        called: &FunctionId,
        arguments: &[super::CallableArgument],
        guaranteed_path: bool,
        stack: &mut Vec<FunctionId>,
    ) -> Result<Fallthrough, String> {
        let Some(facts) = self.functions.unique_exact(called)? else {
            return Ok(Fallthrough::guaranteed());
        };
        let mut fallthrough = Fallthrough::guaranteed();
        for (index, guaranteed) in facts
            .guaranteed_called_parameters
            .iter()
            .map(|index| (*index, true))
            .chain(
                facts
                    .conditional_called_parameters
                    .iter()
                    .map(|index| (*index, false)),
            )
        {
            match arguments.get(index) {
                Some(super::CallableArgument::InlineClosure) => {}
                Some(super::CallableArgument::Known(target)) => {
                    if target.matches_any_name(super::FORBIDDEN_CALLS) {
                        return Err(format!(
                            "negative fixture passes forbidden callable `{}` into `{called}`",
                            target.name
                        ));
                    }
                    self.functions.require_function_namespace(target)?;
                    let candidates = self.functions.matching_functions(target);
                    if candidates.is_empty() {
                        return Err(format!(
                            "negative fixture passes unresolved callable `{}` into `{called}`",
                            target.name
                        ));
                    }
                    let exact_dispatch = candidates.len() == 1 && !target.imprecise_dispatch;
                    let mut callable_fallthrough = Fallthrough::guaranteed();
                    for candidate in candidates {
                        let candidate_falls_through = self.expand(
                            &candidate,
                            guaranteed_path && guaranteed,
                            stack,
                            None,
                            exact_dispatch,
                        )?;
                        callable_fallthrough = callable_fallthrough.and(candidate_falls_through);
                    }
                    if guaranteed {
                        fallthrough = fallthrough.and(callable_fallthrough);
                    } else if !callable_fallthrough.guaranteed {
                        fallthrough.guaranteed = false;
                    }
                }
                Some(super::CallableArgument::Opaque) | None => {
                    return Err(format!(
                        "negative fixture passes an unresolved callable argument into `{called}`"
                    ));
                }
                Some(super::CallableArgument::Parameter(_)) => {
                    return Err(format!(
                        "negative fixture passes an unbound callable parameter into `{called}`"
                    ));
                }
            }
        }
        Ok(fallthrough)
    }
}

fn resolve_callable_arguments(
    arguments: &[super::CallableArgument],
    bound_arguments: Option<&[super::CallableArgument]>,
) -> Vec<super::CallableArgument> {
    arguments
        .iter()
        .map(|argument| match argument {
            super::CallableArgument::Parameter(index) => bound_arguments
                .and_then(|arguments| arguments.get(*index))
                .cloned()
                .unwrap_or(super::CallableArgument::Opaque),
            argument => argument.clone(),
        })
        .collect()
}

fn reject_unresolved_control_flow_call(
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

fn record_invocation_witness(
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

fn require_clean_function(
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

fn conditional_invocation_error(function: &FunctionId) -> String {
    format!(
        "negative fixture reaches an invocation-bound oracle macro only through conditional control flow in `{function}`"
    )
}
