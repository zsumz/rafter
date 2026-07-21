//! Recursive call-graph expansion and witness reachability.

use std::collections::{BTreeMap, BTreeSet};

mod fallthrough;
mod policy;

use super::{
    function_index::{FunctionId, FunctionIndex},
    CallableArgument, DetectorInvocationContract, FunctionCall, FunctionEvent,
};
use fallthrough::Fallthrough;
use policy::{
    conditional_invocation_error, record_invocation_witness, reject_unresolved_control_flow_call,
    require_clean_function, resolve_callable_arguments,
};

pub(super) fn expand_reachable_fixture(
    functions: &FunctionIndex,
    target_graph: &crate::verification::target::TargetSourceGraph,
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
    target_graph: &'a crate::verification::target::TargetSourceGraph,
    registered_function: &'a FunctionId,
    crate_name: &'a str,
    declarations: &'a BTreeMap<String, Vec<String>>,
    contract: &'a mut DetectorInvocationContract,
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
