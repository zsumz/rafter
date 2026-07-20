//! Function-call binding, callable aliases, and method-target resolution.

use syn::{visit::Visit, Expr, ExprCall, ExprMethodCall};

use super::FunctionBodyVisitor;
use crate::verification::detector::source::{
    function_index::CallTarget,
    model::{CallableArgument, FunctionCall, FunctionEvent, SourceDefect},
    policy::FORBIDDEN_CALLS,
    syntax::{unqualified_called_function, unqualified_expression_name},
};

impl FunctionBodyVisitor<'_> {
    fn record_call(&mut self, target: CallTarget, arguments: Vec<CallableArgument>) {
        if target.matches_any_name(FORBIDDEN_CALLS) {
            self.facts.defects.insert(SourceDefect::ForbiddenWitness);
        }
        let call = FunctionCall { target, arguments };
        self.facts.events.push(FunctionEvent::Call {
            call,
            guaranteed: self.guaranteed && !self.statement.may_exit,
        });
    }

    fn callable_arguments<'ast>(
        &self,
        arguments: impl IntoIterator<Item = &'ast Expr>,
    ) -> Vec<CallableArgument> {
        arguments
            .into_iter()
            .map(|argument| self.callable_argument(argument))
            .collect()
    }

    fn callable_argument(&self, argument: &Expr) -> CallableArgument {
        match argument {
            Expr::Closure(_) => CallableArgument::InlineClosure,
            Expr::Group(group) => self.callable_argument(&group.expr),
            Expr::Paren(paren) => self.callable_argument(&paren.expr),
            Expr::Reference(reference) => self.callable_argument(&reference.expr),
            Expr::Path(_) => {
                if let Some(index) = unqualified_expression_name(argument)
                    .and_then(|name| self.parameter_bindings.get(&name))
                {
                    return CallableArgument::Parameter(*index);
                }
                self.alias_target(argument)
                    .map_or(CallableArgument::Opaque, CallableArgument::Known)
            }
            _ => self
                .alias_target(argument)
                .map_or(CallableArgument::Opaque, CallableArgument::Known),
        }
    }

    fn record_potential_callable_arguments<'ast>(
        &mut self,
        arguments: impl IntoIterator<Item = &'ast Expr>,
    ) {
        let targets = arguments
            .into_iter()
            .filter_map(|argument| self.alias_target(argument))
            .collect::<Vec<_>>();
        self.facts.potential_callable_arguments.extend(targets);
    }

    pub(super) fn call_target(&self, call: &ExprCall) -> Option<CallTarget> {
        if let Some(name) = unqualified_called_function(call) {
            if let Some(alias) = self.value_aliases.get(&name).cloned() {
                return Some(alias);
            }
            if self.value_bindings.contains(&name) {
                return None;
            }
        }
        self.expression_target(&call.func)
    }

    fn expression_target(&self, expression: &Expr) -> Option<CallTarget> {
        if let Some(alias) = unqualified_expression_name(expression)
            .and_then(|name| self.value_aliases.get(&name).cloned())
        {
            return Some(alias);
        }
        if let Some(target) = self.resolver.self_target(expression, self.self_type) {
            return Some(target);
        }
        if let Some(target) = self
            .scoped_resolver
            .explicit_target(expression, self.module)
        {
            return Some(self.resolver.classify_target(target));
        }
        let target = match (
            self.resolver.call_target(expression, self.module),
            self.scoped_resolver.call_target(expression, self.module),
        ) {
            (Some(module), Some(scoped)) => module.merge(scoped),
            (Some(target), None) | (None, Some(target)) => target,
            (None, None) => return None,
        };
        Some(self.resolver.classify_target(target))
    }

    pub(super) fn alias_target(&self, expression: &Expr) -> Option<CallTarget> {
        let target = self.expression_target(expression)?;
        (self.resolver.can_name_reviewed_function(&target)
            || target.matches_any_name(FORBIDDEN_CALLS))
        .then_some(target)
    }

    pub(super) fn analyze_call(&mut self, call: &ExprCall) {
        let target = self.call_target(call);
        let arguments = self.callable_arguments(&call.args);
        self.record_potential_callable_arguments(&call.args);
        self.visit_expr(&call.func);
        for argument in &call.args {
            self.visit_expr(argument);
        }
        if let Some(target) = target {
            self.record_call(target, arguments);
        } else if let Some(index) =
            unqualified_called_function(call).and_then(|name| self.parameter_bindings.get(&name))
        {
            if self.guaranteed && !self.statement.may_exit {
                self.facts.guaranteed_called_parameters.insert(*index);
            } else {
                self.facts.conditional_called_parameters.insert(*index);
            }
        } else {
            self.facts.defects.insert(SourceDefect::OpaqueCallable);
        }
    }

    pub(super) fn analyze_method_call(&mut self, call: &ExprMethodCall) {
        let receiver_type = unqualified_expression_name(&call.receiver).and_then(|name| {
            if name == "self" {
                self.self_type
            } else {
                self.value_types.get(&name).map(Vec::as_slice)
            }
        });
        let receiver_type = receiver_type
            .map(<[String]>::to_vec)
            .or_else(|| {
                self.resolver.field_expression_type_module(
                    &call.receiver,
                    self.module,
                    self.self_type,
                    &self.value_types,
                )
            })
            .or_else(|| {
                self.scoped_resolver.field_expression_type_module(
                    &call.receiver,
                    self.module,
                    self.self_type,
                    &self.value_types,
                )
            });
        let module_target = self.resolver.method_target(
            &call.receiver,
            &call.method.to_string(),
            self.module,
            receiver_type.as_deref(),
        );
        let scoped_target = self.scoped_resolver.method_target(
            &call.receiver,
            &call.method.to_string(),
            self.module,
            receiver_type.as_deref(),
        );
        let target = self
            .resolver
            .classify_target(module_target.merge(scoped_target));
        let arguments = self.callable_arguments(&call.args);
        self.record_potential_callable_arguments(&call.args);
        self.visit_expr(&call.receiver);
        for argument in &call.args {
            self.visit_expr(argument);
        }
        self.record_call(target, arguments);
    }
}
