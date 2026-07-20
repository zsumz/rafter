//! Conservative control-flow transitions for function-body source analysis.

use syn::{
    visit::{self, Visit},
    BinOp, ExprAsync, ExprBinary, ExprBreak, ExprClosure, ExprContinue, ExprForLoop, ExprIf,
    ExprLoop, ExprMatch, ExprReturn, ExprTry, ExprUnsafe, ExprWhile,
};

use super::FunctionBodyVisitor;
use crate::verification::detector::source::{
    control_flow::PathReachability, model::SourceDefect, syntax::loop_may_complete_normally,
};

impl FunctionBodyVisitor<'_> {
    pub(super) fn analyze_unsafe(&mut self, expression: &ExprUnsafe) {
        self.facts.defects.insert(SourceDefect::UnsafeCapability);
        visit::visit_expr_unsafe(self, expression);
    }

    pub(super) fn analyze_try(&mut self, expression: &ExprTry) {
        self.visit_expr(&expression.expr);
        self.statement.may_exit = true;
    }

    pub(super) fn analyze_return(&mut self, expression: &ExprReturn) {
        if let Some(value) = &expression.expr {
            self.visit_expr(value);
        }
        self.function.normal_return_seen |=
            self.reachability.is_reachable() && !self.statement.may_exit;
        self.statement.may_exit = true;
    }

    pub(super) fn analyze_break(&mut self, expression: &ExprBreak) {
        if let Some(value) = &expression.expr {
            self.visit_expr(value);
        }
        self.statement.may_exit = true;
    }

    pub(super) fn analyze_continue(&mut self, _expression: &ExprContinue) {
        self.statement.may_exit = true;
    }

    pub(super) fn analyze_if(&mut self, expression: &ExprIf) {
        self.visit_expr(&expression.cond);
        self.with_guarantee(false, |visitor| {
            visitor.visit_block(&expression.then_branch);
        });
        if let Some((_, otherwise)) = &expression.else_branch {
            self.with_guarantee(false, |visitor| visitor.visit_expr(otherwise));
        }
    }

    pub(super) fn analyze_match(&mut self, expression: &ExprMatch) {
        self.visit_expr(&expression.expr);
        for arm in &expression.arms {
            self.visit_pat(&arm.pat);
            self.with_guarantee(false, |visitor| {
                if let Some((_, guard)) = &arm.guard {
                    visitor.visit_expr(guard);
                }
                visitor.visit_expr(&arm.body);
            });
        }
    }

    pub(super) fn analyze_for_loop(&mut self, expression: &ExprForLoop) {
        self.visit_pat(&expression.pat);
        self.visit_expr(&expression.expr);
        self.with_guarantee(false, |visitor| visitor.visit_block(&expression.body));
    }

    pub(super) fn analyze_while(&mut self, expression: &ExprWhile) {
        self.with_guarantee(false, |visitor| {
            visitor.visit_expr(&expression.cond);
            visitor.visit_block(&expression.body);
        });
    }

    pub(super) fn analyze_loop(&mut self, expression: &ExprLoop) {
        let previous_may_exit = self.statement.may_exit;
        let previous_may_diverge = self.statement.may_diverge;
        self.statement.may_exit = false;
        self.statement.may_diverge = false;
        self.with_guarantee(false, |visitor| visitor.visit_block(&expression.body));
        let loop_may_complete = loop_may_complete_normally(expression);
        self.statement.may_exit = previous_may_exit || !loop_may_complete;
        self.statement.may_diverge =
            previous_may_diverge || self.statement.may_diverge || !loop_may_complete;
    }

    pub(super) fn analyze_closure(&mut self, expression: &ExprClosure) {
        let previous_may_exit = self.statement.may_exit;
        let previous_may_diverge = self.statement.may_diverge;
        let previous_function_may_diverge = self.function.may_diverge;
        let previous_return_seen = self.function.normal_return_seen;
        let previous_reachability = self.reachability;
        let previous_aliases = self.value_aliases.clone();
        let previous_bindings = self.value_bindings.clone();
        let previous_types = self.value_types.clone();
        self.statement.may_exit = false;
        self.statement.may_diverge = false;
        self.function.may_diverge = false;
        self.function.normal_return_seen = false;
        self.reachability = PathReachability::Reachable;
        for input in &expression.inputs {
            self.visit_pat(input);
        }
        self.with_guarantee(false, |visitor| visitor.visit_expr(&expression.body));
        self.value_aliases = previous_aliases;
        self.value_bindings = previous_bindings;
        self.value_types = previous_types;
        self.statement.may_exit = previous_may_exit;
        self.statement.may_diverge = previous_may_diverge;
        self.function.may_diverge = previous_function_may_diverge;
        self.function.normal_return_seen = previous_return_seen;
        self.reachability = previous_reachability;
    }

    pub(super) fn analyze_async(&mut self, expression: &ExprAsync) {
        self.with_guarantee(false, |visitor| visitor.visit_block(&expression.block));
    }

    pub(super) fn analyze_binary(&mut self, expression: &ExprBinary) {
        self.visit_expr(&expression.left);
        if matches!(expression.op, BinOp::And(_) | BinOp::Or(_)) {
            self.with_guarantee(false, |visitor| visitor.visit_expr(&expression.right));
        } else {
            self.visit_expr(&expression.right);
        }
    }
}
