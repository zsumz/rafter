//! `syn::Visit` dispatch into function-body analysis domains.

use syn::{
    visit::Visit, Block, ExprAssign, ExprAsync, ExprBinary, ExprBreak, ExprCall, ExprClosure,
    ExprContinue, ExprForLoop, ExprIf, ExprLoop, ExprMatch, ExprMethodCall, ExprReturn, ExprTry,
    ExprUnsafe, ExprWhile, ItemConst, ItemFn, ItemStatic, Local, Macro, PatIdent, PatType,
};

use super::FunctionBodyVisitor;

impl<'ast> Visit<'ast> for FunctionBodyVisitor<'_> {
    fn visit_expr_unsafe(&mut self, expression: &'ast ExprUnsafe) {
        self.analyze_unsafe(expression);
    }

    fn visit_block(&mut self, block: &'ast Block) {
        self.analyze_block(block);
    }

    fn visit_expr_call(&mut self, call: &'ast ExprCall) {
        self.analyze_call(call);
    }

    fn visit_expr_method_call(&mut self, call: &'ast ExprMethodCall) {
        self.analyze_method_call(call);
    }

    fn visit_local(&mut self, local: &'ast Local) {
        self.analyze_local(local);
    }

    fn visit_expr_assign(&mut self, expression: &'ast ExprAssign) {
        self.analyze_assignment(expression);
    }

    fn visit_macro(&mut self, invocation: &'ast Macro) {
        self.analyze_macro(invocation);
    }

    fn visit_pat_ident(&mut self, pattern: &'ast PatIdent) {
        self.analyze_pattern_binding(pattern);
    }

    fn visit_pat_type(&mut self, pattern: &'ast PatType) {
        self.analyze_typed_pattern(pattern);
    }

    fn visit_item_const(&mut self, item: &'ast ItemConst) {
        self.analyze_const(item);
    }

    fn visit_item_static(&mut self, item: &'ast ItemStatic) {
        self.analyze_static(item);
    }

    fn visit_expr_try(&mut self, expression: &'ast ExprTry) {
        self.analyze_try(expression);
    }

    fn visit_expr_return(&mut self, expression: &'ast ExprReturn) {
        self.analyze_return(expression);
    }

    fn visit_expr_break(&mut self, expression: &'ast ExprBreak) {
        self.analyze_break(expression);
    }

    fn visit_expr_continue(&mut self, expression: &'ast ExprContinue) {
        self.analyze_continue(expression);
    }

    fn visit_expr_if(&mut self, expression: &'ast ExprIf) {
        self.analyze_if(expression);
    }

    fn visit_expr_match(&mut self, expression: &'ast ExprMatch) {
        self.analyze_match(expression);
    }

    fn visit_expr_for_loop(&mut self, expression: &'ast ExprForLoop) {
        self.analyze_for_loop(expression);
    }

    fn visit_expr_while(&mut self, expression: &'ast ExprWhile) {
        self.analyze_while(expression);
    }

    fn visit_expr_loop(&mut self, expression: &'ast ExprLoop) {
        self.analyze_loop(expression);
    }

    fn visit_expr_closure(&mut self, expression: &'ast ExprClosure) {
        self.analyze_closure(expression);
    }

    fn visit_expr_async(&mut self, expression: &'ast ExprAsync) {
        self.analyze_async(expression);
    }

    fn visit_expr_binary(&mut self, expression: &'ast ExprBinary) {
        self.analyze_binary(expression);
    }

    fn visit_item_fn(&mut self, _function: &'ast ItemFn) {}

    fn visit_item_macro(&mut self, item: &'ast syn::ItemMacro) {
        self.record_macro_declaration(item);
    }
}
