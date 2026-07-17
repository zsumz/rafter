use syn::{Block, Expr, ExprCall, ExprLoop, Macro, Stmt};

pub(super) fn statement_unconditionally_exits(statement: &Stmt) -> bool {
    matches!(
        statement,
        Stmt::Expr(Expr::Return(_) | Expr::Break(_) | Expr::Continue(_), _)
    )
}

pub(super) fn block_end_may_complete_normally(block: &Block) -> bool {
    block
        .stmts
        .last()
        .is_none_or(statement_may_complete_normally)
}

pub(super) fn statement_may_complete_normally(statement: &Stmt) -> bool {
    match statement {
        Stmt::Expr(expression, _) => expression_may_complete_normally(expression),
        Stmt::Macro(invocation) => macro_may_complete_normally(&invocation.mac),
        Stmt::Local(_) | Stmt::Item(_) => true,
    }
}

fn expression_may_complete_normally(expression: &Expr) -> bool {
    match expression {
        Expr::Return(_) => false,
        Expr::Macro(invocation) => macro_may_complete_normally(&invocation.mac),
        Expr::Block(block) => block_end_may_complete_normally(&block.block),
        Expr::Group(group) => expression_may_complete_normally(&group.expr),
        Expr::Loop(expression) => loop_may_complete_normally(expression),
        Expr::Paren(paren) => expression_may_complete_normally(&paren.expr),
        _ => true,
    }
}

pub(super) fn loop_may_complete_normally(expression: &ExprLoop) -> bool {
    block_guarantees_break(
        &expression.body,
        expression.label.as_ref().map(|label| &label.name),
    )
}

fn block_guarantees_break(block: &Block, loop_label: Option<&syn::Lifetime>) -> bool {
    for statement in &block.stmts {
        let Stmt::Expr(expression, _) = statement else {
            continue;
        };
        if expression_guarantees_break(expression, loop_label) {
            return true;
        }
        if !expression_may_complete_normally(expression) {
            return false;
        }
    }
    false
}

fn expression_guarantees_break(expression: &Expr, loop_label: Option<&syn::Lifetime>) -> bool {
    match expression {
        Expr::Break(expression) => match (&expression.label, loop_label) {
            (None, _) => true,
            (Some(actual), Some(expected)) => actual.ident == expected.ident,
            (Some(_), None) => false,
        },
        Expr::Block(expression) => block_guarantees_break(&expression.block, loop_label),
        Expr::Group(expression) => expression_guarantees_break(&expression.expr, loop_label),
        Expr::If(expression) => {
            let literal_condition = match expression.cond.as_ref() {
                Expr::Lit(literal) => match &literal.lit {
                    syn::Lit::Bool(value) => Some(value.value),
                    _ => None,
                },
                _ => None,
            };
            let then_breaks = block_guarantees_break(&expression.then_branch, loop_label);
            let else_breaks = expression
                .else_branch
                .as_ref()
                .is_some_and(|(_, expression)| expression_guarantees_break(expression, loop_label));
            match literal_condition {
                Some(true) => then_breaks,
                Some(false) => else_breaks,
                None => then_breaks && else_breaks,
            }
        }
        Expr::Paren(expression) => expression_guarantees_break(&expression.expr, loop_label),
        _ => false,
    }
}

fn macro_may_complete_normally(invocation: &Macro) -> bool {
    invocation.path.segments.last().is_none_or(|segment| {
        !matches!(segment.ident.to_string().as_str(), "panic" | "unreachable")
    })
}

pub(super) fn unqualified_called_function(call: &ExprCall) -> Option<String> {
    unqualified_expression_name(&call.func)
}

pub(super) fn unqualified_expression_name(expression: &Expr) -> Option<String> {
    let path = match expression {
        Expr::Path(path) => path,
        Expr::Group(group) => return unqualified_expression_name(&group.expr),
        Expr::Paren(paren) => return unqualified_expression_name(&paren.expr),
        _ => return None,
    };
    if path.qself.is_some() || path.path.leading_colon.is_some() || path.path.segments.len() != 1 {
        return None;
    }
    path.path
        .segments
        .first()
        .map(|segment| segment.ident.to_string())
}
