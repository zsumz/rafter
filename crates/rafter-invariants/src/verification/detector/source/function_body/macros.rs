//! Trusted macro parsing and invocation-bound witness analysis.

use syn::{
    parse::{Parse, ParseStream, Parser},
    punctuated::Punctuated,
    visit::Visit,
    Expr, Macro, Pat, Token,
};

use super::FunctionBodyVisitor;
use crate::verification::detector::source::{
    function_index::CallTarget,
    imports::{safe_builtin_macro, trusted_oracle_macro},
    model::{FunctionEvent, InvocationCall, InvocationKind, SourceDefect},
    policy::{FORBIDDEN_CALLS, INVOCATION_MACROS, ORACLE_MACROS, TOKEN_ONLY_MACROS},
};

impl FunctionBodyVisitor<'_> {
    pub(super) fn analyze_macro(&mut self, invocation: &Macro) {
        let name = invocation
            .path
            .segments
            .last()
            .map(|segment| segment.ident.to_string())
            .unwrap_or_default();
        if self.reject_forbidden_macro(&name) {
            return;
        }
        let arguments = expressions(&name, invocation);
        if self.analyze_invocation_macro(&name, invocation, arguments.as_deref()) {
            return;
        }
        self.analyze_regular_macro(&name, invocation, arguments.as_deref());
    }

    fn reject_forbidden_macro(&mut self, name: &str) -> bool {
        if name == "panic" || name == "unreachable" {
            self.statement.may_exit = true;
        }
        if name == "oracle_detector_witness" || name == "oracle_fabricated_detector_witness" {
            self.facts.defects.insert(SourceDefect::ForbiddenWitness);
            return true;
        }
        if matches!(
            name,
            "eprint" | "eprintln" | "print" | "println" | "write" | "writeln" | "dbg"
        ) {
            self.facts.defects.insert(SourceDefect::ForbiddenWitness);
            return true;
        }
        if self.imports.local_macros.contains(name) {
            self.facts.defects.insert(SourceDefect::OpaqueMacro);
            return true;
        }
        false
    }

    fn analyze_invocation_macro(
        &mut self,
        name: &str,
        invocation: &Macro,
        arguments: Option<&[Expr]>,
    ) -> bool {
        if !INVOCATION_MACROS.contains(&name) {
            return false;
        }
        if !trusted_oracle_macro(&invocation.path, &self.imports) {
            self.facts
                .defects
                .insert(SourceDefect::UntrustedOracleMacro);
            return true;
        }
        let Some(arguments) = arguments else {
            self.facts
                .defects
                .insert(SourceDefect::MalformedInvocationMacro);
            return true;
        };
        let expected_arguments = if name == "oracle_expect_err" { 2 } else { 1 };
        if arguments.len() != expected_arguments {
            self.facts
                .defects
                .insert(SourceDefect::MalformedInvocationMacro);
            return true;
        }
        let Some(Expr::Call(call)) = arguments.first() else {
            self.facts
                .defects
                .insert(SourceDefect::MalformedInvocationMacro);
            return true;
        };
        if !matches!(call.func.as_ref(), Expr::Path(path)
            if path.qself.is_none()
                && path.path.leading_colon.is_none()
                && path.path.segments.len() == 1)
        {
            self.facts
                .defects
                .insert(SourceDefect::MalformedInvocationMacro);
            return true;
        }
        let Some(target) = self.call_target(call) else {
            self.facts
                .defects
                .insert(SourceDefect::MalformedInvocationMacro);
            return true;
        };
        let kind = if name == "oracle_expect_err" {
            InvocationKind::ExpectErr
        } else {
            InvocationKind::Recorder
        };
        for argument in arguments.iter().skip(1) {
            self.visit_expr(argument);
        }
        self.visit_expr(&call.func);
        for argument in &call.args {
            self.visit_expr(argument);
        }
        self.record_invocation(kind, target);
        true
    }

    fn analyze_regular_macro(
        &mut self,
        name: &str,
        invocation: &Macro,
        arguments: Option<&[Expr]>,
    ) {
        if ORACLE_MACROS.contains(&name) {
            if !trusted_oracle_macro(&invocation.path, &self.imports) {
                self.facts
                    .defects
                    .insert(SourceDefect::UntrustedOracleMacro);
                return;
            }
        } else if !safe_builtin_macro(&invocation.path, &self.imports) {
            self.facts.defects.insert(SourceDefect::OpaqueMacro);
            return;
        }
        if TOKEN_ONLY_MACROS.contains(&name) {
            return;
        }
        let Some(arguments) = arguments else {
            self.facts.defects.insert(SourceDefect::OpaqueMacro);
            return;
        };
        self.with_guarantee(false, |visitor| {
            for argument in arguments {
                visitor.visit_expr(argument);
            }
        });
    }

    fn record_invocation(&mut self, kind: InvocationKind, target: CallTarget) {
        if target.matches_any_name(FORBIDDEN_CALLS) {
            self.facts.defects.insert(SourceDefect::ForbiddenWitness);
        }
        let invocation = InvocationCall { kind, target };
        self.facts.events.push(FunctionEvent::Invocation {
            invocation,
            guaranteed: self.guaranteed && !self.statement.may_exit,
        });
    }
}

fn expressions(name: &str, invocation: &Macro) -> Option<Vec<Expr>> {
    match name {
        "matches" => syn::parse2::<MatchesMacroArguments>(invocation.tokens.clone())
            .ok()
            .map(|arguments| {
                std::iter::once(arguments.expression)
                    .chain(arguments.guard)
                    .collect()
            }),
        "vec" => syn::parse2::<VecMacroArguments>(invocation.tokens.clone())
            .ok()
            .map(|arguments| arguments.expressions),
        _ => Punctuated::<Expr, Token![,]>::parse_terminated
            .parse2(invocation.tokens.clone())
            .ok()
            .map(Punctuated::into_iter)
            .map(Iterator::collect),
    }
}

struct MatchesMacroArguments {
    expression: Expr,
    guard: Option<Expr>,
}

impl Parse for MatchesMacroArguments {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let expression = input.parse()?;
        input.parse::<Token![,]>()?;
        Pat::parse_multi_with_leading_vert(input)?;
        let guard = if input.peek(Token![if]) {
            input.parse::<Token![if]>()?;
            Some(input.parse()?)
        } else {
            None
        };
        input.parse::<Option<Token![,]>>()?;
        if !input.is_empty() {
            return Err(input.error("unexpected matches! arguments"));
        }
        Ok(Self { expression, guard })
    }
}

struct VecMacroArguments {
    expressions: Vec<Expr>,
}

impl Parse for VecMacroArguments {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        if input.is_empty() {
            return Ok(Self {
                expressions: Vec::new(),
            });
        }
        let first = input.parse()?;
        if input.peek(Token![;]) {
            input.parse::<Token![;]>()?;
            let length = input.parse()?;
            if !input.is_empty() {
                return Err(input.error("unexpected vec! repeat arguments"));
            }
            return Ok(Self {
                expressions: vec![first, length],
            });
        }
        let mut expressions = vec![first];
        while !input.is_empty() {
            input.parse::<Token![,]>()?;
            if input.is_empty() {
                break;
            }
            expressions.push(input.parse()?);
        }
        Ok(Self { expressions })
    }
}
