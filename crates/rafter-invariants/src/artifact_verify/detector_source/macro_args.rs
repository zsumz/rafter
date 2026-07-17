use syn::{
    parse::{Parse, ParseStream, Parser},
    punctuated::Punctuated,
    Expr, Macro, Pat, Token,
};

pub(super) fn expressions(name: &str, invocation: &Macro) -> Option<Vec<Expr>> {
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
