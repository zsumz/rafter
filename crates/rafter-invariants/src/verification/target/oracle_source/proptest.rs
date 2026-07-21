//! Conservative extraction of test declarations from `proptest!` input tokens.

use proc_macro2::{Delimiter, Group, TokenStream, TokenTree};

pub(super) struct GeneratedFunction {
    pub(super) name: String,
    pub(super) test_attribute: bool,
    pub(super) should_panic: bool,
    pub(super) body: Group,
}

pub(super) fn generated_functions(tokens: &TokenStream) -> Vec<GeneratedFunction> {
    let tokens = tokens.clone().into_iter().collect::<Vec<_>>();
    let mut attributes = Vec::<String>::new();
    let mut generated = Vec::new();
    let mut index = 0;
    while index < tokens.len() {
        if matches!(&tokens[index], TokenTree::Punct(punct) if punct.as_char() == '#') {
            let mut group_index = index + 1;
            if matches!(tokens.get(group_index), Some(TokenTree::Punct(punct)) if punct.as_char() == '!')
            {
                group_index += 1;
            }
            if let Some(TokenTree::Group(group)) = tokens.get(group_index) {
                if group.delimiter() == Delimiter::Bracket {
                    attributes.push(group.stream().to_string());
                    index = group_index + 1;
                    continue;
                }
            }
        }
        let is_fn = matches!(&tokens[index], TokenTree::Ident(identifier) if identifier == "fn");
        if is_fn {
            let name = tokens.get(index + 1).and_then(|token| match token {
                TokenTree::Ident(identifier) => Some(identifier.to_string()),
                _ => None,
            });
            let body = tokens[index + 2..].iter().find_map(|token| match token {
                TokenTree::Group(group) if group.delimiter() == Delimiter::Brace => {
                    Some(group.clone())
                }
                _ => None,
            });
            if let (Some(name), Some(body)) = (name, body) {
                generated.push(GeneratedFunction {
                    name,
                    test_attribute: attributes.iter().any(|attribute| attribute == "test"),
                    should_panic: attributes
                        .iter()
                        .any(|attribute| attribute.starts_with("should_panic")),
                    body,
                });
            }
            attributes.clear();
        }
        index += 1;
    }
    generated
}
