//! Conservative scanning of macro-generated Rust source inputs.

use std::collections::HashMap;

use syn::Macro;

use super::is_include_name;

mod tokens;

use tokens::{macro_tokens, MacroToken};

pub(super) fn contains_include_invocation(
    invocation: &Macro,
    aliases: &HashMap<String, String>,
) -> bool {
    let values = macro_tokens(invocation);
    values.windows(2).any(|window| {
        matches!(&window[0], MacroToken::Ident(ident)
            if is_include_name(ident) || aliases.contains_key(ident))
            && window[1] == MacroToken::Punct('!')
    })
}

pub(super) fn contains_include_reference(
    invocation: &Macro,
    aliases: &HashMap<String, String>,
) -> bool {
    macro_tokens(invocation).iter().any(|token| {
        matches!(token, MacroToken::Ident(ident)
            if is_include_name(ident) || aliases.contains_key(ident))
    })
}

pub(super) fn contains_dynamic_macro_invocation(invocation: &Macro) -> bool {
    let values = macro_tokens(invocation);
    values.windows(3).any(|window| {
        window[0] == MacroToken::Punct('$')
            && matches!(&window[1], MacroToken::Ident(_))
            && window[2] == MacroToken::Punct('!')
    })
}

pub(super) fn contains_generated_out_of_line_module(invocation: &Macro) -> bool {
    let values = macro_tokens(invocation);
    values.windows(2).enumerate().any(|(index, window)| {
        if window != [MacroToken::Punct('='), MacroToken::Punct('>')] {
            return false;
        }
        let transcriber = index + 2;
        let Some(end) = matching_delimiter(&values, transcriber) else {
            return false;
        };
        contains_out_of_line_module_tokens(&values[transcriber + 1..end])
    })
}

pub(super) fn contains_out_of_line_module_argument(invocation: &Macro) -> bool {
    contains_out_of_line_module_tokens(&macro_tokens(invocation))
}

fn matching_delimiter(tokens: &[MacroToken], start: usize) -> Option<usize> {
    let MacroToken::Punct(opening @ ('(' | '[' | '{')) = tokens.get(start)? else {
        return None;
    };
    let closing = match opening {
        '(' => ')',
        '[' => ']',
        '{' => '}',
        _ => unreachable!(),
    };
    let mut depth = 0_usize;
    for (offset, token) in tokens[start..].iter().enumerate() {
        match token {
            MacroToken::Punct(value) if value == opening => depth += 1,
            MacroToken::Punct(value) if *value == closing => {
                depth -= 1;
                if depth == 0 {
                    return Some(start + offset);
                }
            }
            _ => {}
        }
    }
    None
}

fn contains_out_of_line_module_tokens(tokens: &[MacroToken]) -> bool {
    tokens.iter().enumerate().any(|(index, token)| {
        if !matches!(token, MacroToken::Ident(ident) if ident == "mod") {
            return false;
        }
        let out_of_line = matches!(
            tokens.get(index + 1..index + 3),
            Some([MacroToken::Ident(_), MacroToken::Punct(';')])
        ) || matches!(
            tokens.get(index + 1..index + 4),
            Some([
                MacroToken::Punct('$'),
                MacroToken::Ident(_),
                MacroToken::Punct(';')
            ])
        );
        out_of_line && !module_is_definitively_inactive(tokens, index)
    })
}

fn module_is_definitively_inactive(tokens: &[MacroToken], module: usize) -> bool {
    let mut index = 0;
    while index < module {
        if tokens.get(index) != Some(&MacroToken::Punct('#'))
            || tokens.get(index + 1) != Some(&MacroToken::Punct('['))
        {
            index += 1;
            continue;
        }

        let mut cursor = index;
        let mut inactive = false;
        while tokens.get(cursor) == Some(&MacroToken::Punct('#'))
            && tokens.get(cursor + 1) == Some(&MacroToken::Punct('['))
        {
            let Some(end) = matching_delimiter(tokens, cursor + 1) else {
                break;
            };
            inactive |= attribute_definitively_disables(&tokens[cursor + 2..end]);
            cursor = end + 1;
        }
        cursor = skip_visibility(tokens, cursor);
        if cursor == module {
            return inactive;
        }
        index += 1;
    }
    false
}

fn skip_visibility(tokens: &[MacroToken], index: usize) -> usize {
    if !matches!(tokens.get(index), Some(MacroToken::Ident(ident)) if ident == "pub") {
        return index;
    }
    if tokens.get(index + 1) == Some(&MacroToken::Punct('(')) {
        matching_delimiter(tokens, index + 1).map_or(index + 1, |end| end + 1)
    } else {
        index + 1
    }
}

fn attribute_definitively_disables(tokens: &[MacroToken]) -> bool {
    let Some(MacroToken::Ident(name)) = tokens.first() else {
        return false;
    };
    let Some(arguments) = delimited_arguments(tokens, 1) else {
        return false;
    };
    if name == "cfg" {
        return evaluate_cfg_tokens(arguments) == CfgValue::False;
    }
    if name != "cfg_attr" {
        return false;
    }
    let items = split_top_level(arguments);
    let Some((predicate, nested)) = items.split_first() else {
        return false;
    };
    evaluate_cfg_tokens(predicate) == CfgValue::True
        && nested
            .iter()
            .any(|attribute| attribute_definitively_disables(attribute))
}

fn delimited_arguments(tokens: &[MacroToken], opening: usize) -> Option<&[MacroToken]> {
    if tokens.get(opening) != Some(&MacroToken::Punct('(')) {
        return None;
    }
    let closing = matching_delimiter(tokens, opening)?;
    Some(&tokens[opening + 1..closing])
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CfgValue {
    True,
    False,
    Unknown,
}

impl CfgValue {
    fn and(self, other: Self) -> Self {
        match (self, other) {
            (Self::False, _) | (_, Self::False) => Self::False,
            (Self::True, Self::True) => Self::True,
            _ => Self::Unknown,
        }
    }

    fn or(self, other: Self) -> Self {
        match (self, other) {
            (Self::True, _) | (_, Self::True) => Self::True,
            (Self::False, Self::False) => Self::False,
            _ => Self::Unknown,
        }
    }

    fn not(self) -> Self {
        match self {
            Self::True => Self::False,
            Self::False => Self::True,
            Self::Unknown => Self::Unknown,
        }
    }
}

fn evaluate_cfg_tokens(tokens: &[MacroToken]) -> CfgValue {
    let Some(MacroToken::Ident(operator)) = tokens.first() else {
        return CfgValue::Unknown;
    };
    let Some(arguments) = delimited_arguments(tokens, 1) else {
        return CfgValue::Unknown;
    };
    let items = split_top_level(arguments);
    match operator.as_str() {
        "all" => items.iter().fold(CfgValue::True, |value, item| {
            value.and(evaluate_cfg_tokens(item))
        }),
        "any" => items.iter().fold(CfgValue::False, |value, item| {
            value.or(evaluate_cfg_tokens(item))
        }),
        "not" if items.len() == 1 => evaluate_cfg_tokens(items[0]).not(),
        _ => CfgValue::Unknown,
    }
}

fn split_top_level(tokens: &[MacroToken]) -> Vec<&[MacroToken]> {
    if tokens.is_empty() {
        return Vec::new();
    }
    let mut items = Vec::new();
    let mut depth = 0_usize;
    let mut start = 0;
    for (index, token) in tokens.iter().enumerate() {
        match token {
            MacroToken::Punct('(' | '[' | '{') => depth += 1,
            MacroToken::Punct(')' | ']' | '}') => depth = depth.saturating_sub(1),
            MacroToken::Punct(',') if depth == 0 => {
                items.push(&tokens[start..index]);
                start = index + 1;
            }
            _ => {}
        }
    }
    if start < tokens.len() {
        items.push(&tokens[start..]);
    }
    items
}
