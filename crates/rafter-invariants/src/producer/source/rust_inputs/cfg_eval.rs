use syn::{punctuated::Punctuated, Attribute, Meta, Token};

use super::path_is_ident;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CfgValue {
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

fn evaluate_cfg(meta: &Meta) -> CfgValue {
    let Meta::List(list) = meta else {
        return CfgValue::Unknown;
    };
    let Ok(items) = list.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated) else {
        return CfgValue::Unknown;
    };
    if path_is_ident(&list.path, "all") {
        items
            .iter()
            .fold(CfgValue::True, |value, item| value.and(evaluate_cfg(item)))
    } else if path_is_ident(&list.path, "any") {
        items
            .iter()
            .fold(CfgValue::False, |value, item| value.or(evaluate_cfg(item)))
    } else if path_is_ident(&list.path, "not") && items.len() == 1 {
        evaluate_cfg(&items[0]).not()
    } else {
        CfgValue::Unknown
    }
}

pub(super) fn walk_effective_metas(
    meta: &Meta,
    guard: CfgValue,
    visit: &mut impl FnMut(&Meta, CfgValue) -> Result<(), String>,
) -> Result<(), String> {
    if guard == CfgValue::False {
        return Ok(());
    }
    if !path_is_ident(meta.path(), "cfg_attr") {
        return visit(meta, guard);
    }
    let Meta::List(list) = meta else {
        return Err("#[cfg_attr] must contain a predicate and attributes".to_owned());
    };
    let items = list
        .parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)
        .map_err(|error| format!("parse #[cfg_attr]: {error}"))?;
    let mut items = items.iter();
    let predicate = items
        .next()
        .ok_or_else(|| "#[cfg_attr] must contain a predicate".to_owned())?;
    let nested_guard = guard.and(evaluate_cfg(predicate));
    for nested in items {
        walk_effective_metas(nested, nested_guard, visit)?;
    }
    Ok(())
}

pub(super) fn item_is_definitively_inactive(item: &syn::Item) -> Result<bool, String> {
    let Some(attributes) = item_attributes(item) else {
        return Ok(false);
    };
    let mut inactive = false;
    for attribute in attributes {
        walk_effective_metas(&attribute.meta, CfgValue::True, &mut |meta, guard| {
            if guard == CfgValue::True && path_is_ident(meta.path(), "cfg") {
                let Meta::List(list) = meta else {
                    return Err("#[cfg] must contain one predicate".to_owned());
                };
                let predicate = list
                    .parse_args::<Meta>()
                    .map_err(|error| format!("parse #[cfg]: {error}"))?;
                inactive |= evaluate_cfg(&predicate) == CfgValue::False;
            }
            Ok(())
        })?;
        if inactive {
            break;
        }
    }
    Ok(inactive)
}

fn item_attributes(item: &syn::Item) -> Option<&[Attribute]> {
    match item {
        syn::Item::Const(item) => Some(&item.attrs),
        syn::Item::Enum(item) => Some(&item.attrs),
        syn::Item::ExternCrate(item) => Some(&item.attrs),
        syn::Item::Fn(item) => Some(&item.attrs),
        syn::Item::ForeignMod(item) => Some(&item.attrs),
        syn::Item::Impl(item) => Some(&item.attrs),
        syn::Item::Macro(item) => Some(&item.attrs),
        syn::Item::Mod(item) => Some(&item.attrs),
        syn::Item::Static(item) => Some(&item.attrs),
        syn::Item::Struct(item) => Some(&item.attrs),
        syn::Item::Trait(item) => Some(&item.attrs),
        syn::Item::TraitAlias(item) => Some(&item.attrs),
        syn::Item::Type(item) => Some(&item.attrs),
        syn::Item::Union(item) => Some(&item.attrs),
        syn::Item::Use(item) => Some(&item.attrs),
        _ => None,
    }
}
