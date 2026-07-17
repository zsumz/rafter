use syn::{parse::Parser, punctuated::Punctuated, Attribute, Meta, Token};

pub(crate) fn module_active_for_test(attributes: &[Attribute]) -> Result<bool, String> {
    attributes
        .iter()
        .map(|attribute| meta_keeps_item_active(&attribute.meta))
        .try_fold(true, |active, keep| keep.map(|keep| active && keep))
}

fn meta_keeps_item_active(meta: &Meta) -> Result<bool, String> {
    let Meta::List(list) = meta else {
        return Ok(true);
    };
    if list.path.is_ident("cfg") {
        let predicate = syn::parse2::<Meta>(list.tokens.clone())
            .map_err(|error| format!("parse cfg predicate: {error}"))?;
        return cfg_predicate_active_for_test(&predicate);
    }
    if !list.path.is_ident("cfg_attr") {
        return Ok(true);
    }
    let arguments = Punctuated::<Meta, Token![,]>::parse_terminated
        .parse2(list.tokens.clone())
        .map_err(|error| format!("parse cfg_attr arguments: {error}"))?;
    let mut arguments = arguments.iter();
    let predicate = arguments.next().ok_or("cfg_attr requires a predicate")?;
    if !cfg_predicate_active_for_test(predicate)? {
        return Ok(true);
    }
    arguments
        .map(meta_keeps_item_active)
        .try_fold(true, |active, keep| keep.map(|keep| active && keep))
}

pub(super) fn cfg_predicate_active_for_test(predicate: &Meta) -> Result<bool, String> {
    cfg_value_for_test(predicate).into_result()
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum CfgValue {
    False,
    True,
    Unknown,
}

impl CfgValue {
    fn into_result(self) -> Result<bool, String> {
        match self {
            Self::False => Ok(false),
            Self::True => Ok(true),
            Self::Unknown => Err(
                "registered Cargo target uses a cfg predicate outside the reviewed test context"
                    .to_owned(),
            ),
        }
    }
}

fn cfg_value_for_test(predicate: &Meta) -> CfgValue {
    match predicate {
        Meta::Path(path) => cfg_path_value_for_test(path),
        Meta::List(list) if list.path.is_ident("any") || list.path.is_ident("all") => {
            let Ok(items) =
                Punctuated::<Meta, Token![,]>::parse_terminated.parse2(list.tokens.clone())
            else {
                return CfgValue::Unknown;
            };
            if list.path.is_ident("any") {
                if items
                    .iter()
                    .any(|item| cfg_value_for_test(item) == CfgValue::True)
                {
                    CfgValue::True
                } else if items
                    .iter()
                    .all(|item| cfg_value_for_test(item) == CfgValue::False)
                {
                    CfgValue::False
                } else {
                    CfgValue::Unknown
                }
            } else if items
                .iter()
                .any(|item| cfg_value_for_test(item) == CfgValue::False)
            {
                CfgValue::False
            } else if items
                .iter()
                .all(|item| cfg_value_for_test(item) == CfgValue::True)
            {
                CfgValue::True
            } else {
                CfgValue::Unknown
            }
        }
        Meta::List(list) if list.path.is_ident("not") => {
            match syn::parse2::<Meta>(list.tokens.clone()).map(|item| cfg_value_for_test(&item)) {
                Ok(CfgValue::True) => CfgValue::False,
                Ok(CfgValue::False) => CfgValue::True,
                Ok(CfgValue::Unknown) | Err(_) => CfgValue::Unknown,
            }
        }
        Meta::NameValue(value) => cfg_name_value_for_test(value),
        Meta::List(_) => CfgValue::Unknown,
    }
}

fn cfg_path_value_for_test(path: &syn::Path) -> CfgValue {
    if path.is_ident("test") {
        CfgValue::True
    } else if path.is_ident("doctest") || path.is_ident("miri") {
        CfgValue::False
    } else if path.is_ident("unix") {
        bool_cfg(cfg!(unix))
    } else if path.is_ident("windows") {
        bool_cfg(cfg!(windows))
    } else {
        CfgValue::Unknown
    }
}

fn cfg_name_value_for_test(value: &syn::MetaNameValue) -> CfgValue {
    let syn::Expr::Lit(expression) = &value.value else {
        return CfgValue::Unknown;
    };
    let syn::Lit::Str(expected) = &expression.lit else {
        return CfgValue::Unknown;
    };
    let expected = expected.value();
    let Some(name) = value.path.get_ident().map(ToString::to_string) else {
        return CfgValue::Unknown;
    };
    match name.as_str() {
        // Detector targets are compiled with Cargo's reviewed
        // `--no-default-features` contract and no explicit feature set.
        "feature" => CfgValue::False,
        "target_arch" => bool_cfg(expected == std::env::consts::ARCH),
        "target_os" => bool_cfg(expected == std::env::consts::OS),
        "target_family" => target_family_cfg(&expected),
        "target_endian" => target_endian_cfg(&expected),
        "target_pointer_width" => bool_cfg(expected == usize::BITS.to_string()),
        "target_env" => known_cfg_value(&expected, target_env()),
        "target_vendor" => known_cfg_value(&expected, target_vendor()),
        "target_abi" => known_cfg_value(&expected, target_abi()),
        _ => CfgValue::Unknown,
    }
}

fn bool_cfg(value: bool) -> CfgValue {
    if value {
        CfgValue::True
    } else {
        CfgValue::False
    }
}

fn known_cfg_value(expected: &str, actual: Option<&str>) -> CfgValue {
    actual.map_or(CfgValue::Unknown, |actual| bool_cfg(expected == actual))
}

fn target_family_cfg(expected: &str) -> CfgValue {
    match expected {
        "unix" => bool_cfg(cfg!(unix)),
        "windows" => bool_cfg(cfg!(windows)),
        "wasm" => bool_cfg(cfg!(target_family = "wasm")),
        _ => CfgValue::False,
    }
}

fn target_endian_cfg(expected: &str) -> CfgValue {
    match expected {
        "little" => bool_cfg(cfg!(target_endian = "little")),
        "big" => bool_cfg(cfg!(target_endian = "big")),
        _ => CfgValue::False,
    }
}

fn target_env() -> Option<&'static str> {
    if cfg!(target_env = "gnu") {
        Some("gnu")
    } else if cfg!(target_env = "musl") {
        Some("musl")
    } else if cfg!(target_env = "msvc") {
        Some("msvc")
    } else if cfg!(target_env = "sgx") {
        Some("sgx")
    } else if cfg!(target_env = "uclibc") {
        Some("uclibc")
    } else if cfg!(target_env = "newlib") {
        Some("newlib")
    } else if cfg!(target_env = "ohos") {
        Some("ohos")
    } else if cfg!(target_env = "") {
        Some("")
    } else {
        None
    }
}

fn target_vendor() -> Option<&'static str> {
    if cfg!(target_vendor = "apple") {
        Some("apple")
    } else if cfg!(target_vendor = "pc") {
        Some("pc")
    } else if cfg!(target_vendor = "unknown") {
        Some("unknown")
    } else if cfg!(target_vendor = "fortanix") {
        Some("fortanix")
    } else if cfg!(target_vendor = "nintendo") {
        Some("nintendo")
    } else if cfg!(target_vendor = "sony") {
        Some("sony")
    } else if cfg!(target_vendor = "espressif") {
        Some("espressif")
    } else {
        None
    }
}

fn target_abi() -> Option<&'static str> {
    if cfg!(target_abi = "eabi") {
        Some("eabi")
    } else if cfg!(target_abi = "eabihf") {
        Some("eabihf")
    } else if cfg!(target_abi = "macabi") {
        Some("macabi")
    } else if cfg!(target_abi = "sim") {
        Some("sim")
    } else if cfg!(target_abi = "") {
        Some("")
    } else {
        None
    }
}
