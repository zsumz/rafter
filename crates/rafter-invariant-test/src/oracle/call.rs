//! Compiler-bound function calls used by detector and recorder macros.

use std::{any::type_name, fmt::Debug};

use super::marker::{observed, violation};
use crate::detector::{record_expected_rejection, record_recorder_invocation};

/// Internal function-call adapter used by the invocation-bound oracle macros.
///
/// The function item itself is retained as `Self`, so its compiler-resolved
/// type name cannot be replaced with a caller-supplied detector label.
#[doc(hidden)]
pub trait OracleCall<Arguments> {
    type Output;

    fn __oracle_call(self, arguments: Arguments) -> Self::Output;
}

macro_rules! impl_oracle_call {
    (() => ()) => {
        impl<Function, Output> OracleCall<()> for Function
        where
            Function: FnOnce() -> Output,
        {
            type Output = Output;

            fn __oracle_call(self, (): ()) -> Self::Output {
                self()
            }
        }
    };
    (($($argument:ident),+) => ($($value:ident),+)) => {
        impl<Function, Output, $($argument),+> OracleCall<($($argument,)+)> for Function
        where
            Function: FnOnce($($argument),+) -> Output,
        {
            type Output = Output;

            #[allow(non_snake_case)]
            fn __oracle_call(self, ($($value,)+): ($($argument,)+)) -> Self::Output {
                self($($value),+)
            }
        }
    };
}

impl_oracle_call!(() => ());
impl_oracle_call!((A0) => (a0));
impl_oracle_call!((A0, A1) => (a0, a1));
impl_oracle_call!((A0, A1, A2) => (a0, a1, a2));
impl_oracle_call!((A0, A1, A2, A3) => (a0, a1, a2, a3));
impl_oracle_call!((A0, A1, A2, A3, A4) => (a0, a1, a2, a3, a4));
impl_oracle_call!((A0, A1, A2, A3, A4, A5) => (a0, a1, a2, a3, a4, a5));
impl_oracle_call!((A0, A1, A2, A3, A4, A5, A6) => (a0, a1, a2, a3, a4, a5, a6));
impl_oracle_call!((A0, A1, A2, A3, A4, A5, A6, A7) => (a0, a1, a2, a3, a4, a5, a6, a7));

/// Invoke a detector and record its compiler-resolved identity after rejection.
#[doc(hidden)]
#[track_caller]
pub fn expect_error<Function, Arguments, Value, Error, Message>(
    detector: Function,
    arguments: Arguments,
    message: Message,
) -> Error
where
    Function: OracleCall<Arguments, Output = Result<Value, Error>>,
    Value: Debug,
    Message: std::fmt::Display,
{
    let detector_identity = type_name::<Function>();
    match detector.__oracle_call(arguments) {
        Err(error) => {
            record_expected_rejection(detector_identity);
            observed();
            error
        }
        Ok(value) => violation(format_args!("{message}: {value:?}")),
    }
}

/// Invoke a recorder and record its compiler-resolved identity after return.
#[doc(hidden)]
pub fn invoke_recorder<Function, Arguments>(recorder: Function, arguments: Arguments)
where
    Function: OracleCall<Arguments, Output = ()>,
{
    let recorder_identity = type_name::<Function>();
    recorder.__oracle_call(arguments);
    record_recorder_invocation(recorder_identity);
}
