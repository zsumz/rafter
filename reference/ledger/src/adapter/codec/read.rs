//! One field grammar over borrowed bytes or bounded streamed input.

use super::input::Input;
use super::{
    LedgerCodecError, NonZeroField, CACHED_COMPLETION, MUTATION_CLOSE_ACCOUNT, MUTATION_DEPOSIT,
    MUTATION_OPEN_ACCOUNT, MUTATION_TRANSFER, NO_CACHED_COMPLETION,
    REJECTION_ACCOUNT_ALREADY_EXISTS, REJECTION_ACCOUNT_CAPACITY_EXCEEDED,
    REJECTION_ACCOUNT_NOT_EMPTY, REJECTION_ACCOUNT_NOT_FOUND, REJECTION_BALANCE_OVERFLOW,
    REJECTION_INSUFFICIENT_FUNDS, REJECTION_SAME_ACCOUNT, REJECTION_SUPPLY_OVERFLOW,
    RESULT_ACCOUNT_CLOSED, RESULT_ACCOUNT_OPENED, RESULT_DEPOSITED, RESULT_REJECTED,
    RESULT_TRANSFERRED,
};
use crate::{
    AccountId, Amount, BusinessRejection, ClientId, Mutation, MutationResult, Sequence,
    SessionEpoch,
};

pub(super) struct Cursor<I> {
    input: I,
}
impl<'a> Cursor<&'a [u8]> {
    pub(super) const fn new(bytes: &'a [u8]) -> Self {
        Self { input: bytes }
    }
}
impl<I: Input> Cursor<I> {
    pub(super) const fn from_input(input: I) -> Self {
        Self { input }
    }
    pub(super) fn take_u8(&mut self) -> Result<u8, I::Error> {
        Ok(self.input.take::<1>()?[0])
    }
    pub(super) fn take_u32(&mut self) -> Result<u32, I::Error> {
        Ok(u32::from_be_bytes(self.input.take()?))
    }
    pub(super) fn take_u64(&mut self) -> Result<u64, I::Error> {
        Ok(u64::from_be_bytes(self.input.take()?))
    }
    pub(super) fn take_u128(&mut self) -> Result<u128, I::Error> {
        Ok(u128::from_be_bytes(self.input.take()?))
    }
    pub(super) fn take_len(&mut self) -> Result<usize, I::Error> {
        let length = self.take_u32()?;
        usize::try_from(length).map_err(|_| {
            LedgerCodecError::LengthOutOfRange {
                length: u64::from(length),
            }
            .into()
        })
    }
    pub(super) fn require_records(
        &self,
        count: usize,
        minimum_record_bytes: usize,
        following_bytes: usize,
    ) -> Result<(), I::Error> {
        let required = count
            .checked_mul(minimum_record_bytes)
            .and_then(|bytes| bytes.checked_add(following_bytes))
            .ok_or(LedgerCodecError::LengthOutOfRange {
                length: u64::try_from(count).unwrap_or(u64::MAX),
            })?;
        if u64::try_from(required).unwrap_or(u64::MAX) > self.input.remaining() {
            return Err(LedgerCodecError::TruncatedFrame {
                required,
                available: usize::try_from(self.input.remaining()).unwrap_or(usize::MAX),
            }
            .into());
        }
        Ok(())
    }
    pub(super) fn finish(&self) -> Result<(), I::Error> {
        let remaining = self.input.remaining();
        if remaining == 0 {
            Ok(())
        } else {
            Err(LedgerCodecError::TrailingBytes {
                remaining: usize::try_from(remaining).unwrap_or(usize::MAX),
            }
            .into())
        }
    }
    pub(super) fn take_client(&mut self) -> Result<ClientId, I::Error> {
        Ok(ClientId::new(self.take_u32()?))
    }

    pub(super) fn take_session_epoch(&mut self) -> Result<SessionEpoch, I::Error> {
        SessionEpoch::new(self.take_u64()?).ok_or_else(|| {
            LedgerCodecError::ZeroValuedField {
                field: NonZeroField::SessionEpoch,
            }
            .into()
        })
    }

    pub(super) fn take_sequence(&mut self) -> Result<Sequence, I::Error> {
        Sequence::new(self.take_u64()?).ok_or_else(|| {
            LedgerCodecError::ZeroValuedField {
                field: NonZeroField::Sequence,
            }
            .into()
        })
    }

    pub(super) fn take_amount(&mut self) -> Result<Amount, I::Error> {
        Amount::new(self.take_u64()?).ok_or_else(|| {
            LedgerCodecError::ZeroValuedField {
                field: NonZeroField::Amount,
            }
            .into()
        })
    }

    pub(super) fn take_mutation(&mut self) -> Result<Mutation, I::Error> {
        match self.take_u8()? {
            MUTATION_OPEN_ACCOUNT => Ok(Mutation::OpenAccount {
                account_id: AccountId::new(self.take_u64()?),
            }),
            MUTATION_DEPOSIT => Ok(Mutation::Deposit {
                account_id: AccountId::new(self.take_u64()?),
                amount: self.take_amount()?,
            }),
            MUTATION_TRANSFER => Ok(Mutation::Transfer {
                from: AccountId::new(self.take_u64()?),
                to: AccountId::new(self.take_u64()?),
                amount: self.take_amount()?,
            }),
            MUTATION_CLOSE_ACCOUNT => Ok(Mutation::CloseAccount {
                account_id: AccountId::new(self.take_u64()?),
            }),
            tag => Err(LedgerCodecError::UnknownMutationTag { tag }.into()),
        }
    }

    pub(super) fn take_result(&mut self) -> Result<MutationResult, I::Error> {
        match self.take_u8()? {
            RESULT_ACCOUNT_OPENED => Ok(MutationResult::AccountOpened),
            RESULT_DEPOSITED => Ok(MutationResult::Deposited {
                balance: self.take_u64()?,
            }),
            RESULT_TRANSFERRED => Ok(MutationResult::Transferred {
                from_balance: self.take_u64()?,
                to_balance: self.take_u64()?,
            }),
            RESULT_ACCOUNT_CLOSED => Ok(MutationResult::AccountClosed),
            RESULT_REJECTED => Ok(MutationResult::Rejected(self.take_rejection()?)),
            tag => Err(LedgerCodecError::UnknownResultTag { tag }.into()),
        }
    }

    pub(super) fn take_rejection(&mut self) -> Result<BusinessRejection, I::Error> {
        match self.take_u8()? {
            REJECTION_ACCOUNT_ALREADY_EXISTS => Ok(BusinessRejection::AccountAlreadyExists),
            REJECTION_ACCOUNT_CAPACITY_EXCEEDED => Ok(BusinessRejection::AccountCapacityExceeded),
            REJECTION_ACCOUNT_NOT_FOUND => Ok(BusinessRejection::AccountNotFound),
            REJECTION_SAME_ACCOUNT => Ok(BusinessRejection::SameAccount),
            REJECTION_INSUFFICIENT_FUNDS => Ok(BusinessRejection::InsufficientFunds),
            REJECTION_BALANCE_OVERFLOW => Ok(BusinessRejection::BalanceOverflow),
            REJECTION_SUPPLY_OVERFLOW => Ok(BusinessRejection::SupplyOverflow),
            REJECTION_ACCOUNT_NOT_EMPTY => Ok(BusinessRejection::AccountNotEmpty),
            tag => Err(LedgerCodecError::UnknownRejectionTag { tag }.into()),
        }
    }

    pub(super) fn take_cached_completion(
        &mut self,
    ) -> Result<Option<(Sequence, Mutation, MutationResult)>, I::Error> {
        match self.take_u8()? {
            NO_CACHED_COMPLETION => Ok(None),
            CACHED_COMPLETION => Ok(Some((
                self.take_sequence()?,
                self.take_mutation()?,
                self.take_result()?,
            ))),
            marker => Err(LedgerCodecError::UnknownCacheMarker { marker }.into()),
        }
    }
}
