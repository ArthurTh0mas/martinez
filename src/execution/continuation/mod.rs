use self::{interrupt::*, interrupt_data::*, resume_data::*};
use super::*;
use crate::consensus::ValidationError;
use derive_more::From;
use enum_as_inner::EnumAsInner;
use ethereum_types::Address;
use ethnum::U256;
use std::{
    ops::{Generator, GeneratorState},
    pin::Pin,
};

/// Interrupts.
pub mod interrupt;
/// Data attached to interrupts.
pub mod interrupt_data;
/// Data required for resume.
pub mod resume_data;

pub(crate) type StateGenerator<'a, Output> =
    Pin<Box<dyn Generator<ResumeData, Yield = InterruptData, Return = Output> + Send + Sync + 'a>>;

pub(crate) type InnerCoroutine = Pin<
    Box<
        dyn Generator<ResumeData, Yield = InterruptData, Return = Result<(), Box<ValidationError>>>
            + Send
            + Sync,
    >,
>;

#[macro_export]
macro_rules! gen_await {
    ($e:expr) => {{
        let mut resume_data = ().into();
        let mut __gen = $e;
        loop {
            match unsafe { ::core::pin::Pin::new_unchecked(&mut __gen) }.resume(resume_data) {
                ::core::ops::GeneratorState::Yielded(interrupt) => {
                    resume_data = yield interrupt;
                }
                ::core::ops::GeneratorState::Complete(result) => break result,
            }
        }
    }};
}

fn resume_interrupt(mut inner: InnerCoroutine, resume_data: ResumeData) -> Interrupt {
    match inner.as_mut().resume(resume_data) {
        GeneratorState::Yielded(interrupt) => match interrupt {
            InterruptData::ReadAccount { address } => Interrupt::ReadAccount {
                interrupt: ReadAccountInterrupt { inner },
                address,
            },
            InterruptData::ReadStorage { address, location } => Interrupt::ReadStorage {
                interrupt: ReadStorageInterrupt { inner },
                address,
                location,
            },
            InterruptData::ReadCode { code_hash } => Interrupt::ReadCode {
                interrupt: ReadCodeInterrupt { inner },
                code_hash,
            },
            InterruptData::EraseStorage { address } => Interrupt::EraseStorage {
                interrupt: EraseStorageInterrupt { inner },
                address,
            },
            InterruptData::ReadHeader {
                block_number,
                block_hash,
            } => Interrupt::ReadHeader {
                interrupt: ReadHeaderInterrupt { inner },
                block_number,
                block_hash,
            },
            InterruptData::ReadBody {
                block_number,
                block_hash,
            } => Interrupt::ReadBody {
                interrupt: ReadBodyInterrupt { inner },
                block_number,
                block_hash,
            },
            InterruptData::ReadTotalDifficulty {
                block_number,
                block_hash,
            } => Interrupt::ReadTotalDifficulty {
                interrupt: ReadTotalDifficultyInterrupt { inner },
                block_number,
                block_hash,
            },
            InterruptData::BeginBlock { block_number } => Interrupt::BeginBlock {
                interrupt: BeginBlockInterrupt { inner },
                block_number,
            },
            InterruptData::UpdateAccount {
                address,
                initial,
                current,
            } => Interrupt::UpdateAccount {
                interrupt: UpdateAccountInterrupt { inner },
                address,
                initial,
                current,
            },
            InterruptData::UpdateCode { code_hash, code } => Interrupt::UpdateCode {
                interrupt: UpdateCodeInterrupt { inner },
                code_hash,
                code,
            },
            InterruptData::UpdateStorage {
                address,
                location,
                initial,
                current,
            } => Interrupt::UpdateStorage {
                interrupt: UpdateStorageInterrupt { inner },
                address,
                location,
                initial,
                current,
            },

            InterruptData::ReadBodyWithSenders { number, hash } => Interrupt::ReadBodyWithSenders {
                interrupt: ReadBodyWithSendersInterrupt { inner },
                number,
                hash,
            },
            InterruptData::InsertBlock { block, hash } => Interrupt::InsertBlock {
                interrupt: InsertBlockInterrupt { inner },
                block,
                hash,
            },
            InterruptData::CanonizeBlock { number, hash } => Interrupt::CanonizeBlock {
                interrupt: CanonizeBlockInterrupt { inner },
                number,
                hash,
            },

            InterruptData::DecanonizeBlock { number } => Interrupt::DecanonizeBlock {
                interrupt: DecanonizeBlockInterrupt { inner },
                number,
            },
            InterruptData::CanonicalHash { number } => Interrupt::CanonicalHash {
                interrupt: CanonicalHashInterrupt { inner },
                number,
            },
            InterruptData::UnwindStateChanges { number } => Interrupt::UnwindStateChanges {
                interrupt: UnwindStateChangesInterrupt { inner },
                number,
            },
            InterruptData::CurrentCanonicalBlock => Interrupt::CurrentCanonicalBlock {
                interrupt: CurrentCanonicalBlockInterrupt { inner },
            },
            InterruptData::StateRootHash => Interrupt::StateRootHash {
                interrupt: StateRootHashInterrupt { inner },
            },
        },
        GeneratorState::Complete(result) => Interrupt::Complete {
            interrupt: FinishedInterrupt(inner),
            result,
        },
    }
}
