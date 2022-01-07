use crate::consensus::ValidationError;

use super::*;
use bytes::Bytes;

macro_rules! interrupt {
    ( $(#[$outer:meta])* $name:ident => $resume_with:ty) => {
		$(#[$outer])*
        pub struct $name {
            pub(crate) inner: InnerCoroutine,
        }

        impl $name {
            pub fn resume(self, resume_data: $resume_with) -> Interrupt {
                resume_interrupt(self.inner, resume_data.into())
            }
        }
    };
}

interrupt! {
    /// Resume this interrupt to start execution.
    #[derive(From)]
    StartedInterrupt => ()
}
interrupt! {
    ReadAccountInterrupt => Option<Account>
}
interrupt! {
    ReadStorageInterrupt => U256
}
interrupt! {
    ReadCodeInterrupt => Bytes
}
interrupt! {
    EraseStorageInterrupt => ()
}
interrupt! {
    ReadHeaderInterrupt => Box<Option<BlockHeader>>
}
interrupt! {
    ReadBodyInterrupt => Box<Option<BlockBody>>
}
interrupt! {
    ReadTotalDifficultyInterrupt => Option<U256>
}
interrupt! {
    BeginBlockInterrupt => ()
}
interrupt! {
    UpdateAccountInterrupt => ()
}
interrupt! {
    UpdateCodeInterrupt => ()
}
interrupt! {
    UpdateStorageInterrupt => ()
}

// InMemoryState extensions
interrupt! {
    ReadBodyWithSendersInterrupt => Box<Option<BlockBodyWithSenders>>
}
interrupt! {
    InsertBlockInterrupt => ()
}
interrupt! {
    CanonizeBlockInterrupt => ()
}
interrupt! {
    DecanonizeBlockInterrupt => ()
}
interrupt! {
    CanonicalHashInterrupt => Option<H256>
}
interrupt! {
    UnwindStateChangesInterrupt => ()
}
interrupt! {
    CurrentCanonicalBlockInterrupt => BlockNumber
}
interrupt! {
    StateRootHashInterrupt => H256
}

/// Execution complete, this interrupt cannot be resumed.
pub struct FinishedInterrupt(pub(crate) InnerCoroutine);

/// Collection of all possible interrupts. Match on this to get the specific interrupt returned.
#[derive(From)]
pub enum Interrupt {
    ReadAccount {
        interrupt: ReadAccountInterrupt,
        address: Address,
    },
    ReadStorage {
        interrupt: ReadStorageInterrupt,
        address: Address,
        location: U256,
    },
    ReadCode {
        interrupt: ReadCodeInterrupt,
        code_hash: H256,
    },
    EraseStorage {
        interrupt: EraseStorageInterrupt,
        address: Address,
    },
    ReadHeader {
        interrupt: ReadHeaderInterrupt,
        block_number: BlockNumber,
        block_hash: H256,
    },
    ReadBody {
        interrupt: ReadBodyInterrupt,
        block_number: BlockNumber,
        block_hash: H256,
    },
    ReadTotalDifficulty {
        interrupt: ReadTotalDifficultyInterrupt,
        block_number: BlockNumber,
        block_hash: H256,
    },
    BeginBlock {
        interrupt: BeginBlockInterrupt,
        block_number: BlockNumber,
    },
    UpdateAccount {
        interrupt: UpdateAccountInterrupt,
        address: Address,
        initial: Option<Account>,
        current: Option<Account>,
    },
    UpdateCode {
        interrupt: UpdateCodeInterrupt,
        code_hash: H256,
        code: Bytes,
    },
    UpdateStorage {
        interrupt: UpdateStorageInterrupt,
        address: Address,
        location: U256,
        initial: U256,
        current: U256,
    },

    // InMemoryState extensions
    ReadBodyWithSenders {
        interrupt: ReadBodyWithSendersInterrupt,
        number: BlockNumber,
        hash: H256,
    },
    InsertBlock {
        interrupt: InsertBlockInterrupt,
        block: Box<Block>,
        hash: H256,
    },
    CanonizeBlock {
        interrupt: CanonizeBlockInterrupt,
        number: BlockNumber,
        hash: H256,
    },
    DecanonizeBlock {
        interrupt: DecanonizeBlockInterrupt,
        number: BlockNumber,
    },
    CanonicalHash {
        interrupt: CanonicalHashInterrupt,
        number: BlockNumber,
    },
    UnwindStateChanges {
        interrupt: UnwindStateChangesInterrupt,
        number: BlockNumber,
    },
    CurrentCanonicalBlock {
        interrupt: CurrentCanonicalBlockInterrupt,
    },
    StateRootHash {
        interrupt: StateRootHashInterrupt,
    },

    Complete {
        interrupt: FinishedInterrupt,
        result: Result<(), Box<ValidationError>>,
    },
}
