use super::{common::Message, CreateMessage};
use ethereum_types::Address;
use ethnum::U256;

/// State access status (EIP-2929).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AccessStatus {
    Cold,
    Warm,
}

impl Default for AccessStatus {
    fn default() -> Self {
        Self::Cold
    }
}

#[derive(Clone, Copy, Debug)]
pub enum StorageStatus {
    /// The value of a storage item has been left unchanged: 0 -> 0 and X -> X.
    Unchanged,
    /// The value of a storage item has been modified: X -> Y.
    Modified,
    /// A storage item has been modified after being modified before: X -> Y -> Z.
    ModifiedAgain,
    /// A new storage item has been added: 0 -> X.
    Added,
    /// A storage item has been deleted: X -> 0.
    Deleted,
}

/// The transaction and block data for execution.
#[derive(Clone, Debug)]
pub struct TxContext {
    /// The transaction gas price.
    pub tx_gas_price: U256,
    /// The transaction origin account.
    pub tx_origin: Address,
    /// The miner of the block.
    pub block_coinbase: Address,
    /// The block number.
    pub block_number: u64,
    /// The block timestamp.
    pub block_timestamp: u64,
    /// The block gas limit.
    pub block_gas_limit: u64,
    /// The block difficulty.
    pub block_difficulty: U256,
    /// The blockchain's ChainID.
    pub chain_id: U256,
    /// The block base fee per gas (EIP-1559, EIP-3198).
    pub block_base_fee: U256,
}

#[derive(Clone, Debug)]
pub enum Call {
    Call(Message),
    Create(CreateMessage),
}
