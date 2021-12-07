use super::common::{Message, Output};
use ethereum_types::Address;
use ethnum::U256;
use std::future::Future;

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

/// Abstraction that exposes host context to EVM.
pub trait Host {
    /// Check if an account exists.
    fn account_exists(&self, address: Address) -> bool;
    /// Get value of a storage key.
    ///
    /// Returns `Ok(U256::zero())` if does not exist.
    fn get_storage(&self, address: Address, key: U256) -> U256;
    /// Set value of a storage key.
    fn set_storage(&mut self, address: Address, key: U256, value: U256) -> StorageStatus;
    /// Get balance of an account.
    ///
    /// Returns `Ok(0)` if account does not exist.
    fn get_balance(&self, address: Address) -> U256;
    /// Get code size of an account.
    ///
    /// Returns `Ok(0)` if account does not exist.
    fn get_code_size(&self, address: Address) -> U256;
    /// Get code hash of an account.
    ///
    /// Returns `Ok(0)` if account does not exist.
    fn get_code_hash(&self, address: Address) -> U256;
    /// Copy code of an account.
    ///
    /// Returns `Ok(0)` if offset is invalid.
    fn copy_code(&self, address: Address, offset: usize, buffer: &mut [u8]) -> usize;
    /// Self-destruct account.
    fn selfdestruct(&mut self, address: Address, beneficiary: Address);
    /// Call to another account.
    fn call(&mut self, msg: &Message) -> Output;
    /// Retrieve transaction context.
    fn get_tx_context(&self) -> TxContext;
    /// Get block hash.
    ///
    /// Returns `Ok(U256::zero())` if block does not exist.
    fn get_block_hash(&self, block_number: u64) -> U256;
    /// Emit a log.
    fn emit_log(&mut self, address: Address, data: &[u8], topics: &[U256]);
    /// Mark account as warm, return previous access status.
    ///
    /// Returns `Ok(AccessStatus::Cold)` if account does not exist.
    fn access_account(&mut self, address: Address) -> AccessStatus;
    /// Mark storage key as warm, return previous access status.
    ///
    /// Returns `Ok(AccessStatus::Cold)` if account does not exist.
    fn access_storage(&mut self, address: Address, key: U256) -> AccessStatus;
}

/// Abstraction that exposes host context to EVM.
pub trait AsyncHost: Send + Sync {
    type AccountExistsFut<'a>: Future<Output = anyhow::Result<bool>> + Send + 'a;
    type GetStorageFut<'a>: Future<Output = anyhow::Result<U256>> + Send + 'a;
    type SetStorageFut<'a>: Future<Output = anyhow::Result<StorageStatus>> + Send + 'a;
    type GetBalanceFut<'a>: Future<Output = anyhow::Result<U256>> + Send + 'a;
    type GetCodeSizeFut<'a>: Future<Output = anyhow::Result<U256>> + Send + 'a;
    type GetCodeHashFut<'a>: Future<Output = anyhow::Result<U256>> + Send + 'a;
    type CopyCodeFut<'a>: Future<Output = anyhow::Result<usize>> + Send + 'a;
    type SelfdestructFut<'a>: Future<Output = anyhow::Result<()>> + Send + 'a;
    type CallFut<'a>: Future<Output = anyhow::Result<Output>> + Send + 'a;
    type GetTxContextFut<'a>: Future<Output = anyhow::Result<TxContext>> + Send + 'a;
    type GetBlockHashFut<'a>: Future<Output = anyhow::Result<U256>> + Send + 'a;
    type EmitLogFut<'a>: Future<Output = anyhow::Result<()>> + Send + 'a;
    type AccessAccountFut<'a>: Future<Output = anyhow::Result<AccessStatus>> + Send + 'a;
    type AccessStorageFut<'a>: Future<Output = anyhow::Result<AccessStatus>> + Send + 'a;

    /// Check if an account exists.
    fn account_exists(&mut self, address: Address) -> Self::AccountExistsFut<'_>;
    /// Get value of a storage key.
    ///
    /// Returns `Ok(U256::zero())` if does not exist.
    fn get_storage(&mut self, address: Address, key: U256) -> Self::GetStorageFut<'_>;
    /// Set value of a storage key.
    fn set_storage(&mut self, address: Address, key: U256, value: U256) -> Self::SetStorageFut<'_>;
    /// Get balance of an account.
    ///
    /// Returns `Ok(0)` if account does not exist.
    fn get_balance(&mut self, address: Address) -> Self::GetBalanceFut<'_>;
    /// Get code size of an account.
    ///
    /// Returns `Ok(0)` if account does not exist.
    fn get_code_size(&mut self, address: Address) -> Self::GetCodeSizeFut<'_>;
    /// Get code hash of an account.
    ///
    /// Returns `Ok(0)` if account does not exist.
    fn get_code_hash(&mut self, address: Address) -> Self::GetCodeHashFut<'_>;
    /// Copy code of an account.
    ///
    /// Returns `Ok(0)` if offset is invalid.
    fn copy_code<'a>(
        &'a mut self,
        address: Address,
        offset: usize,
        buffer: &'a mut [u8],
    ) -> Self::CopyCodeFut<'a>;
    /// Self-destruct account.
    fn selfdestruct(&mut self, address: Address, beneficiary: Address)
        -> Self::SelfdestructFut<'_>;
    /// Call to another account.
    fn call<'a>(&'a mut self, msg: &'a Message) -> Self::CallFut<'a>;
    /// Retrieve transaction context.
    fn get_tx_context(&mut self) -> Self::GetTxContextFut<'_>;
    /// Get block hash.
    ///
    /// Returns `Ok(U256::zero())` if block does not exist.
    fn get_block_hash(&mut self, block_number: u64) -> Self::GetBlockHashFut<'_>;
    /// Emit a log.
    fn emit_log<'a>(
        &'a mut self,
        address: Address,
        data: &'a [u8],
        topics: &'a [U256],
    ) -> Self::EmitLogFut<'a>;
    /// Mark account as warm, return previous access status.
    ///
    /// Returns `Ok(AccessStatus::Cold)` if account does not exist.
    fn access_account(&mut self, address: Address) -> Self::AccessAccountFut<'_>;
    /// Mark storage key as warm, return previous access status.
    ///
    /// Returns `Ok(AccessStatus::Cold)` if account does not exist.
    fn access_storage(&mut self, address: Address, key: U256) -> Self::AccessStorageFut<'_>;
}

/// Host that does not support any ops.
pub struct DummyHost;

impl Host for DummyHost {
    fn account_exists(&self, _: Address) -> bool {
        todo!()
    }

    fn get_storage(&self, _: Address, _: U256) -> U256 {
        todo!()
    }

    fn set_storage(&mut self, _: Address, _: U256, _: U256) -> StorageStatus {
        todo!()
    }

    fn get_balance(&self, _: Address) -> U256 {
        todo!()
    }

    fn get_code_size(&self, _: Address) -> U256 {
        todo!()
    }

    fn get_code_hash(&self, _: Address) -> U256 {
        todo!()
    }

    fn copy_code(&self, _: Address, _: usize, _: &mut [u8]) -> usize {
        todo!()
    }

    fn selfdestruct(&mut self, _: Address, _: Address) {
        todo!()
    }

    fn call(&mut self, _: &Message) -> Output {
        todo!()
    }

    fn get_tx_context(&self) -> TxContext {
        todo!()
    }

    fn get_block_hash(&self, _: u64) -> U256 {
        todo!()
    }

    fn emit_log(&mut self, _: Address, _: &[u8], _: &[U256]) {
        todo!()
    }

    fn access_account(&mut self, _: Address) -> AccessStatus {
        todo!()
    }

    fn access_storage(&mut self, _: Address, _: U256) -> AccessStatus {
        todo!()
    }
}
