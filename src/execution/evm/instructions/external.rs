use crate::execution::evm::{common::address_to_u256, host::*, state::ExecutionState};
use ethnum::U256;

pub(crate) fn address(state: &mut ExecutionState) {
    state.stack.push(address_to_u256(state.message.recipient));
}

pub(crate) fn caller(state: &mut ExecutionState) {
    state.stack.push(address_to_u256(state.message.sender));
}

pub(crate) fn callvalue(state: &mut ExecutionState) {
    state.stack.push(state.message.value);
}

#[doc(hidden)]
#[macro_export]
macro_rules! balance_async {
    ($state:expr,$gasometer:expr,$rev:expr,$host:expr) => {
        use $crate::execution::evm::{common::*, host::*, instructions::properties::*};

        let address = u256_to_address($state.stack.pop());

        if $rev >= Revision::Berlin {
            if $host.access_account(address) == AccessStatus::Cold {
                $gasometer.subtract(ADDITIONAL_COLD_ACCOUNT_ACCESS_COST)?;
            }
        }

        let balance = $host.get_balance(address).await?;

        $state.stack.push(balance);
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! extcodesize_async {
    ($state:expr,$gasometer:expr,$rev:expr,$host:expr) => {
        use crate::execution::evm::{common::*, host::*, instructions::properties::*};

        let address = u256_to_address($state.stack.pop());

        if $rev >= Revision::Berlin {
            if $host.access_account(address) == AccessStatus::Cold {
                $gasometer.subtract(ADDITIONAL_COLD_ACCOUNT_ACCESS_COST)?;
            }
        }

        let code_size = $host.get_code_size(address).await?;
        $state.stack.push(code_size);
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! push_txcontext_async {
    ($state:expr, $accessor:expr, $host:expr) => {
        let tx_context = $host.get_tx_context();

        $state.stack.push($accessor(tx_context));
    };
}

pub(crate) fn origin_accessor(tx_context: TxContext) -> U256 {
    address_to_u256(tx_context.tx_origin)
}

pub(crate) fn coinbase_accessor(tx_context: TxContext) -> U256 {
    address_to_u256(tx_context.block_coinbase)
}

pub(crate) fn gasprice_accessor(tx_context: TxContext) -> U256 {
    tx_context.tx_gas_price
}

pub(crate) fn timestamp_accessor(tx_context: TxContext) -> U256 {
    tx_context.block_timestamp.into()
}

pub(crate) fn number_accessor(tx_context: TxContext) -> U256 {
    tx_context.block_number.into()
}

pub(crate) fn gaslimit_accessor(tx_context: TxContext) -> U256 {
    tx_context.block_gas_limit.into()
}

pub(crate) fn difficulty_accessor(tx_context: TxContext) -> U256 {
    tx_context.block_difficulty
}

pub(crate) fn chainid_accessor(tx_context: TxContext) -> U256 {
    tx_context.chain_id
}

pub(crate) fn basefee_accessor(tx_context: TxContext) -> U256 {
    tx_context.block_base_fee
}

#[doc(hidden)]
#[macro_export]
macro_rules! selfbalance_async {
    ($state:expr,$host:expr) => {{
        let balance = $host.get_balance($state.message.recipient).await?;

        $state.stack.push(balance);
    }};
}

#[doc(hidden)]
#[macro_export]
macro_rules! blockhash_async {
    ($state:expr,$host:expr) => {
        let number = $state.stack.pop();

        let upper_bound = $host.get_tx_context().block_number;
        let lower_bound = upper_bound.saturating_sub(256);

        let mut header = U256::ZERO;
        if number <= u128::from(u64::MAX) {
            let n = number.as_u64();
            if (lower_bound..upper_bound).contains(&n) {
                header = $host.get_block_hash(n).await?;
            }
        }

        $state.stack.push(header);
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! do_log_async {
    ($state:expr, $gasometer:expr, $num_topics:expr,$host:expr) => {{
        use arrayvec::ArrayVec;

        if $state.message.is_static {
            return Err(StatusCode::StaticModeViolation.into());
        }

        let offset = $state.stack.pop();
        let size = $state.stack.pop();

        let region = memory::get_memory_region($state, $gasometer, offset, size)?;

        if let Some(region) = &region {
            let cost = region.size.get() as u64 * 8;
            $gasometer.subtract(cost)?;
        }

        let mut topics = ArrayVec::<_, 4>::new();
        for _ in 0..$num_topics {
            topics.push($state.stack.pop());
        }

        let data = if let Some(region) = region {
            &$state.memory[region.offset..region.offset + region.size.get()]
        } else {
            &[]
        }
        .to_vec()
        .into();

        $host.emit_log($state.message.recipient, data, topics);
    }};
}

#[doc(hidden)]
#[macro_export]
macro_rules! sload_async {
    ($state:expr,$gasometer:expr,$rev:expr,$host:expr) => {{
        use $crate::execution::evm::{
            host::*,
            instructions::properties::{COLD_SLOAD_COST, WARM_STORAGE_READ_COST},
        };

        let key = $state.stack.pop();

        if $rev >= Revision::Berlin {
            if $host.access_storage($state.message.recipient, key) == AccessStatus::Cold {
                // The warm storage access cost is already applied (from the cost table).
                // Here we need to apply additional cold storage access cost.
                const ADDITIONAL_COLD_SLOAD_COST: u64 = COLD_SLOAD_COST - WARM_STORAGE_READ_COST;
                $gasometer.subtract(ADDITIONAL_COLD_SLOAD_COST)?;
            }
        }

        let storage = $host.get_storage($state.message.recipient, key).await?;

        $state.stack.push(storage);
    }};
}

#[doc(hidden)]
#[macro_export]
macro_rules! sstore_async {
    ($state:expr,$gasometer:expr,$rev:expr,$host:expr) => {{
        use $crate::execution::evm::{
            host::*,
            instructions::properties::{COLD_SLOAD_COST, WARM_STORAGE_READ_COST},
        };

        if $state.message.is_static {
            return Err(StatusCode::StaticModeViolation.into());
        }

        if $rev >= Revision::Istanbul {
            if $gasometer.gas_left() <= 2300 {
                return Err(StatusCode::OutOfGas.into());
            }
        }

        let key = $state.stack.pop();
        let value = $state.stack.pop();

        let mut cost = 0;
        if $rev >= Revision::Berlin {
            if $host.access_storage($state.message.recipient, key) == AccessStatus::Cold {
                cost = COLD_SLOAD_COST;
            }
        }

        let status = $host
            .set_storage($state.message.recipient, key, value)
            .await?;

        cost = match status {
            StorageStatus::Unchanged | StorageStatus::ModifiedAgain => {
                if $rev >= Revision::Berlin {
                    cost + WARM_STORAGE_READ_COST
                } else if $rev == Revision::Istanbul {
                    800
                } else if $rev == Revision::Constantinople {
                    200
                } else {
                    5000
                }
            }
            StorageStatus::Modified | StorageStatus::Deleted => {
                if $rev >= Revision::Berlin {
                    cost + 5000 - COLD_SLOAD_COST
                } else {
                    5000
                }
            }
            StorageStatus::Added => cost + 20000,
        };
        $gasometer.subtract(cost)?;
    }};
}

#[doc(hidden)]
#[macro_export]
macro_rules! selfdestruct_async {
    ($state:expr,$gasometer:expr,$rev:expr,$host:expr) => {{
        use crate::execution::evm::{common::*, host::*, instructions::properties::*};

        if $state.message.is_static {
            return Err(StatusCode::StaticModeViolation.into());
        }

        let beneficiary = u256_to_address($state.stack.pop());

        if $rev >= Revision::Berlin {
            if $host.access_account(beneficiary) == AccessStatus::Cold {
                $gasometer.subtract(COLD_ACCOUNT_ACCESS_COST)?;
            }
        }

        if $rev >= Revision::Tangerine {
            if ($rev == Revision::Tangerine
                || ($host.get_balance($state.message.recipient).await? != 0))
            {
                // After TANGERINE_WHISTLE apply additional cost of
                // sending value to a non-existing account.
                if !$host.account_exists(beneficiary).await? {
                    $gasometer.subtract(25_000)?;
                }
            }
        }

        $host
            .selfdestruct($state.message.recipient, beneficiary)
            .await?;
    }};
}

#[cfg(test)]
mod tests {
    use crate::execution::evm::common::u256_to_address;
    use ethereum_types::Address;
    use hex_literal::hex;

    #[test]
    fn u256_to_address_conversion() {
        assert_eq!(
            u256_to_address(0x42_u128.into()),
            Address::from(hex!("0000000000000000000000000000000000000042"))
        );
    }
}
