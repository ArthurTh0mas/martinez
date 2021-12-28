#[doc(hidden)]
#[macro_export]
macro_rules! do_call_async {
    ($state:expr, $gasometer:expr, $rev:expr, $kind:expr, $is_static:expr, $host:expr) => {{
        use std::cmp::min;
        use $crate::execution::evm::{
            common::u256_to_address,
            host::*,
            instructions::{memory::MemoryRegion, properties::*},
            CallKind, Message,
        };

        let gas = $state.stack.pop();
        let dst = u256_to_address($state.stack.pop());
        let value = if $is_static || matches!($kind, CallKind::DelegateCall) {
            U256::ZERO
        } else {
            $state.stack.pop()
        };
        let has_value = value != 0;
        let input_offset = $state.stack.pop();
        let input_size = $state.stack.pop();
        let output_offset = $state.stack.pop();
        let output_size = $state.stack.pop();

        $state.stack.push(U256::ZERO); // Assume failure.

        if $rev >= Revision::Berlin {
            if $host.access_account(dst) == AccessStatus::Cold {
                $gasometer.subtract(ADDITIONAL_COLD_ACCOUNT_ACCESS_COST)?;
            }
        }

        let input_region = memory::get_memory_region($state, $gasometer, input_offset, input_size)?;
        let output_region =
            memory::get_memory_region($state, $gasometer, output_offset, output_size)?;

        let mut msg = Message {
            kind: $kind,
            is_static: $is_static || $state.message.is_static,
            depth: $state.message.depth + 1,
            recipient: if matches!($kind, CallKind::Call) {
                dst
            } else {
                $state.message.recipient
            },
            code_address: dst,
            sender: if matches!($kind, CallKind::DelegateCall) {
                $state.message.sender
            } else {
                $state.message.recipient
            },
            gas: i64::MAX as u64,
            value: if matches!($kind, CallKind::DelegateCall) {
                $state.message.value
            } else {
                value
            },
            input_data: input_region
                .map(|MemoryRegion { offset, size }| {
                    $state.memory[offset..offset + size.get()].to_vec().into()
                })
                .unwrap_or_default(),
        };

        let mut cost = if has_value { 9000_u64 } else { 0_u64 };

        if matches!($kind, CallKind::Call) {
            if has_value && $state.message.is_static {
                return Err(StatusCode::StaticModeViolation.into());
            }

            if (has_value || $rev < Revision::Spurious) && !$host.account_exists(dst).await? {
                cost += 25000;
            }
        }
        $gasometer.subtract(cost)?;

        if gas < u128::from(msg.gas) {
            msg.gas = gas.as_u64();
        }

        if $rev >= Revision::Tangerine {
            // TODO: Always true for STATICCALL.
            msg.gas = min(msg.gas, $gasometer.gas_left() - $gasometer.gas_left() / 64);
        } else if msg.gas > $gasometer.gas_left() {
            return Err(StatusCode::OutOfGas.into());
        }

        if has_value {
            msg.gas += 2300; // Add stipend.
            $gasometer.refund(2300);
        }

        $state.return_data.clear();

        if $state.message.depth < 1024
            && !(has_value && $host.get_balance($state.message.recipient).await? < value)
        {
            let msg_gas = msg.gas;
            let result = $host.call(Call::Call(msg)).await?;
            $state.return_data = result.output_data.clone();
            *$state.stack.get_mut(0) = if matches!(result.status_code, StatusCode::Success) {
                U256::ONE
            } else {
                U256::ZERO
            };

            if let Some(MemoryRegion { offset, size }) = output_region {
                let copy_size = min(size.get(), result.output_data.len());
                if copy_size > 0 {
                    $state.memory[offset..offset + copy_size]
                        .copy_from_slice(&result.output_data[..copy_size]);
                }
            }

            let gas_used = msg_gas - result.gas_left;
            $gasometer.subtract_unchecked(gas_used);
        }
    }};
}

#[doc(hidden)]
#[macro_export]
macro_rules! do_create_async {
    ($state:expr, $gasometer:expr, $rev:expr, $create2:expr, $host:expr) => {{
        use ethnum::U256;
        use $crate::execution::evm::{common::*, host::*, CreateMessage};

        if $state.message.is_static {
            return Err(StatusCode::StaticModeViolation.into());
        }

        let endowment = $state.stack.pop();
        let init_code_offset = $state.stack.pop();
        let init_code_size = $state.stack.pop();

        let region =
            memory::get_memory_region($state, $gasometer, init_code_offset, init_code_size)?;

        let salt = if $create2 {
            let salt = $state.stack.pop();

            if let Some(region) = &region {
                let salt_cost = memory::num_words(region.size.get()) * 6;
                $gasometer.subtract(salt_cost)?;
            }

            Some(salt)
        } else {
            None
        };

        $state.stack.push(U256::ZERO);
        $state.return_data.clear();

        if $state.message.depth < 1024
            && !(endowment != 0 && $host.get_balance($state.message.recipient).await? < endowment)
        {
            let msg = CreateMessage {
                gas: if $rev >= Revision::Tangerine {
                    $gasometer.gas_left() - $gasometer.gas_left() / 64
                } else {
                    $gasometer.gas_left()
                },

                salt,
                initcode: if init_code_size != 0 {
                    $state.memory[init_code_offset.as_usize()
                        ..init_code_offset.as_usize() + init_code_size.as_usize()]
                        .to_vec()
                        .into()
                } else {
                    Bytes::new()
                },
                sender: $state.message.recipient,
                depth: $state.message.depth + 1,
                endowment,
            };
            let msg_gas = msg.gas;
            let result = $host.call(Call::Create(msg)).await?;
            let gas_used = msg_gas - result.gas_left;
            $gasometer.subtract_unchecked(gas_used);

            $state.return_data = result.output_data;
            if result.status_code == StatusCode::Success {
                *$state.stack.get_mut(0) =
                    address_to_u256(result.create_address.expect("expected create address"));
            }
        }
    }};
}
