use self::instruction_table::*;
use super::{
    common::{InterpreterMessage, *},
    instructions::{control::*, stack_manip::*, *},
    state::*,
    *,
};
use crate::{execution::tracer::Tracer, models::*};
use ethnum::U256;
use std::sync::Arc;

fn check_requirements(
    instruction_table: &InstructionTable,
    state: &mut ExecutionState,
    op: OpCode,
) -> Result<(), StatusCode> {
    let metrics = instruction_table[op.to_usize()]
        .as_ref()
        .ok_or(StatusCode::UndefinedInstruction)?;

    state.gas_left -= metrics.gas_cost as i64;
    if state.gas_left < 0 {
        return Err(StatusCode::OutOfGas);
    }

    let stack_size = state.stack.len();
    if stack_size == STACK_SIZE {
        if metrics.can_overflow_stack {
            return Err(StatusCode::StackOverflow);
        }
    } else if stack_size < metrics.stack_height_required.into() {
        return Err(StatusCode::StackUnderflow);
    }

    Ok(())
}

#[derive(Clone, Debug)]
pub struct JumpdestMap(Arc<[bool]>);

impl JumpdestMap {
    pub fn contains(&self, dst: U256) -> bool {
        dst < u128::try_from(self.0.len()).unwrap() && self.0[dst.as_usize()]
    }
}

/// Code with analysis.
#[derive(Clone, Debug)]
pub struct AnalyzedCode {
    jumpdest_map: JumpdestMap,
    code: Bytes,
    padded_code: Bytes,
}

impl AnalyzedCode {
    /// Analyze code and prepare it for execution.
    pub fn analyze(code: &[u8]) -> Self {
        let mut jumpdest_map = vec![false; code.len()];

        let mut i = 0;
        while i < code.len() {
            let opcode = OpCode(code[i]);
            i += match opcode {
                OpCode::JUMPDEST => {
                    jumpdest_map[i] = true;
                    1
                }
                OpCode::PUSH1
                | OpCode::PUSH2
                | OpCode::PUSH3
                | OpCode::PUSH4
                | OpCode::PUSH5
                | OpCode::PUSH6
                | OpCode::PUSH7
                | OpCode::PUSH8
                | OpCode::PUSH9
                | OpCode::PUSH10
                | OpCode::PUSH11
                | OpCode::PUSH12
                | OpCode::PUSH13
                | OpCode::PUSH14
                | OpCode::PUSH15
                | OpCode::PUSH16
                | OpCode::PUSH17
                | OpCode::PUSH18
                | OpCode::PUSH19
                | OpCode::PUSH20
                | OpCode::PUSH21
                | OpCode::PUSH22
                | OpCode::PUSH23
                | OpCode::PUSH24
                | OpCode::PUSH25
                | OpCode::PUSH26
                | OpCode::PUSH27
                | OpCode::PUSH28
                | OpCode::PUSH29
                | OpCode::PUSH30
                | OpCode::PUSH31
                | OpCode::PUSH32 => opcode.to_usize() - OpCode::PUSH1.to_usize() + 2,
                _ => 1,
            }
        }

        let code_len = code.len();

        let mut padded_code = code.to_vec();
        padded_code.resize(i + 1, OpCode::STOP.to_u8());

        let jumpdest_map = JumpdestMap(jumpdest_map.into());
        let padded_code = Bytes::from(padded_code);
        let mut code = padded_code.clone();
        code.truncate(code_len);

        Self {
            jumpdest_map,
            code,
            padded_code,
        }
    }

    /// Execute analyzed EVM bytecode using provided `Host` context.
    pub fn execute<H, T>(
        self,
        host: &mut H,
        tracer: &mut T,
        message: InterpreterMessage,
        revision: Revision,
    ) -> Output
    where
        H: Host,
        T: Tracer + ?Sized,
    {
        let state = ExecutionState::new(message);
        let f = match (tracer.trace_instructions(), revision) {
            (true, Revision::Frontier) => execute_message::<H, T, true, { Revision::Frontier }>,
            (true, Revision::Homestead) => execute_message::<H, T, true, { Revision::Homestead }>,
            (true, Revision::Tangerine) => execute_message::<H, T, true, { Revision::Tangerine }>,
            (true, Revision::Spurious) => execute_message::<H, T, true, { Revision::Spurious }>,
            (true, Revision::Byzantium) => execute_message::<H, T, true, { Revision::Byzantium }>,
            (true, Revision::Constantinople) => {
                execute_message::<H, T, true, { Revision::Constantinople }>
            }
            (true, Revision::Petersburg) => execute_message::<H, T, true, { Revision::Petersburg }>,
            (true, Revision::Istanbul) => execute_message::<H, T, true, { Revision::Istanbul }>,
            (true, Revision::Berlin) => execute_message::<H, T, true, { Revision::Berlin }>,
            (true, Revision::London) => execute_message::<H, T, true, { Revision::London }>,
            (true, Revision::Shanghai) => execute_message::<H, T, true, { Revision::Shanghai }>,
            (false, Revision::Frontier) => execute_message::<H, T, false, { Revision::Frontier }>,
            (false, Revision::Homestead) => execute_message::<H, T, false, { Revision::Homestead }>,
            (false, Revision::Tangerine) => execute_message::<H, T, false, { Revision::Tangerine }>,
            (false, Revision::Spurious) => execute_message::<H, T, false, { Revision::Spurious }>,
            (false, Revision::Byzantium) => execute_message::<H, T, false, { Revision::Byzantium }>,
            (false, Revision::Constantinople) => {
                execute_message::<H, T, false, { Revision::Constantinople }>
            }
            (false, Revision::Petersburg) => {
                execute_message::<H, T, false, { Revision::Petersburg }>
            }
            (false, Revision::Istanbul) => execute_message::<H, T, false, { Revision::Istanbul }>,
            (false, Revision::Berlin) => execute_message::<H, T, false, { Revision::Berlin }>,
            (false, Revision::London) => execute_message::<H, T, false, { Revision::London }>,
            (false, Revision::Shanghai) => execute_message::<H, T, false, { Revision::Shanghai }>,
        };

        let output = match (f)(self, state, host, tracer) {
            Ok(output) => output.into(),
            Err(status_code) => Output {
                status_code,
                gas_left: 0,
                output_data: Bytes::new(),
                create_address: None,
            },
        };

        tracer.capture_end(&output);

        output
    }
}

#[allow(clippy::needless_borrow)]
fn execute_message<H, T, const TRACE: bool, const REVISION: Revision>(
    s: AnalyzedCode,
    mut state: ExecutionState,
    host: &mut H,
    tracer: &mut T,
) -> Result<SuccessfulOutput, StatusCode>
where
    H: Host,
    T: Tracer + ?Sized,
{
    let instruction_table = get_instruction_table(REVISION);

    let mut reverted = false;

    let mut pc = 0;

    loop {
        let op = OpCode(s.padded_code[pc]);

        let metrics = instruction_table[op.to_usize()]
            .as_ref()
            .ok_or(StatusCode::UndefinedInstruction)?;

        if TRACE {
            // Do not print stop on the final STOP
            if pc < s.code.len() {
                tracer.capture_state(
                    &state,
                    pc,
                    op,
                    metrics.gas_cost as u64,
                    state.message.depth as u16,
                );
            }
        }

        check_requirements(instruction_table, &mut state, op)?;

        match op {
            OpCode::STOP => {
                break;
            }
            OpCode::ADD => {
                arithmetic::add(&mut state.stack);
            }
            OpCode::MUL => {
                arithmetic::mul(&mut state.stack);
            }
            OpCode::SUB => {
                arithmetic::sub(&mut state.stack);
            }
            OpCode::DIV => {
                arithmetic::div(&mut state.stack);
            }
            OpCode::SDIV => {
                arithmetic::sdiv(&mut state.stack);
            }
            OpCode::MOD => {
                arithmetic::modulo(&mut state.stack);
            }
            OpCode::SMOD => {
                arithmetic::smod(&mut state.stack);
            }
            OpCode::ADDMOD => {
                arithmetic::addmod(&mut state.stack);
            }
            OpCode::MULMOD => {
                arithmetic::mulmod(&mut state.stack);
            }
            OpCode::EXP => {
                arithmetic::exp::<REVISION>(&mut state)?;
            }
            OpCode::SIGNEXTEND => {
                arithmetic::signextend(&mut state.stack);
            }
            OpCode::LT => {
                boolean::lt(&mut state.stack);
            }
            OpCode::GT => {
                boolean::gt(&mut state.stack);
            }
            OpCode::SLT => {
                boolean::slt(&mut state.stack);
            }
            OpCode::SGT => {
                boolean::sgt(&mut state.stack);
            }
            OpCode::EQ => {
                boolean::eq(&mut state.stack);
            }
            OpCode::ISZERO => {
                boolean::iszero(&mut state.stack);
            }
            OpCode::AND => {
                boolean::and(&mut state.stack);
            }
            OpCode::OR => {
                boolean::or(&mut state.stack);
            }
            OpCode::XOR => {
                boolean::xor(&mut state.stack);
            }
            OpCode::NOT => {
                boolean::not(&mut state.stack);
            }
            OpCode::BYTE => {
                bitwise::byte(&mut state.stack);
            }
            OpCode::SHL => {
                bitwise::shl(&mut state.stack);
            }
            OpCode::SHR => {
                bitwise::shr(&mut state.stack);
            }
            OpCode::SAR => {
                bitwise::sar(&mut state.stack);
            }

            OpCode::KECCAK256 => {
                memory::keccak256(&mut state)?;
            }
            OpCode::ADDRESS => {
                external::address(&mut state);
            }
            OpCode::BALANCE => {
                balance!(&mut state, host, REVISION);
            }
            OpCode::CALLER => {
                external::caller(&mut state);
            }
            OpCode::CALLVALUE => {
                external::callvalue(&mut state);
            }
            OpCode::CALLDATALOAD => {
                calldataload(&mut state);
            }
            OpCode::CALLDATASIZE => {
                calldatasize(&mut state);
            }
            OpCode::CALLDATACOPY => {
                memory::calldatacopy(&mut state)?;
            }
            OpCode::CODESIZE => {
                memory::codesize(&mut state.stack, &s.code[..]);
            }
            OpCode::CODECOPY => {
                memory::codecopy(&mut state, &s.code[..])?;
            }
            OpCode::EXTCODESIZE => {
                extcodesize!(&mut state, host, REVISION);
            }
            OpCode::EXTCODECOPY => {
                extcodecopy!(state, host, REVISION);
            }
            OpCode::RETURNDATASIZE => {
                memory::returndatasize(&mut state);
            }
            OpCode::RETURNDATACOPY => {
                memory::returndatacopy(&mut state)?;
            }
            OpCode::EXTCODEHASH => {
                extcodehash!(state, host, REVISION);
            }
            OpCode::BLOCKHASH => {
                blockhash!(state, host);
            }
            OpCode::ORIGIN
            | OpCode::COINBASE
            | OpCode::GASPRICE
            | OpCode::TIMESTAMP
            | OpCode::NUMBER
            | OpCode::DIFFICULTY
            | OpCode::GASLIMIT
            | OpCode::CHAINID
            | OpCode::BASEFEE => {
                state.stack.push(match op {
                    OpCode::ORIGIN => external::origin_accessor,
                    OpCode::COINBASE => external::coinbase_accessor,
                    OpCode::GASPRICE => external::gasprice_accessor,
                    OpCode::TIMESTAMP => external::timestamp_accessor,
                    OpCode::NUMBER => external::number_accessor,
                    OpCode::DIFFICULTY => external::difficulty_accessor,
                    OpCode::GASLIMIT => external::gaslimit_accessor,
                    OpCode::CHAINID => external::chainid_accessor,
                    OpCode::BASEFEE => external::basefee_accessor,
                    _ => unreachable!(),
                }(host.get_tx_context()));
            }
            OpCode::SELFBALANCE => {
                selfbalance!(state, host);
            }
            OpCode::POP => pop(&mut state.stack),
            OpCode::MLOAD => memory::mload(&mut state)?,
            OpCode::MSTORE => memory::mstore(&mut state)?,
            OpCode::MSTORE8 => memory::mstore8(&mut state)?,
            OpCode::JUMP => {
                pc = op_jump(&mut state, &s.jumpdest_map)?;

                continue;
            }
            OpCode::JUMPI => {
                if *state.stack.get(1) != 0 {
                    pc = op_jump(&mut state, &s.jumpdest_map)?;
                    state.stack.pop();

                    continue;
                } else {
                    state.stack.pop();
                    state.stack.pop();
                }
            }
            OpCode::PC => state.stack.push(u128::try_from(pc).unwrap().into()),
            OpCode::MSIZE => memory::msize(&mut state),
            OpCode::SLOAD => {
                sload!(state, host, REVISION);
            }
            OpCode::SSTORE => {
                sstore!(state, host, REVISION);
            }
            OpCode::GAS => state
                .stack
                .push(u128::try_from(state.gas_left).unwrap().into()),
            OpCode::JUMPDEST => {}
            OpCode::PUSH1 => {
                push1(&mut state.stack, s.padded_code[pc + 1]);
                pc += 1;
            }
            OpCode::PUSH2 => pc += push::<2>(&mut state.stack, &s.padded_code[pc + 1..]),
            OpCode::PUSH3 => pc += push::<3>(&mut state.stack, &s.padded_code[pc + 1..]),
            OpCode::PUSH4 => pc += push::<4>(&mut state.stack, &s.padded_code[pc + 1..]),
            OpCode::PUSH5 => pc += push::<5>(&mut state.stack, &s.padded_code[pc + 1..]),
            OpCode::PUSH6 => pc += push::<6>(&mut state.stack, &s.padded_code[pc + 1..]),
            OpCode::PUSH7 => pc += push::<7>(&mut state.stack, &s.padded_code[pc + 1..]),
            OpCode::PUSH8 => pc += push::<8>(&mut state.stack, &s.padded_code[pc + 1..]),
            OpCode::PUSH9 => pc += push::<9>(&mut state.stack, &s.padded_code[pc + 1..]),
            OpCode::PUSH10 => pc += push::<10>(&mut state.stack, &s.padded_code[pc + 1..]),
            OpCode::PUSH11 => pc += push::<11>(&mut state.stack, &s.padded_code[pc + 1..]),
            OpCode::PUSH12 => pc += push::<12>(&mut state.stack, &s.padded_code[pc + 1..]),
            OpCode::PUSH13 => pc += push::<13>(&mut state.stack, &s.padded_code[pc + 1..]),
            OpCode::PUSH14 => pc += push::<14>(&mut state.stack, &s.padded_code[pc + 1..]),
            OpCode::PUSH15 => pc += push::<15>(&mut state.stack, &s.padded_code[pc + 1..]),
            OpCode::PUSH16 => pc += push::<16>(&mut state.stack, &s.padded_code[pc + 1..]),
            OpCode::PUSH17 => pc += push::<17>(&mut state.stack, &s.padded_code[pc + 1..]),
            OpCode::PUSH18 => pc += push::<18>(&mut state.stack, &s.padded_code[pc + 1..]),
            OpCode::PUSH19 => pc += push::<19>(&mut state.stack, &s.padded_code[pc + 1..]),
            OpCode::PUSH20 => pc += push::<20>(&mut state.stack, &s.padded_code[pc + 1..]),
            OpCode::PUSH21 => pc += push::<21>(&mut state.stack, &s.padded_code[pc + 1..]),
            OpCode::PUSH22 => pc += push::<22>(&mut state.stack, &s.padded_code[pc + 1..]),
            OpCode::PUSH23 => pc += push::<23>(&mut state.stack, &s.padded_code[pc + 1..]),
            OpCode::PUSH24 => pc += push::<24>(&mut state.stack, &s.padded_code[pc + 1..]),
            OpCode::PUSH25 => pc += push::<25>(&mut state.stack, &s.padded_code[pc + 1..]),
            OpCode::PUSH26 => pc += push::<26>(&mut state.stack, &s.padded_code[pc + 1..]),
            OpCode::PUSH27 => pc += push::<27>(&mut state.stack, &s.padded_code[pc + 1..]),
            OpCode::PUSH28 => pc += push::<28>(&mut state.stack, &s.padded_code[pc + 1..]),
            OpCode::PUSH29 => pc += push::<29>(&mut state.stack, &s.padded_code[pc + 1..]),
            OpCode::PUSH30 => pc += push::<30>(&mut state.stack, &s.padded_code[pc + 1..]),
            OpCode::PUSH31 => pc += push::<31>(&mut state.stack, &s.padded_code[pc + 1..]),
            OpCode::PUSH32 => {
                push32(&mut state.stack, &s.padded_code[pc + 1..]);
                pc += 32;
            }

            OpCode::DUP1 => dup::<1>(&mut state.stack),
            OpCode::DUP2 => dup::<2>(&mut state.stack),
            OpCode::DUP3 => dup::<3>(&mut state.stack),
            OpCode::DUP4 => dup::<4>(&mut state.stack),
            OpCode::DUP5 => dup::<5>(&mut state.stack),
            OpCode::DUP6 => dup::<6>(&mut state.stack),
            OpCode::DUP7 => dup::<7>(&mut state.stack),
            OpCode::DUP8 => dup::<8>(&mut state.stack),
            OpCode::DUP9 => dup::<9>(&mut state.stack),
            OpCode::DUP10 => dup::<10>(&mut state.stack),
            OpCode::DUP11 => dup::<11>(&mut state.stack),
            OpCode::DUP12 => dup::<12>(&mut state.stack),
            OpCode::DUP13 => dup::<13>(&mut state.stack),
            OpCode::DUP14 => dup::<14>(&mut state.stack),
            OpCode::DUP15 => dup::<15>(&mut state.stack),
            OpCode::DUP16 => dup::<16>(&mut state.stack),

            OpCode::SWAP1 => swap::<1>(&mut state.stack),
            OpCode::SWAP2 => swap::<2>(&mut state.stack),
            OpCode::SWAP3 => swap::<3>(&mut state.stack),
            OpCode::SWAP4 => swap::<4>(&mut state.stack),
            OpCode::SWAP5 => swap::<5>(&mut state.stack),
            OpCode::SWAP6 => swap::<6>(&mut state.stack),
            OpCode::SWAP7 => swap::<7>(&mut state.stack),
            OpCode::SWAP8 => swap::<8>(&mut state.stack),
            OpCode::SWAP9 => swap::<9>(&mut state.stack),
            OpCode::SWAP10 => swap::<10>(&mut state.stack),
            OpCode::SWAP11 => swap::<11>(&mut state.stack),
            OpCode::SWAP12 => swap::<12>(&mut state.stack),
            OpCode::SWAP13 => swap::<13>(&mut state.stack),
            OpCode::SWAP14 => swap::<14>(&mut state.stack),
            OpCode::SWAP15 => swap::<15>(&mut state.stack),
            OpCode::SWAP16 => swap::<16>(&mut state.stack),

            OpCode::LOG0 | OpCode::LOG1 | OpCode::LOG2 | OpCode::LOG3 | OpCode::LOG4 => {
                do_log!(&mut state, host, op.0 - OpCode::LOG0.0);
            }
            OpCode::CREATE | OpCode::CREATE2 => {
                do_create!(&mut state, host, REVISION, op == OpCode::CREATE2);
            }
            OpCode::CALL | OpCode::CALLCODE | OpCode::DELEGATECALL | OpCode::STATICCALL => {
                do_call!(
                    &mut state,
                    host,
                    REVISION,
                    match op {
                        OpCode::CALL | OpCode::STATICCALL => CallKind::Call,
                        OpCode::CALLCODE => CallKind::CallCode,
                        OpCode::DELEGATECALL => CallKind::DelegateCall,
                        _ => unreachable!(),
                    },
                    op == OpCode::STATICCALL
                );
            }
            OpCode::RETURN | OpCode::REVERT => {
                ret(&mut state)?;
                reverted = op == OpCode::REVERT;
                break;
            }
            OpCode::INVALID => {
                return Err(StatusCode::InvalidInstruction);
            }
            OpCode::SELFDESTRUCT => {
                selfdestruct!(state, host, REVISION);
                break;
            }
            other => {
                unreachable!("reached unhandled opcode: {}", other);
            }
        }

        pc += 1;
    }

    let output = SuccessfulOutput {
        reverted,
        gas_left: state.gas_left,
        output_data: state.output_data.clone(),
    };

    Ok(output)
}
