mod call_tracer;
mod eip3155_tracer;

use crate::{
    execution::evm::{ExecutionState, OpCode, StatusCode},
    models::*,
};
use bytes::Bytes;
use std::collections::{BTreeMap, HashMap};

pub use call_tracer::*;
pub use eip3155_tracer::*;

#[derive(Clone, Debug, PartialEq)]
pub enum CodeKind {
    Precompile,
    Bytecode(Option<Bytes>),
}

#[derive(Clone, Debug, PartialEq)]
pub enum CallKind {
    Call,
    CallCode,
    DelegateCall,
    StaticCall,
}

#[derive(Clone, Debug, PartialEq)]
pub enum MessageKind {
    Create,
    Call {
        call_kind: CallKind,
        code_kind: CodeKind,
    },
}

#[allow(unused, clippy::too_many_arguments)]
pub trait Tracer: Send + 'static {
    fn capture_start(
        &mut self,
        depth: u16,
        from: Address,
        to: Address,
        call_type: MessageKind,
        input: Bytes,
        gas: u64,
        value: U256,
    ) {
    }
    fn capture_state(
        &mut self,
        env: &ExecutionState,
        pc: u64,
        op: OpCode,
        cost: u64,
        return_data: Bytes,
        depth: u16,
        err: StatusCode,
    ) {
    }
    fn capture_end(&mut self, depth: u16, output: Bytes, gas_left: u64, err: StatusCode) {}
    fn capture_self_destruct(&mut self, caller: Address, beneficiary: Address) {}
    fn capture_account_read(&mut self, account: Address) {}
    fn capture_account_write(&mut self, account: Address) {}
}
