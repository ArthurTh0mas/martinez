use bytes::Bytes;
pub use common::{
    CallKind, CreateMessage, Message, Output, Revision, StatusCode, SuccessfulOutput,
};
pub use interpreter::AnalyzedCode;
pub use opcode::OpCode;
pub use state::{ExecutionState, Stack};

/// Maximum allowed EVM bytecode size.
pub const MAX_CODE_SIZE: usize = 0x6000;

mod common;
pub mod host;
#[macro_use]
pub mod instructions;
mod interpreter;
pub mod opcode;
mod state;
