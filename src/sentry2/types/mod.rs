mod block;
mod header;
mod message;
mod penalty;
mod rlp;

pub use self::rlp::*;
pub use block::*;
pub use header::*;
pub use message::*;
pub use penalty::*;

#[derive(Clone, Debug, PartialEq)]
pub enum PeerFilter {
    All,
    Random(u64),
    PeerId(PeerId),
    MinBlock(u64),
}
