use crate::models::Message;
use ethereum_interfaces::types::H512;

pub type PeerId = H512;

pub enum PenaltyKind {
    BadBlock,
    DuplicateHeader,
    WrongChildBlockHeight,
    WrongChildDifficulty,
    InvalidSeal,
    TooFarFuture,
    TooFarPast,
}

pub struct Penalty {
    pub peer_id: PeerId,
    pub kind: PenaltyKind,
}

pub struct InboundMessage {
    pub msg: Message,
    pub peer_id: PeerId,
}