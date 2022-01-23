use ethereum_interfaces::{sentry as grpc_sentry, types::H512};

pub type PeerId = H512;

#[derive(Debug, Clone)]
pub enum PenaltyKind {
    BadBlock,
    DuplicateHeader,
    WrongChildBlockHeight,
    WrongChildDifficulty,
    InvalidSeal,
    TooFarFuture,
    TooFarPast,
}

#[derive(Debug, Clone)]
pub struct Penalty {
    pub peer_id: PeerId,
    pub kind: PenaltyKind,
}

impl From<Penalty> for grpc_sentry::PenalizePeerRequest {
    fn from(penalty: Penalty) -> Self {
        grpc_sentry::PenalizePeerRequest {
            peer_id: Some(penalty.peer_id),
            penalty: 0,
        }
    }
}
