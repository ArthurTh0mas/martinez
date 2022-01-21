use crate::models::{Block, BlockNumber, H256};
use rlp_derive::*;
#[derive(Debug, Copy, Clone, PartialEq)]
pub enum BlockId {
    Hash(H256),
    Number(BlockNumber),
}

#[derive(Debug, Clone, PartialEq, RlpEncodable, RlpDecodable)]
pub struct BlockHashAndNumber {
    pub hash: H256,
    pub number: BlockNumber,
}

#[derive(Debug, Clone, PartialEq, RlpEncodableWrapper, RlpDecodableWrapper)]
pub struct NewBlockHashes(pub Vec<BlockHashAndNumber>);

impl NewBlockHashes {
    fn new(block_hashes: Vec<(H256, BlockNumber)>) -> Self {
        Self(
            block_hashes
                .into_iter()
                .map(|(hash, number)| BlockHashAndNumber { hash, number })
                .collect(),
        )
    }
}

#[derive(Debug, Clone, PartialEq, RlpEncodable, RlpDecodable)]
pub struct NewBlock {
    pub block: Block,
    pub total_difficulty: u128,
}

impl NewBlock {
    pub fn new(block: Block, total_difficulty: u128) -> Self {
        Self {
            block,
            total_difficulty,
        }
    }
}
pub struct BodyRequest {}
