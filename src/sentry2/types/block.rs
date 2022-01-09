use crate::models::{Block, BlockHeader, BlockNumber, H256};
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
pub struct GetBlockHeaders {
    pub request_id: u64,
    pub params: GetBlockHeadersParams,
}

impl GetBlockHeaders {
    pub fn new(request_id: u64, params: GetBlockHeadersParams) -> Self {
        Self { request_id, params }
    }
}

#[derive(Debug, Clone, PartialEq, RlpEncodable, RlpDecodable)]
pub struct GetBlockHeadersParams {
    pub start: BlockId,
    pub limit: u64,
    pub skip: u64,
    pub reverse: u8,
}

impl GetBlockHeadersParams {
    pub fn new(start: BlockId, limit: u64, skip: u64, reverse: u8) -> Self {
        Self {
            start,
            limit,
            skip,
            reverse,
        }
    }
}

#[derive(Debug, Clone, PartialEq, RlpEncodable, RlpDecodable)]
pub struct BlockHeaders {
    pub request_id: u64,
    pub headers: Vec<BlockHeader>,
}

impl BlockHeaders {
    pub fn new(request_id: u64, headers: Vec<BlockHeader>) -> Self {
        Self {
            request_id,
            headers,
        }
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
