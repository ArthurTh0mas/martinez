use crate::{
    models::{BlockHeader, BlockNumber, H256},
    sentry2::types::BlockId,
};
use rlp_derive::*;

impl From<BlockNumber> for u64 {
    fn from(block_number: BlockNumber) -> Self {
        block_number.0
    }
}

#[derive(Debug, Clone)]
pub struct HeaderRequest {
    pub hash: H256,
    pub number: BlockNumber,
    pub limit: u64,
    pub skip: Option<u64>,
    pub reverse: bool,
}

impl HeaderRequest {
    pub fn new(
        hash: H256,
        number: BlockNumber,
        limit: u64,
        skip: Option<u64>,
        reverse: bool,
    ) -> Self {
        Self {
            hash,
            number,
            limit,
            skip,
            reverse,
        }
    }
}

impl Default for HeaderRequest {
    fn default() -> Self {
        HeaderRequest {
            hash: Default::default(),
            number: Default::default(),
            limit: 192,
            skip: None,
            reverse: false,
        }
    }
}

pub struct Announce {
    pub hash: H256,
    pub number: BlockNumber,
}

impl Announce {
    pub fn new(hash: H256, number: BlockNumber) -> Self {
        Self { hash, number }
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
