use crate::models::{BlockNumber, H256};

impl From<BlockNumber> for u64 {
    fn from(block_number: BlockNumber) -> Self {
        block_number.0
    }
}

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
