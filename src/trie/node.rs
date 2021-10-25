#![allow(clippy::if_same_then_else)]
use crate::trie::util::assert_subset;
use ethereum_types::H256;

#[derive(Clone, Debug, PartialEq)]
pub struct Node {
    pub state_mask: u16,
    pub tree_mask: u16,
    pub hash_mask: u16,
    pub hashes: Vec<H256>,
    pub root_hash: Option<H256>,
}

impl Node {
    pub fn new(
        state_mask: u16,
        tree_mask: u16,
        hash_mask: u16,
        hashes: Vec<H256>,
        root_hash: Option<H256>,
    ) -> Self {
        assert_subset(tree_mask, state_mask);
        assert_subset(hash_mask, state_mask);
        assert_eq!(hash_mask.count_ones() as usize, hashes.len());
        Self {
            state_mask,
            tree_mask,
            hash_mask,
            hashes,
            root_hash,
        }
    }
}
