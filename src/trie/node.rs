use crate::models::KECCAK_LENGTH;
use bytes::Buf;
use ethereum_types::H256;
use fixedbitset::FixedBitSet;

#[derive(Debug, Default, PartialEq)]
pub(crate) struct Node {
    state_mask: u16,
    tree_mask: u16,
    hash_mask: u16,
    hashes: Vec<H256>,
    root_hash: Option<H256>,
}

fn is_subset(sub: u16, sup: u16) -> bool {
    let intersection = sub & sup;
    intersection == sub
}

impl Node {
    pub fn new(
        state_mask: u16,
        tree_mask: u16,
        hash_mask: u16,
        hashes: Vec<H256>,
        root_hash: Option<H256>,
    ) -> Self {
        assert!(is_subset(tree_mask, state_mask));
        assert!(is_subset(hash_mask, state_mask));
        Self {
            state_mask,
            tree_mask,
            hash_mask,
            hashes,
            root_hash,
        }
    }

    pub fn get_hash_mask(&self) -> u16 {
        self.hash_mask
    }

    pub fn get_hashes(&self) -> &[H256] {
        &self.hashes
    }

    pub fn get_root_hash(&self) -> Option<H256> {
        self.root_hash
    }

    pub fn set_root_hash(&mut self, root_hash: Option<H256>) {
        self.root_hash = root_hash;
    }

    pub fn serialize(&self) -> Vec<u8> {
        let buf_size =
            6 + if self.get_root_hash().is_some() {
                KECCAK_LENGTH
            } else {
                0
            } + self.get_hashes().len() * KECCAK_LENGTH;

        let mut buf = vec![0; buf_size];
        let mut pos = 0;

        for v in [self.state_mask, self.tree_mask, self.hash_mask] {
            buf[pos..pos + 2].copy_from_slice(&v.to_be_bytes());
            pos += 2;
        }

        if let Some(hash) = self.get_root_hash() {
            buf[pos..pos + KECCAK_LENGTH].copy_from_slice(hash.as_bytes());
            pos += KECCAK_LENGTH;
        }

        for hash in self.get_hashes() {
            buf[pos..pos + KECCAK_LENGTH].copy_from_slice(hash.as_bytes());
            pos += KECCAK_LENGTH;
        }

        let _ = pos;
        buf
    }

    pub fn deserialize(mut b: &[u8]) -> Option<Self> {
        if b.len() < 6 {
            // At least state/tree/hash masks need to be present
            return None;
        }

        if (b.len() - 6) % KECCAK_LENGTH != 0 {
            // Beyond the 6th byte the length must be a multiple of hash length
            return None;
        }

        let state_mask = b.get_u16();
        let tree_mask = b.get_u16();
        let hash_mask = b.get_u16();

        let mut root_hash = None;
        if FixedBitSet::with_capacity_and_blocks(16, [u32::from(hash_mask)]).count_ones(..) + 1
            == b.len() / KECCAK_LENGTH
        {
            root_hash = Some(H256::from_slice(&b[..KECCAK_LENGTH]));
            b.advance(KECCAK_LENGTH);
        }

        let num_hashes = b.len() / KECCAK_LENGTH;
        let mut hashes = Vec::with_capacity(num_hashes);
        for _ in 0..num_hashes {
            hashes.push(H256::from_slice(&b[..KECCAK_LENGTH]));
            b.advance(KECCAK_LENGTH);
        }

        Some(Self {
            state_mask,
            tree_mask,
            hash_mask,
            hashes,
            root_hash,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hex_literal::hex;

    #[test]
    fn node_marshalling() {
        for root_hash in [
            None,
            Some(hex!("aaaabbbb0006767767776fffffeee44444000005567645600000000eeddddddd").into()),
        ] {
            let n = Node::new(
                0xf607,
                0x0005,
                0x4004,
                vec![
                    hex!("90d53cd810cc5d4243766cd4451e7b9d14b736a1148b26b3baac7617f617d321").into(),
                    hex!("cc35c964dda53ba6c0b87798073a9628dbc9cd26b5cce88eb69655a9c609caf1").into(),
                ],
                root_hash,
            );

            assert_eq!(
                FixedBitSet::with_capacity_and_blocks(16, [u32::from(n.get_hash_mask())])
                    .count_ones(..),
                n.get_hashes().len()
            );

            let b = n.serialize();

            assert_eq!(Node::deserialize(&b).unwrap(), n);
        }
    }
}
