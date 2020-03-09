pub mod difficulty;

use super::Consensus;
use crate::models::*;
use async_trait::*;
use ethereum_types::*;
use std::{collections::BTreeMap, fmt::Debug};
use thiserror::Error;

#[derive(Debug, PartialEq)]
pub struct BlockDifficultyBomb {
    pub delay_to: BlockNumber,
}

#[derive(Debug, PartialEq)]
pub struct BlockEthashParams {
    pub duration_limit: u64,
    pub block_reward: Option<U256>,
    pub homestead_formula: bool,
    pub byzantium_adj_factor: bool,
    pub difficulty_bomb: Option<BlockDifficultyBomb>,
}

#[derive(Debug)]
pub struct Ethash {
    duration_limit: u64,
    block_reward: BTreeMap<BlockNumber, U256>,
    homestead_formula: Option<BlockNumber>,
    byzantium_adj_factor: Option<BlockNumber>,
    difficulty_bomb: Option<DifficultyBomb>,
}

impl Ethash {
    pub fn collect_block_params(&self, block_number: impl Into<BlockNumber>) -> BlockEthashParams {
        let block_number = block_number.into();
        BlockEthashParams {
            duration_limit: self.duration_limit,
            block_reward: self
                .block_reward
                .range(..=block_number)
                .next_back()
                .map(|(_, &v)| v),
            homestead_formula: self
                .homestead_formula
                .map(|transition_block| block_number >= transition_block)
                .unwrap_or(false),
            byzantium_adj_factor: self
                .byzantium_adj_factor
                .map(|transition_block| block_number >= transition_block)
                .unwrap_or(false),
            difficulty_bomb: self
                .difficulty_bomb
                .map(|difficulty_bomb| BlockDifficultyBomb {
                    delay_to: difficulty_bomb
                        .delays
                        .range(..=block_number)
                        .next_back()
                        .map(|(_, &v)| v)
                        .unwrap_or(BlockNumber(0)),
                }),
        }
    }
}

#[derive(Debug, Error)]
pub enum ValidationError {
    #[error("invalid difficulty (expected {expected:?}, got {got:?})")]
    WrongDifficulty { expected: U256, got: U256 },
}

#[async_trait]
impl Consensus for Ethash {
    async fn verify_header(
        &self,
        header: &BlockHeader,
        parent: &BlockHeader,
    ) -> anyhow::Result<()> {
        let block_params = self.collect_block_params(header.number);

        // TODO: port Ethash PoW verification
        // let epoch_number = {header.number / ethash::epoch_length};
        // auto epoch_context{ethash::create_epoch_context(static_cast<int>(epoch_number))};

        // auto boundary256{header.boundary()};
        // auto seal_hash(header.hash(/*for_sealing =*/true));
        // ethash::hash256 sealh256{*reinterpret_cast<ethash::hash256*>(seal_hash.bytes)};
        // ethash::hash256 mixh256{};
        // std::memcpy(mixh256.bytes, header.mix_hash.bytes, 32);

        // uint64_t nonce{endian::load_big_u64(header.nonce.data())};
        // return ethash::verify(*epoch_context, sealh256, mixh256, nonce, boundary256) ? ValidationError::Ok
        //                                                                              : ValidationError::InvalidSeal;

        let parent_has_uncles = parent.ommers_hash != EMPTY_LIST_HASH;
        let difficulty = difficulty::canonical_difficulty(
            header.number,
            header.timestamp,
            parent.difficulty,
            parent.timestamp,
            parent_has_uncles,
            &block_params,
        );
        if difficulty != header.difficulty {
            return Err(ValidationError::WrongDifficulty {
                expected: difficulty,
                got: header.difficulty,
            }
            .into());
        }
        Ok(())
    }
}
