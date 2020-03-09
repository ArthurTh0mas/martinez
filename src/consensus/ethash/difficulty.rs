use super::BlockEthashParams;
use crate::models::*;
use ethereum_types::*;

const MIN_DIFFICULTY: u64 = 131_072;

pub fn canonical_difficulty(
    block_number: impl Into<BlockNumber>,
    block_timestamp: u64,
    parent_difficulty: U256,
    parent_timestamp: u64,
    parent_has_uncles: bool,
    config: &BlockEthashParams,
) -> U256 {
    let block_number = block_number.into();

    let mut difficulty = parent_difficulty;

    let x = parent_difficulty >> 11; // parent_difficulty / 2048;

    if config.byzantium_adj_factor {
        // Byzantium
        difficulty -= x * 99;

        // https://eips.ethereum.org/EIPS/eip-100
        let y = if parent_has_uncles { 2 } else { 1 };
        let z = (block_timestamp - parent_timestamp) / 9;
        if 99 + y > z {
            difficulty += U256::from(99 + y - z) * x;
        }
    } else if config.homestead_formula {
        // Homestead
        difficulty -= x * 99;

        let z = (block_timestamp - parent_timestamp) / 10;
        if 100 > z {
            difficulty += U256::from(100 - z) * x;
        }
    } else {
        // Frontier
        if block_timestamp - parent_timestamp < 13 {
            difficulty += x;
        } else {
            difficulty -= x;
        }
    }

    if let Some(bomb_config) = config.difficulty_bomb {
        // https://eips.ethereum.org/EIPS/eip-649
        let n = block_number.saturating_sub(bomb_config.delay_to.0) / 100_000;
        if n >= 2 {
            difficulty += U256::one() << (n - 2);
        }

        if difficulty < U256::from(MIN_DIFFICULTY) {
            difficulty = U256::from(MIN_DIFFICULTY);
        }
        difficulty
    } else {
        difficulty
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        chain::config::MAINNET_CONSENSUS_CONFIG,
        consensus::{ethash::Ethash, init_consensus},
    };

    #[test]
    fn difficulty_test_34() {
        let block_number = 0x33e140;
        let block_timestamp = 0x04bdbdaf;
        let parent_difficulty = U256::from(0x7268db7b46b0b154_u64);
        let parent_timestamp = 0x04bdbdaf;
        let parent_has_uncles = false;

        let mainnet_ethash_config = init_consensus(MAINNET_CONSENSUS_CONFIG.clone())
            .unwrap()
            .downcast::<Ethash>()
            .unwrap()
            .collect_block_params(block_number);

        let difficulty = canonical_difficulty(
            block_number,
            block_timestamp,
            parent_difficulty,
            parent_timestamp,
            parent_has_uncles,
            &mainnet_ethash_config,
        );
        assert_eq!(difficulty, U256::from(0x72772897b619876a_u64));
    }
}
