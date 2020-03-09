use crate::models::*;
use once_cell::sync::Lazy;

pub static MAINNET_CONFIG: Lazy<ChainSpec> =
    Lazy::new(|| ron::from_str(include_str!("../models/chains/ethereum/chain.ron")).unwrap());
pub static RINKEBY_CONFIG: Lazy<ChainSpec> =
    Lazy::new(|| ron::from_str(include_str!("../models/chains/rinkeby/chain.ron")).unwrap());

pub static MAINNET_CONSENSUS_CONFIG: Lazy<ConsensusSpec> =
    Lazy::new(|| ron::from_str(include_str!("../models/chains/ethereum/consensus.ron")).unwrap());
