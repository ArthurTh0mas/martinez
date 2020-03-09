use crate::{models::*, util::*};
use bytes::Bytes;
use derive_more::Deref;
use ethereum_types::*;
use evmodin::Revision;
use serde::*;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    convert::identity,
    time::Duration,
};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct DifficultyBomb {
    pub delays: BTreeMap<BlockNumber, BlockNumber>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum ConsensusSpec {
    Clique {
        #[serde(deserialize_with = "deserialize_period_as_duration")]
        period: Duration,
        epoch: u64,
    },
    Ethash {
        duration_limit: u64,
        block_reward: BTreeMap<BlockNumber, U256>,
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            with = "::serde_with::rust::unwrap_or_skip"
        )]
        homestead_formula: Option<BlockNumber>,
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            with = "::serde_with::rust::unwrap_or_skip"
        )]
        byzantium_adj_factor: Option<BlockNumber>,
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            with = "::serde_with::rust::unwrap_or_skip"
        )]
        difficulty_bomb: Option<DifficultyBomb>,
    },
    NoProof,
}

impl ConsensusSpec {
    pub fn gather_forks(&self) -> BTreeSet<BlockNumber> {
        match self {
            ConsensusSpec::Ethash {
                duration_limit,
                block_reward,
                homestead_formula,
                byzantium_adj_factor,
                difficulty_bomb,
            } => {
                let mut forks = BTreeSet::new();
                for block in block_reward.keys() {
                    forks.insert(*block);
                }

                if let Some(block) = homestead_formula {
                    forks.insert(*block);
                }

                if let Some(block) = byzantium_adj_factor {
                    forks.insert(*block);
                }

                if let Some(bomb) = difficulty_bomb {
                    for delay in bomb.delays.keys() {
                        forks.insert(*delay);
                    }
                }
                forks.remove(&BlockNumber(0));
                forks
            }
            _ => BTreeSet::new(),
        }
    }
}

struct DeserializePeriodAsDuration;

impl<'de> de::Visitor<'de> for DeserializePeriodAsDuration {
    type Value = Duration;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("an u64")
    }

    fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Duration::from_millis(v))
    }
}

fn deserialize_period_as_duration<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where
    D: de::Deserializer<'de>,
{
    deserializer.deserialize_any(DeserializePeriodAsDuration)
}
