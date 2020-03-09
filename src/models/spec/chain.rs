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

type NodeUrl = String;

#[derive(Debug, PartialEq)]
pub struct BlockChainSpec {
    pub revision: Revision,
    pub active_transitions: HashSet<Revision>,
    pub params: Params,
    pub system_contracts: HashMap<Address, Contract>,
    pub balance_changes: HashMap<Address, U256>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Deref)]
pub struct Spec {
    pub consensus: ConsensusSpec,
    #[deref]
    pub chain: ChainSpec,
    pub bootnodes: Vec<NodeUrl>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct ChainSpec {
    pub name: String,
    #[serde(default)]
    pub upgrades: Upgrades,
    pub params: Params,
    pub genesis: Genesis,
    pub contracts: BTreeMap<BlockNumber, HashMap<Address, Contract>>,
    pub balances: BTreeMap<BlockNumber, HashMap<Address, U256>>,
}

impl ChainSpec {
    pub fn collect_block_spec(&self, block_number: impl Into<BlockNumber>) -> BlockChainSpec {
        let block_number = block_number.into();
        let mut revision = Revision::Frontier;
        let mut active_transitions = HashSet::new();
        for (fork, r) in [
            (self.upgrades.london, Revision::London),
            (self.upgrades.berlin, Revision::Berlin),
            (self.upgrades.istanbul, Revision::Istanbul),
            (self.upgrades.petersburg, Revision::Petersburg),
            (self.upgrades.constantinople, Revision::Constantinople),
            (self.upgrades.byzantium, Revision::Byzantium),
            (self.upgrades.spurious, Revision::Spurious),
            (self.upgrades.tangerine, Revision::Tangerine),
            (self.upgrades.homestead, Revision::Homestead),
        ] {
            if let Some(fork_block) = fork {
                if block_number >= fork_block {
                    if block_number == fork_block {
                        active_transitions.insert(r);
                    }
                    if revision == Revision::Frontier {
                        revision = r;
                    }

                    break;
                }
            }
        }

        BlockChainSpec {
            revision,
            active_transitions,
            params: self.params.clone(),
            system_contracts: self.contracts.range(..=block_number).fold(
                HashMap::new(),
                |acc, (_, contracts)| {
                    for (addr, contract) in contracts {
                        acc.insert(*addr, *contract);
                    }

                    acc
                },
            ),
            balance_changes: self
                .balances
                .get(&block_number)
                .cloned()
                .unwrap_or_default(),
        }
    }

    pub fn gather_forks(&self) -> BTreeSet<BlockNumber> {
        let mut forks = [
            self.upgrades.homestead,
            self.upgrades.tangerine,
            self.upgrades.spurious,
            self.upgrades.byzantium,
            self.upgrades.constantinople,
            self.upgrades.petersburg,
            self.upgrades.istanbul,
            self.upgrades.berlin,
            self.upgrades.london,
        ]
        .iter()
        .copied()
        .filter_map(identity)
        .chain(self.contracts.keys().copied())
        .chain(self.balances.keys().copied())
        .collect::<BTreeSet<BlockNumber>>();

        forks.remove(&BlockNumber(0));

        forks
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct EthashGenesis {
    pub nonce: H64,
    pub mix_hash: H256,
}

// deserialize_str_as_u64
#[derive(Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct Upgrades {
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "::serde_with::rust::unwrap_or_skip"
    )]
    pub homestead: Option<BlockNumber>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "::serde_with::rust::unwrap_or_skip"
    )]
    pub tangerine: Option<BlockNumber>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "::serde_with::rust::unwrap_or_skip"
    )]
    pub spurious: Option<BlockNumber>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "::serde_with::rust::unwrap_or_skip"
    )]
    pub byzantium: Option<BlockNumber>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "::serde_with::rust::unwrap_or_skip"
    )]
    pub constantinople: Option<BlockNumber>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "::serde_with::rust::unwrap_or_skip"
    )]
    pub petersburg: Option<BlockNumber>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "::serde_with::rust::unwrap_or_skip"
    )]
    pub istanbul: Option<BlockNumber>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "::serde_with::rust::unwrap_or_skip"
    )]
    pub berlin: Option<BlockNumber>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "::serde_with::rust::unwrap_or_skip"
    )]
    pub london: Option<BlockNumber>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Params {
    pub chain_id: u64,
    pub network_id: u64,
    pub maximum_extra_data_size: u64,
    pub min_gas_limit: u64,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub enum Seal {
    Ethash { nonce: H64, mix_hash: H256 },
    Clique { vanity: H256, signers: Vec<Address> },
    AuthorityRound { step: usize, signature: H520 },
    Raw { bytes: Vec<u8> },
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct Genesis {
    pub author: Address,
    pub difficulty: U256,
    pub gas_limit: u64,
    pub timestamp: u64,
    pub seal: Seal,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub enum Contract {
    Contract {
        #[serde(deserialize_with = "deserialize_str_as_bytes")]
        code: Bytes,
    },
    Precompile(Precompile),
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub enum ModExpVersion {
    ModExp198,
    ModExp2565,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub enum Precompile {
    EcRecover { base: u64, word: u64 },
    Sha256 { base: u64, word: u64 },
    Ripemd160 { base: u64, word: u64 },
    Identity { base: u64, word: u64 },
    ModExp { version: ModExpVersion },
    AltBn128Add { price: u64 },
    AltBn128Mul { price: u64 },
    AltBn128Pairing { base: u64, pair: u64 },
    Blake2F { gas_per_round: u64 },
}

fn deserialize_str_as_u64<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: de::Deserializer<'de>,
{
    U64::deserialize(deserializer).map(|num| num.as_u64())
}
#[cfg(test)]
mod tests {
    use crate::chain::config::RINKEBY_CONFIG;

    use super::*;
    use hex_literal::hex;
    use maplit::*;

    #[test]
    fn load_chainspec() {
        assert_eq!(
            ChainSpec {
                name: "Rinkeby".into(),
                upgrades: Upgrades {
                    homestead: Some(1150000.into()),
                    tangerine: Some(2463000.into()),
                    spurious: Some(2675000.into()),
                    byzantium: Some(4370000.into()),
                    constantinople: Some(7280000.into()),
                    petersburg: Some(7280000.into()),
                    istanbul: Some(9069000.into()),
                    berlin: Some(12244000.into()),
                    london: Some(12965000.into()),
                },
                params: Params {
                    chain_id: 4,
                    network_id: 4,
                    maximum_extra_data_size: 65535,
                    min_gas_limit: 5000,
                },
                genesis: Genesis {
                    author: hex!("0000000000000000000000000000000000000000").into(),
                    difficulty: 0x1.into(),
                    gas_limit: 0x47b760,
                    timestamp: 0x58ee40ba,
                    seal: Seal::Clique {
                        vanity: hex!(
                            "52657370656374206d7920617574686f7269746168207e452e436172746d616e"
                        )
                        .into(),
                        signers: vec![
                            hex!("42eb768f2244c8811c63729a21a3569731535f06").into(),
                            hex!("7ffc57839b00206d1ad20c69a1981b489f772031").into(),
                            hex!("b279182d99e65703f0076e4812653aab85fca0f0").into(),
                        ],
                    },
                },
                contracts: btreemap! {
                    0.into() => hashmap! {
                        hex!("0000000000000000000000000000000000000001").into() => Contract::Precompile(Precompile::EcRecover {
                            base: 3000,
                            word: 0,
                        }),
                        hex!("0000000000000000000000000000000000000002").into() => Contract::Precompile(Precompile::Sha256 {
                            base: 60,
                            word: 12,
                        }),
                        hex!("0000000000000000000000000000000000000003").into() => Contract::Precompile(Precompile::Ripemd160 {
                            base: 600,
                            word: 120,
                        }),
                        hex!("0000000000000000000000000000000000000004").into() => Contract::Precompile(Precompile::Identity {
                            base: 15,
                            word: 3,
                        }),
                    },
                    1035301.into() => hashmap! {
                        hex!("0000000000000000000000000000000000000005").into() => Contract::Precompile(Precompile::ModExp {
                            version: ModExpVersion::ModExp198,
                        }),
                        hex!("0000000000000000000000000000000000000006").into() => Contract::Precompile(Precompile::AltBn128Add {
                            price: 500,
                        }),
                        hex!("0000000000000000000000000000000000000007").into() => Contract::Precompile(Precompile::AltBn128Mul {
                            price: 40000,
                        }),
                        hex!("0000000000000000000000000000000000000008").into() => Contract::Precompile(Precompile::AltBn128Pairing {
                            base: 100000,
                            pair: 80000,
                        }),
                    },
                    5435345.into() => hashmap! {
                        hex!("0000000000000000000000000000000000000006").into() => Contract::Precompile(Precompile::AltBn128Add {
                            price: 150,
                        }),
                        hex!("0000000000000000000000000000000000000007").into() => Contract::Precompile(Precompile::AltBn128Mul {
                            price: 6000,
                        }),
                        hex!("0000000000000000000000000000000000000008").into() => Contract::Precompile(Precompile::AltBn128Pairing {
                            base: 45000,
                            pair: 34000,
                        }),
                        hex!("0000000000000000000000000000000000000009").into() => Contract::Precompile(Precompile::Blake2F {
                            gas_per_round: 1,
                        }),
                    },
                    8290928.into() => hashmap! {
                        hex!("0000000000000000000000000000000000000005").into() => Contract::Precompile(Precompile::ModExp {
                            version: ModExpVersion::ModExp2565,
                        })
                    }
                },
                balances: btreemap! {
                    0.into() => hashmap! {
                        hex!("31b98d14007bdee637298086988a0bbd31184523").into() => "0x200000000000000000000000000000000000000000000000000000000000000".into(),
                    },
                },
            },
            *RINKEBY_CONFIG,
        );
    }
}
