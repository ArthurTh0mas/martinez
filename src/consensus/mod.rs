pub mod ethash;

use crate::{models::*, IntraBlockState, State};
use anyhow::bail;
use async_trait::*;
use downcast_rs::{impl_downcast, DowncastSync};
use std::fmt::Debug;

#[async_trait]
pub trait Consensus: DowncastSync + Debug + Send + Sync + 'static {
    async fn verify_header(
        &self,
        header: &BlockHeader,
        parent: &BlockHeader,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn finalize<'db, S: State>(
        &self,
        state: &mut IntraBlockState<'db, S>,
        header: &BlockHeader,
    ) -> anyhow::Result<()>
    where
        Self: Sized,
    {
        Ok(())
    }
}

impl_downcast!(sync Consensus);

#[derive(Debug)]
pub struct NoProof;

#[async_trait]
impl Consensus for NoProof {
    async fn verify_header(&self, _: &BlockHeader, parent: &BlockHeader) -> anyhow::Result<()> {
        Ok(())
    }
}

pub type Clique = NoProof;
pub type AuRa = NoProof;

pub fn init_consensus(params: ConsensusSpec) -> anyhow::Result<Box<dyn Consensus>> {
    Ok(match params {
        ConsensusSpec::Clique { period, epoch } => bail!("Clique is not yet implemented"),
        ConsensusSpec::Ethash {
            duration_limit,
            block_reward,
            homestead_formula,
            byzantium_adj_factor,
            difficulty_bomb,
        } => Box::new(ethash::Ethash {
            duration_limit,
            block_reward,
            homestead_formula,
            byzantium_adj_factor,
            difficulty_bomb,
        }),
        ConsensusSpec::NoProof => Box::new(NoProof),
    })
}
