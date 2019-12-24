use crate::{kv::*, models::*, Transaction};
use ethereum_types::H256;
use tracing::*;

pub async fn read_chain_config<'db: 'tx, 'tx, Tx: Transaction<'db>>(
    tx: &'tx Tx,
    block: H256,
) -> anyhow::Result<Option<ChainConfig>> {
    trace!("Reading chain config for block {:?}", block,);

    tx.get(&tables::Config, block).await
}
