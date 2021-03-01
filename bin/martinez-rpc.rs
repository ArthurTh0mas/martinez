use martinez::{
    binutil::MartinezDataDir,
    kv::{tables, traits::*},
    models::*,
    stagedsync::stages::*,
};
use async_trait::async_trait;
use ethereum_types::{Address, U256};
use jsonrpsee::{core::RpcResult, http_server::HttpServerBuilder, proc_macros::rpc};
use std::{future::pending, net::SocketAddr, sync::Arc};
use structopt::StructOpt;
use tracing_subscriber::{prelude::*, EnvFilter};

#[derive(StructOpt)]
#[structopt(name = "Martinez RPC", about = "RPC server for Martinez")]
pub struct Opt {
    #[structopt(long, env)]
    pub datadir: MartinezDataDir,

    #[structopt(long, env)]
    pub listen_address: SocketAddr,
}

#[rpc(server, namespace = "eth")]
pub trait EthApi {
    #[method(name = "blockNumber")]
    async fn block_number(&self) -> RpcResult<BlockNumber>;
    #[method(name = "getBalance")]
    async fn get_balance(&self, address: Address, block_number: BlockNumber) -> RpcResult<U256>;
}

pub struct EthApiServerImpl<DB>
where
    DB: KV,
{
    db: Arc<DB>,
}

#[async_trait]
impl<DB> EthApiServer for EthApiServerImpl<DB>
where
    DB: KV,
{
    async fn block_number(&self) -> RpcResult<BlockNumber> {
        Ok(self
            .db
            .begin()
            .await?
            .get(&tables::SyncStage, FINISH)
            .await?
            .unwrap_or(BlockNumber(0)))
    }

    async fn get_balance(&self, address: Address, block_number: BlockNumber) -> RpcResult<U256> {
        Ok(
            martinez::get_account_data_as_of(&self.db.begin().await?, address, block_number)
                .await?
                .map(|acc| acc.balance)
                .unwrap_or_else(U256::zero),
        )
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opt = Opt::from_args();

    let env_filter = if std::env::var(EnvFilter::DEFAULT_ENV)
        .unwrap_or_default()
        .is_empty()
    {
        EnvFilter::new("martinez=info,rpc=info")
    } else {
        EnvFilter::from_default_env()
    };
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_target(false))
        .with(env_filter)
        .init();

    let db = Arc::new(martinez::MdbxEnvironment::<mdbx::NoWriteMap>::open_ro(
        mdbx::Environment::new(),
        &opt.datadir,
        martinez::kv::tables::CHAINDATA_TABLES.clone(),
    )?);

    let server = HttpServerBuilder::default().build(opt.listen_address)?;
    let _server_handle = server.start(EthApiServerImpl { db }.into_rpc())?;

    pending().await
}
