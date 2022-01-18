use martinez::{binutil::MartinezDataDir, kv::mdbx::*, models::*, stagedsync::stages::*};
use async_trait::async_trait;
use clap::Parser;
use ethnum::U256;
use jsonrpsee::{core::RpcResult, http_server::HttpServerBuilder, proc_macros::rpc};
use mdbx::EnvironmentKind;
use std::{future::pending, net::SocketAddr, sync::Arc};
use tracing_subscriber::{prelude::*, EnvFilter};

#[derive(Parser)]
#[clap(name = "Martinez RPC", about = "RPC server for Martinez")]
pub struct Opt {
    #[clap(long)]
    pub datadir: MartinezDataDir,

    #[clap(long)]
    pub listen_address: SocketAddr,
}

#[rpc(server, namespace = "eth")]
pub trait EthApi {
    #[method(name = "blockNumber")]
    async fn block_number(&self) -> RpcResult<BlockNumber>;
    #[method(name = "getBalance")]
    async fn get_balance(&self, address: Address, block_number: BlockNumber) -> RpcResult<U256>;
}

pub struct EthApiServerImpl<E>
where
    E: EnvironmentKind,
{
    db: Arc<MdbxEnvironment<E>>,
}

#[async_trait]
impl<E> EthApiServer for EthApiServerImpl<E>
where
    E: EnvironmentKind,
{
    async fn block_number(&self) -> RpcResult<BlockNumber> {
        Ok(FINISH
            .get_progress(&self.db.begin()?)?
            .unwrap_or(BlockNumber(0)))
    }

    async fn get_balance(&self, address: Address, block_number: BlockNumber) -> RpcResult<U256> {
        Ok(
            martinez::accessors::state::account::read(&self.db.begin()?, address, Some(block_number))?
                .map(|acc| acc.balance)
                .unwrap_or(U256::ZERO),
        )
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opt = Opt::parse();

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

    let db = Arc::new(
        martinez::kv::mdbx::MdbxEnvironment::<mdbx::NoWriteMap>::open_ro(
            mdbx::Environment::new(),
            &opt.datadir,
            martinez::kv::tables::CHAINDATA_TABLES.clone(),
        )?,
    );

    let server = HttpServerBuilder::default().build(opt.listen_address)?;
    let _server_handle = server.start(EthApiServerImpl { db }.into_rpc())?;

    pending().await
}
