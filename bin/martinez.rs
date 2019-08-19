use martinez::stagedsync;
use std::time::Duration;
use structopt::StructOpt;
use tokio::time::sleep;
use tracing_subscriber::{prelude::*, EnvFilter};

#[derive(StructOpt)]
#[structopt(name = "Martinez", about = "Ethereum client based on Thorax architecture")]
pub struct Opt {
    #[structopt(long, env)]
    pub tokio_console: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opt = Opt::from_args();

    let filter = if std::env::var(EnvFilter::DEFAULT_ENV)
        .unwrap_or_default()
        .is_empty()
    {
        EnvFilter::new("martinez=info")
    } else {
        EnvFilter::from_default_env()
    };
    let registry = tracing_subscriber::registry()
        // the `TasksLayer` can be used in combination with other `tracing` layers...
        .with(tracing_subscriber::fmt::layer().with_target(false));

    if opt.tokio_console {
        let (layer, server) = console_subscriber::TasksLayer::new();
        registry
            .with(filter.add_directive("tokio=trace".parse()?))
            .with(layer)
            .init();
        tokio::spawn(async move { server.serve().await.expect("server failed") });
    } else {
        registry.with(filter).init();
    }

    let db = martinez::new_mem_database()?;

    let mut staged_sync = stagedsync::StagedSync::new(|| async move {
        sleep(Duration::from_millis(6000)).await;
    });
    staged_sync.push(martinez::stages::HeaderDownload);
    // staged_sync.push(martinez::stages::BlockHashes);
    // staged_sync.push(martinez::stages::ExecutionStage);

    // stagedsync::StagedSync::new(vec![], vec![]);
    staged_sync.run(&db).await?;
}
