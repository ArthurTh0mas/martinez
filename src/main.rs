use martinez::table_sizes;
use std::path::PathBuf;
use structopt::StructOpt;

#[derive(StructOpt)]
#[structopt(
    name = "Martinez",
    about = "Ethereum client based on turbo-geth architecture"
)]
pub enum Opt {
    /// Print database statistics
    DbStats {
        /// Chain data path
        #[structopt(parse(from_os_str))]
        chaindata: PathBuf,
        /// Whether to print CSV
        #[structopt(long)]
        csv: bool,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opt = Opt::from_args();

    match opt {
        Opt::DbStats { chaindata, csv } => {
            let env = martinez::Environment::open_ro(
                mdbx::Environment::new(),
                &chaindata,
                &martinez::tables::TABLE_MAP,
            )?;
            let mut sizes = table_sizes(&env.begin_ro_txn()?)?
                .into_iter()
                .collect::<Vec<_>>();
            sizes.sort_by_key(|(_, size)| *size);

            let mut out = Vec::new();
            if csv {
                out.push("Table,Size".to_string());
                for (table, size) in &sizes {
                    out.push(format!("{},{}", table, size));
                }
            } else {
                for (table, size) in &sizes {
                    out.push(format!("{} - {}", table, bytesize::ByteSize::b(*size)));
                }
                out.push(format!(
                    "TOTAL: {}",
                    bytesize::ByteSize::b(sizes.into_iter().map(|(_, size)| size).sum())
                ));
            }

            for line in out {
                println!("{}", line);
            }
        }
    }

    Ok(())
}
