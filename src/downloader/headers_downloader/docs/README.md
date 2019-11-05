# <h1 align="center"> 🧬 Martinez 🧬 </h1>

Next-generation implementation of Ethereum protocol ("client") written in Rust, based on [Erigon architecture](https://github.com/ledgerwatch/interfaces).

## Why run Martinez?

Look at Mgas/s.

![](./src/res/readme-screenshot.png)


## Building the source

Install `rustup` from rustup.rs.

```
git clone https://github.com/ng8eke/martinez
cd martinez
cargo build --all --profile=production
```

You can find built binaries in `target/production` folder.

## Running

* `martinez` takes an _already synced_ [Erigon](https://github.com/ledgerwatch/erigon) database with downloaded blocks and headers (stages 1-3), imports them, executes and verifies state root:

```
martinez --datadir=<path to martinez database directory> --erigon-datadir=<path to Erigon database directory>
```

* `martinez-toolbox` provides various helper commands to check and manipulate martinez's database. Please consult its help for more info:
```
martinez-toolbox --help
```
