#![warn(clippy::all, clippy::pedantic, future_incompatible)]

use clap::Parser;
use kvs::{Command, KvStoreError, Result};

fn main() -> Result<()> {
    let store = kvs::KvStore::new();
    let cmd = Cli::parse().command;

    match store.execute(cmd) {
        Err(e) => {
            println!("{e}");
            if let KvStoreError::FailedGet(_) = e {
                Ok(())
            } else {
                Err(e)
            }
        }
        Ok(s) => {
            println!("{s}");
            Ok(())
        }
    }
}

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}
