#![warn(clippy::all, clippy::pedantic, future_incompatible)]

use clap::Parser;
use kvs::{Command, KvStoreError, Result};

fn main() -> Result<()> {
    let store = kvs::KvStore::new();
    let cmd = Cli::parse().command;

    match store.execute(cmd) {
        Err(e) => match e {
            KvStoreError::DeserializeCommand(_)
            | KvStoreError::InvalidCommand(_)
            | KvStoreError::MissingKey(_)
            | KvStoreError::MissingValue(_)
            | KvStoreError::FailedRead(_)
            | KvStoreError::FailedSet(_)
            | KvStoreError::FailedRm => {
                println!("{e}");
                Err(e)
            }
            KvStoreError::FailedGet => {
                println!("{e}");
                Ok(())
            }
        },
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
