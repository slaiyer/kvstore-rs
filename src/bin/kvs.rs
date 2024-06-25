#![deny(missing_docs)]
#![warn(clippy::all, clippy::pedantic, future_incompatible)]

//! Key-value (KV) store CLI client

use clap::Parser;
use kvs::{Command, Result};
use std::env;

fn main() -> Result<()> {
    let current_dir = env::current_dir().unwrap();
    let store = kvs::KvStore::open(current_dir)?;

    let cmd = Cli::parse().command;
    match store.execute(cmd) {
        Err(e) => {
            println!("{e}");
            Err(e)
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
