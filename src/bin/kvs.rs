#![warn(clippy::all, clippy::pedantic, future_incompatible)]

use clap::{Parser, Subcommand};
use std::process;

fn main() {
    let mut store = kvs::KvStore::new();

    match Cli::parse().command {
        Commands::Get { key } => {
            if let Some(value) = store.get(&key) {
                println!("{key}: {value}");
            } else {
                eprintln!("key not found: {key}");
                process::exit(3)
            }
        }
        Commands::Set { key, value } => store.set(key, value),
        Commands::Rm { key } => store.remove(&key),
    }
}

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    Get {
        #[arg(required = true)]
        key: String,
    },
    Set {
        #[arg(required = true)]
        key: String,
        #[arg(required = true)]
        value: String,
    },
    Rm {
        #[arg(required = true)]
        key: String,
    },
}
