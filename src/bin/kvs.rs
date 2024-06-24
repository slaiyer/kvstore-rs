use clap::{Parser, Subcommand};
use std::process;

fn main() {
    let mut store = kvs::KvStore::new();

    match Cli::parse().command {
        Commands::Get { key } => match store.get(key.clone()) {
            Some(value) => println!("{key}: {value}"),
            _ => {
                eprintln!("key not found: {key}");
                process::exit(3)
            }
        },
        Commands::Set { key, value } => store.set(key, value),
        Commands::Rm { key } => store.remove(key),
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
