use clap::{Parser, Subcommand};
use std::process;

fn main() {
    match Cli::parse().command {
        cmd @ Commands::Get { .. } | cmd @ Commands::Set { .. } | cmd @ Commands::Rm { .. } => {
            exit_subcommand_invalid(cmd)
        }
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

fn exit_subcommand_invalid(cmd: Commands) {
    eprintln!("unimplemented: {cmd:?}");
    process::exit(2)
}
