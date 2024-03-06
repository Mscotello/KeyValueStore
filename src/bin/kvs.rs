use clap::{Parser, Subcommand};
use kvs::KvStore;
use std::{env, process};

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    cmd: Commands,
}

#[derive(Subcommand, Debug, Clone)]
enum Commands {
    Get { key: String },
    Set { key: String, value: String },
    Rm { key: String },
}

fn main() {
    let args = Args::parse();
    let cwd = env::current_dir().unwrap();

    let mut kv_store = match KvStore::open(&cwd) {
        Ok(kv_store) => kv_store,
        Err(e) => {
            println!("Failed to create key-value store: {}", e);
            process::exit(0);
        }
    };

    match args.cmd {
        Commands::Get { key } => {
            let value = kv_store.get(key);
            match value {
                Ok(value) => match value {
                    Some(value) => println!("{value}"),
                    _ => println!("Key not found"),
                },
                Err(e) => {
                    eprint!("Error getting value: {}", e);
                    process::exit(1);
                }
            }
        }
        Commands::Set { key, value } => match kv_store.set(key, value) {
            Ok(_) => (),
            Err(_) => {
                println!("Failed to set key");
                process::exit(1);
            }
        },
        Commands::Rm { key } => match kv_store.remove(key) {
            Ok(_) => (),
            Err(_) => {
                println!("Key not found");
                process::exit(1);
            }
        },
    }

    process::exit(0);
}
