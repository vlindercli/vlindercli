use std::io::{self, Read, Write};
use std::process;
use std::sync::Arc;

use clap::Subcommand;

use crate::config::CliConfig;
use vlinder_core::domain::SecretStore;
use vlinder_nats::secret_service::GrpcSecretClient;

#[derive(Subcommand, Debug, PartialEq)]
pub enum SecretCommand {
    /// Store a secret (reads value from stdin)
    Put { name: String },
    /// Retrieve and print a secret
    Get { name: String },
    /// Delete a secret
    Delete { name: String },
    /// Check if a secret exists (exit code 0/1)
    Exists { name: String },
}

pub async fn execute(cmd: SecretCommand) {
    match cmd {
        SecretCommand::Put { name } => put(&name).await,
        SecretCommand::Get { name } => get(&name).await,
        SecretCommand::Delete { name } => delete(&name).await,
        SecretCommand::Exists { name } => exists(&name).await,
    }
}

async fn open_store() -> Arc<dyn SecretStore> {
    let config = CliConfig::load();
    let addr = config.daemon.secret_addr.clone();
    match GrpcSecretClient::connect_async(&addr).await {
        Ok(client) => Arc::new(client),
        Err(e) => {
            eprintln!("Failed to connect to secret service at {addr}: {e}");
            process::exit(1);
        }
    }
}

async fn put(name: &str) {
    let mut buf = Vec::new();
    if let Err(e) = io::stdin().read_to_end(&mut buf) {
        eprintln!("Failed to read stdin: {e}");
        process::exit(1);
    }

    let store = open_store().await;

    if let Err(e) = store.put(name, &buf).await {
        eprintln!("Failed to store secret: {e}");
        process::exit(1);
    }

    eprintln!("Stored secret '{name}'");
}

async fn get(name: &str) {
    let store = open_store().await;

    match store.get(name).await {
        Ok(value) => {
            if let Err(e) = io::stdout().write_all(&value) {
                eprintln!("Failed to write to stdout: {e}");
                process::exit(1);
            }
        }
        Err(e) => {
            eprintln!("{e}");
            process::exit(1);
        }
    }
}

async fn delete(name: &str) {
    let store = open_store().await;

    if let Err(e) = store.delete(name).await {
        eprintln!("{e}");
        process::exit(1);
    }

    eprintln!("Deleted secret '{name}'");
}

async fn exists(name: &str) {
    let store = open_store().await;

    match store.exists(name).await {
        Ok(true) => {
            println!("exists");
        }
        Ok(false) => {
            println!("not found");
            process::exit(1);
        }
        Err(e) => {
            eprintln!("{e}");
            process::exit(1);
        }
    }
}
