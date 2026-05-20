mod commands;
mod config;
mod tracing_setup;
mod tui;

#[tokio::main]
async fn main() {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    let config = config::CliConfig::load();
    let filter = format!("warn,vlinder={}", config.logging.level);
    tracing_setup::init_tracing(&filter);

    commands::run().await;
}
