use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};

use crate::config::Config;

pub fn init_tracing(config: &Config) {
    let logs_dir = crate::config::logs_dir();
    std::fs::create_dir_all(&logs_dir).expect("Failed to create logs directory");

    let file_appender = RollingFileAppender::builder()
        .rotation(Rotation::DAILY)
        .filename_prefix("vlinder")
        .filename_suffix("jsonl")
        .max_log_files(7)
        .build(&logs_dir)
        .expect("Failed to initialize log file appender");

    let stderr_layer = fmt::layer()
        .with_writer(std::io::stderr)
        .with_target(false)
        .with_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new(config.tracing_filter())),
        );

    let file_layer = fmt::layer()
        .json()
        .with_writer(file_appender)
        .with_target(true)
        .with_current_span(true)
        .with_span_list(true)
        .with_filter(EnvFilter::new(
            "vlinderd=trace,vlinder_core=info,vlinder_dolt=info,warn",
        ));

    tracing_subscriber::registry()
        .with(stderr_layer)
        .with(file_layer)
        .init();
}
