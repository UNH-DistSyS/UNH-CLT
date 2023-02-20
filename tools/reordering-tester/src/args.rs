use clap::Parser;
use serde::Serialize;
use std::{net::IpAddr, path::PathBuf};

use strum::{Display, EnumString};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, Display, EnumString, Serialize)]
pub enum LoggingFormat {
    #[strum(ascii_case_insensitive)]
    Tracing,
    #[strum(ascii_case_insensitive)]
    Pretty,
    #[strum(ascii_case_insensitive)]
    Json,
}

#[derive(Debug, Parser)]
pub struct Args {
    /// A UUID to identify this execution. Usually omitted. Randomized for each execution.
    #[arg(long, env, default_value_t = Uuid::new_v4())]
    pub run_id: Uuid,

    /// The destination IPv4 or IPv6 address to send traffic to
    #[arg(long, env)]
    pub address: IpAddr,

    /// The destination port
    #[arg(long, env, default_value_t = 8000)]
    pub port: u16,

    /// The packet size in bytes to send
    #[arg(long, env, default_value_t = 800)]
    pub packet_size: usize,

    /// Pad with random bytes instead of zeros
    #[arg(long, env, default_value_t = false)]
    pub enable_random_padding: bool,

    /// How long to send packets for
    #[arg(long, env, default_value = "60s")]
    pub test_duration: humantime::Duration,

    /// Desired pause between packets (As fast as possible by default)
    #[arg(long, env)]
    pub pause_duration: Option<humantime::Duration>,

    /// The number of threads to spawn to generate packets, must be less than or equal to the number of CPU cores.
    /// Each will try to send the desired packet configuration.
    #[arg(long, env, default_value_t = 1)]
    pub workers: usize,

    /// The logging format
    #[arg(long, env, default_value_t = LoggingFormat::Tracing)]
    pub logging_format: LoggingFormat,

    /// The output path (truncate on collision)
    #[arg(long, env, default_value = "out.csv")]
    pub output_path: PathBuf,
}

pub fn setup_logger(format: &LoggingFormat) {
    match format {
        LoggingFormat::Tracing => tracing_subscriber::fmt().init(),
        LoggingFormat::Pretty => tracing_subscriber::fmt().pretty().init(),
        LoggingFormat::Json => tracing_subscriber::fmt().json().init(),
    };
}
