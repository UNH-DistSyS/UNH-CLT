#![deny(unsafe_code)]
#![deny(clippy::pedantic)]

use std::{
    net::SocketAddr,
    process::exit,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use clap::Parser;
use glommio::{net::UdpSocket, prelude::*};
use rand::{Rng, SeedableRng};
use tracing::instrument;

use crate::args::{setup_logger, Args};

pub mod args;

static WORKER_ID_COUNTER: AtomicU64 = AtomicU64::new(0);

fn main() {
    let args = Arc::new(Args::parse());

    setup_logger(&args.logging_format);

    tracing::info!(?args);

    let placement = PoolPlacement::MaxSpread(args.workers, None);

    let results = LocalExecutorPoolBuilder::new(placement)
        .io_memory(2 << 32)
        .ring_depth(4096)
        .spin_before_park(Duration::from_secs(1))
        .on_all_shards(|| async move {
            let args = args.clone();
            let id = WORKER_ID_COUNTER.fetch_add(1, Ordering::SeqCst);
            return client_main(id, args).await;
        })
        .expect("failed to spawn executor")
        .join_all();

    for result in results {
        result.unwrap().unwrap();
    }
}

#[instrument(level = "trace", skip(args))]
async fn client_main(id: u64, args: Arc<Args>) -> Result<(), GlommioError<()>> {
    tracing::info!("Worker started");
    let socket = UdpSocket::bind("0:0").unwrap();
    let socket_addr = SocketAddr::new(args.address, args.port);
    socket.connect(socket_addr).await?;

    if args.test_duration.as_millis() > u128::from(u64::MAX) {
        tracing::error!("Please use a reasonable duration for your test.");
        exit(1);
    }

    let mut buffer = vec![0u8; args.packet_size];

    let id_bytes = id.to_le_bytes();
    let id_bytes_len = id_bytes.len();
    buffer[..id_bytes_len].copy_from_slice(&id_bytes[..]);

    let mut sequence_number: u64 = 0;

    let mut rng = rand::rngs::SmallRng::from_entropy();

    // Truncation was checked
    #[allow(clippy::cast_possible_truncation)]
    let wait_time = args
        .pause_duration
        .map_or(Duration::from_secs(0), |wait_time| {
            Duration::from_millis(wait_time.as_millis() as u64)
        });

    #[allow(clippy::cast_possible_truncation)]
    let test_duration = Duration::from_millis(args.test_duration.as_millis() as u64);

    // Truncation was checked

    let stop_time = Instant::now() + test_duration;

    while Instant::now() + wait_time < stop_time {
        // Copy the sequence number into the buffer
        sequence_number = sequence_number.wrapping_add(1);
        let sequence_number_bytes = sequence_number.to_le_bytes();
        let sequence_number_bytes_len = sequence_number_bytes.len();
        buffer[id_bytes_len..id_bytes_len + sequence_number_bytes_len]
            .copy_from_slice(&sequence_number_bytes[..]);

        // random write the rest of the buffer if enabled
        if args.enable_random_padding {
            let offset = id_bytes_len + sequence_number_bytes_len;
            let to_write = &mut buffer[offset..];
            rng.fill(to_write);
        }

        // send the packet
        socket.send(&buffer).await?;
        tracing::trace!(sequence_number, "Sent message");
        if wait_time.as_nanos() > 0 {
            glommio::timer::sleep(wait_time).await;
        }
    }
    tracing::info!(sequence_number, "Worker finished");
    Ok(())
}
